# v242_1_execution_ready_main.py
# v242.1 Execution-Ready（持倉修復版）
# 目標：
# 1) 保留 T+1 執行、成本、滑價、一次性觸發
# 2) 修復 v242 把持倉連續性打斷的問題
# 3) macro 改為曝險調節，不再整組清空
# 4) 優先維持 3~5 檔持倉，避免長期空倉
# 5) 放寬成交限制，讓補股機制能真正落地

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5
MIN_HOLDINGS_FLOOR = 3

STOP_LOSS_1 = -0.07
STOP_LOSS_2 = -0.12
ADD_LV1 = 0.05
ADD_LV2 = 0.10

BUY_FEE_RATE = 0.001425
SELL_FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003
SLIPPAGE_BUY = 0.0010
SLIPPAGE_SELL = 0.0010

MAX_DAILY_VOLUME_PARTICIPATION = 0.30   # v242.1 放寬，避免補股失敗
MIN_TRADE_VALUE = 500.0                 # 降低門檻，避免小額補不進
MIN_POSITION_VALUE = 500.0

MACRO_FLOOR_EXPOSURE = 0.35             # macro 再弱也保留低曝險，不整組清空
REDEPLOY_CASH_RATIO = 0.98              # 補股時可動用的現金比例


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date", "date", "datetime", "signal_date"] if c in df.columns), None)
    if date_col is None:
        raise ValueError("price_panel_daily.csv 缺少日期欄位（trade_date/date/datetime/signal_date）")
    if "symbol" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 symbol 欄位")
    if "close" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 close 欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    else:
        df["volume"] = np.nan

    df = df.dropna(subset=["trade_date", "symbol", "close"])
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    g = df.groupby("symbol")
    df["ret"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro.columns = [str(c).lower().strip() for c in macro.columns]
    if "trade_date" not in macro.columns or "macro_score" not in macro.columns:
        raise ValueError("macro_signal.csv 需要包含 trade_date, macro_score")

    macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    macro = macro[["trade_date", "macro_score"]].drop_duplicates(subset=["trade_date"]).sort_values("trade_date")

    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    return df


def get_exposure(macro_score: float) -> float:
    # v242.1：macro 只調節曝險，不再作為全清開關
    if macro_score > 0.5:
        return 1.00
    elif macro_score > 0.0:
        return 1.00
    elif macro_score > -0.6:
        return 0.80
    else:
        return MACRO_FLOOR_EXPOSURE


def rank_day(day_df: pd.DataFrame) -> pd.DataFrame:
    d = day_df.dropna(subset=["mom5", "mom10"]).copy()
    d["score"] = d["mom10"] * 0.6 + d["mom5"] * 0.4
    return d.sort_values("score", ascending=False).reset_index(drop=True)


def ensure_state_fields(state: dict):
    state.setdefault("shares", 0.0)
    state.setdefault("avg_cost", 0.0)
    state.setdefault("weight_mult", 1.0)
    state.setdefault("stop1_hit", False)
    state.setdefault("add1_hit", False)
    state.setdefault("add2_hit", False)
    state.setdefault("last_close", np.nan)


def estimate_fee_and_tax(trade_value: float, side: str) -> float:
    if trade_value <= 0:
        return 0.0
    if side == "BUY":
        return trade_value * BUY_FEE_RATE
    if side == "SELL":
        return trade_value * (SELL_FEE_RATE + SELL_TAX_RATE)
    return 0.0


def apply_execution_targets(
    pending_targets: dict,
    holdings: dict,
    cash: float,
    day_map: dict,
    trade_date,
    mark_nav_before_trade: float,
    trade_rows: list,
):
    """
    用今天收盤價執行昨天產生的 target weights。
    執行優先順序：
    1) 先賣（釋放現金）
    2) 再買（補齊持倉）
    v242.1：偏向維持持倉數，不讓現金限制把部位自然縮掉
    """
    if pending_targets is None:
        return holdings, cash

    symbols = sorted(set(list(holdings.keys()) + list(pending_targets.keys())))

    # 先處理 SELL，避免本來有賣單但買單先卡死
    for pass_side in ["SELL", "BUY"]:
        for sym in symbols:
            row = day_map.get(sym)
            if row is None:
                continue

            close_px = float(row["close"])
            if not np.isfinite(close_px) or close_px <= 0:
                continue

            ensure_state_fields(holdings.setdefault(sym, {}))
            current_shares = float(holdings[sym].get("shares", 0.0))
            current_value = current_shares * close_px
            target_weight = float(pending_targets.get(sym, 0.0))
            target_value = max(0.0, target_weight * mark_nav_before_trade)
            delta_value = target_value - current_value

            if abs(delta_value) < MIN_TRADE_VALUE:
                continue

            volume = row.get("volume", np.nan)
            if np.isfinite(volume) and volume > 0:
                max_trade_value = float(volume) * close_px * MAX_DAILY_VOLUME_PARTICIPATION
                if max_trade_value <= 0:
                    continue
                delta_value = max(-max_trade_value, min(max_trade_value, delta_value))
                if abs(delta_value) < MIN_TRADE_VALUE:
                    continue

            if pass_side == "SELL" and delta_value < 0:
                exec_px = close_px * (1.0 - SLIPPAGE_SELL)
                desired_sell_value = abs(delta_value)
                sell_shares = min(current_shares, desired_sell_value / max(exec_px, 1e-12))
                if sell_shares <= 0:
                    continue

                trade_value = sell_shares * exec_px
                if trade_value < MIN_TRADE_VALUE:
                    continue

                fee_tax = estimate_fee_and_tax(trade_value, "SELL")
                proceeds = trade_value - fee_tax

                holdings[sym]["shares"] = current_shares - sell_shares
                cash += proceeds

                if holdings[sym]["shares"] * close_px < MIN_POSITION_VALUE:
                    holdings[sym]["shares"] = 0.0

                trade_rows.append([
                    trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax,
                    "T+1 target execution"
                ])

            if pass_side == "BUY" and delta_value > 0:
                exec_px = close_px * (1.0 + SLIPPAGE_BUY)
                raw_shares = delta_value / exec_px
                if raw_shares <= 0:
                    continue

                trade_value = raw_shares * exec_px
                fee_tax = estimate_fee_and_tax(trade_value, "BUY")
                total_cash_needed = trade_value + fee_tax

                buyable_cash = max(0.0, cash * REDEPLOY_CASH_RATIO)
                if total_cash_needed > buyable_cash:
                    if buyable_cash <= 0:
                        continue
                    raw_shares = buyable_cash / (exec_px * (1.0 + BUY_FEE_RATE))
                    trade_value = raw_shares * exec_px
                    fee_tax = estimate_fee_and_tax(trade_value, "BUY")
                    total_cash_needed = trade_value + fee_tax

                if trade_value < MIN_TRADE_VALUE or raw_shares <= 0:
                    continue

                prev_shares = float(holdings[sym].get("shares", 0.0))
                prev_cost = float(holdings[sym].get("avg_cost", 0.0))
                new_shares = prev_shares + raw_shares
                new_avg_cost = ((prev_shares * prev_cost) + (raw_shares * exec_px)) / max(new_shares, 1e-12)

                holdings[sym]["shares"] = new_shares
                holdings[sym]["avg_cost"] = new_avg_cost
                cash -= total_cash_needed

                trade_rows.append([
                    trade_date, "BUY", sym, close_px, exec_px, raw_shares, trade_value, fee_tax,
                    "T+1 target execution"
                ])

    to_drop = []
    for sym, pos in holdings.items():
        row = day_map.get(sym)
        px = float(row["close"]) if row is not None else np.nan
        shares = float(pos.get("shares", 0.0))
        if shares <= 0 or (np.isfinite(px) and shares * px < MIN_POSITION_VALUE):
            to_drop.append(sym)
    for sym in to_drop:
        holdings.pop(sym, None)

    return holdings, cash


def build_next_day_targets(day_df: pd.DataFrame, holdings: dict):
    """
    用今天資料建立明天的 target weights。
    核心修正：
    1) macro 只縮曝險，不全清
    2) 現有持倉只因 STOP_FULL 才移除
    3) 持倉不足時一定補到至少 MIN_HOLDINGS_FLOOR，目標仍優先補滿 TOP_N
    """
    if day_df.empty:
        return {}, holdings, 0.0, []

    macro_score = float(day_df["macro_score"].iloc[0])
    exposure = get_exposure(macro_score)
    ranked = rank_day(day_df)
    day_map = {str(r["symbol"]): r for _, r in day_df.iterrows()}

    signals = []
    desired = {}

    for sym, pos in list(holdings.items()):
        ensure_state_fields(pos)
        row = day_map.get(sym)
        if row is None:
            continue

        close_px = float(row["close"])
        if not np.isfinite(close_px) or close_px <= 0:
            continue

        avg_cost = float(pos.get("avg_cost", 0.0))
        pnl = (close_px / avg_cost - 1.0) if avg_cost > 0 else 0.0

        if pnl <= STOP_LOSS_2:
            desired[sym] = 0.0
            signals.append((sym, "STOP_FULL", pnl))
            continue

        if (pnl <= STOP_LOSS_1) and (not pos["stop1_hit"]):
            pos["weight_mult"] *= 0.5
            pos["stop1_hit"] = True
            signals.append((sym, "STOP_PART", pnl))

        if (pnl >= ADD_LV1) and (not pos["add1_hit"]):
            pos["weight_mult"] *= 1.05
            pos["add1_hit"] = True
            signals.append((sym, "ADD_LV1", pnl))

        if (pnl >= ADD_LV2) and (not pos["add2_hit"]):
            pos["weight_mult"] *= 1.10
            pos["add2_hit"] = True
            signals.append((sym, "ADD_LV2", pnl))

        desired[sym] = 1.0

    active_syms = [s for s, keep in desired.items() if keep > 0]
    active_set = set(active_syms)

    # 先確保至少 3 檔，再盡量補到 TOP_N
    target_slots = TOP_N if exposure >= 0.70 else max(MIN_HOLDINGS_FLOOR, min(TOP_N, len(active_set) if len(active_set) > 0 else MIN_HOLDINGS_FLOOR))
    target_slots = max(target_slots, MIN_HOLDINGS_FLOOR)

    if len(active_set) < target_slots:
        for _, r in ranked.iterrows():
            sym = str(r["symbol"])
            if sym in active_set:
                continue
            holdings.setdefault(sym, {
                "shares": 0.0,
                "avg_cost": 0.0,
                "weight_mult": 1.0,
                "stop1_hit": False,
                "add1_hit": False,
                "add2_hit": False,
                "last_close": np.nan,
            })
            desired[sym] = 1.0
            active_set.add(sym)
            signals.append((sym, "BUY_CANDIDATE", np.nan))
            if len(active_set) >= target_slots:
                break

    keep_syms = [s for s, keep in desired.items() if keep > 0]
    if len(keep_syms) == 0:
        return {}, holdings, exposure, signals

    mult_sum = 0.0
    for sym in keep_syms:
        ensure_state_fields(holdings[sym])
        mult_sum += float(holdings[sym].get("weight_mult", 1.0))

    if mult_sum <= 0:
        target_weights = {sym: exposure / len(keep_syms) for sym in keep_syms}
    else:
        target_weights = {
            sym: exposure * float(holdings[sym].get("weight_mult", 1.0)) / mult_sum
            for sym in keep_syms
        }

    return target_weights, holdings, exposure, signals


def simulate(df: pd.DataFrame):
    all_dates = sorted(df["trade_date"].dropna().unique())
    holdings = {}
    cash = INITIAL_CAPITAL
    peak_nav = INITIAL_CAPITAL

    nav_rows = []
    trade_rows = []
    signal_rows = []

    pending_targets = None

    for date in all_dates:
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue

        day_map = {str(r["symbol"]): r for _, r in day.iterrows()}

        mtm_value = 0.0
        for sym, pos in list(holdings.items()):
            row = day_map.get(sym)
            if row is None:
                continue
            px = float(row["close"])
            if np.isfinite(px) and px > 0:
                mtm_value += float(pos.get("shares", 0.0)) * px
                holdings[sym]["last_close"] = px

        nav_before_trade = cash + mtm_value

        holdings, cash = apply_execution_targets(
            pending_targets=pending_targets,
            holdings=holdings,
            cash=cash,
            day_map=day_map,
            trade_date=date,
            mark_nav_before_trade=nav_before_trade,
            trade_rows=trade_rows,
        )

        nav = cash
        count = 0
        gross_value = 0.0
        for sym, pos in list(holdings.items()):
            row = day_map.get(sym)
            if row is None:
                continue
            px = float(row["close"])
            if np.isfinite(px) and px > 0:
                position_value = float(pos.get("shares", 0.0)) * px
                if position_value >= MIN_POSITION_VALUE:
                    nav += position_value
                    gross_value += position_value
                    count += 1
                else:
                    holdings[sym]["shares"] = 0.0

        holdings = {s: p for s, p in holdings.items() if float(p.get("shares", 0.0)) > 0}

        peak_nav = max(peak_nav, nav)
        dd = nav / peak_nav - 1.0

        macro_score = float(day["macro_score"].iloc[0]) if "macro_score" in day.columns else 0.0
        current_exposure = gross_value / nav if nav > 0 else 0.0

        nav_rows.append([
            date, nav, cash, macro_score, current_exposure, dd, count
        ])

        pending_targets, holdings, signal_exposure, signals = build_next_day_targets(day, holdings)
        for sym, action, pnl in signals:
            signal_rows.append([date, action, sym, pnl, signal_exposure])

    nav_df = pd.DataFrame(nav_rows, columns=[
        "date", "nav", "cash", "macro", "exposure", "dd", "count"
    ])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)

    trade_df = pd.DataFrame(trade_rows, columns=[
        "date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "note"
    ])

    signal_df = pd.DataFrame(signal_rows, columns=[
        "date", "signal", "symbol", "pnl_vs_cost", "target_exposure"
    ])

    return nav_df, trade_df, signal_df


def build_summary(nav: pd.DataFrame, trades: pd.DataFrame):
    if nav.empty:
        raise ValueError("nav 為空，無法產生 summary")

    total_return = nav["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0
    mdd = nav["dd"].min()
    avg_exposure = nav["exposure"].mean()
    avg_count = nav["count"].mean()
    trade_count = len(trades)

    daily_ret = nav["ret"].fillna(0.0)
    vol = daily_ret.std(ddof=0)
    sharpe = (daily_ret.mean() / vol * np.sqrt(252)) if vol > 0 else np.nan
    win_days = (daily_ret > 0).mean() if len(daily_ret) > 0 else np.nan

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "avg_exposure": avg_exposure,
        "avg_count": avg_count,
        "trade_count": trade_count,
        "sharpe_daily": sharpe,
        "win_day_ratio": win_days,
    }])


def main():
    df = load_data()
    df = load_macro(df)

    nav, trades, signals = simulate(df)
    summary = build_summary(nav, trades)

    nav.to_csv("v242_1_nav.csv", index=False)
    trades.to_csv("v242_1_trades.csv", index=False)
    signals.to_csv("v242_1_signals.csv", index=False)
    summary.to_csv("v242_1_summary.csv", index=False)

    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
