# v243_official_release_main.py
# 正式上線版（Production / Release Candidate）
# 目的：
# 1) 固定策略核心（不再動 Alpha）
# 2) 支援 2022-2025 任意區間回測 / walk-forward 模擬
# 3) 保留 execution realism：T+1、手續費、交易稅、滑價、一次性觸發、成交量限制
# 4) 輸出正式版結果：nav / trades / signals / daily_positions / summary / meta
# 5) 支援 CLI 參數，方便 GitHub Actions / 本機 / 雲端 / API 共用
#
# 使用方式範例：
# python v243_official_release_main.py ^
#   --price price_panel_daily.csv ^
#   --macro macro_signal.csv ^
#   --start 2022-01-01 ^
#   --end 2025-12-31 ^
#   --output-prefix v243_full_2022_2025
#
# 注意：
# - 不再修改 ranking / TOP_N / stop/add 核心
# - 手機 App 不應直接跑此檔；建議由 API / 雲端工作流執行後同步結果

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

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

MAX_DAILY_VOLUME_PARTICIPATION = 0.30
MIN_TRADE_VALUE = 500.0
MIN_POSITION_VALUE = 500.0

# macro 不再當開關，而是當調節器
MIN_EXPOSURE_FLOOR = 0.35


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--price", default="price_panel_daily.csv")
    p.add_argument("--macro", default="macro_signal.csv")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    p.add_argument("--output-prefix", default="v243_release")
    p.add_argument("--max-volume-participation", type=float, default=MAX_DAILY_VOLUME_PARTICIPATION)
    p.add_argument("--buy-slippage", type=float, default=SLIPPAGE_BUY)
    p.add_argument("--sell-slippage", type=float, default=SLIPPAGE_SELL)
    p.add_argument("--buy-fee-rate", type=float, default=BUY_FEE_RATE)
    p.add_argument("--sell-fee-rate", type=float, default=SELL_FEE_RATE)
    p.add_argument("--sell-tax-rate", type=float, default=SELL_TAX_RATE)
    return p.parse_args()


def load_price_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date", "date", "datetime", "signal_date"] if c in df.columns), None)
    if date_col is None:
        raise ValueError("price file 缺少日期欄位（trade_date/date/datetime/signal_date）")
    if "symbol" not in df.columns:
        raise ValueError("price file 缺少 symbol 欄位")
    if "close" not in df.columns:
        raise ValueError("price file 缺少 close 欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    else:
        df["volume"] = np.nan

    df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    g = df.groupby("symbol")
    df["ret"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)

    return df


def load_macro_data(path: str) -> pd.DataFrame:
    macro = pd.read_csv(path)
    macro.columns = [str(c).lower().strip() for c in macro.columns]
    if "trade_date" not in macro.columns or "macro_score" not in macro.columns:
        raise ValueError("macro file 需要包含 trade_date, macro_score")

    macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    macro = (
        macro[["trade_date", "macro_score"]]
        .dropna(subset=["trade_date"])
        .drop_duplicates(subset=["trade_date"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return macro


def merge_data(price: pd.DataFrame, macro: pd.DataFrame, start=None, end=None) -> pd.DataFrame:
    df = price.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)

    if start:
        df = df[df["trade_date"] >= pd.Timestamp(start)]
    if end:
        df = df[df["trade_date"] <= pd.Timestamp(end)]

    df = df.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    return df


def get_exposure(macro_score: float) -> float:
    if macro_score > 0.5:
        return 1.0
    elif macro_score > 0:
        return 1.0
    elif macro_score > -0.6:
        return 0.8
    else:
        return MIN_EXPOSURE_FLOOR


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


def calc_fee_tax(trade_value: float, side: str, buy_fee_rate: float, sell_fee_rate: float, sell_tax_rate: float) -> float:
    if trade_value <= 0:
        return 0.0
    if side == "BUY":
        return trade_value * buy_fee_rate
    elif side == "SELL":
        return trade_value * (sell_fee_rate + sell_tax_rate)
    return 0.0


def desired_symbol_count(exposure: float) -> int:
    if exposure >= 0.95:
        return TOP_N
    if exposure >= 0.70:
        return 4
    return MIN_HOLDINGS_FLOOR


def apply_execution_targets(
    pending_targets: dict,
    holdings: dict,
    cash: float,
    day_map: dict,
    trade_date,
    nav_before_trade: float,
    trade_rows: list,
    cfg: dict,
):
    if pending_targets is None:
        return holdings, cash

    symbols = sorted(set(list(holdings.keys()) + list(pending_targets.keys())))

    # 先賣後買，避免可釋放資金沒被用到
    sell_first = []
    buy_second = []
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
        target_value = max(0.0, target_weight * nav_before_trade)
        delta_value = target_value - current_value

        if abs(delta_value) < MIN_TRADE_VALUE:
            continue

        if delta_value < 0:
            sell_first.append((sym, row, close_px, current_shares, current_value, target_value, delta_value))
        else:
            buy_second.append((sym, row, close_px, current_shares, current_value, target_value, delta_value))

    for bucket in [sell_first, buy_second]:
        for sym, row, close_px, current_shares, current_value, target_value, delta_value in bucket:
            volume = row.get("volume", np.nan)
            if np.isfinite(volume) and volume > 0:
                max_trade_value = float(volume) * close_px * cfg["max_volume_participation"]
                if max_trade_value <= 0:
                    continue
                delta_value = max(-max_trade_value, min(max_trade_value, delta_value))
                if abs(delta_value) < MIN_TRADE_VALUE:
                    continue

            if delta_value > 0:
                exec_px = close_px * (1.0 + cfg["buy_slippage"])
                shares = delta_value / exec_px
                if shares <= 0:
                    continue

                trade_value = shares * exec_px
                fee_tax = calc_fee_tax(
                    trade_value, "BUY", cfg["buy_fee_rate"], cfg["sell_fee_rate"], cfg["sell_tax_rate"]
                )
                total_cash_needed = trade_value + fee_tax

                if total_cash_needed > cash:
                    if cash <= 0:
                        continue
                    shares = cash / (exec_px * (1.0 + cfg["buy_fee_rate"]))
                    trade_value = shares * exec_px
                    fee_tax = calc_fee_tax(
                        trade_value, "BUY", cfg["buy_fee_rate"], cfg["sell_fee_rate"], cfg["sell_tax_rate"]
                    )
                    total_cash_needed = trade_value + fee_tax

                if trade_value < MIN_TRADE_VALUE or shares <= 0:
                    continue

                prev_shares = float(holdings[sym].get("shares", 0.0))
                prev_cost = float(holdings[sym].get("avg_cost", 0.0))
                new_shares = prev_shares + shares
                new_avg_cost = ((prev_shares * prev_cost) + (shares * exec_px)) / max(new_shares, 1e-12)

                holdings[sym]["shares"] = new_shares
                holdings[sym]["avg_cost"] = new_avg_cost
                cash -= total_cash_needed

                trade_rows.append([
                    trade_date, "BUY", sym, close_px, exec_px, shares, trade_value, fee_tax, "T+1 target execution"
                ])

            else:
                desired_sell_value = abs(delta_value)
                exec_px = close_px * (1.0 - cfg["sell_slippage"])
                sell_shares = min(current_shares, desired_sell_value / max(exec_px, 1e-12))
                if sell_shares <= 0:
                    continue

                trade_value = sell_shares * exec_px
                if trade_value < MIN_TRADE_VALUE:
                    continue

                fee_tax = calc_fee_tax(
                    trade_value, "SELL", cfg["buy_fee_rate"], cfg["sell_fee_rate"], cfg["sell_tax_rate"]
                )
                proceeds = trade_value - fee_tax

                holdings[sym]["shares"] = current_shares - sell_shares
                cash += proceeds

                if holdings[sym]["shares"] * close_px < MIN_POSITION_VALUE:
                    holdings[sym]["shares"] = 0.0

                trade_rows.append([
                    trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax, "T+1 target execution"
                ])

    holdings = {
        s: p for s, p in holdings.items()
        if float(p.get("shares", 0.0)) > 0
    }
    return holdings, cash


def build_next_day_targets(day_df: pd.DataFrame, holdings: dict):
    if day_df.empty:
        return {}, holdings, 0.0, []

    macro_score = float(day_df["macro_score"].iloc[0])
    exposure = get_exposure(macro_score)
    ranked = rank_day(day_df)
    day_map = {str(r["symbol"]): r for _, r in day_df.iterrows()}
    signals = []
    desired = {}

    # 現有持倉：先保留，再依 stop/add 調整 multiplier
    for sym, pos in list(holdings.items()):
        ensure_state_fields(pos)
        row = day_map.get(sym)
        if row is None:
            continue

        close_px = float(row["close"])
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

    # 用 exposure 決定應維持幾檔，但不低於 3 檔
    target_slots = desired_symbol_count(exposure)
    active_set = {s for s, keep in desired.items() if keep > 0}

    # 每天補股
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

    mult_sum = sum(float(holdings[s].get("weight_mult", 1.0)) for s in keep_syms)
    if mult_sum <= 0:
        target_weights = {sym: exposure / len(keep_syms) for sym in keep_syms}
    else:
        target_weights = {
            sym: exposure * float(holdings[sym].get("weight_mult", 1.0)) / mult_sum
            for sym in keep_syms
        }

    return target_weights, holdings, exposure, signals


def simulate(df: pd.DataFrame, cfg: dict):
    all_dates = sorted(df["trade_date"].dropna().unique())
    holdings = {}
    cash = cfg["initial_capital"]
    peak_nav = cfg["initial_capital"]

    nav_rows = []
    trade_rows = []
    signal_rows = []
    position_rows = []

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
            pending_targets, holdings, cash, day_map, date, nav_before_trade, trade_rows, cfg
        )

        nav = cash
        count = 0
        gross_value = 0.0

        to_drop = []
        for sym, pos in list(holdings.items()):
            row = day_map.get(sym)
            if row is None:
                continue
            px = float(row["close"])
            position_value = float(pos.get("shares", 0.0)) * px
            if position_value >= MIN_POSITION_VALUE:
                nav += position_value
                gross_value += position_value
                count += 1
                position_rows.append([
                    date, sym, px, float(pos.get("shares", 0.0)), float(pos.get("avg_cost", 0.0)),
                    position_value
                ])
            else:
                to_drop.append(sym)

        for sym in to_drop:
            holdings.pop(sym, None)

        peak_nav = max(peak_nav, nav)
        dd = nav / peak_nav - 1.0
        macro_score = float(day["macro_score"].iloc[0])
        current_exposure = gross_value / nav if nav > 0 else 0.0

        nav_rows.append([date, nav, cash, macro_score, current_exposure, dd, count])

        pending_targets, holdings, signal_exposure, signals = build_next_day_targets(day, holdings)
        for sym, action, pnl in signals:
            signal_rows.append([date, action, sym, pnl, signal_exposure])

    nav_df = pd.DataFrame(nav_rows, columns=["date", "nav", "cash", "macro", "exposure", "dd", "count"])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)

    trade_df = pd.DataFrame(
        trade_rows,
        columns=["date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "note"]
    )
    signal_df = pd.DataFrame(
        signal_rows,
        columns=["date", "signal", "symbol", "pnl_vs_cost", "target_exposure"]
    )
    position_df = pd.DataFrame(
        position_rows,
        columns=["date", "symbol", "close_px", "shares", "avg_cost", "position_value"]
    )
    return nav_df, trade_df, signal_df, position_df


def build_summary(nav: pd.DataFrame, trades: pd.DataFrame, signal_df: pd.DataFrame):
    total_return = nav["nav"].iloc[-1] / nav["nav"].iloc[0] - 1.0 if len(nav) > 0 else np.nan
    mdd = nav["dd"].min() if len(nav) > 0 else np.nan
    avg_exposure = nav["exposure"].mean() if len(nav) > 0 else np.nan
    avg_count = nav["count"].mean() if len(nav) > 0 else np.nan
    trade_count = len(trades)

    daily_ret = nav["ret"].fillna(0.0)
    vol = daily_ret.std(ddof=0)
    sharpe = (daily_ret.mean() / vol * np.sqrt(252)) if vol > 0 else np.nan
    win_day_ratio = (daily_ret > 0).mean() if len(daily_ret) > 0 else np.nan

    buy_count = int((trades["action"] == "BUY").sum()) if len(trades) > 0 else 0
    sell_count = int((trades["action"] == "SELL").sum()) if len(trades) > 0 else 0

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "avg_exposure": avg_exposure,
        "avg_count": avg_count,
        "trade_count": trade_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "signal_count": len(signal_df),
        "sharpe_daily": sharpe,
        "win_day_ratio": win_day_ratio,
        "start_date": str(nav["date"].iloc[0]) if len(nav) else None,
        "end_date": str(nav["date"].iloc[-1]) if len(nav) else None,
    }])


def main():
    args = parse_args()
    cfg = {
        "initial_capital": args.initial_capital,
        "max_volume_participation": args.max_volume_participation,
        "buy_slippage": args.buy_slippage,
        "sell_slippage": args.sell_slippage,
        "buy_fee_rate": args.buy_fee_rate,
        "sell_fee_rate": args.sell_fee_rate,
        "sell_tax_rate": args.sell_tax_rate,
    }

    price = load_price_data(args.price)
    macro = load_macro_data(args.macro)
    df = merge_data(price, macro, start=args.start, end=args.end)

    if df.empty:
        raise ValueError("篩選後資料為空，請檢查 start/end 與來源檔案")

    nav, trades, signals, positions = simulate(df, cfg)
    summary = build_summary(nav, trades, signals)

    prefix = Path(args.output_prefix)
    nav.to_csv(f"{prefix}_nav.csv", index=False)
    trades.to_csv(f"{prefix}_trades.csv", index=False)
    signals.to_csv(f"{prefix}_signals.csv", index=False)
    positions.to_csv(f"{prefix}_daily_positions.csv", index=False)
    summary.to_csv(f"{prefix}_summary.csv", index=False)

    meta = {
        "strategy_version": "v243_official_release",
        "price_file": args.price,
        "macro_file": args.macro,
        "start": args.start,
        "end": args.end,
        "config": cfg,
    }
    Path(f"{prefix}_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
