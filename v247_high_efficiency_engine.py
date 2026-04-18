# v247_high_efficiency_engine.txt
# v247 高效率版（在 v246 基礎上放大 alpha，但保留生存能力）
# 設計目標：
# 1) 嚴格 T+1，不偷看
# 2) 保留 v246 可活的風控骨架
# 3) 放寬曝險與持股數，讓策略「敢賺」
# 4) 目標：Sharpe 提升、Return 提升、MDD 仍控制在可活範圍

import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd

INITIAL_CAPITAL = 100000.0
TOP_N = 6

STOP_LOSS_1 = -0.05
STOP_LOSS_2 = -0.09
ADD_LV1 = 0.05
ADD_LV2 = 0.10

BUY_FEE_RATE = 0.001425
SELL_FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003
SLIPPAGE_BUY = 0.0010
SLIPPAGE_SELL = 0.0010

MAX_DAILY_VOLUME_PARTICIPATION = 0.20
MIN_TRADE_VALUE = 1000.0
MIN_POSITION_VALUE = 1000.0
MIN_DAILY_TURNOVER = 1_000_000.0

MAX_POSITION_WEIGHT = 0.20
MAX_GROSS_EXPOSURE = 0.95
MIN_ACTIVE_EXPOSURE = 0.30

NAV_SOFT_DD = -0.10
NAV_MEDIUM_DD = -0.18
NAV_HARD_DD = -0.25
NAV_SOFT_EXPOSURE_CAP = 0.75
NAV_MEDIUM_EXPOSURE_CAP = 0.55
NAV_HARD_EXPOSURE_CAP = 0.30
REENTRY_DD = -0.10

PRICE_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--label", required=True)
    return p.parse_args()


def load_price_panel():
    path = Path(PRICE_FILE)
    if not path.exists():
        raise FileNotFoundError(f"缺少 {PRICE_FILE}")
    df = pd.read_csv(path)
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date", "date", "datetime", "signal_date"] if c in df.columns), None)
    if date_col is None:
        raise ValueError("price_panel_daily.csv 缺少日期欄位")
    if "symbol" not in df.columns or "close" not in df.columns:
        raise ValueError("price_panel_daily.csv 需要 symbol / close 欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    for col in ["open", "high", "low", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    df = df.dropna(subset=["trade_date", "symbol", "close"])
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    g = df.groupby("symbol")
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["prev_close"] = g["close"].shift(1)
    df["gap_ret"] = df["close"] / df["prev_close"] - 1.0
    df["turnover"] = df["close"] * df["volume"]
    return df


def load_macro():
    path = Path(MACRO_FILE)
    if not path.exists():
        raise FileNotFoundError(f"缺少 {MACRO_FILE}")
    macro = pd.read_csv(path)
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    if "trade_date" in macro.columns:
        macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    elif "date" in macro.columns:
        macro["trade_date"] = pd.to_datetime(macro["date"])
    else:
        raise ValueError("macro_signal.csv 缺少 trade_date / date 欄位")

    if "macro_score" not in macro.columns:
        raise ValueError("macro_signal.csv 缺少 macro_score 欄位")

    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    return macro[["trade_date", "macro_score"]].drop_duplicates(subset=["trade_date"]).sort_values("trade_date")


def build_market_regime(df):
    mkt = df.groupby("trade_date")["close"].mean().rename("market_close").reset_index().sort_values("trade_date")
    mkt["market_ma40"] = mkt["market_close"].rolling(40, min_periods=40).mean()
    mkt["market_ma120"] = mkt["market_close"].rolling(120, min_periods=120).mean()

    cond_bear = mkt["market_close"] < mkt["market_ma120"]
    cond_weak = mkt["market_close"] < mkt["market_ma40"]
    mkt["market_regime"] = np.where(cond_bear, -1, np.where(cond_weak, 0, 1))
    return mkt[["trade_date", "market_close", "market_ma40", "market_ma120", "market_regime"]]


def merge_and_filter(price, macro, start, end):
    regime = build_market_regime(price)
    df = price.merge(macro, on="trade_date", how="left")
    df = df.merge(regime, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    df["market_regime"] = df["market_regime"].ffill().fillna(0)

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    filtered = df[(df["trade_date"] >= start_ts) & (df["trade_date"] <= end_ts)].copy()

    meta = {
        "requested_start": str(start_ts.date()),
        "requested_end": str(end_ts.date()),
        "available_start": str(df["trade_date"].min().date()) if len(df) else None,
        "available_end": str(df["trade_date"].max().date()) if len(df) else None,
        "filtered_rows": int(len(filtered)),
        "filtered_symbols": int(filtered["symbol"].nunique()) if len(filtered) else 0,
    }
    if filtered.empty:
        raise ValueError("篩選後資料為空")
    return filtered, meta


def get_base_exposure(macro_score, market_regime):
    if market_regime == -1:
        regime_cap = 0.45
    elif market_regime == 0:
        regime_cap = 0.70
    else:
        regime_cap = MAX_GROSS_EXPOSURE

    if macro_score > 0.5:
        base = 0.95
    elif macro_score > 0:
        base = 0.80
    elif macro_score > -0.6:
        base = 0.55
    else:
        base = 0.30

    out = min(base, regime_cap)
    return max(out, MIN_ACTIVE_EXPOSURE if regime_cap > 0 else 0.0)


def apply_nav_breaker(base_exposure, dd_now, hard_mode):
    if hard_mode:
        if dd_now >= REENTRY_DD:
            hard_mode = False
        else:
            return min(max(base_exposure, MIN_ACTIVE_EXPOSURE), NAV_HARD_EXPOSURE_CAP), True, "NAV_HARD"

    if dd_now <= NAV_HARD_DD:
        return min(max(base_exposure, MIN_ACTIVE_EXPOSURE), NAV_HARD_EXPOSURE_CAP), True, "NAV_HARD"
    elif dd_now <= NAV_MEDIUM_DD:
        return min(max(base_exposure, MIN_ACTIVE_EXPOSURE), NAV_MEDIUM_EXPOSURE_CAP), False, "NAV_MEDIUM"
    elif dd_now <= NAV_SOFT_DD:
        return min(max(base_exposure, MIN_ACTIVE_EXPOSURE), NAV_SOFT_EXPOSURE_CAP), False, "NAV_SOFT"
    else:
        return base_exposure, False, "NORMAL"


def rank_day(day_df):
    d = day_df.dropna(subset=["mom5", "mom10", "mom20"]).copy()
    d = d[d["turnover"].fillna(0) >= MIN_DAILY_TURNOVER].copy()
    # v247：拉高 alpha 強度，提升中短期趨勢權重
    d["score"] = d["mom20"] * 0.45 + d["mom10"] * 0.35 + d["mom5"] * 0.20
    return d.sort_values("score", ascending=False).reset_index(drop=True)


def ensure_state_fields(state):
    defaults = {
        "shares": 0.0,
        "avg_cost": 0.0,
        "weight_mult": 1.0,
        "stop1_hit": False,
        "add1_hit": False,
        "add2_hit": False,
    }
    for k, v in defaults.items():
        state.setdefault(k, v)


def estimate_fee_and_tax(value, side):
    if value <= 0:
        return 0.0
    if side == "BUY":
        return value * BUY_FEE_RATE
    if side == "SELL":
        return value * (SELL_FEE_RATE + SELL_TAX_RATE)
    return 0.0


def current_nav_and_gross(holdings, cash, day_map):
    nav = float(cash)
    gross = 0.0
    count = 0
    for sym, pos in holdings.items():
        row = day_map.get(sym)
        if row is None:
            continue
        px = float(row["close"])
        if not np.isfinite(px) or px <= 0:
            continue
        value = float(pos.get("shares", 0.0)) * px
        if value >= MIN_POSITION_VALUE:
            nav += value
            gross += value
            count += 1
    return max(nav, 1.0), gross, count


def normalize_targets(raw_targets, exposure):
    keep = {k: max(0.0, float(v)) for k, v in raw_targets.items() if float(v) > 0}
    if not keep or exposure <= 0:
        return {}
    keep_items = sorted(keep.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    s = sum(v for _, v in keep_items)
    if s <= 0:
        out = {k: min(exposure / len(keep_items), MAX_POSITION_WEIGHT) for k, _ in keep_items}
    else:
        out = {k: min(exposure * v / s, MAX_POSITION_WEIGHT) for k, v in keep_items}
    s2 = sum(out.values())
    if s2 > exposure and s2 > 0:
        out = {k: exposure * v / s2 for k, v in out.items()}
    return out


def build_signals_for_next_day(day_df, holdings, nav_basis, dd_now, hard_mode):
    if day_df.empty:
        packet = {"target_weights": {}, "nav_basis": nav_basis, "signal_exposure": 0.0, "risk_mode": "EMPTY", "hard_mode": hard_mode}
        return packet, holdings, []

    macro_score = float(day_df["macro_score"].iloc[0])
    market_regime = int(day_df["market_regime"].iloc[0])
    base_exposure = get_base_exposure(macro_score, market_regime)
    target_exposure, hard_mode, risk_mode = apply_nav_breaker(base_exposure, dd_now, hard_mode)
    ranked = rank_day(day_df)
    day_map = {str(r["symbol"]): r for _, r in day_df.iterrows()}
    signal_rows = []
    raw_targets = {}

    for sym, pos in list(holdings.items()):
        ensure_state_fields(pos)
        row = day_map.get(sym)
        if row is None:
            continue
        close_px = float(row["close"])
        avg_cost = float(pos.get("avg_cost", 0.0))
        pnl = (close_px / avg_cost - 1.0) if avg_cost > 0 else 0.0
        gap_ret = float(row["gap_ret"]) if pd.notna(row["gap_ret"]) else np.nan

        if np.isfinite(gap_ret) and gap_ret <= STOP_LOSS_2:
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "GAP_STOP_FULL", pnl, target_exposure, risk_mode))
            continue

        if pnl <= STOP_LOSS_2:
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "STOP_FULL", pnl, target_exposure, risk_mode))
            continue

        if pnl <= STOP_LOSS_1 and (not pos["stop1_hit"]):
            pos["weight_mult"] *= 0.6
            pos["stop1_hit"] = True
            signal_rows.append((sym, "STOP_PART", pnl, target_exposure, risk_mode))

        # v247：更敢賺，soft/medium 也允許有限度加碼
        if risk_mode in ["NORMAL", "NAV_SOFT", "NAV_MEDIUM"] and market_regime >= 0:
            if pnl >= ADD_LV1 and (not pos["add1_hit"]):
                pos["weight_mult"] *= 1.05
                pos["add1_hit"] = True
                signal_rows.append((sym, "ADD_LV1", pnl, target_exposure, risk_mode))
            if pnl >= ADD_LV2 and (not pos["add2_hit"]):
                pos["weight_mult"] *= 1.08
                pos["add2_hit"] = True
                signal_rows.append((sym, "ADD_LV2", pnl, target_exposure, risk_mode))

        raw_targets[sym] = float(pos.get("weight_mult", 1.0))

    active_set = {s for s, v in raw_targets.items() if v > 0}
    allow_new_positions = (market_regime >= 0 and risk_mode != "NAV_HARD")
    if allow_new_positions and len(active_set) < TOP_N:
        for _, r in ranked.iterrows():
            sym = str(r["symbol"])
            if sym in active_set:
                continue
            holdings.setdefault(sym, {"shares": 0.0, "avg_cost": 0.0, "weight_mult": 1.0, "stop1_hit": False, "add1_hit": False, "add2_hit": False})
            raw_targets[sym] = float(holdings[sym].get("weight_mult", 1.0))
            active_set.add(sym)
            signal_rows.append((sym, "BUY_CANDIDATE", np.nan, target_exposure, risk_mode))
            if len(active_set) >= TOP_N:
                break

    packet = {
        "target_weights": normalize_targets(raw_targets, target_exposure),
        "nav_basis": float(max(nav_basis, 1.0)),
        "signal_exposure": float(target_exposure),
        "risk_mode": risk_mode,
        "hard_mode": hard_mode,
    }
    return packet, holdings, signal_rows


def apply_signal_packet(packet, holdings, cash, day_map, trade_date, trade_rows):
    if packet is None:
        return holdings, cash

    target_weights = dict(packet.get("target_weights", {}))
    nav_basis = float(packet.get("nav_basis", 0.0))
    risk_mode = packet.get("risk_mode", "NORMAL")
    if nav_basis <= 0:
        return holdings, cash

    symbols = sorted(set(list(holdings.keys()) + list(target_weights.keys())))
    sell_list, buy_list = [], []

    for sym in symbols:
        row = day_map.get(sym)
        if row is None:
            continue
        close_px = float(row["close"])
        turnover = float(row["turnover"]) if pd.notna(row["turnover"]) else np.nan
        if not np.isfinite(close_px) or close_px <= 0:
            continue

        ensure_state_fields(holdings.setdefault(sym, {}))
        current_shares = float(holdings[sym].get("shares", 0.0))
        current_value = current_shares * close_px
        target_value = max(0.0, float(target_weights.get(sym, 0.0)) * nav_basis)
        delta_value = target_value - current_value
        can_buy = np.isfinite(turnover) and turnover >= MIN_DAILY_TURNOVER

        volume = row.get("volume", np.nan)
        if np.isfinite(volume) and volume > 0:
            max_trade = float(volume) * close_px * MAX_DAILY_VOLUME_PARTICIPATION
            delta_value = max(-max_trade, min(max_trade, delta_value))

        if delta_value < -MIN_TRADE_VALUE:
            sell_list.append((sym, close_px, abs(delta_value)))
        elif delta_value > MIN_TRADE_VALUE and can_buy:
            buy_list.append((sym, close_px, delta_value))

    for sym, close_px, desired_sell in sell_list:
        current_shares = float(holdings[sym].get("shares", 0.0))
        exec_px = close_px * (1.0 - SLIPPAGE_SELL)
        sell_shares = min(current_shares, desired_sell / max(exec_px, 1e-12))
        trade_value = sell_shares * exec_px
        if sell_shares <= 0 or trade_value < MIN_TRADE_VALUE:
            continue
        fee_tax = estimate_fee_and_tax(trade_value, "SELL")
        proceeds = trade_value - fee_tax
        holdings[sym]["shares"] = max(0.0, current_shares - sell_shares)
        cash += proceeds
        trade_rows.append([trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax, nav_basis, risk_mode])

    def effective_count():
        c = 0
        for sym, pos in holdings.items():
            row = day_map.get(sym)
            if row is None:
                continue
            px = float(row["close"])
            val = float(pos.get("shares", 0.0)) * px
            if val >= MIN_POSITION_VALUE:
                c += 1
        return c

    for sym, close_px, desired_buy in buy_list:
        current_shares = float(holdings[sym].get("shares", 0.0))
        current_val = current_shares * close_px
        is_new = current_val < MIN_POSITION_VALUE
        if is_new and effective_count() >= TOP_N:
            continue

        exec_px = close_px * (1.0 + SLIPPAGE_BUY)
        raw_shares = desired_buy / max(exec_px, 1e-12)
        if raw_shares <= 0:
            continue
        trade_value = raw_shares * exec_px
        fee_tax = estimate_fee_and_tax(trade_value, "BUY")
        total = trade_value + fee_tax

        if total > cash and cash > 0:
            raw_shares = cash / (exec_px * (1.0 + BUY_FEE_RATE))
            trade_value = raw_shares * exec_px
            fee_tax = estimate_fee_and_tax(trade_value, "BUY")
            total = trade_value + fee_tax

        if raw_shares <= 0 or trade_value < MIN_TRADE_VALUE or total > cash:
            continue

        prev_shares = float(holdings[sym].get("shares", 0.0))
        prev_cost = float(holdings[sym].get("avg_cost", 0.0))
        new_shares = prev_shares + raw_shares
        new_avg = ((prev_shares * prev_cost) + (raw_shares * exec_px)) / max(new_shares, 1e-12)
        holdings[sym]["shares"] = new_shares
        holdings[sym]["avg_cost"] = new_avg
        cash -= total
        trade_rows.append([trade_date, "BUY", sym, close_px, exec_px, raw_shares, trade_value, fee_tax, nav_basis, risk_mode])

    to_drop = []
    for sym, pos in holdings.items():
        row = day_map.get(sym)
        if row is None:
            continue
        px = float(row["close"])
        if float(pos.get("shares", 0.0)) * px < MIN_POSITION_VALUE:
            to_drop.append(sym)
    for sym in to_drop:
        holdings.pop(sym, None)

    return holdings, cash


def simulate(df):
    all_dates = sorted(df["trade_date"].dropna().unique())
    holdings = {}
    cash = INITIAL_CAPITAL
    peak_nav = INITIAL_CAPITAL
    hard_mode = False
    nav_rows, trade_rows, signal_rows = [], [], []
    pending = None

    for date in all_dates:
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue
        day_map = {str(r["symbol"]): r for _, r in day.iterrows()}

        holdings, cash = apply_signal_packet(pending, holdings, cash, day_map, date, trade_rows)

        nav, gross, count = current_nav_and_gross(holdings, cash, day_map)
        peak_nav = max(peak_nav, nav)
        dd = nav / peak_nav - 1.0
        macro_score = float(day["macro_score"].iloc[0]) if "macro_score" in day.columns else 0.0
        exposure = gross / nav if nav > 0 else 0.0
        nav_rows.append([date, nav, cash, macro_score, exposure, dd, count, "HARD" if hard_mode else "NORMAL"])

        pending, holdings, sig_rows = build_signals_for_next_day(day, holdings, nav, dd, hard_mode)
        hard_mode = bool(pending.get("hard_mode", False))
        for sym, action, pnl, sig_exposure, risk_mode in sig_rows:
            signal_rows.append([date, action, sym, pnl, sig_exposure, float(pending.get("nav_basis", nav)), risk_mode])

    nav_df = pd.DataFrame(nav_rows, columns=["date", "nav", "cash", "macro", "exposure", "dd", "count", "risk_mode"])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    trade_df = pd.DataFrame(trade_rows, columns=["date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "signal_nav_basis", "risk_mode"])
    signal_df = pd.DataFrame(signal_rows, columns=["date", "signal", "symbol", "pnl_vs_cost", "target_exposure", "signal_nav_basis", "risk_mode"])
    return nav_df, trade_df, signal_df


def build_summary(nav, trades, meta):
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
        "requested_start": meta["requested_start"],
        "requested_end": meta["requested_end"],
        "available_start": meta["available_start"],
        "available_end": meta["available_end"],
        "filtered_rows": meta["filtered_rows"],
        "filtered_symbols": meta["filtered_symbols"],
    }])


def main():
    args = parse_args()
    price = load_price_panel()
    macro = load_macro()
    df, meta = merge_and_filter(price, macro, args.start, args.end)
    nav, trades, signals = simulate(df)
    if (nav["nav"] <= 0).any():
        raise ValueError("偵測到 nav <= 0，請檢查 engine")
    if (nav["count"] > TOP_N).any():
        raise ValueError(f"偵測到 count > TOP_N({TOP_N})，請檢查 engine")
    summary = build_summary(nav, trades, meta)
    label = args.label
    nav.to_csv(f"{label}_nav.csv", index=False)
    trades.to_csv(f"{label}_trades.csv", index=False)
    signals.to_csv(f"{label}_signals.csv", index=False)
    summary.to_csv(f"{label}_summary.csv", index=False)
    with open(f"{label}_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
