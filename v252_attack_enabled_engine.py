# v252_attack_enabled_engine.txt
# 覆蓋後改名：v252_attack_enabled_engine.py
# 目標：
# 1) 在 v251 風控骨架上打開攻擊
# 2) 減少過度 RISK_OFF
# 3) 弱化 DEF，主力回到 AGG
# 4) 放寬 alpha 進場條件，提高曝險與報酬
# 5) 嚴格 T+1，不偷看

import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd

INITIAL_CAPITAL = 100000.0

PRICE_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"

MIN_TRADE_VALUE = 1000.0
MIN_POSITION_VALUE = 1000.0
MIN_DAILY_TURNOVER = 1_500_000.0
MAX_DAILY_VOLUME_PARTICIPATION = 0.20

BUY_FEE_RATE = 0.001425
SELL_FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003
SLIPPAGE_BUY = 0.0010
SLIPPAGE_SELL = 0.0010

# AGG：主戰場
AGG_TOP_N = 6
AGG_MAX_EXPOSURE = 0.95
AGG_MIN_EXPOSURE = 0.50
AGG_MAX_POSITION_WEIGHT = 0.20

# DEF：幾乎關閉，只保留極少量參與
DEF_TOP_N = 1
DEF_MAX_EXPOSURE = 0.08
DEF_MIN_EXPOSURE = 0.00
DEF_MAX_POSITION_WEIGHT = 0.08

AGG_STOP1 = -0.05
AGG_STOP2 = -0.08
DEF_STOP1 = -0.04
DEF_STOP2 = -0.06

AGG_ADD1 = 0.04
AGG_ADD2 = 0.08
DEF_ADD1 = 0.08
DEF_ADD2 = 0.15

AGG_ADD_MULT1 = 1.05
AGG_ADD_MULT2 = 1.08
DEF_ADD_MULT1 = 1.02
DEF_ADD_MULT2 = 1.03

NAV_SOFT_DD = -0.08
NAV_MEDIUM_DD = -0.14
NAV_HARD_DD = -0.22

NAV_SOFT_CAP = 0.70
NAV_MEDIUM_CAP = 0.45
NAV_HARD_CAP = 0.00
REENTRY_DD = -0.06


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

    date_col = None
    for c in ["trade_date", "date", "datetime", "signal_date"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError("price_panel_daily.csv 缺少 trade_date / date 欄位")

    symbol_col = None
    for c in ["stock_id", "symbol", "stockid", "ticker", "code"]:
        if c in df.columns:
            symbol_col = c
            break
    if symbol_col is None:
        raise ValueError("price_panel_daily.csv 缺少 stock_id / symbol / ticker / code 欄位")

    if "close" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 close 欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df[symbol_col].astype(str).str.strip()
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
    df["ret"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["prev_close"] = g["close"].shift(1)
    df["gap_ret"] = df["close"] / df["prev_close"] - 1.0
    df["turnover"] = df["close"] * df["volume"]
    df["vol20"] = g["ret"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
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


def build_market_state(price):
    mkt = price.groupby("trade_date")["close"].mean().rename("market_close").reset_index().sort_values("trade_date")
    mkt["market_ret_5d"] = mkt["market_close"].pct_change(5)
    mkt["ma20"] = mkt["market_close"].rolling(20, min_periods=20).mean()
    mkt["ma60"] = mkt["market_close"].rolling(60, min_periods=60).mean()

    # v252：放寬 risk_off，降低過度恐懼
    risk_off = (mkt["market_close"] < mkt["ma60"] * 0.95) | (mkt["market_ret_5d"] < -0.08)
    defensive = (mkt["market_close"] < mkt["ma20"]) | (mkt["market_ret_5d"] < -0.01)
    mkt["market_state"] = np.where(risk_off, "RISK_OFF", np.where(defensive, "DEF", "AGG"))
    return mkt[["trade_date", "market_close", "market_ret_5d", "ma20", "ma60", "market_state"]]


def merge_and_filter(price, macro, start, end):
    market = build_market_state(price)
    df = price.merge(macro, on="trade_date", how="left")
    df = df.merge(market, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    df["market_state"] = df["market_state"].ffill().fillna("DEF")

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    out = df[(df["trade_date"] >= start_ts) & (df["trade_date"] <= end_ts)].copy()
    if out.empty:
        raise ValueError("篩選後資料為空")

    meta = {
        "requested_start": str(start_ts.date()),
        "requested_end": str(end_ts.date()),
        "available_start": str(df["trade_date"].min().date()),
        "available_end": str(df["trade_date"].max().date()),
        "filtered_rows": int(len(out)),
        "filtered_symbols": int(out["symbol"].nunique()),
    }
    return out, meta


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


def apply_nav_breaker(base_exposure, dd_now, hard_mode):
    if hard_mode:
        if dd_now >= REENTRY_DD:
            hard_mode = False
        else:
            return min(base_exposure, NAV_HARD_CAP), True, "NAV_HARD"
    if dd_now <= NAV_HARD_DD:
        return min(base_exposure, NAV_HARD_CAP), True, "NAV_HARD"
    elif dd_now <= NAV_MEDIUM_DD:
        return min(base_exposure, NAV_MEDIUM_CAP), False, "NAV_MEDIUM"
    elif dd_now <= NAV_SOFT_DD:
        return min(base_exposure, NAV_SOFT_CAP), False, "NAV_SOFT"
    else:
        return base_exposure, False, "NORMAL"


def regime_params(market_state, macro_score):
    if market_state == "AGG":
        base = 0.90 if macro_score > 0.5 else (0.80 if macro_score > 0 else 0.65)
        return {
            "mode": "AGG",
            "top_n": AGG_TOP_N,
            "max_exposure": AGG_MAX_EXPOSURE,
            "min_exposure": AGG_MIN_EXPOSURE,
            "max_pos_w": AGG_MAX_POSITION_WEIGHT,
            "stop1": AGG_STOP1,
            "stop2": AGG_STOP2,
            "add1": AGG_ADD1,
            "add2": AGG_ADD2,
            "addm1": AGG_ADD_MULT1,
            "addm2": AGG_ADD_MULT2,
            "base_exposure": min(max(base, AGG_MIN_EXPOSURE), AGG_MAX_EXPOSURE),
        }
    elif market_state == "DEF":
        base = 0.06 if macro_score > 0 else 0.03
        return {
            "mode": "DEF",
            "top_n": DEF_TOP_N,
            "max_exposure": DEF_MAX_EXPOSURE,
            "min_exposure": DEF_MIN_EXPOSURE,
            "max_pos_w": DEF_MAX_POSITION_WEIGHT,
            "stop1": DEF_STOP1,
            "stop2": DEF_STOP2,
            "add1": DEF_ADD1,
            "add2": DEF_ADD2,
            "addm1": DEF_ADD_MULT1,
            "addm2": DEF_ADD_MULT2,
            "base_exposure": min(max(base, DEF_MIN_EXPOSURE), DEF_MAX_EXPOSURE),
        }
    else:
        return {
            "mode": "RISK_OFF",
            "top_n": 0,
            "max_exposure": 0.0,
            "min_exposure": 0.0,
            "max_pos_w": 0.0,
            "stop1": DEF_STOP1,
            "stop2": DEF_STOP2,
            "add1": DEF_ADD1,
            "add2": DEF_ADD2,
            "addm1": 1.0,
            "addm2": 1.0,
            "base_exposure": 0.0,
        }


def rank_day(day_df, mode):
    d = day_df.dropna(subset=["mom10", "mom20", "vol20"]).copy()
    d = d[d["turnover"].fillna(0) >= MIN_DAILY_TURNOVER].copy()
    d = d[d["close"] >= 15].copy()

    if mode == "AGG":
        # 放寬 alpha，讓 AGG 能進場
        d = d[(d["mom10"] > 0) & (d["mom20"] > -0.02)].copy()
        d["score"] = (d["mom20"] / (d["vol20"] + 1e-9)) * 0.45 + d["mom10"] * 0.35 + d["mom5"] * 0.20
    elif mode == "DEF":
        d = d[(d["mom20"] > 0) & (d["mom10"] > 0)].copy()
        d["score"] = (d["mom20"] / (d["vol20"] + 1e-9)) * 0.85 + d["mom10"] * 0.15
    else:
        return d.iloc[0:0].copy()

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


def normalize_targets(raw_targets, exposure, top_n, max_pos_w):
    keep = {k: max(0.0, float(v)) for k, v in raw_targets.items() if float(v) > 0}
    if not keep or exposure <= 0 or top_n <= 0:
        return {}

    keep_items = sorted(keep.items(), key=lambda x: x[1], reverse=True)[:top_n]
    s = sum(v for _, v in keep_items)
    if s <= 0:
        out = {k: min(exposure / len(keep_items), max_pos_w) for k, _ in keep_items}
    else:
        out = {k: min(exposure * v / s, max_pos_w) for k, v in keep_items}

    s2 = sum(out.values())
    if s2 > exposure and s2 > 0:
        out = {k: exposure * v / s2 for k, v in out.items()}
    return out


def build_signals_for_next_day(day_df, holdings, nav_basis, dd_now, hard_mode):
    if day_df.empty:
        return {"target_weights": {}, "nav_basis": nav_basis, "signal_exposure": 0.0, "risk_mode": "EMPTY", "hard_mode": hard_mode, "strategy_mode": "EMPTY"}, holdings, []

    macro_score = float(day_df["macro_score"].iloc[0])
    market_state = str(day_df["market_state"].iloc[0])
    cfg = regime_params(market_state, macro_score)

    target_exposure, hard_mode, risk_mode = apply_nav_breaker(cfg["base_exposure"], dd_now, hard_mode)
    ranked = rank_day(day_df, cfg["mode"])
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

        if market_state == "RISK_OFF":
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "RISK_OFF_EXIT", pnl, target_exposure, risk_mode, cfg["mode"]))
            continue

        if np.isfinite(gap_ret) and gap_ret <= cfg["stop2"]:
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "GAP_STOP_FULL", pnl, target_exposure, risk_mode, cfg["mode"]))
            continue

        if pnl <= cfg["stop2"]:
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "STOP_FULL", pnl, target_exposure, risk_mode, cfg["mode"]))
            continue

        if pnl <= cfg["stop1"] and (not pos["stop1_hit"]):
            pos["weight_mult"] *= 0.5
            pos["stop1_hit"] = True
            signal_rows.append((sym, "STOP_PART", pnl, target_exposure, risk_mode, cfg["mode"]))

        if cfg["mode"] == "AGG" and risk_mode in ["NORMAL", "NAV_SOFT", "NAV_MEDIUM"]:
            if pnl >= cfg["add1"] and (not pos["add1_hit"]):
                pos["weight_mult"] *= cfg["addm1"]
                pos["add1_hit"] = True
                signal_rows.append((sym, "ADD_LV1", pnl, target_exposure, risk_mode, cfg["mode"]))
            if pnl >= cfg["add2"] and (not pos["add2_hit"]):
                pos["weight_mult"] *= cfg["addm2"]
                pos["add2_hit"] = True
                signal_rows.append((sym, "ADD_LV2", pnl, target_exposure, risk_mode, cfg["mode"]))

        raw_targets[sym] = float(pos.get("weight_mult", 1.0))

    active_set = {s for s, v in raw_targets.items() if v > 0}
    allow_new = (cfg["mode"] != "RISK_OFF") and (risk_mode != "NAV_HARD")
    if allow_new and len(active_set) < cfg["top_n"]:
        for _, r in ranked.iterrows():
            sym = str(r["symbol"])
            if sym in active_set:
                continue
            holdings.setdefault(sym, {"shares": 0.0, "avg_cost": 0.0, "weight_mult": 1.0, "stop1_hit": False, "add1_hit": False, "add2_hit": False})
            raw_targets[sym] = float(holdings[sym].get("weight_mult", 1.0))
            active_set.add(sym)
            signal_rows.append((sym, "BUY_CANDIDATE", np.nan, target_exposure, risk_mode, cfg["mode"]))
            if len(active_set) >= cfg["top_n"]:
                break

    packet = {
        "target_weights": normalize_targets(raw_targets, target_exposure, cfg["top_n"], cfg["max_pos_w"]),
        "nav_basis": float(max(nav_basis, 1.0)),
        "signal_exposure": float(target_exposure),
        "risk_mode": risk_mode,
        "hard_mode": hard_mode,
        "strategy_mode": cfg["mode"],
        "top_n": cfg["top_n"],
    }
    return packet, holdings, signal_rows


def apply_signal_packet(packet, holdings, cash, day_map, trade_date, trade_rows):
    if packet is None:
        return holdings, cash

    target_weights = dict(packet.get("target_weights", {}))
    nav_basis = float(packet.get("nav_basis", 0.0))
    risk_mode = packet.get("risk_mode", "NORMAL")
    strategy_mode = packet.get("strategy_mode", "NA")
    top_n = int(packet.get("top_n", 6))
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
        trade_rows.append([trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax, nav_basis, risk_mode, strategy_mode])

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
        if is_new and effective_count() >= top_n:
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
        trade_rows.append([trade_date, "BUY", sym, close_px, exec_px, raw_shares, trade_value, fee_tax, nav_basis, risk_mode, strategy_mode])

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
        strategy_mode = pending.get("strategy_mode", "NA") if isinstance(pending, dict) else "NA"
        nav_rows.append([date, nav, cash, macro_score, exposure, dd, count, "HARD" if hard_mode else "NORMAL", strategy_mode])

        pending, holdings, sig_rows = build_signals_for_next_day(day, holdings, nav, dd, hard_mode)
        hard_mode = bool(pending.get("hard_mode", False))
        for sym, action, pnl, sig_exposure, risk_mode, strat_mode in sig_rows:
            signal_rows.append([date, action, sym, pnl, sig_exposure, float(pending.get("nav_basis", nav)), risk_mode, strat_mode])

    nav_df = pd.DataFrame(nav_rows, columns=["date", "nav", "cash", "macro", "exposure", "dd", "count", "risk_mode", "strategy_mode"])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    trade_df = pd.DataFrame(trade_rows, columns=["date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "signal_nav_basis", "risk_mode", "strategy_mode"])
    signal_df = pd.DataFrame(signal_rows, columns=["date", "signal", "symbol", "pnl_vs_cost", "target_exposure", "signal_nav_basis", "risk_mode", "strategy_mode"])
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
        "agg_day_ratio": (nav["strategy_mode"] == "AGG").mean(),
        "def_day_ratio": (nav["strategy_mode"] == "DEF").mean(),
        "risk_off_day_ratio": (nav["strategy_mode"] == "RISK_OFF").mean(),
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
