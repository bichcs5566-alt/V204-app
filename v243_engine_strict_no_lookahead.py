# v243_engine_strict_no_lookahead.py
# 嚴格無偷看版回測引擎
# 可直接覆蓋使用

import argparse
import json
from pathlib import Path
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd

INITIAL_CAPITAL = 100000.0
TOP_N = 5

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

PRICE_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--label", required=True)
    return p.parse_args()


def load_price_panel() -> pd.DataFrame:
    path = Path(PRICE_FILE)
    if not path.exists():
        raise FileNotFoundError(f"缺少 {PRICE_FILE}，請先執行 merge_chunked_price_panel.py")

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


def load_macro() -> pd.DataFrame:
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
    macro = macro[["trade_date", "macro_score"]].drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
    return macro


def merge_and_filter(price: pd.DataFrame, macro: pd.DataFrame, start: str, end: str):
    df = price.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)

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
        raise ValueError(
            f"篩選後資料為空 | requested={meta['requested_start']}~{meta['requested_end']} | "
            f"available={meta['available_start']}~{meta['available_end']} | "
            f"rows={meta['filtered_rows']} | symbols={meta['filtered_symbols']}"
        )
    return filtered, meta


def get_exposure(macro_score: float) -> float:
    if macro_score > 0.5:
        return 1.0
    elif macro_score > 0:
        return 1.0
    elif macro_score > -0.6:
        return 0.8
    else:
        return 0.35


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


def estimate_fee_and_tax(trade_value: float, side: str) -> float:
    if trade_value <= 0:
        return 0.0
    if side == "BUY":
        return trade_value * BUY_FEE_RATE
    if side == "SELL":
        return trade_value * (SELL_FEE_RATE + SELL_TAX_RATE)
    return 0.0


def current_nav_and_gross(holdings: dict, cash: float, day_map: dict) -> Tuple[float, float, int]:
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
    nav = max(nav, 1.0)
    return nav, gross, count


def normalize_targets(raw_targets: Dict[str, float], exposure: float) -> Dict[str, float]:
    keep = {k: max(0.0, float(v)) for k, v in raw_targets.items() if float(v) > 0}
    if not keep:
        return {}
    keep_items = sorted(keep.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    s = sum(v for _, v in keep_items)
    if s <= 0:
        equal_w = exposure / len(keep_items)
        return {k: equal_w for k, _ in keep_items}
    return {k: exposure * v / s for k, v in keep_items}


def build_signals_for_next_day(day_df: pd.DataFrame, holdings: dict, nav_basis: float):
    if day_df.empty:
        return {"target_weights": {}, "nav_basis": nav_basis, "signal_exposure": 0.0}, holdings, []

    macro_score = float(day_df["macro_score"].iloc[0])
    exposure = get_exposure(macro_score)
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

        if pnl <= STOP_LOSS_2:
            raw_targets[sym] = 0.0
            signal_rows.append((sym, "STOP_FULL", pnl, exposure))
            continue

        if (pnl <= STOP_LOSS_1) and (not pos["stop1_hit"]):
            pos["weight_mult"] *= 0.5
            pos["stop1_hit"] = True
            signal_rows.append((sym, "STOP_PART", pnl, exposure))

        if (pnl >= ADD_LV1) and (not pos["add1_hit"]):
            pos["weight_mult"] *= 1.05
            pos["add1_hit"] = True
            signal_rows.append((sym, "ADD_LV1", pnl, exposure))

        if (pnl >= ADD_LV2) and (not pos["add2_hit"]):
            pos["weight_mult"] *= 1.10
            pos["add2_hit"] = True
            signal_rows.append((sym, "ADD_LV2", pnl, exposure))

        raw_targets[sym] = float(pos.get("weight_mult", 1.0))

    active_set = {s for s, v in raw_targets.items() if v > 0}
    if len(active_set) < TOP_N:
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
            })
            raw_targets[sym] = float(holdings[sym].get("weight_mult", 1.0))
            active_set.add(sym)
            signal_rows.append((sym, "BUY_CANDIDATE", np.nan, exposure))
            if len(active_set) >= TOP_N:
                break

    target_weights = normalize_targets(raw_targets, exposure)
    signal_packet = {
        "target_weights": target_weights,
        "nav_basis": float(max(nav_basis, 1.0)),
        "signal_exposure": float(exposure),
    }
    return signal_packet, holdings, signal_rows


def apply_signal_packet(signal_packet: dict, holdings: dict, cash: float, day_map: dict, trade_date, trade_rows: list):
    if signal_packet is None:
        return holdings, cash

    target_weights = dict(signal_packet.get("target_weights", {}))
    nav_basis = float(signal_packet.get("nav_basis", 0.0))
    if nav_basis <= 0:
        return holdings, cash

    symbols = sorted(set(list(holdings.keys()) + list(target_weights.keys())))
    sell_list: List[Tuple[str, float, float]] = []
    buy_list: List[Tuple[str, float, float]] = []

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
        target_weight = float(target_weights.get(sym, 0.0))
        target_value = max(0.0, target_weight * nav_basis)
        delta_value = target_value - current_value

        volume = row.get("volume", np.nan)
        if np.isfinite(volume) and volume > 0:
            max_trade_value = float(volume) * close_px * MAX_DAILY_VOLUME_PARTICIPATION
            delta_value = max(-max_trade_value, min(max_trade_value, delta_value))

        if delta_value < -MIN_TRADE_VALUE:
            sell_list.append((sym, close_px, abs(delta_value)))
        elif delta_value > MIN_TRADE_VALUE:
            buy_list.append((sym, close_px, delta_value))

    for sym, close_px, desired_sell_value in sell_list:
        current_shares = float(holdings[sym].get("shares", 0.0))
        exec_px = close_px * (1.0 - SLIPPAGE_SELL)
        sell_shares = min(current_shares, desired_sell_value / max(exec_px, 1e-12))
        trade_value = sell_shares * exec_px
        if sell_shares <= 0 or trade_value < MIN_TRADE_VALUE:
            continue
        fee_tax = estimate_fee_and_tax(trade_value, "SELL")
        proceeds = trade_value - fee_tax
        holdings[sym]["shares"] = max(0.0, current_shares - sell_shares)
        cash += proceeds
        trade_rows.append([trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax, nav_basis])

    def effective_count() -> int:
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

    for sym, close_px, desired_buy_value in buy_list:
        current_shares = float(holdings[sym].get("shares", 0.0))
        current_val_now = current_shares * close_px
        is_new_position = current_val_now < MIN_POSITION_VALUE
        if is_new_position and effective_count() >= TOP_N:
            continue

        exec_px = close_px * (1.0 + SLIPPAGE_BUY)
        raw_shares = desired_buy_value / max(exec_px, 1e-12)
        if raw_shares <= 0:
            continue

        trade_value = raw_shares * exec_px
        fee_tax = estimate_fee_and_tax(trade_value, "BUY")
        total_cash_needed = trade_value + fee_tax

        if total_cash_needed > cash and cash > 0:
            raw_shares = cash / (exec_px * (1.0 + BUY_FEE_RATE))
            trade_value = raw_shares * exec_px
            fee_tax = estimate_fee_and_tax(trade_value, "BUY")
            total_cash_needed = trade_value + fee_tax

        if raw_shares <= 0 or trade_value < MIN_TRADE_VALUE or total_cash_needed > cash:
            continue

        prev_shares = float(holdings[sym].get("shares", 0.0))
        prev_cost = float(holdings[sym].get("avg_cost", 0.0))
        new_shares = prev_shares + raw_shares
        new_avg_cost = ((prev_shares * prev_cost) + (raw_shares * exec_px)) / max(new_shares, 1e-12)

        holdings[sym]["shares"] = new_shares
        holdings[sym]["avg_cost"] = new_avg_cost
        cash -= total_cash_needed
        trade_rows.append([trade_date, "BUY", sym, close_px, exec_px, raw_shares, trade_value, fee_tax, nav_basis])

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


def simulate(df: pd.DataFrame):
    all_dates = sorted(df["trade_date"].dropna().unique())
    holdings: Dict[str, dict] = {}
    cash = INITIAL_CAPITAL
    peak_nav = INITIAL_CAPITAL

    nav_rows = []
    trade_rows = []
    signal_rows = []

    pending_signal_packet = None

    for date in all_dates:
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue
        day_map = {str(r["symbol"]): r for _, r in day.iterrows()}

        holdings, cash = apply_signal_packet(
            signal_packet=pending_signal_packet,
            holdings=holdings,
            cash=cash,
            day_map=day_map,
            trade_date=date,
            trade_rows=trade_rows,
        )

        nav, gross_value, count = current_nav_and_gross(holdings, cash, day_map)
        peak_nav = max(peak_nav, nav)
        dd = nav / peak_nav - 1.0
        macro_score = float(day["macro_score"].iloc[0]) if "macro_score" in day.columns else 0.0
        exposure = gross_value / nav if nav > 0 else 0.0

        nav_rows.append([date, nav, cash, macro_score, exposure, dd, count])

        pending_signal_packet, holdings, sig_rows = build_signals_for_next_day(
            day_df=day,
            holdings=holdings,
            nav_basis=nav,
        )

        for sym, action, pnl, sig_exposure in sig_rows:
            signal_rows.append([date, action, sym, pnl, sig_exposure, float(pending_signal_packet.get("nav_basis", nav))])

    nav_df = pd.DataFrame(nav_rows, columns=["date", "nav", "cash", "macro", "exposure", "dd", "count"])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)

    trade_df = pd.DataFrame(trade_rows, columns=[
        "date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "signal_nav_basis"
    ])
    signal_df = pd.DataFrame(signal_rows, columns=[
        "date", "signal", "symbol", "pnl_vs_cost", "target_exposure", "signal_nav_basis"
    ])

    return nav_df, trade_df, signal_df


def build_summary(nav: pd.DataFrame, trades: pd.DataFrame, meta: dict):
    total_return = nav["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0
    mdd = nav["dd"].min()
    avg_exposure = nav["exposure"].mean()
    avg_count = nav["count"].mean()
    trade_count = len(trades)

    daily_ret = nav["ret"].fillna(0.0)
    vol = daily_ret.std(ddof=0)
    sharpe = (daily_ret.mean() / vol * np.sqrt(252)) if vol > 0 else np.nan
    win_days = (daily_ret > 0).mean() if len(daily_ret) > 0 else np.nan

    out = {
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
    }
    return pd.DataFrame([out])


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
