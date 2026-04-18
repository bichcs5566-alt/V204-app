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
MIN_EXPOSURE_FLOOR = 0.35
MIN_HISTORY_DAYS = 60


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--price", default="price_panel_daily.csv")
    p.add_argument("--macro", default="macro_signal.csv")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--output-prefix", default="v243_release")
    p.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    p.add_argument("--max-volume-participation", type=float, default=MAX_DAILY_VOLUME_PARTICIPATION)
    p.add_argument("--buy-fee-rate", type=float, default=BUY_FEE_RATE)
    p.add_argument("--sell-fee-rate", type=float, default=SELL_FEE_RATE)
    p.add_argument("--sell-tax-rate", type=float, default=SELL_TAX_RATE)
    p.add_argument("--buy-slippage", type=float, default=SLIPPAGE_BUY)
    p.add_argument("--sell-slippage", type=float, default=SLIPPAGE_SELL)
    return p.parse_args()


def ensure_cols(df: pd.DataFrame, required: list[str], name: str):
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"{name} ç¼ºå°æ¬ä½: {miss}")


def load_price_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).lower().strip() for c in df.columns]
    date_col = next((c for c in ["trade_date", "date", "datetime", "signal_date"] if c in df.columns), None)
    if date_col is None:
        raise ValueError("price file ç¼ºå°æ¥ææ¬ä½ï¼trade_date/date/datetime/signal_dateï¼")
    ensure_cols(df, ["symbol", "close"], "price file")

    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    else:
        df["volume"] = np.nan

    df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
    df = df[df["close"] > 0].sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    g = df.groupby("symbol")
    df["hist_idx"] = g.cumcount() + 1
    df["ret"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def load_macro_data(path: str) -> pd.DataFrame:
    macro = pd.read_csv(path)
    macro.columns = [str(c).lower().strip() for c in macro.columns]
    ensure_cols(macro, ["trade_date", "macro_score"], "macro file")
    macro["trade_date"] = pd.to_datetime(macro["trade_date"], errors="coerce")
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    macro = (
        macro[["trade_date", "macro_score"]]
        .dropna(subset=["trade_date"])
        .drop_duplicates(subset=["trade_date"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    return macro


def merge_and_filter(price: pd.DataFrame, macro: pd.DataFrame, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    raw_min = price["trade_date"].min()
    raw_max = price["trade_date"].max()
    raw_symbols = int(price["symbol"].nunique())
    raw_rows = int(len(price))

    df = price.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    df = df[(df["trade_date"] >= start_ts) & (df["trade_date"] <= end_ts)].copy()

    if df.empty:
        raise ValueError(
            f"ç¯©é¸å¾è³æçºç©º | requested={start}~{end} | available={raw_min.date()}~{raw_max.date()} | rows={raw_rows} | symbols={raw_symbols}"
        )

    df = df[df["hist_idx"] >= MIN_HISTORY_DAYS].copy()
    if df.empty:
        raise ValueError(
            f"åéå§è³æå­å¨ï¼ä½æ²æè¶³å¤ æ­·å²å¤©æ¸ä¾ç¹å¾µè¨ç® | requested={start}~{end} | min_history_days={MIN_HISTORY_DAYS}"
        )

    meta = {
        "requested_start": start,
        "requested_end": end,
        "available_min_date": str(raw_min.date()),
        "available_max_date": str(raw_max.date()),
        "raw_rows": raw_rows,
        "raw_symbols": raw_symbols,
        "filtered_rows": int(len(df)),
        "filtered_symbols": int(df["symbol"].nunique()),
        "trade_days": int(df["trade_date"].nunique()),
    }
    return df.sort_values(["trade_date", "symbol"]).reset_index(drop=True), meta


def get_exposure(macro_score: float) -> float:
    if macro_score > 0.5:
        return 1.0
    if macro_score > 0:
        return 1.0
    if macro_score > -0.6:
        return 0.8
    return MIN_EXPOSURE_FLOOR


def desired_symbol_count(exposure: float) -> int:
    if exposure >= 0.95:
        return TOP_N
    if exposure >= 0.70:
        return 4
    return MIN_HOLDINGS_FLOOR


def rank_day(day_df: pd.DataFrame) -> pd.DataFrame:
    d = day_df.dropna(subset=["mom5", "mom10"]).copy()
    d = d[d["hist_idx"] >= MIN_HISTORY_DAYS]
    if d.empty:
        return d
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


def calc_fee_tax(trade_value: float, side: str, cfg: dict) -> float:
    if trade_value <= 0:
        return 0.0
    if side == "BUY":
        return trade_value * cfg["buy_fee_rate"]
    if side == "SELL":
        return trade_value * (cfg["sell_fee_rate"] + cfg["sell_tax_rate"])
    return 0.0


def apply_execution_targets(pending_targets, holdings, cash, day_map, trade_date, nav_before_trade, trade_rows, cfg):
    if pending_targets is None:
        return holdings, cash

    symbols = sorted(set(list(holdings.keys()) + list(pending_targets.keys())))
    sell_bucket, buy_bucket = [], []

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
        rec = [sym, row, close_px, current_shares, delta_value]
        if delta_value < 0:
            sell_bucket.append(rec)
        else:
            buy_bucket.append(rec)

    for bucket_name, bucket in [("SELL", sell_bucket), ("BUY", buy_bucket)]:
        for sym, row, close_px, current_shares, delta_value in bucket:
            volume = row.get("volume", np.nan)
            if np.isfinite(volume) and volume > 0:
                max_trade_value = float(volume) * close_px * cfg["max_volume_participation"]
                if max_trade_value <= 0:
                    continue
                delta_value = max(-max_trade_value, min(max_trade_value, delta_value))
                if abs(delta_value) < MIN_TRADE_VALUE:
                    continue

            if bucket_name == "SELL":
                exec_px = close_px * (1.0 - cfg["sell_slippage"])
                sell_shares = min(current_shares, abs(delta_value) / max(exec_px, 1e-12))
                if sell_shares <= 0:
                    continue
                trade_value = sell_shares * exec_px
                if trade_value < MIN_TRADE_VALUE:
                    continue
                fee_tax = calc_fee_tax(trade_value, "SELL", cfg)
                cash += trade_value - fee_tax
                holdings[sym]["shares"] = current_shares - sell_shares
                if holdings[sym]["shares"] * close_px < MIN_POSITION_VALUE:
                    holdings[sym]["shares"] = 0.0
                trade_rows.append([trade_date, "SELL", sym, close_px, exec_px, sell_shares, trade_value, fee_tax, "T+1 target execution"])

            else:
                exec_px = close_px * (1.0 + cfg["buy_slippage"])
                buyable_cash = max(0.0, cash * 0.98)
                if buyable_cash <= 0:
                    continue
                shares = delta_value / exec_px
                trade_value = shares * exec_px
                fee_tax = calc_fee_tax(trade_value, "BUY", cfg)
                total_cash_needed = trade_value + fee_tax
                if total_cash_needed > buyable_cash:
                    shares = buyable_cash / (exec_px * (1.0 + cfg["buy_fee_rate"]))
                    trade_value = shares * exec_px
                    fee_tax = calc_fee_tax(trade_value, "BUY", cfg)
                    total_cash_needed = trade_value + fee_tax
                if shares <= 0 or trade_value < MIN_TRADE_VALUE:
                    continue
                prev_shares = float(holdings[sym].get("shares", 0.0))
                prev_cost = float(holdings[sym].get("avg_cost", 0.0))
                new_shares = prev_shares + shares
                new_avg_cost = ((prev_shares * prev_cost) + (shares * exec_px)) / max(new_shares, 1e-12)
                holdings[sym]["shares"] = new_shares
                holdings[sym]["avg_cost"] = new_avg_cost
                cash -= total_cash_needed
                trade_rows.append([trade_date, "BUY", sym, close_px, exec_px, shares, trade_value, fee_tax, "T+1 target execution"])

    holdings = {s: p for s, p in holdings.items() if float(p.get("shares", 0.0)) > 0}
    return holdings, cash


def build_next_day_targets(day_df, holdings):
    if day_df.empty:
        return {}, holdings, 0.0, []

    macro_score = float(day_df["macro_score"].iloc[0])
    exposure = get_exposure(macro_score)
    ranked = rank_day(day_df)
    day_map = {str(r["symbol"]): r for _, r in day_df.iterrows()}
    desired, signals = {}, []

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
        if pnl <= STOP_LOSS_1 and not pos["stop1_hit"]:
            pos["weight_mult"] *= 0.5
            pos["stop1_hit"] = True
            signals.append((sym, "STOP_PART", pnl))
        if pnl >= ADD_LV1 and not pos["add1_hit"]:
            pos["weight_mult"] *= 1.05
            pos["add1_hit"] = True
            signals.append((sym, "ADD_LV1", pnl))
        if pnl >= ADD_LV2 and not pos["add2_hit"]:
            pos["weight_mult"] *= 1.10
            pos["add2_hit"] = True
            signals.append((sym, "ADD_LV2", pnl))
        desired[sym] = 1.0

    target_n = desired_symbol_count(exposure)
    active_set = {s for s, keep in desired.items() if keep > 0}
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
        if len(active_set) >= target_n:
            break

    keep_syms = [s for s, keep in desired.items() if keep > 0]
    if not keep_syms:
        return {}, holdings, exposure, signals
    mult_sum = sum(float(holdings[s].get("weight_mult", 1.0)) for s in keep_syms)
    if mult_sum <= 0:
        target_weights = {sym: exposure / len(keep_syms) for sym in keep_syms}
    else:
        target_weights = {sym: exposure * float(holdings[s].get("weight_mult", 1.0)) / mult_sum for sym in keep_syms}
    return target_weights, holdings, exposure, signals


def simulate(df, cfg):
    all_dates = sorted(df["trade_date"].dropna().unique())
    holdings, cash = {}, cfg["initial_capital"]
    peak_nav = cfg["initial_capital"]
    nav_rows, trade_rows, signal_rows, position_rows = [], [], [], []
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
        gross_value = 0.0
        count = 0
        for sym, pos in list(holdings.items()):
            row = day_map.get(sym)
            if row is None:
                continue
            px = float(row["close"])
            val = float(pos.get("shares", 0.0)) * px
            if val >= MIN_POSITION_VALUE:
                nav += val
                gross_value += val
                count += 1
                position_rows.append([date, sym, px, pos.get("shares", 0.0), val, pos.get("avg_cost", 0.0)])
        holdings = {s: p for s, p in holdings.items() if float(p.get("shares", 0.0)) > 0}

        peak_nav = max(peak_nav, nav)
        dd = nav / peak_nav - 1.0
        macro_score = float(day["macro_score"].iloc[0]) if "macro_score" in day.columns else 0.0
        exposure_now = gross_value / nav if nav > 0 else 0.0
        nav_rows.append([date, nav, cash, macro_score, exposure_now, dd, count])

        pending_targets, holdings, signal_exposure, signals = build_next_day_targets(day, holdings)
        for sym, action, pnl in signals:
            signal_rows.append([date, action, sym, pnl, signal_exposure])

    nav_df = pd.DataFrame(nav_rows, columns=["date", "nav", "cash", "macro", "exposure", "dd", "count"])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    trade_df = pd.DataFrame(trade_rows, columns=["date", "action", "symbol", "close_px", "exec_px", "shares", "trade_value", "fee_tax", "note"])
    signal_df = pd.DataFrame(signal_rows, columns=["date", "signal", "symbol", "pnl_vs_cost", "target_exposure"])
    pos_df = pd.DataFrame(position_rows, columns=["date", "symbol", "close", "shares", "position_value", "avg_cost"])
    return nav_df, trade_df, signal_df, pos_df


def build_summary(nav, trades, meta):
    total_return = nav["nav"].iloc[-1] / meta["initial_capital"] - 1.0
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
        "trade_days": int(len(nav)),
        "filtered_symbols": meta["filtered_symbols"],
        "available_min_date": meta["available_min_date"],
        "available_max_date": meta["available_max_date"],
    }])


def main():
    args = parse_args()
    cfg = {
        "initial_capital": float(args.initial_capital),
        "max_volume_participation": float(args.max_volume_participation),
        "buy_fee_rate": float(args.buy_fee_rate),
        "sell_fee_rate": float(args.sell_fee_rate),
        "sell_tax_rate": float(args.sell_tax_rate),
        "buy_slippage": float(args.buy_slippage),
        "sell_slippage": float(args.sell_slippage),
    }

    price = load_price_data(args.price)
    macro = load_macro_data(args.macro)
    df, meta = merge_and_filter(price, macro, args.start, args.end)
    meta["initial_capital"] = cfg["initial_capital"]
    meta["price_file"] = args.price
    meta["macro_file"] = args.macro

    nav, trades, signals, positions = simulate(df, cfg)
    if nav.empty:
        raise ValueError(f"åæ¸¬çµæçºç©º | start={args.start} end={args.end} | filtered_rows={meta['filtered_rows']}")

    summary = build_summary(nav, trades, meta)
    prefix = Path(args.output_prefix)
    nav.to_csv(f"{prefix}_nav.csv", index=False)
    trades.to_csv(f"{prefix}_trades.csv", index=False)
    signals.to_csv(f"{prefix}_signals.csv", index=False)
    positions.to_csv(f"{prefix}_daily_positions.csv", index=False)
    summary.to_csv(f"{prefix}_summary.csv", index=False)
    with open(f"{prefix}_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta, ensure_ascii=False))
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
