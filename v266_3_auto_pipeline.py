import os
import subprocess
import sys
from pathlib import Path
import pandas as pd
import numpy as np

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25

CORE_TOP_N = 25
ALPHA_TOP_N = 6

MIN_CORE_FILL = 5
MIN_ALPHA_FILL = 2

MAX_POSITION_WEIGHT = 0.10

FEE = 0.0015
SLIPPAGE = 0.001

INITIAL_CAPITAL = 1000000
STOP_LOSS_2 = -0.10

POSITIONS_FILE = "current_positions.csv"
PRICE_PANEL_FILE = "price_panel_daily.csv"

DASHBOARD_DIR = Path("mobile_dashboard_v1")
DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"

def ensure_price_panel():
    if os.path.exists(PRICE_PANEL_FILE):
        print(f"[OK] Found {PRICE_PANEL_FILE}")
        return
    if os.path.exists("merge_chunked_price_panel.py"):
        print("[INFO] price_panel_daily.csv missing, running merge_chunked_price_panel.py ...")
        subprocess.run([sys.executable, "merge_chunked_price_panel.py"], check=True)
    else:
        raise FileNotFoundError("price_panel_daily.csv missing and merge_chunked_price_panel.py not found")
    if not os.path.exists(PRICE_PANEL_FILE):
        raise FileNotFoundError("Failed to create price_panel_daily.csv")

def ensure_dashboard_dir():
    DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for name, cols in {
        "current_positions.csv": ["stock_id", "shares", "avg_cost", "last_action_date", "note"],
        "watchlist.csv": ["stock_id"],
        "selection_debug.csv": ["date","total_input","valid_after_na","core_primary_count","alpha_primary_count","core_final_count","alpha_final_count"],
    }.items():
        path = DASHBOARD_DATA_DIR / name
        if not path.exists():
            pd.DataFrame(columns=cols).to_csv(path, index=False)

def load_price():
    df = pd.read_csv(PRICE_PANEL_FILE)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
        else:
            raise ValueError("no date column")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        else:
            raise ValueError("no stock_id column")

    if "close" not in df.columns:
        raise ValueError("no close column")

    if "volume" not in df.columns:
        df["volume"] = np.nan

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.strip()

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df

def build_features(df):
    g = df.groupby("stock_id")
    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)
    return df

def price_tier_from_price(price):
    if pd.isna(price):
        return "unknown"
    price = float(price)
    if price < 50:
        return "50ä»¥ä¸"
    if price < 100:
        return "50-100"
    if price < 300:
        return "100-300"
    if price < 500:
        return "300-500"
    if price < 1000:
        return "500-1000"
    return "1000ä»¥ä¸"

def select_stocks(d):
    total_input = len(d)
    valid = d.dropna(subset=["mom20", "mom60", "vol20"]).copy()
    valid_count = len(valid)

    core_primary = valid[valid["mom20"] > -0.02].copy()
    core_primary["score"] = core_primary["mom20"] * 0.6 + core_primary["mom60"] * 0.4
    core_primary = core_primary.sort_values("score", ascending=False)

    core_fallback = valid.copy()
    core_fallback["score"] = core_fallback["mom20"] * 0.6 + core_fallback["mom60"] * 0.4
    core_fallback = core_fallback.sort_values("score", ascending=False)

    core = core_primary.head(CORE_TOP_N).copy()
    if len(core) < MIN_CORE_FILL:
        extra = core_fallback[~core_fallback["stock_id"].isin(set(core["stock_id"]))].head(MIN_CORE_FILL - len(core))
        core = pd.concat([core, extra], ignore_index=True)
    if len(core) < CORE_TOP_N:
        extra2 = core_fallback[~core_fallback["stock_id"].isin(set(core["stock_id"]))].head(CORE_TOP_N - len(core))
        core = pd.concat([core, extra2], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(CORE_TOP_N)

    alpha_primary = valid[valid["mom20"] > 0].copy()
    alpha_primary["quality"] = (alpha_primary["mom20"] * 0.6 + alpha_primary["mom60"] * 0.4) / (alpha_primary["vol20"] + 1e-6)
    alpha_primary = alpha_primary.sort_values("quality", ascending=False)

    alpha_fallback = valid.copy()
    alpha_fallback["quality"] = (alpha_fallback["mom20"] * 0.6 + alpha_fallback["mom60"] * 0.4) / (alpha_fallback["vol20"] + 1e-6)
    alpha_fallback = alpha_fallback.sort_values("quality", ascending=False)

    alpha = alpha_primary.head(ALPHA_TOP_N).copy()
    if len(alpha) < MIN_ALPHA_FILL:
        extra = alpha_fallback[~alpha_fallback["stock_id"].isin(set(alpha["stock_id"]))].head(MIN_ALPHA_FILL - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True)
    if len(alpha) < ALPHA_TOP_N:
        extra2 = alpha_fallback[~alpha_fallback["stock_id"].isin(set(alpha["stock_id"]))].head(ALPHA_TOP_N - len(alpha))
        alpha = pd.concat([alpha, extra2], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(ALPHA_TOP_N)

    debug = pd.DataFrame([{
        "date": d["date"].iloc[0] if len(d) else pd.NaT,
        "total_input": total_input,
        "valid_after_na": valid_count,
        "core_primary_count": len(core_primary),
        "alpha_primary_count": len(alpha_primary),
        "core_final_count": len(core),
        "alpha_final_count": len(alpha),
    }])

    return core, alpha, debug

def build_target_weights(core, alpha):
    target = {}
    core_n = max(len(core), 1)
    alpha_n = max(len(alpha), 1)

    for _, r in core.iterrows():
        target[r["stock_id"]] = CORE_WEIGHT / core_n
    for _, r in alpha.iterrows():
        target[r["stock_id"]] = target.get(r["stock_id"], 0.0) + ALPHA_WEIGHT / alpha_n

    for k in list(target.keys()):
        target[k] = min(target[k], MAX_POSITION_WEIGHT)
    return target

def load_current_positions():
    preferred = DASHBOARD_DATA_DIR / "current_positions.csv"
    pos_path = preferred if preferred.exists() else Path(POSITIONS_FILE)

    if not pos_path.exists():
        cols = ["stock_id", "shares", "avg_cost", "last_action_date", "note"]
        pd.DataFrame(columns=cols).to_csv(pos_path, index=False)
        return pd.DataFrame(columns=cols)

    pos = pd.read_csv(pos_path)
    pos.columns = [str(c).lower().strip() for c in pos.columns]

    if "stock_id" not in pos.columns:
        raise ValueError("current_positions.csv must contain stock_id")

    for col in ["shares", "avg_cost"]:
        if col not in pos.columns:
            pos[col] = np.nan
    for col in ["last_action_date", "note"]:
        if col not in pos.columns:
            pos[col] = ""

    pos["stock_id"] = pos["stock_id"].astype(str).str.strip()
    pos["shares"] = pd.to_numeric(pos["shares"], errors="coerce")
    pos["avg_cost"] = pd.to_numeric(pos["avg_cost"], errors="coerce")
    return pos[["stock_id", "shares", "avg_cost", "last_action_date", "note"]]

def load_watchlist():
    path = DASHBOARD_DATA_DIR / "watchlist.csv"
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
        if "stock_id" not in df.columns:
            return set()
        return set(df["stock_id"].astype(str).str.strip())
    except Exception:
        return set()

def build_trade_plan(df, latest_capital=INITIAL_CAPITAL):
    df = df.dropna(subset=["ret1", "mom5", "mom20", "mom60", "vol20"]).copy()
    dates = sorted(df["date"].unique())
    if len(dates) < 2:
        raise ValueError("not enough dates")

    signal_date = dates[-2]
    trade_date = dates[-1]

    signal_df = df[df["date"] == signal_date].copy()
    trade_df = df[df["date"] == trade_date].copy()

    trade_price = {r["stock_id"]: r["close"] for _, r in trade_df.iterrows()}
    current_positions = load_current_positions()
    current_map = current_positions.set_index("stock_id").to_dict("index") if len(current_positions) > 0 else {}
    watchlist = load_watchlist()

    core, alpha, debug = select_stocks(signal_df)
    target = build_target_weights(core, alpha)

    core_set = set(core["stock_id"])
    alpha_set = set(alpha["stock_id"])
    all_symbols = sorted(set(target.keys()) | set(current_map.keys()) | watchlist)

    rows = []
    for stock_id in all_symbols:
        px_raw = trade_price.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        target_weight = float(target.get(stock_id, 0.0))
        current = current_map.get(stock_id, {})
        current_shares = current.get("shares", np.nan)
        avg_cost = current.get("avg_cost", np.nan)

        source = []
        if stock_id in core_set:
            source.append("CORE")
        if stock_id in alpha_set:
            source.append("ALPHA")
        if stock_id in watchlist:
            source.append("WATCHLIST")
        source_text = "+".join(source)

        suggested_amount = latest_capital * target_weight if target_weight > 0 else 0.0
        suggested_shares = suggested_amount / ref_price if (pd.notna(ref_price) and ref_price > 0 and target_weight > 0) else np.nan
        estimated_total_cost = suggested_amount * (1 + FEE) if target_weight > 0 else 0.0

        action = None
        note = []

        if stock_id in current_map:
            if pd.notna(px_raw) and pd.notna(avg_cost) and avg_cost > 0:
                pnl = px_raw / avg_cost - 1.0
            else:
                pnl = np.nan

            if pd.notna(pnl) and pnl <= STOP_LOSS_2:
                action = "STOP_LOSS"
                note.append(f"pnl<= {STOP_LOSS_2:.0%}")
            elif target_weight <= 0:
                action = "SELL"
                note.append("not_in_target")
            else:
                current_value = (current_shares * px_raw) if (pd.notna(current_shares) and pd.notna(px_raw)) else np.nan
                current_weight_est = current_value / latest_capital if (pd.notna(current_value) and latest_capital > 0) else np.nan

                if pd.notna(current_weight_est):
                    diff = target_weight - current_weight_est
                    if diff > 0.015:
                        action = "BUY"
                        note.append("increase_to_target")
                    elif diff < -0.015:
                        action = "REDUCE"
                        note.append("decrease_to_target")
                    else:
                        action = "HOLD"
                        note.append("within_band")
                else:
                    action = "HOLD"
                    note.append("missing_position_size")
        else:
            if target_weight > 0:
                action = "BUY"
                note.append("new_entry")
            elif stock_id in watchlist:
                action = "WATCH"
                note.append("watchlist_only")

        if action is None:
            continue

        rows.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "action": action,
            "stock_id": stock_id,
            "price_tier": price_tier_from_price(ref_price),
            "target_weight": round(target_weight, 4),
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else np.nan,
            "current_shares": round(current_shares, 2) if pd.notna(current_shares) else np.nan,
            "avg_cost": round(avg_cost, 4) if pd.notna(avg_cost) else np.nan,
            "suggested_amount": round(suggested_amount, 2),
            "suggested_shares": round(suggested_shares, 2) if pd.notna(suggested_shares) else np.nan,
            "estimated_total_cost": round(estimated_total_cost, 2),
            "source": source_text,
            "note": ";".join(note),
        })

    plan = pd.DataFrame(rows)
    action_order = {"STOP_LOSS": 0, "SELL": 1, "REDUCE": 2, "BUY": 3, "HOLD": 4, "WATCH": 5}
    if not plan.empty:
        plan["action_rank"] = plan["action"].map(action_order).fillna(99)
        plan = plan.sort_values(["action_rank", "target_weight", "ref_price"], ascending=[True, False, True]).drop(columns=["action_rank"])
    return plan, core, alpha, signal_date, trade_date, debug

def build_watchlist_monitor(df):
    dates = sorted(df["date"].unique())
    if len(dates) < 2:
        return pd.DataFrame()

    signal_date = dates[-2]
    trade_date = dates[-1]

    signal_df = df[df["date"] == signal_date].copy()
    trade_df = df[df["date"] == trade_date].copy()
    trade_price = {r["stock_id"]: r["close"] for _, r in trade_df.iterrows()}
    current_positions = load_current_positions()
    current_map = current_positions.set_index("stock_id").to_dict("index") if len(current_positions) > 0 else {}
    watchlist = sorted(load_watchlist())

    core, alpha, _ = select_stocks(signal_df)
    core_set = set(core["stock_id"])
    alpha_set = set(alpha["stock_id"])

    rows = []
    for stock_id in watchlist:
        px_raw = trade_price.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        holding = stock_id in current_map
        avg_cost = current_map.get(stock_id, {}).get("avg_cost", np.nan)
        shares = current_map.get(stock_id, {}).get("shares", np.nan)

        if holding and pd.notna(px_raw) and pd.notna(avg_cost) and avg_cost > 0:
            pnl_pct = px_raw / avg_cost - 1.0
        else:
            pnl_pct = np.nan

        if stock_id in core_set:
            strategy_bucket = "CORE"
            action = "åé¸"
        elif stock_id in alpha_set:
            strategy_bucket = "ALPHA"
            action = "åé¸"
        else:
            strategy_bucket = "NONE"
            action = "è§å¯"

        if holding:
            if pd.notna(pnl_pct) and pnl_pct <= STOP_LOSS_2:
                action = "åæ"
            else:
                action = "ææç£æ§"

        rows.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "stock_id": stock_id,
            "price_tier": price_tier_from_price(ref_price),
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else np.nan,
            "holding_status": "å·²ææ" if holding else "æªææ",
            "shares": round(shares, 2) if pd.notna(shares) else np.nan,
            "avg_cost": round(avg_cost, 4) if pd.notna(avg_cost) else np.nan,
            "pnl_pct": round(pnl_pct, 4) if pd.notna(pnl_pct) else np.nan,
            "strategy_bucket": strategy_bucket,
            "action": action,
        })
    return pd.DataFrame(rows)

def run_backtest(df):
    df = df.dropna(subset=["ret1", "mom5", "mom20", "mom60", "vol20"]).copy()
    dates = sorted(df["date"].unique())
    cash = INITIAL_CAPITAL
    holdings = {}
    nav_list = []

    for i in range(len(dates) - 1):
        today = dates[i]
        next_day = dates[i + 1]
        today_df = df[df["date"] == today].copy()
        next_df = df[df["date"] == next_day].copy()
        next_price = {r["stock_id"]: r["close"] for _, r in next_df.iterrows()}

        core, alpha, _ = select_stocks(today_df)
        target = build_target_weights(core, alpha)

        nav = cash
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        new_holdings = {}
        new_cash = nav
        for s, w in target.items():
            if s not in next_price:
                continue
            price = next_price[s] * (1 + SLIPPAGE)
            alloc_value = nav * w
            shares = alloc_value / price
            gross_cost = shares * price
            total_cost = gross_cost * (1 + FEE)
            if total_cost > new_cash:
                continue
            new_cash -= total_cost
            new_holdings[s] = {"shares": shares, "cost": price}

        holdings = new_holdings
        cash = new_cash

        nav = cash
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]
        nav_list.append({"date": next_day, "nav": nav})

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    return nav_df

def evaluate(nav_df):
    total_return = nav_df["nav"].iloc[-1] / nav_df["nav"].iloc[0] - 1.0
    mdd = (nav_df["nav"] / nav_df["nav"].cummax() - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-6)
    return pd.DataFrame([{"return": total_return, "mdd": mdd, "sharpe_daily": sharpe}])

def save_output_both(df, filename):
    df.to_csv(filename, index=False)
    df.to_csv(DASHBOARD_DATA_DIR / filename, index=False)

if __name__ == "__main__":
    ensure_dashboard_dir()
    ensure_price_panel()

    df = load_price()
    df = build_features(df)

    nav_df = run_backtest(df)
    summary_df = evaluate(nav_df)
    trade_plan_df, core_df, alpha_df, signal_date, trade_date, debug_df = build_trade_plan(df)
    watchlist_monitor_df = build_watchlist_monitor(df)

    core_df["price_tier"] = core_df["close"].apply(price_tier_from_price)
    alpha_df["price_tier"] = alpha_df["close"].apply(price_tier_from_price)

    save_output_both(nav_df, "daily_nav.csv")
    save_output_both(summary_df, "full_summary.csv")
    save_output_both(trade_plan_df, "trade_plan.csv")
    save_output_both(core_df, "core_candidates.csv")
    save_output_both(alpha_df, "alpha_candidates.csv")
    save_output_both(debug_df, "selection_debug.csv")
    save_output_both(watchlist_monitor_df, "watchlist_monitor.csv")

    current_positions = load_current_positions()
    current_positions.to_csv(DASHBOARD_DATA_DIR / "current_positions.csv", index=False)
    current_positions.to_csv(POSITIONS_FILE, index=False)

    watchlist_path = DASHBOARD_DATA_DIR / "watchlist.csv"
    if not watchlist_path.exists():
        pd.DataFrame(columns=["stock_id"]).to_csv(watchlist_path, index=False)

    print("Signal date:", signal_date)
    print("Trade date:", trade_date)
    print("Dashboard data dir:", str(DASHBOARD_DATA_DIR))
    print(summary_df.to_string(index=False))
    print("\nTop trade plan:")
    print(trade_plan_df.head(15).to_string(index=False))
