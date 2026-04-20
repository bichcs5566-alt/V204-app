import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25
CORE_TOP_N = 25
ALPHA_TOP_N = 6
MIN_CORE_FILL = 5
MIN_ALPHA_FILL = 2
MAX_POSITION_WEIGHT = 0.10
BUY_BAND = 0.015
REDUCE_BAND = -0.015
SLIPPAGE = 0.001
INITIAL_CAPITAL = 1000000
STOP_LOSS_2 = -0.10
DEFAULT_SHARES = 1000

PRICE_PANEL_FILE = "price_panel_daily.csv"
POSITIONS_FILE = "current_positions.csv"
DASHBOARD_DIR = Path("mobile_dashboard_v1")
DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"

def now_taipei():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)

def read_csv_auto(path):
    for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def price_tier_key(price):
    if pd.isna(price): return "unknown"
    p = float(price)
    if p < 50: return "lt_50"
    if p < 100: return "p50_100"
    if p < 300: return "p100_300"
    if p < 500: return "p300_500"
    if p < 1000: return "p500_1000"
    return "gt_1000"

def ensure_dashboard_files():
    DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    templates = {
        "current_positions.csv": ["stock_id","shares","avg_cost","last_action_date","note"],
        "watchlist.csv": ["stock_id"],
        "selection_debug.csv": ["date","total_input","valid_after_na","core_primary_count","alpha_primary_count","core_final_count","alpha_final_count"],
        "position_monitor.csv": ["signal_date","trade_date","stock_id","price_tier","ref_price","shares","avg_cost","pnl_pct","target_weight","current_weight_est","action","note"],
        "watchlist_monitor.csv": ["signal_date","trade_date","stock_id","price_tier","ref_price","holding_status","strategy_bucket","action","pnl_pct"],
        "trade_plan.csv": ["signal_date","trade_date","action","stock_id","price_tier","target_weight","ref_price","suggested_amount","note"],
        "full_summary.csv": ["return","mdd","sharpe_daily"],
    }
    for name, cols in templates.items():
        path = DASHBOARD_DATA_DIR / name
        if not path.exists():
            pd.DataFrame(columns=cols).to_csv(path, index=False, encoding="utf-8-sig")
    meta = DASHBOARD_DATA_DIR / "meta.json"
    if not meta.exists():
        meta.write_text(json.dumps({}, ensure_ascii=False, indent=2), encoding="utf-8")

def ensure_price_panel():
    if Path(PRICE_PANEL_FILE).exists():
        return
    if Path("merge_chunked_price_panel.py").exists():
        subprocess.run([sys.executable, "merge_chunked_price_panel.py"], check=True)
    if not Path(PRICE_PANEL_FILE).exists():
        raise FileNotFoundError("æ¾ä¸å° price_panel_daily.csv")

def load_price():
    df = read_csv_auto(PRICE_PANEL_FILE)
    df.columns = [str(c).lower().strip() for c in df.columns]
    if "date" not in df.columns:
        for alt in ["trade_date", "datetime"]:
            if alt in df.columns:
                df["date"] = df[alt]
                break
    if "stock_id" not in df.columns:
        for alt in ["symbol", "code"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break
    if "close" not in df.columns:
        raise ValueError("price_panel_daily.csv ç¼ºå° close æ¬ä½")
    if "volume" not in df.columns:
        df["volume"] = np.nan
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df = df.dropna(subset=["date","stock_id","close"]).copy()
    df = df[df["close"] > 0].sort_values(["stock_id","date"]).reset_index(drop=True)
    return df

def build_features(df):
    g = df.groupby("stock_id")
    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)
    return df

def select_stocks(day_df):
    total_input = len(day_df)
    valid = day_df.dropna(subset=["mom20","mom60","vol20"]).copy()
    valid_count = len(valid)

    core_primary = valid[valid["mom20"] > -0.02].copy()
    core_primary["score"] = core_primary["mom20"] * 0.6 + core_primary["mom60"] * 0.4
    core_primary = core_primary.sort_values("score", ascending=False)
    core_fallback = valid.copy()
    core_fallback["score"] = core_fallback["mom20"] * 0.6 + core_fallback["mom60"] * 0.4
    core_fallback = core_fallback.sort_values("score", ascending=False)
    core = core_primary.head(CORE_TOP_N).copy()
    if len(core) < MIN_CORE_FILL:
        extra = core_fallback[~core_fallback["stock_id"].isin(core["stock_id"])].head(MIN_CORE_FILL - len(core))
        core = pd.concat([core, extra], ignore_index=True)
    if len(core) < CORE_TOP_N:
        extra = core_fallback[~core_fallback["stock_id"].isin(core["stock_id"])].head(CORE_TOP_N - len(core))
        core = pd.concat([core, extra], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(CORE_TOP_N)

    alpha_primary = valid[valid["mom20"] > 0].copy()
    alpha_primary["quality"] = (alpha_primary["mom20"] * 0.6 + alpha_primary["mom60"] * 0.4) / (alpha_primary["vol20"] + 1e-6)
    alpha_primary = alpha_primary.sort_values("quality", ascending=False)
    alpha_fallback = valid.copy()
    alpha_fallback["quality"] = (alpha_fallback["mom20"] * 0.6 + alpha_fallback["mom60"] * 0.4) / (alpha_fallback["vol20"] + 1e-6)
    alpha_fallback = alpha_fallback.sort_values("quality", ascending=False)
    alpha = alpha_primary.head(ALPHA_TOP_N).copy()
    if len(alpha) < MIN_ALPHA_FILL:
        extra = alpha_fallback[~alpha_fallback["stock_id"].isin(alpha["stock_id"])].head(MIN_ALPHA_FILL - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True)
    if len(alpha) < ALPHA_TOP_N:
        extra = alpha_fallback[~alpha_fallback["stock_id"].isin(alpha["stock_id"])].head(ALPHA_TOP_N - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(ALPHA_TOP_N)

    debug = pd.DataFrame([{
        "date": str(day_df["date"].iloc[0].date()) if len(day_df) else "",
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
        target[r["stock_id"]] = target.get(r["stock_id"], 0) + ALPHA_WEIGHT / alpha_n
    for k in list(target.keys()):
        target[k] = min(target[k], MAX_POSITION_WEIGHT)
    return target

def load_positions():
    preferred = DASHBOARD_DATA_DIR / "current_positions.csv"
    root_path = Path(POSITIONS_FILE)
    src = preferred if preferred.exists() else root_path
    if not src.exists():
        return pd.DataFrame(columns=["stock_id","shares","avg_cost","last_action_date","note"])
    pos = read_csv_auto(src)
    pos.columns = [str(c).lower().strip() for c in pos.columns]
    for col in ["shares","avg_cost","last_action_date","note"]:
        if col not in pos.columns:
            pos[col] = ""
    pos["stock_id"] = pos["stock_id"].astype(str).str.strip()
    pos["shares"] = pd.to_numeric(pos["shares"], errors="coerce").fillna(DEFAULT_SHARES)
    pos["avg_cost"] = pd.to_numeric(pos["avg_cost"], errors="coerce")
    return pos[["stock_id","shares","avg_cost","last_action_date","note"]]

def load_watchlist():
    path = DASHBOARD_DATA_DIR / "watchlist.csv"
    if not path.exists():
        return set()
    try:
        df = read_csv_auto(path)
        if "stock_id" not in df.columns:
            return set()
        return set(df["stock_id"].astype(str).str.strip())
    except Exception:
        return set()

def build_outputs(df):
    dates = sorted(df["date"].unique())
    if len(dates) < 2:
        raise ValueError("äº¤ææ¥ä¸è¶³ï¼ç¡æ³ç¢çè¨è")
    signal_date = dates[-2]
    trade_date = dates[-1]
    signal_df = df[df["date"] == signal_date].copy()
    trade_df = df[df["date"] == trade_date].copy()
    trade_price = {r["stock_id"]: r["close"] for _, r in trade_df.iterrows()}
    positions = load_positions()
    pos_map = positions.set_index("stock_id").to_dict("index") if len(positions) else {}
    watchlist = load_watchlist()
    core, alpha, debug = select_stocks(signal_df)
    target = build_target_weights(core, alpha)
    core_set = set(core["stock_id"]); alpha_set = set(alpha["stock_id"])
    all_symbols = sorted(set(target.keys()) | set(pos_map.keys()) | watchlist)

    trade_rows, pos_rows, watch_rows = [], [], []
    for stock_id in all_symbols:
        px_raw = trade_price.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        tier = price_tier_key(ref_price)
        target_weight = float(target.get(stock_id, 0))
        current = pos_map.get(stock_id, {})
        shares = current.get("shares", DEFAULT_SHARES)
        avg_cost = current.get("avg_cost", np.nan)
        pnl_pct = np.nan
        if pd.notna(px_raw) and pd.notna(avg_cost) and float(avg_cost) > 0:
            pnl_pct = px_raw / avg_cost - 1.0
        current_value = shares * px_raw if pd.notna(shares) and pd.notna(px_raw) else np.nan
        current_weight_est = current_value / INITIAL_CAPITAL if pd.notna(current_value) else np.nan

        if stock_id in pos_map:
            if pd.notna(pnl_pct) and pnl_pct <= STOP_LOSS_2:
                action = "STOP_LOSS"; note = "éåææ¢ä»¶"
            elif target_weight <= 0:
                action = "SELL"; note = "ä¸å¨ç®æ¨æ± "
            elif pd.notna(current_weight_est):
                diff = target_weight - current_weight_est
                if diff > BUY_BAND: action = "ADD"; note = "ä½æ¼ç®æ¨æ¬é"
                elif diff < REDUCE_BAND: action = "REDUCE"; note = "é«æ¼ç®æ¨æ¬é"
                else: action = "HOLD"; note = "æ¬éå¨å®¹è¨±ç¯å"
            else:
                action = "HOLD"; note = "ç¼ºå°ç®åæ¬é"
            pos_rows.append({
                "signal_date": str(signal_date.date()), "trade_date": str(trade_date.date()), "stock_id": stock_id,
                "price_tier": tier, "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
                "shares": int(shares) if pd.notna(shares) else DEFAULT_SHARES,
                "avg_cost": round(avg_cost, 4) if pd.notna(avg_cost) else "",
                "pnl_pct": round(pnl_pct, 4) if pd.notna(pnl_pct) else "",
                "target_weight": round(target_weight, 4),
                "current_weight_est": round(current_weight_est, 4) if pd.notna(current_weight_est) else "",
                "action": action, "note": note
            })
        elif target_weight > 0:
            trade_rows.append({
                "signal_date": str(signal_date.date()), "trade_date": str(trade_date.date()), "action": "BUY",
                "stock_id": stock_id, "price_tier": tier, "target_weight": round(target_weight, 4),
                "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
                "suggested_amount": round(INITIAL_CAPITAL * target_weight, 2), "note": "æ°é²å ´"
            })

        if stock_id in watchlist:
            bucket = "NONE"
            if stock_id in core_set: bucket = "CANDIDATE"
            elif stock_id in alpha_set: bucket = "BUY_READY"
            watch_action = "WATCH"
            if stock_id in pos_map: watch_action = "HOLD_MONITOR"
            elif stock_id in alpha_set: watch_action = "BUY_READY"
            elif stock_id in core_set: watch_action = "CANDIDATE"
            watch_rows.append({
                "signal_date": str(signal_date.date()), "trade_date": str(trade_date.date()), "stock_id": stock_id,
                "price_tier": tier, "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
                "holding_status": "å·²ææ" if stock_id in pos_map else "æªææ",
                "strategy_bucket": bucket, "action": watch_action,
                "pnl_pct": round(pnl_pct, 4) if pd.notna(pnl_pct) else ""
            })

    summary = pd.DataFrame([{"return": 0, "mdd": 0, "sharpe_daily": 0}])
    meta = {
        "generated_at": now_taipei().strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": str(signal_date.date()),
        "trade_date": str(trade_date.date()),
        "data_state": "fresh",
        "source": "v266.8_dual_button"
    }
    return pd.DataFrame(trade_rows), pd.DataFrame(pos_rows), pd.DataFrame(watch_rows), summary, debug, meta

def write_csv_both(df, filename):
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    df.to_csv(DASHBOARD_DATA_DIR / filename, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    ensure_dashboard_files()
    ensure_price_panel()
    df = build_features(load_price())
    trade_df, pos_df, watch_df, summary, debug, meta = build_outputs(df)
    write_csv_both(trade_df, "trade_plan.csv")
    write_csv_both(pos_df, "position_monitor.csv")
    write_csv_both(watch_df, "watchlist_monitor.csv")
    write_csv_both(summary, "full_summary.csv")
    write_csv_both(debug, "selection_debug.csv")

    positions = load_positions()
    positions.to_csv(DASHBOARD_DATA_DIR / "current_positions.csv", index=False, encoding="utf-8-sig")
    positions.to_csv("current_positions.csv", index=False, encoding="utf-8-sig")

    (DASHBOARD_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    Path("meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print("å®æè¼¸åº")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
