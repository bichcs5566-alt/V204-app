"""
v3_1_dashboard_bridge.py
v265.3 dual engine dashboard bridge

只同步資料，不改策略。
支援 market_regime / strategy_type 欄位。
"""

from pathlib import Path
from datetime import datetime
import json
import shutil
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SCHEMAS = {
    "trade_plan.csv": [
        "signal_date","trade_date","market_regime","strategy_type","action","action_label",
        "action_sub","stock_id","price_tier","ref_price","target_weight","suggested_amount",
        "suggested_shares","estimated_total_cost","entry_score","source","note"
    ],
    "core_candidates.csv": [
        "date","stock_id","close","volume","mom5","mom10","mom20","mom60",
        "entry_score","strategy_type","action","action_label","action_sub","note"
    ],
    "alpha_candidates.csv": [
        "date","stock_id","close","volume","mom5","mom10","mom20","mom60",
        "entry_score","strategy_type","action","action_label","action_sub","note"
    ],
    "candidates.csv": [
        "date","stock_id","close","volume","mom5","mom10","mom20","mom60",
        "entry_score","strategy_type","engine","action","action_label","action_sub","note"
    ],
    "selection_debug.csv": [
        "generated_at","price_source","signal_date","market_regime","regime_score",
        "pct_above_ma20","pct_above_ma60","pct_mom20_pos","pct_strong","median_mom20",
        "total_input_rows","latest_stock_count","core_buy_count","core_test_count",
        "core_watch_count","alpha_buy_count","alpha_test_count","alpha_watch_count",
        "trade_buy_count","trade_test_count","trade_watch_count","core_count","alpha_count",
        "trade_plan_count","core_max_score","alpha_max_score","note"
    ],
    "full_summary.csv": ["return","mdd","sharpe_daily"],
    "daily_nav.csv": ["date","nav","ret"],
    "current_positions.csv": ["stock_id","shares","avg_cost"],
    "position_monitor.csv": ["stock_id","shares","avg_cost","note"],
    "watchlist_monitor.csv": ["stock_id","note"],
}

def copy_csv(name):
    src = ROOT / name
    dst = DATA_DIR / name
    if src.exists() and src.stat().st_size > 0:
        try:
            df = pd.read_csv(src)
        except Exception:
            df = pd.DataFrame(columns=SCHEMAS.get(name, []))
    else:
        df = pd.DataFrame(columns=SCHEMAS.get(name, []))
    df.to_csv(dst, index=False, encoding="utf-8")
    return {"file": name, "rows": int(len(df)), "state": "ok"}

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def main():
    results = [copy_csv(name) for name in SCHEMAS]

    if (ROOT / "price_panel_daily.csv").exists():
        shutil.copyfile(ROOT / "price_panel_daily.csv", DATA_DIR / "price_panel_daily.csv")

    meta = load_json(ROOT / "meta.json")

    try:
        tp = pd.read_csv(DATA_DIR / "trade_plan.csv")
    except Exception:
        tp = pd.DataFrame()

    meta.setdefault("source", "v265_3_dual_engine_core")
    meta["bridge_source"] = "v265_3_dashboard_bridge"
    meta["bridge_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta["trade_plan_count"] = int(len(tp))
    meta["buy_count"] = int((tp.get("action", pd.Series(dtype=str)) == "BUY").sum()) if len(tp) else 0
    meta["test_count"] = int((tp.get("action", pd.Series(dtype=str)) == "TEST").sum()) if len(tp) else 0
    meta["watch_count"] = int((tp.get("action", pd.Series(dtype=str)) == "WATCH").sum()) if len(tp) else 0
    meta["core_trade_count"] = int((tp.get("strategy_type", pd.Series(dtype=str)) == "CORE").sum()) if len(tp) else 0
    meta["alpha_trade_count"] = int((tp.get("strategy_type", pd.Series(dtype=str)) == "ALPHA").sum()) if len(tp) else 0
    meta["data_state"] = "fresh"
    meta["bridge_files"] = results

    with open(DATA_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    with open(ROOT / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    pd.DataFrame(results).to_csv(DATA_DIR / "bridge_debug.csv", index=False, encoding="utf-8")
    pd.DataFrame(results).to_csv(ROOT / "bridge_debug.csv", index=False, encoding="utf-8")

    print("v265.3 bridge completed")
    print(pd.DataFrame(results).to_string(index=False))

if __name__ == "__main__":
    main()
