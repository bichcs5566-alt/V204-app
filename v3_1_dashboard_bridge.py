"""
v3_1_dashboard_bridge.py
v265.1 統一接線版

定位：
只做資料同步，不做策略、不改名單、不覆蓋策略判斷。

來源：
repo root:
- trade_plan.csv
- candidates.csv
- core_candidates.csv
- alpha_candidates.csv
- selection_debug.csv
- full_summary.csv
- daily_nav.csv
- meta.json
- current_positions.csv
- position_monitor.csv
- watchlist_monitor.csv

目的地：
mobile_dashboard_v1/data/

原則：
1. trade_plan.csv 是唯一正式操作清單
2. bridge 不再產生候選、不再補假資料
3. 缺檔時建立只有表頭的安全檔，不產生 0 byte
4. app.js 只讀 mobile_dashboard_v1/data/
"""

from pathlib import Path
import json
import shutil
from datetime import datetime

import pandas as pd


ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


SCHEMAS = {
    "trade_plan.csv": [
        "signal_date", "trade_date", "action", "action_label", "action_sub",
        "stock_id", "price_tier", "ref_price", "target_weight",
        "suggested_amount", "suggested_shares", "estimated_total_cost",
        "entry_score", "trend_score", "volume_score", "structure_score",
        "confirm_score", "source", "note"
    ],
    "candidates.csv": [
        "date", "stock_id", "close", "volume", "mom5", "mom10", "mom20",
        "mom60", "entry_score", "trend_score", "volume_score",
        "structure_score", "confirm_score", "action", "action_label",
        "action_sub", "note"
    ],
    "core_candidates.csv": [
        "date", "stock_id", "close", "volume", "mom5", "mom10", "mom20",
        "mom60", "entry_score", "trend_score", "volume_score",
        "structure_score", "confirm_score", "action", "action_label",
        "action_sub", "note"
    ],
    "alpha_candidates.csv": [
        "date", "stock_id", "close", "volume", "mom5", "mom10", "mom20",
        "mom60", "entry_score", "trend_score", "volume_score",
        "structure_score", "confirm_score", "action", "action_label",
        "action_sub", "note"
    ],
    "selection_debug.csv": [
        "generated_at", "price_source", "signal_date", "total_input_rows",
        "latest_stock_count", "scored_count", "buy_count", "test_count",
        "watch_count", "skip_count", "core_count", "alpha_count",
        "trade_plan_count", "trade_buy_count", "trade_test_count",
        "trade_watch_count", "note"
    ],
    "full_summary.csv": ["return", "mdd", "sharpe_daily"],
    "daily_nav.csv": ["date", "nav", "ret"],
    "current_positions.csv": ["stock_id", "shares", "avg_cost"],
    "position_monitor.csv": ["stock_id", "shares", "avg_cost", "note"],
    "watchlist_monitor.csv": ["stock_id", "note"],
}


def safe_copy_csv(name):
    src = ROOT / name
    dst = DATA_DIR / name

    if src.exists() and src.stat().st_size > 0:
        try:
            df = pd.read_csv(src)
            if df.empty and len(df.columns) == 0:
                df = pd.DataFrame(columns=SCHEMAS.get(name, []))
            df.to_csv(dst, index=False, encoding="utf-8-sig")
            return {"file": name, "state": "copied", "rows": int(len(df))}
        except Exception as e:
            df = pd.DataFrame(columns=SCHEMAS.get(name, []))
            df.to_csv(dst, index=False, encoding="utf-8-sig")
            return {"file": name, "state": "fallback_schema", "rows": 0, "error": str(e)}

    df = pd.DataFrame(columns=SCHEMAS.get(name, []))
    df.to_csv(dst, index=False, encoding="utf-8-sig")
    return {"file": name, "state": "created_empty_schema", "rows": 0}


def safe_copy_raw(name):
    src = ROOT / name
    dst = DATA_DIR / name
    if src.exists() and src.stat().st_size > 0:
        shutil.copyfile(src, dst)
        return {"file": name, "state": "copied_raw"}
    return {"file": name, "state": "missing"}


def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def main():
    results = []

    for name in SCHEMAS:
        results.append(safe_copy_csv(name))

    if (ROOT / "price_panel_daily.csv").exists():
        results.append(safe_copy_raw("price_panel_daily.csv"))

    meta_root = ROOT / "meta.json"
    meta = load_json(meta_root)

    # 用 trade_plan 與 debug 修正 meta，避免前端顯示錯誤
    try:
        tp = pd.read_csv(DATA_DIR / "trade_plan.csv")
    except Exception:
        tp = pd.DataFrame()

    try:
        dbg = pd.read_csv(DATA_DIR / "selection_debug.csv")
    except Exception:
        dbg = pd.DataFrame()

    meta.setdefault("source", "v265_clean_strategy_core")
    meta["bridge_source"] = "v265_1_unified_dashboard_bridge"
    meta["bridge_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta["trade_plan_count"] = int(len(tp))
    meta["buy_count"] = int((tp.get("action", pd.Series(dtype=str)) == "BUY").sum()) if not tp.empty else 0
    meta["test_count"] = int((tp.get("action", pd.Series(dtype=str)) == "TEST").sum()) if not tp.empty else 0
    meta["watch_count"] = int((tp.get("action", pd.Series(dtype=str)) == "WATCH").sum()) if not tp.empty else 0

    if "signal_date" not in meta or not meta.get("signal_date"):
        if not dbg.empty and "signal_date" in dbg.columns:
            meta["signal_date"] = str(dbg["signal_date"].iloc[0])
        elif not tp.empty and "signal_date" in tp.columns:
            meta["signal_date"] = str(tp["signal_date"].iloc[0])

    meta["data_state"] = "fresh"
    meta["bridge_files"] = results

    with open(DATA_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    with open(ROOT / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    pd.DataFrame(results).to_csv(DATA_DIR / "bridge_debug.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(results).to_csv(ROOT / "bridge_debug.csv", index=False, encoding="utf-8-sig")

    print("v265.1 dashboard bridge completed")
    print(pd.DataFrame(results).to_string(index=False))


if __name__ == "__main__":
    main()
