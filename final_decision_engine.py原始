"""
final_decision_engine.py

C 完整版：最終操作決策引擎

把進場系統 trading_system_plan 與出場系統 exit_risk_plan 合併成每日真正操作表。

決策優先順序：
1. SELL / REDUCE 優先於 BUY
2. 已有持倉的股票，不重複列為新買進
3. allowed=True 且 execution_flag=TOP 才能列為 BUY/TEST
4. WATCH/BLOCK 只做觀察
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_COLUMNS = [
    "final_action", "stock_id", "source", "bucket", "score", "entry_type",
    "execution_flag", "allowed", "close", "suggested_amount", "target_weight",
    "priority", "reason", "system_note",
]


def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if p.exists() and p.stat().st_size > 0:
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return df
            except Exception:
                pass
    return pd.DataFrame()


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_true(x):
    return str(x).strip().lower() in ["true", "1", "yes"] or x is True


def main():
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    trading = read_csv_any([ROOT / "trading_system_plan.csv", DATA_DIR / "trading_system_plan.csv"])
    exitp = read_csv_any([ROOT / "exit_risk_plan.csv", DATA_DIR / "exit_risk_plan.csv"])

    rows = []
    holding_ids = set()

    if not exitp.empty:
        exitp.columns = [str(c).strip() for c in exitp.columns]
        if "stock_id" in exitp.columns:
            exitp["stock_id"] = exitp["stock_id"].apply(normalize_stock_id)
            holding_ids = set(exitp["stock_id"])

        for _, r in exitp.iterrows():
            action = str(r.get("exit_action", "")).upper()
            if action not in ["SELL", "REDUCE", "WATCH"]:
                continue
            rows.append({
                "final_action": action,
                "stock_id": r.get("stock_id", ""),
                "source": "EXIT",
                "bucket": "POSITION",
                "score": r.get("exit_priority", 0),
                "entry_type": "",
                "execution_flag": action,
                "allowed": True if action in ["SELL", "REDUCE"] else False,
                "close": r.get("close", ""),
                "suggested_amount": "",
                "target_weight": "",
                "priority": 0 if action == "SELL" else 1,
                "reason": r.get("exit_reason", ""),
                "system_note": "出場優先" if action in ["SELL", "REDUCE"] else "持倉觀察",
            })

    if not trading.empty:
        trading.columns = [str(c).strip() for c in trading.columns]
        if "stock_id" in trading.columns:
            trading["stock_id"] = trading["stock_id"].apply(normalize_stock_id)

        for _, r in trading.iterrows():
            sid = r.get("stock_id", "")
            if sid in holding_ids:
                continue

            allowed = is_true(r.get("allowed", False))
            flag = str(r.get("execution_flag", ""))
            bucket = str(r.get("bucket", ""))

            if allowed and flag == "TOP":
                final_action = "BUY" if bucket == "CORE" else "TEST"
                priority = 2 if bucket == "CORE" else 3
            elif flag == "WATCH":
                final_action = "WATCH"
                priority = 8
            else:
                final_action = "BLOCK"
                priority = 9

            rows.append({
                "final_action": final_action,
                "stock_id": sid,
                "source": "ENTRY",
                "bucket": bucket,
                "score": r.get("score", ""),
                "entry_type": r.get("entry_type", ""),
                "execution_flag": flag,
                "allowed": allowed,
                "close": r.get("close", ""),
                "suggested_amount": r.get("suggested_amount", ""),
                "target_weight": r.get("target_weight", ""),
                "priority": priority,
                "reason": r.get("reason", ""),
                "system_note": r.get("system_note", ""),
            })

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if not out.empty:
        out = out.sort_values(["priority", "score", "stock_id"], ascending=[True, False, True])

    out.to_csv(ROOT / "final_action_plan.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "final_action_plan.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": generated_at,
        "source": "final_decision_engine",
        "rows": int(len(out)),
        "sell_count": int((out["final_action"] == "SELL").sum()) if not out.empty else 0,
        "reduce_count": int((out["final_action"] == "REDUCE").sum()) if not out.empty else 0,
        "buy_count": int((out["final_action"] == "BUY").sum()) if not out.empty else 0,
        "test_count": int((out["final_action"] == "TEST").sum()) if not out.empty else 0,
        "watch_count": int((out["final_action"] == "WATCH").sum()) if not out.empty else 0,
        "block_count": int((out["final_action"] == "BLOCK").sum()) if not out.empty else 0,
        "rule": "先處理 SELL/REDUCE，再看 BUY/TEST；只做 allowed=True 的操作。"
    }

    for p in [ROOT / "final_action_summary.json", DATA_DIR / "final_action_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
