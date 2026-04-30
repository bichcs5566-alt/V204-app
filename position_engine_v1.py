"""
position_engine_v1.py

資金配置引擎 Position Engine v1

不破壞目前 v2_8_auto_update 舊穩定流程。
自動尋找來源，更新 suggested_amount / target_weight。

優先讀取：
1. final_action_plan.csv
2. mobile_dashboard_v1/data/final_action_plan.csv
3. trading_system_plan.csv
4. mobile_dashboard_v1/data/trading_system_plan.csv
5. trade_plan.csv
6. mobile_dashboard_v1/data/trade_plan.csv

輸出：
- trade_plan.csv
- mobile_dashboard_v1/data/trade_plan.csv
- position_engine_summary.json
- mobile_dashboard_v1/data/position_engine_summary.json
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TOTAL_CAPITAL = 1_000_000
DAILY_USE_RATIO = 0.40
MAX_PER_STOCK_RATIO = 0.10

SOURCE_FILES = [
    ROOT / "final_action_plan.csv",
    DATA_DIR / "final_action_plan.csv",
    ROOT / "trading_system_plan.csv",
    DATA_DIR / "trading_system_plan.csv",
    ROOT / "trade_plan.csv",
    DATA_DIR / "trade_plan.csv",
]

OUTPUT_ROOT = ROOT / "trade_plan.csv"
OUTPUT_DASHBOARD = DATA_DIR / "trade_plan.csv"
SUMMARY_ROOT = ROOT / "position_engine_summary.json"
SUMMARY_DASHBOARD = DATA_DIR / "position_engine_summary.json"


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def find_source():
    for p in SOURCE_FILES:
        if p.exists() and p.stat().st_size > 0:
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return p, df
            except Exception:
                pass
    raise FileNotFoundError("找不到可用來源：final_action_plan / trading_system_plan / trade_plan")


def normalize_columns(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns:
        for alt in ["symbol", "code", "ticker"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break

    if "stock_id" not in df.columns:
        raise ValueError(f"missing stock_id column, columns={list(df.columns)}")

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    if "action" not in df.columns:
        if "final_action" in df.columns:
            df["action"] = df["final_action"]
        else:
            df["action"] = "WATCH"

    if "execution_flag" not in df.columns:
        df["execution_flag"] = ""

    if "entry_type" not in df.columns:
        df["entry_type"] = ""

    if "bucket" not in df.columns:
        df["bucket"] = ""

    if "score" not in df.columns:
        for alt in ["pre_score", "rank_score", "quality"]:
            if alt in df.columns:
                df["score"] = df[alt]
                break
        if "score" not in df.columns:
            df["score"] = 0

    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)

    return df


def normalize_action(x):
    s = str(x).strip().upper()
    mapping = {
        "買進": "BUY",
        "賣出": "SELL",
        "減碼": "REDUCE",
        "試單": "TEST",
        "觀察": "WATCH",
        "禁止": "BLOCK",
    }
    return mapping.get(s, s if s else "WATCH")


def classify(row):
    action = normalize_action(row.get("action", ""))
    flag = str(row.get("execution_flag", "")).upper().strip()
    entry = str(row.get("entry_type", "")).upper().strip()
    bucket = str(row.get("bucket", "")).upper().strip()

    if action in ["SELL", "REDUCE"]:
        return action

    if action == "BLOCK":
        return "BLOCK"

    if action == "WATCH":
        return "WATCH"

    if action == "TEST" or bucket == "PRE":
        return "TEST"

    if flag == "TOP" and entry == "BREAK":
        return "TOP_BREAK"

    if flag == "TOP":
        return "TOP"

    if entry == "BREAK":
        return "BREAK"

    if action == "BUY":
        return "NORMAL_BUY"

    return "NORMAL"


WEIGHTS = {
    "TOP_BREAK": 0.20,
    "TOP": 0.15,
    "BREAK": 0.10,
    "NORMAL_BUY": 0.05,
    "NORMAL": 0.03,
    "TEST": 0.03,
    "WATCH": 0.00,
    "BLOCK": 0.00,
    "SELL": 0.00,
    "REDUCE": 0.00,
}


def calculate_position(df):
    df = df.copy()
    df["action"] = df["action"].apply(normalize_action)
    df["position_group"] = df.apply(classify, axis=1)

    usable_capital = TOTAL_CAPITAL * DAILY_USE_RATIO
    max_per_stock = TOTAL_CAPITAL * MAX_PER_STOCK_RATIO

    investable_groups = ["TOP_BREAK", "TOP", "BREAK", "NORMAL_BUY", "NORMAL", "TEST"]
    group_counts = df[df["position_group"].isin(investable_groups)]["position_group"].value_counts().to_dict()

    def amount(row):
        g = row["position_group"]
        if g not in investable_groups:
            return 0

        weight = WEIGHTS.get(g, 0)
        count = max(int(group_counts.get(g, 1)), 1)
        raw = usable_capital * weight / count
        capped = min(raw, max_per_stock)
        return int(round(capped / 1000) * 1000)

    df["suggested_amount"] = df.apply(amount, axis=1).astype(int)
    df["target_weight"] = (df["suggested_amount"] / TOTAL_CAPITAL).round(4)

    def note(row):
        g = row["position_group"]
        if g == "TOP_BREAK":
            return "TOP+BREAK 主攻，可優先分批"
        if g == "TOP":
            return "TOP 候選，可分批"
        if g == "BREAK":
            return "BREAK 候選，小心追高"
        if g in ["NORMAL_BUY", "NORMAL"]:
            return "一般買進，小倉"
        if g == "TEST":
            return "試單，小倉"
        if g == "SELL":
            return "出場優先，不配置新資金"
        if g == "REDUCE":
            return "減碼優先，不配置新資金"
        if g == "WATCH":
            return "觀察，不下單"
        if g == "BLOCK":
            return "禁止，不下單"
        return "無"

    df["position_note"] = df.apply(note, axis=1)
    return df


def main():
    source_path, raw = find_source()
    df = normalize_columns(raw)
    out = calculate_position(df)

    out.to_csv(OUTPUT_ROOT, index=False, encoding="utf-8-sig")
    out.to_csv(OUTPUT_DASHBOARD, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "position_engine_v1",
        "input_file": str(source_path),
        "total_capital": TOTAL_CAPITAL,
        "daily_use_ratio": DAILY_USE_RATIO,
        "max_per_stock_ratio": MAX_PER_STOCK_RATIO,
        "rows": int(len(out)),
        "total_suggested_amount": int(out["suggested_amount"].sum()),
        "group_counts": out["position_group"].value_counts().to_dict(),
        "action_counts": out["action"].value_counts().to_dict(),
        "rule": "依 TOP/BREAK/TEST 自動分配 suggested_amount 與 target_weight。"
    }

    for p in [SUMMARY_ROOT, SUMMARY_DASHBOARD]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
