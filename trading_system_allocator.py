"""
trading_system_allocator.py

交易體系配置器

目的：
把 PRE / CORE / ALPHA 三套訊號整合成一張「真正可執行」的交易計畫。

輸入：
- market_regime.json
- pre_move_candidates.csv
- trade_plan.csv
- core_candidates.csv
- alpha_candidates.csv

輸出：
- trading_system_plan.csv
- trading_system_summary.json
- mobile_dashboard_v1/data/trading_system_plan.csv
- mobile_dashboard_v1/data/trading_system_summary.json

核心原則：
1. PRE 只做小倉，不重倉。
2. CORE 是主倉。
3. ALPHA 是補位。
4. 每日總新增部位受市場狀態限制。
5. 同一股票不可重複出現在多個桶，優先順序：CORE > PRE > ALPHA。
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CAPITAL = 1_000_000

OUTPUT_COLUMNS = [
    "bucket",
    "priority",
    "stock_id",
    "action",
    "score",
    "close",
    "price_tier",
    "target_weight",
    "suggested_amount",
    "execution",
    "reason",
    "risk_note",
]


def load_json(path, default=None):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return default
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


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


def normalize_action(x):
    s = str(x).upper().strip()
    if s in ["BUY", "買進"]:
        return "BUY"
    if s in ["TEST", "試單"]:
        return "TEST"
    if s in ["WATCH", "觀察"]:
        return "WATCH"
    return s if s else "WATCH"


def get_score(row, fallback=0):
    for c in ["score", "pre_score", "quality", "rank_score"]:
        if c in row and pd.notna(row[c]):
            try:
                return float(row[c])
            except Exception:
                pass
    return fallback


def get_close(row):
    for c in ["close", "ref_price", "price"]:
        if c in row and pd.notna(row[c]):
            try:
                return float(row[c])
            except Exception:
                pass
    return 0.0


def get_tier(row, close):
    for c in ["price_tier", "價格分層"]:
        if c in row and pd.notna(row[c]):
            return str(row[c])
    if close < 50:
        return "50以下"
    if close < 100:
        return "50-100"
    if close < 500:
        return "100-500"
    if close < 1000:
        return "500-1000"
    return "1000以上"


def make_pre_rows(df, budget, capital):
    rows = []
    if df.empty or budget <= 0:
        return rows

    df = df.copy()
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 0), axis=1)
    df = df[df["action"].isin(["BUY", "TEST"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).head(8)

    max_each = min(0.01, budget / max(len(df), 1))

    for i, r in df.iterrows():
        close = get_close(r)
        action = "TEST" if r["score_value"] < 80 else "BUY"
        weight = max_each if action == "BUY" else min(0.005, max_each)
        amount = int(round(capital * weight / 1000) * 1000)
        if amount <= 0:
            continue
        rows.append({
            "bucket": "PRE",
            "priority": 2,
            "stock_id": r["stock_id"],
            "action": action,
            "score": r["score_value"],
            "close": close,
            "price_tier": get_tier(r, close),
            "target_weight": round(weight, 4),
            "suggested_amount": amount,
            "execution": "小倉分批",
            "reason": str(r.get("signal_tags", r.get("setup_type", "主力佈局"))),
            "risk_note": "PRE 是提前佈局，不可重倉；跌破結構需退出。"
        })
    return rows


def make_core_rows(trade_plan, core_df, budget, capital):
    rows = []
    source = trade_plan if not trade_plan.empty else core_df
    if source.empty or budget <= 0:
        return rows

    df = source.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]

    if "action" not in df.columns:
        df["action"] = "BUY"

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 80), axis=1)
    df = df[df["action"].isin(["BUY", "TEST"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).head(10)

    max_each = min(0.02, budget / max(len(df), 1))

    for _, r in df.iterrows():
        close = get_close(r)
        weight = max_each
        amount = int(round(capital * weight / 1000) * 1000)
        if amount <= 0:
            continue
        rows.append({
            "bucket": "CORE",
            "priority": 1,
            "stock_id": r["stock_id"],
            "action": "BUY",
            "score": r["score_value"],
            "close": close,
            "price_tier": get_tier(r, close),
            "target_weight": round(weight, 4),
            "suggested_amount": amount,
            "execution": "主倉分批",
            "reason": str(r.get("note", r.get("signal_tags", "強勢主攻"))),
            "risk_note": "CORE 為主倉，但仍需按總曝險限制分批。"
        })
    return rows


def make_alpha_rows(alpha_df, budget, capital):
    rows = []
    if alpha_df.empty or budget <= 0:
        return rows

    df = alpha_df.copy()
    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]
    if "action" not in df.columns:
        df["action"] = "TEST"

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 58), axis=1)
    df = df[df["action"].isin(["BUY", "TEST", "WATCH"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).head(6)

    max_each = min(0.01, budget / max(len(df), 1))

    for _, r in df.iterrows():
        close = get_close(r)
        weight = max_each
        amount = int(round(capital * weight / 1000) * 1000)
        if amount <= 0:
            continue
        rows.append({
            "bucket": "ALPHA",
            "priority": 3,
            "stock_id": r["stock_id"],
            "action": "TEST",
            "score": r["score_value"],
            "close": close,
            "price_tier": get_tier(r, close),
            "target_weight": round(weight, 4),
            "suggested_amount": amount,
            "execution": "機動試單",
            "reason": str(r.get("note", r.get("signal_tags", "補位機會"))),
            "risk_note": "ALPHA 優先度低於 CORE，只做補位。"
        })
    return rows


def main():
    regime = load_json(ROOT / "market_regime.json", default={}) or {}
    if not regime:
        regime = load_json(DATA_DIR / "market_regime.json", default={}) or {}

    label = regime.get("label", "未知")
    regime_code = regime.get("regime", "UNKNOWN")
    budget = regime.get("budget", {"PRE": 0.08, "CORE": 0.35, "ALPHA": 0.07})
    gross_exposure = float(regime.get("gross_exposure", 0.5))

    pre = read_csv_any([ROOT / "pre_move_candidates.csv", DATA_DIR / "pre_move_candidates.csv"])
    trade_plan = read_csv_any([ROOT / "trade_plan.csv", DATA_DIR / "trade_plan.csv"])
    core = read_csv_any([ROOT / "core_candidates.csv", DATA_DIR / "core_candidates.csv"])
    alpha = read_csv_any([ROOT / "alpha_candidates.csv", DATA_DIR / "alpha_candidates.csv"])

    rows = []
    rows += make_core_rows(trade_plan, core, float(budget.get("CORE", 0)), DEFAULT_CAPITAL)
    rows += make_pre_rows(pre, float(budget.get("PRE", 0)), DEFAULT_CAPITAL)
    rows += make_alpha_rows(alpha, float(budget.get("ALPHA", 0)), DEFAULT_CAPITAL)

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    if not out.empty:
        # 去重：同一股票優先 CORE > PRE > ALPHA
        out = out.sort_values(["priority", "score"], ascending=[True, False])
        out = out.drop_duplicates("stock_id", keep="first")
        out = out.sort_values(["priority", "score"], ascending=[True, False]).reset_index(drop=True)

        # 總曝險控制
        max_amount = int(DEFAULT_CAPITAL * gross_exposure)
        running = 0
        keep = []
        for _, r in out.iterrows():
            amt = int(r["suggested_amount"])
            if running + amt <= max_amount:
                keep.append(True)
                running += amt
            else:
                keep.append(False)
        out["allowed"] = keep
        out["system_note"] = out["allowed"].map(lambda x: "可執行" if x else "超出總曝險，暫緩")
    else:
        out["allowed"] = []
        out["system_note"] = []

    out.to_csv(ROOT / "trading_system_plan.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "trading_system_plan.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "trading_system_allocator",
        "capital_assumption": DEFAULT_CAPITAL,
        "regime": regime_code,
        "label": label,
        "gross_exposure": gross_exposure,
        "budget": budget,
        "total_rows": int(len(out)),
        "allowed_rows": int(out["allowed"].sum()) if not out.empty else 0,
        "blocked_rows": int((~out["allowed"]).sum()) if not out.empty else 0,
        "bucket_counts": out["bucket"].value_counts().to_dict() if not out.empty else {},
        "allowed_amount": int(out.loc[out["allowed"], "suggested_amount"].sum()) if not out.empty else 0,
        "rule": "CORE 主倉，PRE 小倉佈局，ALPHA 補位；依市場狀態控制總曝險。"
    }

    for p in [ROOT / "trading_system_summary.json", DATA_DIR / "trading_system_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
