"""
trading_system_allocator.py

最終整合版交易體系配置器

整合：
1. 市場狀態 market_regime
2. PRE / CORE / ALPHA 分流
3. TOP / WATCH / BLOCK 執行權限
4. entry_type 進場 timing
5. allowed 最終可下單判斷
6. 保留完整名單，不刪除非 TOP

輸出：
- trading_system_plan.csv
- trading_system_summary.json
- mobile_dashboard_v1/data/trading_system_plan.csv
- mobile_dashboard_v1/data/trading_system_summary.json
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
    "rank",
    "bucket",
    "bucket_rank",
    "priority",
    "stock_id",
    "action",
    "score",
    "timing_score",
    "entry_type",
    "execution_flag",
    "allowed",
    "close",
    "price_tier",
    "target_weight",
    "suggested_amount",
    "execution",
    "reason",
    "entry_note",
    "risk_note",
    "system_note",
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
    if s in ["SKIP", "略過"]:
        return "SKIP"
    return s if s else "WATCH"


def get_score(row, fallback=0):
    for c in ["score", "pre_score", "quality", "rank_score"]:
        if c in row and pd.notna(row[c]):
            try:
                return float(row[c])
            except Exception:
                pass
    return float(fallback)


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


def get_slots(regime_code):
    if regime_code == "BEAR":
        return {"CORE": {"top": 2, "watch": 5}, "PRE": {"top": 0, "watch": 5}, "ALPHA": {"top": 1, "watch": 4}}
    if regime_code == "RANGE":
        return {"CORE": {"top": 3, "watch": 6}, "PRE": {"top": 2, "watch": 6}, "ALPHA": {"top": 1, "watch": 5}}
    if regime_code == "BULL":
        return {"CORE": {"top": 5, "watch": 8}, "PRE": {"top": 3, "watch": 6}, "ALPHA": {"top": 2, "watch": 6}}
    if regime_code == "EXPLOSIVE":
        return {"CORE": {"top": 7, "watch": 8}, "PRE": {"top": 2, "watch": 6}, "ALPHA": {"top": 3, "watch": 6}}
    return {"CORE": {"top": 2, "watch": 5}, "PRE": {"top": 0, "watch": 5}, "ALPHA": {"top": 1, "watch": 4}}


def base_amount(bucket, flag, budget, capital, top_count):
    if flag != "TOP" or top_count <= 0 or budget <= 0:
        return 0
    if bucket == "CORE":
        max_each = min(0.02, budget / top_count)
    elif bucket == "PRE":
        max_each = min(0.01, budget / top_count)
    else:
        max_each = min(0.01, budget / top_count)
    return int(round(capital * max_each / 1000) * 1000)


def build_rows_from_df(df, bucket, priority, fallback_score, default_action, default_reason, risk_note):
    if df.empty:
        return []

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]
    if "stock_id" not in df.columns:
        return []
    if "action" not in df.columns:
        df["action"] = default_action

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, fallback_score), axis=1)
    df = df[df["action"].isin(["BUY", "TEST", "WATCH"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).reset_index(drop=True)

    rows = []
    for _, r in df.iterrows():
        close = get_close(r)
        rows.append({
            "bucket": bucket,
            "priority": priority,
            "stock_id": r["stock_id"],
            "action": default_action if bucket == "ALPHA" else r["action"],
            "score": float(r["score_value"]),
            "close": close,
            "price_tier": get_tier(r, close),
            "reason": str(r.get("note", r.get("signal_tags", r.get("setup_type", default_reason)))),
            "risk_note": risk_note,
        })
    return rows


def apply_timing(out):
    timing = read_csv_any([ROOT / "timing_candidates.csv", DATA_DIR / "timing_candidates.csv"])
    if out.empty or timing.empty:
        out["timing_score"] = 0
        out["entry_type"] = "WAIT"
        out["entry_note"] = "未取得 timing 資料"
        return out

    timing.columns = [str(c).strip() for c in timing.columns]
    timing["stock_id"] = timing["stock_id"].apply(normalize_stock_id)

    tmap = timing.set_index("stock_id").to_dict("index")

    timing_scores = []
    entry_types = []
    entry_notes = []

    for _, r in out.iterrows():
        t = tmap.get(r["stock_id"], {})
        timing_scores.append(float(t.get("timing_score", 0) or 0))
        entry_types.append(str(t.get("entry_type", "WAIT")))
        entry_notes.append(str(t.get("entry_note", "等待")))

    out["timing_score"] = timing_scores
    out["entry_type"] = entry_types
    out["entry_note"] = entry_notes
    return out


def apply_execution(out, regime_code, budget, gross_exposure):
    if out.empty:
        return out

    slots = get_slots(regime_code)

    # 加入 timing 後排序：分數 + timing 綜合
    out["combined_score"] = out["score"] * 0.75 + out["timing_score"] * 0.25
    out = out.sort_values(["priority", "combined_score", "score", "stock_id"], ascending=[True, False, False, True]).copy()

    # 同股多桶：高優先桶接管，低優先桶 BLOCK 但保留
    first_seen = set()
    dup = []
    for sid in out["stock_id"]:
        if sid in first_seen:
            dup.append(True)
        else:
            dup.append(False)
            first_seen.add(sid)
    out["is_duplicate_lower_priority"] = dup

    parts = []
    for bucket, g in out.groupby("bucket", sort=False):
        g = g.sort_values(["combined_score", "score", "stock_id"], ascending=[False, False, True]).copy()
        g["bucket_rank"] = range(1, len(g) + 1)

        top_n = slots.get(bucket, {}).get("top", 0)
        watch_n = slots.get(bucket, {}).get("watch", 0)

        flags = []
        for _, r in g.iterrows():
            if bool(r["is_duplicate_lower_priority"]):
                flags.append("BLOCK")
            elif r["bucket_rank"] <= top_n:
                flags.append("TOP")
            elif r["bucket_rank"] <= top_n + watch_n:
                flags.append("WATCH")
            else:
                flags.append("BLOCK")
        g["execution_flag"] = flags
        parts.append(g)

    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["priority", "bucket_rank"]).reset_index(drop=True)
    out["rank"] = range(1, len(out) + 1)

    # timing gating：TOP 但 entry_type WAIT，仍可保留 TOP，但 allowed False
    out["target_weight"] = 0.0
    out["suggested_amount"] = 0

    for bucket in ["CORE", "PRE", "ALPHA"]:
        mask = (out["bucket"] == bucket) & (out["execution_flag"] == "TOP")
        top_count = int(mask.sum())
        b_budget = float(budget.get(bucket, 0))
        for idx in out[mask].index:
            amount = base_amount(bucket, "TOP", b_budget, DEFAULT_CAPITAL, top_count)
            out.at[idx, "suggested_amount"] = amount
            out.at[idx, "target_weight"] = round(amount / DEFAULT_CAPITAL, 4)

    max_total_amount = int(DEFAULT_CAPITAL * float(gross_exposure))
    running = 0
    allowed = []
    notes = []
    executions = []

    for _, r in out.iterrows():
        flag = r["execution_flag"]
        entry = r["entry_type"]
        bucket = r["bucket"]
        amount = int(r["suggested_amount"])

        entry_ok = False
        if bucket == "CORE":
            entry_ok = entry in ["BREAK", "PULLBACK"]
        elif bucket == "ALPHA":
            entry_ok = entry in ["REVERSAL", "PULLBACK"]
        elif bucket == "PRE":
            # PRE 只允許小倉觀察，通常不主動重倉；熊市 top_n = 0
            entry_ok = entry in ["WAIT", "PULLBACK"] and regime_code in ["RANGE", "BULL", "EXPLOSIVE"]

        if flag == "TOP":
            if not entry_ok:
                allowed.append(False)
                notes.append(f"TOP 但 entry={entry}，等待更好進場點")
                executions.append("等待")
            elif running + amount <= max_total_amount:
                allowed.append(True)
                running += amount
                notes.append("TOP + timing 合格：允許分批")
                executions.append("可分批下單")
            else:
                allowed.append(False)
                notes.append("TOP 但超出總曝險，暫緩")
                executions.append("暫緩")
        elif flag == "WATCH":
            allowed.append(False)
            notes.append("WATCH：保留觀察，不下單")
            executions.append("只觀察")
        else:
            allowed.append(False)
            if bool(r.get("is_duplicate_lower_priority", False)):
                notes.append("BLOCK：同股已由高優先桶接管")
            else:
                notes.append("BLOCK：超出本市場狀態可執行名額")
            executions.append("不可下單")

    out["allowed"] = allowed
    out["system_note"] = notes
    out["execution"] = executions
    return out


def main():
    regime = load_json(ROOT / "market_regime.json", default={}) or load_json(DATA_DIR / "market_regime.json", default={}) or {}

    regime_code = regime.get("regime", "UNKNOWN")
    label = regime.get("label", "未知")
    budget = regime.get("budget", {"PRE": 0.0, "CORE": 0.12, "ALPHA": 0.03})
    gross_exposure = float(regime.get("gross_exposure", 0.15))

    trade_plan = read_csv_any([ROOT / "trade_plan.csv", DATA_DIR / "trade_plan.csv"])
    core = read_csv_any([ROOT / "core_candidates.csv", DATA_DIR / "core_candidates.csv"])
    alpha = read_csv_any([ROOT / "alpha_candidates.csv", DATA_DIR / "alpha_candidates.csv"])
    pre = read_csv_any([ROOT / "pre_move_candidates.csv", DATA_DIR / "pre_move_candidates.csv"])

    core_source = trade_plan if not trade_plan.empty else core

    rows = []
    rows += build_rows_from_df(core_source, "CORE", 1, 80, "BUY", "強勢主攻", "CORE 是主倉來源，但受市場與 timing 限制。")
    rows += build_rows_from_df(pre, "PRE", 2, 70, "TEST", "主力佈局", "PRE 是主升段前卡位，只能小倉。")
    rows += build_rows_from_df(alpha, "ALPHA", 3, 58, "TEST", "短線補位", "ALPHA 是短線/反轉補位，不可重倉。")

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        out = apply_timing(out)
        out = apply_execution(out, regime_code, budget, gross_exposure)
        out = out[OUTPUT_COLUMNS]

    out.to_csv(ROOT / "trading_system_plan.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "trading_system_plan.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "final_trading_system_allocator",
        "capital_assumption": DEFAULT_CAPITAL,
        "regime": regime_code,
        "label": label,
        "gross_exposure": gross_exposure,
        "budget": budget,
        "slot_rule": get_slots(regime_code),
        "total_rows": int(len(out)),
        "top_rows": int((out["execution_flag"] == "TOP").sum()) if not out.empty else 0,
        "watch_rows": int((out["execution_flag"] == "WATCH").sum()) if not out.empty else 0,
        "block_rows": int((out["execution_flag"] == "BLOCK").sum()) if not out.empty else 0,
        "allowed_rows": int(out["allowed"].sum()) if not out.empty else 0,
        "entry_counts": out["entry_type"].value_counts().to_dict() if not out.empty else {},
        "bucket_counts": out["bucket"].value_counts().to_dict() if not out.empty else {},
        "allowed_amount": int(out.loc[out["allowed"], "suggested_amount"].sum()) if not out.empty else 0,
        "rule": "市場狀態 + 策略分流 + TOP標註 + entry timing + 總曝險。只做 allowed=True。"
    }

    for p in [ROOT / "trading_system_summary.json", DATA_DIR / "trading_system_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
