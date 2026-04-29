"""
trading_system_allocator.py

交易體系配置器：Top 標註版

目的：
保留完整候選名單，不刪除非 Top 股票。
但新增：
- rank：總排名
- bucket_rank：桶內排名
- execution_flag：TOP / WATCH / BLOCK
- allowed：是否允許執行
- system_note：執行說明

核心：
1. 名單全部保留。
2. 只有 TOP 允許下單。
3. WATCH 可觀察，不建議下單。
4. BLOCK 不可下單。
5. 根據市場狀態自動決定每個桶可執行幾檔。

輸入：
- market_regime.json
- trade_plan.csv
- core_candidates.csv
- alpha_candidates.csv
- pre_move_candidates.csv

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
    "close",
    "price_tier",
    "target_weight",
    "suggested_amount",
    "execution_flag",
    "allowed",
    "execution",
    "reason",
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
    """
    slot 定義：
    top_slots = 可以執行的檔數
    watch_slots = 額外觀察檔數
    """
    if regime_code == "BEAR":
        return {
            "CORE": {"top": 2, "watch": 5},
            "PRE": {"top": 0, "watch": 5},
            "ALPHA": {"top": 1, "watch": 4},
        }
    if regime_code == "RANGE":
        return {
            "CORE": {"top": 3, "watch": 6},
            "PRE": {"top": 2, "watch": 6},
            "ALPHA": {"top": 1, "watch": 5},
        }
    if regime_code == "BULL":
        return {
            "CORE": {"top": 5, "watch": 8},
            "PRE": {"top": 3, "watch": 6},
            "ALPHA": {"top": 2, "watch": 6},
        }
    if regime_code == "EXPLOSIVE":
        return {
            "CORE": {"top": 7, "watch": 8},
            "PRE": {"top": 2, "watch": 6},
            "ALPHA": {"top": 3, "watch": 6},
        }

    return {
        "CORE": {"top": 2, "watch": 5},
        "PRE": {"top": 0, "watch": 5},
        "ALPHA": {"top": 1, "watch": 4},
    }


def base_amount(bucket, flag, budget, capital, top_count):
    if flag != "TOP":
        return 0

    if top_count <= 0 or budget <= 0:
        return 0

    if bucket == "CORE":
        max_each = min(0.02, budget / top_count)
    elif bucket == "PRE":
        max_each = min(0.01, budget / top_count)
    else:
        max_each = min(0.01, budget / top_count)

    return int(round(capital * max_each / 1000) * 1000)


def build_core_rows(trade_plan, core_df):
    source = trade_plan if not trade_plan.empty else core_df
    if source.empty:
        return []

    df = source.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]
    if "action" not in df.columns:
        df["action"] = "BUY"

    if "stock_id" not in df.columns:
        return []

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 80), axis=1)
    df = df[df["action"].isin(["BUY", "TEST", "WATCH"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).reset_index(drop=True)

    rows = []
    for i, r in df.iterrows():
        close = get_close(r)
        rows.append({
            "bucket": "CORE",
            "priority": 1,
            "stock_id": r["stock_id"],
            "action": "BUY" if r["action"] == "BUY" else r["action"],
            "score": float(r["score_value"]),
            "close": close,
            "price_tier": get_tier(r, close),
            "reason": str(r.get("note", r.get("signal_tags", "強勢主攻"))),
            "risk_note": "CORE 是主倉來源，但仍受市場狀態與 Top slot 限制。",
        })
    return rows


def build_pre_rows(pre_df):
    if pre_df.empty:
        return []

    df = pre_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns:
        return []

    if "action" not in df.columns:
        df["action"] = "WATCH"

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 0), axis=1)
    df = df[df["action"].isin(["BUY", "TEST", "WATCH"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).reset_index(drop=True)

    rows = []
    for _, r in df.iterrows():
        close = get_close(r)
        rows.append({
            "bucket": "PRE",
            "priority": 2,
            "stock_id": r["stock_id"],
            "action": r["action"],
            "score": float(r["score_value"]),
            "close": close,
            "price_tier": get_tier(r, close),
            "reason": str(r.get("signal_tags", r.get("setup_type", "主力佈局"))),
            "risk_note": "PRE 是提前佈局，只能小倉，熊市禁止進場。",
        })
    return rows


def build_alpha_rows(alpha_df):
    if alpha_df.empty:
        return []

    df = alpha_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]
    if "action" not in df.columns:
        df["action"] = "TEST"

    if "stock_id" not in df.columns:
        return []

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["action"] = df["action"].apply(normalize_action)
    df["score_value"] = df.apply(lambda r: get_score(r, 58), axis=1)
    df = df[df["action"].isin(["BUY", "TEST", "WATCH"])].copy()
    df = df.sort_values(["score_value", "stock_id"], ascending=[False, True]).reset_index(drop=True)

    rows = []
    for _, r in df.iterrows():
        close = get_close(r)
        rows.append({
            "bucket": "ALPHA",
            "priority": 3,
            "stock_id": r["stock_id"],
            "action": "TEST" if r["action"] == "BUY" else r["action"],
            "score": float(r["score_value"]),
            "close": close,
            "price_tier": get_tier(r, close),
            "reason": str(r.get("note", r.get("signal_tags", "補位機會"))),
            "risk_note": "ALPHA 是補位，不可優先於 CORE。",
        })
    return rows


def apply_execution_flags(out, regime_code, budget, gross_exposure):
    if out.empty:
        return out

    slots = get_slots(regime_code)
    out = out.sort_values(["priority", "score", "stock_id"], ascending=[True, False, True]).copy()

    # 同股票多桶重複：保留最高優先權那筆為主要，其他標 BLOCK 但不刪除
    first_seen = set()
    duplicate_mask = []
    for sid in out["stock_id"]:
        if sid in first_seen:
            duplicate_mask.append(True)
        else:
            duplicate_mask.append(False)
            first_seen.add(sid)
    out["is_duplicate_lower_priority"] = duplicate_mask

    result_parts = []
    for bucket, g in out.groupby("bucket", sort=False):
        g = g.sort_values(["score", "stock_id"], ascending=[False, True]).copy()
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
        result_parts.append(g)

    out = pd.concat(result_parts, ignore_index=True)
    out = out.sort_values(["priority", "bucket_rank"]).reset_index(drop=True)
    out["rank"] = range(1, len(out) + 1)

    # 先用 slot 產生 allowed，再用總曝險做第二層限制
    out["allowed"] = out["execution_flag"] == "TOP"

    # amount 只給 TOP
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

    # 總曝險上限
    max_total_amount = int(DEFAULT_CAPITAL * float(gross_exposure))
    running = 0
    final_allowed = []
    system_notes = []
    executions = []

    for _, r in out.iterrows():
        flag = r["execution_flag"]
        amount = int(r["suggested_amount"])

        if flag == "TOP":
            if running + amount <= max_total_amount:
                final_allowed.append(True)
                running += amount
                system_notes.append("TOP：允許執行")
                executions.append("可分批下單")
            else:
                final_allowed.append(False)
                system_notes.append("TOP 但超出總曝險，暫緩")
                executions.append("暫緩")
        elif flag == "WATCH":
            final_allowed.append(False)
            system_notes.append("WATCH：保留觀察，不下單")
            executions.append("只觀察")
        else:
            final_allowed.append(False)
            if bool(r.get("is_duplicate_lower_priority", False)):
                system_notes.append("BLOCK：同股已由高優先桶接管")
            else:
                system_notes.append("BLOCK：超出本市場狀態可執行名額")
            executions.append("不可下單")

    out["allowed"] = final_allowed
    out["system_note"] = system_notes
    out["execution"] = executions

    return out


def main():
    regime = load_json(ROOT / "market_regime.json", default={}) or {}
    if not regime:
        regime = load_json(DATA_DIR / "market_regime.json", default={}) or {}

    regime_code = regime.get("regime", "UNKNOWN")
    label = regime.get("label", "未知")
    budget = regime.get("budget", {"PRE": 0.0, "CORE": 0.12, "ALPHA": 0.03})
    gross_exposure = float(regime.get("gross_exposure", 0.15))

    trade_plan = read_csv_any([ROOT / "trade_plan.csv", DATA_DIR / "trade_plan.csv"])
    core = read_csv_any([ROOT / "core_candidates.csv", DATA_DIR / "core_candidates.csv"])
    pre = read_csv_any([ROOT / "pre_move_candidates.csv", DATA_DIR / "pre_move_candidates.csv"])
    alpha = read_csv_any([ROOT / "alpha_candidates.csv", DATA_DIR / "alpha_candidates.csv"])

    rows = []
    rows += build_core_rows(trade_plan, core)
    rows += build_pre_rows(pre)
    rows += build_alpha_rows(alpha)

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        out = apply_execution_flags(out, regime_code, budget, gross_exposure)
        out = out[OUTPUT_COLUMNS]

    out.to_csv(ROOT / "trading_system_plan.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "trading_system_plan.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "trading_system_allocator_top_flag",
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
        "bucket_counts": out["bucket"].value_counts().to_dict() if not out.empty else {},
        "allowed_amount": int(out.loc[out["allowed"], "suggested_amount"].sum()) if not out.empty else 0,
        "rule": "保留完整名單；只允許 TOP 下單；WATCH 觀察；BLOCK 禁止。"
    }

    for p in [ROOT / "trading_system_summary.json", DATA_DIR / "trading_system_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
