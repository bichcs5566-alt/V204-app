from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_COLUMNS = [
    "final_action", "stock_id", "source", "bucket", "strategy_type", "score", "entry_type",
    "execution_flag", "allowed", "close", "suggested_amount", "target_weight",
    "priority", "reason", "system_note",
    "liquidity_level", "liquidity_tag", "liquidity_score", "volume", "turnover",
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
    return s.zfill(4) if s.isdigit() and len(s) <= 4 else s

def is_true(x):
    return str(x).strip().lower() in ["true", "1", "yes"] or x is True

def pct_text(x):
    try:
        return f"{round(float(x) * 100, 2)}%"
    except Exception:
        return ""

def make_lookup():
    frames = []
    for name in ["trade_plan.csv", "candidates.csv", "alpha_candidates.csv", "core_candidates.csv"]:
        df = read_csv_any([ROOT / name, DATA_DIR / name])
        if not df.empty and "stock_id" in df.columns:
            df = df.copy()
            df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
            frames.append(df)
    if not frames:
        return {}
    all_df = pd.concat(frames, ignore_index=True).drop_duplicates("stock_id", keep="first")
    return {str(r["stock_id"]): r.to_dict() for _, r in all_df.iterrows()}

def clean(v, default=""):
    if v is None:
        return default
    s = str(v)
    if s in ["", "nan", "None"]:
        return default
    return v

def pick(row, lookup, col, default=""):
    v = clean(row.get(col, ""), None)
    if v is not None:
        return v
    sid = normalize_stock_id(row.get("stock_id", ""))
    return clean(lookup.get(sid, {}).get(col, ""), default)

def main():
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lookup = make_lookup()

    trading = read_csv_any([
        ROOT / "trading_system_plan.csv",
        DATA_DIR / "trading_system_plan.csv",
        ROOT / "trade_plan.csv",
        DATA_DIR / "trade_plan.csv",
    ])
    exitp = read_csv_any([ROOT / "exit_risk_plan.csv", DATA_DIR / "exit_risk_plan.csv"])

    rows = []
    holding_ids = set()

    if not exitp.empty:
        exitp.columns = [str(c).strip() for c in exitp.columns]
        if "stock_id" in exitp.columns:
            exitp["stock_id"] = exitp["stock_id"].apply(normalize_stock_id)
            holding_ids = set(exitp["stock_id"])

        for _, r in exitp.iterrows():
            raw_action = str(r.get("exit_action", "")).upper()
            if raw_action == "SELL":
                final_action, priority, allowed, note = "SELL", 0, True, "æåé¢¨æ§ï¼å¿é åªåèçåºå ´"
            elif raw_action == "REDUCE":
                final_action, priority, allowed, note = "REDUCE", 1, True, "æåé¢¨æ§ï¼å»ºè­°éåæ§é¢¨éª"
            elif raw_action in ["HOLD", "WATCH"]:
                final_action, priority, allowed, note = "WATCH", 7, False, "æåè§å¯ï¼ç®åä¸æ°å¢ãä¸åºå ´"
            else:
                continue

            reason_parts = []
            if clean(r.get("exit_reason", "")):
                reason_parts.append(str(r.get("exit_reason", "")))
            u = pct_text(r.get("unrealized_pct", ""))
            if u:
                reason_parts.append(f"æç {u}")
            if clean(r.get("avg_cost", "")):
                reason_parts.append(f"åå¹ {r.get('avg_cost')}")
            if clean(r.get("lots", "")):
                reason_parts.append(f"å¼µæ¸ {r.get('lots')}")

            rows.append({
                "final_action": final_action,
                "stock_id": r.get("stock_id", ""),
                "source": "EXIT",
                "bucket": "POSITION",
                "strategy_type": "POSITION",
                "score": r.get("exit_priority", 0),
                "entry_type": raw_action,
                "execution_flag": raw_action,
                "allowed": allowed,
                "close": r.get("close", ""),
                "suggested_amount": r.get("position_value", ""),
                "target_weight": "",
                "priority": priority,
                "reason": " | ".join(reason_parts),
                "system_note": f"{note}ï½é¢¨éª {r.get('risk_level', '')}",
                "liquidity_level": "",
                "liquidity_tag": "",
                "liquidity_score": "",
                "volume": "",
                "turnover": "",
            })

    if not trading.empty:
        trading.columns = [str(c).strip() for c in trading.columns]
        if "stock_id" in trading.columns:
            trading["stock_id"] = trading["stock_id"].apply(normalize_stock_id)

        for _, r in trading.iterrows():
            sid = r.get("stock_id", "")
            if sid in holding_ids:
                continue

            raw_action = str(r.get("action", r.get("final_action", ""))).upper()
            allowed = is_true(r.get("allowed", True))
            strategy_type = pick(r, lookup, "strategy_type", pick(r, lookup, "bucket", ""))
            bucket = pick(r, lookup, "bucket", strategy_type)

            if raw_action in ["BUY", "TEST", "WATCH", "BLOCK"]:
                final_action = raw_action
            else:
                flag = str(r.get("execution_flag", "")).upper()
                if allowed and flag == "TOP":
                    final_action = "BUY" if str(strategy_type).upper() == "ALPHA" else "TEST"
                elif flag == "WATCH":
                    final_action = "WATCH"
                else:
                    final_action = "BLOCK"

            # å¯¦æ°ä¿è­·ï¼æµåæ§ä¸è¶³ä¸å¯ BUY
            liq = str(pick(r, lookup, "liquidity_level", "")).upper()
            if final_action == "BUY" and liq in ["LOW", "BLOCK", ""]:
                final_action = "TEST" if liq == "LOW" else "BLOCK"

            priority = {"SELL": 0, "REDUCE": 1, "BUY": 2, "TEST": 3, "WATCH": 8, "BLOCK": 9}.get(final_action, 9)

            rows.append({
                "final_action": final_action,
                "stock_id": sid,
                "source": pick(r, lookup, "source", "ENTRY"),
                "bucket": bucket,
                "strategy_type": strategy_type,
                "score": pick(r, lookup, "score", pick(r, lookup, "entry_score", "")),
                "entry_type": pick(r, lookup, "action_sub", r.get("entry_type", "")),
                "execution_flag": r.get("execution_flag", raw_action),
                "allowed": allowed,
                "close": pick(r, lookup, "close", pick(r, lookup, "ref_price", "")),
                "suggested_amount": pick(r, lookup, "suggested_amount", ""),
                "target_weight": pick(r, lookup, "target_weight", ""),
                "priority": priority,
                "reason": pick(r, lookup, "reason", pick(r, lookup, "note", "")),
                "system_note": pick(r, lookup, "system_note", pick(r, lookup, "note", "")),
                "liquidity_level": pick(r, lookup, "liquidity_level", ""),
                "liquidity_tag": pick(r, lookup, "liquidity_tag", ""),
                "liquidity_score": pick(r, lookup, "liquidity_score", ""),
                "volume": pick(r, lookup, "volume", ""),
                "turnover": pick(r, lookup, "turnover", ""),
            })

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if not out.empty:
        out = out.sort_values(["priority", "score", "stock_id"], ascending=[True, False, True])

    out.to_csv(ROOT / "final_action_plan.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "final_action_plan.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": generated_at,
        "source": "final_decision_engine_final_integrated",
        "rows": int(len(out)),
        "sell_count": int((out["final_action"] == "SELL").sum()) if not out.empty else 0,
        "reduce_count": int((out["final_action"] == "REDUCE").sum()) if not out.empty else 0,
        "buy_count": int((out["final_action"] == "BUY").sum()) if not out.empty else 0,
        "test_count": int((out["final_action"] == "TEST").sum()) if not out.empty else 0,
        "watch_count": int((out["final_action"] == "WATCH").sum()) if not out.empty else 0,
        "block_count": int((out["final_action"] == "BLOCK").sum()) if not out.empty else 0,
        "alpha_count": int((out["strategy_type"].astype(str).str.upper() == "ALPHA").sum()) if not out.empty else 0,
        "core_count": int((out["strategy_type"].astype(str).str.upper() == "CORE").sum()) if not out.empty else 0,
        "high_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "HIGH").sum()) if not out.empty else 0,
        "medium_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "MEDIUM").sum()) if not out.empty else 0,
        "low_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "LOW").sum()) if not out.empty else 0,
    }

    for p in [ROOT / "final_action_summary.json", DATA_DIR / "final_action_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
