"""
final_decision_engine.py
v266.10 market_snapshot 主表版：多來源補資料層

基於 v266.9.2：
1. 保留所有 UI 欄位。
2. 除了 feature_panel_daily.csv，也從 pre_move_candidates.csv / timing_candidates.csv 補資料。
3. 不改策略判斷，只做資料補齊。

原 v266.9.2 說明：
完整補流動性資料

目的：
1. 保留 UI 欄位，不再因為缺資料就空白。
2. 從 feature_panel_daily.csv 補每檔最新：
   close / volume / turnover / liquidity_level / liquidity_tag / liquidity_score
3. trade_plan / candidates / alpha / core 有資料優先，沒有才用 feature_panel 補。
4. 持倉 EXIT 優先，進場策略不改。
5. CSV 輸出 utf-8-sig。
"""

from pathlib import Path
from datetime import datetime
import json
import math
import pandas as pd
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_COLUMNS = [
    "final_action", "stock_id", "stock_name", "source", "bucket", "strategy_type", "score", "entry_type",
    "execution_flag", "allowed", "close", "suggested_amount", "target_weight",
    "priority", "reason", "system_note",
    "liquidity_level", "liquidity_tag", "liquidity_score", "volume", "turnover",
]

def clean_text(v, default=""):
    if v is None:
        return default
    if isinstance(v, float) and math.isnan(v):
        return default
    s = str(v)
    if s.lower() in ["nan", "none", "null"]:
        return default
    return s

def normalize_stock_id(x):
    s = clean_text(x).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s

def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc, dtype={"stock_id": str})
                if not df.empty:
                    df.columns = [str(c).strip() for c in df.columns]
                    if "stock_id" in df.columns:
                        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
                    return df
            except Exception:
                continue
    return pd.DataFrame()

def write_csv_both(df, name):
    df.to_csv(ROOT / name, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / name, index=False, encoding="utf-8-sig")

def is_true(x):
    return str(x).strip().lower() in ["true", "1", "yes"] or x is True

def pct_text(x):
    try:
        return f"{round(float(x) * 100, 2)}%"
    except Exception:
        return ""

def calc_liquidity(df):
    df = df.copy()

    for c in ["close", "volume"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["turnover"] = df["close"] * df["volume"] * 1000

    vol_rank = df["volume"].rank(pct=True).fillna(0)
    turnover_rank = df["turnover"].rank(pct=True).fillna(0)
    df["liquidity_score"] = (vol_rank * 50 + turnover_rank * 50).round(2)

    high = (df["volume"] >= 3000) | (df["turnover"] >= 80_000_000) | (df["liquidity_score"] >= 75)
    medium = (df["volume"] >= 1000) | (df["turnover"] >= 30_000_000) | (df["liquidity_score"] >= 45)
    low = df["volume"] >= 500

    df["liquidity_level"] = np.select(
        [high, medium, low],
        ["HIGH", "MEDIUM", "LOW"],
        default="BLOCK"
    )

    df["liquidity_tag"] = df["liquidity_level"].map({
        "HIGH": "高流動性",
        "MEDIUM": "中流動性",
        "LOW": "低流動性",
        "BLOCK": "流動性不足",
    })

    return df

def load_feature_lookup():
    df = read_csv_any([ROOT / "feature_panel_daily.csv", DATA_DIR / "feature_panel_daily.csv"])
    if df.empty or "stock_id" not in df.columns:
        return {}

    df = df.copy()
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(["stock_id", "date"])
        latest = df.groupby("stock_id", as_index=False).tail(1).copy()
    else:
        latest = df.drop_duplicates("stock_id", keep="last").copy()

    latest = calc_liquidity(latest)

    keep = ["stock_id", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]
    for c in keep:
        if c not in latest.columns:
            latest[c] = ""

    return {str(r["stock_id"]): r.to_dict() for _, r in latest[keep].iterrows()}

def make_lookup():
    frames = []

    for name in [
        "trade_plan.csv",
        "trading_system_plan.csv",
        "candidates.csv",
        "alpha_candidates.csv",
        "core_candidates.csv",
        "pre_move_candidates.csv",
        "timing_candidates.csv",
    ]:
        df = read_csv_any([ROOT / name, DATA_DIR / name])
        if not df.empty and "stock_id" in df.columns:
            df = df.copy()
            df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
            frames.append(df)

    feature_lookup = load_feature_lookup()

    if not frames:
        return feature_lookup

    all_df = pd.concat(frames, ignore_index=True)
    all_df["stock_id"] = all_df["stock_id"].apply(normalize_stock_id)

    # 如果主表沒有流動性欄位，先補空欄位，再用 feature_lookup 填補。
    for c in ["stock_name", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]:
        if c not in all_df.columns:
            all_df[c] = ""

    for idx, row in all_df.iterrows():
        sid = str(row["stock_id"])
        f = feature_lookup.get(sid, {})
        for c in ["stock_name", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]:
            v = clean_text(all_df.at[idx, c], "")
            if v == "" or v == "0" or v == "0.0":
                if c in f:
                    all_df.at[idx, c] = f[c]

    all_df = all_df.drop_duplicates("stock_id", keep="first")
    out = {str(r["stock_id"]): r.to_dict() for _, r in all_df.iterrows()}

    # feature lookup 也要補進來，讓 PRE/WATCH/BLOCK 都有資料
    for sid, data in feature_lookup.items():
        if sid not in out:
            out[sid] = data

    return out

def pick(row, lookup, col, default=""):
    v = row.get(col, default) if hasattr(row, "get") else default
    v = clean_text(v, "")
    if v != "" and v != "0" and v != "0.0":
        return v

    sid = normalize_stock_id(row.get("stock_id", "")) if hasattr(row, "get") else ""
    src = lookup.get(sid, {})
    return clean_text(src.get(col, default), default)

def norm_action(v):
    s = clean_text(v).strip().upper()
    mapping = {
        "買進": "BUY",
        "試單": "TEST",
        "觀察": "WATCH",
        "禁止": "BLOCK",
        "賣出": "SELL",
        "減碼": "REDUCE",
    }
    return mapping.get(s, s)


def load_market_guard():
    """
    v266.11 市場濾網：
    優先讀 market_regime.json 的大盤狀態。
    若沒有，再 fallback 到 market_snapshot_summary.json 的流動性市場分數。

    BULL / STRONG：BUY / TEST / WATCH 全開
    NEUTRAL / MID：BUY 降級 TEST
    BEAR / WEAK：BUY / TEST 降級 WATCH
    """
    regime_data = {}
    for p in [ROOT / "market_regime.json", DATA_DIR / "market_regime.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                regime_data = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    if regime_data:
        regime = str(regime_data.get("market_regime", "NEUTRAL")).upper()
        label = str(regime_data.get("market_label", "大盤中性"))
        change_text = str(regime_data.get("index_change_pct_text", ""))
        score = float(regime_data.get("market_score", 50) or 50)

        if regime == "BULL":
            mode = "STRONG"
            guard_label = f"{label} {change_text}：BUY / TEST / WATCH 全開"
        elif regime == "BEAR":
            mode = "WEAK"
            guard_label = f"{label} {change_text}：BUY / TEST 降級 WATCH，只觀察"
        else:
            mode = "MID"
            guard_label = f"{label} {change_text}：BUY 降級 TEST，控制追高"

        return {
            "market_guard_mode": mode,
            "market_guard_score": round(score, 2),
            "market_guard_label": guard_label,
            "market_regime": regime,
            "market_label": label,
            "index_change_pct_text": change_text,
            "market_regime_source": regime_data.get("source", ""),
            "market_regime_method": regime_data.get("method", ""),
        }

    # fallback：沒有 market_regime 時，使用流動性市場分數
    summary = {}
    for p in [ROOT / "market_snapshot_summary.json", DATA_DIR / "market_snapshot_summary.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                summary = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    high = int(float(summary.get("high_liquidity_count", 0) or 0))
    mid = int(float(summary.get("medium_liquidity_count", 0) or 0))
    block = int(float(summary.get("block_liquidity_count", 0) or 0))
    score = high * 1.0 + mid * 0.5 - block * 0.7

    if score >= 300:
        mode = "STRONG"
        label = "市場流動性強：BUY / TEST / WATCH 全開"
        regime = "BULL"
    elif score >= 150:
        mode = "MID"
        label = "市場流動性中性：BUY 降級 TEST"
        regime = "NEUTRAL"
    else:
        mode = "WEAK"
        label = "市場流動性弱：BUY / TEST 降級 WATCH"
        regime = "BEAR"

    return {
        "market_guard_mode": mode,
        "market_guard_score": round(score, 2),
        "market_guard_label": label,
        "market_regime": regime,
        "market_label": label,
        "index_change_pct_text": "",
        "market_regime_source": "market_snapshot_summary",
        "market_regime_method": "liquidity_fallback",
    }

def apply_market_guard(out):
    """
    只調整最終動作，不改原始策略分數與資料欄位。
    持倉 EXIT / SELL / REDUCE 不受市場風控影響。
    """
    guard = load_market_guard()

    if out.empty:
        return out, guard

    out = out.copy()
    mode = guard["market_guard_mode"]
    label = guard["market_guard_label"]

    protected = (
        out["source"].astype(str).str.upper().eq("EXIT")
        | out["final_action"].astype(str).str.upper().isin(["SELL", "REDUCE"])
    )

    if mode == "MID":
        mask = (~protected) & out["final_action"].astype(str).str.upper().eq("BUY")
        out.loc[mask, "final_action"] = "TEST"
        out.loc[mask, "priority"] = 3

        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"]
            .astype(str)
            .replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + label)
        )

    elif mode == "WEAK":
        mask = (~protected) & out["final_action"].astype(str).str.upper().isin(["BUY", "TEST"])
        out.loc[mask, "final_action"] = "WATCH"
        out.loc[mask, "priority"] = 8
        out.loc[mask, "suggested_amount"] = 0
        out.loc[mask, "target_weight"] = 0

        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"]
            .astype(str)
            .replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + label)
        )

    return out, guard


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

    # 持倉優先
    if not exitp.empty and "stock_id" in exitp.columns:
        exitp["stock_id"] = exitp["stock_id"].apply(normalize_stock_id)
        holding_ids = set(exitp["stock_id"])

        for _, r in exitp.iterrows():
            raw_action = norm_action(r.get("exit_action", ""))

            if raw_action == "SELL":
                final_action, priority, allowed, note = "SELL", 0, True, "持倉風控：必須優先處理出場"
            elif raw_action == "REDUCE":
                final_action, priority, allowed, note = "REDUCE", 1, True, "持倉風控：建議降倉控風險"
            elif raw_action in ["HOLD", "WATCH"]:
                final_action, priority, allowed, note = "WATCH", 7, False, "持倉觀察：目前不新增、不出場"
            else:
                continue

            sid = normalize_stock_id(r.get("stock_id", ""))
            reason_parts = []

            er = clean_text(r.get("exit_reason", ""))
            if er:
                reason_parts.append(er)

            u = pct_text(r.get("unrealized_pct", ""))
            if u:
                reason_parts.append(f"損益 {u}")

            avg = clean_text(r.get("avg_cost", ""))
            if avg:
                reason_parts.append(f"均價 {avg}")

            lots = clean_text(r.get("lots", ""))
            if lots:
                reason_parts.append(f"張數 {lots}")

            rows.append({
                "final_action": final_action,
                "stock_id": sid,
                "stock_name": pick({"stock_id": sid}, lookup, "stock_name", ""),
                "source": "EXIT",
                "bucket": "POSITION",
                "strategy_type": "POSITION",
                "score": clean_text(r.get("exit_priority", 0)),
                "entry_type": raw_action,
                "execution_flag": raw_action,
                "allowed": allowed,
                "close": clean_text(r.get("close", pick({"stock_id": sid}, lookup, "close", ""))),
                "suggested_amount": clean_text(r.get("position_value", "")),
                "target_weight": "",
                "priority": priority,
                "reason": " | ".join(reason_parts),
                "system_note": f"{note}｜風險 {clean_text(r.get('risk_level', ''))}",
                "liquidity_level": pick({"stock_id": sid}, lookup, "liquidity_level", ""),
                "liquidity_tag": pick({"stock_id": sid}, lookup, "liquidity_tag", ""),
                "liquidity_score": pick({"stock_id": sid}, lookup, "liquidity_score", ""),
                "volume": pick({"stock_id": sid}, lookup, "volume", ""),
                "turnover": pick({"stock_id": sid}, lookup, "turnover", ""),
            })

    # 進場/觀察/禁止
    if not trading.empty and "stock_id" in trading.columns:
        trading["stock_id"] = trading["stock_id"].apply(normalize_stock_id)

        for _, r in trading.iterrows():
            sid = normalize_stock_id(r.get("stock_id", ""))
            if not sid or sid in holding_ids:
                continue

            raw_action = norm_action(r.get("action", r.get("final_action", "")))
            allowed = is_true(r.get("allowed", True))
            strategy_type = pick(r, lookup, "strategy_type", pick(r, lookup, "bucket", ""))
            bucket = pick(r, lookup, "bucket", strategy_type)
            liq = pick(r, lookup, "liquidity_level", "").upper()

            if raw_action in ["BUY", "TEST", "WATCH", "BLOCK"]:
                final_action = raw_action
            else:
                flag = norm_action(r.get("execution_flag", ""))
                if allowed and flag == "TOP":
                    final_action = "BUY" if str(strategy_type).upper() == "ALPHA" else "TEST"
                elif flag == "WATCH":
                    final_action = "WATCH"
                else:
                    final_action = "BLOCK"

            # 實戰保護：流動性不足不可 BUY
            if final_action == "BUY" and liq in ["LOW", "BLOCK", ""]:
                final_action = "TEST" if liq == "LOW" else "BLOCK"

            priority = {"SELL": 0, "REDUCE": 1, "BUY": 2, "TEST": 3, "WATCH": 8, "BLOCK": 9}.get(final_action, 9)

            rows.append({
                "final_action": final_action,
                "stock_id": sid,
                "stock_name": pick(r, lookup, "stock_name", ""),
                "source": pick(r, lookup, "source", "ENTRY"),
                "bucket": bucket,
                "strategy_type": strategy_type,
                "score": pick(r, lookup, "score", pick(r, lookup, "entry_score", "")),
                "entry_type": pick(r, lookup, "action_sub", r.get("entry_type", "")),
                "execution_flag": pick(r, lookup, "execution_flag", raw_action),
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
        out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

        # v266.10.1：最後保險補股票名稱
        # 來源順序：market_snapshot.csv → stock_basic_tw_full.csv → stock_basic.csv
        name_maps = []
        for name_file in ["market_snapshot.csv", "stock_basic_tw_full.csv", "stock_basic.csv"]:
            df_name = read_csv_any([ROOT / name_file, DATA_DIR / name_file])
            if df_name.empty or "stock_id" not in df_name.columns:
                continue

            df_name = df_name.copy()

            if "stock_name" not in df_name.columns:
                for alt in ["name", "證券名稱", "股票名稱", "公司名稱"]:
                    if alt in df_name.columns:
                        df_name["stock_name"] = df_name[alt]
                        break

            if "stock_name" not in df_name.columns:
                continue

            df_name["stock_id"] = df_name["stock_id"].apply(normalize_stock_id)
            df_name["stock_name"] = df_name["stock_name"].astype(str).replace(["nan", "None", "null"], "")
            df_name = df_name[df_name["stock_name"].astype(str).str.strip() != ""]
            if not df_name.empty:
                name_maps.append(
                    df_name[["stock_id", "stock_name"]]
                    .drop_duplicates("stock_id", keep="first")
                    .set_index("stock_id")["stock_name"]
                    .to_dict()
                )

        def fill_stock_name(row):
            cur = clean_text(row.get("stock_name", ""), "")
            if cur not in ["", "--"]:
                return cur
            sid = normalize_stock_id(row.get("stock_id", ""))
            for mp in name_maps:
                v = clean_text(mp.get(sid, ""), "")
                if v not in ["", "--"]:
                    return v
            return ""

        out["stock_name"] = out.apply(fill_stock_name, axis=1)

        out, market_guard = apply_market_guard(out)

        out["_score_num"] = pd.to_numeric(out["score"], errors="coerce").fillna(0)
        out["_priority_num"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9)
        out = out.sort_values(["_priority_num", "_score_num", "stock_id"], ascending=[True, False, True])
        out = out.drop(columns=["_score_num", "_priority_num"])

    if "market_guard" not in locals():
        market_guard = load_market_guard()

    write_csv_both(out, "final_action_plan.csv")

    summary = {
        "generated_at": generated_at,
        "source": "final_decision_engine_v266_11_market_filter",
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
        "backfill_source": "feature_panel_daily.csv",
        "extra_lookup_sources": [
            "trade_plan.csv",
            "trading_system_plan.csv",
            "candidates.csv",
            "alpha_candidates.csv",
            "core_candidates.csv",
            "pre_move_candidates.csv",
            "timing_candidates.csv"
        ],
        "with_name_count": int((out["stock_name"].astype(str).str.strip() != "").sum()) if not out.empty else 0,
        "encoding": "utf-8-sig",
    }

    for p in [ROOT / "final_action_summary.json", DATA_DIR / "final_action_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
