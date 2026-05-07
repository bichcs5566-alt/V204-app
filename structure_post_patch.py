# -*- coding: utf-8 -*-
"""
v266.57.7.1 structure_post_patch.py

放在 GitHub Actions 最後階段執行：
所有 engine / dashboard bridge 都跑完後，再補寫：
- structure_pre_score
- continuation_quality_score
- adjusted_signal_score_v26657_7
- structure_rank_v26657_7

不改原本：
- entry_score
- action
- target_weight
- 持倉
- 資金配置
"""

from pathlib import Path
from datetime import datetime
import json
import re
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"

TARGET_FILES = [
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
    "trade_plan.csv",
    "ignition_candidates.csv",
    "strategy_evolution.csv",
    "selection_debug.csv",
    "pre_move_candidates.csv",
    "top_opportunities.csv",
    "final_action_plan.csv",
]


def taipei_now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sid(v):
    s = str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s


def sf(v, default=np.nan):
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default


def read_csv(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ["utf-8-sig", "utf-8"]:
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()


def write_csv(df, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding="utf-8-sig")


def find_feature_file():
    for p in [
        ROOT / "feature_panel_daily.csv",
        DATA_DIR / "feature_panel_daily.csv",
        ROOT / "features.csv",
        DATA_DIR / "features.csv",
    ]:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def load_feature():
    p = find_feature_file()
    if not p:
        return pd.DataFrame()
    df = read_csv(p)
    print(f"[v266.57.7.1] feature source: {p} rows={len(df)}")
    return df


def normalize_features(df):
    if df.empty or "stock_id" not in df.columns or "date" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["stock_id"] = df["stock_id"].map(sid)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id"]).sort_values(["stock_id", "date"])

    for c in [
        "open", "high", "low", "close", "volume",
        "ma5", "ma10", "ma20", "ma60",
        "mom5", "mom10", "mom20", "mom60",
        "volume_ratio", "ma20_slope",
    ]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def calc_structure_pre_map(feat):
    out = {}
    if feat.empty:
        return out

    for stock_id, g in feat.groupby("stock_id"):
        h = g.tail(80).copy()
        if len(h) < 25:
            continue

        last = h.iloc[-1]
        prev3 = h.iloc[-4] if len(h) >= 4 else h.iloc[0]
        prev10 = h.iloc[-11] if len(h) >= 11 else h.iloc[0]

        close = sf(last.get("close"))
        open_ = sf(last.get("open"))
        high = sf(last.get("high"))
        low = sf(last.get("low"))
        volume = sf(last.get("volume"))
        ma5 = sf(last.get("ma5"))
        ma10 = sf(last.get("ma10"))
        ma20 = sf(last.get("ma20"))
        ma60 = sf(last.get("ma60"))
        ma20_p3 = sf(prev3.get("ma20"))
        ma60_p10 = sf(prev10.get("ma60"))
        mom5 = sf(last.get("mom5"))
        mom10 = sf(last.get("mom10"))
        mom20 = sf(last.get("mom20"))
        vol_ratio = sf(last.get("volume_ratio"))

        if not np.isfinite(close) or close <= 0:
            continue

        g5 = h.tail(5)
        g20 = h.tail(20)
        g40 = h.tail(40) if len(h) >= 40 else h
        g60 = h.tail(60) if len(h) >= 60 else h

        vol5 = pd.to_numeric(g5["volume"], errors="coerce").mean()
        vol20 = pd.to_numeric(g20["volume"], errors="coerce").mean()
        vol20_med = pd.to_numeric(g20["volume"], errors="coerce").median()
        high20 = pd.to_numeric(g20["high"], errors="coerce").max()
        high40 = pd.to_numeric(g40["high"], errors="coerce").max()
        low40 = pd.to_numeric(g40["low"], errors="coerce").min()
        high60 = pd.to_numeric(g60["high"], errors="coerce").max()
        low60 = pd.to_numeric(g60["low"], errors="coerce").min()

        score = 0.0
        reasons = []
        penalties = []

        if np.isfinite(ma20) and close > ma20:
            score += 1.0
            reasons.append("20D站上MA20")
        if np.isfinite(ma20) and np.isfinite(ma20_p3) and ma20 >= ma20_p3:
            score += 1.0
            reasons.append("MA20走平翻揚")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            score += 1.0
            reasons.append("MA5站上MA10")
        if np.isfinite(high20) and close >= high20 * 0.96:
            score += 1.0
            reasons.append("接近20日高")

        if np.isfinite(high40) and np.isfinite(low40) and close > 0 and (high40 - low40) / close <= 0.35:
            score += 1.5
            reasons.append("40D平台收斂")
        if np.isfinite(ma5) and np.isfinite(ma10) and np.isfinite(ma20):
            spread = (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / close
            if spread <= 0.08:
                score += 1.5
                reasons.append("短中均線糾結")
        if len(g40) >= 30:
            prior_low = pd.to_numeric(g40.iloc[:20]["low"], errors="coerce").min()
            recent_low = pd.to_numeric(g40.iloc[-20:]["low"], errors="coerce").min()
            if np.isfinite(prior_low) and np.isfinite(recent_low) and recent_low >= prior_low * 0.96:
                score += 1.0
                reasons.append("40D低點守住")

        if np.isfinite(ma60) and close >= ma60 * 0.96:
            score += 1.0
            reasons.append("接近/站回MA60")
        if np.isfinite(ma60) and np.isfinite(ma60_p10) and ma60 >= ma60_p10 * 0.995:
            score += 1.0
            reasons.append("MA60不再下彎")
        if np.isfinite(high60) and np.isfinite(low60) and high60 > low60:
            if (close - low60) / (high60 - low60) >= 0.45:
                score += 1.0
                reasons.append("站回60D區間中上")

        if np.isfinite(vol5) and np.isfinite(vol20) and vol20 > 0 and vol5 <= vol20 * 0.80:
            score += 1.0
            reasons.append("近5日量縮")
        if np.isfinite(volume) and np.isfinite(vol20_med) and vol20_med > 0:
            vr = volume / vol20_med
            if 1.15 <= vr <= 2.8:
                score += 2.0
                reasons.append("量縮後溫和放量")
            elif vr > 4.0:
                score -= 1.5
                penalties.append("單日爆量偏高")

        if np.isfinite(mom5) and mom5 > 0:
            score += 0.8
            reasons.append("5D動能轉正")
        if np.isfinite(mom10) and mom10 > 0:
            score += 0.8
            reasons.append("10D動能轉正")

        if np.isfinite(ma20) and ma20 > 0 and close > ma20 * 1.22:
            score -= 2.0
            penalties.append("距MA20過遠")
        if np.isfinite(mom20) and mom20 > 0.45:
            score -= 1.5
            penalties.append("20D漲幅過熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and high > low:
            upper = (high - max(open_, close)) / (high - low)
            if upper >= 0.45:
                score -= 2.0
                penalties.append("長上影壓力")
        if np.isfinite(vol_ratio) and vol_ratio >= 5.5:
            score -= 1.0
            penalties.append("成交量異常爆量")

        score = max(-5.0, min(12.0, score))
        if score >= 8:
            grade, stype = "A", "起漲結構強"
        elif score >= 6:
            grade, stype = "B", "起漲結構中"
        elif score >= 4:
            grade, stype = "C", "結構觀察"
        elif score >= 2:
            grade, stype = "D", "結構偏弱"
        else:
            grade, stype = "E", "結構不足"

        if grade in ["A", "B"] and any(("40D" in r or "糾結" in r) for r in reasons):
            hint = "平台壓縮後轉強，可優先觀察起漲/試單。"
        elif grade in ["A", "B"] and any(("60D" in r or "MA60" in r) for r in reasons):
            hint = "長底翻多結構，可偏CORE早期卡位。"
        elif grade in ["A", "B"]:
            hint = "短線轉強結構，可搭配原動能延續。"
        elif penalties:
            hint = "有過熱或假突破壓力，避免追高。"
        else:
            hint = "結構證據不足，保留原策略但降低信心。"

        out[stock_id] = {
            "structure_pre_score": round(float(score), 2),
            "structure_pre_grade": grade,
            "structure_pre_type": stype,
            "structure_pre_reason": "｜".join(reasons + (["扣分:" + "、".join(penalties)] if penalties else [])),
            "structure_pre_hint": hint,
            "structure_pre_patch_version": "v266.57.7.1",
        }
    return out


def calc_continuation_quality_map(feat):
    out = {}
    if feat.empty:
        return out

    for stock_id, g in feat.groupby("stock_id"):
        h = g.tail(35).copy()
        if len(h) < 12:
            continue

        last = h.iloc[-1]
        close = sf(last.get("close"))
        open_ = sf(last.get("open"))
        high = sf(last.get("high"))
        low = sf(last.get("low"))
        ma5 = sf(last.get("ma5"))
        ma10 = sf(last.get("ma10"))
        ma20 = sf(last.get("ma20"))
        mom5 = sf(last.get("mom5"))
        mom10 = sf(last.get("mom10"))
        mom20 = sf(last.get("mom20"))
        vol_ratio = sf(last.get("volume_ratio"))

        if not np.isfinite(close) or close <= 0:
            continue

        g5 = h.tail(5)
        g20 = h.tail(20)
        recent_low5 = pd.to_numeric(g5["low"], errors="coerce").min()
        prior_low10 = pd.to_numeric(h.iloc[-15:-5]["low"], errors="coerce").min() if len(h) >= 15 else np.nan
        vol5 = pd.to_numeric(g5["volume"], errors="coerce").mean()
        vol20 = pd.to_numeric(g20["volume"], errors="coerce").mean()
        high20 = pd.to_numeric(g20["high"], errors="coerce").max()

        score = 0.0
        reasons = []
        penalties = []

        if np.isfinite(recent_low5) and np.isfinite(prior_low10) and recent_low5 >= prior_low10 * 0.97:
            score += 2.0
            reasons.append("回檔不破前低")
        if np.isfinite(ma5) and close >= ma5 * 0.995:
            score += 1.5
            reasons.append("收盤守MA5")
        elif np.isfinite(ma10) and close >= ma10 * 0.995:
            score += 0.8
            reasons.append("回測守MA10")
        else:
            score -= 1.0
            penalties.append("跌破短均")

        if np.isfinite(vol5) and np.isfinite(vol20) and vol20 > 0:
            vr5 = vol5 / vol20
            if 0.55 <= vr5 <= 0.95:
                score += 1.5
                reasons.append("回檔量縮")
            elif vr5 > 1.8:
                score -= 1.2
                penalties.append("回檔量放大")

        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            score += 1.2
            reasons.append("MA5仍在MA10上")
        if np.isfinite(ma20) and close >= ma20:
            score += 1.0
            reasons.append("仍站MA20")
        if np.isfinite(mom10) and mom10 > 0:
            score += 1.0
            reasons.append("10D動能維持")
        if np.isfinite(mom5) and mom5 < -0.08:
            score -= 1.2
            penalties.append("短線急殺")

        if np.isfinite(high20) and high20 > 0:
            if close / high20 >= 0.98 and np.isfinite(mom5) and mom5 > 0.08:
                score -= 1.0
                penalties.append("接近20日高且短線過熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and high > low:
            upper = (high - max(open_, close)) / (high - low)
            if upper >= 0.45:
                score -= 1.5
                penalties.append("長上影壓力")
        if np.isfinite(vol_ratio) and vol_ratio >= 5.5:
            score -= 1.0
            penalties.append("異常爆量")
        if np.isfinite(mom20) and mom20 > 0.45:
            score -= 1.2
            penalties.append("20D漲幅偏熱")

        score = max(-5.0, min(10.0, score))
        if score >= 7:
            grade, label = "A", "續強品質強"
        elif score >= 5:
            grade, label = "B", "續強品質中"
        elif score >= 3:
            grade, label = "C", "續強觀察"
        elif score >= 1:
            grade, label = "D", "續強偏弱"
        else:
            grade, label = "E", "續強不足"

        hint = "回檔承接尚可，若原策略入選，可優先觀察延續。" if grade in ["A", "B"] else ("續強品質不足或有假突破壓力，避免追高。" if penalties else "尚未看到明確回檔承接，保留觀察。")

        out[stock_id] = {
            "continuation_quality_score": round(float(score), 2),
            "continuation_quality_grade": grade,
            "continuation_quality_type": label,
            "continuation_quality_reason": "｜".join(reasons + (["扣分:" + "、".join(penalties)] if penalties else [])),
            "continuation_quality_hint": hint,
            "continuation_quality_patch_version": "v266.57.7.1",
        }
    return out


def pick_score_col(df):
    for c in ["entry_score", "score", "total_score", "final_score", "momentum_score", "rank_score", "composite_score"]:
        if c in df.columns:
            return c
    return None


def strategy_bucket(row):
    txt = " ".join(str(row.get(c, "")) for c in ["strategy_type", "strategy_name", "bucket", "source", "action_sub", "entry_type", "execution_flag"]).upper()
    if "ALPHA" in txt:
        return "ALPHA"
    if "IGNITION" in txt or "起漲" in txt:
        return "IGNITION"
    if "EVOLUTION" in txt or "進化" in txt:
        return "EVOLUTION"
    if "CORE" in txt:
        return "CORE"
    if "TEST" in txt or "試單" in txt:
        return "TEST"
    return "GENERIC"


def structure_weight(bucket):
    return {"CORE": 0.95, "IGNITION": 1.05, "EVOLUTION": 0.65, "TEST": 0.80, "ALPHA": 0.35}.get(bucket, 0.55)


def continuation_weight(bucket):
    return {"ALPHA": 0.75, "EVOLUTION": 0.85, "CORE": 0.55, "IGNITION": 0.45, "TEST": 0.50}.get(bucket, 0.50)


def enrich_csv(path, structure_map, quality_map):
    df = read_csv(path)
    if df.empty or "stock_id" not in df.columns:
        return False, 0

    df = df.copy()
    sids = df["stock_id"].map(sid)

    for col in ["structure_pre_score", "structure_pre_grade", "structure_pre_type", "structure_pre_reason", "structure_pre_hint", "structure_pre_patch_version"]:
        df[col] = [structure_map.get(x, {}).get(col, "") for x in sids]
    for col in ["continuation_quality_score", "continuation_quality_grade", "continuation_quality_type", "continuation_quality_reason", "continuation_quality_hint", "continuation_quality_patch_version"]:
        df[col] = [quality_map.get(x, {}).get(col, "") for x in sids]

    base_col = pick_score_col(df)
    base_score = pd.to_numeric(df[base_col], errors="coerce").fillna(0) if base_col else pd.Series([0] * len(df))
    structure_pre = pd.to_numeric(df["structure_pre_score"], errors="coerce").fillna(0)
    continuation_q = pd.to_numeric(df["continuation_quality_score"], errors="coerce").fillna(0)

    buckets = df.apply(strategy_bucket, axis=1)
    s_weights = buckets.map(structure_weight).astype(float)
    c_weights = buckets.map(continuation_weight).astype(float)

    df["strategy_bucket_v26657_7"] = buckets
    df["structure_weight_v26657_7"] = s_weights.round(2)
    df["continuation_weight_v26657_7"] = c_weights.round(2)
    df["adjusted_signal_score_v26657_7"] = (base_score + structure_pre * s_weights + continuation_q * c_weights).round(3)
    df["structure_rank_v26657_7"] = pd.to_numeric(df["adjusted_signal_score_v26657_7"], errors="coerce").rank(ascending=False, method="min")
    df["adjusted_signal_note_v26657_7"] = "最後後處理補寫：原分數+結構前置分*策略權重+續強品質*策略權重；不覆蓋原策略"

    def append_note(row):
        parts = []
        h1 = str(row.get("structure_pre_hint", "")).strip()
        h2 = str(row.get("continuation_quality_hint", "")).strip()
        if h1:
            parts.append("結構：" + h1)
        if h2:
            parts.append("續強：" + h2)
        return "｜".join(parts)

    if "system_note" in df.columns:
        df["system_note"] = df.apply(lambda r: str(r.get("system_note", "")) + ("｜v266.57.7.1：" + append_note(r) if append_note(r) else ""), axis=1)
    elif "note" in df.columns:
        df["note"] = df.apply(lambda r: str(r.get("note", "")) + ("｜v266.57.7.1：" + append_note(r) if append_note(r) else ""), axis=1)

    write_csv(df, path)
    return True, len(df)


def main():
    feat = normalize_features(load_feature())
    report = {
        "version": "v266.57.7.1",
        "mode": "post_process_structure_continuation_patch",
        "changed_strategy_logic": False,
        "changed_original_score": False,
        "changed_action": False,
        "changed_position": False,
        "feature_rows": int(len(feat)),
        "files": {},
        "updated_at": taipei_now_str(),
        "description": "在所有 engine 結束後最後補寫結構前置分、續強品質分與測試排序分，避免被後續流程覆蓋。",
    }

    if feat.empty:
        report["warning"] = "feature file missing or empty; patch skipped"
    else:
        structure_map = calc_structure_pre_map(feat)
        quality_map = calc_continuation_quality_map(feat)
        report["structure_stock_count"] = len(structure_map)
        report["continuation_stock_count"] = len(quality_map)

        for name in TARGET_FILES:
            for base in [ROOT, DATA_DIR]:
                p = base / name
                ok, n = enrich_csv(p, structure_map, quality_map)
                if ok:
                    report["files"][str(p)] = n
                    print(f"[v266.57.7.1] enriched {p} rows={n}")

    for p in [ROOT / "structure_post_patch_report.json", DATA_DIR / "structure_post_patch_report.json"]:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        except Exception as e:
            print(f"[v266.57.7.1] write report failed {p}: {e}")

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
