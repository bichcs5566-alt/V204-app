# -*- coding: utf-8 -*-
"""
chip_concentration_v26621.py
v266.23.8 救援覆蓋版
"""

from pathlib import Path
import re
import math
import pandas as pd


def _valid(v):
    try:
        if v is None or pd.isna(v):
            return False
    except Exception:
        if v is None:
            return False
    return str(v).strip() not in ("", "--", "-", "nan", "NaN", "None", "null")


def _num(v, default=0.0):
    try:
        if not _valid(v):
            return default
        s = str(v).replace(",", "").replace("+", "").replace("%", "").replace("張", "").replace("股", "").strip()
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _sid(v):
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else ""


def _row_sid(row):
    for c in ["stock_id", "symbol", "code", "個股", "股票代號"]:
        if c in row.index and _valid(row.get(c)):
            s = _sid(row.get(c))
            if s:
                return s
    return ""


def _read_csv(p):
    try:
        return pd.read_csv(p, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(p)


def _load_twse():
    for p in [
        Path("mobile_dashboard_v1/data/chip_source_twse.csv"),
        Path("chip_source_twse.csv"),
    ]:
        if p.exists():
            try:
                df = _read_csv(p)
                if "stock_id" in df.columns:
                    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})")[0]
                    df = df.dropna(subset=["stock_id"])
                return df
            except Exception:
                pass
    return pd.DataFrame()


def _label(score):
    score = _num(score)
    if score >= 80:
        return "🔥 高度集中"
    if score >= 60:
        return "🟢 偏集中"
    if score >= 40:
        return "🟡 普通"
    if score >= 20:
        return "⚠️ 分散"
    return "❌ 極度分散"


def _score_from_row(row):
    foreign = _num(row.get("foreign_net_buy", 0))
    trust = _num(row.get("trust_net_buy", 0))
    dealer = _num(row.get("dealer_net_buy", 0))
    inst = _num(row.get("inst_net_buy", foreign + trust + dealer), foreign + trust + dealer)
    inst_valid = _num(row.get("inst_valid", 0))

    margin_valid = _num(row.get("margin_valid", 0))
    margin_chg = _num(row.get("margin_balance_change", 0))
    short_chg = _num(row.get("short_balance_change", 0))

    valid_count = 0
    score = 50.0
    reasons = []

    if inst_valid > 0 or foreign != 0 or trust != 0 or dealer != 0 or inst != 0:
        valid_count += 1
        if inst > 0:
            score += min(25, 8 + abs(inst) / 2000)
            reasons.append("三大法人買超")
        elif inst < 0:
            score -= min(25, 8 + abs(inst) / 2000)
            reasons.append("三大法人賣超")
        else:
            reasons.append("法人中性")

        if trust > 0:
            score += min(10, 4 + abs(trust) / 1000)
            reasons.append("投信買超")
        elif trust < 0:
            score -= min(10, 4 + abs(trust) / 1000)
            reasons.append("投信賣超")

    if margin_valid > 0:
        valid_count += 1
        if margin_chg < 0:
            score += 8
            reasons.append("融資下降")
        elif margin_chg > 0:
            score -= 6
            reasons.append("融資增加")
        else:
            reasons.append("融資變化尚未計算")

        if short_chg > 0:
            score += 3
            reasons.append("融券增加")

    score = max(0, min(100, score))
    label = _label(score)

    if valid_count == 0:
        return {
            "chip_score": 50,
            "chip_label": "🟡 普通",
            "chip_display": "50（🟡 普通）",
            "chip_reason": "籌碼資料不足",
            "chip_hint": "籌碼資料不足，只能當輔助，不可重倉。",
            "chip_valid_count": 0,
            "chip_missing": "三大法人、融資融券",
            "chip_confidence": "📉 資料不足",
        }

    confidence = "⚠️ 中信心" if valid_count >= 2 else "📉 低信心"
    return {
        "chip_score": round(score, 2),
        "chip_label": label,
        "chip_display": f"{round(score):.0f}（{label}）",
        "chip_reason": "｜".join(reasons) if reasons else "籌碼中性",
        "chip_hint": "籌碼偏集中，有資金進場跡象，可搭配技術面確認。" if score >= 60 else "籌碼普通或資料有限，需搭配技術面確認。",
        "chip_valid_count": valid_count,
        "chip_missing": "" if valid_count >= 2 else "融資融券或法人其中一項不足",
        "chip_confidence": confidence,
    }


def add_chip_columns(df):
    if df is None or len(df) == 0:
        return df

    out = df.copy()
    src = _load_twse()
    out["_chip_sid"] = out.apply(_row_sid, axis=1)

    if not src.empty and "stock_id" in src.columns:
        src = src.drop_duplicates(subset=["stock_id"], keep="last").copy()
        out = out.merge(src, left_on="_chip_sid", right_on="stock_id", how="left", suffixes=("", "_chip_src"))
        if "stock_id_chip_src" in out.columns:
            out.drop(columns=["stock_id_chip_src"], inplace=True, errors="ignore")

    results = out.apply(_score_from_row, axis=1)

    out["chip_score"] = results.apply(lambda x: x["chip_score"])
    out["chip_label"] = results.apply(lambda x: x["chip_label"])
    out["chip_display"] = results.apply(lambda x: x["chip_display"])
    out["chip_reason"] = results.apply(lambda x: x["chip_reason"])
    out["chip_hint"] = results.apply(lambda x: x["chip_hint"])
    out["chip_valid_count"] = results.apply(lambda x: x["chip_valid_count"])
    out["chip_missing"] = results.apply(lambda x: x["chip_missing"])
    out["chip_confidence"] = results.apply(lambda x: x["chip_confidence"])

    out.drop(columns=["_chip_sid"], inplace=True, errors="ignore")
    return out


def get_chip_score(stock_id):
    src = _load_twse()
    sid = _sid(stock_id)
    if not src.empty and "stock_id" in src.columns:
        hit = src[src["stock_id"].astype(str) == sid]
        if not hit.empty:
            r = _score_from_row(hit.iloc[-1])
            return {
                "score": r["chip_score"],
                "label": r["chip_label"],
                "display": r["chip_display"],
                "reason": r["chip_reason"],
                "hint": r["chip_hint"],
                "confidence": r["chip_confidence"],
            }

    return {
        "score": 50,
        "label": "🟡 普通",
        "display": "50（🟡 普通）",
        "reason": "籌碼資料不足",
        "hint": "籌碼資料不足，只能當輔助，不可重倉。",
        "confidence": "📉 資料不足",
    }
