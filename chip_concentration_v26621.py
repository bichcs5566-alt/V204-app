# -*- coding: utf-8 -*-
"""
v266.23.3 chip_concentration_v26621.py 兼容覆蓋版

用途：
- 保留 final_decision_engine.py 原本 import 名稱：chip_concentration_v26621
- 實際改成讀取 TWSE 產出的 chip_source_twse.csv
- 解決畫面一直顯示「籌碼資料不足 / 50 普通」的問題

需要存在：
- chip_source_twse.csv
或
- mobile_dashboard_v1/data/chip_source_twse.csv
"""

from __future__ import annotations

from pathlib import Path
import math
import re
import pandas as pd


_CHIP_SOURCE_CACHE = None


def _valid(v) -> bool:
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except Exception:
        pass
    s = str(v).strip()
    return s not in ("", "--", "-", "nan", "NaN", "None", "null")


def _num(v, default=0.0) -> float:
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


def _stock_id_from_any(v) -> str:
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else ""


def _stock_id_from_row(row) -> str:
    for c in ["stock_id", "symbol", "code", "個股", "股票代號"]:
        if c in row.index and _valid(row.get(c)):
            sid = _stock_id_from_any(row.get(c))
            if sid:
                return sid
    return ""


def _read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path)


def _load_chip_source() -> pd.DataFrame:
    global _CHIP_SOURCE_CACHE
    if _CHIP_SOURCE_CACHE is not None:
        return _CHIP_SOURCE_CACHE

    candidates = [
        Path("mobile_dashboard_v1/data/chip_source_twse.csv"),
        Path("chip_source_twse.csv"),
        Path("mobile_dashboard_v1/data/chip_source_free.csv"),
        Path("chip_source_free.csv"),
    ]

    for p in candidates:
        if p.exists():
            try:
                df = _read_csv_safe(p)
                if "stock_id" in df.columns:
                    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})")[0]
                    df = df.dropna(subset=["stock_id"])
                _CHIP_SOURCE_CACHE = df
                return _CHIP_SOURCE_CACHE
            except Exception:
                continue

    _CHIP_SOURCE_CACHE = pd.DataFrame()
    return _CHIP_SOURCE_CACHE


def _merge_chip_source(row):
    base = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    sid = _stock_id_from_row(row)

    src = _load_chip_source()
    if sid and not src.empty and "stock_id" in src.columns:
        hit = src[src["stock_id"].astype(str) == sid]
        if not hit.empty:
            extra = hit.iloc[-1].to_dict()
            for k, v in extra.items():
                if not _valid(base.get(k)):
                    base[k] = v

    return pd.Series(base)


def _label(score: float) -> str:
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


def _confidence(valid_count: int) -> str:
    if valid_count >= 3:
        return "📊 高信心"
    if valid_count >= 2:
        return "⚠️ 中信心"
    if valid_count >= 1:
        return "📉 低信心"
    return "📉 資料不足"


def _hint(score: float, conf: str) -> str:
    score = _num(score)
    if "資料不足" in conf:
        return "籌碼資料不足，只能當輔助，不可重倉。"
    if "低信心" in conf:
        return "目前只有部分籌碼資料，僅可輔助判斷，不可單獨重倉。"
    if score >= 80:
        return "籌碼高度集中，資金共識強，可搭配技術面優先觀察。"
    if score >= 60:
        return "籌碼偏集中，有資金進場跡象，可小量試單或觀察續強。"
    if score >= 40:
        return "籌碼普通，尚未形成明顯優勢，需搭配技術面確認。"
    if score >= 20:
        return "籌碼偏分散，資金共識不足，建議降低部位或等待確認。"
    return "籌碼極度分散，不具主力優勢，避免追高或重倉。"


def _score_inst(row):
    inst_valid = _num(row.get("inst_valid", 0), 0)
    foreign = _num(row.get("foreign_net_buy", 0), 0)
    trust = _num(row.get("trust_net_buy", 0), 0)
    dealer = _num(row.get("dealer_net_buy", 0), 0)
    inst = _num(row.get("inst_net_buy", foreign + trust + dealer), foreign + trust + dealer)
    buy_days = _num(row.get("inst_buy_days", 0), 0)

    if inst_valid <= 0 and foreign == 0 and trust == 0 and dealer == 0 and inst == 0:
        return None, "三大法人資料不足"

    score = 50.0
    reasons = []

    if buy_days >= 5:
        score += 25
        reasons.append("法人連買5日以上")
    elif buy_days >= 3:
        score += 18
        reasons.append("法人連買3日")
    elif buy_days >= 1:
        score += 8
        reasons.append("法人單日買超")
    else:
        reasons.append("法人未連買")

    if inst > 0:
        score += min(25, 8 + abs(inst) / 2000)
        reasons.append("三大法人買超")
    elif inst < 0:
        score -= min(30, 10 + abs(inst) / 2000)
        reasons.append("三大法人賣超")

    if trust > 0:
        score += min(12, 5 + abs(trust) / 1000)
        reasons.append("投信買超")
    elif trust < 0:
        score -= min(12, 5 + abs(trust) / 1000)
        reasons.append("投信賣超")

    return max(0, min(100, score)), "｜".join(reasons)


def _score_margin(row):
    margin_valid = _num(row.get("margin_valid", 0), 0)
    margin_chg = _num(row.get("margin_balance_change", 0), 0)
    short_chg = _num(row.get("short_balance_change", 0), 0)
    margin_bal = _num(row.get("margin_balance", 0), 0)
    short_bal = _num(row.get("short_balance", 0), 0)

    if margin_valid <= 0 and margin_chg == 0 and short_chg == 0 and margin_bal == 0 and short_bal == 0:
        return None, "融資融券資料不足"

    score = 50.0
    reasons = []

    if margin_chg < -500:
        score += 22
        reasons.append("融資明顯下降")
    elif margin_chg < 0:
        score += 12
        reasons.append("融資下降")
    elif margin_chg > 1000:
        score -= 22
        reasons.append("融資暴增")
    elif margin_chg > 0:
        score -= 10
        reasons.append("融資增加")
    else:
        reasons.append("融資變化不足或尚未計算")

    if short_chg > 0:
        score += 6
        reasons.append("融券增加")
    elif short_chg < 0:
        score += 3
        reasons.append("融券回補")

    return max(0, min(100, score)), "｜".join(reasons)


def calc_chip_score(row) -> dict:
    row = _merge_chip_source(row)

    parts = [
        ("三大法人", 0.70, _score_inst(row)),
        ("融資融券", 0.30, _score_margin(row)),
    ]

    valid = []
    reasons = []
    missing = []

    for name, weight, result in parts:
        score, reason = result
        if score is None:
            missing.append(name)
        else:
            valid.append((name, weight, score))
            reasons.append(reason)

    if not valid:
        score = 50.0
        conf = _confidence(0)
        label = _label(score)
        return {
            "chip_score": round(score, 2),
            "chip_label": label,
            "chip_display": f"{round(score):.0f}（{label}）",
            "chip_reason": "籌碼資料不足",
            "chip_hint": _hint(score, conf),
            "chip_valid_count": 0,
            "chip_missing": "、".join(missing),
            "chip_confidence": conf,
        }

    total_weight = sum(w for _, w, _ in valid)
    score = sum((w / total_weight) * s for _, w, s in valid)
    score = max(0, min(100, float(score)))

    conf = _confidence(len(valid))
    label = _label(score)

    reason = "｜".join([r for r in reasons if r])
    if missing:
        reason += "｜缺資料：" + "、".join(missing)

    return {
        "chip_score": round(score, 2),
        "chip_label": label,
        "chip_display": f"{round(score):.0f}（{label}）",
        "chip_reason": reason,
        "chip_hint": _hint(score, conf),
        "chip_valid_count": len(valid),
        "chip_missing": "、".join(missing),
        "chip_confidence": conf,
    }


def add_chip_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    results = out.apply(lambda r: calc_chip_score(r), axis=1)

    out["chip_score"] = results.apply(lambda x: x["chip_score"])
    out["chip_label"] = results.apply(lambda x: x["chip_label"])
    out["chip_display"] = results.apply(lambda x: x["chip_display"])
    out["chip_reason"] = results.apply(lambda x: x["chip_reason"])
    out["chip_hint"] = results.apply(lambda x: x["chip_hint"])
    out["chip_valid_count"] = results.apply(lambda x: x["chip_valid_count"])
    out["chip_missing"] = results.apply(lambda x: x["chip_missing"])
    out["chip_confidence"] = results.apply(lambda x: x["chip_confidence"])

    return out


def get_chip_score(stock_id):
    sid = _stock_id_from_any(stock_id)
    row = pd.Series({"stock_id": sid})
    r = calc_chip_score(row)
    return {
        "score": r["chip_score"],
        "label": r["chip_label"],
        "display": r["chip_display"],
        "reason": r["chip_reason"],
        "hint": r["chip_hint"],
        "confidence": r["chip_confidence"],
    }
