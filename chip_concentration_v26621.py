# -*- coding: utf-8 -*-
"""
v266.21 籌碼可用版

核心：
- 先讓籌碼集中度真正可用，不再大量 fallback 50。
- 優先吃「三大法人」與「融資融券」。
- 外資持股、投信、大戶/董監欄位保留，抓不到就降權。
- 支援多種可能欄位名稱，避免因欄位名不同造成假性沒資料。

輸出欄位：
chip_score
chip_label
chip_display
chip_reason
chip_hint
chip_valid_count
chip_missing
chip_confidence
"""

from __future__ import annotations

import math
import re
import numpy as np
import pandas as pd


def _norm_col(c: str) -> str:
    return str(c).strip().lower().replace(" ", "").replace("-", "_")


def _build_colmap(row) -> dict:
    return {_norm_col(k): k for k in row.index}


def _get(row, names, default=None):
    cmap = _build_colmap(row)
    for name in names:
        key = _norm_col(name)
        if key in cmap:
            v = row.get(cmap[key])
            if _valid(v):
                return v
    return default


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
        s = str(v).replace(",", "").replace("%", "").replace("張", "").replace("股", "").strip()
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _to_lot_if_needed(v) -> float:
    """
    台股成交量有些資料是「股」，有些是「張」。
    若數值超大，推定為股，轉成張。
    """
    x = _num(v, 0.0)
    if abs(x) >= 1_000_000:
        return x / 1000.0
    return x


def chip_label(score: float) -> str:
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


def chip_hint(score: float, confidence: str) -> str:
    score = _num(score)
    if "資料不足" in confidence:
        return "籌碼資料不足，只能當輔助，不可重倉。"
    if score >= 80:
        return "籌碼高度集中，主力/資金共識強，可搭配技術面優先觀察。"
    if score >= 60:
        return "籌碼偏集中，有資金進場跡象，可小量試單或觀察續強。"
    if score >= 40:
        return "籌碼普通，尚未形成明顯優勢，需搭配技術面確認。"
    if score >= 20:
        return "籌碼偏分散，資金共識不足，建議降低部位或等待確認。"
    return "籌碼極度分散，不具主力優勢，避免追高或重倉。"


def chip_confidence(valid_count: int) -> str:
    if valid_count >= 4:
        return "📊 高信心"
    if valid_count >= 2:
        return "⚠️ 中信心"
    return "📉 資料不足"


def _score_three_major(row):
    """
    三大法人 / 法人買賣超

    支援欄位範例：
    - inst_buy_days / institutional_buy_days / three_major_buy_days / 法人連買天數
    - inst_net_buy / institutional_net_buy / three_major_net_buy / 三大法人買賣超
    - foreign_net_buy / trust_net_buy / dealer_net_buy
    - 外資買賣超 / 投信買賣超 / 自營商買賣超
    """
    buy_days = _num(_get(row, [
        "inst_buy_days", "institutional_buy_days", "three_major_buy_days",
        "法人連買天數", "三大法人連買天數", "連買天數"
    ], 0), 0)

    inst_net = _get(row, [
        "inst_net_buy", "institutional_net_buy", "three_major_net_buy",
        "三大法人買賣超", "三大法人買超", "法人買賣超", "法人買超"
    ], None)

    if inst_net is None:
        foreign = _num(_get(row, ["foreign_net_buy", "foreign_buy", "外資買賣超", "外資買超"], 0), 0)
        trust = _num(_get(row, ["trust_net_buy", "investment_trust_net_buy", "投信買賣超", "投信買超"], 0), 0)
        dealer = _num(_get(row, ["dealer_net_buy", "dealer_buy", "自營商買賣超", "自營商買超"], 0), 0)
        inst_net_num = foreign + trust + dealer
        has_net = any(abs(x) > 0 for x in [foreign, trust, dealer])
    else:
        inst_net_num = _num(inst_net, 0)
        has_net = True

    if buy_days == 0 and not has_net:
        return None, "法人資料不足"

    score = 50
    reason = []

    if buy_days >= 5:
        score += 30
        reason.append("法人連買5日以上")
    elif buy_days >= 3:
        score += 22
        reason.append("法人連買3日")
    elif buy_days >= 1:
        score += 10
        reason.append("法人轉買")
    else:
        reason.append("法人未連買")

    if inst_net_num > 0:
        score += min(20, 8 + abs(inst_net_num) / 1000)
        reason.append("法人買超")
    elif inst_net_num < 0:
        score -= min(25, 10 + abs(inst_net_num) / 1000)
        reason.append("法人賣超")

    return max(0, min(100, score)), "｜".join(reason)


def _score_trust(row):
    buy_days = _num(_get(row, [
        "trust_buy_days", "investment_trust_buy_days", "投信連買天數"
    ], 0), 0)

    net = _get(row, [
        "trust_net_buy", "investment_trust_net_buy", "trust_buy",
        "投信買賣超", "投信買超"
    ], None)

    if buy_days == 0 and net is None:
        return None, "投信資料不足"

    net_num = _num(net, 0)
    score = 50
    reason = []

    if buy_days >= 5:
        score += 30
        reason.append("投信連買5日以上")
    elif buy_days >= 3:
        score += 22
        reason.append("投信連買3日")
    elif buy_days >= 1:
        score += 10
        reason.append("投信轉買")

    if net_num > 0:
        score += min(20, 8 + abs(net_num) / 500)
        reason.append("投信買超")
    elif net_num < 0:
        score -= min(25, 10 + abs(net_num) / 500)
        reason.append("投信賣超")

    if not reason:
        reason.append("投信持平")

    return max(0, min(100, score)), "｜".join(reason)


def _score_foreign_holding(row):
    delta = _get(row, [
        "foreign_holding_change", "foreign_hold_pct_change", "foreign_holding_pct_delta",
        "foreign_pct_delta", "外資持股變化", "外資持股比例變化"
    ], None)

    if delta is None:
        return None, "外資持股資料不足"

    d = _num(delta, 0)
    if d >= 1.0:
        return 90, "外資持股明顯提升"
    if d >= 0.3:
        return 75, "外資持股提升"
    if d > 0:
        return 62, "外資持股小幅提升"
    if d <= -1.0:
        return 20, "外資持股明顯下降"
    if d <= -0.3:
        return 35, "外資持股下降"
    return 50, "外資持股持平"


def _score_margin(row):
    """
    融資融券：
    - 融資下降：籌碼較乾淨，加分
    - 融資暴增：散戶追高風險，扣分
    - 融券增加：可能有軋空燃料，小加分
    """
    margin_delta = _get(row, [
        "margin_balance_change", "margin_change", "margin_delta", "margin_financing_delta",
        "融資變化", "融資餘額增減", "融資增減"
    ], None)

    short_delta = _get(row, [
        "short_balance_change", "short_change", "short_delta", "securities_lending_delta",
        "融券變化", "融券餘額增減", "融券增減"
    ], None)

    if margin_delta is None and short_delta is None:
        return None, "融資融券資料不足"

    score = 50
    reason = []

    if margin_delta is not None:
        md = _to_lot_if_needed(margin_delta)
        if md < -500:
            score += 25
            reason.append("融資明顯下降")
        elif md < 0:
            score += 15
            reason.append("融資下降")
        elif md > 1000:
            score -= 25
            reason.append("融資暴增")
        elif md > 0:
            score -= 10
            reason.append("融資增加")
        else:
            reason.append("融資持平")

    if short_delta is not None:
        sd = _to_lot_if_needed(short_delta)
        if sd > 0:
            score += 8
            reason.append("融券增加")
        elif sd < 0:
            score += 3
            reason.append("融券回補")
        else:
            reason.append("融券持平")

    return max(0, min(100, score)), "｜".join(reason)


def _score_major_holder(row):
    delta = _get(row, [
        "major_holder_change", "big_holder_change", "major_holder_pct_change",
        "director_holding_change", "insider_holding_change", "director_pct_change",
        "大戶持股變化", "董監持股變化"
    ], None)

    if delta is None:
        return None, "大戶/董監資料不足"

    d = _num(delta, 0)
    if d >= 1.0:
        return 88, "大戶/董監持股明顯增加"
    if d >= 0.2:
        return 72, "大戶/董監持股增加"
    if d > -0.2:
        return 55, "大戶/董監持股穩定"
    if d <= -1.0:
        return 20, "大戶/董監持股明顯下降"
    return 35, "大戶/董監持股下降"


def calc_chip_score(row) -> dict:
    """
    v266.21 權重：
    - 三大法人 40%
    - 融資融券 30%
    - 投信 15%
    - 外資持股 10%
    - 大戶/董監 5%

    原因：
    現階段先讓可每日抓的資料權重提高；
    慢資料或常缺資料保留但不讓它拖垮系統。
    """
    parts = [
        ("三大法人", 0.40, _score_three_major(row)),
        ("融資融券", 0.30, _score_margin(row)),
        ("投信", 0.15, _score_trust(row)),
        ("外資持股", 0.10, _score_foreign_holding(row)),
        ("大戶/董監", 0.05, _score_major_holder(row)),
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
            if reason:
                reasons.append(reason)

    if not valid:
        score = 50.0
        conf = chip_confidence(0)
        label = chip_label(score)
        return {
            "chip_score": round(score, 2),
            "chip_label": label,
            "chip_display": f"{round(score):.0f}（{label}）",
            "chip_reason": "籌碼資料不足",
            "chip_hint": chip_hint(score, conf),
            "chip_valid_count": 0,
            "chip_missing": "、".join(missing),
            "chip_confidence": conf,
        }

    total_weight = sum(w for _, w, _ in valid)
    score = sum((w / total_weight) * s for _, w, s in valid)
    score = max(0, min(100, float(score)))

    conf = chip_confidence(len(valid))
    label = chip_label(score)

    reason = "｜".join(reasons)
    if missing:
        reason += "｜缺資料：" + "、".join(missing)

    return {
        "chip_score": round(score, 2),
        "chip_label": label,
        "chip_display": f"{round(score):.0f}（{label}）",
        "chip_reason": reason,
        "chip_hint": chip_hint(score, conf),
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
