# -*- coding: utf-8 -*-
"""
v266.20 完整B：籌碼集中度完整接入模組

用途：
- 可直接 import 到 final_decision_engine / data_pipeline
- 支援五類籌碼：
  1. 三大法人連買 / 買超集中度
  2. 外資持股比例變化
  3. 投信買超集中度
  4. 大戶持股 / 董監持股變化（慢速資料）
  5. 融資融券變化

設計原則：
- 有資料就計分
- 沒資料就降權，不讓系統中斷
- 最終輸出：
  chip_score
  chip_label
  chip_display
  chip_reason
  chip_hint
"""

from __future__ import annotations

import math
import pandas as pd
import numpy as np


def _num(v, default=0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
            if v in ("", "--", "nan", "None"):
                return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _has(row, key: str) -> bool:
    try:
        v = row.get(key, None)
        return v is not None and str(v).strip() not in ("", "--", "nan", "None")
    except Exception:
        return False


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


def chip_hint(score: float) -> str:
    score = _num(score)
    if score >= 80:
        return "主力資金集中，籌碼結構偏多，可搭配趨勢操作。"
    if score >= 60:
        return "籌碼偏集中，有資金進場跡象，可小量試單或觀察續強。"
    if score >= 40:
        return "籌碼普通，尚未形成明顯優勢，需搭配技術面確認。"
    if score >= 20:
        return "籌碼偏分散，資金共識不足，建議降低部位或等待確認。"
    return "籌碼極度分散，不具主力優勢，避免追高或重倉。"


def _score_inst(row):
    """
    三大法人：連買 / 買超集中度
    可吃欄位：
    - inst_buy_days / institutional_buy_days
    - inst_net_buy / institutional_net_buy
    - foreign_net_buy, trust_net_buy, dealer_net_buy
    """
    buy_days = max(
        _num(row.get("inst_buy_days", 0)),
        _num(row.get("institutional_buy_days", 0)),
        _num(row.get("three_major_buy_days", 0)),
    )
    inst_net = max(
        _num(row.get("inst_net_buy", 0)),
        _num(row.get("institutional_net_buy", 0)),
        _num(row.get("three_major_net_buy", 0)),
        _num(row.get("foreign_net_buy", 0)) + _num(row.get("trust_net_buy", 0)) + _num(row.get("dealer_net_buy", 0)),
    )

    score = 50
    reason = []

    if buy_days >= 5:
        score += 35
        reason.append("法人連買5日以上")
    elif buy_days >= 3:
        score += 25
        reason.append("法人連買3日")
    elif buy_days >= 1:
        score += 12
        reason.append("法人轉買")
    else:
        reason.append("法人未連買")

    if inst_net > 0:
        score += 15
        reason.append("法人買超")
    elif inst_net < 0:
        score -= 20
        reason.append("法人賣超")

    return max(0, min(100, score)), "、".join(reason)


def _score_foreign_holding(row):
    """
    外資持股比例變化
    可吃欄位：
    - foreign_holding_change
    - foreign_hold_pct_change
    - foreign_holding_pct_delta
    """
    delta = None
    for k in ("foreign_holding_change", "foreign_hold_pct_change", "foreign_holding_pct_delta", "foreign_pct_delta"):
        if _has(row, k):
            delta = _num(row.get(k))
            break

    if delta is None:
        return None, "外資持股無資料"

    score = 50
    if delta >= 1.0:
        return 90, "外資持股明顯提升"
    if delta >= 0.3:
        return 75, "外資持股提升"
    if delta > 0:
        return 62, "外資持股小幅提升"
    if delta <= -1.0:
        return 20, "外資持股明顯下降"
    if delta <= -0.3:
        return 35, "外資持股下降"
    return score, "外資持股持平"


def _score_trust(row):
    """
    投信買超集中度
    可吃欄位：
    - trust_buy_days
    - trust_net_buy
    - investment_trust_net_buy
    """
    buy_days = max(_num(row.get("trust_buy_days", 0)), _num(row.get("investment_trust_buy_days", 0)))
    net = max(_num(row.get("trust_net_buy", 0)), _num(row.get("investment_trust_net_buy", 0)))

    if not any(_has(row, k) for k in ("trust_buy_days", "investment_trust_buy_days", "trust_net_buy", "investment_trust_net_buy")):
        return None, "投信無資料"

    score = 50
    reason = []

    if buy_days >= 5:
        score += 35
        reason.append("投信連買5日以上")
    elif buy_days >= 3:
        score += 25
        reason.append("投信連買3日")
    elif buy_days >= 1:
        score += 12
        reason.append("投信轉買")

    if net > 0:
        score += 15
        reason.append("投信買超")
    elif net < 0:
        score -= 20
        reason.append("投信賣超")

    if not reason:
        reason.append("投信持平")

    return max(0, min(100, score)), "、".join(reason)


def _score_margin(row):
    """
    融資融券變化
    實戰解讀：
    - 上漲/強勢中，融資下降或不暴增 = 健康
    - 融資暴增 = 散戶追高風險
    - 融券增加有時是軋空燃料，但先保守給中性偏多
    可吃欄位：
    - margin_balance_change / margin_change / margin_delta
    - short_balance_change / short_change / short_delta
    """
    margin_delta = None
    short_delta = None

    for k in ("margin_balance_change", "margin_change", "margin_delta", "margin_financing_delta"):
        if _has(row, k):
            margin_delta = _num(row.get(k))
            break

    for k in ("short_balance_change", "short_change", "short_delta", "securities_lending_delta"):
        if _has(row, k):
            short_delta = _num(row.get(k))
            break

    if margin_delta is None and short_delta is None:
        return None, "融資融券無資料"

    score = 50
    reason = []

    if margin_delta is not None:
        if margin_delta < 0:
            score += 22
            reason.append("融資下降")
        elif margin_delta > 0:
            score -= 15
            reason.append("融資增加")
        else:
            reason.append("融資持平")

    if short_delta is not None:
        if short_delta > 0:
            score += 8
            reason.append("融券增加")
        elif short_delta < 0:
            score += 3
            reason.append("融券回補")
        else:
            reason.append("融券持平")

    return max(0, min(100, score)), "、".join(reason)


def _score_major_holder(row):
    """
    大戶 / 董監持股變化（慢速資料）
    可吃欄位：
    - major_holder_change / big_holder_change
    - director_holding_change / insider_holding_change
    """
    values = []
    reasons = []

    for k in ("major_holder_change", "big_holder_change", "major_holder_pct_change"):
        if _has(row, k):
            values.append(_num(row.get(k)))
            reasons.append("大戶持股")
            break

    for k in ("director_holding_change", "insider_holding_change", "director_pct_change"):
        if _has(row, k):
            values.append(_num(row.get(k)))
            reasons.append("董監持股")
            break

    if not values:
        return None, "大戶/董監無資料"

    avg = float(np.mean(values))
    if avg >= 1.0:
        return 88, "大戶/董監持股明顯增加"
    if avg >= 0.2:
        return 72, "大戶/董監持股增加"
    if avg > -0.2:
        return 55, "大戶/董監持股穩定"
    if avg <= -1.0:
        return 20, "大戶/董監持股明顯下降"
    return 35, "大戶/董監持股下降"


def calc_chip_score(row) -> dict:
    """
    權重完整版 B：
    三大法人 30%
    外資持股 20%
    投信 20%
    融資融券 20%
    大戶/董監 10%

    若部分資料缺失，剩餘可用權重自動重配。
    """
    parts = [
        ("三大法人", 0.30, _score_inst(row)),
        ("外資持股", 0.20, _score_foreign_holding(row)),
        ("投信", 0.20, _score_trust(row)),
        ("融資融券", 0.20, _score_margin(row)),
        ("大戶/董監", 0.10, _score_major_holder(row)),
    ]

    valid = []
    reason_list = []
    missing = []

    for name, weight, result in parts:
        score, reason = result
        if score is None:
            missing.append(name)
            continue
        valid.append((name, weight, score))
        if reason:
            reason_list.append(reason)

    if not valid:
        score = 50.0
        label = chip_label(score)
        return {
            "chip_score": round(score, 2),
            "chip_label": label,
            "chip_display": f"{round(score):.0f}（{label}）",
            "chip_reason": "籌碼資料不足",
            "chip_hint": "籌碼資料不足，先以中性處理，不作為重倉依據。",
            "chip_valid_count": 0,
            "chip_missing": "、".join(missing),
        }

    weight_sum = sum(w for _, w, _ in valid)
    score = sum((w / weight_sum) * s for _, w, s in valid)
    label = chip_label(score)

    reason = "｜".join(reason_list)
    if missing:
        reason += f"｜缺資料：{'、'.join(missing)}"

    return {
        "chip_score": round(float(score), 2),
        "chip_label": label,
        "chip_display": f"{round(float(score)):.0f}（{label}）",
        "chip_reason": reason,
        "chip_hint": chip_hint(score),
        "chip_valid_count": len(valid),
        "chip_missing": "、".join(missing),
    }


def add_chip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    對 final_decision_engine 輸出的 DataFrame 加上籌碼欄位。
    """
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

    return out


# ===== 整合範例 =====
# from chip_concentration_v26620 import add_chip_columns
# final_df = add_chip_columns(final_df)
# final_df.to_csv("mobile_dashboard_v1/data/final_decision_engine.csv", index=False, encoding="utf-8-sig")
