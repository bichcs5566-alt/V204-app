# -*- coding: utf-8 -*-
"""
chip_concentration_v26621.py
v266.24 完整籌碼評分版

目的：
1. 保留 final_decision_engine.py import：
   from chip_concentration_v26621 import add_chip_columns
2. 讀 chip_source_twse.csv / mobile_dashboard_v1/data/chip_source_twse.csv
3. 沒法人資料不再直接等於「系統壞掉」：
   - 有 stock_id 但 inst_valid=0 → 視為法人中性/資料有限
   - 完全沒有 stock_id → 才是資料不足
4. 輸出欄位：
   chip_score, chip_label, chip_display, chip_reason, chip_hint,
   chip_valid_count, chip_missing, chip_confidence
"""

from pathlib import Path
import re
import math
import pandas as pd


VERSION = "v266.24"


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


def _load_chip_source():
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
                return df.drop_duplicates("stock_id", keep="last")
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


def _hint(score, confidence):
    score = _num(score)
    if "無對應" in confidence:
        return "籌碼來源未涵蓋此股，僅能參考技術面與流動性。"
    if "低信心" in confidence:
        if score >= 60:
            return "籌碼偏集中但資料有限，可搭配技術面確認，不宜單獨重倉。"
        if score <= 30:
            return "籌碼偏弱且資料有限，避免重倉追高。"
        return "籌碼普通或資料有限，需搭配技術面確認。"
    if score >= 80:
        return "籌碼高度集中，資金共識強，可搭配技術面優先觀察。"
    if score >= 60:
        return "籌碼偏集中，有資金進場跡象，可搭配技術面確認。"
    if score >= 40:
        return "籌碼普通，尚未形成明顯優勢，需搭配技術面確認。"
    if score >= 20:
        return "籌碼偏分散，資金共識不足，建議降低部位或等待確認。"
    return "籌碼極度分散，不具主力優勢，避免追高或重倉。"


def _score_row(row):
    matched = bool(_num(row.get("_chip_matched", 0), 0) > 0)

    if not matched:
        return {
            "chip_score": 50,
            "chip_label": "🟡 普通",
            "chip_display": "50（🟡 普通）",
            "chip_reason": "籌碼來源未涵蓋此股",
            "chip_hint": "籌碼來源未涵蓋此股，僅能參考技術面與流動性。",
            "chip_valid_count": 0,
            "chip_missing": "無對應籌碼來源",
            "chip_confidence": "📉 無對應資料",
        }

    foreign = _num(row.get("foreign_net_buy", 0))
    trust = _num(row.get("trust_net_buy", 0))
    dealer = _num(row.get("dealer_net_buy", 0))
    inst = _num(row.get("inst_net_buy", foreign + trust + dealer), foreign + trust + dealer)
    inst_valid = int(_num(row.get("inst_valid", 0)))

    margin_valid = int(_num(row.get("margin_valid", 0)))
    margin_chg = _num(row.get("margin_balance_change", 0))
    short_chg = _num(row.get("short_balance_change", 0))

    score = 50.0
    valid_count = 0
    reasons = []

    # 法人分數：有資料則加權；沒資料則中性，不直接判資料不足
    if inst_valid > 0:
        valid_count += 1
        if inst > 0:
            add = min(25, 8 + abs(inst) / 2000)
            score += add
            reasons.append("三大法人買超")
        elif inst < 0:
            sub = min(25, 8 + abs(inst) / 2000)
            score -= sub
            reasons.append("三大法人賣超")
        else:
            reasons.append("法人中性")

        if trust > 0:
            score += min(10, 4 + abs(trust) / 1000)
            reasons.append("投信買超")
        elif trust < 0:
            score -= min(10, 4 + abs(trust) / 1000)
            reasons.append("投信賣超")
    else:
        reasons.append("法人無明顯交易或當日未揭露")

    # 融資融券：有資料才影響
    if margin_valid > 0:
        valid_count += 1
        if margin_chg < -500:
            score += 12
            reasons.append("融資明顯下降")
        elif margin_chg < 0:
            score += 6
            reasons.append("融資下降")
        elif margin_chg > 1000:
            score -= 12
            reasons.append("融資暴增")
        elif margin_chg > 0:
            score -= 6
            reasons.append("融資增加")
        else:
            reasons.append("融資變化中性")

        if short_chg > 0:
            score += 3
            reasons.append("融券增加")
    else:
        reasons.append("融資融券資料有限")

    score = max(0, min(100, score))
    label = _label(score)

    if valid_count >= 2:
        confidence = "📊 中高信心"
        missing = ""
    elif valid_count == 1:
        confidence = "📉 低信心"
        missing = "部分籌碼資料有限"
    else:
        # 有進 universe，但當日法人與融資都沒有有效訊號
        confidence = "📉 低信心"
        missing = "法人/融資資料有限"

    return {
        "chip_score": round(score, 2),
        "chip_label": label,
        "chip_display": f"{round(score):.0f}（{label}）",
        "chip_reason": "｜".join(reasons),
        "chip_hint": _hint(score, confidence),
        "chip_valid_count": valid_count,
        "chip_missing": missing,
        "chip_confidence": confidence,
    }


def add_chip_columns(df):
    if df is None or len(df) == 0:
        return df

    out = df.copy()
    out["_chip_sid"] = out.apply(_row_sid, axis=1)

    chip = _load_chip_source()
    if not chip.empty and "stock_id" in chip.columns:
        chip = chip.copy()
        chip["_chip_matched"] = 1
        out = out.merge(
            chip,
            left_on="_chip_sid",
            right_on="stock_id",
            how="left",
            suffixes=("", "_chip_src")
        )

        # 保護原本 stock_id
        if "stock_id_chip_src" in out.columns:
            out.drop(columns=["stock_id_chip_src"], inplace=True, errors="ignore")
    else:
        out["_chip_matched"] = 0

    if "_chip_matched" not in out.columns:
        out["_chip_matched"] = 0
    out["_chip_matched"] = pd.to_numeric(out["_chip_matched"], errors="coerce").fillna(0)

    for c in [
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid", "margin_balance", "short_balance",
        "margin_balance_change", "short_balance_change", "margin_valid"
    ]:
        if c not in out.columns:
            out[c] = 0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    results = out.apply(_score_row, axis=1)

    out["chip_score"] = results.apply(lambda x: x["chip_score"])
    out["chip_label"] = results.apply(lambda x: x["chip_label"])
    out["chip_display"] = results.apply(lambda x: x["chip_display"])
    out["chip_reason"] = results.apply(lambda x: x["chip_reason"])
    out["chip_hint"] = results.apply(lambda x: x["chip_hint"])
    out["chip_valid_count"] = results.apply(lambda x: x["chip_valid_count"])
    out["chip_missing"] = results.apply(lambda x: x["chip_missing"])
    out["chip_confidence"] = results.apply(lambda x: x["chip_confidence"])

    out.drop(columns=["_chip_sid", "_chip_matched"], inplace=True, errors="ignore")
    return out


def get_chip_score(stock_id):
    chip = _load_chip_source()
    sid = _sid(stock_id)

    if not chip.empty and "stock_id" in chip.columns:
        hit = chip[chip["stock_id"].astype(str) == sid]
        if not hit.empty:
            row = hit.iloc[-1].copy()
            row["_chip_matched"] = 1
            r = _score_row(row)
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
        "reason": "籌碼來源未涵蓋此股",
        "hint": "籌碼來源未涵蓋此股，僅能參考技術面與流動性。",
        "confidence": "📉 無對應資料",
    }
