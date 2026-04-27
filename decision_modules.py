"""
decision_modules.py
v1.9 strategy repair version

目的：
- 修復 entry_score 全部 0 的問題
- 缺欄位不爆掉，但有資料就一定計分
- 建立三層行動：
  BUY   >= 70
  TEST  >= 55
  READY >= 40
  SKIP  < 40

注意：
- pipeline 最終仍可把 BUY / TEST / READY 映射到前端。
- 籌碼欄位如果沒有，不扣分，只標註「籌碼缺省」。
"""

import numpy as np
import pandas as pd


def safe_num(v, default=np.nan):
    try:
        n = pd.to_numeric(v, errors="coerce")
        if pd.isna(n):
            return default
        return float(n)
    except Exception:
        return default


def has_value(v):
    try:
        return not pd.isna(v)
    except Exception:
        return v is not None


def add_decision_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]

    if "stock_id" not in out.columns:
        return out

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)
    else:
        out = out.sort_values(["stock_id"]).reset_index(drop=True)

    if "close" not in out.columns:
        return out

    if "volume" not in out.columns:
        out["volume"] = np.nan
    if "high" not in out.columns:
        out["high"] = out["close"]
    if "low" not in out.columns:
        out["low"] = out["close"]

    g = out.groupby("stock_id", group_keys=False)

    for n in [5, 10, 20, 60]:
        col = f"ma{n}"
        if col not in out.columns:
            out[col] = g["close"].rolling(n).mean().reset_index(level=0, drop=True)

    if "return_5d" not in out.columns:
        out["return_5d"] = g["close"].pct_change(5)
    if "return_10d" not in out.columns:
        out["return_10d"] = g["close"].pct_change(10)
    if "return_20d" not in out.columns:
        out["return_20d"] = g["close"].pct_change(20)

    if "mom5" not in out.columns:
        out["mom5"] = out["return_5d"]
    if "mom20" not in out.columns:
        out["mom20"] = out["return_20d"]
    if "mom60" not in out.columns:
        out["mom60"] = g["close"].pct_change(60)

    if "volume_ma20" not in out.columns:
        out["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    if "volume_ratio" not in out.columns:
        out["volume_ratio"] = out["volume"] / (out["volume_ma20"] + 1e-9)

    # 前高，不含今日
    if "prev_high_10" not in out.columns:
        out["prev_high_10"] = g["close"].shift(1).rolling(10).max().reset_index(level=0, drop=True)
    if "prev_high_20" not in out.columns:
        out["prev_high_20"] = g["close"].shift(1).rolling(20).max().reset_index(level=0, drop=True)

    # KD 簡化版
    low9 = g["low"].rolling(9).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    if "kd_k" not in out.columns:
        out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    if "kd_d" not in out.columns:
        out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    if "macd_diff" not in out.columns:
        out["macd_diff"] = ema12 - ema26
    if "macd_signal" not in out.columns:
        out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    if "macd_hist" not in out.columns:
        out["macd_hist"] = out["macd_diff"] - out["macd_signal"]

    return out


def market_score(market_row=None):
    # 目前沒有大盤資料時，不加不扣
    if market_row is None:
        return 0, "大盤資料缺省：不加不扣"

    close = safe_num(market_row.get("close"))
    ma20 = safe_num(market_row.get("ma20"))
    ma60 = safe_num(market_row.get("ma60"))

    if has_value(close) and has_value(ma60) and close < ma60:
        return -20, "大盤跌破MA60"
    if has_value(close) and has_value(ma20) and close < ma20:
        return -8, "大盤跌破MA20"
    if has_value(close) and has_value(ma20) and close >= ma20:
        return 10, "大盤站上MA20"
    return 0, "大盤中性"


def trend_score(row):
    score = 0
    reasons = []

    close = safe_num(row.get("close"))
    ma5 = safe_num(row.get("ma5"))
    ma10 = safe_num(row.get("ma10"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    prev_high_20 = safe_num(row.get("prev_high_20"))

    if has_value(close) and has_value(ma20):
        if close > ma20:
            score += 18
            reasons.append("站上MA20")
        elif close >= ma20 * 0.98:
            score += 10
            reasons.append("貼近MA20")

    if all(has_value(x) for x in [ma5, ma10, ma20]):
        if ma5 > ma10 > ma20:
            score += 18
            reasons.append("短均多排")
        elif ma5 >= ma10 * 0.995 and ma10 >= ma20 * 0.995:
            score += 10
            reasons.append("均線靠攏")

    if all(has_value(x) for x in [close, ma60]) and close > ma60:
        score += 8
        reasons.append("站上MA60")

    if all(has_value(x) for x in [close, prev_high_20]) and close >= prev_high_20 * 0.98:
        score += 10
        reasons.append("接近20日高")

    return score, "；".join(reasons) if reasons else "趨勢不足"


def momentum_score(row):
    score = 0
    reasons = []

    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    kd_k = safe_num(row.get("kd_k"))
    kd_d = safe_num(row.get("kd_d"))
    macd_diff = safe_num(row.get("macd_diff"))
    macd_hist = safe_num(row.get("macd_hist"))
    volume_ratio = safe_num(row.get("volume_ratio"))

    if has_value(mom5):
        if mom5 > 0:
            score += 10
            reasons.append("5日動能正")
        elif mom5 > -0.02:
            score += 5
            reasons.append("5日動能未破壞")

    if has_value(mom20):
        if mom20 > 0:
            score += 10
            reasons.append("20日動能正")
        elif mom20 > -0.03:
            score += 4
            reasons.append("20日動能中性")

    if all(has_value(x) for x in [kd_k, kd_d]):
        if kd_k > kd_d and kd_k < 88:
            score += 8
            reasons.append("KD偏多")
        elif 35 <= kd_k <= 75:
            score += 4
            reasons.append("KD中性可觀察")

    if has_value(macd_diff):
        if macd_diff > 0:
            score += 8
            reasons.append("MACD偏多")
        elif has_value(macd_hist) and macd_hist > 0:
            score += 5
            reasons.append("MACD柱轉強")

    if has_value(volume_ratio):
        if 1.0 <= volume_ratio <= 3.5:
            score += 6
            reasons.append("量能配合")
        elif volume_ratio > 0.7:
            score += 3
            reasons.append("量能尚可")

    return score, "；".join(reasons) if reasons else "動能不足"


def chip_score(row):
    score = 0
    reasons = []

    concentration = np.nan
    for col in ["chip_concentration", "concentration", "holder_concentration"]:
        if col in row:
            concentration = safe_num(row.get(col))
            break

    if has_value(concentration):
        if concentration > 0:
            score += 8
            reasons.append("籌碼集中上升")
        else:
            reasons.append("籌碼未增")
    else:
        reasons.append("籌碼缺省")

    inst_buy = 0
    found = False
    for col in ["foreign_net_buy", "invest_trust_net_buy", "dealer_net_buy"]:
        if col in row:
            v = safe_num(row.get(col), 0)
            if has_value(v):
                inst_buy += v
                found = True

    if found and inst_buy > 0:
        score += 7
        reasons.append("法人偏買")

    return score, "；".join(reasons)


def risk_penalty(row):
    penalty = 0
    reasons = []

    r5 = safe_num(row.get("return_5d"))
    r10 = safe_num(row.get("return_10d"))
    kd_k = safe_num(row.get("kd_k"))
    volume_ratio = safe_num(row.get("volume_ratio"))

    if has_value(r5) and r5 > 0.22:
        penalty -= 12
        reasons.append("5日漲幅偏熱")
    if has_value(r10) and r10 > 0.35:
        penalty -= 12
        reasons.append("10日漲幅偏熱")
    if has_value(kd_k) and kd_k > 92:
        penalty -= 6
        reasons.append("KD偏熱")
    if has_value(volume_ratio) and volume_ratio > 6:
        penalty -= 6
        reasons.append("爆量風險")

    return penalty, "；".join(reasons) if reasons else "風險正常"


def entry_score(row, market_row=None):
    ms, mr = market_score(market_row)
    ts, tr = trend_score(row)
    mos, mor = momentum_score(row)
    cs, cr = chip_score(row)
    rp, rr = risk_penalty(row)

    total = ms + ts + mos + cs + rp

    if total >= 70:
        action = "BUY"
    elif total >= 55:
        action = "TEST"
    elif total >= 40:
        action = "READY"
    else:
        action = "SKIP"

    return {
        "entry_score": int(round(total)),
        "entry_action": action,
        "market_score": ms,
        "trend_score": ts,
        "momentum_score": mos,
        "chip_score": cs,
        "risk_penalty": rp,
        "entry_reason": f"趨勢:{tr}｜動能:{mor}｜籌碼:{cr}｜風險:{rr}",
    }


def position_stage(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    macd_diff = safe_num(row.get("macd_diff"))
    kd_k = safe_num(row.get("kd_k"))

    if has_value(close) and has_value(ma60) and close < ma60:
        return "防守"
    if has_value(close) and has_value(ma20) and has_value(macd_diff) and close > ma20 and macd_diff > 0:
        return "波段"
    if has_value(close) and has_value(ma60) and close > ma60:
        return "中長"
    if has_value(kd_k) and kd_k < 85:
        return "短線"
    return "未分類"
