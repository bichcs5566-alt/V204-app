"""
decision_modules.py
v2.1 strategy-layer repair

核心調整：
1. KD / MACD 不需要同時黃金交叉，只要任一黃金交叉就加分。
2. 加入低位籌碼增加機制：
   - 股價長期低位 / 低於50元 / 低於MA60
   - 籌碼集中、法人買超、或籌碼相關欄位增加
   => 可推升到 READY / TEST。
3. 不做硬保底補名單；名單應該由策略候選池推出。
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

    if "stock_id" not in out.columns or "close" not in out.columns:
        return out

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)
    else:
        out = out.sort_values(["stock_id"]).reset_index(drop=True)

    if "volume" not in out.columns:
        out["volume"] = np.nan
    if "high" not in out.columns:
        out["high"] = out["close"]
    if "low" not in out.columns:
        out["low"] = out["close"]

    for c in ["close", "high", "low", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    g = out.groupby("stock_id", group_keys=False)

    for n in [5, 10, 20, 60]:
        out[f"ma{n}"] = g["close"].rolling(n).mean().reset_index(level=0, drop=True)

    out["return_5d"] = g["close"].pct_change(5)
    out["return_10d"] = g["close"].pct_change(10)
    out["return_20d"] = g["close"].pct_change(20)

    out["mom5"] = out["return_5d"]
    out["mom20"] = out["return_20d"]
    out["mom60"] = g["close"].pct_change(60)

    out["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["volume_ma20"] + 1e-9)

    out["prev_high_10"] = g["close"].shift(1).rolling(10).max().reset_index(level=0, drop=True)
    out["prev_high_20"] = g["close"].shift(1).rolling(20).max().reset_index(level=0, drop=True)

    # KD
    low9 = g["low"].rolling(9).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_k_prev"] = g["kd_k"].shift(1)
    out["kd_d_prev"] = g["kd_d"].shift(1)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_diff_prev"] = g["macd_diff"].shift(1)
    out["macd_signal_prev"] = g["macd_signal"].shift(1)

    return out


def market_score(market_row=None):
    if market_row is None:
        return 0, "大盤資料缺省"
    close = safe_num(market_row.get("close"))
    ma20 = safe_num(market_row.get("ma20"))
    ma60 = safe_num(market_row.get("ma60"))
    if has_value(close) and has_value(ma60) and close < ma60:
        return -15, "大盤跌破MA60"
    if has_value(close) and has_value(ma20) and close < ma20:
        return -6, "大盤跌破MA20"
    if has_value(close) and has_value(ma20) and close >= ma20:
        return 8, "大盤站上MA20"
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
            score += 16
            reasons.append("短均多排")
        elif ma5 >= ma10 * 0.995 and ma10 >= ma20 * 0.995:
            score += 10
            reasons.append("均線靠攏")

    if has_value(close) and has_value(ma60) and close > ma60:
        score += 6
        reasons.append("站上MA60")

    if has_value(close) and has_value(prev_high_20) and close >= prev_high_20 * 0.98:
        score += 8
        reasons.append("接近20日高")

    return score, "；".join(reasons) if reasons else "趨勢不足"


def momentum_score(row):
    score = 0
    reasons = []

    kd_k = safe_num(row.get("kd_k"))
    kd_d = safe_num(row.get("kd_d"))
    kd_k_prev = safe_num(row.get("kd_k_prev"))
    kd_d_prev = safe_num(row.get("kd_d_prev"))

    macd_diff = safe_num(row.get("macd_diff"))
    macd_signal = safe_num(row.get("macd_signal"))
    macd_diff_prev = safe_num(row.get("macd_diff_prev"))
    macd_signal_prev = safe_num(row.get("macd_signal_prev"))

    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    volume_ratio = safe_num(row.get("volume_ratio"))

    kd_cross = (
        all(has_value(x) for x in [kd_k, kd_d, kd_k_prev, kd_d_prev])
        and kd_k > kd_d
        and kd_k_prev <= kd_d_prev
    )

    macd_cross = (
        all(has_value(x) for x in [macd_diff, macd_signal, macd_diff_prev, macd_signal_prev])
        and macd_diff > macd_signal
        and macd_diff_prev <= macd_signal_prev
    )

    # 核心：任一黃金交叉即可，不需要兩個都成立
    if kd_cross or macd_cross:
        score += 18
        reasons.append("KD或MACD黃金交叉")

    if has_value(mom5):
        if mom5 > 0:
            score += 8
            reasons.append("5日動能正")
        elif mom5 > -0.02:
            score += 4
            reasons.append("5日動能未破壞")

    if has_value(mom20):
        if mom20 > 0:
            score += 8
            reasons.append("20日動能正")
        elif mom20 > -0.03:
            score += 4
            reasons.append("20日動能中性")

    if has_value(volume_ratio):
        if 1.0 <= volume_ratio <= 3.8:
            score += 6
            reasons.append("量能配合")
        elif volume_ratio > 0.7:
            score += 3
            reasons.append("量能尚可")

    return score, "；".join(reasons) if reasons else "動能未確認"


def chip_score(row):
    score = 0
    reasons = []

    chip_up = False

    # 籌碼集中度
    for col in ["chip_concentration", "concentration", "holder_concentration", "chip_change", "major_holder_change"]:
        if col in row:
            v = safe_num(row.get(col))
            if has_value(v) and v > 0:
                chip_up = True
                reasons.append("籌碼集中上升")
                break

    # 法人買超
    inst_buy = 0
    found_inst = False
    for col in ["foreign_net_buy", "invest_trust_net_buy", "dealer_net_buy", "institution_net_buy"]:
        if col in row:
            v = safe_num(row.get(col), 0)
            if has_value(v):
                inst_buy += v
                found_inst = True
    if found_inst and inst_buy > 0:
        chip_up = True
        reasons.append("法人偏買")

    if chip_up:
        score += 12
    else:
        reasons.append("籌碼缺省或未增")

    return score, "；".join(reasons), chip_up


def low_price_chip_push_score(row, chip_up):
    """
    你指定的新機制：
    長期低於50 / 低位整理 + 籌碼增加，可以推上來。
    這裡同時支援兩種低位定義：
    1. 股價低於50
    2. close < MA60，代表長期仍偏低位
    """
    close = safe_num(row.get("close"))
    ma60 = safe_num(row.get("ma60"))
    mom20 = safe_num(row.get("mom20"))

    low_zone = False
    reasons = []

    if has_value(close) and close < 50:
        low_zone = True
        reasons.append("股價低於50")
    if has_value(close) and has_value(ma60) and close < ma60:
        low_zone = True
        reasons.append("低於MA60")
    if has_value(mom20) and mom20 < 0.03:
        reasons.append("低位未過熱")

    if low_zone and chip_up:
        return 14, "低位籌碼增加"
    return 0, "無低位籌碼推升"


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
    cs, cr, chip_up = chip_score(row)
    low_push, low_reason = low_price_chip_push_score(row, chip_up)
    rp, rr = risk_penalty(row)

    total = ms + ts + mos + cs + low_push + rp

    if total >= 65:
        action = "BUY"
    elif total >= 50:
        action = "TEST"
    elif total >= 35:
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
        "low_chip_push_score": low_push,
        "risk_penalty": rp,
        "entry_reason": f"趨勢:{tr}｜動能:{mor}｜籌碼:{cr}｜低位推升:{low_reason}｜風險:{rr}",
    }


def position_stage(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    macd_diff = safe_num(row.get("macd_diff"))
    kd_k = safe_num(row.get("kd_k"))

    if has_value(close) and has_value(ma60) and close < ma60:
        return "低位觀察"
    if has_value(close) and has_value(ma20) and has_value(macd_diff) and close > ma20 and macd_diff > 0:
        return "波段"
    if has_value(close) and has_value(ma60) and close > ma60:
        return "中長"
    if has_value(kd_k) and kd_k < 85:
        return "短線"
    return "未分類"
