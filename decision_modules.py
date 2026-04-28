"""
decision_modules.py
v2.4 主力同步 + 不空單版

目標：
- 不追高，只抓主力行為階段。
- 三層：
  READY：主力佈局中
  TEST ：試盤 / 轉強
  BUY  ：發動 / 可進場

核心：
1. 主力佈局：低位、止跌、量微增、籌碼/替代籌碼增加
2. 試盤：KD 或 MACD 任一黃金交叉 + 量能 + 接近 MA20
3. 發動：站上 MA20 + MA20 上彎 + 量能 + 短動能
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

    for n in [5, 10, 20, 60, 120]:
        out[f"ma{n}"] = g["close"].rolling(n, min_periods=max(3, min(n, 20))).mean().reset_index(level=0, drop=True)

    out["return_5d"] = g["close"].pct_change(5)
    out["return_10d"] = g["close"].pct_change(10)
    out["return_20d"] = g["close"].pct_change(20)

    out["mom5"] = out["return_5d"]
    out["mom10"] = out["return_10d"]
    out["mom20"] = out["return_20d"]
    out["mom60"] = g["close"].pct_change(60)

    out["volume_ma20"] = g["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["volume_ma20"] + 1e-9)

    out["low_20d"] = g["low"].rolling(20, min_periods=5).min().reset_index(level=0, drop=True)
    out["low_60d"] = g["low"].rolling(60, min_periods=10).min().reset_index(level=0, drop=True)
    out["prev_low_20"] = g["close"].shift(1).rolling(20, min_periods=5).min().reset_index(level=0, drop=True)

    out["prev_high_10"] = g["close"].shift(1).rolling(10, min_periods=5).max().reset_index(level=0, drop=True)
    out["prev_high_20"] = g["close"].shift(1).rolling(20, min_periods=5).max().reset_index(level=0, drop=True)

    out["ma20_slope"] = g["ma20"].diff(3) / (g["ma20"].shift(3) + 1e-9)

    # KD
    low9 = g["low"].rolling(9, min_periods=5).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9, min_periods=5).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_k_prev"] = g["kd_k"].shift(1)
    out["kd_d_prev"] = g["kd_d"].shift(1)
    out["kd_cross_up"] = ((out["kd_k"] > out["kd_d"]) & (out["kd_k_prev"] <= out["kd_d_prev"])).astype(int)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_diff_prev"] = g["macd_diff"].shift(1)
    out["macd_signal_prev"] = g["macd_signal"].shift(1)
    out["macd_cross_up"] = ((out["macd_diff"] > out["macd_signal"]) & (out["macd_diff_prev"] <= out["macd_signal_prev"])).astype(int)

    # 籌碼替代：若沒有真籌碼欄位，先用量能溫和增加替代。
    if "chip_delta_5d" not in out.columns:
        out["chip_delta_5d"] = np.nan

    return out


def kd_gold(row):
    return safe_num(row.get("kd_cross_up"), 0) == 1


def macd_gold(row):
    return safe_num(row.get("macd_cross_up"), 0) == 1


def chip_delta_positive(row):
    # 真籌碼欄位優先
    for col in ["chip_delta_5d", "chip_concentration", "concentration", "holder_concentration", "chip_change", "major_holder_change"]:
        if col in row:
            v = safe_num(row.get(col))
            if has_value(v) and v > 0:
                return True

    total = 0
    found = False
    for col in ["foreign_net_buy", "invest_trust_net_buy", "dealer_net_buy", "institution_net_buy"]:
        if col in row:
            total += safe_num(row.get(col), 0)
            found = True
    if found and total > 0:
        return True

    # 沒籌碼資料時，用「量能微增但未爆量」作為主力吃貨替代訊號
    vr = safe_num(row.get("volume_ratio"))
    mom5 = safe_num(row.get("mom5"))
    if has_value(vr) and 1.05 <= vr <= 2.8 and has_value(mom5) and abs(mom5) < 0.08:
        return True

    return False


def is_accumulation(row):
    close = safe_num(row.get("close"))
    ma60 = safe_num(row.get("ma60"))
    ma120 = safe_num(row.get("ma120"))
    low20 = safe_num(row.get("low_20d"))
    low60 = safe_num(row.get("low_60d"))
    vr = safe_num(row.get("volume_ratio"))
    mom5 = safe_num(row.get("mom5"))

    low_zone = False

    if has_value(close) and close < 50:
        low_zone = True
    if has_value(close) and has_value(ma60) and close < ma60:
        low_zone = True
    if has_value(close) and has_value(ma120) and close > ma120 * 0.92:
        low_zone = True

    stop_falling = (
        has_value(low20) and has_value(low60)
        and low20 >= low60 * 0.95
    )

    quiet_price = has_value(mom5) and abs(mom5) < 0.06

    volume_warm = has_value(vr) and 1.03 <= vr <= 3.2

    return bool(
        low_zone
        and stop_falling
        and quiet_price
        and volume_warm
        and chip_delta_positive(row)
    )


def is_testing(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    vr = safe_num(row.get("volume_ratio"))

    return bool(
        (kd_gold(row) or macd_gold(row))
        and has_value(vr) and vr > 1.08
        and has_value(close) and has_value(ma20)
        and close >= ma20 * 0.97
    )


def is_breakout(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma20_slope = safe_num(row.get("ma20_slope"))
    vr = safe_num(row.get("volume_ratio"))
    mom5 = safe_num(row.get("mom5"))

    return bool(
        has_value(close) and has_value(ma20) and close > ma20
        and has_value(ma20_slope) and ma20_slope > -0.005
        and has_value(vr) and vr > 1.12
        and has_value(mom5) and mom5 > 0
    )


def entry_score(row, market_row=None):
    score = 0
    reasons = []

    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    vr = safe_num(row.get("volume_ratio"))

    acc = is_accumulation(row)
    test = is_testing(row)
    brk = is_breakout(row)

    if has_value(close) and has_value(ma20):
        if close > ma20:
            score += 16
            reasons.append("站上MA20")
        elif close >= ma20 * 0.97:
            score += 9
            reasons.append("靠近MA20")

    if has_value(close) and has_value(ma60) and close > ma60:
        score += 6
        reasons.append("站上MA60")

    if kd_gold(row) or macd_gold(row):
        score += 18
        reasons.append("KD或MACD黃金交叉")

    if has_value(mom5):
        if mom5 > 0:
            score += 8
            reasons.append("5日動能正")
        elif abs(mom5) < 0.06:
            score += 6
            reasons.append("橫盤吸籌")

    if has_value(mom20):
        if mom20 > 0:
            score += 8
            reasons.append("20日動能正")
        elif mom20 > -0.05:
            score += 5
            reasons.append("20日未破壞")

    if has_value(vr):
        if vr > 1.2:
            score += 10
            reasons.append("量能放大")
        elif vr > 1.03:
            score += 6
            reasons.append("量能微增")

    if acc:
        score += 25
        reasons.append("主力佈局中")
    if test:
        score += 24
        reasons.append("試盤轉強")
    if brk:
        score += 28
        reasons.append("主升發動")
    if chip_delta_positive(row):
        score += 8
        reasons.append("籌碼/量能增加")

    # 過熱扣分
    r5 = safe_num(row.get("return_5d"))
    r10 = safe_num(row.get("return_10d"))
    if has_value(r5) and r5 > 0.22:
        score -= 10
        reasons.append("短線過熱")
    if has_value(r10) and r10 > 0.35:
        score -= 10
        reasons.append("10日過熱")

    if brk and score >= 55:
        action = "BUY"
        stage = "發動"
    elif test and score >= 42:
        action = "TEST"
        stage = "試盤"
    elif acc and score >= 24:
        action = "READY"
        stage = "佈局"
    elif score >= 50:
        action = "TEST"
        stage = "試盤"
    elif score >= 28:
        action = "READY"
        stage = "佈局"
    else:
        action = "OBSERVE"
        stage = "弱觀察"

    return {
        "entry_score": int(round(score)),
        "entry_action": action,
        "main_force_stage": stage,
        "entry_reason": "；".join(reasons) if reasons else "無主力跡象",
        "accumulation": acc,
        "testing": test,
        "breakout": brk,
        "kd_cross": kd_gold(row),
        "macd_cross": macd_gold(row),
        "chip_delta_positive": chip_delta_positive(row),
    }


def position_stage(row):
    try:
        return entry_score(row).get("main_force_stage", "未分類")
    except Exception:
        return "未分類"
