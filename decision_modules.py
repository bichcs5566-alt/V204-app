"""
decision_modules.py
v2.5 主力結構 + 動能保留版

順序：
1. 主力候選結構：流動性、價格層級、非雜魚
2. 動能啟動：mom / breakout / KD / MACD
3. 主力階段：佈局 / 試盤 / 主升
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

    if "volume" not in out.columns:
        out["volume"] = np.nan
    if "high" not in out.columns:
        out["high"] = out["close"]
    if "low" not in out.columns:
        out["low"] = out["close"]

    for c in ["close", "high", "low", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["turnover_value"] = out["close"] * out["volume"]

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

    low9 = g["low"].rolling(9, min_periods=5).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9, min_periods=5).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_k_prev"] = g["kd_k"].shift(1)
    out["kd_d_prev"] = g["kd_d"].shift(1)
    out["kd_cross_up"] = ((out["kd_k"] > out["kd_d"]) & (out["kd_k_prev"] <= out["kd_d_prev"])).astype(int)

    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_diff_prev"] = g["macd_diff"].shift(1)
    out["macd_signal_prev"] = g["macd_signal"].shift(1)
    out["macd_cross_up"] = ((out["macd_diff"] > out["macd_signal"]) & (out["macd_diff_prev"] <= out["macd_signal_prev"])).astype(int)

    return out


def structure_score(row):
    close = safe_num(row.get("close"))
    volume = safe_num(row.get("volume"))
    turnover_value = safe_num(row.get("turnover_value"))
    vol_ratio = safe_num(row.get("volume_ratio"))

    score = 0
    reasons = []

    # 價格結構：避免全低價，但不完全排除低價
    if has_value(close):
        if close >= 50:
            score += 18
            reasons.append("價格結構健康")
        elif close >= 20:
            score += 12
            reasons.append("中低價可接受")
        elif close >= 10:
            score += 5
            reasons.append("低價降權")
        else:
            score -= 12
            reasons.append("過低價風險")

    # 流動性：主力需要能進出
    if has_value(volume):
        if volume >= 3000:
            score += 16
            reasons.append("成交量足")
        elif volume >= 1000:
            score += 10
            reasons.append("成交量可")
        elif volume >= 300:
            score += 4
            reasons.append("成交量偏低")
        else:
            score -= 10
            reasons.append("流動性不足")

    if has_value(turnover_value):
        if turnover_value >= 200000:
            score += 12
            reasons.append("成交值佳")
        elif turnover_value >= 50000:
            score += 6
            reasons.append("成交值可")

    if has_value(vol_ratio) and 0.8 <= vol_ratio <= 4.0:
        score += 6
        reasons.append("量能不失真")

    return score, "；".join(reasons) if reasons else "結構不足"


def momentum_score(row):
    score = 0
    reasons = []

    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    prev_high_20 = safe_num(row.get("prev_high_20"))
    kd_cross = safe_num(row.get("kd_cross_up"), 0) == 1
    macd_cross = safe_num(row.get("macd_cross_up"), 0) == 1
    macd_diff = safe_num(row.get("macd_diff"))

    if has_value(mom5):
        if mom5 > 0.03:
            score += 16
            reasons.append("5日動能強")
        elif mom5 > 0:
            score += 10
            reasons.append("5日動能正")
        elif mom5 > -0.03:
            score += 5
            reasons.append("5日未破壞")

    if has_value(mom20):
        if mom20 > 0.08:
            score += 16
            reasons.append("20日動能強")
        elif mom20 > 0:
            score += 10
            reasons.append("20日動能正")
        elif mom20 > -0.05:
            score += 4
            reasons.append("20日中性")

    if has_value(close) and has_value(prev_high_20) and close >= prev_high_20 * 0.98:
        score += 10
        reasons.append("接近20日高")

    if has_value(close) and has_value(ma20) and close >= ma20 * 0.98:
        score += 8
        reasons.append("靠近MA20")

    if kd_cross or macd_cross:
        score += 16
        reasons.append("KD或MACD轉強")

    if has_value(macd_diff) and macd_diff > 0:
        score += 6
        reasons.append("MACD偏多")

    return score, "；".join(reasons) if reasons else "動能不足"


def main_force_stage_score(row):
    score = 0
    reasons = []

    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    ma20_slope = safe_num(row.get("ma20_slope"))
    mom5 = safe_num(row.get("mom5"))
    vol_ratio = safe_num(row.get("volume_ratio"))
    low20 = safe_num(row.get("low_20d"))
    low60 = safe_num(row.get("low_60d"))

    accumulation = (
        has_value(close) and has_value(low20) and has_value(low60)
        and low20 >= low60 * 0.95
        and has_value(mom5) and abs(mom5) <= 0.06
        and has_value(vol_ratio) and 0.9 <= vol_ratio <= 3.2
    )

    testing = (
        (safe_num(row.get("kd_cross_up"), 0) == 1 or safe_num(row.get("macd_cross_up"), 0) == 1)
        and has_value(vol_ratio) and vol_ratio >= 1.05
        and has_value(close) and has_value(ma20) and close >= ma20 * 0.97
    )

    breakout = (
        has_value(close) and has_value(ma20) and close > ma20
        and has_value(ma20_slope) and ma20_slope >= -0.005
        and has_value(mom5) and mom5 > 0
        and has_value(vol_ratio) and vol_ratio >= 1.1
    )

    if accumulation:
        score += 18
        reasons.append("佈局結構")
    if testing:
        score += 20
        reasons.append("試盤轉強")
    if breakout:
        score += 24
        reasons.append("主升發動")

    stage = "發動" if breakout else "試盤" if testing else "佈局" if accumulation else "觀察"
    return score, stage, "；".join(reasons) if reasons else "主力階段未明"


def entry_score(row, market_row=None):
    ss, sr = structure_score(row)
    ms, mr = momentum_score(row)
    fs, stage, fr = main_force_stage_score(row)

    score = ss * 0.35 + ms * 0.40 + fs * 0.25

    # 過熱與雜訊扣分
    r5 = safe_num(row.get("return_5d"))
    close = safe_num(row.get("close"))
    if has_value(r5) and r5 > 0.22:
        score -= 8
    if has_value(close) and close < 10:
        score -= 8

    if stage == "發動" and score >= 45:
        action = "BUY"
    elif stage == "試盤" and score >= 36:
        action = "TEST"
    elif score >= 28:
        action = "READY"
    else:
        action = "OBSERVE"

    return {
        "entry_score": int(round(score)),
        "entry_action": action,
        "main_force_stage": stage,
        "structure_score": round(ss, 2),
        "momentum_score": round(ms, 2),
        "main_force_score": round(fs, 2),
        "entry_reason": f"結構:{sr}｜動能:{mr}｜階段:{fr}",
    }


def position_stage(row):
    try:
        return entry_score(row).get("main_force_stage", "未分類")
    except Exception:
        return "未分類"
