"""
decision_modules.py
v2.7 三層主力模型

核心修正：
v2.6 會把「還沒放量、還沒 KD/MACD 轉強，但線型已收斂」的潛伏股誤殺。
v2.7 補回 Layer 1：

Layer 1 潛伏：均線收斂、波動縮小、低點抬高、量穩定不爆，不要求放量/黃金交叉
Layer 2 試盤：KD 或 MACD 任一轉強 + 微量能 / 小動能
Layer 3 主升：站上 MA20 + 動能 + 放量

輸出：
READY = 潛伏 / 佈局
TEST  = 試盤
BUY   = 主升
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

    out["ma_max"] = out[["ma5", "ma10", "ma20"]].max(axis=1)
    out["ma_min"] = out[["ma5", "ma10", "ma20"]].min(axis=1)
    out["ma_converge_pct"] = (out["ma_max"] - out["ma_min"]) / (out["close"] + 1e-9)

    out["range_20"] = (g["high"].rolling(20, min_periods=5).max().reset_index(level=0, drop=True) -
                       g["low"].rolling(20, min_periods=5).min().reset_index(level=0, drop=True)) / (out["close"] + 1e-9)

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


def is_dead_stock(row):
    vr = safe_num(row.get("volume_ratio"))
    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    volume = safe_num(row.get("volume"))
    turnover = safe_num(row.get("turnover_value"))
    ma_converge = safe_num(row.get("ma_converge_pct"))
    range20 = safe_num(row.get("range_20"))

    # v2.7：不能把收斂潛伏誤判成死股
    is_converging = (
        has_value(ma_converge) and ma_converge <= 0.055
        and has_value(range20) and range20 <= 0.18
    )

    quiet = has_value(mom5) and abs(mom5) < 0.018
    weak20 = (not has_value(mom20)) or mom20 <= 0.015
    no_volume_push = has_value(vr) and vr < 0.75
    low_liquidity = (has_value(volume) and volume < 250) or (has_value(turnover) and turnover < 15000)

    if is_converging and not low_liquidity:
        return False

    return bool((quiet and weak20 and no_volume_push) or (quiet and low_liquidity))


def layer_latent(row):
    """Layer 1 潛伏：收斂、止跌、低點不破、量穩，不要求放量或黃金交叉。"""
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma60 = safe_num(row.get("ma60"))
    ma20_slope = safe_num(row.get("ma20_slope"))
    low20 = safe_num(row.get("low_20d"))
    low60 = safe_num(row.get("low_60d"))
    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))
    vr = safe_num(row.get("volume_ratio"))
    ma_converge = safe_num(row.get("ma_converge_pct"))
    range20 = safe_num(row.get("range_20"))

    ma_squeeze = has_value(ma_converge) and ma_converge <= 0.075
    range_squeeze = has_value(range20) and range20 <= 0.22
    low_not_break = has_value(low20) and has_value(low60) and low20 >= low60 * 0.96
    ma_flat = has_value(ma20_slope) and ma20_slope > -0.004
    price_near_ma = has_value(close) and has_value(ma20) and close >= ma20 * 0.94
    not_far_under_60 = (not has_value(ma60)) or (has_value(close) and close >= ma60 * 0.88)
    momentum_quiet = has_value(mom5) and abs(mom5) <= 0.075 and (not has_value(mom20) or mom20 >= -0.08)
    volume_stable = has_value(vr) and 0.65 <= vr <= 2.8

    return bool(
        ma_squeeze
        and range_squeeze
        and low_not_break
        and ma_flat
        and price_near_ma
        and not_far_under_60
        and momentum_quiet
        and volume_stable
        and not is_dead_stock(row)
    )


def layer_testing(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    mom5 = safe_num(row.get("mom5"))
    vr = safe_num(row.get("volume_ratio"))
    kd_cross = safe_num(row.get("kd_cross_up"), 0) == 1
    macd_cross = safe_num(row.get("macd_cross_up"), 0) == 1

    return bool(
        (kd_cross or macd_cross or (has_value(mom5) and mom5 > 0.025))
        and has_value(vr) and vr >= 0.95
        and has_value(close) and has_value(ma20) and close >= ma20 * 0.97
        and not is_dead_stock(row)
    )


def layer_breakout(row):
    close = safe_num(row.get("close"))
    ma20 = safe_num(row.get("ma20"))
    ma20_slope = safe_num(row.get("ma20_slope"))
    mom5 = safe_num(row.get("mom5"))
    vr = safe_num(row.get("volume_ratio"))
    prev_high_20 = safe_num(row.get("prev_high_20"))

    breakout_price = has_value(close) and has_value(prev_high_20) and close >= prev_high_20 * 0.985

    return bool(
        has_value(close) and has_value(ma20) and close > ma20
        and has_value(ma20_slope) and ma20_slope >= -0.003
        and has_value(mom5) and mom5 > 0
        and has_value(vr) and vr >= 1.03
        and (breakout_price or vr >= 1.12)
        and not is_dead_stock(row)
    )


def structure_score(row):
    close = safe_num(row.get("close"))
    volume = safe_num(row.get("volume"))
    turnover_value = safe_num(row.get("turnover_value"))
    vol_ratio = safe_num(row.get("volume_ratio"))

    score = 0
    reasons = []

    if has_value(close):
        if close >= 50:
            score += 18; reasons.append("價格結構健康")
        elif close >= 20:
            score += 12; reasons.append("中低價可接受")
        elif close >= 10:
            score += 4; reasons.append("低價降權")
        else:
            score -= 14; reasons.append("過低價風險")

    if has_value(volume):
        if volume >= 3000:
            score += 16; reasons.append("成交量足")
        elif volume >= 1000:
            score += 10; reasons.append("成交量可")
        elif volume >= 300:
            score += 3; reasons.append("成交量偏低")
        else:
            score -= 12; reasons.append("流動性不足")

    if has_value(turnover_value):
        if turnover_value >= 200000:
            score += 12; reasons.append("成交值佳")
        elif turnover_value >= 50000:
            score += 6; reasons.append("成交值可")
        elif turnover_value < 20000:
            score -= 6; reasons.append("成交值不足")

    if has_value(vol_ratio):
        if 0.65 <= vol_ratio <= 4.0:
            score += 6; reasons.append("量能可用")
        elif vol_ratio < 0.55:
            score -= 6; reasons.append("量能不足")

    if is_dead_stock(row):
        score -= 18; reasons.append("死股排除")

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
            score += 16; reasons.append("5日動能強")
        elif mom5 > 0:
            score += 10; reasons.append("5日動能正")
        elif mom5 > -0.03:
            score += 4; reasons.append("5日未破壞")

    if has_value(mom20):
        if mom20 > 0.08:
            score += 16; reasons.append("20日動能強")
        elif mom20 > 0:
            score += 10; reasons.append("20日動能正")
        elif mom20 > -0.08:
            score += 4; reasons.append("20日未破壞")

    if has_value(close) and has_value(prev_high_20) and close >= prev_high_20 * 0.98:
        score += 10; reasons.append("接近20日高")

    if has_value(close) and has_value(ma20) and close >= ma20 * 0.97:
        score += 8; reasons.append("靠近MA20")

    if kd_cross or macd_cross:
        score += 16; reasons.append("KD或MACD轉強")

    if has_value(macd_diff) and macd_diff > 0:
        score += 6; reasons.append("MACD偏多")

    return score, "；".join(reasons) if reasons else "動能尚未啟動"


def main_force_stage_score(row):
    latent = layer_latent(row)
    testing = layer_testing(row)
    breakout = layer_breakout(row)

    score = 0
    reasons = []

    if latent:
        score += 18
        reasons.append("潛伏收斂")
    if testing:
        score += 22
        reasons.append("試盤轉強")
    if breakout:
        score += 28
        reasons.append("主升發動")
    if is_dead_stock(row):
        score -= 20
        reasons.append("死股排除")

    stage = "發動" if breakout else "試盤" if testing else "潛伏" if latent else "觀察"
    return score, stage, "；".join(reasons) if reasons else "主力階段未明"


def entry_score(row, market_row=None):
    ss, sr = structure_score(row)
    ms, mr = momentum_score(row)
    fs, stage, fr = main_force_stage_score(row)

    score = ss * 0.32 + ms * 0.36 + fs * 0.32

    r5 = safe_num(row.get("return_5d"))
    close = safe_num(row.get("close"))

    if has_value(r5) and r5 > 0.22:
        score -= 8
    if has_value(close) and close < 10:
        score -= 8
    if is_dead_stock(row):
        score -= 16

    if stage == "發動" and score >= 44:
        action = "BUY"
    elif stage == "試盤" and score >= 34:
        action = "TEST"
    elif stage == "潛伏" and score >= 26:
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
        "latent_layer": latent,
        "testing_layer": testing,
        "breakout_layer": breakout,
        "is_dead_stock": is_dead_stock(row),
        "entry_reason": f"結構:{sr}｜動能:{mr}｜階段:{fr}",
    }


def position_stage(row):
    try:
        return entry_score(row).get("main_force_stage", "未分類")
    except Exception:
        return "未分類"
