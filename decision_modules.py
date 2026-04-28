"""
decision_modules.py
v2.2 main-force behavior version

三層：
READY = 主力佈局：橫盤 + 量微增 + 不破底 / 低位籌碼增加
TEST  = 試盤：KD 或 MACD 任一黃金交叉 + 量能 + MA20附近
BUY   = 發動：趨勢成立 + 量能 + 動能
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
    g = out.groupby("stock_id", group_keys=False)
    for n in [5, 10, 20, 60]:
        out[f"ma{n}"] = g["close"].rolling(n).mean().reset_index(level=0, drop=True)
    out["return_5d"] = g["close"].pct_change(5)
    out["return_10d"] = g["close"].pct_change(10)
    out["return_20d"] = g["close"].pct_change(20)
    out["mom5"] = out["return_5d"]
    out["mom10"] = out["return_10d"]
    out["mom20"] = out["return_20d"]
    out["mom60"] = g["close"].pct_change(60)
    out["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["volume_ma20"] + 1e-9)
    out["prev_low_20"] = g["close"].shift(1).rolling(20).min().reset_index(level=0, drop=True)
    out["prev_high_10"] = g["close"].shift(1).rolling(10).max().reset_index(level=0, drop=True)
    out["prev_high_20"] = g["close"].shift(1).rolling(20).max().reset_index(level=0, drop=True)

    low9 = g["low"].rolling(9).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_k_prev"] = g["kd_k"].shift(1)
    out["kd_d_prev"] = g["kd_d"].shift(1)

    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_diff_prev"] = g["macd_diff"].shift(1)
    out["macd_signal_prev"] = g["macd_signal"].shift(1)
    return out

def kd_gold(row):
    k=safe_num(row.get("kd_k")); d=safe_num(row.get("kd_d"))
    kp=safe_num(row.get("kd_k_prev")); dp=safe_num(row.get("kd_d_prev"))
    return all(has_value(x) for x in [k,d,kp,dp]) and k>d and kp<=dp

def macd_gold(row):
    diff=safe_num(row.get("macd_diff")); sig=safe_num(row.get("macd_signal"))
    diffp=safe_num(row.get("macd_diff_prev")); sigp=safe_num(row.get("macd_signal_prev"))
    return all(has_value(x) for x in [diff,sig,diffp,sigp]) and diff>sig and diffp<=sigp

def chip_up(row):
    for col in ["chip_concentration","concentration","holder_concentration","chip_change","major_holder_change"]:
        if col in row:
            v=safe_num(row.get(col))
            if has_value(v) and v>0:
                return True
    total=0; found=False
    for col in ["foreign_net_buy","invest_trust_net_buy","dealer_net_buy","institution_net_buy"]:
        if col in row:
            total += safe_num(row.get(col), 0)
            found = True
    return found and total > 0

def behavior_flags(row):
    close=safe_num(row.get("close")); ma20=safe_num(row.get("ma20")); ma60=safe_num(row.get("ma60"))
    mom5=safe_num(row.get("mom5")); mom20=safe_num(row.get("mom20")); vr=safe_num(row.get("volume_ratio"))
    low20=safe_num(row.get("prev_low_20"))
    kg=kd_gold(row); mg=macd_gold(row); cross=kg or mg
    not_breakdown = has_value(close) and has_value(low20) and close > low20 * 1.01
    accumulation = has_value(mom5) and abs(mom5) < 0.05 and has_value(vr) and vr > 1.05 and not_breakdown
    testing = cross and has_value(vr) and vr > 1.15 and has_value(close) and has_value(ma20) and close >= ma20 * 0.98
    trend_up = has_value(close) and has_value(ma20) and has_value(ma60) and close > ma20 and ma20 >= ma60 * 0.98
    entry = trend_up and has_value(vr) and vr > 1.25 and (cross or (has_value(mom20) and mom20 > 0.03))
    low_chip = chip_up(row) and has_value(close) and (close < 50 or (has_value(ma60) and close < ma60))
    return {"accumulation": bool(accumulation or low_chip), "testing": bool(testing), "entry": bool(entry),
            "kd_cross": bool(kg), "macd_cross": bool(mg), "chip_up": bool(chip_up(row)), "low_chip_push": bool(low_chip)}

def entry_score(row, market_row=None):
    close=safe_num(row.get("close")); ma20=safe_num(row.get("ma20")); ma60=safe_num(row.get("ma60"))
    mom5=safe_num(row.get("mom5")); mom20=safe_num(row.get("mom20")); vr=safe_num(row.get("volume_ratio"))
    flags=behavior_flags(row)
    score=0; reasons=[]
    if has_value(close) and has_value(ma20):
        if close > ma20:
            score += 16; reasons.append("站上MA20")
        elif close >= ma20*0.98:
            score += 10; reasons.append("貼近MA20")
    if has_value(close) and has_value(ma60) and close > ma60:
        score += 6; reasons.append("站上MA60")
    if flags["kd_cross"] or flags["macd_cross"]:
        score += 20; reasons.append("KD或MACD黃金交叉")
    if has_value(mom5):
        if mom5 > 0:
            score += 8; reasons.append("5日動能正")
        elif abs(mom5) < 0.05:
            score += 6; reasons.append("橫盤整理")
    if has_value(mom20):
        if mom20 > 0:
            score += 8; reasons.append("20日動能正")
        elif mom20 > -0.03:
            score += 4; reasons.append("20日動能中性")
    if has_value(vr):
        if vr > 1.25:
            score += 10; reasons.append("量能放大")
        elif vr > 1.05:
            score += 6; reasons.append("量能微增")
    if flags["accumulation"]:
        score += 15; reasons.append("主力佈局型")
    if flags["testing"]:
        score += 18; reasons.append("試盤型")
    if flags["entry"]:
        score += 18; reasons.append("發動型")
    if flags["low_chip_push"]:
        score += 12; reasons.append("低位籌碼增加")
    r5=safe_num(row.get("return_5d")); r10=safe_num(row.get("return_10d"))
    if has_value(r5) and r5 > 0.22:
        score -= 10; reasons.append("5日偏熱扣分")
    if has_value(r10) and r10 > 0.35:
        score -= 10; reasons.append("10日偏熱扣分")
    if flags["entry"] and score >= 55:
        action="BUY"; stage="發動"
    elif flags["testing"] and score >= 45:
        action="TEST"; stage="試盤"
    elif flags["accumulation"] and score >= 25:
        action="READY"; stage="佈局"
    elif score >= 55:
        action="TEST"; stage="試盤"
    elif score >= 35:
        action="READY"; stage="佈局"
    else:
        action="SKIP"; stage="無訊號"
    return {"entry_score": int(round(score)), "entry_action": action, "main_force_stage": stage,
            "entry_reason": "；".join(reasons) if reasons else "條件不足",
            "kd_cross": flags["kd_cross"], "macd_cross": flags["macd_cross"],
            "chip_up": flags["chip_up"], "low_chip_push": flags["low_chip_push"]}

def position_stage(row):
    try:
        return entry_score(row).get("main_force_stage", "未分類")
    except Exception:
        return "未分類"
