"""
decision_modules.py
v1.0 modular decision layer

用途：
- 給 v1_stable_pipeline.py 呼叫
- 不取代原本選股策略，只負責進場品質評分與持倉判斷輔助
- 所有函式都容錯：缺欄位時不讓 pipeline 爆掉
"""

import math
import numpy as np
import pandas as pd


# =========================
# Basic helpers
# =========================

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


# =========================
# Feature engineering
# =========================

def add_decision_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    加上決策模組需要的指標：
    - MA5 / MA10 / MA20 / MA60
    - return_5d / return_10d / return_20d
    - volume MA20 / volume ratio
    - KD 粗略版
    - MACD

    注意：
    如果 price_panel 沒有 high / low，KD 會用 close 近似計算，不會中斷。
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)

    if "volume" not in out.columns:
        out["volume"] = np.nan
    if "high" not in out.columns:
        out["high"] = out["close"]
    if "low" not in out.columns:
        out["low"] = out["close"]

    g = out.groupby("stock_id", group_keys=False)

    out["ma5"] = g["close"].rolling(5).mean().reset_index(level=0, drop=True)
    out["ma10"] = g["close"].rolling(10).mean().reset_index(level=0, drop=True)
    out["ma20"] = g["close"].rolling(20).mean().reset_index(level=0, drop=True)
    out["ma60"] = g["close"].rolling(60).mean().reset_index(level=0, drop=True)

    out["return_5d"] = g["close"].pct_change(5)
    out["return_10d"] = g["close"].pct_change(10)
    out["return_20d"] = g["close"].pct_change(20)

    out["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / out["volume_ma20"].replace(0, np.nan)

    low9 = g["low"].rolling(9).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9).replace(0, np.nan) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(alpha=1/3, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(alpha=1/3, adjust=False).mean().reset_index(level=0, drop=True)

    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_dea"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_dea"]

    return out


# =========================
# Decision score modules
# =========================

def market_score(market_row=None) -> tuple[int, str]:
    """
    大盤開關。
    目前如果沒有指數資料，預設不加不扣，避免阻斷原系統。
    未來可傳入 index row：close / ma20 / ma60。
    """
    if market_row is None:
        return 0, "大盤資料缺省：不加不扣"

    close = safe_num(market_row.get("close"))
    ma20 = safe_num(market_row.get("ma20"))
    ma60 = safe_num(market_row.get("ma60"))

    if not has_value(close):
        return 0, "大盤收盤缺值"
    if has_value(ma60) and close < ma60:
        return -30, "大盤跌破MA60：防守"
    if has_value(ma20) and close < ma20:
        return -15, "大盤跌破MA20：保守"
    if has_value(ma20) and close >= ma20:
        return 20, "大盤站上MA20：允許進場"
    return 0, "大盤條件中性"


def trend_score(row) -> tuple[int, str]:
    score = 0
    reasons = []

    close = safe_num(row.get("close"))
    ma5 = safe_num(row.get("ma5"))
    ma10 = safe_num(row.get("ma10"))
    ma20 = safe_num(row.get("ma20"))

    if has_value(close) and has_value(ma20) and close > ma20:
        score += 12
        reasons.append("收盤站上MA20")
    if all(has_value(x) for x in [ma5, ma10, ma20]) and ma5 > ma10 > ma20:
        score += 13
        reasons.append("MA5>MA10>MA20")

    return score, "；".join(reasons) if reasons else "趨勢未確認"


def momentum_score(row) -> tuple[int, str]:
    score = 0
    reasons = []

    kd_k = safe_num(row.get("kd_k"))
    kd_d = safe_num(row.get("kd_d"))
    macd_diff = safe_num(row.get("macd_diff"))
    macd_hist = safe_num(row.get("macd_hist"))
    mom5 = safe_num(row.get("mom5"))
    mom20 = safe_num(row.get("mom20"))

    if has_value(mom5) and mom5 > 0:
        score += 5
        reasons.append("5日動能為正")
    if has_value(mom20) and mom20 > 0:
        score += 5
        reasons.append("20日動能為正")
    if all(has_value(x) for x in [kd_k, kd_d]) and kd_k > kd_d and kd_k < 85:
        score += 5
        reasons.append("KD偏多且未過熱")
    if all(has_value(x) for x in [macd_diff, macd_hist]) and macd_diff > 0 and macd_hist > 0:
        score += 5
        reasons.append("MACD偏多")

    return score, "；".join(reasons) if reasons else "動能不足"


def chip_score(row) -> tuple[int, str]:
    """
    籌碼模組。
    若資料欄位不存在，不扣分；未來若 price_panel 合併籌碼欄位，即可自動生效。
    支援欄位：
    - chip_concentration / concentration / holder_concentration
    - foreign_net_buy / invest_trust_net_buy / dealer_net_buy
    - margin_growth / margin_balance_growth
    """
    score = 0
    reasons = []

    concentration = np.nan
    for col in ["chip_concentration", "concentration", "holder_concentration"]:
        if col in row:
            concentration = safe_num(row.get(col))
            break
    if has_value(concentration) and concentration > 0:
        score += 10
        reasons.append("籌碼集中度上升")

    inst_buy = 0
    found_inst = False
    for col in ["foreign_net_buy", "invest_trust_net_buy", "dealer_net_buy"]:
        if col in row:
            v = safe_num(row.get(col), 0)
            if has_value(v):
                inst_buy += v
                found_inst = True
    if found_inst and inst_buy > 0:
        score += 8
        reasons.append("法人籌碼偏買")

    margin_growth = np.nan
    for col in ["margin_growth", "margin_balance_growth"]:
        if col in row:
            margin_growth = safe_num(row.get(col))
            break
    if has_value(margin_growth) and margin_growth <= 0.10:
        score += 7
        reasons.append("融資未失控")

    return score, "；".join(reasons) if reasons else "籌碼資料缺省或中性"


def risk_penalty(row) -> tuple[int, str]:
    penalty = 0
    reasons = []

    r5 = safe_num(row.get("return_5d"))
    r10 = safe_num(row.get("return_10d"))
    kd_k = safe_num(row.get("kd_k"))
    volume_ratio = safe_num(row.get("volume_ratio"))

    if has_value(r5) and r5 > 0.15:
        penalty -= 20
        reasons.append("5日漲幅過熱")
    if has_value(r10) and r10 > 0.25:
        penalty -= 20
        reasons.append("10日漲幅過熱")
    if has_value(kd_k) and kd_k > 90:
        penalty -= 10
        reasons.append("KD過熱")
    if has_value(volume_ratio) and volume_ratio > 4:
        penalty -= 10
        reasons.append("爆量風險")

    return penalty, "；".join(reasons) if reasons else "無明顯過熱"


def entry_score(row, market_row=None) -> dict:
    """
    統一入口：回傳總分、動作與理由。
    分數設計：
    - market: -30 ~ +20
    - trend: 0 ~ +25
    - momentum: 0 ~ +20
    - chip: 0 ~ +25
    - risk: 0 ~ -60
    """
    ms, mr = market_score(market_row)
    ts, tr = trend_score(row)
    mos, mor = momentum_score(row)
    cs, cr = chip_score(row)
    rp, rr = risk_penalty(row)

    total = ms + ts + mos + cs + rp

    if total >= 80:
        action = "BUY"
    elif total >= 60:
        action = "WATCH"
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
        "entry_reason": f"大盤:{mr}｜趨勢:{tr}｜動能:{mor}｜籌碼:{cr}｜風險:{rr}",
    }


def position_stage(row) -> str:
    """
    持有階段判斷：短線 / 波段 / 中長 / 防守。
    """
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
