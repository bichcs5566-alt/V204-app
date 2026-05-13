"""
v266_strategy_engine.py
雙策略版：CORE 早期卡位 + ALPHA 高流動性強勢延續

設計原則：
1. 保留原本輸出檔名，不影響後面 pipeline：
   - core_candidates.csv
   - alpha_candidates.csv
   - candidates.csv
   - trade_plan.csv
   - selection_debug.csv
   - meta.json

2. 新增真正雙策略：
   - CORE：Early Entry / 早期卡位，允許中低流動性，但控倉
   - ALPHA：Trend Momentum / 高流動性強勢股，優先放大資金

3. 新增流動性欄位：
   - turnover：成交金額估算 close * volume * 1000
   - liquidity_score
   - liquidity_level：LOW / MEDIUM / HIGH
   - liquidity_tag：低流動性 / 中流動性 / 高流動性

4. 不依賴新資料欄位；只用 feature_panel_daily.csv 既有欄位。
"""

from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CAPITAL = 1_000_000

def taipei_now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def price_tier(p):
    p = float(p)
    if p < 50:
        return "50以下"
    if p < 100:
        return "50-100"
    if p < 300:
        return "100-300"
    if p < 500:
        return "300-500"
    if p < 1000:
        return "500-1000"
    return "1000以上"


def next_trade_date(signal_date):
    d = pd.to_datetime(signal_date) + pd.Timedelta(days=1)
    if d.weekday() == 5:
        d += pd.Timedelta(days=2)
    elif d.weekday() == 6:
        d += pd.Timedelta(days=1)
    return d


def write_both(df, name):
    df.to_csv(ROOT / name, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / name, index=False, encoding="utf-8-sig")


def safe_num(s, default=np.nan):
    try:
        return pd.to_numeric(s, errors="coerce")
    except Exception:
        return default


def load_feature():
    p = ROOT / "feature_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR / "feature_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        raise FileNotFoundError("feature_panel_daily.csv not found")

    df = pd.read_csv(p)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.zfill(4)
    return df


def latest_valid(df):
    latest_date = df["date"].max()
    x = df[
        (df["date"] == latest_date)
        & (df["has_60d_history"].astype(str).str.lower().isin(["true", "1"]))
    ].copy()

    numeric = [
        "open", "high", "low", "close", "volume",
        "mom3", "mom5", "mom10", "mom20", "mom60",
        "ma5", "ma10", "ma20", "ma60",
        "vol20", "volume_ratio", "vol_dry_ratio",
        "high_20", "low_20", "high_60", "low_60",
        "range_20", "ma_converge_pct", "ma20_slope",
        "kd_cross", "macd_cross", "macd_diff",
        "obv_mom5", "obv_up_count_5", "low_non_down_count_5",
    ]

    for c in numeric:
        x[c] = pd.to_numeric(x.get(c), errors="coerce")

    x = x.dropna(subset=["close", "volume", "mom20", "ma20", "ma60"])
    x = x[(x["close"] > 0) & (x["volume"] > 0)].copy()

    return latest_date, add_liquidity_fields(x)


def add_liquidity_fields(d):
    d = d.copy()

    # 台股 volume 常見是張數；成交金額估算 close * volume * 1000。
    d["turnover"] = d["close"] * d["volume"] * 1000

    # liquidity_score：以分位數做相對評分，比固定門檻更穩。
    vol_rank = d["volume"].rank(pct=True).fillna(0)
    turnover_rank = d["turnover"].rank(pct=True).fillna(0)
    d["liquidity_score"] = (vol_rank * 50 + turnover_rank * 50).round(2)

    high_liq = (d["volume"] >= 3000) | (d["turnover"] >= 80_000_000) | (d["liquidity_score"] >= 75)
    mid_liq = (d["volume"] >= 1000) | (d["turnover"] >= 30_000_000) | (d["liquidity_score"] >= 45)

    d["liquidity_level"] = np.where(high_liq, "HIGH", np.where(mid_liq, "MEDIUM", "LOW"))
    d["liquidity_tag"] = d["liquidity_level"].map({
        "HIGH": "高流動性",
        "MEDIUM": "中流動性",
        "LOW": "低流動性",
    })

    return d


def detect_regime(x):
    pct_ma20 = float((x["close"] >= x["ma20"]).mean())
    pct_ma60 = float((x["close"] >= x["ma60"]).mean())
    pct_mom20 = float((x["mom20"] > 0).mean())
    pct_strong = float(((x["mom20"] > 0.08) & (x["close"] >= x["high_60"] * 0.92)).mean())
    med_mom20 = float(x["mom20"].median())

    score = (
        int(pct_ma20 >= 0.55)
        + int(pct_ma60 >= 0.50)
        + int(pct_mom20 >= 0.50)
        + int(pct_strong >= 0.08)
        + int(med_mom20 > 0.015)
    )

    if pct_ma60 < 0.35 and pct_mom20 < 0.35:
        regime = "BEAR"
    elif score >= 4:
        regime = "TREND"
    else:
        regime = "RANGE"

    return regime, {
        "pct_above_ma20": round(pct_ma20, 4),
        "pct_above_ma60": round(pct_ma60, 4),
        "pct_mom20_pos": round(pct_mom20, 4),
        "pct_strong": round(pct_strong, 4),
        "median_mom20": round(med_mom20, 4),
        "regime_score": score,
    }


def set_action(df, buy, test, watch, buy_sub, test_sub, watch_sub):
    df["action"] = "SKIP"
    df.loc[watch, "action"] = "WATCH"
    df.loc[test, "action"] = "TEST"
    df.loc[buy, "action"] = "BUY"

    df["action_label"] = df["action"].map({
        "BUY": "買進",
        "TEST": "試單",
        "WATCH": "觀察",
        "SKIP": "排除",
    }).fillna("排除")

    df["action_sub"] = "條件不足"
    df.loc[df["action"] == "BUY", "action_sub"] = buy_sub
    df.loc[df["action"] == "TEST", "action_sub"] = test_sub
    df.loc[df["action"] == "WATCH", "action_sub"] = watch_sub




# ===== v309 ATTACK-FIRST CORE PATCH / 攻擊優先核心修正 =====
# 目的：不是 UI 修正；這段只處理策略名單生成與排序。
# 核心：TEST / BUY / TOP 候選必須先有攻擊結構，避免高流動、均線貼近、金融、箱型牛皮股排前面。
def apply_attack_first_v309(d, mode="CORE"):
    """
    v312 BALANCED ATTACK CORE

    目標：
    - 不再全擋成 BLOCK。
    - TEST：真正有攻擊條件的標的。
    - WATCH：準攻擊 / 轉強中，但還不夠試單。
    - BLOCK：金融、防守牛皮、底部修復、短線轉弱、低信心。
    - 不動 UI、不動 app.js、不動持倉、不動 workflow。
    """
    import numpy as np
    import pandas as pd

    if d is None or len(d) == 0:
        return d

    d = d.copy()
    mode = str(mode or "CORE").upper()

    def n(col, default=0.0):
        if col in d.columns:
            return pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=d.index, dtype="float64")

    def s(col, default=""):
        if col in d.columns:
            return d[col].astype(str).fillna(default)
        return pd.Series(default, index=d.index, dtype="object")

    close = n("close")
    open_ = n("open", close)
    high = n("high", close)
    low = n("low", close)
    volume = n("volume")
    turnover = n("turnover", close * volume * 1000)

    mom3 = n("mom3")
    mom5 = n("mom5")
    mom10 = n("mom10")
    mom20 = n("mom20")

    ma5 = n("ma5", close)
    ma10 = n("ma10", close)
    ma20 = n("ma20", close)
    ma60 = n("ma60", close)
    ma20_slope = n("ma20_slope")

    high20 = n("high_20", high)
    low20 = n("low_20", low)
    high60 = n("high_60", high)

    ma_conv = n("ma_converge_pct", 999)
    vol_ratio = n("volume_ratio", 1)
    liq_score = n("liquidity_score")
    main_force = n("main_force_score_v300")
    chip_score = n("chip_score")
    obv = n("obv_mom5")
    obv_up5 = n("obv_up_count_5")
    low_hold = n("low_non_down_count_5")

    sid = s("stock_id")
    industry = s("industry")

    finance_like = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

    # 防守類不直接全擋；只在沒有攻擊分時降級，避免誤殺真正轉強股。
    defensive_like = industry.str.contains("航運|觀光|百貨|食品|水泥|塑膠|鋼鐵|紡織", na=False)

    ma5_vs_ma10 = (ma5 / ma10.replace(0, np.nan) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    ma10_vs_ma20 = (ma10 / ma20.replace(0, np.nan) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    close_ma5_gap = (close / ma5.replace(0, np.nan) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    close_ma10_gap = (close / ma10.replace(0, np.nan) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    close_ma20_gap = (close / ma20.replace(0, np.nan) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    close_to_high20 = (close / high20.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
    close_to_high60 = (close / high60.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
    range20 = ((high20 - low20) / low20.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
    box_pos20 = ((close - low20) / (high20 - low20).replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)

    candle_power = ((close - open_) / open_.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)

    # ===== v312 條件：不再要求全部同時完美，但一定要有方向 =====
    trend_ok = (
        ((close >= ma5 * 0.990) & (ma5_vs_ma10 > -0.004) & (ma10_vs_ma20 > -0.010)) |
        ((close >= ma20 * 1.005) & (mom10 > 0.018))
    )

    momentum_ok = (
        ((mom5 > 0.006) & (mom10 > 0.014)) |
        ((mom10 > 0.026) & (mom20 > 0.012))
    ) & (mom20 <= 0.42)

    breakout_ok = (
        (close_to_high20 >= 0.945) |
        ((box_pos20 >= 0.66) & (close_to_high20 >= 0.925)) |
        ((close_ma20_gap >= 0.015) & (mom10 > 0.022))
    )

    volume_ok = (
        (vol_ratio >= 1.08) |
        ((main_force >= 58) & (mom10 > 0.018)) |
        ((turnover >= 30_000_000) & (mom5 > 0))
    )

    chip_ok = (
        (main_force >= 48) |
        (chip_score >= 55) |
        (obv > 0) |
        (obv_up5 >= 2) |
        (low_hold >= 3)
    )

    short_turn_weak = (
        ((close < ma5 * 0.985) & (mom3 <= 0) & (mom5 <= 0)) |
        ((mom5 < -0.012) & (ma5_vs_ma10 < -0.008))
    )

    bottom_repair_only = (
        (close_to_high20 < 0.910) &
        (box_pos20 < 0.62) &
        (mom10 < 0.018) &
        (vol_ratio < 1.30)
    )

    ma_sticky_no_attack = (
        (ma_conv <= 0.12) &
        (range20 < 0.095) &
        (mom10 < 0.018) &
        (mom20 < 0.050) &
        (vol_ratio < 1.25) &
        (box_pos20 < 0.72)
    )

    box_middle = (
        (box_pos20 < 0.58) &
        (close_to_high20 < 0.920) &
        (close_ma20_gap.between(-0.030, 0.050)) &
        (vol_ratio < 1.25)
    )

    liquidity_only = (
        (liq_score >= 65) &
        (mom5 <= 0.006) &
        (mom10 <= 0.014) &
        (vol_ratio < 1.15) &
        (main_force < 50) &
        (box_pos20 < 0.75)
    )

    fake_breakout = (
        ((upper_shadow > 0.075) & (vol_ratio > 1.50)) |
        (mom20 > 0.52) |
        (close_ma20_gap > 0.32)
    )

    low_confidence = (
        (main_force < 35) &
        (chip_score < 35) &
        (obv <= 0) &
        (obv_up5 < 1)
    )

    hard_reject = finance_like | short_turn_weak | bottom_repair_only | ma_sticky_no_attack | box_middle | liquidity_only | fake_breakout | low_confidence

    attack = pd.Series(0.0, index=d.index)
    attack += trend_ok.astype(int) * 20
    attack += momentum_ok.astype(int) * 22
    attack += breakout_ok.astype(int) * 22
    attack += volume_ok.astype(int) * 18
    attack += chip_ok.astype(int) * 14

    attack += (mom3 > 0.002).astype(int) * 4
    attack += (mom5 > 0.014).astype(int) * 7
    attack += (mom10 > 0.030).astype(int) * 9
    attack += (close_to_high20 >= 0.975).astype(int) * 9
    attack += (vol_ratio.between(1.25, 4.50)).astype(int) * 9
    attack += (candle_power > 0.002).astype(int) * 4
    attack += (main_force >= 62).astype(int) * 7
    attack += (chip_score >= 65).astype(int) * 5
    attack += ((low_hold >= 3) & (mom5 > 0)).astype(int) * 4

    attack -= finance_like.astype(int) * 120
    attack -= defensive_like.astype(int) * 10
    attack -= short_turn_weak.astype(int) * 60
    attack -= bottom_repair_only.astype(int) * 52
    attack -= ma_sticky_no_attack.astype(int) * 48
    attack -= box_middle.astype(int) * 42
    attack -= liquidity_only.astype(int) * 38
    attack -= fake_breakout.astype(int) * 36
    attack -= low_confidence.astype(int) * 28
    attack += (liq_score >= 70).astype(int) * 2

    # TEST：三大核心至少二個成立，且不可硬拒絕。
    core_hits = trend_ok.astype(int) + momentum_ok.astype(int) + breakout_ok.astype(int) + volume_ok.astype(int) + chip_ok.astype(int)

    strict_test_ok = (
        (~hard_reject) &
        (attack >= 60) &
        (core_hits >= 4) &
        (momentum_ok | breakout_ok) &
        (volume_ok | chip_ok)
    )

    # WATCH：有方向、有準備，但未達 TEST。
    watch_ok = (
        (~hard_reject) &
        (attack >= 42) &
        (core_hits >= 3) &
        (trend_ok | momentum_ok | breakout_ok)
    )

    d["attack_score_v309"] = attack.round(2)
    d["attack_score_v310"] = attack.round(2)
    d["attack_score_v312"] = attack.round(2)
    d["final_attack_score_v309"] = attack.round(2)
    d["final_attack_score_v310"] = attack.round(2)
    d["final_attack_score_v312"] = attack.round(2)

    d["trend_ok_v310"] = trend_ok.astype(int)
    d["momentum_ok_v310"] = momentum_ok.astype(int)
    d["breakout_ok_v310"] = breakout_ok.astype(int)
    d["volume_ok_v310"] = volume_ok.astype(int)
    d["chip_ok_v310"] = chip_ok.astype(int)
    d["strict_test_ok_v310"] = strict_test_ok.astype(int)
    d["watch_ok_v310"] = watch_ok.astype(int)

    d["hard_reject_v309"] = hard_reject.astype(int)
    d["hard_reject_v310"] = hard_reject.astype(int)
    d["hard_reject_v312"] = hard_reject.astype(int)
    d["short_turn_weak_v309"] = short_turn_weak.astype(int)
    d["ma_sticky_no_attack_v309"] = ma_sticky_no_attack.astype(int)
    d["box_middle_v309"] = box_middle.astype(int)
    d["liquidity_only_v309"] = liquidity_only.astype(int)
    d["bottom_repair_only_v310"] = bottom_repair_only.astype(int)

    if "entry_score" not in d.columns:
        d["entry_score"] = 0
    d["entry_score"] = pd.to_numeric(d["entry_score"], errors="coerce").fillna(0)
    d["final_sort_score_v309"] = (d["entry_score"] * 0.20 + attack * 0.80).round(2)
    d["final_sort_score_v310"] = d["final_sort_score_v309"]
    d["final_sort_score_v312"] = d["final_sort_score_v309"]

    # 這裡先分類一次；最終仍由 apply_v311_final_action_lock 再鎖一次。
    if "action" in d.columns:
        d.loc[strict_test_ok, "action"] = "TEST"
        d.loc[(~strict_test_ok) & watch_ok, "action"] = "WATCH"
        d.loc[(~strict_test_ok) & (~watch_ok), "action"] = "BLOCK"

        d.loc[d["action"].astype(str).str.upper().eq("TEST"), "action_label"] = "試單"
        d.loc[d["action"].astype(str).str.upper().eq("WATCH"), "action_label"] = "觀察"
        d.loc[d["action"].astype(str).str.upper().eq("BLOCK"), "action_label"] = "禁止"

        d.loc[d["action"].astype(str).str.upper().eq("TEST"), "action_sub"] = "v312：攻擊條件達標，最大機會試單"
        d.loc[d["action"].astype(str).str.upper().eq("WATCH"), "action_sub"] = "v312：準攻擊，優先觀察"
        d.loc[d["action"].astype(str).str.upper().eq("BLOCK"), "action_sub"] = "v312：非攻擊型或風險過高，禁止"

    return d.sort_values(["final_sort_score_v312", "attack_score_v312", "entry_score", "stock_id"], ascending=[False, False, False, True])




def core_engine(x):
    """
    CORE：早期卡位策略。
    目的：抓剛轉強、尚未完全爆量的股票。
    風控：低流動性只能 TEST，不讓它直接大部位 BUY。
    """
    d = x.copy()
    d["strategy_type"] = "CORE"
    d["strategy_name"] = "CORE Early Entry"
    d["entry_score"] = 0.0

    # 原本強勢/轉強概念保留
    d["entry_score"] += (d["mom10"] > 0.025).astype(int) * 10
    d["entry_score"] += (d["mom20"] > 0.045).astype(int) * 14
    d["entry_score"] += (d["mom60"] > 0.08).astype(int) * 8
    d["entry_score"] += (d["close"] > d["ma20"] * 0.985).astype(int) * 10
    d["entry_score"] += (d["ma5"] >= d["ma20"] * 0.995).astype(int) * 8
    d["entry_score"] += (d["ma20"] >= d["ma60"] * 0.98).astype(int) * 8
    d["entry_score"] += (d["close"] >= d["high_60"] * 0.88).astype(int) * 8

    # 早期卡位：不追爆量，吃量能回溫
    d["entry_score"] += d["volume_ratio"].between(1.05, 4.5).astype(int) * 10
    d["entry_score"] += (d["volume"] >= 300).astype(int) * 5
    d["entry_score"] += (d["volume"] >= 800).astype(int) * 4

    # 結構加分
    d["entry_score"] += (d["ma_converge_pct"] <= 0.12).astype(int) * 6
    d["entry_score"] += (d["low_non_down_count_5"] >= 3).astype(int) * 5

    # 風險扣分
    d["entry_score"] -= (d["close"] < 10).astype(int) * 16
    d["entry_score"] -= (d["close"] < 20).astype(int) * 8
    d["entry_score"] -= (d["volume"] < 1000).astype(int) * 30
    d["entry_score"] -= (d["mom20"] > 0.40).astype(int) * 10
    d["entry_score"] -= (d["volume_ratio"] > 5.5).astype(int) * 8

    core_liq_ok = (d["volume"] >= 1000) & d["liquidity_level"].isin(["MEDIUM", "HIGH"])
    low_liq = d["liquidity_level"].eq("LOW")

    buy = (
        (d["entry_score"] >= 58)
        & (d["mom20"] > 0.05)
        & (d["close"] > d["ma20"])
        & (d["close"] >= 20)
        & core_liq_ok
    )

    # 低流動性即使分數夠，也只允許試單，避免你資金被卡住。
    test = (
        (d["entry_score"] >= 44)
        & ~buy
        & (d["mom10"] > 0.01)
        & (d["close"] > d["ma20"] * 0.97)
        & (d["volume"] >= 1000)
    )

    watch = (d["entry_score"] >= 34) & ~buy & ~test

    set_action(d, buy, test, watch, "早期卡位", "低量試單", "早期觀察")

    d["note"] = (
        "CORE早期卡位｜剛轉強｜靠近MA20｜量能回溫｜"
        + d["liquidity_tag"].astype(str)
    )

    d = apply_attack_first_v309(d, mode="CORE")
    return d.sort_values(["final_sort_score_v309", "attack_score_v309", "entry_score", "mom20", "mom10"], ascending=[False, False, False, False, False])


def alpha_engine(x):
    """
    ALPHA：高流動性強勢延續策略。
    目的：只挑成交量/成交金額夠大的主流強勢股。
    """
    d = x.copy()
    d["strategy_type"] = "ALPHA"
    d["strategy_name"] = "ALPHA Trend Momentum"
    d["entry_score"] = 0.0

    high_liq = d["liquidity_level"].eq("HIGH")
    mid_or_high = d["liquidity_level"].isin(["MEDIUM", "HIGH"])

    # 流動性是 ALPHA 的第一門檻
    d["entry_score"] += high_liq.astype(int) * 20
    d["entry_score"] += (d["volume"] >= 3000).astype(int) * 10
    d["entry_score"] += (d["turnover"] >= 80_000_000).astype(int) * 10

    # 強勢延續
    d["entry_score"] += (d["mom5"] > 0.015).astype(int) * 8
    d["entry_score"] += (d["mom10"] > 0.035).astype(int) * 10
    d["entry_score"] += (d["mom20"] > 0.07).astype(int) * 12
    d["entry_score"] += (d["mom60"] > 0.12).astype(int) * 6

    # 趨勢結構
    d["entry_score"] += (d["close"] > d["ma20"]).astype(int) * 8
    d["entry_score"] += (d["ma20"] > d["ma60"]).astype(int) * 8
    d["entry_score"] += (d["ma20_slope"] > 0).astype(int) * 6

    # 突破/接近高點
    d["entry_score"] += (d["close"] >= d["high_20"] * 0.995).astype(int) * 10
    d["entry_score"] += (d["close"] >= d["high_60"] * 0.94).astype(int) * 6

    # 量價確認
    d["entry_score"] += (d["volume_ratio"] >= 1.25).astype(int) * 8
    d["entry_score"] += d["volume_ratio"].between(1.25, 6.0).astype(int) * 6

    # 避免過熱
    d["entry_score"] -= (d["mom20"] > 0.55).astype(int) * 12
    d["entry_score"] -= (d["volume_ratio"] > 8.0).astype(int) * 10
    d["entry_score"] -= (~mid_or_high).astype(int) * 30

    buy = (
        (d["entry_score"] >= 70)
        & high_liq
        & (d["close"] > d["ma20"])
        & (d["ma20"] > d["ma60"])
        & (d["mom10"] > 0.03)
        & (d["volume_ratio"] >= 1.25)
    )

    test = (
        (d["entry_score"] >= 58)
        & ~buy
        & mid_or_high
        & (d["close"] > d["ma20"])
        & (d["mom5"] > 0)
    )

    watch = (d["entry_score"] >= 46) & ~buy & ~test

    set_action(d, buy, test, watch, "高流動性強勢買進", "強勢試單", "高流動性觀察")

    d["note"] = (
        "ALPHA高流動性強勢延續｜成交量/成交金額優先｜突破/趨勢確認｜"
        + d["liquidity_tag"].astype(str)
    )

    d = apply_attack_first_v309(d, mode="ALPHA")
    return d.sort_values(["final_sort_score_v309", "attack_score_v309", "entry_score", "mom20", "liquidity_score"], ascending=[False, False, False, False, False])



# ===== v311 FINAL ACTION OUTPUT LOCK =====
# 目的：防止前面 v310 判斷完成後，build_trade_plan / 後段輸出再把 TEST/WATCH/BLOCK 洗掉。
# 只鎖 action / action_label / action_sub / 排序，不動 UI、不動 app.js、不動持倉。
def apply_v311_final_action_lock(df):
    """
    v312 balanced final action lock.
    延續 v311 欄位名稱，讓 app.js 不用再改。
    """
    import numpy as np
    import pandas as pd

    if df is None or len(df) == 0:
        return df

    d = df.copy()

    def _num(col, default=0):
        if col in d.columns:
            return pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=d.index, dtype="float64")

    def _txt(col, default=""):
        if col in d.columns:
            return d[col].astype(str).fillna(default)
        return pd.Series(default, index=d.index, dtype="object")

    sid = _txt("stock_id")
    industry = _txt("industry")

    attack = _num("attack_score_v312")
    if float(attack.abs().sum()) == 0:
        attack = _num("attack_score_v310")
    if float(attack.abs().sum()) == 0:
        attack = _num("attack_score_v309")

    final_sort = _num("final_sort_score_v312")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num("final_sort_score_v310")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num("final_sort_score_v309")

    hard_reject = _num("hard_reject_v312")
    if float(hard_reject.abs().sum()) == 0:
        hard_reject = _num("hard_reject_v310")
    if float(hard_reject.abs().sum()) == 0:
        hard_reject = _num("hard_reject_v309")

    strict_test = _num("strict_test_ok_v310")
    watch_ok = _num("watch_ok_v310")

    finance_like = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

    # 金融維持 BLOCK；其他防守產業不再整批封殺，由 attack / hard_reject 判斷。
    is_test = (strict_test >= 1) & (hard_reject < 1) & (~finance_like) & (attack >= 60)
    is_watch = (~is_test) & (watch_ok >= 1) & (hard_reject < 1) & (~finance_like) & (attack >= 42)

    d["v311_locked_action"] = np.where(is_test, "TEST", np.where(is_watch, "WATCH", "BLOCK"))
    d["action"] = d["v311_locked_action"]

    d["action_label"] = np.where(
        is_test, "試單",
        np.where(is_watch, "觀察", "禁止")
    )
    d["action_sub"] = np.where(
        is_test, "v312鎖定：攻擊條件達標，最大機會試單",
        np.where(is_watch, "v312鎖定：準攻擊，優先觀察", "v312鎖定：非攻擊型或風險過高，禁止")
    )

    d["priority"] = np.where(d["action"].eq("TEST"), 1, np.where(d["action"].eq("WATCH"), 2, 9))

    for c in ["top_opportunity", "section_top_opportunity", "opportunity_rank", "top_reason", "top_rank_v3066", "is_top_v3066"]:
        if c not in d.columns:
            d[c] = ""

    d["top_opportunity"] = ""
    d["section_top_opportunity"] = ""
    d["opportunity_rank"] = ""
    d["top_reason"] = ""
    d["top_rank_v3066"] = 9999
    d["is_top_v3066"] = 0

    test_idx = (
        d.loc[d["action"].eq("TEST")]
        .sort_values(["attack_score_v312", "final_sort_score_v312", "entry_score", "stock_id"],
                     ascending=[False, False, False, True])
        .head(5)
        .index
    )
    for rank, idx in enumerate(test_idx, start=1):
        d.loc[idx, "top_opportunity"] = f"🔥TOP{rank}"
        d.loc[idx, "section_top_opportunity"] = f"TOP{rank}_TEST"
        d.loc[idx, "opportunity_rank"] = str(rank)
        d.loc[idx, "top_rank_v3066"] = rank
        d.loc[idx, "is_top_v3066"] = 1
        d.loc[idx, "top_reason"] = "v312平衡TOP｜攻擊條件達標｜排除金融/橫盤/低信心"

    watch_idx = (
        d.loc[d["action"].eq("WATCH")]
        .sort_values(["attack_score_v312", "final_sort_score_v312", "entry_score", "stock_id"],
                     ascending=[False, False, False, True])
        .head(5)
        .index
    )
    for rank, idx in enumerate(watch_idx, start=1):
        d.loc[idx, "top_opportunity"] = f"觀察TOP{rank}"
        d.loc[idx, "section_top_opportunity"] = f"TOP{rank}_WATCH"
        d.loc[idx, "opportunity_rank"] = str(rank)
        d.loc[idx, "top_rank_v3066"] = rank
        d.loc[idx, "is_top_v3066"] = 1
        d.loc[idx, "top_reason"] = "v312觀察TOP｜準攻擊但未達試單"

    d = d.sort_values(
        ["priority", "top_rank_v3066", "attack_score_v312", "final_sort_score_v312", "entry_score", "stock_id"],
        ascending=[True, True, False, False, False, True]
    )

    return d


def build_trade_plan(core, alpha, regime, signal_date):
    """
    v311 最終輸出鎖定版：
    - 不再用舊 parts/head 規則把 TEST 洗回來。
    - 先套 apply_v311_final_action_lock，再依 locked action 輸出。
    - TEST 只來自 strict_test_ok_v310。
    - WATCH 只來自 watch_ok_v310。
    - 其餘全部 BLOCK，不給試單。
    """
    core = apply_v311_final_action_lock(core)
    alpha = apply_v311_final_action_lock(alpha)

    pool = pd.concat([
        alpha.assign(engine="ALPHA"),
        core.assign(engine="CORE"),
    ], ignore_index=True)

    if pool.empty:
        return pd.DataFrame()

    # 再鎖一次，避免 concat 或缺欄後被洗掉
    pool = apply_v311_final_action_lock(pool)

    # 最終輸出：TEST 優先，其次 WATCH；BLOCK 只輸出部分給前端禁止清單
    test_pool = pool[pool["action"].astype(str).str.upper().eq("TEST")].copy()
    watch_pool = pool[pool["action"].astype(str).str.upper().eq("WATCH")].copy()
    block_pool = pool[pool["action"].astype(str).str.upper().eq("BLOCK")].copy()

    test_pool = test_pool.sort_values(
        ["attack_score_v310", "final_sort_score_v310", "entry_score", "stock_id"],
        ascending=[False, False, False, True]
    ).head(20)

    watch_pool = watch_pool.sort_values(
        ["attack_score_v310", "final_sort_score_v310", "entry_score", "stock_id"],
        ascending=[False, False, False, True]
    ).head(20)

    block_pool = block_pool.sort_values(
        ["attack_score_v310", "final_sort_score_v310", "entry_score", "stock_id"],
        ascending=[False, False, False, True]
    ).head(40)

    s = pd.concat([test_pool, watch_pool, block_pool], ignore_index=True)
    s = s.drop_duplicates("stock_id")

    # 最後再鎖一次，保證 trade_plan.csv 寫出去前不會被舊 action 混入
    s = apply_v311_final_action_lock(s)

    trade_date = next_trade_date(signal_date)
    rows = []

    for _, r in s.iterrows():
        px = float(r.get("close", 0)) * 1.001
        action = str(r.get("action", "BLOCK")).upper()
        st = r.get("strategy_type", r.get("engine", ""))
        score = float(r.get("final_sort_score_v312", r.get("final_sort_score_v310", r.get("final_sort_score_v309", r.get("entry_score", 0)))))
        liq = str(r.get("liquidity_level", ""))

        # v311：只有 TEST 給試單資金，WATCH/BLOCK 都 0
        if action == "TEST" and st == "ALPHA":
            w = 0.010
        elif action == "TEST" and st == "CORE":
            w = 0.005
        else:
            w = 0.0

        amount = INITIAL_CAPITAL * w
        shares = amount / px if px > 0 else 0

        rows.append({
            "signal_date": str(signal_date.date()),
            "trade_date": str(trade_date.date()),
            "stock_id": r["stock_id"],
            "stock_name": r.get("stock_name", ""),
            "industry": r.get("industry", ""),
            "action": action,
            "action_label": r.get("action_label", action),
            "action_sub": r.get("action_sub", ""),
            "strategy_type": st,
            "price": round(px, 2),
            "ref_price": round(float(r.get("close", 0)), 2),
            "target_weight": round(w, 4),
            "suggest_amount": round(amount, 0),
            "suggest_shares": round(shares, 0),
            "entry_type": r.get("entry_type", r.get("action_sub", "")),
            "score": round(score, 2),
            "entry_score": round(float(r.get("entry_score", 0)), 2),
            "attack_score_v310": round(float(r.get("attack_score_v312", r.get("attack_score_v310", r.get("attack_score_v309", 0)))), 2),
            "final_attack_score_v310": round(float(r.get("final_attack_score_v312", r.get("final_attack_score_v310", r.get("final_attack_score_v309", 0)))), 2),
            "final_sort_score_v310": round(float(r.get("final_sort_score_v312", r.get("final_sort_score_v310", r.get("final_sort_score_v309", score)))), 2),
            "strict_test_ok_v310": r.get("strict_test_ok_v310", ""),
            "watch_ok_v310": r.get("watch_ok_v310", ""),
            "hard_reject_v310": r.get("hard_reject_v310", r.get("hard_reject_v309", "")),
            "v311_locked_action": r.get("v311_locked_action", action),
            "trend_ok_v310": r.get("trend_ok_v310", ""),
            "momentum_ok_v310": r.get("momentum_ok_v310", ""),
            "breakout_ok_v310": r.get("breakout_ok_v310", ""),
            "volume_ok_v310": r.get("volume_ok_v310", ""),
            "chip_ok_v310": r.get("chip_ok_v310", ""),
            "bottom_repair_only_v310": r.get("bottom_repair_only_v310", ""),
            "short_turn_weak_v309": r.get("short_turn_weak_v309", ""),
            "ma_sticky_no_attack_v309": r.get("ma_sticky_no_attack_v309", ""),
            "box_middle_v309": r.get("box_middle_v309", ""),
            "liquidity_only_v309": r.get("liquidity_only_v309", ""),
            "top_opportunity": r.get("top_opportunity", ""),
            "section_top_opportunity": r.get("section_top_opportunity", ""),
            "opportunity_rank": r.get("opportunity_rank", ""),
            "top_rank_v3066": r.get("top_rank_v3066", ""),
            "is_top_v3066": r.get("is_top_v3066", ""),
            "top_reason": r.get("top_reason", ""),
            "liquidity_level": liq,
            "liquidity_score": round(float(r.get("liquidity_score", 0)), 2),
            "volume": round(float(r.get("volume", 0)), 0),
            "turnover": round(float(r.get("turnover", 0)), 0),
            "source": "v312_balanced_attack_final_lock",
            "reason": r.get("reason", r.get("note", "")),
            "system_note": r.get("system_note", r.get("note", "")),
            "note": r.get("note", ""),
        })

    return pd.DataFrame(rows)


def main():
    df = load_feature()
    signal_date, latest = latest_valid(df)
    regime, info = detect_regime(latest)

    core = apply_v311_final_action_lock(core_engine(latest)).head(60)
    alpha = apply_v311_final_action_lock(alpha_engine(latest)).head(60)

    plan = build_trade_plan(core, alpha, regime, signal_date)

    debug = pd.DataFrame([{
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_regime": regime,
        **info,
        "latest_stock_count": len(latest),
        "high_liquidity_count": int((latest["liquidity_level"] == "HIGH").sum()),
        "medium_liquidity_count": int((latest["liquidity_level"] == "MEDIUM").sum()),
        "low_liquidity_count": int((latest["liquidity_level"] == "LOW").sum()),
        "core_count": len(core),
        "alpha_count": len(alpha),
        "core_buy_count": int((core.action == "BUY").sum()),
        "core_test_count": int((core.action == "TEST").sum()),
        "alpha_buy_count": int((alpha.action == "BUY").sum()),
        "alpha_test_count": int((alpha.action == "TEST").sum()),
        "trade_plan_count": len(plan),
        "trade_buy_count": int((plan.action == "BUY").sum()) if not plan.empty else 0,
        "trade_test_count": int((plan.action == "TEST").sum()) if not plan.empty else 0,
        "trade_watch_count": int((plan.action == "WATCH").sum()) if not plan.empty else 0,
        "core_max_score": float(core.entry_score.max()) if len(core) else 0,
        "alpha_max_score": float(alpha.entry_score.max()) if len(alpha) else 0,
    }])

    candidates = pd.concat([
        core.assign(engine="CORE"),
        alpha.assign(engine="ALPHA"),
    ], ignore_index=True)

    write_both(core, "core_candidates.csv")
    write_both(alpha, "alpha_candidates.csv")
    write_both(candidates, "candidates.csv")
    write_both(plan, "trade_plan.csv")
    write_both(debug, "selection_debug.csv")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v312_balanced_attack_final_lock",
        "signal_date": str(signal_date.date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "data_state": "fresh",
        "market_regime": regime,
        "regime_info": info,
        "trade_plan_count": len(plan),
        "buy_count": int((plan.action == "BUY").sum()) if not plan.empty else 0,
        "test_count": int((plan.action == "TEST").sum()) if not plan.empty else 0,
        "watch_count": int((plan.action == "WATCH").sum()) if not plan.empty else 0,
        "dual_strategy": {
            "CORE": "早期卡位 / 1000張以上小倉",
            "ALPHA": "高流動性強勢延續 / 3000張以上主力倉位",
        },
    }

    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        with open(p, "w", encoding="utf-8-sig") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta, ensure_ascii=False, indent=2))




# ===== v266.57.2 續強提示修補層（append-only，不改原本策略核心） =====
# 原則：先讓原本 main() 完整跑完，再補欄位到輸出 CSV。
# 不改 CORE / ALPHA / TEST / WATCH 條件；不改原本 entry_score；不改資金配置；不改出場。
def _num_v266572(v, default=np.nan):
    try:
        return pd.to_numeric(v, errors="coerce")
    except Exception:
        return default


def _safe_read_csv_v266572(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=str, encoding="utf-8-sig")
    except Exception:
        try:
            return pd.read_csv(p, dtype=str)
        except Exception:
            return pd.DataFrame()


def _sid_v266572(v):
    s = str(v or "").strip()
    import re
    m = re.search(r"\d{4}", s)
    return m.group(0) if m else s.zfill(4) if s.isdigit() else s


def _latest_feature_history_v266572():
    """讀 feature_panel_daily.csv，建立每檔股票最近 5~6 根資料的續強判斷表。"""
    try:
        df = load_feature()
    except Exception as e:
        print("v266.57.2 continuation patch skip: load_feature failed", e)
        return {}

    if df.empty or "stock_id" not in df.columns or "date" not in df.columns:
        return {}

    need_cols = ["open", "high", "low", "close", "volume", "ma5", "ma10", "ma20", "mom5", "mom10", "volume_ratio"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.copy()
    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock_id"].astype(str))
    df = df.sort_values(["stock_id", "date"])

    out = {}
    for sid, g in df.groupby("stock_id"):
        h = g.tail(6).copy()
        if len(h) < 5:
            continue

        last5 = h.tail(5)
        last = h.iloc[-1]
        prev = h.iloc[-2] if len(h) >= 2 else last

        highs = last5["high"].astype(float)
        lows = last5["low"].astype(float)
        closes = last5["close"].astype(float)
        vols = last5["volume"].astype(float)

        close = float(last.get("close", np.nan))
        open_ = float(last.get("open", np.nan))
        high = float(last.get("high", np.nan))
        low = float(last.get("low", np.nan))
        ma5 = float(last.get("ma5", np.nan))
        ma10 = float(last.get("ma10", np.nan))
        ma20 = float(last.get("ma20", np.nan))
        mom5 = float(last.get("mom5", np.nan))
        mom10 = float(last.get("mom10", np.nan))
        vol_ratio = float(last.get("volume_ratio", np.nan))

        score = 0
        reasons = []
        risks = []

        if highs.iloc[-1] >= highs.max() * 0.995 and highs.iloc[-1] >= highs.iloc[0]:
            score += 2; reasons.append("5日高點墊高")
        if lows.iloc[-1] >= lows.iloc[0] * 0.98:
            score += 2; reasons.append("5日低點未破")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            score += 1; reasons.append("MA5站上MA10")
        if np.isfinite(ma20) and close >= ma20:
            score += 1; reasons.append("收盤站上MA20")
        if np.isfinite(mom5) and mom5 > 0:
            score += 1; reasons.append("5日動能為正")
        if np.isfinite(mom10) and mom10 > 0:
            score += 1; reasons.append("10日動能為正")
        if len(vols.dropna()) >= 5 and vols.iloc[-1] >= vols.tail(5).median() * 1.05:
            score += 1; reasons.append("量能高於近期中位")

        # 假突破 / 隔日容易掛風險：只加提示，不直接刪名單。
        rng = max(high - low, 1e-9) if np.isfinite(high) and np.isfinite(low) else np.nan
        body = abs(close - open_) if np.isfinite(close) and np.isfinite(open_) else np.nan
        upper = high - max(open_, close) if np.isfinite(high) and np.isfinite(open_) and np.isfinite(close) else np.nan
        lower = min(open_, close) - low if np.isfinite(low) and np.isfinite(open_) and np.isfinite(close) else np.nan
        upper_ratio = upper / rng if np.isfinite(upper) and np.isfinite(rng) and rng > 0 else np.nan
        body_ratio = body / rng if np.isfinite(body) and np.isfinite(rng) and rng > 0 else np.nan

        risk_points = 0
        if np.isfinite(vol_ratio) and vol_ratio >= 3.0:
            risk_points += 2; risks.append("單日爆量過大")
        if np.isfinite(upper_ratio) and upper_ratio >= 0.45:
            risk_points += 2; risks.append("長上影壓力")
        if np.isfinite(body_ratio) and body_ratio <= 0.18:
            risk_points += 1; risks.append("實體過小猶豫K")
        if np.isfinite(ma20) and ma20 > 0 and close >= ma20 * 1.18:
            risk_points += 2; risks.append("乖離MA20過大")
        if np.isfinite(open_) and np.isfinite(close) and close < open_:
            risk_points += 1; risks.append("收黑K")
        if len(closes.dropna()) >= 5 and closes.iloc[-1] < closes.iloc[-2] and highs.iloc[-1] >= highs.max() * 0.995:
            risk_points += 2; risks.append("創高後收弱")

        final_score = max(0, min(10, score - min(risk_points, 4)))

        if risk_points >= 4:
            fake_level = "高"
        elif risk_points >= 2:
            fake_level = "中"
        else:
            fake_level = "低"

        if final_score >= 7 and fake_level == "低":
            hint = "🟢 續強機率較高：可保留試單／優先觀察是否延續。"
        elif final_score >= 5 and fake_level != "高":
            hint = "🟡 有轉強但仍需確認：隔日看量價是否延續，不追高。"
        elif fake_level == "高":
            hint = "🔴 假突破風險高：容易今天強、明天弱，建議降級觀察。"
        else:
            hint = "⚪ 續強證據不足：只觀察，不急著放大。"

        out[sid] = {
            "continuation_score": round(float(final_score), 2),
            "continuation_grade": "強" if final_score >= 7 else "中" if final_score >= 5 else "弱",
            "continuation_reason": "、".join(reasons) if reasons else "續強條件不足",
            "fake_breakout_risk": fake_level,
            "fake_breakout_reason": "、".join(risks) if risks else "未見明顯假突破風險",
            "next_day_follow_hint": hint,
            "continuation_patch_version": "v266.57.2",
        }
    return out


def _enrich_csv_v266572(name, cont_map):
    """只補欄位，不刪列、不改 action、不改 entry_score。"""
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df = _safe_read_csv_v266572(p)
        if df.empty or "stock_id" not in df.columns:
            continue
        df = df.copy()
        sids = df["stock_id"].map(_sid_v266572)
        for col in [
            "continuation_score",
            "continuation_grade",
            "continuation_reason",
            "fake_breakout_risk",
            "fake_breakout_reason",
            "next_day_follow_hint",
            "continuation_patch_version",
        ]:
            df[col] = [cont_map.get(sid, {}).get(col, "") for sid in sids]

        # 只補提示，不覆蓋原本 note/system_note。若前端有讀 reason，也保留原值。
        if "system_note" in df.columns:
            df["system_note"] = df.apply(
                lambda r: str(r.get("system_note", "")) + (
                    "｜續強提示：" + str(r.get("next_day_follow_hint", ""))
                    if str(r.get("next_day_follow_hint", "")).strip() else ""
                ), axis=1
            )

        df.to_csv(p, index=False, encoding="utf-8-sig")
        print("v266.57.2 continuation enriched:", p, len(df))


def apply_continuation_hint_patch_v266572():
    cont_map = _latest_feature_history_v266572()
    if not cont_map:
        print("v266.57.2 continuation patch: no continuation map, skip")
        return
    for name in [
        "core_candidates.csv",
        "alpha_candidates.csv",
        "candidates.csv",
        "trade_plan.csv",
        "ignition_candidates.csv",
        "strategy_evolution.csv",
    ]:
        _enrich_csv_v266572(name, cont_map)

    report = {
        "version": "v266.57.2",
        "mode": "append_only_continuation_hint",
        "changed_strategy_logic": False,
        "changed_actions": False,
        "changed_position_sizing": False,
        "enriched_stock_count": len(cont_map),
        "updated_at": taipei_now_str(),
        "description": "只補續強分數、假突破風險、隔日續強提示；不改原本選股/排序/資金配置核心。",
    }
    for p in [ROOT / "continuation_patch_report.json", DATA_DIR / "continuation_patch_report.json"]:
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main_v266572_continuation_patch():
    main()
    apply_continuation_hint_patch_v266572()


# ===== v266.57.5.1 20/40/60 結構欄位寫入修補（append-only） =====
# 只修補：確保 structure 欄位在原本策略跑完後寫入 CSV。
# 不改：CORE / ALPHA / entry_score / action / target_weight / allocator / 持倉邏輯。
def _read_json_v2665751(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return {}
    for enc in ["utf-8-sig", "utf-8"]:
        try:
            return json.loads(p.read_text(encoding=enc))
        except Exception:
            pass
    return {}


def _sf_v2665751(v, default=np.nan):
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default


def _sid_v2665751(v):
    s = str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s


def _vol_mild_v2665751(last_vol, med_vol):
    last_vol = _sf_v2665751(last_vol)
    med_vol = _sf_v2665751(med_vol)
    if not np.isfinite(last_vol) or not np.isfinite(med_vol) or med_vol <= 0:
        return False
    r = last_vol / med_vol
    return 1.05 <= r <= 2.8


def _range_pct_v2665751(g, ref):
    ref = _sf_v2665751(ref)
    if g.empty or not np.isfinite(ref) or ref <= 0:
        return np.nan
    hi = pd.to_numeric(g.get("high"), errors="coerce").max()
    lo = pd.to_numeric(g.get("low"), errors="coerce").min()
    if not np.isfinite(hi) or not np.isfinite(lo):
        return np.nan
    return (hi - lo) / ref


def _market_context_v2665751():
    meta = {}
    macro = {}
    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        meta.update(_read_json_v2665751(p))
    for p in [ROOT / "macro_regime.json", DATA_DIR / "macro_regime.json"]:
        macro.update(_read_json_v2665751(p))

    mr = str(meta.get("market_regime") or meta.get("regime") or meta.get("market_label") or "")
    ml = str(macro.get("macro_label") or macro.get("macro_regime") or meta.get("macro_label") or "")

    upper = mr.upper()
    if "TREND" in upper or "BULL" in upper or "多" in mr:
        weights = {"20": 0.45, "40": 0.30, "60": 0.25}
        env_note = "多頭/趨勢盤：偏重20D主升延續"
    elif "BEAR" in upper or "空" in mr or "弱" in mr:
        weights = {"20": 0.25, "40": 0.30, "60": 0.45}
        env_note = "弱勢/空頭盤：偏重60D大結構防守"
    else:
        weights = {"20": 0.30, "40": 0.40, "60": 0.30}
        env_note = "盤整盤：偏重40D平台整理"

    macro_adj = 0.0
    macro_note = "總經中性或資料不足：不額外加權"
    if any(k in ml for k in ["偏多", "多頭", "RISK_ON"]):
        macro_adj = 0.5
        macro_note = "總經偏多：結構分數小幅加權"
    elif any(k in ml for k in ["偏空", "空頭", "RISK_OFF"]):
        macro_adj = -0.8
        macro_note = "總經偏空：結構分數小幅保守"

    return {
        "market_regime": mr or "--",
        "macro_label": ml or "--",
        "weights": weights,
        "macro_adj": macro_adj,
        "env_note": env_note,
        "macro_note": macro_note,
    }


def _build_structure_map_v2665751():
    try:
        df = load_feature()
    except Exception as e:
        print("v266.57.5.1 structure skip: load_feature failed:", e)
        return {}, _market_context_v2665751()

    if df.empty or "stock_id" not in df.columns or "date" not in df.columns:
        print("v266.57.5.1 structure skip: feature empty or missing stock_id/date")
        return {}, _market_context_v2665751()

    df = df.copy()
    df["stock_id"] = df["stock_id"].map(_sid_v2665751)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id"]).sort_values(["stock_id", "date"])

    for c in ["open", "high", "low", "close", "volume", "ma5", "ma10", "ma20", "ma60", "mom5", "mom10", "mom20", "mom60", "volume_ratio", "ma20_slope"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    ctx = _market_context_v2665751()
    w = ctx["weights"]
    macro_adj = float(ctx["macro_adj"])

    result = {}
    for sid, g in df.groupby("stock_id"):
        h = g.tail(70).copy()
        if len(h) < 20:
            continue

        last = h.iloc[-1]
        close = _sf_v2665751(last.get("close"))
        if not np.isfinite(close) or close <= 0:
            continue

        open_ = _sf_v2665751(last.get("open"))
        high = _sf_v2665751(last.get("high"))
        low = _sf_v2665751(last.get("low"))
        vol = _sf_v2665751(last.get("volume"))
        ma5 = _sf_v2665751(last.get("ma5"))
        ma10 = _sf_v2665751(last.get("ma10"))
        ma20 = _sf_v2665751(last.get("ma20"))
        ma60 = _sf_v2665751(last.get("ma60"))
        mom10 = _sf_v2665751(last.get("mom10"))
        mom20 = _sf_v2665751(last.get("mom20"))
        mom60 = _sf_v2665751(last.get("mom60"))
        vol_ratio = _sf_v2665751(last.get("volume_ratio"))
        ma20_slope = _sf_v2665751(last.get("ma20_slope"))

        g20 = h.tail(20)
        g40 = h.tail(40) if len(h) >= 40 else h
        g60 = h.tail(60) if len(h) >= 60 else h

        high20 = pd.to_numeric(g20["high"], errors="coerce").max()
        high40 = pd.to_numeric(g40["high"], errors="coerce").max()
        high60 = pd.to_numeric(g60["high"], errors="coerce").max()
        low60 = pd.to_numeric(g60["low"], errors="coerce").min()
        vol20 = pd.to_numeric(g20["volume"], errors="coerce").median()
        vol40 = pd.to_numeric(g40["volume"], errors="coerce").median()
        vol60 = pd.to_numeric(g60["volume"], errors="coerce").median()

        range40 = _range_pct_v2665751(g40, close)
        ma_tight = (
            np.isfinite(ma5) and np.isfinite(ma10) and np.isfinite(ma20)
            and (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / close <= 0.08
        )

        s20, r20 = 0.0, []
        if np.isfinite(ma20) and close > ma20:
            s20 += 2; r20.append("站上MA20")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            s20 += 2; r20.append("MA5>=MA10")
        if np.isfinite(high20) and close >= high20 * 0.96:
            s20 += 2; r20.append("接近20日高")
        if _vol_mild_v2665751(vol, vol20):
            s20 += 2; r20.append("20D量能溫和回升")
        if np.isfinite(mom10) and mom10 > 0:
            s20 += 1; r20.append("10D動能轉正")
        if np.isfinite(ma20) and ma20 > 0 and close <= ma20 * 1.15:
            s20 += 1; r20.append("未嚴重乖離MA20")

        s40, r40 = 0.0, []
        if np.isfinite(range40) and range40 <= 0.35:
            s40 += 2; r40.append("40D區間收斂")
        if ma_tight:
            s40 += 2; r40.append("均線糾結")
        if np.isfinite(ma20) and close >= ma20 * 0.98:
            s40 += 2; r40.append("站回平台成本")
        if len(g40) >= 30:
            prior_low = pd.to_numeric(g40.iloc[:20]["low"], errors="coerce").min()
            recent_low = pd.to_numeric(g40.iloc[-20:]["low"], errors="coerce").min()
            if np.isfinite(prior_low) and np.isfinite(recent_low) and recent_low >= prior_low * 0.96:
                s40 += 2; r40.append("低點未再破")
        if _vol_mild_v2665751(vol, vol40):
            s40 += 1; r40.append("40D量能回溫")
        if np.isfinite(ma20_slope) and ma20_slope >= 0:
            s40 += 1; r40.append("MA20走平翻揚")

        s60, r60 = 0.0, []
        if np.isfinite(ma60) and close >= ma60 * 0.96:
            s60 += 2; r60.append("接近/站回MA60")
        if np.isfinite(ma20) and np.isfinite(ma60) and ma20 >= ma60 * 0.95:
            s60 += 2; r60.append("MA20接近MA60")
        if np.isfinite(high60) and np.isfinite(low60) and high60 > low60:
            pos60 = (close - low60) / (high60 - low60)
            if pos60 >= 0.45:
                s60 += 2; r60.append("站回60D區間中上")
        if len(g60) >= 50:
            prior_low60 = pd.to_numeric(g60.iloc[:30]["low"], errors="coerce").min()
            recent_low60 = pd.to_numeric(g60.iloc[-30:]["low"], errors="coerce").min()
            if np.isfinite(prior_low60) and np.isfinite(recent_low60) and recent_low60 >= prior_low60 * 0.95:
                s60 += 2; r60.append("60D低點守住")
        if np.isfinite(mom60) and mom60 > -0.05:
            s60 += 1; r60.append("60D動能不再惡化")
        if _vol_mild_v2665751(vol, vol60):
            s60 += 1; r60.append("60D量能溫和回補")

        heat, hr = 0.0, []
        if np.isfinite(ma20) and ma20 > 0 and close > ma20 * 1.22:
            heat += 1.5; hr.append("距MA20過遠")
        if np.isfinite(vol_ratio) and vol_ratio > 5.5:
            heat += 1.0; hr.append("單日爆量偏高")
        if np.isfinite(mom20) and mom20 > 0.45:
            heat += 1.0; hr.append("20D漲幅偏熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and np.isfinite(close) and high > low:
            upper = (high - max(open_, close)) / (high - low)
            if upper >= 0.45:
                heat += 1.0; hr.append("長上影壓力")

        s20 = max(0, min(10, s20 - heat))
        s40 = max(0, min(10, s40 - heat * 0.5))
        s60 = max(0, min(10, s60 - heat * 0.3))

        composite = max(0, min(10, s20 * w["20"] + s40 * w["40"] + s60 * w["60"] + macro_adj))

        scores = {"20D短線轉強": s20, "40D平台整理": s40, "60D長底翻多": s60}
        best_type = max(scores, key=scores.get)

        if composite < 4:
            stype = "結構不足"
        elif heat >= 2 and s20 >= 6:
            stype = "過熱延續"
        else:
            stype = best_type

        grade = "強" if composite >= 7 else ("中" if composite >= 5 else "弱")
        reasons = [
            "20D:" + ("、".join(r20) if r20 else "短線結構不足"),
            "40D:" + ("、".join(r40) if r40 else "平台結構不足"),
            "60D:" + ("、".join(r60) if r60 else "長底結構不足"),
        ]
        if hr:
            reasons.append("過熱扣分:" + "、".join(hr))

        if stype == "20D短線轉強":
            hint = "偏主升初段/延續觀察：搭配原本動能，避免追過熱。"
        elif stype == "40D平台整理":
            hint = "偏平台整理後轉強：觀察是否從WATCH/TEST升級。"
        elif stype == "60D長底翻多":
            hint = "偏長底翻多：適合CORE早期卡位，小倉觀察放量確認。"
        elif stype == "過熱延續":
            hint = "已有動能但乖離偏高：保留原策略判斷，操作上不追高。"
        else:
            hint = "結構證據不足：原策略若入選，仍需降低信心。"

        result[sid] = {
            "structure_20_score": round(float(s20), 2),
            "structure_40_score": round(float(s40), 2),
            "structure_60_score": round(float(s60), 2),
            "structure_score": round(float(composite), 2),
            "structure_grade": grade,
            "structure_type": stype,
            "structure_reason": "｜".join(reasons),
            "structure_market_fit": ctx["env_note"] + "｜" + ctx["macro_note"],
            "structure_hint": hint,
            "structure_patch_version": "v266.57.5.1",
        }

    return result, ctx


def _enrich_one_csv_v2665751(path, structure_map):
    df = _safe_read_csv_v266572(path)
    if df.empty or "stock_id" not in df.columns:
        return False, 0

    df = df.copy()
    sids = df["stock_id"].map(_sid_v2665751)

    cols = [
        "structure_20_score",
        "structure_40_score",
        "structure_60_score",
        "structure_score",
        "structure_grade",
        "structure_type",
        "structure_reason",
        "structure_market_fit",
        "structure_hint",
        "structure_patch_version",
    ]

    for col in cols:
        df[col] = [structure_map.get(sid, {}).get(col, "") for sid in sids]

    # 不覆蓋原 note；只追加提示。
    if "system_note" in df.columns:
        df["system_note"] = df.apply(
            lambda r: str(r.get("system_note", "")) + (
                "｜結構提示：" + str(r.get("structure_hint", ""))
                if str(r.get("structure_hint", "")).strip() else ""
            ), axis=1
        )
    elif "note" in df.columns:
        df["note"] = df.apply(
            lambda r: str(r.get("note", "")) + (
                "｜結構提示：" + str(r.get("structure_hint", ""))
                if str(r.get("structure_hint", "")).strip() else ""
            ), axis=1
        )

    df.to_csv(path, index=False, encoding="utf-8-sig")
    return True, len(df)


def apply_structure_score_patch_v2665751():
    structure_map, ctx = _build_structure_map_v2665751()

    report = {
        "version": "v266.57.5.1",
        "mode": "append_only_20_40_60_structure_score_write_fix",
        "changed_strategy_logic": False,
        "changed_actions": False,
        "changed_entry_score": False,
        "changed_position_sizing": False,
        "market_regime": ctx.get("market_regime", "--"),
        "macro_label": ctx.get("macro_label", "--"),
        "market_weighting": ctx.get("weights", {}),
        "macro_adjustment": ctx.get("macro_adj", 0),
        "enriched_stock_count": len(structure_map),
        "files": {},
        "updated_at": taipei_now_str(),
        "description": "只補20/40/60日結構分數並強制寫入既有CSV，不改原策略核心。",
    }

    targets = [
        "core_candidates.csv",
        "alpha_candidates.csv",
        "candidates.csv",
        "trade_plan.csv",
        "ignition_candidates.csv",
        "strategy_evolution.csv",
        "final_action_plan.csv",
        "top_opportunities.csv",
    ]

    for name in targets:
        for base in [ROOT, DATA_DIR]:
            p = base / name
            ok, n = _enrich_one_csv_v2665751(p, structure_map)
            if ok:
                report["files"][str(p)] = n
                print("v266.57.5.1 structure enriched:", p, n)

    for p in [ROOT / "structure_patch_report.json", DATA_DIR / "structure_patch_report.json"]:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        except Exception as e:
            print("write structure report failed:", p, e)

    print(json.dumps(report, ensure_ascii=False, indent=2))


def main_v2665751_structure_write_fix():
    # 原本策略 + 原本續強提示先跑完，再補結構欄位。
    main_v266572_continuation_patch()
    apply_structure_score_patch_v2665751()



# ===== v266.57.6 structure pre-score candidate weighting（append-only 測試修補） =====
# 只新增：structure_pre_score / adjusted_signal_score / structure_rank
# 不覆蓋：entry_score / action / target_weight / 持倉 / 原策略核心
def _sid_v266576(v):
    s = str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s

def _sf_v266576(v, default=np.nan):
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default

def _read_csv_v266576(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ["utf-8-sig", "utf-8"]:
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()

def _write_csv_v266576(df, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def _build_pre_score_map_v266576():
    try:
        feat = load_feature()
    except Exception as e:
        print("v266.57.6 skip load_feature:", e)
        return {}

    if feat.empty or "stock_id" not in feat.columns or "date" not in feat.columns:
        return {}

    df = feat.copy()
    df["stock_id"] = df["stock_id"].map(_sid_v266576)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id"]).sort_values(["stock_id", "date"])

    for c in ["open","high","low","close","volume","ma5","ma10","ma20","ma60","mom5","mom10","mom20","volume_ratio"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out = {}
    for sid, g in df.groupby("stock_id"):
        h = g.tail(80).copy()
        if len(h) < 25:
            continue

        last = h.iloc[-1]
        prev3 = h.iloc[-4] if len(h) >= 4 else h.iloc[0]
        prev10 = h.iloc[-11] if len(h) >= 11 else h.iloc[0]

        close = _sf_v266576(last.get("close"))
        open_ = _sf_v266576(last.get("open"))
        high = _sf_v266576(last.get("high"))
        low = _sf_v266576(last.get("low"))
        volume = _sf_v266576(last.get("volume"))
        ma5 = _sf_v266576(last.get("ma5"))
        ma10 = _sf_v266576(last.get("ma10"))
        ma20 = _sf_v266576(last.get("ma20"))
        ma60 = _sf_v266576(last.get("ma60"))
        ma20_p3 = _sf_v266576(prev3.get("ma20"))
        ma60_p10 = _sf_v266576(prev10.get("ma60"))
        mom5 = _sf_v266576(last.get("mom5"))
        mom10 = _sf_v266576(last.get("mom10"))
        mom20 = _sf_v266576(last.get("mom20"))
        vol_ratio = _sf_v266576(last.get("volume_ratio"))

        if not np.isfinite(close) or close <= 0:
            continue

        g5, g20 = h.tail(5), h.tail(20)
        g40 = h.tail(40) if len(h) >= 40 else h
        g60 = h.tail(60) if len(h) >= 60 else h

        vol5 = pd.to_numeric(g5["volume"], errors="coerce").mean()
        vol20 = pd.to_numeric(g20["volume"], errors="coerce").mean()
        vol20_med = pd.to_numeric(g20["volume"], errors="coerce").median()
        high20 = pd.to_numeric(g20["high"], errors="coerce").max()
        high40 = pd.to_numeric(g40["high"], errors="coerce").max()
        low40 = pd.to_numeric(g40["low"], errors="coerce").min()
        high60 = pd.to_numeric(g60["high"], errors="coerce").max()
        low60 = pd.to_numeric(g60["low"], errors="coerce").min()

        score, reasons, penalties = 0.0, [], []

        # 20D：短線剛翻強
        if np.isfinite(ma20) and close > ma20:
            score += 1.0; reasons.append("20D站上MA20")
        if np.isfinite(ma20) and np.isfinite(ma20_p3) and ma20 >= ma20_p3:
            score += 1.0; reasons.append("MA20走平翻揚")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            score += 1.0; reasons.append("MA5站上MA10")
        if np.isfinite(high20) and close >= high20 * 0.96:
            score += 1.0; reasons.append("接近20日高")

        # 40D：平台壓縮
        if np.isfinite(high40) and np.isfinite(low40) and close > 0 and (high40-low40)/close <= 0.35:
            score += 1.5; reasons.append("40D平台收斂")
        if np.isfinite(ma5) and np.isfinite(ma10) and np.isfinite(ma20):
            spread = (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / close
            if spread <= 0.08:
                score += 1.5; reasons.append("短中均線糾結")
        if len(g40) >= 30:
            prior_low = pd.to_numeric(g40.iloc[:20]["low"], errors="coerce").min()
            recent_low = pd.to_numeric(g40.iloc[-20:]["low"], errors="coerce").min()
            if np.isfinite(prior_low) and np.isfinite(recent_low) and recent_low >= prior_low * 0.96:
                score += 1.0; reasons.append("40D低點守住")

        # 60D：長底翻多
        if np.isfinite(ma60) and close >= ma60 * 0.96:
            score += 1.0; reasons.append("接近/站回MA60")
        if np.isfinite(ma60) and np.isfinite(ma60_p10) and ma60 >= ma60_p10 * 0.995:
            score += 1.0; reasons.append("MA60不再下彎")
        if np.isfinite(high60) and np.isfinite(low60) and high60 > low60 and (close-low60)/(high60-low60) >= 0.45:
            score += 1.0; reasons.append("站回60D區間中上")

        # 量縮後轉強
        if np.isfinite(vol5) and np.isfinite(vol20) and vol20 > 0 and vol5 <= vol20 * 0.80:
            score += 1.0; reasons.append("近5日量縮")
        if np.isfinite(volume) and np.isfinite(vol20_med) and vol20_med > 0:
            vr = volume / vol20_med
            if 1.15 <= vr <= 2.8:
                score += 2.0; reasons.append("量縮後溫和放量")
            elif vr > 4.0:
                score -= 1.5; penalties.append("單日爆量偏高")

        # 動能確認，不取代原動能
        if np.isfinite(mom5) and mom5 > 0:
            score += 0.8; reasons.append("5D動能轉正")
        if np.isfinite(mom10) and mom10 > 0:
            score += 0.8; reasons.append("10D動能轉正")

        # 過熱/假突破扣分
        if np.isfinite(ma20) and ma20 > 0 and close > ma20 * 1.22:
            score -= 2.0; penalties.append("距MA20過遠")
        if np.isfinite(mom20) and mom20 > 0.45:
            score -= 1.5; penalties.append("20D漲幅過熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and high > low:
            upper = (high - max(open_, close)) / (high - low)
            if upper >= 0.45:
                score -= 2.0; penalties.append("長上影壓力")
        if np.isfinite(vol_ratio) and vol_ratio >= 5.5:
            score -= 1.0; penalties.append("成交量異常爆量")

        score = max(-5.0, min(12.0, score))
        if score >= 8:
            grade, stype = "A", "起漲結構強"
        elif score >= 6:
            grade, stype = "B", "起漲結構中"
        elif score >= 4:
            grade, stype = "C", "結構觀察"
        elif score >= 2:
            grade, stype = "D", "結構偏弱"
        else:
            grade, stype = "E", "結構不足"

        if grade in ["A","B"] and any(("40D" in r or "糾結" in r) for r in reasons):
            hint = "平台壓縮後轉強，可優先觀察起漲/試單。"
        elif grade in ["A","B"] and any(("60D" in r or "MA60" in r) for r in reasons):
            hint = "長底翻多結構，可偏CORE早期卡位。"
        elif grade in ["A","B"]:
            hint = "短線轉強結構，可搭配原動能延續。"
        elif penalties:
            hint = "有過熱或假突破壓力，避免追高。"
        else:
            hint = "結構證據不足，保留原策略但降低信心。"

        out[sid] = {
            "structure_pre_score": round(float(score), 2),
            "structure_pre_grade": grade,
            "structure_pre_type": stype,
            "structure_pre_reason": "｜".join(reasons + (["扣分:" + "、".join(penalties)] if penalties else [])),
            "structure_pre_hint": hint,
            "structure_pre_patch_version": "v266.57.6",
        }
    return out

def _pick_base_score_col_v266576(df):
    for c in ["entry_score","score","total_score","final_score","momentum_score","rank_score","composite_score"]:
        if c in df.columns:
            return c
    return None

def _apply_pre_score_csv_v266576(path, score_map):
    df = _read_csv_v266576(path)
    if df.empty or "stock_id" not in df.columns:
        return False, 0
    df = df.copy()
    sids = df["stock_id"].map(_sid_v266576)

    cols = ["structure_pre_score","structure_pre_grade","structure_pre_type","structure_pre_reason","structure_pre_hint","structure_pre_patch_version"]
    for col in cols:
        df[col] = [score_map.get(sid, {}).get(col, "") for sid in sids]

    base_col = _pick_base_score_col_v266576(df)
    if base_col:
        base = pd.to_numeric(df[base_col], errors="coerce")
        bonus = pd.to_numeric(df["structure_pre_score"], errors="coerce").fillna(0)
        df["adjusted_signal_score"] = (base + bonus * 0.35).round(3)
        df["adjusted_signal_note"] = "原分數欄位:" + base_col + "｜結構前置加權僅供排序觀察，不覆蓋原策略"
    else:
        df["adjusted_signal_score"] = pd.to_numeric(df["structure_pre_score"], errors="coerce").fillna(0).round(3)
        df["adjusted_signal_note"] = "無原分數欄位｜僅顯示結構前置分數"

    if "system_note" in df.columns:
        df["system_note"] = df.apply(lambda r: str(r.get("system_note","")) + ("｜前置結構：" + str(r.get("structure_pre_hint","")) if str(r.get("structure_pre_hint","")).strip() else ""), axis=1)
    elif "note" in df.columns:
        df["note"] = df.apply(lambda r: str(r.get("note","")) + ("｜前置結構：" + str(r.get("structure_pre_hint","")) if str(r.get("structure_pre_hint","")).strip() else ""), axis=1)

    df["structure_rank"] = pd.to_numeric(df["adjusted_signal_score"], errors="coerce").rank(ascending=False, method="min")
    _write_csv_v266576(df, path)
    return True, len(df)

def apply_structure_pre_score_patch_v266576():
    score_map = _build_pre_score_map_v266576()
    targets = [
        "candidates.csv","core_candidates.csv","alpha_candidates.csv","trade_plan.csv",
        "ignition_candidates.csv","strategy_evolution.csv","selection_debug.csv",
        "pre_move_candidates.csv","top_opportunities.csv","final_action_plan.csv",
    ]
    report = {
        "version": "v266.57.6",
        "mode": "append_only_structure_pre_score_candidate_weighting",
        "changed_strategy_logic": False,
        "changed_original_score": False,
        "changed_action": False,
        "changed_position": False,
        "enriched_stock_count": len(score_map),
        "files": {},
        "updated_at": taipei_now_str(),
        "description": "只新增structure_pre_score與adjusted_signal_score作為排序觀察，不覆蓋原本動能/操作邏輯。",
    }
    for name in targets:
        for base in [ROOT, DATA_DIR]:
            p = base / name
            ok, n = _apply_pre_score_csv_v266576(p, score_map)
            if ok:
                report["files"][str(p)] = n
                print("v266.57.6 structure pre-score enriched:", p, n)

    for p in [ROOT / "structure_pre_score_report.json", DATA_DIR / "structure_pre_score_report.json"]:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        except Exception as e:
            print("write structure pre-score report failed:", p, e)
    print(json.dumps(report, ensure_ascii=False, indent=2))

def main_v266576_structure_pre_score_patch():
    main_v2665751_structure_write_fix()
    apply_structure_pre_score_patch_v266576()


# ===== v266.57.7 structure weight split + continuation quality（append-only 測試修補） =====
# 不改原始 entry_score / action / target_weight；只新增測試排序分與續強品質欄位。
def _sid_v266577(v):
    s = str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s

def _sf_v266577(v, default=np.nan):
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default

def _read_csv_v266577(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    for enc in ["utf-8-sig", "utf-8"]:
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()

def _write_csv_v266577(df, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def _pick_score_col_v266577(df):
    for c in ["entry_score", "score", "total_score", "final_score", "momentum_score", "rank_score", "composite_score"]:
        if c in df.columns:
            return c
    return None

def _strategy_bucket_v266577(row):
    txt = " ".join(str(row.get(c, "")) for c in ["strategy_type", "strategy_name", "bucket", "source", "action_sub", "entry_type", "execution_flag"]).upper()
    if "ALPHA" in txt:
        return "ALPHA"
    if "IGNITION" in txt or "起漲" in txt:
        return "IGNITION"
    if "EVOLUTION" in txt or "進化" in txt:
        return "EVOLUTION"
    if "CORE" in txt:
        return "CORE"
    if "TEST" in txt or "試單" in txt:
        return "TEST"
    return "GENERIC"

def _structure_weight_v266577(bucket):
    return {"CORE": 0.95, "IGNITION": 1.05, "EVOLUTION": 0.65, "TEST": 0.80, "ALPHA": 0.35}.get(bucket, 0.55)

def _continuation_weight_v266577(bucket):
    return {"ALPHA": 0.75, "EVOLUTION": 0.85, "CORE": 0.55, "IGNITION": 0.45, "TEST": 0.50}.get(bucket, 0.50)

def _build_continuation_quality_map_v266577():
    try:
        feat = load_feature()
    except Exception as e:
        print("v266.57.7 continuation skip load_feature:", e)
        return {}
    if feat.empty or "stock_id" not in feat.columns or "date" not in feat.columns:
        return {}

    df = feat.copy()
    df["stock_id"] = df["stock_id"].map(_sid_v266577)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id"]).sort_values(["stock_id", "date"])

    for c in ["open","high","low","close","volume","ma5","ma10","ma20","mom5","mom10","mom20","volume_ratio"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out = {}
    for sid, g in df.groupby("stock_id"):
        h = g.tail(35).copy()
        if len(h) < 12:
            continue
        last = h.iloc[-1]
        close = _sf_v266577(last.get("close"))
        open_ = _sf_v266577(last.get("open"))
        high = _sf_v266577(last.get("high"))
        low = _sf_v266577(last.get("low"))
        volume = _sf_v266577(last.get("volume"))
        ma5 = _sf_v266577(last.get("ma5"))
        ma10 = _sf_v266577(last.get("ma10"))
        ma20 = _sf_v266577(last.get("ma20"))
        mom5 = _sf_v266577(last.get("mom5"))
        mom10 = _sf_v266577(last.get("mom10"))
        mom20 = _sf_v266577(last.get("mom20"))
        vol_ratio = _sf_v266577(last.get("volume_ratio"))
        if not np.isfinite(close) or close <= 0:
            continue

        g5 = h.tail(5)
        g20 = h.tail(20)
        recent_low5 = pd.to_numeric(g5["low"], errors="coerce").min()
        prior_low10 = pd.to_numeric(h.iloc[-15:-5]["low"], errors="coerce").min() if len(h) >= 15 else np.nan
        vol5 = pd.to_numeric(g5["volume"], errors="coerce").mean()
        vol20 = pd.to_numeric(g20["volume"], errors="coerce").mean()
        high20 = pd.to_numeric(g20["high"], errors="coerce").max()

        score = 0.0
        reasons = []
        penalties = []

        if np.isfinite(recent_low5) and np.isfinite(prior_low10) and recent_low5 >= prior_low10 * 0.97:
            score += 2.0; reasons.append("回檔不破前低")
        if np.isfinite(ma5) and close >= ma5 * 0.995:
            score += 1.5; reasons.append("收盤守MA5")
        elif np.isfinite(ma10) and close >= ma10 * 0.995:
            score += 0.8; reasons.append("回測守MA10")
        else:
            score -= 1.0; penalties.append("跌破短均")
        if np.isfinite(vol5) and np.isfinite(vol20) and vol20 > 0:
            vr5 = vol5 / vol20
            if 0.55 <= vr5 <= 0.95:
                score += 1.5; reasons.append("回檔量縮")
            elif vr5 > 1.8:
                score -= 1.2; penalties.append("回檔量放大")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            score += 1.2; reasons.append("MA5仍在MA10上")
        if np.isfinite(ma20) and close >= ma20:
            score += 1.0; reasons.append("仍站MA20")
        if np.isfinite(mom10) and mom10 > 0:
            score += 1.0; reasons.append("10D動能維持")
        if np.isfinite(mom5) and mom5 < -0.08:
            score -= 1.2; penalties.append("短線急殺")
        if np.isfinite(high20) and high20 > 0 and close / high20 >= 0.98 and np.isfinite(mom5) and mom5 > 0.08:
            score -= 1.0; penalties.append("接近20日高且短線過熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and high > low:
            upper = (high - max(open_, close)) / (high - low)
            if upper >= 0.45:
                score -= 1.5; penalties.append("長上影壓力")
        if np.isfinite(vol_ratio) and vol_ratio >= 5.5:
            score -= 1.0; penalties.append("異常爆量")
        if np.isfinite(mom20) and mom20 > 0.45:
            score -= 1.2; penalties.append("20D漲幅偏熱")

        score = max(-5.0, min(10.0, score))
        if score >= 7:
            grade, label = "A", "續強品質強"
        elif score >= 5:
            grade, label = "B", "續強品質中"
        elif score >= 3:
            grade, label = "C", "續強觀察"
        elif score >= 1:
            grade, label = "D", "續強偏弱"
        else:
            grade, label = "E", "續強不足"

        hint = "回檔承接尚可，若原策略入選，可優先觀察延續。" if grade in ["A", "B"] else ("續強品質不足或有假突破壓力，避免追高。" if penalties else "尚未看到明確回檔承接，保留觀察。")
        out[sid] = {
            "continuation_quality_score": round(float(score), 2),
            "continuation_quality_grade": grade,
            "continuation_quality_type": label,
            "continuation_quality_reason": "｜".join(reasons + (["扣分:" + "、".join(penalties)] if penalties else [])),
            "continuation_quality_hint": hint,
            "continuation_quality_patch_version": "v266.57.7",
        }
    return out

def _apply_v266577_to_csv(path, quality_map):
    df = _read_csv_v266577(path)
    if df.empty or "stock_id" not in df.columns:
        return False, 0
    df = df.copy()
    sids = df["stock_id"].map(_sid_v266577)
    for c in ["continuation_quality_score","continuation_quality_grade","continuation_quality_type","continuation_quality_reason","continuation_quality_hint","continuation_quality_patch_version"]:
        df[c] = [quality_map.get(sid, {}).get(c, "") for sid in sids]

    base_col = _pick_score_col_v266577(df)
    base_score = pd.to_numeric(df[base_col], errors="coerce").fillna(0) if base_col else pd.Series([0] * len(df))
    structure_pre = pd.to_numeric(df["structure_pre_score"], errors="coerce").fillna(0) if "structure_pre_score" in df.columns else pd.Series([0] * len(df))
    continuation_q = pd.to_numeric(df["continuation_quality_score"], errors="coerce").fillna(0)
    buckets = df.apply(_strategy_bucket_v266577, axis=1)
    s_weights = buckets.map(_structure_weight_v266577).astype(float)
    c_weights = buckets.map(_continuation_weight_v266577).astype(float)

    df["adjusted_signal_score_v26657_7"] = (base_score + structure_pre * s_weights + continuation_q * c_weights).round(3)
    df["structure_weight_v26657_7"] = s_weights.round(2)
    df["continuation_weight_v26657_7"] = c_weights.round(2)
    df["strategy_bucket_v26657_7"] = buckets
    df["adjusted_signal_note_v26657_7"] = "測試排序分=原分數+結構前置分*策略權重+續強品質*策略權重；不覆蓋原策略"
    df["structure_rank_v26657_7"] = pd.to_numeric(df["adjusted_signal_score_v26657_7"], errors="coerce").rank(ascending=False, method="min")

    def _append_note(row):
        parts = []
        h1 = str(row.get("structure_pre_hint", "")).strip()
        h2 = str(row.get("continuation_quality_hint", "")).strip()
        if h1:
            parts.append("結構：" + h1)
        if h2:
            parts.append("續強：" + h2)
        return "｜".join(parts)

    if "system_note" in df.columns:
        df["system_note"] = df.apply(lambda r: str(r.get("system_note", "")) + ("｜v266.57.7：" + _append_note(r) if _append_note(r) else ""), axis=1)
    elif "note" in df.columns:
        df["note"] = df.apply(lambda r: str(r.get("note", "")) + ("｜v266.57.7：" + _append_note(r) if _append_note(r) else ""), axis=1)

    _write_csv_v266577(df, path)
    return True, len(df)

def apply_structure_weight_continuation_patch_v266577():
    quality_map = _build_continuation_quality_map_v266577()
    targets = ["candidates.csv","core_candidates.csv","alpha_candidates.csv","trade_plan.csv","ignition_candidates.csv","strategy_evolution.csv","selection_debug.csv","pre_move_candidates.csv","top_opportunities.csv","final_action_plan.csv"]
    report = {
        "version": "v266.57.7",
        "mode": "append_only_structure_weight_split_plus_continuation_quality",
        "changed_strategy_logic": False,
        "changed_original_score": False,
        "changed_action": False,
        "changed_position": False,
        "enriched_stock_count": len(quality_map),
        "files": {},
        "updated_at": taipei_now_str(),
        "description": "CORE/IGNITION提高結構權重，ALPHA保留動能延續；新增回檔不破/量縮整理/守短均續強品質分，只作測試排序參考。",
    }
    for name in targets:
        for base in [ROOT, DATA_DIR]:
            p = base / name
            ok, n = _apply_v266577_to_csv(p, quality_map)
            if ok:
                report["files"][str(p)] = n
                print("v266.57.7 enriched:", p, n)
    for p in [ROOT / "structure_weight_continuation_report.json", DATA_DIR / "structure_weight_continuation_report.json"]:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        except Exception as e:
            print("write v266.57.7 report failed:", p, e)
    print(json.dumps(report, ensure_ascii=False, indent=2))

def main_v266577_structure_weight_continuation_patch():
    main_v266576_structure_pre_score_patch()
    apply_structure_weight_continuation_patch_v266577()


# ===== v311.1 CSV FINAL LOCK =====
# 在所有 v266.57.x append-only 補丁後，重新鎖定 trade_plan / candidates / core / alpha 的 action 與 TOP。
def apply_v311_csv_final_lock():
    targets = [
        "core_candidates.csv",
        "alpha_candidates.csv",
        "candidates.csv",
        "trade_plan.csv",
    ]
    for name in targets:
        for base in [ROOT, DATA_DIR]:
            p = base / name
            if not p.exists() or p.stat().st_size == 0:
                continue
            try:
                df = pd.read_csv(p, encoding="utf-8-sig")
            except Exception:
                try:
                    df = pd.read_csv(p, encoding="utf-8")
                except Exception:
                    continue
            if df.empty or "stock_id" not in df.columns:
                continue

            # 只有含 v310 欄位的檔案才鎖，避免破壞不相關 CSV
            if "attack_score_v312" not in df.columns and "attack_score_v310" not in df.columns and "attack_score_v309" not in df.columns:
                continue

            locked = apply_v311_final_action_lock(df)
            locked["source"] = "v312_balanced_attack_final_lock"
            locked.to_csv(p, index=False, encoding="utf-8-sig")
            print("v311 final csv locked:", p, len(locked))


if __name__ == "__main__":
    main_v266577_structure_weight_continuation_patch()
    apply_v311_csv_final_lock()
