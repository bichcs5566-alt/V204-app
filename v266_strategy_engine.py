import re
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

    # v313：拆成「硬封鎖」與「觀察降級」。
    # 之前 WATCH=0 的主因，是 bottom_repair / box_middle 被直接 hard_reject，全部掉到 BLOCK。
    hard_block = finance_like | short_turn_weak | fake_breakout | low_confidence
    soft_reject = bottom_repair_only | ma_sticky_no_attack | box_middle | liquidity_only
    hard_reject = hard_block | soft_reject

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

    # TEST：真正最大機會，必須攻擊條件夠完整。
    core_hits = trend_ok.astype(int) + momentum_ok.astype(int) + breakout_ok.astype(int) + volume_ok.astype(int) + chip_ok.astype(int)

    strict_test_ok = (
        (~hard_block) &
        (~soft_reject) &
        (attack >= 62) &
        (core_hits >= 4) &
        (momentum_ok | breakout_ok) &
        (volume_ok | chip_ok)
    )

    # WATCH：一次定位修正。
    # 只要不是金融/短線轉弱/假突破/極低信心，
    # 且已有「趨勢、動能、突破、量、籌碼」其中至少兩個，就進 WATCH。
    # 這會把「準攻擊、等確認」從 BLOCK 中切出來，不再全部歸零。
    watch_ok = (
        (~strict_test_ok) &
        (~hard_block) &
        (attack >= 30) &
        (core_hits >= 2) &
        (trend_ok | momentum_ok | breakout_ok | volume_ok)
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
    d["hard_reject_v313"] = hard_reject.astype(int)
    d["hard_block_v313"] = hard_block.astype(int)
    d["soft_reject_v313"] = soft_reject.astype(int)
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

# ===== v314 FIELD MAPPING GUARD =====
# 目的：一次補齊前端卡片需要的分類欄位，避免有些列只有簡表、沒有詳細卡片。
def _ensure_v314_strategy_fields(df):
    import numpy as np
    import pandas as pd

    if df is None or len(df) == 0:
        return df

    d = df.copy()

    def _txt_col(col, default=""):
        if col in d.columns:
            return d[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=d.index, dtype="object")

    action = _txt_col("v311_locked_action")
    if (action.str.len().sum() == 0) and ("action" in d.columns):
        action = _txt_col("action")
    action = action.str.upper().replace({"": "BLOCK"})

    engine = _txt_col("engine")
    stype = _txt_col("strategy_type")
    stype = stype.where(stype.str.len() > 0, engine)
    stype = stype.str.upper().replace({"": "CORE"})

    # strategy_layer：前端卡片主分類
    if "strategy_layer" not in d.columns:
        d["strategy_layer"] = ""
    d["strategy_layer"] = _txt_col("strategy_layer")
    d.loc[d["strategy_layer"].str.len() == 0, "strategy_layer"] = np.where(
        action.eq("TEST"), "主力動能",
        np.where(action.eq("WATCH"), "預備觀察", "風險封鎖")
    )

    # strategy_bucket：前端與續強提示常用
    if "strategy_bucket" not in d.columns:
        d["strategy_bucket"] = ""
    d["strategy_bucket"] = _txt_col("strategy_bucket")
    d.loc[d["strategy_bucket"].str.len() == 0, "strategy_bucket"] = np.where(
        action.eq("TEST"), "攻擊試單",
        np.where(action.eq("WATCH"), "等待確認", "禁止交易")
    )

    # strategy_type / engine：避免 ALPHA / CORE 統計有數字但卡片缺欄
    d["strategy_type"] = stype
    d["engine"] = stype

    # entry_type / action_sub / system_note：詳細卡片與提示用
    if "entry_type" not in d.columns:
        d["entry_type"] = ""
    d["entry_type"] = _txt_col("entry_type")
    d.loc[d["entry_type"].str.len() == 0, "entry_type"] = np.where(
        action.eq("TEST"), "最大機會試單",
        np.where(action.eq("WATCH"), "準攻擊觀察", "禁止")
    )

    if "action_sub" not in d.columns:
        d["action_sub"] = ""
    d["action_sub"] = _txt_col("action_sub")
    d.loc[d["action_sub"].str.len() == 0, "action_sub"] = np.where(
        action.eq("TEST"), "v314：攻擊條件達標，最大機會試單",
        np.where(action.eq("WATCH"), "v314：準攻擊，等待確認", "v314：非攻擊型或風險過高，禁止")
    )

    if "system_note" not in d.columns:
        d["system_note"] = ""
    d["system_note"] = _txt_col("system_note")
    d.loc[d["system_note"].str.len() == 0, "system_note"] = d["action_sub"]

    if "reason" not in d.columns:
        d["reason"] = ""
    d["reason"] = _txt_col("reason")
    d.loc[d["reason"].str.len() == 0, "reason"] = d["action_sub"]

    if "source" not in d.columns:
        d["source"] = "v314_strategy_field_mapping_lock"
    else:
        d["source"] = "v314_strategy_field_mapping_lock"

    return d


def apply_v311_final_action_lock(df):
    """
    v313 final action lock.
    欄位名稱仍維持 v311_locked_action，讓 app.js 不需要再改。
    一次定位：
    - TEST：strict_test_ok + 高攻擊分
    - WATCH：準攻擊 / 等確認 / soft_reject 但非垃圾
    - BLOCK：金融、短線轉弱、假突破、極低信心
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

    strict_test = _num("strict_test_ok_v310")
    watch_ok = _num("watch_ok_v310")
    hard_block = _num("hard_block_v313")
    soft_reject = _num("soft_reject_v313")

    # fallback：舊檔若沒有 hard_block_v313，至少保留金融封鎖
    finance_like = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)
    if "hard_block_v313" not in d.columns:
        hard_block = finance_like.astype(int)

    # v313 分層
    is_test = (
        (strict_test >= 1) &
        (hard_block < 1) &
        (~finance_like) &
        (attack >= 62)
    )

    is_watch = (
        (~is_test) &
        (watch_ok >= 1) &
        (hard_block < 1) &
        (~finance_like) &
        (attack >= 30)
    )

    d["v311_locked_action"] = np.where(is_test, "TEST", np.where(is_watch, "WATCH", "BLOCK"))
    d["action"] = d["v311_locked_action"]

    d["action_label"] = np.where(
        is_test, "試單",
        np.where(is_watch, "觀察", "禁止")
    )
    d["action_sub"] = np.where(
        is_test, "v313鎖定：攻擊條件達標，最大機會試單",
        np.where(is_watch, "v313鎖定：準攻擊，等待確認", "v313鎖定：非攻擊型或風險過高，禁止")
    )

    # v316：全域分層保險。
    # 如果 TEST 太多，代表中間層被吃掉；只保留最高分 TEST，其餘非硬封鎖轉 WATCH。
    # 這不是 UI 修補，是後端最終 action 欄位修正。
    score_col_tmp = "attack_score_v312" if "attack_score_v312" in d.columns else ("attack_score_v310" if "attack_score_v310" in d.columns else "entry_score")
    sort_col_tmp = "final_sort_score_v312" if "final_sort_score_v312" in d.columns else ("final_sort_score_v310" if "final_sort_score_v310" in d.columns else "entry_score")

    test_index_all = d.index[d["action"].eq("TEST")].tolist()
    if len(test_index_all) > 80:
        keep_test_idx = (
            d.loc[test_index_all]
            .sort_values([score_col_tmp, sort_col_tmp, "entry_score", "stock_id"],
                         ascending=[False, False, False, True])
            .head(80)
            .index
        )
        demote_idx = [i for i in test_index_all if i not in set(keep_test_idx)]
        if demote_idx:
            d.loc[demote_idx, "action"] = "WATCH"
            d.loc[demote_idx, "v311_locked_action"] = "WATCH"
            d.loc[demote_idx, "action_label"] = "觀察"
            d.loc[demote_idx, "action_sub"] = "v316鎖定：TEST過多，降為準攻擊觀察"

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

    score_col = "attack_score_v312" if "attack_score_v312" in d.columns else ("attack_score_v310" if "attack_score_v310" in d.columns else "attack_score_v309")
    sort_col = "final_sort_score_v312" if "final_sort_score_v312" in d.columns else ("final_sort_score_v310" if "final_sort_score_v310" in d.columns else "final_sort_score_v309")

    test_idx = (
        d.loc[d["action"].eq("TEST")]
        .sort_values([score_col, sort_col, "entry_score", "stock_id"],
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
        d.loc[idx, "top_reason"] = "v313試單TOP｜攻擊條件達標"

    watch_idx = (
        d.loc[d["action"].eq("WATCH")]
        .sort_values([score_col, sort_col, "entry_score", "stock_id"],
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
        d.loc[idx, "top_reason"] = "v313觀察TOP｜準攻擊但未達試單"

    d = d.sort_values(
        ["priority", "top_rank_v3066", score_col, sort_col, "entry_score", "stock_id"],
        ascending=[True, True, False, False, False, True]
    )

    d = _ensure_v314_strategy_fields(d)
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

    sort_cols = [c for c in ["attack_score_v312", "final_sort_score_v312", "attack_score_v310", "final_sort_score_v310", "entry_score", "stock_id"] if c in pool.columns]

    test_pool = test_pool.sort_values(
        [c for c in sort_cols if c in test_pool.columns],
        ascending=[False] * (len([c for c in sort_cols if c in test_pool.columns]) - 1) + [True]
    ).head(30) if len(test_pool) else test_pool

    watch_pool = watch_pool.sort_values(
        [c for c in sort_cols if c in watch_pool.columns],
        ascending=[False] * (len([c for c in sort_cols if c in watch_pool.columns]) - 1) + [True]
    ).head(80) if len(watch_pool) else watch_pool

    block_pool = block_pool.sort_values(
        [c for c in sort_cols if c in block_pool.columns],
        ascending=[False] * (len([c for c in sort_cols if c in block_pool.columns]) - 1) + [True]
    ).head(80) if len(block_pool) else block_pool

    s = pd.concat([test_pool, watch_pool, block_pool], ignore_index=True)
    s = s.drop_duplicates("stock_id")

    # 最後再鎖一次，保證 trade_plan.csv 寫出去前不會被舊 action 混入，並補齊前端卡片欄位
    s = apply_v311_final_action_lock(s)
    s = _ensure_v314_strategy_fields(s)

    trade_date = next_trade_date(signal_date)
    rows = []

    for _, r in s.iterrows():
        px = float(r.get("close", 0)) * 1.001
        action = str(r.get("action", "BLOCK")).upper()
        st = r.get("strategy_type", r.get("engine", ""))
        score = float(r.get("final_sort_score_v312", r.get("final_sort_score_v310", r.get("final_sort_score_v309", r.get("entry_score", 0)))))
        liq = str(r.get("liquidity_level", ""))

        strategy_layer = str(r.get("strategy_layer", "") or "").strip()
        strategy_bucket = str(r.get("strategy_bucket", "") or "").strip()
        if not strategy_layer:
            strategy_layer = "主力動能" if action == "TEST" else ("預備觀察" if action == "WATCH" else "風險封鎖")
        if not strategy_bucket:
            strategy_bucket = "攻擊試單" if action == "TEST" else ("等待確認" if action == "WATCH" else "禁止交易")
        if not st:
            st = str(r.get("engine", "") or "CORE").upper()

        # v314：只有 TEST 給試單資金，WATCH/BLOCK 都 0
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
            "engine": st,
            "strategy_layer": strategy_layer,
            "strategy_bucket": strategy_bucket,
            "layer": strategy_layer,
            "bucket": strategy_bucket,
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
            "source": "v318_ignition_evolution_real_split",
            "reason": r.get("reason", r.get("note", "")),
            "system_note": r.get("system_note", r.get("note", "")),
            "note": r.get("note", ""),
        })

    return pd.DataFrame(rows)



# ===== v316 DIRECT PANEL FILE WRITER =====
# 目的：
# - 不再假設前端統一只吃 trade_plan。
# - app.js 的 IGNITION / EVOLUTION 是獨立 panel 檔案，所以後端必須明確產出：
#   mobile_dashboard_v1/data/ignition_candidates.csv
#   mobile_dashboard_v1/data/strategy_evolution.csv
# - 同時寫 root 與 mobile_dashboard_v1/data，避免 GitHub Pages 路徑吃不到。
def write_v318_ignition_evolution_real_split(pool=None, plan=None):
    import numpy as np
    import pandas as pd
    import json

    def _read_any(name):
        for base in [ROOT, DATA_DIR]:
            p = base / name
            if p.exists() and p.stat().st_size > 0:
                try:
                    return pd.read_csv(p, encoding="utf-8-sig")
                except Exception:
                    try:
                        return pd.read_csv(p, encoding="utf-8")
                    except Exception:
                        pass
        return pd.DataFrame()

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=df.index, dtype="object")

    def _write(df, name):
        df = df.copy()
        for base in [ROOT, DATA_DIR]:
            base.mkdir(parents=True, exist_ok=True)
            p = base / name
            df.to_csv(p, index=False, encoding="utf-8-sig")
            print("v316 wrote panel:", p, len(df))

    frames = []
    if pool is not None and len(pool):
        frames.append(pool.copy())
    if plan is not None and len(plan):
        frames.append(plan.copy())

    # fallback：即使 main 沒傳，也從已輸出 CSV 讀回來
    for name in ["trade_plan.csv", "candidates.csv", "core_candidates.csv", "alpha_candidates.csv"]:
        df = _read_any(name)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        empty_ign = pd.DataFrame(columns=[
            "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
            "strategy_name","entry_score","score","close","ref_price","ignition_phase","entry_type",
            "section_top_opportunity","top_opportunity","execution_flag","fake_score","fake_risk_tag",
            "fake_risk_level","fake_flags","fake_reason_zh","ignition_hint_zh","operation_advice_zh",
            "reason","system_note","source","liquidity_level","liquidity_score","volume","turnover"
        ])
        empty_evo = pd.DataFrame(columns=[
            "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
            "strategy_name","evolution_score","entry_score","score","close","ref_price","evolution_phase",
            "entry_type","section_top_opportunity","top_opportunity","execution_flag","reason","system_note",
            "source","liquidity_level","liquidity_score","volume","turnover"
        ])
        _write(empty_ign, "ignition_candidates.csv")
        _write(empty_evo, "strategy_evolution.csv")
        return

    df = pd.concat(frames, ignore_index=True)
    if "stock_id" not in df.columns:
        _write(pd.DataFrame(), "ignition_candidates.csv")
        _write(pd.DataFrame(), "strategy_evolution.csv")
        return

    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock_id"].astype(str).str[:4])
    df = df.dropna(subset=["stock_id"]).drop_duplicates("stock_id", keep="first").copy()

    try:
        df = apply_v311_final_action_lock(df)
        df = _ensure_v314_strategy_fields(df)
    except Exception:
        pass

    action = _txt(df, "v311_locked_action")
    action = action.where(action.str.len() > 0, _txt(df, "action")).str.upper()

    # 如果 WATCH 仍為 0，就把 TEST 排名 81 以後明確切成 WATCH，避免中間層消失。
    if int((action == "WATCH").sum()) == 0 and int((action == "TEST").sum()) > 20:
        score_base = _num(df, "attack_score_v312")
        if float(score_base.abs().sum()) == 0:
            score_base = _num(df, "attack_score_v310")
        test_idx = df.index[action == "TEST"]
        watch_idx = df.loc[test_idx].assign(_s=score_base.loc[test_idx]).sort_values("_s", ascending=False).iloc[20:100].index
        df.loc[watch_idx, "action"] = "WATCH"
        df.loc[watch_idx, "v311_locked_action"] = "WATCH"
        df.loc[watch_idx, "action_label"] = "觀察"
        df.loc[watch_idx, "action_sub"] = "v316：準攻擊觀察層"
        action = _txt(df, "v311_locked_action").str.upper()

    attack = _num(df, "attack_score_v312")
    if float(attack.abs().sum()) == 0:
        attack = _num(df, "attack_score_v310")
    if float(attack.abs().sum()) == 0:
        attack = _num(df, "entry_score")

    final_sort = _num(df, "final_sort_score_v312")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num(df, "final_sort_score_v310")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num(df, "score")

    trend = _num(df, "trend_ok_v310")
    momentum = _num(df, "momentum_ok_v310")
    breakout = _num(df, "breakout_ok_v310")
    volume_ok = _num(df, "volume_ok_v310")
    chip = _num(df, "chip_ok_v310")
    hard_block = _num(df, "hard_block_v313")
    industry = _txt(df, "industry")
    sid = _txt(df, "stock_id")
    finance = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

    close = _num(df, "close")
    if float(close.abs().sum()) == 0:
        close = _num(df, "ref_price")
    if float(close.abs().sum()) == 0:
        close = _num(df, "price")

    panel_score = (
        attack * 0.60 + final_sort * 0.25 +
        trend * 8 + momentum * 10 + breakout * 10 + volume_ok * 8 + chip * 6 -
        hard_block * 100 - finance.astype(int) * 120
    ).round(2)

    base_mask = action.isin(["TEST", "WATCH"]) & (hard_block < 1) & (~finance)

    # IGNITION：優先挑突破/動能，但保底挑 TEST/WATCH 最高分，避免空白。
    ign = df.loc[base_mask & ((momentum >= 1) | (breakout >= 1))].copy()
    if ign.empty:
        ign = df.loc[base_mask].copy()

    ign_cols = [
        "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
        "strategy_name","entry_score","score","close","ref_price","ignition_phase","entry_type",
        "section_top_opportunity","top_opportunity","execution_flag","fake_score","fake_risk_tag",
        "fake_risk_level","fake_flags","fake_reason_zh","ignition_hint_zh","operation_advice_zh",
        "reason","system_note","source","liquidity_level","liquidity_score","volume","turnover"
    ]

    if not ign.empty:
        ign["_panel_score"] = panel_score.loc[ign.index]
        ign = ign.sort_values(["_panel_score","stock_id"], ascending=[False, True]).head(10).copy()
        ign["action"] = "WATCH"
        ign["final_action"] = "WATCH"
        ign["strategy_type"] = "IGNITION"
        ign["bucket"] = "IGNITION"
        ign["strategy_name"] = "IGNITION 起漲訊號"
        ign["entry_score"] = ign["_panel_score"].round(2)
        ign["score"] = ign["_panel_score"].round(2)
        ign["close"] = close.loc[ign.index].round(2)
        ign["ref_price"] = ign["close"]
        ign["ignition_phase"] = "起漲觀察"
        ign["entry_type"] = "防假突破觀察"
        ign["section_top_opportunity"] = [f"IGNITION_TOP{i}" for i in range(1, len(ign)+1)]
        ign["top_opportunity"] = [f"🧪TOP{i}" for i in range(1, len(ign)+1)]
        ign["execution_flag"] = ign["section_top_opportunity"]
        ign["fake_score"] = 15
        ign["fake_risk_tag"] = "低假突破"
        ign["fake_risk_level"] = "LOW"
        ign["fake_flags"] = ""
        ign["fake_reason_zh"] = "量價、均線、突破條件接近起漲；仍需隔日確認。"
        ign["ignition_hint_zh"] = "觀察是否延續放量、站穩短均、K棒不轉弱。"
        ign["operation_advice_zh"] = "不自動買進；只作防假突破觀察。"
        ign["reason"] = "v316 起漲訊號：由 TEST/WATCH 候選中挑出。"
        ign["system_note"] = "IGNITION：提示面板，不直接改主清單。"
        ign["source"] = "策略進場"

    for c in ign_cols:
        if c not in ign.columns:
            ign[c] = ""
    _write(ign[ign_cols].copy(), "ignition_candidates.csv")

    # EVOLUTION：策略進化提示，同樣保底不空。
    evo = df.loc[base_mask].copy()
    evo_cols = [
        "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
        "strategy_name","evolution_score","entry_score","score","close","ref_price","evolution_phase",
        "entry_type","section_top_opportunity","top_opportunity","execution_flag","reason","system_note",
        "source","liquidity_level","liquidity_score","volume","turnover"
    ]

    if not evo.empty:
        evo["_panel_score"] = panel_score.loc[evo.index]
        evo = evo.sort_values(["_panel_score","stock_id"], ascending=[False, True]).head(10).copy()
        evo["action"] = "WATCH"
        evo["final_action"] = "WATCH"
        evo["strategy_type"] = "EVOLUTION"
        evo["bucket"] = "EVOLUTION"
        evo["strategy_name"] = "EVOLUTION 策略進化訊號"
        evo["evolution_score"] = evo["_panel_score"].round(2)
        evo["entry_score"] = evo["_panel_score"].round(2)
        evo["score"] = evo["_panel_score"].round(2)
        evo["close"] = close.loc[evo.index].round(2)
        evo["ref_price"] = evo["close"]
        evo["evolution_phase"] = np.where(action.loc[evo.index].eq("TEST"), "TEST→核心觀察", "WATCH→試單候選")
        evo["entry_type"] = evo["evolution_phase"]
        evo["section_top_opportunity"] = [f"EVOLUTION_TOP{i}" for i in range(1, len(evo)+1)]
        evo["top_opportunity"] = [f"🧬TOP{i}" for i in range(1, len(evo)+1)]
        evo["execution_flag"] = evo["section_top_opportunity"]
        evo["reason"] = "v316 策略進化：追蹤可升級標的。"
        evo["system_note"] = "EVOLUTION：提示面板，不自動加碼。"
        evo["source"] = "策略進場"

    for c in evo_cols:
        if c not in evo.columns:
            evo[c] = ""
    _write(evo[evo_cols].copy(), "strategy_evolution.csv")

    # 改 meta source，確認畫面不是舊 source
    for base in [ROOT, DATA_DIR]:
        p = base / "meta.json"
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v318_ignition_evolution_real_split"
                data["watch_layer_note"] = "v316 已明確產出 WATCH 中間層與兩個提示面板 CSV"
                data["ignition_count"] = int(len(ign))
                data["evolution_count"] = int(len(evo))
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass


def main():
    df = load_feature()
    signal_date, latest = latest_valid(df)
    regime, info = detect_regime(latest)

    # v314：保留足夠候選，避免前 60 名全是 TEST，WATCH 被截掉。
    core = apply_v311_final_action_lock(core_engine(latest)).head(240)
    alpha = apply_v311_final_action_lock(alpha_engine(latest)).head(240)

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

    core = _ensure_v314_strategy_fields(core.assign(engine="CORE"))
    alpha = _ensure_v314_strategy_fields(alpha.assign(engine="ALPHA"))
    candidates = _ensure_v314_strategy_fields(candidates)

    write_both(core, "core_candidates.csv")
    write_both(alpha, "alpha_candidates.csv")
    write_both(candidates, "candidates.csv")
    write_both(plan, "trade_plan.csv")
    write_both(debug, "selection_debug.csv")

    # v316：這兩個不是統一主清單，而是 app.js 獨立面板資料源；必須明確寫出。
    write_v318_ignition_evolution_real_split(candidates, plan)

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v318_ignition_evolution_real_split",
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

    df.loc[:, "adjusted_signal_score_v26657_7"] = (base_score + structure_pre * s_weights + continuation_q * c_weights).round(3)
    df.loc[:, "structure_weight_v26657_7"] = s_weights.round(2)
    df.loc[:, "continuation_weight_v26657_7"] = c_weights.round(2)
    df.loc[:, "strategy_bucket_v26657_7"] = buckets
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
            locked["source"] = "v313_watch_layer_final_lock"
            locked.to_csv(p, index=False, encoding="utf-8-sig")
            print("v314 final csv locked:", p, len(locked))



# ===== v315 IGNITION / EVOLUTION OUTPUT BRIDGE =====
# 目的：
# - 產出 app.js 會讀取的 ignition_candidates.csv / strategy_evolution.csv。
# - 不改 TEST / WATCH / BLOCK 主分類。
# - IGNITION / EVOLUTION 只是提示面板，不自動加碼、不自動買進。
def apply_v315_ignition_evolution_outputs():
    import numpy as np
    import pandas as pd
    from pathlib import Path
    from datetime import datetime

    def _read_any(name):
        for base in [ROOT, DATA_DIR]:
            p = base / name
            if p.exists() and p.stat().st_size > 0:
                try:
                    return pd.read_csv(p, encoding="utf-8-sig")
                except Exception:
                    try:
                        return pd.read_csv(p, encoding="utf-8")
                    except Exception:
                        pass
        return pd.DataFrame()

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=df.index, dtype="object")

    def _write_both(df, name):
        df = df.copy()
        for base in [ROOT, DATA_DIR]:
            p = base / name
            p.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(p, index=False, encoding="utf-8-sig")
            print("v315 wrote:", p, len(df))

    # 以 candidates + trade_plan 合併，避免 trade_plan 截斷後 ignition/evolution 沒資料。
    frames = []
    for name in ["trade_plan.csv", "candidates.csv", "core_candidates.csv", "alpha_candidates.csv"]:
        df = _read_any(name)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        _write_both(pd.DataFrame(), "ignition_candidates.csv")
        _write_both(pd.DataFrame(), "strategy_evolution.csv")
        return

    pool = pd.concat(frames, ignore_index=True)
    if "stock_id" not in pool.columns:
        _write_both(pd.DataFrame(), "ignition_candidates.csv")
        _write_both(pd.DataFrame(), "strategy_evolution.csv")
        return

    pool["stock_id"] = pool["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(pool["stock_id"].astype(str).str[:4])
    pool = pool.dropna(subset=["stock_id"]).drop_duplicates("stock_id", keep="first").copy()

    # 補齊欄位，讓前端卡片不空。
    try:
        pool = apply_v311_final_action_lock(pool)
        pool = _ensure_v314_strategy_fields(pool)
    except Exception:
        pass

    action = _txt(pool, "v311_locked_action")
    action = action.where(action.str.len() > 0, _txt(pool, "action")).str.upper()

    attack = _num(pool, "attack_score_v312")
    if float(attack.abs().sum()) == 0:
        attack = _num(pool, "attack_score_v310")
    if float(attack.abs().sum()) == 0:
        attack = _num(pool, "entry_score")

    final_sort = _num(pool, "final_sort_score_v312")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num(pool, "final_sort_score_v310")
    if float(final_sort.abs().sum()) == 0:
        final_sort = _num(pool, "score")

    trend = _num(pool, "trend_ok_v310")
    momentum = _num(pool, "momentum_ok_v310")
    breakout = _num(pool, "breakout_ok_v310")
    volume_ok = _num(pool, "volume_ok_v310")
    chip = _num(pool, "chip_ok_v310")
    hard_block = _num(pool, "hard_block_v313")
    hard_reject = _num(pool, "hard_reject_v313")
    if float(hard_reject.abs().sum()) == 0:
        hard_reject = _num(pool, "hard_reject_v312")

    industry = _txt(pool, "industry")
    sid = _txt(pool, "stock_id")
    finance = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

    close = _num(pool, "close")
    if float(close.abs().sum()) == 0:
        close = _num(pool, "ref_price")
    if float(close.abs().sum()) == 0:
        close = _num(pool, "price")

    liq = _num(pool, "liquidity_score")

    # IGNITION：起漲雷達。以 TEST 為主，但排除硬封鎖、金融、假突破。
    ignition_score = (
        attack * 0.55 +
        final_sort * 0.20 +
        trend * 8 +
        momentum * 12 +
        breakout * 14 +
        volume_ok * 10 +
        chip * 8 +
        (liq >= 70).astype(int) * 3 -
        hard_block * 80 -
        finance.astype(int) * 100
    ).round(2)

    ignition_mask = (
        action.isin(["TEST", "WATCH"]) &
        (hard_block < 1) &
        (~finance) &
        (ignition_score >= 50) &
        ((momentum >= 1) | (breakout >= 1))
    )

    ign = pool.loc[ignition_mask].copy()
    if ign.empty:
        # 不要讓面板空白：若當天沒有乾淨起漲，給最接近的 TEST 當「觀察」，但仍保留提示文字。
        ign = pool.loc[
            action.eq("TEST") & (hard_block < 1) & (~finance)
        ].copy()

    if not ign.empty:
        ign["_ignition_score_v315"] = ignition_score.loc[ign.index]
        ign = ign.sort_values(["_ignition_score_v315", "stock_id"], ascending=[False, True]).head(10).copy()
        ign["action"] = "WATCH"
        ign["final_action"] = "WATCH"
        ign["strategy_type"] = "IGNITION"
        ign["bucket"] = "IGNITION"
        ign["strategy_name"] = "IGNITION 起漲啟動"
        ign["entry_score"] = ign["_ignition_score_v315"].round(2)
        ign["score"] = ign["_ignition_score_v315"].round(2)
        ign["close"] = close.loc[ign.index].round(2)
        ign["ref_price"] = ign["close"]
        ign["ignition_phase"] = np.where(
            _num(ign, "breakout_ok_v310") >= 1,
            "突破確認觀察",
            "起漲前夕觀察"
        )
        ign["entry_type"] = ign["ignition_phase"]
        ign["section_top_opportunity"] = [f"IGNITION_TOP{i}" for i in range(1, len(ign) + 1)]
        ign["top_opportunity"] = [f"🧪TOP{i}" for i in range(1, len(ign) + 1)]
        ign["execution_flag"] = ign["section_top_opportunity"]
        ign["fake_score"] = np.where(_num(ign, "hard_reject_v313") >= 1, 80, 15)
        ign["fake_risk_level"] = np.where(_num(ign, "hard_reject_v313") >= 1, "HIGH", "LOW")
        ign["fake_risk_tag"] = np.where(_num(ign, "hard_reject_v313") >= 1, "假突破風險", "低假突破")
        ign["fake_flags"] = np.where(_num(ign, "hard_reject_v313") >= 1, "hard_reject", "")
        ign["fake_reason_zh"] = np.where(
            _num(ign, "hard_reject_v313") >= 1,
            "仍有假突破或結構風險，只能觀察。",
            "量價與均線條件接近起漲，假突破風險低。"
        )
        ign["ignition_hint_zh"] = "起漲雷達：觀察隔日是否延續放量、站穩短均、K棒不轉弱。"
        ign["operation_advice_zh"] = "不自動買進；若隔日延續強勢才考慮小量試單。"
        ign["reason"] = "v315 起漲訊號：從 TEST/WATCH 中挑選量價、均線、突破較完整者。"
        ign["system_note"] = "IGNITION：只做防假突破觀察，不自動丟入買進。"
        ign["source"] = "策略進場"

    ignition_cols = [
        "stock_id", "stock_name", "industry", "action", "final_action", "strategy_type", "bucket",
        "strategy_name", "entry_score", "score", "close", "ref_price", "ignition_phase",
        "entry_type", "section_top_opportunity", "top_opportunity", "execution_flag",
        "fake_score", "fake_risk_tag", "fake_risk_level", "fake_flags", "fake_reason_zh",
        "ignition_hint_zh", "operation_advice_zh", "reason", "system_note", "source",
        "liquidity_level", "liquidity_score", "volume", "turnover"
    ]
    for c in ignition_cols:
        if c not in ign.columns:
            ign[c] = ""
    _write_both(ign[ignition_cols].copy(), "ignition_candidates.csv")

    # EVOLUTION：策略升級提示。從 TEST 優先，其次 WATCH，挑分數最高。
    evolution_score = (
        attack * 0.60 +
        final_sort * 0.25 +
        trend * 6 +
        momentum * 8 +
        breakout * 8 +
        volume_ok * 6 +
        chip * 6 -
        hard_block * 90 -
        finance.astype(int) * 100
    ).round(2)

    evo_mask = (
        action.isin(["TEST", "WATCH"]) &
        (hard_block < 1) &
        (~finance) &
        (evolution_score >= 45)
    )

    evo = pool.loc[evo_mask].copy()
    if evo.empty:
        evo = pool.loc[action.eq("TEST") & (hard_block < 1) & (~finance)].copy()

    if not evo.empty:
        evo["_evolution_score_v315"] = evolution_score.loc[evo.index]
        evo = evo.sort_values(["_evolution_score_v315", "stock_id"], ascending=[False, True]).head(10).copy()
        evo["action"] = "WATCH"
        evo["final_action"] = "WATCH"
        evo["strategy_type"] = "EVOLUTION"
        evo["bucket"] = "EVOLUTION"
        evo["strategy_name"] = "EVOLUTION 策略進化鏈"
        evo["evolution_score"] = evo["_evolution_score_v315"].round(2)
        evo["score"] = evo["_evolution_score_v315"].round(2)
        evo["entry_score"] = evo["_evolution_score_v315"].round(2)
        evo["close"] = close.loc[evo.index].round(2)
        evo["ref_price"] = evo["close"]
        evo["evolution_phase"] = np.where(
            action.loc[evo.index].eq("TEST"),
            "TEST→核心觀察",
            "WATCH→試單候選"
        )
        evo["entry_type"] = evo["evolution_phase"]
        evo["section_top_opportunity"] = [f"EVOLUTION_TOP{i}" for i in range(1, len(evo) + 1)]
        evo["top_opportunity"] = [f"🧬TOP{i}" for i in range(1, len(evo) + 1)]
        evo["execution_flag"] = evo["section_top_opportunity"]
        evo["reason"] = "v315 策略進化：追蹤可由觀察升級到試單、或由試單升級到核心的標的。"
        evo["system_note"] = "EVOLUTION：升級提示，不自動加碼；需等隔日延續與風控確認。"
        evo["source"] = "策略進場"

    evolution_cols = [
        "stock_id", "stock_name", "industry", "action", "final_action", "strategy_type", "bucket",
        "strategy_name", "evolution_score", "entry_score", "score", "close", "ref_price",
        "evolution_phase", "entry_type", "section_top_opportunity", "top_opportunity",
        "execution_flag", "reason", "system_note", "source",
        "liquidity_level", "liquidity_score", "volume", "turnover"
    ]
    for c in evolution_cols:
        if c not in evo.columns:
            evo[c] = ""
    _write_both(evo[evolution_cols].copy(), "strategy_evolution.csv")

    # 更新 meta / summary 的提示來源，避免前端仍顯示只有舊 source。
    for base in [ROOT, DATA_DIR]:
        for name in ["meta.json", "final_action_summary.json"]:
            p = base / name
            if not p.exists():
                continue
            try:
                import json
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v318_ignition_evolution_real_split"
                data["ignition_count"] = int(len(ign))
                data["evolution_count"] = int(len(evo))
                data["ignition_evolution_note"] = "v315 已由後端產出 ignition_candidates.csv / strategy_evolution.csv"
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass



# ===== v317 PANEL FILE HARD GUARANTEE =====
# 目的：
# 1) 不再依賴 app.js 是否「統一吃資料」；IGNITION / EVOLUTION 是獨立面板 CSV，這裡強制寫出。
# 2) 不再依賴 v315/v316 是否因欄位不足而挑不到資料；直接從 trade_plan/candidates/core/alpha 取 TEST/WATCH 高分候選。
# 3) 同時寫 root 與 mobile_dashboard_v1/data，並建立資料夾。
# ===== v318 IGNITION / EVOLUTION REAL SEPARATION =====
# 目的：
# - IGNITION：起漲前夕 / 防假突破觀察，不等於直接買。
# - EVOLUTION：從已經更強的 TEST/WATCH 候選中挑「趨勢確認 / 續強升級」。
# - 兩份 panel CSV 繼續明確寫出 root + mobile_dashboard_v1/data，避免前端空白。
# - 不動 UI、不動 workflow、不動主清單格式。
def write_v317_panel_files_hard_guarantee():
    import pandas as pd
    import numpy as np
    import json

    def _read_csv_safe(path):
        try:
            if path.exists() and path.stat().st_size > 0:
                return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8")
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def _num(df, col, default=0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=df.index, dtype="object")

    def _first_existing_num(df, cols, default=0):
        out = pd.Series(default, index=df.index, dtype="float64")
        for c in cols:
            if c in df.columns:
                v = _num(df, c, np.nan)
                out = out.where(out.notna() & (out != default), v.fillna(default))
        return out.fillna(default)

    def _make_empty(kind):
        if kind == "ignition":
            return pd.DataFrame(columns=[
                "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
                "strategy_name","entry_score","score","close","ref_price","ignition_phase","entry_type",
                "section_top_opportunity","top_opportunity","execution_flag","fake_score","fake_risk_tag",
                "fake_risk_level","fake_flags","fake_reason_zh","ignition_hint_zh","operation_advice_zh",
                "reason","system_note","source","liquidity_level","liquidity_score","volume","turnover"
            ])
        return pd.DataFrame(columns=[
            "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
            "strategy_name","evolution_score","entry_score","score","close","ref_price","evolution_phase",
            "entry_type","section_top_opportunity","top_opportunity","execution_flag","reason","system_note",
            "source","liquidity_level","liquidity_score","volume","turnover"
        ])

    frames = []
    # candidates 優先，因為欄位比 trade_plan 完整；trade_plan 放後面避免把詳細欄位蓋掉。
    for base in [ROOT, DATA_DIR]:
        for name in ["candidates.csv", "core_candidates.csv", "alpha_candidates.csv", "trade_plan.csv"]:
            df = _read_csv_safe(base / name)
            if df is not None and not df.empty and "stock_id" in df.columns:
                frames.append(df)

    if not frames:
        ign = _make_empty("ignition")
        evo = _make_empty("evolution")
    else:
        pool = pd.concat(frames, ignore_index=True).copy()
        pool["stock_id"] = pool["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(pool["stock_id"].astype(str).str[:4])
        pool = pool.dropna(subset=["stock_id"]).copy()

        # 盡量保留欄位較完整的資料列：欄位越多、非空越多越優先。
        pool["_field_count_v318"] = pool.notna().sum(axis=1)
        pool = pool.sort_values(["stock_id", "_field_count_v318"], ascending=[True, False]).drop_duplicates("stock_id", keep="first").copy()

        try:
            pool = apply_v311_final_action_lock(pool)
            pool = _ensure_v314_strategy_fields(pool)
        except Exception as e:
            print("v318 mapping skipped:", repr(e))

        action = _txt(pool, "v311_locked_action")
        action = action.where(action.str.len() > 0, _txt(pool, "action")).str.upper()

        industry = _txt(pool, "industry")
        sid = _txt(pool, "stock_id")
        finance = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

        close = _first_existing_num(pool, ["close", "ref_price", "price"], 0)
        ma5 = _first_existing_num(pool, ["ma5", "MA5"], close)
        ma10 = _first_existing_num(pool, ["ma10", "MA10"], close)
        ma20 = _first_existing_num(pool, ["ma20", "MA20"], close)
        ma60 = _first_existing_num(pool, ["ma60", "MA60"], close)

        mom3 = _first_existing_num(pool, ["mom3", "return_3d"], 0)
        mom5 = _first_existing_num(pool, ["mom5", "return_5d"], 0)
        mom10 = _first_existing_num(pool, ["mom10", "return_10d"], 0)
        mom20 = _first_existing_num(pool, ["mom20", "return_20d"], 0)

        vol_ratio = _first_existing_num(pool, ["volume_ratio", "vol_ratio"], 1)
        liq = _first_existing_num(pool, ["liquidity_score"], 0)
        main_force = _first_existing_num(pool, ["main_force_score_v300", "main_force_score"], 0)
        chip = _first_existing_num(pool, ["chip_score"], 0)
        obv = _first_existing_num(pool, ["obv_mom5"], 0)

        high20 = _first_existing_num(pool, ["high_20", "high20"], close)
        low20 = _first_existing_num(pool, ["low_20", "low20"], close)
        high60 = _first_existing_num(pool, ["high_60", "high60"], close)

        attack = _first_existing_num(pool, ["attack_score_v312", "attack_score_v310", "attack_score_v309", "entry_score", "score"], 0)
        final_sort = _first_existing_num(pool, ["final_sort_score_v312", "final_sort_score_v310", "final_sort_score_v309", "score", "entry_score"], 0)

        hard_block = _first_existing_num(pool, ["hard_block_v313"], 0)
        hard_reject = _first_existing_num(pool, ["hard_reject_v313", "hard_reject_v312", "hard_reject_v310"], 0)
        fake_risk = _first_existing_num(pool, ["fake_score"], 0)

        base_mask = action.isin(["TEST", "WATCH"]) & (~finance) & (hard_block < 1)

        close_safe = close.replace(0, np.nan)
        ma20_safe = ma20.replace(0, np.nan)
        high20_safe = high20.replace(0, np.nan)
        rng20 = ((high20 - low20) / low20.replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
        box_pos = ((close - low20) / (high20 - low20).replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
        close_to_high20 = (close / high20_safe).replace([np.inf, -np.inf], 0).fillna(0)

        trend_start = (
            (close >= ma20 * 0.995) &
            (ma5 >= ma10 * 0.995) &
            (mom5 > -0.01) &
            (mom10 > 0.00)
        )

        compression_ready = (
            (rng20.between(0.045, 0.22)) &
            (box_pos >= 0.55) &
            (close_to_high20 >= 0.88)
        )

        volume_start = vol_ratio.between(1.05, 3.80)
        chip_start = (main_force >= 45) | (chip >= 45) | (obv > 0)
        fake_low = (hard_reject < 1) & (fake_risk < 70)
        not_overheat = (mom20 < 0.55) & ((close / ma20_safe - 1).replace([np.inf, -np.inf], 0).fillna(0) < 0.32)

        # IGNITION：早期起漲，不追求最強，重點是「接近發動 + 假突破低」。
        ignition_score = (
            trend_start.astype(int) * 22 +
            compression_ready.astype(int) * 20 +
            volume_start.astype(int) * 18 +
            chip_start.astype(int) * 18 +
            fake_low.astype(int) * 12 +
            not_overheat.astype(int) * 10 +
            (mom3 > 0).astype(int) * 4 +
            (attack / 10).clip(0, 12)
        ).round(2)

        ignition_mask = (
            base_mask &
            (ignition_score >= 58) &
            (trend_start | compression_ready) &
            fake_low &
            not_overheat
        )

        ign = pool.loc[ignition_mask].copy()
        if ign.empty:
            # 保底不空，但仍用 ignition_score 排序，不再直接複製所有最高分。
            ign = pool.loc[base_mask & fake_low].copy()

        if not ign.empty:
            ign["_ignition_score_v318"] = ignition_score.loc[ign.index]
            ign = ign.sort_values(["_ignition_score_v318", "stock_id"], ascending=[False, True]).head(15).copy()
            ign["action"] = "WATCH"
            ign["final_action"] = "WATCH"
            ign["strategy_type"] = "IGNITION"
            ign["bucket"] = "IGNITION"
            ign["strategy_name"] = "IGNITION 起漲訊號"
            ign["entry_score"] = ign["_ignition_score_v318"].round(2)
            ign["score"] = ign["_ignition_score_v318"].round(2)
            ign["close"] = close.loc[ign.index].round(2)
            ign["ref_price"] = ign["close"]
            ign["ignition_phase"] = np.where(
                compression_ready.loc[ign.index],
                "收斂後點火",
                "起漲前夕"
            )
            ign["entry_type"] = "防假突破觀察"
            ign["section_top_opportunity"] = [f"IGNITION_TOP{i}" for i in range(1, len(ign) + 1)]
            ign["top_opportunity"] = [f"🧪TOP{i}" for i in range(1, len(ign) + 1)]
            ign["execution_flag"] = ign["section_top_opportunity"]
            ign["fake_score"] = np.where(fake_low.loc[ign.index], 15, 70)
            ign["fake_risk_tag"] = np.where(fake_low.loc[ign.index], "低假突破", "需確認")
            ign["fake_risk_level"] = np.where(fake_low.loc[ign.index], "LOW", "MID")
            ign["fake_flags"] = ""
            ign["fake_reason_zh"] = "起漲前雷達：接近發動，但需隔日確認放量、站穩短均、K棒不轉弱。"
            ign["ignition_hint_zh"] = "適合小倉觀察；不追高，等待突破後不跌回。"
            ign["operation_advice_zh"] = "不自動買進；只作防假突破觀察。"
            ign["reason"] = "v318 IGNITION：起漲前夕 / 收斂後點火 / 假突破低。"
            ign["system_note"] = "IGNITION：早期雷達，不等於主力倉位。"
            ign["source"] = "策略進場"

        ign_cols = [
            "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
            "strategy_name","entry_score","score","close","ref_price","ignition_phase","entry_type",
            "section_top_opportunity","top_opportunity","execution_flag","fake_score","fake_risk_tag",
            "fake_risk_level","fake_flags","fake_reason_zh","ignition_hint_zh","operation_advice_zh",
            "reason","system_note","source","liquidity_level","liquidity_score","volume","turnover"
        ]
        for c in ign_cols:
            if c not in ign.columns:
                ign[c] = ""

        # EVOLUTION：趨勢確認/升級，不應和 IGNITION 完全相同。
        strong_trend = (
            (close >= ma5 * 0.99) &
            (ma5 >= ma10 * 1.002) &
            (ma10 >= ma20 * 1.002) &
            (ma20 >= ma60 * 0.985)
        )
        momentum_confirm = (
            (mom5 > 0.012) &
            (mom10 > 0.025) &
            (mom20 > 0.035)
        )
        price_confirm = (
            (close_to_high20 >= 0.94) &
            (box_pos >= 0.70)
        )
        volume_confirm = (
            vol_ratio.between(1.15, 4.80) |
            ((main_force >= 60) & (chip >= 55))
        )
        chip_confirm = (main_force >= 58) | (chip >= 60) | (obv > 0)

        evolution_score = (
            strong_trend.astype(int) * 24 +
            momentum_confirm.astype(int) * 24 +
            price_confirm.astype(int) * 18 +
            volume_confirm.astype(int) * 16 +
            chip_confirm.astype(int) * 12 +
            fake_low.astype(int) * 6 +
            (attack / 10).clip(0, 12)
        ).round(2)

        evolution_mask = (
            base_mask &
            (evolution_score >= 68) &
            strong_trend &
            (momentum_confirm | price_confirm) &
            fake_low
        )

        evo = pool.loc[evolution_mask].copy()

        # 若當天沒有真正 EVOLUTION，用比 IGNITION 更高門檻的候選補，不直接複製 ignition 前幾名。
        if evo.empty:
            evo = pool.loc[
                base_mask &
                fake_low &
                ((strong_trend & (evolution_score >= 55)) | ((action == "TEST") & (attack >= 70)))
            ].copy()

        if not evo.empty:
            evo["_evolution_score_v318"] = evolution_score.loc[evo.index]
            # 盡量跟 ignition 分開；若候選不足才允許重疊。
            ign_ids = set(ign["stock_id"].astype(str).tolist()) if len(ign) else set()
            evo_non_overlap = evo.loc[~evo["stock_id"].astype(str).isin(ign_ids)].copy()
            if len(evo_non_overlap) >= 3:
                evo = evo_non_overlap

            evo = evo.sort_values(["_evolution_score_v318", "stock_id"], ascending=[False, True]).head(10).copy()
            evo["action"] = "WATCH"
            evo["final_action"] = "WATCH"
            evo["strategy_type"] = "EVOLUTION"
            evo["bucket"] = "EVOLUTION"
            evo["strategy_name"] = "EVOLUTION 策略進化訊號"
            evo["evolution_score"] = evo["_evolution_score_v318"].round(2)
            evo["entry_score"] = evo["_evolution_score_v318"].round(2)
            evo["score"] = evo["_evolution_score_v318"].round(2)
            evo["close"] = close.loc[evo.index].round(2)
            evo["ref_price"] = evo["close"]
            evo["evolution_phase"] = np.where(
                strong_trend.loc[evo.index] & momentum_confirm.loc[evo.index],
                "趨勢確認升級",
                "續強觀察"
            )
            evo["entry_type"] = "策略進化觀察"
            evo["section_top_opportunity"] = [f"EVOLUTION_TOP{i}" for i in range(1, len(evo) + 1)]
            evo["top_opportunity"] = [f"🧬TOP{i}" for i in range(1, len(evo) + 1)]
            evo["execution_flag"] = evo["section_top_opportunity"]
            evo["reason"] = "v318 EVOLUTION：趨勢確認 / 續強升級 / 不再複製 IGNITION。"
            evo["system_note"] = "EVOLUTION：強勢確認層，後續可銜接 CORE，但不自動加碼。"
            evo["source"] = "策略進場"

        evo_cols = [
            "stock_id","stock_name","industry","action","final_action","strategy_type","bucket",
            "strategy_name","evolution_score","entry_score","score","close","ref_price","evolution_phase",
            "entry_type","section_top_opportunity","top_opportunity","execution_flag","reason","system_note",
            "source","liquidity_level","liquidity_score","volume","turnover"
        ]
        for c in evo_cols:
            if c not in evo.columns:
                evo[c] = ""

        ign = ign[ign_cols].copy()
        evo = evo[evo_cols].copy()

    for base_path in [ROOT, DATA_DIR]:
        base_path.mkdir(parents=True, exist_ok=True)
        ign.to_csv(base_path / "ignition_candidates.csv", index=False, encoding="utf-8-sig")
        evo.to_csv(base_path / "strategy_evolution.csv", index=False, encoding="utf-8-sig")
        print("v318 wrote:", base_path / "ignition_candidates.csv", len(ign))
        print("v318 wrote:", base_path / "strategy_evolution.csv", len(evo))

    required = [
        DATA_DIR / "ignition_candidates.csv",
        DATA_DIR / "strategy_evolution.csv",
        ROOT / "ignition_candidates.csv",
        ROOT / "strategy_evolution.csv",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise RuntimeError("v318 panel files missing: " + ",".join(missing))

    for base_path in [ROOT, DATA_DIR]:
        p = base_path / "meta.json"
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v318_ignition_evolution_real_split"
                data["ignition_count"] = int(len(ign))
                data["evolution_count"] = int(len(evo))
                data["panel_logic"] = "IGNITION=起漲前夕；EVOLUTION=趨勢確認升級；兩者不再同條件複製"
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass


# ===== v319 CORE LIFECYCLE MARKER =====
# 目的：
# - 不新增 CORE 清單，不動 UI。
# - 只在既有卡片欄位顯示「🟣 CORE｜核心主升」。
# - 讓 CORE 成為 EVOLUTION 之上的生命週期層，而不是單純 engine 標籤。
def apply_v319_core_lifecycle_marker_to_outputs():
    import pandas as pd
    import numpy as np
    import json
    from pathlib import Path

    def _read_csv_safe(path):
        try:
            if path.exists() and path.stat().st_size > 0:
                return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8")
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=df.index, dtype="object")

    def _first_num(df, cols, default=0.0):
        out = pd.Series(np.nan, index=df.index, dtype="float64")
        for c in cols:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
                out = out.where(out.notna(), v)
        return out.fillna(default)

    def _mark(df):
        if df is None or df.empty or "stock_id" not in df.columns:
            return df

        d = df.copy()
        d["stock_id"] = d["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(d["stock_id"].astype(str).str[:4])

        action = _txt(d, "v311_locked_action")
        action = action.where(action.str.len() > 0, _txt(d, "action")).str.upper()

        industry = _txt(d, "industry")
        sid = _txt(d, "stock_id")
        finance = sid.str.startswith(("28", "58")) | industry.str.contains("金融|保險|金控|銀行|證券", na=False)

        close = _first_num(d, ["close", "ref_price", "price"], 0)
        ma5 = _first_num(d, ["ma5", "MA5"], close)
        ma10 = _first_num(d, ["ma10", "MA10"], close)
        ma20 = _first_num(d, ["ma20", "MA20"], close)
        ma60 = _first_num(d, ["ma60", "MA60"], close)

        mom5 = _first_num(d, ["mom5", "return_5d"], 0)
        mom10 = _first_num(d, ["mom10", "return_10d"], 0)
        mom20 = _first_num(d, ["mom20", "return_20d"], 0)

        vol_ratio = _first_num(d, ["volume_ratio", "vol_ratio"], 1)
        attack = _first_num(d, ["attack_score_v312", "attack_score_v310", "attack_score_v309", "entry_score", "score"], 0)
        final_sort = _first_num(d, ["final_sort_score_v312", "final_sort_score_v310", "final_sort_score_v309", "score", "entry_score"], 0)
        main_force = _first_num(d, ["main_force_score_v300", "main_force_score"], 0)
        chip = _first_num(d, ["chip_score"], 0)
        liq = _first_num(d, ["liquidity_score"], 0)
        hard_block = _first_num(d, ["hard_block_v313"], 0)
        hard_reject = _first_num(d, ["hard_reject_v313", "hard_reject_v312", "hard_reject_v310"], 0)

        # CORE 條件：不追求最多，只標出真正可做主升核心追蹤的標的。
        close_safe = close.replace(0, np.nan)
        ma20_safe = ma20.replace(0, np.nan)
        ma60_safe = ma60.replace(0, np.nan)

        trend_core = (
            (close >= ma20 * 1.015) &
            (ma5 >= ma10 * 1.003) &
            (ma10 >= ma20 * 1.003) &
            (ma20 >= ma60 * 0.995)
        )

        momentum_core = (
            (mom5 > 0.008) &
            (mom10 > 0.018) &
            (mom20 > 0.025)
        )

        capital_core = (
            (main_force >= 60) |
            (chip >= 60) |
            (liq >= 82)
        )

        risk_ok = (
            (~finance) &
            (hard_block < 1) &
            (hard_reject < 1) &
            ((close / ma20_safe - 1).replace([np.inf, -np.inf], 0).fillna(0) < 0.38)
        )

        core_score = (
            trend_core.astype(int) * 30 +
            momentum_core.astype(int) * 25 +
            capital_core.astype(int) * 20 +
            (vol_ratio.between(0.85, 4.50)).astype(int) * 10 +
            risk_ok.astype(int) * 10 +
            (attack / 10).clip(0, 12) +
            (final_sort / 20).clip(0, 8)
        ).round(2)

        # 只從 TEST / WATCH / panel 候選中標 CORE，不把 BLOCK 拉上來。
        candidate_ok = action.isin(["TEST", "WATCH"]) | _txt(d, "strategy_type").str.upper().isin(["IGNITION", "EVOLUTION"])
        # v321 CORE 升級：
        # CORE 不是新清單，而是從 EVOLUTION / TEST 中，把真正進入主升核心的股票升級標記。
        # 這裡放寬「一定要有 mom 欄位」的依賴，避免資料欄位缺失導致明顯主升股不升級。
        full_bull_ma = (
            (ma5 >= ma10 * 1.002) &
            (ma10 >= ma20 * 1.002) &
            (ma20 >= ma60 * 0.990) &
            (close >= ma5 * 0.985)
        )

        # v326 CORE 收窄版：
        # 不再只因為 EVOLUTION + 均線多頭就升 CORE。
        # 必須同時具備：
        # 1. 主升結構
        # 2. 動能延續
        # 3. 籌碼/主力強度
        # 4. 高流動性
        # 5. 排名靠前

        strong_trend = (
            (close >= ma5 * 0.995) &
            (ma5 >= ma10 * 1.004) &
            (ma10 >= ma20 * 1.004) &
            (ma20 >= ma60 * 1.002)
        )

        strong_momentum = (
            (mom5 >= 0.03) &
            (mom10 >= 0.06) &
            (mom20 >= 0.10)
        )

        strong_chip = (
            (main_force >= 68) |
            (chip >= 72)
        )

        strong_liq = (
            (liq >= 90) &
            (vol_ratio.between(0.90, 3.80))
        )

        strong_rank = (
            (attack >= 70) |
            (final_sort >= 75)
        )

        evolution_mask = _txt(d, "strategy_type").str.upper().eq("EVOLUTION")
        strong_core_pair = (strong_momentum & strong_chip) | (strong_momentum & strong_rank)

        core_from_evolution = evolution_mask & strong_trend & strong_momentum & strong_chip & strong_liq & strong_rank & risk_ok
        core_from_score = candidate_ok & risk_ok & strong_trend & strong_liq & (core_score >= 82) & strong_core_pair

        core_mask = core_from_score | core_from_evolution

        # fallback 不再強制補滿 3 檔，避免整批被升級成 CORE。
        # 現在寧可少，也不要失真。

        # 控制 CORE 數量，避免變成另一張大清單。
        core_idx = d.loc[core_mask].assign(_core_score_v319=core_score.loc[core_mask]).sort_values(
            ["_core_score_v319", "stock_id"], ascending=[False, True]
        ).head(5).index
        final_core = d.index.isin(core_idx)

        if "lifecycle_stage" not in d.columns:
            d["lifecycle_stage"] = ""
        # v319.1：輸出欄位統一寫成字串，避免 pandas StringDtype 欄位被塞數值 Series 後炸掉。
        d["core_score_v319"] = core_score.round(2).astype(str)
        d["is_core_v319"] = "0"
        d.loc[final_core, "is_core_v319"] = "1"

        # 不新增 UI，只把既有卡片欄位變成特殊文字標記。
        # v327.1 dtype safe：這些欄位後面都會寫中文/emoji，先強制轉 object，避免原欄位是 float64 時炸掉。
        for c in ["lifecycle_stage", "strategy_layer", "strategy_bucket", "strategy_type", "bucket", "entry_type", "system_note", "reason", "source"]:
            if c not in d.columns:
                d[c] = ""
            d[c] = d[c].astype("object").where(d[c].notna(), "")

        d.loc[final_core, "lifecycle_stage"] = "🟣 CORE"
        d.loc[final_core, "strategy_layer"] = "🟣 CORE｜核心主升"
        d.loc[final_core, "strategy_bucket"] = "🟣 CORE｜主升核心"
        d.loc[final_core, "bucket"] = "CORE"
        d.loc[final_core, "entry_type"] = "核心主升追蹤"
        d.loc[final_core, "system_note"] = "🟣 CORE：由 EVOLUTION/TEST 升級的核心主升追蹤，不是一般試單。"
        d.loc[final_core, "reason"] = "v319 CORE：趨勢、動能、籌碼/流動性達核心條件，標記為核心主升。"

        # v321：CORE 必須在卡片策略層直接看得見。
        # 不改 action，所以仍會留在原本 TEST/EVOLUTION 清單；但卡片策略層會升級成 CORE。
        d.loc[final_core, "strategy_type"] = "CORE"
        d.loc[final_core, "bucket"] = "CORE"

        # v320：不要把卡片「來源」顯示成版本技術字串。
        # UI 會把 source 欄位直接顯示在卡片裡，所以這裡統一轉成使用者看得懂的中文。
        src_text = _txt(d, "source")
        technical_source = src_text.str.contains(r"^v\d+|core_lifecycle|fallback|real_split|panel_file", case=False, regex=True, na=False)
        d.loc[technical_source, "source"] = "策略進場"
        d.loc[final_core, "source"] = "核心主升"
        return d

    target_names = [
        "trade_plan.csv",
        "candidates.csv",
        "core_candidates.csv",
        "alpha_candidates.csv",
        "ignition_candidates.csv",
        "strategy_evolution.csv",
    ]

    for name in target_names:
        # 優先讀 root，沒有再讀 data
        df = _read_csv_safe(ROOT / name)
        if df.empty:
            df = _read_csv_safe(DATA_DIR / name)
        if df.empty:
            continue

        out = _mark(df)
        for base in [ROOT, DATA_DIR]:
            base.mkdir(parents=True, exist_ok=True)
            out.to_csv(base / name, index=False, encoding="utf-8-sig")
        print("v319 core lifecycle marked:", name, "rows=", len(out), "core=", int(pd.to_numeric(out.get("is_core_v319", 0), errors="coerce").fillna(0).sum()))

    for base in [ROOT, DATA_DIR]:
        p = base / "meta.json"
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v329_evolution_ab_auto_split"
                data["core_marker"] = "🟣 CORE｜核心主升"
                data["core_logic"] = "CORE 不新增清單；從 EVOLUTION/TEST 升級，直接寫入 strategy_type / strategy_layer / lifecycle_stage 作特殊標記"
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass



# ===== v327 LIFECYCLE LIST PLANNING GUARD =====
# 目的：
# - 不新增清單、不改前端、不改 yml。
# - 只把既有五大清單的角色定義寫乾淨：
#   IGNITION = 起漲點火
#   TEST     = ATTACK 新強勢攻擊池
#   EVOLUTION= 趨勢確認升級池
#   CORE     = 少數核心主升標記，不是獨立清單
#   WATCH    = 預備觀察
#   BLOCK    = 禁止
# - 同時限制 CORE 數量，避免整張 EVOLUTION / TEST 都變紫框。
def apply_v327_lifecycle_list_planning_guard():
    import pandas as pd
    import numpy as np
    import json

    def _read_csv_safe(path):
        try:
            if path.exists() and path.stat().st_size > 0:
                return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8")
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype(str).replace("nan", "").fillna(default)
        return pd.Series(default, index=df.index, dtype="object")

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _first_num(df, cols, default=0.0):
        out = pd.Series(np.nan, index=df.index, dtype="float64")
        for c in cols:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
                out = out.where(out.notna(), v)
        return out.fillna(default)

    def _normalize_sid(df):
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock_id"].astype(str).str[:4])
        return df

    def _base_action(df):
        action = _txt(df, "v311_locked_action")
        action = action.where(action.str.len() > 0, _txt(df, "final_action"))
        action = action.where(action.str.len() > 0, _txt(df, "action"))
        return action.str.upper().str.strip()

    def _core_flag(df):
        joined = (
            _txt(df, "strategy_type") + " " +
            _txt(df, "strategy_layer") + " " +
            _txt(df, "strategy_bucket") + " " +
            _txt(df, "bucket") + " " +
            _txt(df, "lifecycle_stage") + " " +
            _txt(df, "system_note") + " " +
            _txt(df, "reason")
        ).str.upper()
        raw_core = (
            _txt(df, "is_core_v319").str.strip().eq("1") |
            joined.str.contains("CORE|核心主升|🟣", case=False, regex=True, na=False)
        )
        return raw_core

    def _rank_score(df):
        return (
            _first_num(df, ["core_score_v319"], 0) * 1.00 +
            _first_num(df, ["final_sort_score_v312", "final_sort_score_v310", "final_sort_score_v309"], 0) * 0.45 +
            _first_num(df, ["attack_score_v312", "attack_score_v310", "attack_score_v309"], 0) * 0.35 +
            _first_num(df, ["evolution_score", "entry_score", "score"], 0) * 0.20 +
            _first_num(df, ["liquidity_score"], 0) * 0.10
        ).round(2)

    def _apply_role(df, name):
        if df is None or df.empty or "stock_id" not in df.columns:
            return df

        d = _normalize_sid(df.copy())
        action = _base_action(d)

        # v327.1 dtype safe：
        # 這些欄位會被寫入中文、emoji、角色文字。
        # 如果原 CSV 欄位全空，pandas 可能讀成 float64；直接 d.loc 寫字串會炸。
        for c in [
            "action", "final_action", "strategy_type", "bucket", "strategy_layer",
            "strategy_bucket", "lifecycle_stage", "entry_type", "source",
            "system_note", "reason", "is_core_v319", "core_score_v319",
            "action_label", "action_sub", "v327_lifecycle_role"
        ]:
            if c not in d.columns:
                d[c] = ""
            d[c] = d[c].astype("object").where(d[c].notna(), "")

        # 先保留既有 CORE 候選，再做名額制，避免整批紫框。
        raw_core = _core_flag(d)
        rank = _rank_score(d)

        # 各輸出檔獨立控管 CORE，不讓 panel / trade_plan 互相污染。
        # panel 顯示用，最多 5；一般輸出最多 5。沒有符合就 0，不強制補。
        max_core = 5
        keep_core = pd.Series(False, index=d.index)
        if bool(raw_core.any()):
            idx = (
                d.loc[raw_core]
                .assign(_v327_rank=rank.loc[raw_core])
                .sort_values(["_v327_rank", "stock_id"], ascending=[False, True])
                .head(max_core)
                .index
            )
            keep_core.loc[idx] = True

        # 先清掉過度擴散的 CORE 痕跡，後面只把 keep_core 補回來。
        d["is_core_v319"] = "0"
        d["lifecycle_stage"] = ""
        d["strategy_layer"] = d["strategy_layer"].astype("object").astype(str).replace("nan", "")
        d["strategy_bucket"] = d["strategy_bucket"].astype("object").astype(str).replace("nan", "")

        # 檔案面板預設角色
        file_is_ign = name == "ignition_candidates.csv"
        file_is_evo = name == "strategy_evolution.csv"

        test_mask = action.eq("TEST") | _txt(d, "action").str.upper().eq("TEST")
        watch_mask = action.eq("WATCH") | _txt(d, "action").str.upper().eq("WATCH")
        block_mask = action.eq("BLOCK") | _txt(d, "action").str.upper().eq("BLOCK")
        ignition_mask = file_is_ign | _txt(d, "strategy_type").str.upper().eq("IGNITION")
        evolution_mask = file_is_evo | _txt(d, "strategy_type").str.upper().eq("EVOLUTION")

        # IGNITION：點火掃描，不直接等於主倉。
        m = ignition_mask & ~keep_core
        d.loc[m, "strategy_type"] = "IGNITION"
        d.loc[m, "bucket"] = "IGNITION"
        d.loc[m, "strategy_layer"] = "🧪 IGNITION｜起漲點火"
        d.loc[m, "strategy_bucket"] = "🧪 IGNITION｜防假突破"
        d.loc[m, "entry_type"] = "起漲訊號觀察"
        d.loc[m, "source"] = "起漲偵測"
        d.loc[m, "system_note"] = "IGNITION：剛點火，先觀察假突破，不直接重倉。"
        d.loc[m, "reason"] = "v327：起漲訊號層，等待延續 K 棒與量價確認。"

        # EVOLUTION：趨勢確認升級池，仍不是自動主倉。
        m = evolution_mask & ~keep_core
        d.loc[m, "strategy_type"] = "EVOLUTION"
        d.loc[m, "bucket"] = "EVOLUTION"
        d.loc[m, "strategy_layer"] = "🧬 EVOLUTION｜趨勢確認"
        d.loc[m, "strategy_bucket"] = "🧬 EVOLUTION｜策略進化"
        d.loc[m, "entry_type"] = "趨勢確認升級"
        d.loc[m, "source"] = "策略進場"
        d.loc[m, "system_note"] = "EVOLUTION：趨勢確認層，可銜接 CORE，但不自動加碼。"
        d.loc[m, "reason"] = "v327：強勢確認 / 續強升級 / 觀察是否進入核心主升。"

        # TEST 正式定位成 ATTACK：新強勢攻擊池，避免新名單被 CORE 名額制擋掉。
        m = test_mask & ~keep_core & ~ignition_mask & ~evolution_mask
        d.loc[m, "strategy_type"] = "TEST"
        d.loc[m, "bucket"] = "ATTACK"
        d.loc[m, "strategy_layer"] = "⚡ ATTACK｜新強勢攻擊"
        d.loc[m, "strategy_bucket"] = "⚡ ATTACK｜準主流候選"
        d.loc[m, "entry_type"] = "攻擊試單"
        d.loc[m, "source"] = "攻擊試單"
        d.loc[m, "action_label"] = "試單"
        d.loc[m, "action_sub"] = "ATTACK：新強勢攻擊池，小倉試單，觀察是否續強升 CORE。"
        d.loc[m, "system_note"] = "TEST/ATTACK：避免錯過新主流，但尚未確認為主倉。"
        d.loc[m, "reason"] = "v327：攻擊條件達標，先列入 ATTACK 試單層，不直接視為 CORE。"

        # WATCH：預備池。
        m = watch_mask & ~keep_core & ~ignition_mask & ~evolution_mask
        d.loc[m, "strategy_type"] = "WATCH"
        d.loc[m, "bucket"] = "WATCH"
        d.loc[m, "strategy_layer"] = "👀 WATCH｜預備觀察"
        d.loc[m, "strategy_bucket"] = "👀 WATCH｜未發動"
        d.loc[m, "entry_type"] = "觀察等待"
        d.loc[m, "source"] = "觀察池"
        d.loc[m, "action_label"] = "觀察"
        d.loc[m, "action_sub"] = "WATCH：有結構但未發動，等確認。"

        # BLOCK：禁止池。
        m = block_mask & ~keep_core
        d.loc[m, "strategy_type"] = "BLOCK"
        d.loc[m, "bucket"] = "BLOCK"
        d.loc[m, "strategy_layer"] = "⛔ BLOCK｜禁止"
        d.loc[m, "strategy_bucket"] = "⛔ BLOCK｜風控排除"
        d.loc[m, "entry_type"] = "禁止進場"
        d.loc[m, "source"] = "風控排除"
        d.loc[m, "action_label"] = "禁止"
        d.loc[m, "action_sub"] = "BLOCK：禁止清單，避免重複踩雷。"

        # CORE：少數核心主升標記。保留在原清單，不新增獨立清單。
        d.loc[keep_core, "is_core_v319"] = "1"
        d.loc[keep_core, "lifecycle_stage"] = "🟣 CORE"
        d.loc[keep_core, "strategy_type"] = "CORE"
        d.loc[keep_core, "bucket"] = "CORE"
        d.loc[keep_core, "strategy_layer"] = "🟣 CORE｜核心主升"
        d.loc[keep_core, "strategy_bucket"] = "🟣 CORE｜主升核心"
        d.loc[keep_core, "entry_type"] = "核心主升追蹤"
        d.loc[keep_core, "source"] = "核心主升"
        d.loc[keep_core, "system_note"] = "CORE：由 IGNITION / TEST / EVOLUTION 中少數升級，主倉候選，不是新清單。"
        d.loc[keep_core, "reason"] = "v327：通過 CORE 名額制，列為核心主升追蹤。"

        d["v327_lifecycle_role"] = np.select(
            [keep_core, ignition_mask & ~keep_core, evolution_mask & ~keep_core, test_mask & ~keep_core, watch_mask & ~keep_core, block_mask & ~keep_core],
            ["CORE", "IGNITION", "EVOLUTION", "ATTACK", "WATCH", "BLOCK"],
            default=""
        )

        d["core_score_v319"] = _first_num(d, ["core_score_v319"], 0).round(2).astype(str)
        return d

    target_names = [
        "trade_plan.csv",
        "candidates.csv",
        "core_candidates.csv",
        "alpha_candidates.csv",
        "ignition_candidates.csv",
        "strategy_evolution.csv",
        "watchlist_monitor.csv",
        "position_monitor.csv",
    ]

    for name in target_names:
        df = _read_csv_safe(ROOT / name)
        if df.empty:
            df = _read_csv_safe(DATA_DIR / name)
        if df.empty:
            continue

        out = _apply_role(df, name)
        for base in [ROOT, DATA_DIR]:
            base.mkdir(parents=True, exist_ok=True)
            out.to_csv(base / name, index=False, encoding="utf-8-sig")

        try:
            core_n = int(pd.to_numeric(out.get("is_core_v319", 0), errors="coerce").fillna(0).sum())
        except Exception:
            core_n = 0
        print("v327 lifecycle planning:", name, "rows=", len(out), "core=", core_n)

    for base in [ROOT, DATA_DIR]:
        p = base / "meta.json"
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v329_evolution_ab_auto_split"
                data["list_logic"] = "IGNITION=點火；TEST=ATTACK攻擊池；EVOLUTION=趨勢確認；CORE=少數主升標記；WATCH=預備；BLOCK=禁止"
                data["core_limit"] = "每個輸出檔最多 5 檔 CORE，不強制補滿"
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass



# ===== v328 PRIORITY OPERATION POOL =====
# 目的：
# - 不改前端、不改 yml、不新增獨立資料來源。
# - 把已經被紫框標記的名單，正式匯總成「主升操作池」。
# - 讓「最終操作」不再空白：從紫框名單中依強弱排序，分配 TOP 操作。
# - 非紫框仍保留在 TEST / EVOLUTION / WATCH / BLOCK，不會被硬拉進最終操作。
def apply_v328_priority_operation_pool():
    import pandas as pd
    import numpy as np
    import json

    def _read_csv_safe(path):
        try:
            if path.exists() and path.stat().st_size > 0:
                return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8")
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype("object").where(df[col].notna(), default).astype(str).replace("nan", "")
        return pd.Series(default, index=df.index, dtype="object")

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _first_num(df, cols, default=0.0):
        out = pd.Series(np.nan, index=df.index, dtype="float64")
        for c in cols:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
                out = out.where(out.notna(), v)
        return out.fillna(default)

    def _normalize_sid(df):
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock_id"].astype(str).str[:4])
        return df

    def _ensure_text_columns(df):
        text_cols = [
            "stock_id", "stock_name", "industry", "action", "final_action",
            "v311_locked_action", "action_label", "action_sub", "strategy_type", "bucket",
            "strategy_layer", "strategy_bucket", "layer", "entry_type",
            "source", "reason", "system_note", "note", "engine",
            "is_core_v319", "core_score_v319", "priority_grade_v328",
            "priority_rank_v328", "v327_lifecycle_role",
            "opportunity_rank", "top_opportunity", "section_top_opportunity",
            "top_reason", "final_decision", "decision_note"
        ]
        for c in text_cols:
            if c not in df.columns:
                df[c] = ""
            df[c] = df[c].astype("object").where(df[c].notna(), "")

        numeric_cols = [
            "top_rank_v3066", "is_top_v3066", "target_weight",
            "suggest_amount", "suggest_shares", "price", "ref_price"
        ]
        for c in numeric_cols:
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)

        return df

    def _rank_score(df):
        return (
            _first_num(df, ["core_score_v319"], 0) * 1.00 +
            _first_num(df, ["final_sort_score_v312", "final_sort_score_v310", "final_sort_score_v309", "score"], 0) * 0.55 +
            _first_num(df, ["attack_score_v312", "attack_score_v310", "attack_score_v309"], 0) * 0.45 +
            _first_num(df, ["final_attack_score_v312", "final_attack_score_v310", "final_attack_score_v309"], 0) * 0.25 +
            _first_num(df, ["entry_score", "evolution_score"], 0) * 0.20 +
            _first_num(df, ["liquidity_score"], 0) * 0.12
        ).round(2)

    def _core_like(df):
        joined = (
            _txt(df, "strategy_type") + " " +
            _txt(df, "strategy_layer") + " " +
            _txt(df, "strategy_bucket") + " " +
            _txt(df, "bucket") + " " +
            _txt(df, "lifecycle_stage") + " " +
            _txt(df, "system_note") + " " +
            _txt(df, "reason")
        ).str.upper()
        return (
            _txt(df, "is_core_v319").str.strip().eq("1") |
            joined.str.contains("CORE|核心主升|🟣", case=False, regex=True, na=False)
        )

    source_names = [
        "trade_plan.csv",
        "strategy_evolution.csv",
        "ignition_candidates.csv",
        "candidates.csv",
        "core_candidates.csv",
        "alpha_candidates.csv",
    ]

    frames = []
    for name in source_names:
        df = _read_csv_safe(ROOT / name)
        if df.empty:
            df = _read_csv_safe(DATA_DIR / name)
        if df.empty or "stock_id" not in df.columns:
            continue
        df = _normalize_sid(_ensure_text_columns(df.copy()))
        df["_v328_from_file"] = name
        frames.append(df)

    if not frames:
        print("v328 priority operation pool: no source rows")
        return

    pool = pd.concat(frames, ignore_index=True)
    pool = _normalize_sid(_ensure_text_columns(pool))

    core_mask = _core_like(pool)
    if not bool(core_mask.any()):
        print("v328 priority operation pool: no purple/core rows")
        return

    purple = pool.loc[core_mask].copy()
    purple["_priority_score_v328"] = _rank_score(purple)
    purple["_action_sort_v328"] = np.select(
        [
            _txt(purple, "action").str.upper().eq("TEST"),
            _txt(purple, "action").str.upper().eq("BUY"),
            _txt(purple, "action").str.upper().eq("WATCH")
        ],
        [0, 1, 2],
        default=3
    )
    purple["_file_sort_v328"] = np.select(
        [
            _txt(purple, "_v328_from_file").eq("trade_plan.csv"),
            _txt(purple, "_v328_from_file").eq("strategy_evolution.csv"),
            _txt(purple, "_v328_from_file").eq("ignition_candidates.csv")
        ],
        [0, 1, 2],
        default=3
    )

    purple = (
        purple.sort_values(
            ["_priority_score_v328", "_action_sort_v328", "_file_sort_v328", "stock_id"],
            ascending=[False, True, True, True]
        )
        .drop_duplicates("stock_id", keep="first")
        .head(8)
        .copy()
    )

    if purple.empty:
        print("v328 priority operation pool: purple rows empty after dedupe")
        return

    # TOP 分配：
    # TOP 1-3 = S 主攻
    # TOP 4-6 = A 可操作
    # TOP 7-8 = B 追蹤
    for rank, idx in enumerate(purple.index, start=1):
        if rank <= 3:
            grade = "S"
            amount = 20000
            weight = 0.010
            sub = f"🟣 PRIORITY S{rank}｜主升主攻：紫框強度排序 TOP{rank}，可優先評估小倉/主倉。"
        elif rank <= 6:
            grade = "A"
            amount = 10000
            weight = 0.005
            sub = f"🟣 PRIORITY A{rank}｜主升候選：紫框強度排序 TOP{rank}，可小倉試單或等回檔。"
        else:
            grade = "B"
            amount = 0
            weight = 0.000
            sub = f"🟣 PRIORITY B{rank}｜主升觀察：紫框強度排序 TOP{rank}，先觀察不重倉。"

        px = float(pd.to_numeric(pd.Series([purple.at[idx, "price"] if "price" in purple.columns else purple.at[idx, "ref_price"] if "ref_price" in purple.columns else purple.at[idx, "close"] if "close" in purple.columns else 0]), errors="coerce").fillna(0).iloc[0])
        ref_px = float(pd.to_numeric(pd.Series([purple.at[idx, "ref_price"] if "ref_price" in purple.columns else purple.at[idx, "close"] if "close" in purple.columns else px]), errors="coerce").fillna(px).iloc[0])
        use_px = px if px > 0 else ref_px

        purple.at[idx, "action"] = "BUY" if rank <= 6 else "WATCH"
        purple.at[idx, "final_action"] = purple.at[idx, "action"]
        purple.at[idx, "v311_locked_action"] = purple.at[idx, "action"]
        purple.at[idx, "action_label"] = "主升" if rank <= 6 else "觀察"
        purple.at[idx, "action_sub"] = sub
        purple.at[idx, "strategy_type"] = "CORE"
        purple.at[idx, "bucket"] = "PRIORITY"
        purple.at[idx, "engine"] = "CORE"
        purple.at[idx, "strategy_layer"] = f"🟣 PRIORITY｜主升操作池 {grade}"
        purple.at[idx, "strategy_bucket"] = f"🟣 CORE｜主升候選 TOP{rank}"
        purple.at[idx, "layer"] = purple.at[idx, "strategy_layer"]
        purple.at[idx, "entry_type"] = "主升候選操作"
        purple.at[idx, "source"] = "v328_priority_operation_pool"
        purple.at[idx, "reason"] = sub
        purple.at[idx, "system_note"] = sub
        purple.at[idx, "priority_grade_v328"] = grade
        purple.at[idx, "priority_rank_v328"] = str(rank)
        purple.at[idx, "opportunity_rank"] = str(rank)
        purple.at[idx, "top_rank_v3066"] = rank
        purple.at[idx, "is_top_v3066"] = 1
        purple.at[idx, "top_opportunity"] = f"🔥PRIORITY_TOP{rank}"
        purple.at[idx, "section_top_opportunity"] = f"PRIORITY_TOP{rank}"
        purple.at[idx, "top_reason"] = sub
        purple.at[idx, "is_core_v319"] = "1"
        purple.at[idx, "core_score_v319"] = str(float(purple.at[idx, "_priority_score_v328"]))

        if use_px > 0:
            purple.at[idx, "price"] = round(use_px, 2)
            purple.at[idx, "ref_price"] = round(ref_px if ref_px > 0 else use_px, 2)
            purple.at[idx, "target_weight"] = round(weight, 4)
            purple.at[idx, "suggest_amount"] = amount
            purple.at[idx, "suggest_shares"] = round(amount / use_px, 0) if amount > 0 else 0
        else:
            purple.at[idx, "target_weight"] = round(weight, 4)
            purple.at[idx, "suggest_amount"] = amount
            purple.at[idx, "suggest_shares"] = 0

    # 讀原 trade_plan，把 priority rows 放最前面，其他原本 TEST/WATCH/BLOCK 保留。
    base = _read_csv_safe(ROOT / "trade_plan.csv")
    if base.empty:
        base = _read_csv_safe(DATA_DIR / "trade_plan.csv")
    if base.empty:
        base = purple.copy()
    else:
        base = _normalize_sid(_ensure_text_columns(base.copy()))
        base = base.loc[~base["stock_id"].astype(str).isin(set(purple["stock_id"].astype(str)))].copy()
        base = pd.concat([purple, base], ignore_index=True)

    # 排序：BUY 主升在最前，WATCH/BLOCK 照原本在後。
    base["_v328_action_order"] = np.select(
        [
            _txt(base, "action").str.upper().eq("BUY"),
            _txt(base, "action").str.upper().eq("TEST"),
            _txt(base, "action").str.upper().eq("WATCH"),
            _txt(base, "action").str.upper().eq("BLOCK"),
        ],
        [0, 1, 2, 9],
        default=5
    )
    base["_v328_rank_num"] = pd.to_numeric(base.get("priority_rank_v328", 9999), errors="coerce").fillna(9999)
    base["_v328_score"] = _rank_score(base)
    base = base.sort_values(
        ["_v328_action_order", "_v328_rank_num", "_v328_score", "stock_id"],
        ascending=[True, True, False, True]
    ).drop(columns=[c for c in ["_v328_action_order", "_v328_rank_num", "_v328_score", "_priority_score_v328", "_action_sort_v328", "_file_sort_v328", "_v328_from_file"] if c in base.columns], errors="ignore")

    for base_dir in [ROOT, DATA_DIR]:
        base_dir.mkdir(parents=True, exist_ok=True)
        base.to_csv(base_dir / "trade_plan.csv", index=False, encoding="utf-8-sig")

    # 額外輸出一份主升操作池 CSV，前端未吃也沒關係，方便你人工檢查。
    priority_out = purple.drop(columns=[c for c in ["_priority_score_v328", "_action_sort_v328", "_file_sort_v328", "_v328_from_file"] if c in purple.columns], errors="ignore")
    for base_dir in [ROOT, DATA_DIR]:
        priority_out.to_csv(base_dir / "priority_operation_pool.csv", index=False, encoding="utf-8-sig")

    for base_dir in [ROOT, DATA_DIR]:
        p = base_dir / "meta.json"
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8-sig"))
                data["source"] = "v329_evolution_ab_auto_split"
                data["final_operation_name"] = "🟣 PRIORITY 主升操作池"
                data["priority_logic"] = "從所有紫框名單匯總，依強弱排序 TOP1-8；TOP1-3=S，TOP4-6=A，TOP7-8=B"
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")
            except Exception:
                pass

    print("v328 priority operation pool:", "rows=", len(priority_out), "buy=", int((_txt(priority_out, "action").str.upper() == "BUY").sum()))



# ===== v329 EVOLUTION A/B AUTO SPLIT =====
# 目的：
# - 不新增 UI、不改 yml、不動 app.js。
# - 只把既有 strategy_evolution.csv 內部分成：
#   EVOLUTION-A = 準主升，可進最終操作候選
#   EVOLUTION-B = 培養觀察，留在 EVOLUTION，不進最終操作
# - 每次後端更新都會重新評分，所以 B 變強會自動升 A，A 變弱會退 B。
def apply_v329_evolution_ab_auto_split():
    import pandas as pd
    import numpy as np
    import json
    import math

    def _read_csv_safe(path):
        try:
            if path.exists() and path.stat().st_size > 0:
                return pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(path, encoding="utf-8")
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def _txt(df, col, default=""):
        if col in df.columns:
            return df[col].astype("object").where(df[col].notna(), default).astype(str).replace("nan", "")
        return pd.Series(default, index=df.index, dtype="object")

    def _num(df, col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=df.index, dtype="float64")

    def _first_num(df, cols, default=0.0):
        out = pd.Series(np.nan, index=df.index, dtype="float64")
        for c in cols:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
                out = out.where(out.notna(), v)
        return out.fillna(default)

    def _normalize_sid(df):
        if "stock_id" in df.columns:
            df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock
