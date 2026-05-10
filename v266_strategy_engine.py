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




def apply_ignition_rank_v26670(d):
    """
    v266.70:
    主力點火排序層
    不改原策略，只補：
    - 第一段發動優先
    - 壓縮後首次轉強
    - 排除第二段爆噴
    """
    d = d.copy()

    ignition_rank = pd.Series(0.0, index=d.index)

    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    ma20 = _clip_series(d.get("ma20", 0))
    close = _clip_series(d.get("close", 0))
    ma_conv = _clip_series(d.get("ma_converge_pct", 1))
    low_hold = _clip_series(d.get("low_non_down_count_5", 0))
    mom5 = _clip_series(d.get("mom5", 0))
    mom10 = _clip_series(d.get("mom10", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    # 第一根轉強
    ignition_rank += (
        (mom5 > 0.02)
        & (mom10 > 0)
        & (close > ma20)
    ).astype(int) * 25

    # 量縮後首次放量
    ignition_rank += (
        vol_ratio.between(1.2, 2.2)
    ).astype(int) * 20

    # 波動壓縮
    ignition_rank += (
        (ma_conv <= 0.10)
        & (low_hold >= 3)
    ).astype(int) * 15

    # MA20 剛翻揚
    ignition_rank += (
        (mom20 > -0.02)
        & (mom20 < 0.18)
    ).astype(int) * 10

    # 排除已經噴太多
    ignition_rank -= (mom20 > 0.25).astype(int) * 25
    ignition_rank -= (vol_ratio > 3.8).astype(int) * 20

    d["ignition_rank_v26670"] = ignition_rank

    d["entry_score"] += ignition_rank

    return d




def apply_master_trigger_v26670(d):
    """
    v266.70 master trigger patch
    只補：主力收斂、低量壓縮、第一根轉強K、假突破排除、master_trigger_score。
    不改 pipeline / UI / 輸出檔名 / 原始欄位結構。
    """
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    low = _clip_series(d.get("low", close))
    open_ = _clip_series(d.get("open", close))
    volume = _clip_series(d.get("volume", 0))
    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))
    ma20 = _clip_series(d.get("ma20", close))
    mom20 = _clip_series(d.get("mom20", 0))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    chip = _clip_series(d.get("chip_concentration_score", d.get("chip_score", 0)))
    ignition_rank = _clip_series(d.get("ignition_rank_v26670", 0))

    # 1) 波動壓縮：K棒振幅小、均線靠近、靠近 MA20
    range_pct = ((high - low) / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0)
    ma_gap = ((ma5 - ma20).abs() / ma20.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(9)

    range_compress = (
        (range_pct <= 0.055) |
        (ma_gap <= 0.055) |
        ((close > ma20 * 0.96) & (close < ma20 * 1.07))
    )

    # 2) 低量壓縮後溫和放量：不要爆量追高
    low_volume_ready = (
        (vol_ratio >= 0.65) &
        (vol_ratio <= 2.20)
    )

    first_trigger_k = (
        (close > open_) &
        (close >= ma5 * 0.98) &
        (close >= ma10 * 0.97) &
        (vol_ratio >= 1.05) &
        (vol_ratio <= 2.60)
    )

    # 3) 主力籌碼區間：偏好 20~65，過高當成後段
    chip_collect = (
        (chip >= 20) &
        (chip <= 65)
    )

    # 4) 假突破/第二段排除
    upper_shadow_ratio = ((high - close) / (high - low + 0.001)).replace([np.inf, -np.inf], np.nan).fillna(0)
    fake_breakout = (
        (upper_shadow_ratio > 0.55) |
        (vol_ratio > 3.50) |
        (mom20 > 0.28) |
        (close > ma20 * 1.15)
    )

    master_score = (
        range_compress.astype(int) * 20 +
        low_volume_ready.astype(int) * 15 +
        first_trigger_k.astype(int) * 30 +
        chip_collect.astype(int) * 20 +
        ((close > ma20) & (ma5 >= ma10)).astype(int) * 15 +
        ignition_rank.clip(lower=-40, upper=50) * 0.35 -
        fake_breakout.astype(int) * 35
    )

    d["range_compress_v26670"] = range_compress
    d["low_volume_ready_v26670"] = low_volume_ready
    d["first_trigger_k_v26670"] = first_trigger_k
    d["chip_collect_v26670"] = chip_collect
    d["fake_breakout_v26670"] = fake_breakout
    d["master_trigger_score_v26670"] = master_score.round(2)

    phase = pd.Series("WATCH", index=d.index, dtype=object)
    phase.loc[(master_score >= 58) & (~fake_breakout)] = "MASTER_TRIGGER"
    phase.loc[(master_score >= 48) & (master_score < 58) & (~fake_breakout)] = "PRE_MASTER"
    phase.loc[fake_breakout] = "FAKE_BREAKOUT_RISK"
    d["master_trigger_phase_v26670"] = phase

    d["master_trigger_reason_v26670"] = ""
    d.loc[range_compress, "master_trigger_reason_v26670"] += "波動壓縮｜"
    d.loc[low_volume_ready, "master_trigger_reason_v26670"] += "低量溫和｜"
    d.loc[first_trigger_k, "master_trigger_reason_v26670"] += "第一根轉強K｜"
    d.loc[chip_collect, "master_trigger_reason_v26670"] += "籌碼20-65佈局｜"
    d.loc[fake_breakout, "master_trigger_reason_v26670"] += "假突破/追高風險｜"
    d["master_trigger_reason_v26670"] = d["master_trigger_reason_v26670"].str.rstrip("｜")

    # =========================================================
    # v266.71 MASTER CONTROL PATCH
    # =========================================================

    d["chip_keep_v26671"] = (
        _clip_series(
            d.get(
                "chip_concentration_score",
                d.get("chip_score", 0)
            )
        ).rolling(5).mean() >= 28
    )

    d["sideway_range_v26671"] = (
        (
            (
                _clip_series(d["high"]).rolling(10).max() -
                _clip_series(d["low"]).rolling(10).min()
            ) /
            _clip_series(d["close"]).replace(0, np.nan)
        ) <= 0.18
    )

    vol_ratio_v26671 = _clip_series(
        d.get("volume_ratio", 1)
    )

    d["control_style_v26671"] = (
        (vol_ratio_v26671 >= 0.85) &
        (vol_ratio_v26671 <= 2.20) &
        (
            _clip_series(d["close"]) >=
            _clip_series(d["ma20"]) * 0.97
        )
    )

    upper_shadow_v26671 = (
        (
            _clip_series(d["high"]) -
            _clip_series(d["close"])
        ) /
        (
            (
                _clip_series(d["high"]) -
                _clip_series(d["low"])
            ) + 0.001
        )
    )

    d["hard_fake_breakout_v26671"] = (
        (upper_shadow_v26671 > 0.60) |
        (vol_ratio_v26671 > 4.20) |
        (
            _clip_series(d["close"]) >
            _clip_series(d["ma20"]) * 1.18
        )
    )

    v26671_boost = (
        d["chip_keep_v26671"].astype(int) * 12 +
        d["sideway_range_v26671"].astype(int) * 15 +
        d["control_style_v26671"].astype(int) * 18 -
        d["hard_fake_breakout_v26671"].astype(int) * 35
    )

    d["master_trigger_score_v26670"] = (
        _clip_series(
            d["master_trigger_score_v26670"]
        ) + v26671_boost
    ).round(2)

    phase_v26671 = pd.Series(
        "WATCH",
        index=d.index,
        dtype=object
    )

    phase_v26671.loc[
        (
            d["master_trigger_score_v26670"] >= 60
        ) &
        (
            ~d["hard_fake_breakout_v26671"]
        )
    ] = "MASTER_TRIGGER"

    phase_v26671.loc[
        (
            d["master_trigger_score_v26670"] >= 50
        ) &
        (
            d["master_trigger_score_v26670"] < 60
        ) &
        (
            ~d["hard_fake_breakout_v26671"]
        )
    ] = "PRE_MASTER"

    phase_v26671.loc[
        d["hard_fake_breakout_v26671"]
    ] = "FAKE_BREAKOUT_RISK"

    d["master_trigger_phase_v26670"] = phase_v26671



    # 只在 entry_score 存在時做補分，不破壞其他欄位
    if "entry_score" in d.columns:
        d["entry_score"] = _clip_series(d["entry_score"]) + master_score.clip(lower=-25, upper=45)

    # 如果有 action/watch_mode，僅用 master phase 做輔助分層
    if "watch_mode" in d.columns:
        d.loc[d["master_trigger_phase_v26670"].isin(["MASTER_TRIGGER", "PRE_MASTER"]), "watch_mode"] = "TEST"
        d.loc[d["master_trigger_phase_v26670"].eq("FAKE_BREAKOUT_RISK"), "watch_mode"] = "WATCH"

    return d



def apply_time_structure_patch_v26672(d):
    """
    v266.72 TIME STRUCTURE PATCH
    只補：
    - 壓縮天數
    - 量縮後第一次放量
    - 連續小紅K
    - 假突破記憶
    - 主力醞釀時間結構分數
    不動 UI / pipeline / output / 原始欄位結構。
    """
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    low = _clip_series(d.get("low", close))
    open_ = _clip_series(d.get("open", close))
    volume = _clip_series(d.get("volume", 0))
    ma20 = _clip_series(d.get("ma20", close))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))

    # 1) 壓縮天數：若沒有逐日資料，就用當前橫盤/振幅條件做近似
    daily_range_pct = ((high - low) / close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], np.nan).fillna(999)
    compression_flag = (
        (daily_range_pct <= 3.5) |
        (close.between(ma20 * 0.97, ma20 * 1.06))
    )
    compression_days = compression_flag.astype(int) * 5

    # 2) 量縮後第一次放量：以 volume_ratio 近似 5日量縮後初放
    vol_shrink = vol_ratio.between(0.55, 1.15)
    first_expand = vol_ratio.between(1.20, 2.20)

    # 3) 連續小紅K：單日近似 + 低波動環境
    small_red_k = (
        (close > open_) &
        (((close - open_) / open_.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0) <= 0.03) &
        (daily_range_pct <= 5.5)
    )
    small_red_count = small_red_k.astype(int) * 4

    # 4) 假突破記憶：近似當前/近期假突破風險
    upper_shadow = ((high - close) / (high - low + 0.001)).replace([np.inf, -np.inf], np.nan).fillna(0)
    fake_breakout_memory = (
        (upper_shadow > 0.58) |
        (vol_ratio > 3.50) |
        (close > ma20 * 1.16)
    )

    time_score = (
        (compression_days >= 4).astype(int) * 15 +
        vol_shrink.astype(int) * 10 +
        first_expand.astype(int) * 20 +
        (small_red_count >= 4).astype(int) * 15 -
        fake_breakout_memory.astype(int) * 20
    )

    d["compression_days_v26672"] = compression_days
    d["vol_shrink_v26672"] = vol_shrink
    d["first_expand_v26672"] = first_expand
    d["small_red_k_v26672"] = small_red_k
    d["small_red_count_v26672"] = small_red_count
    d["fake_breakout_memory_v26672"] = fake_breakout_memory
    d["time_structure_score_v26672"] = time_score

    # 接到 v266.70 / v266.71 master score，不存在就新建
    if "master_trigger_score_v26670" not in d.columns:
        d["master_trigger_score_v26670"] = 0.0

    d["master_trigger_score_v26670"] = (
        _clip_series(d["master_trigger_score_v26670"]) + time_score
    ).round(2)

    # phase 更新：只輔助，不改原本欄位結構
    phase = pd.Series("WATCH", index=d.index, dtype=object)
    phase.loc[(d["master_trigger_score_v26670"] >= 62) & (~fake_breakout_memory)] = "MASTER_TRIGGER"
    phase.loc[
        (d["master_trigger_score_v26670"] >= 52) &
        (d["master_trigger_score_v26670"] < 62) &
        (~fake_breakout_memory)
    ] = "PRE_MASTER"
    phase.loc[fake_breakout_memory] = "FAKE_BREAKOUT_RISK"

    d["time_structure_phase_v26672"] = phase

    if "master_trigger_phase_v26670" in d.columns:
        d.loc[phase.isin(["MASTER_TRIGGER", "PRE_MASTER", "FAKE_BREAKOUT_RISK"]), "master_trigger_phase_v26670"] = phase

    d["time_structure_reason_v26672"] = ""
    d.loc[compression_days >= 4, "time_structure_reason_v26672"] += "壓縮天數成立｜"
    d.loc[vol_shrink, "time_structure_reason_v26672"] += "量能收斂｜"
    d.loc[first_expand, "time_structure_reason_v26672"] += "量縮後初放｜"
    d.loc[small_red_count >= 4, "time_structure_reason_v26672"] += "連續小紅K｜"
    d.loc[fake_breakout_memory, "time_structure_reason_v26672"] += "假突破記憶風險｜"
    d["time_structure_reason_v26672"] = d["time_structure_reason_v26672"].str.rstrip("｜")

    # 輔助 watch_mode：只在欄位存在時更新
    if "watch_mode" in d.columns:
        d.loc[phase.isin(["MASTER_TRIGGER", "PRE_MASTER"]), "watch_mode"] = "TEST"
        d.loc[phase.eq("FAKE_BREAKOUT_RISK"), "watch_mode"] = "WATCH"

    return d

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

    return d.sort_values(["entry_score", "mom20", "mom10"], ascending=False)


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

    return d.sort_values(["entry_score", "liquidity_score", "mom20"], ascending=False)


def build_trade_plan(core, alpha, regime, signal_date):
    """
    雙策略資金邏輯：
    - ALPHA：主力倉位，流動性高，允許較大資金。
    - CORE：早期卡位，小倉，低流動性只試單。
    """
    if regime == "TREND":
        parts = [
            alpha[alpha.action == "BUY"].head(8),
            alpha[alpha.action == "TEST"].head(5),
            core[core.action == "BUY"].head(3),
            core[core.action == "TEST"].head(5),
            alpha[alpha.action == "WATCH"].head(6),
        ]
    elif regime == "BEAR":
        parts = [
            alpha[alpha.action == "TEST"].head(5),
            alpha[alpha.action == "WATCH"].head(8),
            core[core.action == "TEST"].head(2),
            core[core.action == "WATCH"].head(6),
        ]
    else:
        parts = [
            alpha[alpha.action == "BUY"].head(5),
            alpha[alpha.action == "TEST"].head(6),
            core[core.action == "BUY"].head(3),
            core[core.action == "TEST"].head(6),
            alpha[alpha.action == "WATCH"].head(6),
        ]

    s = pd.concat(parts, ignore_index=True)

    if s.empty:
        s = pd.concat([alpha.head(8), core.head(8)], ignore_index=True).head(10)
        s["action"] = "WATCH"
        s["action_label"] = "觀察"
        s["action_sub"] = "低分觀察，不進場"

    # ALPHA 優先，CORE 次之；同層比分數與流動性。
    s["priority"] = np.where(s["strategy_type"] == "ALPHA", 1, 2)
    s = (
        s.sort_values(["priority", "entry_score", "liquidity_score"], ascending=[True, False, False])
        .drop_duplicates("stock_id")
        .head(36)
    )

    trade_date = next_trade_date(signal_date)
    rows = []

    for _, r in s.iterrows():
        px = float(r["close"]) * 1.001
        action = r["action"]
        st = r["strategy_type"]
        score = float(r["entry_score"])
        liq = str(r.get("liquidity_level", ""))

        # 資金配置：ALPHA 可承載資金，CORE 控小倉。
        if action == "BUY" and st == "ALPHA":
            w = 0.030 if score >= 82 else 0.020
        elif action == "BUY" and st == "CORE":
            w = 0.012 if liq == "HIGH" else 0.008
        elif action == "TEST" and st == "ALPHA":
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
            "market_regime": regime,
            "strategy_type": st,
            "strategy_name": r.get("strategy_name", st),
            "action": action,
            "action_label": r["action_label"],
            "action_sub": r["action_sub"],
            "stock_id": r["stock_id"],
            "price_tier": price_tier(px),
            "ref_price": round(px, 2),
            "target_weight": round(w, 4),
            "suggested_amount": round(amount, 0),
            "suggested_shares": round(shares, 2),
            "estimated_total_cost": round(shares * px * 1.0015, 2),
            "entry_score": round(score, 2),
            "liquidity_level": r.get("liquidity_level", ""),
            "liquidity_tag": r.get("liquidity_tag", ""),
            "liquidity_score": round(float(r.get("liquidity_score", 0)), 2),
            "volume": round(float(r.get("volume", 0)), 0),
            "turnover": round(float(r.get("turnover", 0)), 0),
            "source": "V266_DUAL",
            "reason": r.get("reason", r["note"]),
            "system_note": r.get("system_note", r["note"]),
            "note": r["note"],
        })

    return pd.DataFrame(rows)




def apply_final_output_hardblock_v26676(d):
    """
    v266.76 FINAL OUTPUT HARDBLOCK
    只補最後輸出前封殺層：
    - distribution_hardblock_v26675 == 1 直接移除
    - fake_breakout / 出貨風險 / 跌停風險直接不准進最終名單
    不改 UI / pipeline / 檔名 / 原策略核心。
    """
    d = d.copy()

    block_cols = [
        "distribution_hardblock_v26675",
        "hard_fake_breakout_v26671",
        "fake_breakout_v26670",
        "fake_breakout_memory_v26672",
    ]

    hard_block = pd.Series(False, index=d.index)

    for c in block_cols:
        if c in d.columns:
            hard_block = hard_block | (_clip_series(d[c]) >= 1)

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    open_ = _clip_series(d.get("open", close))
    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))
    ma20 = _clip_series(d.get("ma20", close))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    intraday_drop_from_high = (
        ((close - high) / high.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    upper_shadow_ratio = (
        ((high - close) / (high - open_).abs().replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    final_dump_block = (
        (intraday_drop_from_high <= -0.085) |
        ((close < open_) & (close < ma5) & (vol_ratio >= 2.2)) |
        ((close < ma10) & (mom20 >= 0.25)) |
        ((close > ma20 * 1.15) & (close < ma5)) |
        ((mom5 >= 0.15) & (close < ma5)) |
        (upper_shadow_ratio >= 0.65)
    )

    hard_block = hard_block | final_dump_block

    d["final_output_hardblock_v26676"] = hard_block.astype(int)
    d["final_output_hardblock_reason_v26676"] = ""

    d.loc[intraday_drop_from_high <= -0.085, "final_output_hardblock_reason_v26676"] += "高檔回落/近跌停｜"
    d.loc[(close < open_) & (close < ma5) & (vol_ratio >= 2.2), "final_output_hardblock_reason_v26676"] += "爆量長黑跌破MA5｜"
    d.loc[(close < ma10) & (mom20 >= 0.25), "final_output_hardblock_reason_v26676"] += "主升後跌破MA10｜"
    d.loc[(close > ma20 * 1.15) & (close < ma5), "final_output_hardblock_reason_v26676"] += "乖離過大轉弱｜"
    d.loc[(mom5 >= 0.15) & (close < ma5), "final_output_hardblock_reason_v26676"] += "短線過熱轉弱｜"
    d.loc[upper_shadow_ratio >= 0.65, "final_output_hardblock_reason_v26676"] += "長上影出貨風險｜"

    before = len(d)
    d = d.loc[~hard_block].copy()
    after = len(d)

    print(f"[v266.76] final output hardblock removed {before - after} rows, kept {after}")

    return d



def apply_fallback_hardblock_sync_v26677(d):
    """
    v266.77 FALLBACK HARDBLOCK SYNC
    只補：所有補名單 / fallback / backup / secondary 入口同步封殺。
    目的：避免像 6823 這種已被主名單 hardblock 的股票，又被 fallback 補回 TEST/WATCH。
    不動 UI / pipeline / 檔名 / 原策略核心。
    """
    d = d.copy()

    block_cols = [
        "final_output_hardblock_v26676",
        "distribution_hardblock_v26675",
        "hard_fake_breakout_v26671",
        "fake_breakout_v26670",
        "fake_breakout_memory_v26672",
    ]

    hard_block = pd.Series(False, index=d.index)

    for c in block_cols:
        if c in d.columns:
            hard_block = hard_block | (_clip_series(d[c]) >= 1)

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    open_ = _clip_series(d.get("open", close))
    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))
    ma20 = _clip_series(d.get("ma20", close))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    intraday_drop_from_high = (
        ((close - high) / high.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    final_dump_block = (
        (intraday_drop_from_high <= -0.085) |
        ((close < open_) & (close < ma5) & (vol_ratio >= 2.2)) |
        ((close < ma10) & (mom20 >= 0.25)) |
        ((close > ma20 * 1.15) & (close < ma5)) |
        ((mom5 >= 0.15) & (close < ma5))
    )

    hard_block = hard_block | final_dump_block

    d["fallback_hardblock_sync_v26677"] = hard_block.astype(int)

    before = len(d)
    d = d.loc[~hard_block].copy()
    after = len(d)

    print(f"[v266.77] fallback hardblock sync removed {before - after} rows, kept {after}")

    return d



def apply_trend_acceleration_rank_v26678(d):
    """
    v266.78 主升段排序強化
    不封殺股票，只重排 TEST / WATCH 強弱順序。
    """

    d = d.copy()

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))
    ma20 = _clip_series(d.get("ma20", close))

    volume_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    trend_stack = (
        (ma5 > ma10).astype(int) * 30 +
        (ma10 > ma20).astype(int) * 25 +
        (close > ma5).astype(int) * 20
    )

    ma5_slope = (
        ((ma5 / ma5.shift(3)) - 1)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    acceleration_score = (
        ma5_slope.clip(-0.1, 0.2) * 200
    )

    close_near_high = (
        (close / high.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    close_strength = (
        (close_near_high >= 0.97).astype(int) * 20 +
        (close_near_high >= 0.985).astype(int) * 15
    )

    stable_volume = (
        ((volume_ratio >= 1.2) & (volume_ratio <= 3.5)).astype(int) * 20
    )

    continuation_score = (
        (mom5 >= 0.05).astype(int) * 15 +
        (mom20 >= 0.12).astype(int) * 20
    )

    avoid_blowoff = (
        ((close > ma20 * 1.25) & (close < ma5)).astype(int) * -40
    )

    d["trend_acceleration_rank_v26678"] = (
        trend_stack +
        acceleration_score +
        close_strength +
        stable_volume +
        continuation_score +
        avoid_blowoff
    )

    if "total_score" in d.columns:
        d["total_score"] = (
            _clip_series(d["total_score"]) +
            d["trend_acceleration_rank_v26678"] * 0.35
        )

    return d



def apply_v270_trend_dominant_core(d):
    """
    v270 TREND DOMINANT CORE
    只補核心排序邏輯，不改 pipeline / UI / 輸出檔名 / 原本資料結構。

    目標：
    - 趨勢加速主導
    - 均線斜率主導
    - 回檔品質主導
    - 低波動轉強主導
    - 降低爆量、短線噴出、過熱動能的排序權重
    """
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    low = _clip_series(d.get("low", close))
    open_ = _clip_series(d.get("open", close))

    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))
    ma20 = _clip_series(d.get("ma20", close))

    volume_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom10 = _clip_series(d.get("mom10", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    old_score = _clip_series(d.get("total_score", d.get("entry_score", d.get("score", 0))))

    # 1) 趨勢排列：只看是否進入健康多頭結構
    trend_stack_score = (
        (ma5 > ma10).astype(int) * 22 +
        (ma10 > ma20).astype(int) * 24 +
        (close > ma20).astype(int) * 18 +
        (close > ma10).astype(int) * 12
    )

    # 2) 均線斜率：ma5 / ma10 是否開始上彎
    ma5_slope = ((ma5 / ma5.shift(3) - 1).replace([np.inf, -np.inf], np.nan).fillna(0))
    ma10_slope = ((ma10 / ma10.shift(5) - 1).replace([np.inf, -np.inf], np.nan).fillna(0))

    slope_score = (
        ma5_slope.clip(-0.05, 0.12) * 260 +
        ma10_slope.clip(-0.04, 0.10) * 220
    )

    # 3) 回檔品質：靠近 MA10/MA20 但沒破壞，優於追高
    dist_ma10 = (((close - ma10) / ma10.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(9))
    dist_ma20 = (((close - ma20) / ma20.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(9))

    pullback_quality_score = (
        (dist_ma10.between(-0.015, 0.060)).astype(int) * 22 +
        (dist_ma20.between(0.000, 0.120)).astype(int) * 16 -
        (dist_ma10 > 0.120).astype(int) * 22 -
        (dist_ma20 > 0.220).astype(int) * 30
    )

    # 4) 低波動轉強：溫和放量 + 收斂後轉強
    daily_range = (((high - low) / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(9))
    close_position = (((close - low) / (high - low + 0.001)).replace([np.inf, -np.inf], np.nan).fillna(0))

    controlled_breakout_score = (
        (daily_range.between(0.018, 0.070)).astype(int) * 14 +
        (close_position >= 0.62).astype(int) * 14 +
        (volume_ratio.between(0.85, 2.30)).astype(int) * 18 -
        (volume_ratio > 3.50).astype(int) * 34
    )

    # 5) 趨勢動能：保留早期主升段，降低過熱
    momentum_quality_score = (
        (mom5.between(0.015, 0.120)).astype(int) * 18 +
        (mom10.between(0.020, 0.180)).astype(int) * 18 +
        (mom20.between(0.040, 0.260)).astype(int) * 16 -
        (mom5 > 0.180).astype(int) * 24 -
        (mom20 > 0.360).astype(int) * 34
    )

    # 6) 出貨風險降權
    weak_close_penalty = (
        ((close < open_) & (close < ma5)).astype(int) * -30 +
        ((close < ma10) & (mom20 > 0.18)).astype(int) * -38 +
        (close_position < 0.35).astype(int) * -22
    )

    v270_trend_core_score = (
        trend_stack_score +
        slope_score +
        pullback_quality_score +
        controlled_breakout_score +
        momentum_quality_score +
        weak_close_penalty
    ).round(2)

    d["v270_trend_core_score"] = v270_trend_core_score

    d["v270_trend_phase"] = "WATCH"
    d.loc[v270_trend_core_score >= 105, "v270_trend_phase"] = "TREND_LEADER"
    d.loc[v270_trend_core_score.between(82, 104.999), "v270_trend_phase"] = "TREND_READY"
    d.loc[v270_trend_core_score.between(65, 81.999), "v270_trend_phase"] = "PULLBACK_READY"

    d["v270_trend_reason"] = ""
    d.loc[(ma5 > ma10) & (ma10 > ma20), "v270_trend_reason"] += "均線多頭｜"
    d.loc[(ma5_slope > 0) & (ma10_slope > 0), "v270_trend_reason"] += "斜率上彎｜"
    d.loc[dist_ma10.between(-0.015, 0.060), "v270_trend_reason"] += "靠近MA10｜"
    d.loc[volume_ratio.between(0.85, 2.30), "v270_trend_reason"] += "溫和量能｜"
    d.loc[close_position >= 0.62, "v270_trend_reason"] += "收盤偏強｜"
    d.loc[(volume_ratio > 3.50) | (mom20 > 0.36), "v270_trend_reason"] += "過熱降權｜"
    d["v270_trend_reason"] = d["v270_trend_reason"].str.rstrip("｜")

    # v270 核心：趨勢排序主導，舊分數只保留 20% 作穩定參考
    new_score = (v270_trend_core_score * 0.80 + old_score * 0.20).round(2)

    if "total_score" in d.columns:
        d["total_score"] = new_score
    elif "entry_score" in d.columns:
        d["entry_score"] = new_score
    else:
        d["v270_final_score"] = new_score

    if "system_rank" in d.columns:
        d["system_rank"] = new_score

    return d



def apply_v272_final_csv_output_override(df):
    """v272 FINAL CSV OUTPUT OVERRIDE：輸出 CSV 前接管排序與物理剔除。"""
    if df is None or len(df) == 0:
        return df
    s = df.copy()
    if "v270_trend_core_score" not in s.columns:
        try:
            s = apply_v270_trend_dominant_core(s)
        except Exception as e:
            print(f"[v272] skip v270 score build: {e}")
    close = _clip_series(s.get("close", 0))
    high = _clip_series(s.get("high", close))
    low = _clip_series(s.get("low", close))
    open_ = _clip_series(s.get("open", close))
    ma5 = _clip_series(s.get("ma5", close))
    ma10 = _clip_series(s.get("ma10", close))
    ma20 = _clip_series(s.get("ma20", close))
    volume_ratio = _clip_series(s.get("volume_ratio", 1))
    mom5 = _clip_series(s.get("mom5", 0))
    mom20 = _clip_series(s.get("mom20", 0))
    close_position = (((close - low) / (high - low + 0.001)).replace([np.inf, -np.inf], np.nan).fillna(0))
    upper_shadow_ratio = (((high - close) / (high - low + 0.001)).replace([np.inf, -np.inf], np.nan).fillna(0))
    hard_remove = (
        ((close < open_) & (close < ma5) & (volume_ratio > 2.20)) |
        ((close < ma10) & (mom20 > 0.18)) |
        (((high / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1)) > 1.08) |
        (((close / ma20.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1)) > 1.22) |
        ((mom5 > 0.16) & (close < ma5)) |
        ((upper_shadow_ratio > 0.62) & (volume_ratio > 1.8)) |
        (close_position < 0.22)
    )
    for c in ["distribution_hardblock_v26675", "final_output_hardblock_v26676", "fallback_hardblock_sync_v26677", "hard_fake_breakout_v26671", "fake_breakout_memory_v26672"]:
        if c in s.columns:
            hard_remove = hard_remove | (_clip_series(s[c]) >= 1)
    before = len(s)
    s = s.loc[~hard_remove].copy()
    removed = before - len(s)
    sort_col = "v270_trend_core_score" if "v270_trend_core_score" in s.columns else ("v272_final_rank_score" if "v272_final_rank_score" in s.columns else ("total_score" if "total_score" in s.columns else "entry_score"))
    extra_cols = [c for c in ["liquidity_score", "mom20", "volume_ratio"] if c in s.columns]
    sort_cols = [sort_col] + extra_cols
    s = s.sort_values(sort_cols, ascending=[False]*len(sort_cols))
    if "symbol" in s.columns:
        s = s.drop_duplicates(subset=["symbol"])
    s = s.reset_index(drop=True)
    print(f"[v272] final csv override removed {removed} rows, sorted by {sort_col}, kept {len(s)}")
    return s






def apply_v2731_final_export_continuous_score(df):
    """
    v273.1 FINAL EXPORT CONTINUOUS SCORE PATCH

    只在最後輸出前處理 DataFrame：
    - 產生 v273_continuous_score
    - 用 v273_continuous_score 覆蓋 score / entry_score / total_score / system_rank
    - 依 v273_continuous_score 重新排序
    - 移除明顯高檔出貨/爆量長黑/弱收盤風險

    不處理 Timestamp / date / scalar，避免影響 next_trade_date。
    """
    if df is None:
        return df
    if not hasattr(df, "columns") or not hasattr(df, "copy"):
        return df
    if len(df) == 0:
        return df

    s = df.copy()

    close = _clip_series(s.get("close", 0))
    high = _clip_series(s.get("high", close))
    low = _clip_series(s.get("low", close))
    open_ = _clip_series(s.get("open", close))

    ma5 = _clip_series(s.get("ma5", close))
    ma10 = _clip_series(s.get("ma10", close))
    ma20 = _clip_series(s.get("ma20", close))

    volume_ratio = _clip_series(s.get("volume_ratio", 1))
    liquidity_score = _clip_series(s.get("liquidity_score", 50))
    mom5 = _clip_series(s.get("mom5", 0))
    mom10 = _clip_series(s.get("mom10", 0))
    mom20 = _clip_series(s.get("mom20", 0))

    base_score = _clip_series(
        s.get(
            "v270_trend_core_score",
            s.get("total_score", s.get("entry_score", s.get("score", 50)))
        )
    )

    ma5_slope = (
        (ma5 / ma5.shift(3) - 1)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )
    ma10_slope = (
        (ma10 / ma10.shift(5) - 1)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    trend_strength = (
        (ma5 > ma10).astype(int) * 7.5 +
        (ma10 > ma20).astype(int) * 8.5 +
        (close > ma5).astype(int) * 5.5 +
        (close > ma20).astype(int) * 4.5
    )

    slope_strength = (
        ma5_slope.clip(-0.04, 0.10) * 180 +
        ma10_slope.clip(-0.03, 0.08) * 160
    )

    dist_ma10 = (
        ((close - ma10) / ma10.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(9)
    )
    dist_ma20 = (
        ((close - ma20) / ma20.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(9)
    )

    pullback_quality = (
        dist_ma10.between(-0.015, 0.055).astype(int) * 8.2 +
        dist_ma20.between(0.000, 0.120).astype(int) * 5.8 -
        (dist_ma10 > 0.115).astype(int) * 8.5 -
        (dist_ma20 > 0.220).astype(int) * 12.5
    )

    daily_range = (
        ((high - low) / close.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(9)
    )
    close_position = (
        ((close - low) / (high - low + 0.001))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    candle_quality = (
        daily_range.between(0.015, 0.070).astype(int) * 5.5 +
        (close_position >= 0.62).astype(int) * 6.8 +
        (close_position >= 0.78).astype(int) * 4.2 -
        (close_position < 0.30).astype(int) * 10.0
    )

    volume_quality = (
        volume_ratio.between(0.85, 1.80).astype(int) * 7.0 +
        volume_ratio.between(1.80, 2.60).astype(int) * 4.0 -
        (volume_ratio > 3.50).astype(int) * 15.0 -
        (volume_ratio < 0.45).astype(int) * 5.0
    )

    momentum_quality = (
        mom5.between(0.010, 0.090).astype(int) * 6.5 +
        mom10.between(0.015, 0.140).astype(int) * 6.5 +
        mom20.between(0.030, 0.240).astype(int) * 5.8 -
        (mom5 > 0.160).astype(int) * 8.5 -
        (mom20 > 0.340).astype(int) * 12.0
    )

    liquidity_quality = (liquidity_score.clip(0, 100) / 100.0) * 8.0

    weak_pattern_penalty = (
        ((close < open_) & (close < ma5)).astype(int) * -12.0 +
        ((close < ma10) & (mom20 > 0.16)).astype(int) * -14.0 +
        (
            ((high / close.replace(0, np.nan))
             .replace([np.inf, -np.inf], np.nan)
             .fillna(1)) > 1.08
        ).astype(int) * -12.0
    )

    continuous_score = (
        base_score * 0.30 +
        trend_strength +
        slope_strength +
        pullback_quality +
        candle_quality +
        volume_quality +
        momentum_quality +
        liquidity_quality +
        weak_pattern_penalty
    ).clip(0, 100).round(2)

    s["v273_continuous_score"] = continuous_score
    s["v273_score_reason"] = ""

    s.loc[(ma5 > ma10) & (ma10 > ma20), "v273_score_reason"] += "趨勢多頭｜"
    s.loc[(ma5_slope > 0) & (ma10_slope > 0), "v273_score_reason"] += "均線上彎｜"
    s.loc[dist_ma10.between(-0.015, 0.055), "v273_score_reason"] += "貼近MA10｜"
    s.loc[volume_ratio.between(0.85, 2.60), "v273_score_reason"] += "量能健康｜"
    s.loc[close_position >= 0.62, "v273_score_reason"] += "收盤偏強｜"
    s.loc[(volume_ratio > 3.50) | (mom20 > 0.34), "v273_score_reason"] += "過熱降權｜"
    s["v273_score_reason"] = s["v273_score_reason"].str.rstrip("｜")

    # 最終輸出前強制排除明顯高檔轉弱 / 出貨型態
    upper_shadow_ratio = (
        ((high - close) / (high - low + 0.001))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    hard_remove = (
        ((close < open_) & (close < ma5) & (volume_ratio > 2.20)) |
        ((close < ma10) & (mom20 > 0.18)) |
        (((high / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1)) > 1.08) |
        (((close / ma20.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1)) > 1.22) |
        ((mom5 > 0.16) & (close < ma5)) |
        ((upper_shadow_ratio > 0.62) & (volume_ratio > 1.8)) |
        (close_position < 0.22)
    )

    for c in [
        "distribution_hardblock_v26675",
        "final_output_hardblock_v26676",
        "fallback_hardblock_sync_v26677",
        "hard_fake_breakout_v26671",
        "fake_breakout_memory_v26672",
    ]:
        if c in s.columns:
            hard_remove = hard_remove | (_clip_series(s[c]) >= 1)

    before = len(s)
    s = s.loc[~hard_remove].copy()
    removed = before - len(s)

    # 覆蓋前端既有分數欄位，不改欄位名稱
    for col in ["score", "entry_score", "total_score", "system_rank"]:
        if col in s.columns:
            s[col] = s["v273_continuous_score"].round(2)

    # 排序
    sort_cols = ["v273_continuous_score"]
    for c in ["liquidity_score", "mom20", "volume_ratio"]:
        if c in s.columns:
            sort_cols.append(c)

    s = (
        s.sort_values(sort_cols, ascending=[False] * len(sort_cols))
         .drop_duplicates(subset=["symbol"] if "symbol" in s.columns else None)
         .reset_index(drop=True)
    )

    print(f"[v273.1] final export continuous score removed {removed} rows, kept {len(s)}")
    return s

def main():
    df = load_feature()
    signal_date, latest = latest_valid(df)
    regime, info = detect_regime(latest)

    core = core_engine(latest).head(60)
    alpha = alpha_engine(latest).head(60)

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
        "source": "v266_9_strategy_engine_stable",
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


if __name__ == "__main__":
    main()


# =========================================================
# v266.73 DYNAMIC TRIGGER PATCH
# 只補：
# - 主力剛啟動放大
# - 第一波突破優先
# - 避免過度穩定
# 不動原 pipeline / UI / 檔名 / 輸出。
# =========================================================

def apply_dynamic_trigger_patch_v26673(d):
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    ma20 = _clip_series(d.get("ma20", close))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom10 = _clip_series(d.get("mom10", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    breakout_ready = (
        (close > ma20 * 1.01) &
        (close < ma20 * 1.12)
    )

    early_launch = (
        (mom5 > 0.015) &
        (mom10 > 0.01) &
        (mom20 < 0.22)
    )

    warm_volume = (
        (vol_ratio >= 1.15) &
        (vol_ratio <= 2.80)
    )

    avoid_dead_pool = (
        (vol_ratio < 0.75) |
        (mom5 < -0.03)
    )

    dynamic_score = (
        breakout_ready.astype(int) * 18 +
        early_launch.astype(int) * 20 +
        warm_volume.astype(int) * 15 -
        avoid_dead_pool.astype(int) * 20
    )

    d["dynamic_trigger_score_v26673"] = dynamic_score

    if "entry_score" in d.columns:
        d["entry_score"] = (
            _clip_series(d["entry_score"]) +
            dynamic_score.clip(lower=-15, upper=35)
        )

    return d


# =========================================================
# v266.74 VOLATILITY EXPANSION PATCH
# 只補：
# - 波動開始擴張
# - 主升段前夕優先
# - 壓低過度穩定股
# 不動原本 pipeline / UI / output。
# =========================================================

def apply_volatility_expansion_patch_v26674(d):
    d = d.copy()

    atr_ratio = _clip_series(d.get("atr_ratio", 1))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom10 = _clip_series(d.get("mom10", 0))
    breakout_score = _clip_series(d.get("breakout_score", 0))
    entry_score = _clip_series(d.get("entry_score", 0))

    expanding_volatility = (
        (atr_ratio > 1.08) &
        (atr_ratio < 2.20)
    )

    ignition_volume = (
        (vol_ratio > 1.20) &
        (vol_ratio < 3.50)
    )

    early_momentum = (
        (mom5 > 0.02) &
        (mom10 > 0.01)
    )

    breakout_ready = (
        breakout_score > 55
    )

    over_stable = (
        (atr_ratio < 0.92) &
        (vol_ratio < 1.05)
    )

    expansion_bonus = (
        expanding_volatility.astype(int) * 20 +
        ignition_volume.astype(int) * 15 +
        early_momentum.astype(int) * 18 +
        breakout_ready.astype(int) * 12 -
        over_stable.astype(int) * 25
    )

    d["volatility_expansion_score_v26674"] = expansion_bonus

    if "entry_score" in d.columns:
        d["entry_score"] = (
            entry_score +
            expansion_bonus.clip(lower=-20, upper=40)
        )

    return d


# =========================================================
# v266.75 DISTRIBUTION HARD BLOCK PATCH
# =========================================================

def apply_distribution_hardblock_patch_v26675(d):
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    high = _clip_series(d.get("high", close))
    open_ = _clip_series(d.get("open", close))

    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    mom5 = _clip_series(d.get("mom5", 0))
    mom20 = _clip_series(d.get("mom20", 0))

    ma5 = _clip_series(d.get("ma5", close))
    ma10 = _clip_series(d.get("ma10", close))

    entry_score = _clip_series(d.get("entry_score", 0))

    intraday_drop = (close - high) / high

    limitdown_like = (
        intraday_drop < -0.085
    )

    high_volume_dump = (
        (vol_ratio > 2.8) &
        (close < open_) &
        (close < ma5)
    )

    fake_breakout = (
        (mom20 > 0.35) &
        (close < ma10)
    )

    long_black_distribution = (
        (((close - open_) / open_) < -0.06) &
        (vol_ratio > 2.0)
    )

    exhaustion_move = (
        (mom5 > 0.18) &
        (close < ma5)
    )

    hard_block = (
        limitdown_like |
        high_volume_dump |
        fake_breakout |
        long_black_distribution |
        exhaustion_move
    )

    d["distribution_hardblock_v26675"] = hard_block.astype(int)

    if "entry_score" in d.columns:
        d.loc[hard_block, "entry_score"] = (
            entry_score[hard_block] - 999
        )

    return d
