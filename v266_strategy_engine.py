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
