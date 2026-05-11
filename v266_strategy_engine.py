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




# ===== v276 MAX OPPORTUNITY PATCH =====
# 只做最大機會加權補丁
# 不重寫策略、不改 pipeline、不改 UI、不改欄位 schema

def _v276_num(series, default=0.0):
    return pd.to_numeric(series, errors="coerce").fillna(default)

def apply_v276_max_opportunity_patch(df):
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    score_col = None
    for c in [
        "v273_continuous_score",
        "score",
        "total_score",
        "entry_score",
    ]:
        if c in df.columns:
            score_col = c
            break

    if score_col is None:
        return df

    base_score = _v276_num(df[score_col], 0)

    close = _v276_num(df["close"], 0) if "close" in df.columns else pd.Series(0, index=df.index)
    ma5 = _v276_num(df["ma5"], 0) if "ma5" in df.columns else pd.Series(0, index=df.index)
    ma10 = _v276_num(df["ma10"], 0) if "ma10" in df.columns else pd.Series(0, index=df.index)
    ma20 = _v276_num(df["ma20"], 0) if "ma20" in df.columns else pd.Series(0, index=df.index)

    volume = _v276_num(df["volume"], 0) if "volume" in df.columns else pd.Series(0, index=df.index)

    trend_start = (
        (close > ma5) &
        (ma5 >= ma10) &
        (ma10 >= ma20) &
        (ma20 > 0)
    )

    near_breakout = (
        (ma20 > 0) &
        ((close / ma20 - 1) <= 0.18)
    )

    volume_expand = volume >= volume.quantile(0.65)

    early_turn = trend_start & near_breakout & volume_expand

    strong_leader = (
        trend_start &
        (volume >= volume.quantile(0.85))
    )

    overheat = (
        (ma20 > 0) &
        ((close / ma20 - 1) > 0.30)
    )

    boost = pd.Series(1.0, index=df.index)

    boost = np.where(early_turn, boost * 1.08, boost)
    boost = np.where(strong_leader, boost * 1.12, boost)
    boost = np.where(overheat, boost * 0.75, boost)

    boost = pd.Series(boost, index=df.index).clip(0.75, 1.25)

    df["v276_opportunity_boost"] = boost.round(3)

    enhanced = base_score * boost

    pct = enhanced.rank(method="first", pct=True)
    final_score = (65 + pct * 34).clip(0, 99.9).round(1)

    df["score"] = final_score

    for c in ["entry_score", "total_score", "rank_score"]:
        if c in df.columns:
            df[c] = final_score

    df["v276_opportunity_tag"] = np.where(
        overheat,
        "AVOID_OVERHEAT",
        np.where(
            strong_leader,
            "S_LEADER",
            np.where(
                early_turn,
                "A_EARLY_TURN",
                "B_NORMAL"
            )
        )
    )

    return df

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


# =========================================================
# v274 SCORE NORMALIZATION PATCH
# 只修最後輸出分數尺度：
# - 保留 v273_continuous_score 原始真實分數
# - 另外輸出 v274_normalized_score
# - 前端顯示用 score / entry_score / total_score / system_rank 改為 60~99
# - 不動策略核心 / pipeline / UI / 持倉 / macro
# =========================================================

def apply_v274_score_normalization(df):
    if df is None:
        return df
    if not hasattr(df, "columns") or not hasattr(df, "copy"):
        return df
    if len(df) == 0:
        return df

    s = df.copy()

    # 只處理股票清單型輸出，避免 meta/debug 純統計列被誤改。
    if "stock_id" not in s.columns and "symbol" not in s.columns:
        return s

    score_col = None
    for c in [
        "v273_continuous_score",
        "v270_trend_core_score",
        "total_score",
        "entry_score",
        "score",
    ]:
        if c in s.columns:
            score_col = c
            break

    if score_col is None:
        return s

    raw = pd.to_numeric(s[score_col], errors="coerce")
    if raw.notna().sum() == 0:
        return s

    # 保留原始真實分數，方便後續檢查，不拿掉。
    if "v273_raw_score_before_normalize" not in s.columns:
        s["v273_raw_score_before_normalize"] = raw.round(2)

    # 百分位正規化：避免 139 / 138 這種累加分數直接顯示。
    # top 接近 99，尾端仍保留 60 以上，讓 UI 可讀。
    rank_pct = raw.rank(method="average", pct=True, ascending=True)
    normalized = (60 + rank_pct * 39).clip(60, 99).round(1)

    s["v274_normalized_score"] = normalized

    for col in ["score", "entry_score", "total_score", "system_rank"]:
        if col in s.columns:
            s[col] = normalized

    sort_cols = ["v274_normalized_score"]
    for c in ["v273_continuous_score", "liquidity_score", "mom20", "volume_ratio"]:
        if c in s.columns:
            sort_cols.append(c)

    s = s.sort_values(sort_cols, ascending=[False] * len(sort_cols)).reset_index(drop=True)

    return s


def write_both(df, name):
    # v273 FINAL EXPORT PATCH
    # 只接管最後輸出層，不動策略核心
    try:
        export_targets = {
            "trade_plan.csv",
            "candidates.csv",
            "core_candidates.csv",
            "alpha_candidates.csv",
            "selection_debug.csv",
        }

        if (
            name in export_targets
            and df is not None
            and hasattr(df, "columns")
            and len(df) > 0
        ):
            patched = apply_v2731_final_export_continuous_score(df.copy())

            if (
                patched is not None
                and hasattr(patched, "columns")
                and "v273_continuous_score" in patched.columns
            ):
                patched["score"] = patched["v273_continuous_score"]

                if "entry_score" in patched.columns:
                    patched["entry_score"] = patched["v273_continuous_score"]

                if "total_score" in patched.columns:
                    patched["total_score"] = patched["v273_continuous_score"]

                if "system_rank" in patched.columns:
                    patched["system_rank"] = patched["v273_continuous_score"]

                patched = patched.sort_values(
                    by="v273_continuous_score",
                    ascending=False
                ).reset_index(drop=True)

                # v274：只在最後輸出前把顯示分數壓回 60~99，
                # 不改 v273_continuous_score 原始真實分數。
                patched = apply_v274_score_normalization(patched)

                df = patched

                print(
                    f"[v273 FINAL EXPORT PATCH + v274 NORMALIZE] {name} "
                    f"continuous score normalized"
                )

    except Exception as e:
        print(f"[v273 FINAL EXPORT PATCH ERROR] {e}")

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


# ===== v282 LIQUIDITY GATE + CHIP ACCUMULATION OPPORTUNITY PATCH =====
# 只補最大機會核心排序：
# 1. 流動性先合格，低於門檻直接排除/降權
# 2. 流動性合格後，籌碼開始集中才是 alpha
# 3. 優先剛轉強、未過熱、突破初期、量能剛啟動
# 不重寫原策略、不改輸出檔名、不改 UI / pipeline / 持倉 / macro

def _v282_num(d, col, default=0.0):
    if col in d.columns:
        return pd.to_numeric(d[col], errors="coerce").fillna(default)
    return pd.Series(default, index=d.index, dtype="float64")


def apply_v282_liquidity_chip_opportunity_patch(d, mode="ALPHA"):
    if d is None or len(d) == 0:
        return d

    d = d.copy()
    mode = str(mode or "ALPHA").upper()

    close = _v282_num(d, "close", 0)
    high = _v282_num(d, "high", close)
    open_ = _v282_num(d, "open", close)
    volume = _v282_num(d, "volume", 0)
    turnover = _v282_num(d, "turnover", close * volume * 1000)
    vol_ratio = _v282_num(d, "volume_ratio", 1)
    vol20 = _v282_num(d, "vol20", 0)

    mom3 = _v282_num(d, "mom3", 0)
    mom5 = _v282_num(d, "mom5", 0)
    mom10 = _v282_num(d, "mom10", 0)
    mom20 = _v282_num(d, "mom20", 0)
    mom60 = _v282_num(d, "mom60", 0)

    ma5 = _v282_num(d, "ma5", close)
    ma10 = _v282_num(d, "ma10", close)
    ma20 = _v282_num(d, "ma20", close)
    ma60 = _v282_num(d, "ma60", close)
    ma20_slope = _v282_num(d, "ma20_slope", 0)

    high20 = _v282_num(d, "high_20", close)
    high60 = _v282_num(d, "high_60", close)
    low20 = _v282_num(d, "low_20", close)
    range20 = _v282_num(d, "range_20", 0)
    ma_converge = _v282_num(d, "ma_converge_pct", 999)

    obv_mom5 = _v282_num(d, "obv_mom5", 0)
    obv_up5 = _v282_num(d, "obv_up_count_5", 0)
    low_non_down5 = _v282_num(d, "low_non_down_count_5", 0)

    # 1) 流動性 Gate：
    # 使用者明確要求 500 張內沒有參考價值，風險過大。
    # ALPHA 更嚴，CORE 仍最低要 >=1000 張或成交金額達標。
    base_liq_gate = (
        (volume >= 1000) |
        (turnover >= 30_000_000)
    )

    alpha_liq_gate = (
        (volume >= 3000) |
        (turnover >= 80_000_000) |
        (d.get("liquidity_level", "").astype(str).str.upper().eq("HIGH") if "liquidity_level" in d.columns else False)
    )

    if mode == "ALPHA":
        liquidity_gate = alpha_liq_gate
    else:
        liquidity_gate = base_liq_gate

    # 2) 籌碼開始集中：
    # 沒有法人資料時，用 OBV、低點不破、溫和量增作為「資金建倉」代理。
    chip_accumulation_score = (
        (obv_mom5 > 0).astype(int) * 18 +
        (obv_up5 >= 3).astype(int) * 16 +
        (low_non_down5 >= 3).astype(int) * 12 +
        vol_ratio.between(1.05, 3.20).astype(int) * 12 +
        ((volume >= vol20 * 1.05) & (vol20 > 0)).astype(int) * 8 +
        (ma_converge <= 0.12).astype(int) * 8
    )

    chip_accumulating = chip_accumulation_score >= 34

    # 3) 剛轉強：不是死股，也不是已經噴很遠。
    early_turn_score = (
        (close > ma5).astype(int) * 8 +
        (close > ma10).astype(int) * 8 +
        (ma5 >= ma10 * 0.995).astype(int) * 8 +
        (ma10 >= ma20 * 0.985).astype(int) * 8 +
        (ma20_slope >= 0).astype(int) * 6 +
        mom5.between(0.005, 0.12).astype(int) * 8 +
        mom10.between(0.005, 0.18).astype(int) * 8 +
        mom20.between(0.02, 0.32).astype(int) * 8
    )

    # 4) 突破初期 / 量能剛啟動：避免只抓到已經噴完的熱門股。
    breakout_initial_score = (
        (close >= high20 * 0.985).astype(int) * 10 +
        (close >= high60 * 0.90).astype(int) * 6 +
        (close <= high60 * 1.03).astype(int) * 8 +
        vol_ratio.between(1.10, 3.80).astype(int) * 10 +
        ((high20 > 0) & ((close / high20 - 1).abs() <= 0.04)).astype(int) * 8
    )

    # 5) 過熱 / 出貨風險：強勢但風報比變差，直接扣。
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    close_ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)

    overheat_penalty = (
        (mom5 > 0.16).astype(int) * 18 +
        (mom20 > 0.38).astype(int) * 22 +
        (close_ma20_gap > 0.25).astype(int) * 24 +
        (vol_ratio > 5.50).astype(int) * 18 +
        ((upper_shadow > 0.055) & (vol_ratio > 1.80)).astype(int) * 18 +
        ((close < open_) & (vol_ratio > 2.20)).astype(int) * 20
    )

    opportunity_score = (
        chip_accumulation_score * 1.30 +
        early_turn_score * 1.00 +
        breakout_initial_score * 0.95 -
        overheat_penalty * 1.20
    )

    # 流動性不合格：直接壓掉，不讓 500 張內股票混入最大機會池。
    opportunity_score = pd.Series(opportunity_score, index=d.index)
    opportunity_score.loc[~liquidity_gate] = opportunity_score.loc[~liquidity_gate] - 999

    # 流動性合格但沒有籌碼集中：不是最大機會，只能保守降權。
    opportunity_score.loc[liquidity_gate & ~chip_accumulating] = (
        opportunity_score.loc[liquidity_gate & ~chip_accumulating] - 18
    )

    # 寫入可檢查欄位。
    d["v282_liquidity_gate"] = liquidity_gate.astype(int)
    d["v282_chip_accumulation_score"] = pd.Series(chip_accumulation_score, index=d.index).round(1)
    d["v282_early_turn_score"] = pd.Series(early_turn_score, index=d.index).round(1)
    d["v282_breakout_initial_score"] = pd.Series(breakout_initial_score, index=d.index).round(1)
    d["v282_overheat_penalty"] = pd.Series(overheat_penalty, index=d.index).round(1)
    d["v282_opportunity_score"] = opportunity_score.round(1)
    d["v282_opportunity_tag"] = np.where(
        ~liquidity_gate,
        "BLOCK_LOW_LIQUIDITY",
        np.where(
            chip_accumulating & (overheat_penalty <= 18),
            "S_CHIP_ACCUMULATION",
            np.where(
                chip_accumulating,
                "A_CHIP_WITH_RISK_CHECK",
                "B_LIQUID_BUT_NO_CHIP"
            )
        )
    )

    if "entry_score" in d.columns:
        # 補丁只影響最大機會排序，不完全重寫原始分數。
        # 限制單次加權範圍，避免炸掉原本策略，但低流動性仍直接封殺。
        add_score = opportunity_score.clip(lower=-80, upper=55)
        d["entry_score"] = pd.to_numeric(d["entry_score"], errors="coerce").fillna(0) + add_score
        d.loc[~liquidity_gate, "entry_score"] = d.loc[~liquidity_gate, "entry_score"] - 999

    # note 補充，不覆蓋原本內容。
    if "note" in d.columns:
        d["note"] = d["note"].astype(str) + "｜v282流動性Gate後看籌碼集中"
    else:
        d["note"] = "v282流動性Gate後看籌碼集中"

    return d


# ===== v284 ACCUMULATION STAGE RANKING PATCH / 建倉初期排序補丁 =====
# 目的：
# - 不重寫原策略、不改 pipeline / UI / 持倉 / macro / workflow
# - 修正 v282 後大量同分 128 / 141 的問題
# - 流動性仍是 Gate，不當主要 alpha
# - 以「流動性合格後，籌碼開始集中 + 剛轉強 + 量能剛啟動 + 未過熱」做連續排序
# - 抓起漲前夕，而不是追已經噴完的熱門股

def _v284_num(d, col, default=0.0):
    if col in d.columns:
        return pd.to_numeric(d[col], errors="coerce").fillna(default)
    return pd.Series(default, index=d.index, dtype="float64")


def _v284_rank01(s):
    s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    if len(s) <= 1 or float(s.max()) == float(s.min()):
        return pd.Series(0.5, index=s.index, dtype="float64")
    return s.rank(method="first", pct=True).fillna(0.5)


def _v284_band_score(x, low, sweet_low, sweet_high, high):
    """
    連續帶狀分數：
    - low 以下 = 0
    - sweet_low ~ sweet_high = 1
    - high 以上逐步歸 0
    用來避免「全部符合條件就同分」。
    """
    x = pd.to_numeric(x, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    left = ((x - low) / max(sweet_low - low, 1e-9)).clip(0, 1)
    mid = pd.Series(1.0, index=x.index)
    right = ((high - x) / max(high - sweet_high, 1e-9)).clip(0, 1)
    return np.minimum(np.minimum(left, mid), right)


def apply_v284_accumulation_stage_ranking_patch(d, mode="ALPHA"):
    if d is None or len(d) == 0:
        return d

    d = d.copy()
    mode = str(mode or "ALPHA").upper()

    close = _v284_num(d, "close", 0)
    high = _v284_num(d, "high", close)
    open_ = _v284_num(d, "open", close)
    volume = _v284_num(d, "volume", 0)
    turnover = _v284_num(d, "turnover", close * volume * 1000)
    vol_ratio = _v284_num(d, "volume_ratio", 1)
    vol20 = _v284_num(d, "vol20", 0)

    mom3 = _v284_num(d, "mom3", 0)
    mom5 = _v284_num(d, "mom5", 0)
    mom10 = _v284_num(d, "mom10", 0)
    mom20 = _v284_num(d, "mom20", 0)
    mom60 = _v284_num(d, "mom60", 0)

    ma5 = _v284_num(d, "ma5", close)
    ma10 = _v284_num(d, "ma10", close)
    ma20 = _v284_num(d, "ma20", close)
    ma60 = _v284_num(d, "ma60", close)
    ma20_slope = _v284_num(d, "ma20_slope", 0)

    high20 = _v284_num(d, "high_20", close)
    high60 = _v284_num(d, "high_60", close)
    low20 = _v284_num(d, "low_20", close)
    range20 = _v284_num(d, "range_20", 0)
    ma_converge = _v284_num(d, "ma_converge_pct", 999)

    obv_mom5 = _v284_num(d, "obv_mom5", 0)
    obv_up5 = _v284_num(d, "obv_up_count_5", 0)
    low_non_down5 = _v284_num(d, "low_non_down_count_5", 0)

    liq_level = d["liquidity_level"].astype(str).str.upper() if "liquidity_level" in d.columns else pd.Series("", index=d.index)

    # 1) 流動性 Gate：符合才進排序；不把「越大量」當主 alpha。
    base_liq_gate = (volume >= 1000) | (turnover >= 30_000_000) | liq_level.isin(["MEDIUM", "HIGH"])
    alpha_liq_gate = (volume >= 3000) | (turnover >= 80_000_000) | liq_level.eq("HIGH")
    liquidity_gate = alpha_liq_gate if mode == "ALPHA" else base_liq_gate

    # 2) 籌碼「剛開始集中」：用 OBV、低點墊高、溫和量增做代理。
    # 注意：是初期，不是已經爆量完成。
    obv_rank = _v284_rank01(obv_mom5)
    low_hold_rank = _v284_rank01(low_non_down5)
    obv_up_rank = _v284_rank01(obv_up5)

    chip_early = (
        obv_rank * 28 +
        obv_up_rank * 20 +
        low_hold_rank * 18 +
        _v284_band_score(vol_ratio, 0.95, 1.10, 2.20, 3.80) * 18 +
        (ma_converge.clip(0, 0.30).rsub(0.30) / 0.30).clip(0, 1) * 16
    )

    # 3) 剛轉強：太弱不要，太強也可能已經過熱。
    mom5_band = _v284_band_score(mom5, -0.005, 0.008, 0.065, 0.145)
    mom10_band = _v284_band_score(mom10, -0.003, 0.010, 0.105, 0.220)
    mom20_band = _v284_band_score(mom20, 0.000, 0.030, 0.180, 0.360)

    ma_position = (
        (close > ma5).astype(int) * 8 +
        (close > ma10).astype(int) * 8 +
        (close > ma20).astype(int) * 10 +
        (ma5 >= ma10 * 0.995).astype(int) * 8 +
        (ma10 >= ma20 * 0.985).astype(int) * 8 +
        (ma20 >= ma60 * 0.975).astype(int) * 6 +
        (ma20_slope >= 0).astype(int) * 6
    )

    early_turn = (
        mom5_band * 18 +
        mom10_band * 18 +
        mom20_band * 18 +
        ma_position
    )

    # 4) 量能剛啟動：1.1~2.6 最佳；過大視為末升/隔日沖風險。
    volume_start = (
        _v284_band_score(vol_ratio, 0.95, 1.12, 2.60, 4.20) * 34 +
        ((vol20 > 0) & (volume >= vol20 * 1.05) & (volume <= vol20 * 3.80)).astype(int) * 12
    )

    # 5) 突破初期：接近高點/剛突破，但不要離太遠。
    dist_high20 = ((close / high20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    dist_high60 = ((close / high60) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    range_pos = ((close - low20) / (high20 - low20)).replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)

    breakout_early = (
        _v284_band_score(dist_high20, -0.045, -0.015, 0.018, 0.070) * 24 +
        _v284_band_score(dist_high60, -0.120, -0.060, 0.030, 0.120) * 14 +
        _v284_band_score(ma20_gap, -0.015, 0.015, 0.115, 0.220) * 20 +
        range_pos * 10
    )

    # 6) 過熱/出貨扣分。
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday_weak = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    overheat = (
        (mom5 > 0.15).astype(int) * 20 +
        (mom20 > 0.36).astype(int) * 28 +
        (ma20_gap > 0.22).astype(int) * 28 +
        (vol_ratio > 4.80).astype(int) * 24 +
        ((upper_shadow > 0.055) & (vol_ratio > 1.60)).astype(int) * 24 +
        ((intraday_weak < -0.025) & (vol_ratio > 1.80)).astype(int) * 22
    )

    # 7) 連續排序分：避免 128 / 141 這種桶狀同分。
    raw_stage_score = (
        chip_early * 0.34 +
        early_turn * 0.26 +
        volume_start * 0.18 +
        breakout_early * 0.18 -
        overheat * 0.30
    )

    raw_stage_score = pd.Series(raw_stage_score, index=d.index).replace([np.inf, -np.inf], np.nan).fillna(-999)
    raw_stage_score.loc[~liquidity_gate] = -999

    # 轉成 0~100 內的差異化分數；再用小數拉開同名次。
    valid = raw_stage_score > -900
    stage_score = pd.Series(0.0, index=d.index, dtype="float64")
    if valid.any():
        ranked = raw_stage_score[valid].rank(method="first", pct=True)
        detail = (
            _v284_rank01(chip_early[valid]) * 0.37 +
            _v284_rank01(early_turn[valid]) * 0.25 +
            _v284_rank01(volume_start[valid]) * 0.18 +
            _v284_rank01(breakout_early[valid]) * 0.14 -
            _v284_rank01(overheat[valid]) * 0.06
        )
        stage_score.loc[valid] = (55 + ranked * 38 + detail * 7).clip(0, 99.9)

    # CORE / ALPHA 稍微不同定位。
    if mode == "CORE":
        stage_score = (stage_score + _v284_band_score(mom20, 0.000, 0.020, 0.135, 0.280) * 5).clip(0, 99.9)
    else:
        stage_score = (stage_score + _v284_band_score(turnover, 30_000_000, 80_000_000, 260_000_000, 900_000_000) * 3).clip(0, 99.9)

    d["v284_liquidity_gate"] = liquidity_gate.astype(int)
    d["v284_chip_early_score"] = pd.Series(chip_early, index=d.index).round(2)
    d["v284_early_turn_score"] = pd.Series(early_turn, index=d.index).round(2)
    d["v284_volume_start_score"] = pd.Series(volume_start, index=d.index).round(2)
    d["v284_breakout_early_score"] = pd.Series(breakout_early, index=d.index).round(2)
    d["v284_overheat_penalty"] = pd.Series(overheat, index=d.index).round(2)
    d["v284_stage_raw_score"] = raw_stage_score.round(3)
    d["v284_stage_score"] = stage_score.round(1)
    d["v284_stage_tag"] = np.where(
        ~liquidity_gate,
        "BLOCK_LOW_LIQUIDITY",
        np.where(
            (chip_early >= chip_early[valid].quantile(0.70) if valid.any() else False) & (overheat <= 24),
            "S_ACCUMULATION_EARLY",
            np.where(
                (early_turn >= pd.Series(early_turn, index=d.index)[valid].quantile(0.70) if valid.any() else False),
                "A_EARLY_TURN",
                "B_NORMAL"
            )
        )
    )

    # 關鍵：不要再用桶狀 entry_score 排序；保留原策略基礎，只把 v284 變成主排序。
    base_entry = pd.to_numeric(d.get("entry_score", 0), errors="coerce").fillna(0)
    d["entry_score_before_v284"] = base_entry.round(2)

    # 流動性不合格直接壓掉；合格者用 stage_score 拉開。
    d["entry_score"] = np.where(
        liquidity_gate,
        (stage_score + (base_entry.rank(method="first", pct=True).fillna(0.5) * 8)).round(1),
        -999
    )

    if "note" in d.columns:
        d["note"] = d["note"].astype(str) + "｜v284建倉初期排序"
    else:
        d["note"] = "v284建倉初期排序"

    return d


# ===== v300 ALPHA RESTORE + TOP5 MAIN FORCE LABEL PATCH =====
# 核心原則：
# 1. 回到原始 ALPHA / CORE，不讓 v282 / v284 接管整份 TEST/WATCH 排序。
# 2. 流動性仍是門票：低量、低成交金額不標 TOP。
# 3. TOP5 是獨立標示層，只標示「主力發動前」候選，不改 action、不改 entry_score。
# 4. 主力發動前 = 流動性合格 + 籌碼開始集中 + 剛轉強 + 量能剛啟動 + 未過熱。
# 5. 不動 pipeline / UI / 持倉 / macro / workflow / 原輸出檔名。

def _v300_num(d, col, default=0.0):
    if col in d.columns:
        return pd.to_numeric(d[col], errors="coerce").fillna(default)
    return pd.Series(default, index=d.index, dtype="float64")


def _v300_rank01(s):
    s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    if len(s) <= 1 or float(s.max()) == float(s.min()):
        return pd.Series(0.5, index=s.index, dtype="float64")
    return s.rank(method="first", pct=True).fillna(0.5)


def _v300_band(x, low, sweet_low, sweet_high, high):
    x = pd.to_numeric(x, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    left = ((x - low) / max(sweet_low - low, 1e-9)).clip(0, 1)
    right = ((high - x) / max(high - sweet_high, 1e-9)).clip(0, 1)
    return np.minimum(left, right)


def apply_v300_alpha_restore_top5_labels(d, mode="ALPHA", limit=5):
    if d is None or len(d) == 0:
        return d

    d = d.copy()
    mode = str(mode or "ALPHA").upper()

    close = _v300_num(d, "close", 0)
    high = _v300_num(d, "high", close)
    open_ = _v300_num(d, "open", close)
    volume = _v300_num(d, "volume", 0)
    turnover = _v300_num(d, "turnover", close * volume * 1000)
    vol_ratio = _v300_num(d, "volume_ratio", 1)
    vol20 = _v300_num(d, "vol20", 0)

    mom5 = _v300_num(d, "mom5", 0)
    mom10 = _v300_num(d, "mom10", 0)
    mom20 = _v300_num(d, "mom20", 0)

    ma5 = _v300_num(d, "ma5", close)
    ma10 = _v300_num(d, "ma10", close)
    ma20 = _v300_num(d, "ma20", close)
    ma60 = _v300_num(d, "ma60", close)
    ma20_slope = _v300_num(d, "ma20_slope", 0)

    high20 = _v300_num(d, "high_20", close)
    high60 = _v300_num(d, "high_60", close)
    ma_converge = _v300_num(d, "ma_converge_pct", 999)

    obv_mom5 = _v300_num(d, "obv_mom5", 0)
    obv_up5 = _v300_num(d, "obv_up_count_5", 0)
    low_hold5 = _v300_num(d, "low_non_down_count_5", 0)

    liq_level = d["liquidity_level"].astype(str).str.upper() if "liquidity_level" in d.columns else pd.Series("", index=d.index)
    action = d["action"].astype(str).str.upper() if "action" in d.columns else pd.Series("", index=d.index)

    # 門票：流動性合格。500張內不碰；實戰 TOP 需要更高一點的可成交性。
    liquidity_gate = (
        (volume >= 1500) |
        (turnover >= 50_000_000) |
        liq_level.isin(["MEDIUM", "HIGH"])
    )

    # 攻擊性門票：避免防禦股、牛皮股、純穩定大成交量。
    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high20_gap = ((close / high20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high60_gap = ((close / high60) - 1).replace([np.inf, -np.inf], 0).fillna(0)

    attack_gate = (
        liquidity_gate &
        action.isin(["BUY", "TEST", "WATCH"]) &
        (close >= 15) &
        (close > ma20 * 0.985) &
        (ma5 >= ma10 * 0.985) &
        (ma10 >= ma20 * 0.965) &
        (ma20 >= ma60 * 0.940) &
        (mom5 > -0.015) &
        (mom10 > 0.000) &
        (mom20 > 0.020) &
        (mom20 < 0.420) &
        (high60 > 0) &
        (close >= high60 * 0.860) &
        (vol_ratio >= 0.95) &
        (vol_ratio <= 4.80) &
        (ma20_gap <= 0.240)
    )

    # 籌碼開始集中代理：OBV / 低點墊高 / 溫和量增 / 均線收斂。
    chip_start_score = (
        _v300_rank01(obv_mom5) * 30 +
        _v300_rank01(obv_up5) * 20 +
        _v300_rank01(low_hold5) * 18 +
        _v300_band(vol_ratio, 0.95, 1.10, 2.70, 4.20) * 18 +
        (ma_converge.clip(0, 0.28).rsub(0.28) / 0.28).clip(0, 1) * 14
    )

    # 剛轉強：不是過熱，不是死魚。
    turn_start_score = (
        _v300_band(mom5, -0.010, 0.006, 0.085, 0.170) * 20 +
        _v300_band(mom10, 0.000, 0.012, 0.135, 0.260) * 20 +
        _v300_band(mom20, 0.020, 0.045, 0.240, 0.420) * 18 +
        (close > ma5).astype(int) * 8 +
        (close > ma10).astype(int) * 8 +
        (close > ma20).astype(int) * 10 +
        (ma20_slope >= 0).astype(int) * 8
    )

    # 量能剛啟動：溫和放量，不追爆量。
    volume_start_score = (
        _v300_band(vol_ratio, 0.95, 1.12, 2.80, 4.80) * 30 +
        ((vol20 > 0) & (volume >= vol20 * 1.05) & (volume <= vol20 * 4.50)).astype(int) * 10
    )

    # 突破初期：接近突破，但不是離均線太遠。
    breakout_early_score = (
        _v300_band(high20_gap, -0.060, -0.020, 0.025, 0.085) * 24 +
        _v300_band(high60_gap, -0.150, -0.060, 0.060, 0.160) * 14 +
        _v300_band(ma20_gap, -0.020, 0.010, 0.130, 0.240) * 18
    )

    # 過熱 / 出貨風險扣分。
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday_weak = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    overheat_penalty = (
        (mom5 > 0.170).astype(int) * 24 +
        (mom20 > 0.420).astype(int) * 32 +
        (ma20_gap > 0.240).astype(int) * 30 +
        (vol_ratio > 5.20).astype(int) * 28 +
        ((upper_shadow > 0.060) & (vol_ratio > 1.60)).astype(int) * 26 +
        ((intraday_weak < -0.030) & (vol_ratio > 1.80)).astype(int) * 24
    )

    raw = (
        chip_start_score * 0.35 +
        turn_start_score * 0.25 +
        volume_start_score * 0.20 +
        breakout_early_score * 0.15 -
        overheat_penalty * 0.35
    )

    raw = pd.Series(raw, index=d.index).replace([np.inf, -np.inf], np.nan).fillna(-999)
    raw.loc[~attack_gate] = -999

    score = pd.Series(0.0, index=d.index, dtype="float64")
    valid = raw > -900
    if valid.any():
        score.loc[valid] = (60 + raw.loc[valid].rank(method="first", pct=True) * 39).clip(0, 99.9)

    d["v300_liquidity_gate"] = liquidity_gate.astype(int)
    d["v300_attack_gate"] = attack_gate.astype(int)
    d["v300_chip_start_score"] = pd.Series(chip_start_score, index=d.index).round(2)
    d["v300_turn_start_score"] = pd.Series(turn_start_score, index=d.index).round(2)
    d["v300_volume_start_score"] = pd.Series(volume_start_score, index=d.index).round(2)
    d["v300_breakout_early_score"] = pd.Series(breakout_early_score, index=d.index).round(2)
    d["v300_overheat_penalty"] = pd.Series(overheat_penalty, index=d.index).round(2)
    d["v300_main_force_raw"] = raw.round(3)
    d["v300_main_force_score"] = score.round(1)

    d["top_opportunity"] = ""
    d["section_top_opportunity"] = ""
    d["opportunity_rank"] = ""
    d["section_opportunity_rank"] = ""
    d["top_reason"] = ""

    # TOP5 是標示層：不改 action、不改 entry_score、不改排序核心。
    groups = [
        ("TOP5_TEST", action.isin(["BUY", "TEST"]) & valid & (score >= 82)),
        ("TOP5_WATCH", action.eq("WATCH") & valid & (score >= 78)),
    ]

    for label, mask in groups:
        idx = d.loc[mask].sort_values(
            ["v300_main_force_score", "v300_chip_start_score", "v300_turn_start_score", "stock_id"],
            ascending=[False, False, False, True]
        ).head(limit).index

        if len(idx) == 0:
            continue

        ranks = list(range(1, len(idx) + 1))
        d.loc[idx, "top_opportunity"] = "🔥TOP"
        d.loc[idx, "section_top_opportunity"] = label
        d.loc[idx, "opportunity_rank"] = ranks
        d.loc[idx, "section_opportunity_rank"] = ranks
        d.loc[idx, "top_reason"] = "主力發動前：流動性合格｜籌碼開始集中｜剛轉強｜量能剛啟動｜未過熱"

    if "note" in d.columns:
        d["note"] = np.where(
            d["top_opportunity"].astype(str).ne(""),
            d["note"].astype(str) + "｜v300主力TOP",
            d["note"].astype(str)
        )

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

    # v300：回到原始 CORE 分數；v282/v284 不再接管整份 TEST/WATCH。
    # TOP5 主力發動標示會在 set_action 後獨立套用。
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
    # v300：只標示 TOP5，不改 action / entry_score。
    d = apply_v300_alpha_restore_top5_labels(d, mode="CORE", limit=5)

    d["note"] = (
        "CORE早期卡位｜剛轉強｜靠近MA20｜量能回溫｜"
        + d["liquidity_tag"].astype(str)
    )

    return d.sort_values(["entry_score", "v300_main_force_score", "mom20", "mom10"], ascending=False) if "v300_main_force_score" in d.columns else d.sort_values(["entry_score", "mom20", "mom10"], ascending=False)


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

    # v300：回到原始 ALPHA 分數；v282/v284 不再接管整份 TEST/WATCH。
    # TOP5 主力發動標示會在 set_action 後獨立套用。
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
    # v300：只標示 TOP5，不改 action / entry_score。
    d = apply_v300_alpha_restore_top5_labels(d, mode="ALPHA", limit=5)

    d["note"] = (
        "ALPHA高流動性強勢延續｜成交量/成交金額優先｜突破/趨勢確認｜"
        + d["liquidity_tag"].astype(str)
    )

    return d.sort_values(["entry_score", "v300_main_force_score", "liquidity_score", "mom20"], ascending=False) if "v300_main_force_score" in d.columns else d.sort_values(["entry_score", "liquidity_score", "mom20"], ascending=False)


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
    if "top_opportunity" in s.columns:
        s["_top_sort"] = s["top_opportunity"].astype(str).ne("").astype(int)
    else:
        s["_top_sort"] = 0

    s = (
        s.sort_values(["priority", "_top_sort", "entry_score", "liquidity_score"], ascending=[True, False, False, False])
        .drop_duplicates("stock_id")
        .head(36)
        .drop(columns=["_top_sort"], errors="ignore")
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
            "top_opportunity": r.get("top_opportunity", ""),
            "section_top_opportunity": r.get("section_top_opportunity", ""),
            "opportunity_rank": r.get("opportunity_rank", ""),
            "section_opportunity_rank": r.get("section_opportunity_rank", ""),
            "top_reason": r.get("top_reason", ""),
            "v300_main_force_score": r.get("v300_main_force_score", ""),
            "v300_main_force_raw": r.get("v300_main_force_raw", ""),
            "v300_attack_gate": r.get("v300_attack_gate", ""),
            "v300_chip_start_score": r.get("v300_chip_start_score", ""),
            "v300_turn_start_score": r.get("v300_turn_start_score", ""),
            "v300_volume_start_score": r.get("v300_volume_start_score", ""),
            "v300_breakout_early_score": r.get("v300_breakout_early_score", ""),
            "v300_overheat_penalty": r.get("v300_overheat_penalty", ""),
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
        "source": "v300_alpha_restore_top5_patch",
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
