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



# ===== v266.69 主力發動前夕濾網 / Final Ignition Filter =====
def _clip_series(s, lower=None, upper=None):
    s = pd.to_numeric(s, errors="coerce").fillna(0)
    if lower is not None:
        s = s.clip(lower=lower)
    if upper is not None:
        s = s.clip(upper=upper)
    return s


def apply_final_ignition_filter_v26669(d):
    """
    只補「主力發動前夕」辨識，不覆蓋原策略。

    目的：
    - 加分：收斂、低檔量縮後第一次放量、突破前籌碼/OBV轉強、第一根轉強K。
    - 扣分：爆量追高、離MA20過遠、已經漲太多、長上影假突破。

    產出欄位：
    - compression_score_v26669
    - first_volume_trigger_v26669
    - accumulation_score_v26669
    - first_power_k_v26669
    - fake_breakout_risk_v26669
    - ignition_bonus_v26669
    - ignition_state_v26669
    - ignition_note_v26669
    """
    d = d.copy()

    close = _clip_series(d.get("close", 0))
    open_ = _clip_series(d.get("open", close))
    high = _clip_series(d.get("high", close))
    low = _clip_series(d.get("low", close))
    ma5 = _clip_series(d.get("ma5", 0))
    ma10 = _clip_series(d.get("ma10", 0))
    ma20 = _clip_series(d.get("ma20", 0))
    ma60 = _clip_series(d.get("ma60", 0))
    vol_ratio = _clip_series(d.get("volume_ratio", 1))
    vol_dry = _clip_series(d.get("vol_dry_ratio", 1))
    ma_conv = _clip_series(d.get("ma_converge_pct", 1))
    range20 = _clip_series(d.get("range_20", 1))
    low_hold = _clip_series(d.get("low_non_down_count_5", 0))
    obv_mom5 = _clip_series(d.get("obv_mom5", 0))
    obv_up5 = _clip_series(d.get("obv_up_count_5", 0))
    mom5 = _clip_series(d.get("mom5", 0))
    mom10 = _clip_series(d.get("mom10", 0))
    mom20 = _clip_series(d.get("mom20", 0))
    high20 = _clip_series(d.get("high_20", close))
    high60 = _clip_series(d.get("high_60", close))

    # 1) 波動率壓縮 / 平台收斂
    compression = pd.Series(0.0, index=d.index)
    compression += (ma_conv <= 0.08).astype(int) * 16
    compression += ((ma_conv > 0.08) & (ma_conv <= 0.12)).astype(int) * 8
    compression += (range20 <= 0.18).astype(int) * 12
    compression += ((range20 > 0.18) & (range20 <= 0.24)).astype(int) * 6
    compression += (low_hold >= 3).astype(int) * 10
    compression += ((ma20 > 0) & close.between(ma20 * 0.97, ma20 * 1.06)).astype(int) * 12
    compression += ((vol_ratio >= 0.65) & (vol_ratio <= 1.25)).astype(int) * 6

    # 2) 低檔量縮後第一次溫和放量
    first_volume = pd.Series(0.0, index=d.index)
    first_volume += vol_ratio.between(1.20, 1.85).astype(int) * 22
    first_volume += vol_ratio.between(1.86, 2.50).astype(int) * 10
    first_volume += ((vol_dry <= 0.95) & vol_ratio.between(1.15, 2.30)).astype(int) * 8
    first_volume -= (vol_ratio > 2.80).astype(int) * 10
    first_volume -= (vol_ratio > 4.00).astype(int) * 20

    # 3) 主力/籌碼潛伏替代指標：OBV、低點不破、均線收斂
    accumulation = pd.Series(0.0, index=d.index)
    accumulation += (obv_mom5 > 0).astype(int) * 10
    accumulation += (obv_up5 >= 3).astype(int) * 8
    accumulation += (low_hold >= 3).astype(int) * 8
    accumulation += ((ma5 >= ma20 * 0.995) & (ma20 > 0)).astype(int) * 8
    accumulation += ((ma20 >= ma60 * 0.98) & (ma60 > 0)).astype(int) * 6
    accumulation += ((mom20 >= -0.03) & (mom20 <= 0.18)).astype(int) * 8

    # 若後續資料已有真正籌碼欄位，直接吃進來；沒有也不會炸。
    chip_candidates = [
        "chip_score", "chip_concentration_score", "chip_concentration",
        "chip_score_v26658", "chip_adjusted_score_v26658"
    ]
    chip = pd.Series(0.0, index=d.index)
    for c in chip_candidates:
        if c in d.columns:
            chip = _clip_series(d.get(c, 0))
            break
    accumulation += chip.between(20, 45).astype(int) * 14
    accumulation += chip.between(46, 60).astype(int) * 8
    accumulation -= (chip > 75).astype(int) * 12

    # 4) 第一根轉強K：剛站回、還沒離均線太遠、量溫和
    first_power = (
        (close > ma5)
        & (ma5 >= ma10 * 0.995)
        & (ma10 >= ma20 * 0.985)
        & (close <= ma20 * 1.08)
        & vol_ratio.between(1.15, 2.60)
        & (mom5 > 0)
        & (mom20 <= 0.30)
    )

    # 5) 假突破 / 追高風險
    body = (close - open_).abs().replace(0, np.nan)
    upper_shadow_ratio = ((high - np.maximum(close, open_)) / body).replace([np.inf, -np.inf], 0).fillna(0)
    fake_risk = pd.Series(0.0, index=d.index)
    fake_risk += (upper_shadow_ratio >= 1.6).astype(int) * 18
    fake_risk += ((ma20 > 0) & (close > ma20 * 1.12)).astype(int) * 18
    fake_risk += ((ma20 > 0) & (close > ma20 * 1.18)).astype(int) * 28
    fake_risk += (vol_ratio > 3.5).astype(int) * 14
    fake_risk += (vol_ratio > 5.0).astype(int) * 24
    fake_risk += (mom20 > 0.35).astype(int) * 14
    fake_risk += ((high60 > 0) & (close >= high60 * 0.995) & (mom20 > 0.25)).astype(int) * 10
    fake_risk += ((high20 > 0) & (close >= high20 * 0.995) & (vol_ratio > 2.8)).astype(int) * 8

    # 綜合 bonus：最大目標不是拉高所有強勢股，而是把「前夜」排出來。
    raw_bonus = (
        compression * 0.25
        + first_volume * 0.28
        + accumulation * 0.22
        + first_power.astype(int) * 16
        - fake_risk * 0.55
    )
    ignition_bonus = raw_bonus.clip(lower=-22, upper=24).round(2)

    d["compression_score_v26669"] = compression.round(2)
    d["first_volume_trigger_v26669"] = first_volume.round(2)
    d["accumulation_score_v26669"] = accumulation.round(2)
    d["first_power_k_v26669"] = first_power.astype(int)
    d["fake_breakout_risk_v26669"] = fake_risk.round(2)
    d["ignition_bonus_v26669"] = ignition_bonus

    # 僅調整 entry_score，不改原欄位邏輯與輸出結構。
    d["entry_score"] = (pd.to_numeric(d["entry_score"], errors="coerce").fillna(0) + ignition_bonus).round(2)

    cond_ready = (ignition_bonus >= 16) & (fake_risk <= 18)
    cond_turning = (ignition_bonus >= 8) & (fake_risk <= 25)
    cond_risk = fake_risk >= 32

    d["ignition_state_v26669"] = np.select(
        [cond_ready, cond_turning, cond_risk],
        ["主力剛準備發動", "主力轉強中", "假突破風險"],
        default="一般"
    )

    d["ignition_note_v26669"] = np.select(
        [cond_ready, cond_turning, cond_risk],
        [
            "主力收斂完成｜低檔量縮後轉強｜可優先觀察第一根K",
            "收斂轉強中｜等待放量確認｜不可追高",
            "爆量/乖離/上影風險｜避免追價"
        ],
        default="條件未到臨界點｜維持原策略判斷"
    )

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

    # v266.69：只補主力發動前夕濾網，不動其他策略架構
    d = apply_final_ignition_filter_v26669(d)

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
        + "｜" + d.get("ignition_note_v26669", "").astype(str)
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

    # v266.69：只補主力發動前夕濾網，不動其他策略架構
    d = apply_final_ignition_filter_v26669(d)

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
        + "｜" + d.get("ignition_note_v26669", "").astype(str)
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
            "compression_score_v26669": round(float(r.get("compression_score_v26669", 0)), 2),
            "first_volume_trigger_v26669": round(float(r.get("first_volume_trigger_v26669", 0)), 2),
            "accumulation_score_v26669": round(float(r.get("accumulation_score_v26669", 0)), 2),
            "first_power_k_v26669": int(float(r.get("first_power_k_v26669", 0))),
            "fake_breakout_risk_v26669": round(float(r.get("fake_breakout_risk_v26669", 0)), 2),
            "ignition_bonus_v26669": round(float(r.get("ignition_bonus_v26669", 0)), 2),
            "ignition_state_v26669": r.get("ignition_state_v26669", ""),
            "ignition_note_v26669": r.get("ignition_note_v26669", ""),
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
        "source": "v266_9_strategy_engine_stable_v26669_ignition_filter",
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
