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


# ===== v306.9 IGNITION RELAX PATCH =====
# 只放寬 ignition / evolution 前夕條件：
# - 放寬均線收斂
# - 放寬波動壓縮
# - 放寬第一次放量
# - 放寬 mom20 區間
# 不重寫策略，不動 TOP5，不動 app.js，不動排序。

INITIAL_CAPITAL = 1_000_000

# ===== v306.2 Finance Exclude + Industry Tag Safe Patch =====
# 只做兩件事：
# 1. 從候選池源頭排除金融股 28xx / 58xx，避免金融股進 TEST TOP。
# 2. 輸出 industry / industry_tag 欄位供 app.js 顯示。
FINANCE_PREFIXES_V306 = ("28", "58")

INDUSTRY_EXACT_V306 = {
    "2330": "半導體", "2303": "半導體", "2344": "半導體",
    "3034": "IC", "3443": "IC", "2379": "IC",
    "2317": "AI", "2382": "AI", "3231": "AI", "6669": "AI",
    "2603": "航運", "2609": "航運", "2615": "航運", "2636": "航運", "2610": "航空",
    "6179": "通訊", "6189": "零組件",
    "5876": "金融", "2820": "金融", "2852": "金融",
    "2890": "金融", "2891": "金融",
    "2880": "金融", "2881": "金融", "2882": "金融", "2883": "金融",
    "2884": "金融", "2885": "金融", "2886": "金融", "2887": "金融",
    "2888": "金融", "2889": "金融",
    "6585": "重電", "1513": "重電", "1514": "重電", "1605": "電纜",
    "2368": "PCB", "2367": "PCB", "3037": "PCB", "8046": "PCB",
    "2498": "電子", "2753": "觀光",
}

def is_finance_stock_v306(stock_id):
    sid = str(stock_id).strip()[:4]
    return sid.startswith(FINANCE_PREFIXES_V306)

def industry_tag_v306(stock_id):
    sid = str(stock_id).strip()[:4]
    if sid in INDUSTRY_EXACT_V306:
        return INDUSTRY_EXACT_V306[sid]
    if sid.startswith(("28", "58")):
        return "金融"
    if sid.startswith("26"):
        return "航運"
    if sid.startswith(("15", "16")):
        return "機電"
    if sid.startswith(("23", "24", "30", "34", "61", "62", "65")):
        return "電子"
    if sid.startswith("27"):
        return "觀光"
    return "其他"

def apply_industry_tag_v306(df):
    try:
        if df is None or df.empty or "stock_id" not in df.columns:
            return df
        df = df.copy()
        df["industry"] = df["stock_id"].astype(str).map(industry_tag_v306)
        df["industry_tag"] = df["industry"]
        return df
    except Exception:
        return df

def exclude_finance_candidates_v306(df):
    try:
        if df is None or df.empty or "stock_id" not in df.columns:
            return df
        df = df.copy()
        return df[~df["stock_id"].astype(str).str.slice(0, 4).apply(is_finance_stock_v306)].copy()
    except Exception:
        return df


# ===== v305 金融股排除 / 避免低波金融股污染 TEST / TOP =====
FINANCE_PREFIXES_V305 = ("28", "58")

def is_finance_stock_v305(stock_id):
    sid = str(stock_id).strip()
    return sid.startswith(FINANCE_PREFIXES_V305)


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
    df = exclude_finance_candidates_v306(df)
    df = apply_industry_tag_v306(df)
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

    # v305：金融股排除，只在策略候選池源頭排除 28xx / 58xx。
    # 目的：避免低波金融股靠高流動性擠進 TEST / TOP，不動後面 pipeline / UI。
    x = x[~x["stock_id"].astype(str).apply(is_finance_stock_v305)].copy()

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
    compression += (ma_conv <= 0.12).astype(int) * 16
    compression += ((ma_conv > 0.12) & (ma_conv <= 0.18)).astype(int) * 8
    compression += (range20 <= 0.24).astype(int) * 12
    compression += ((range20 > 0.24) & (range20 <= 0.32)).astype(int) * 6
    compression += (low_hold >= 3).astype(int) * 10
    compression += ((ma20 > 0) & close.between(ma20 * 0.97, ma20 * 1.06)).astype(int) * 12
    compression += ((vol_ratio >= 0.65) & (vol_ratio <= 1.25)).astype(int) * 6

    # 2) 低檔量縮後第一次溫和放量
    first_volume = pd.Series(0.0, index=d.index)
    first_volume += vol_ratio.between(1.05, 2.10).astype(int) * 22
    first_volume += vol_ratio.between(2.11, 3.00).astype(int) * 10
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
    accumulation += ((mom20 >= -0.08) & (mom20 <= 0.45)).astype(int) * 8

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
        & (close <= ma20 * 1.12)
        & vol_ratio.between(1.00, 3.20)
        & (mom5 > 0)
        & (mom20 <= 0.45)
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



# ===== v300 MAIN-FORCE GATE + CHIP CONFIRM PATCH =====
# 使用早期乾淨版為基底，只補主力入口，不重寫原策略。
# 核心：流動性合格 → 籌碼/OBV開始集中 → 剛轉強 → 量能啟動 → 未過熱。
# 目的：阻擋防禦股、牛皮股、低攻擊性高流動股污染 TEST / WATCH。

def _v300_num(d, col, default=0.0):
    if col in d.columns:
        return pd.to_numeric(d[col], errors="coerce").fillna(default)
    return pd.Series(default, index=d.index, dtype="float64")


def _v300_band(x, low, sweet_low, sweet_high, high):
    x = pd.to_numeric(x, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    left = ((x - low) / max(sweet_low - low, 1e-9)).clip(0, 1)
    right = ((high - x) / max(high - sweet_high, 1e-9)).clip(0, 1)
    return np.minimum(left, right)


def merge_chip_source_v300(d):
    d = d.copy()
    if "stock_id" not in d.columns:
        return d

    chip_paths = [
        Path("chip_source_twse.csv"),
        Path("mobile_dashboard_v1/data/chip_source_twse.csv"),
    ]

    chip = None
    for p in chip_paths:
        if p.exists():
            try:
                chip = pd.read_csv(p, encoding="utf-8-sig")
                if not chip.empty:
                    break
            except Exception:
                try:
                    chip = pd.read_csv(p)
                    if not chip.empty:
                        break
                except Exception:
                    chip = None

    if chip is None or chip.empty:
        for c in [
            "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
            "inst_buy_days", "inst_valid", "margin_balance_change", "short_balance_change"
        ]:
            if c not in d.columns:
                d[c] = 0
        d["chip_source_valid_v300"] = 0
        return d

    chip["stock_id"] = chip["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False)
    keep = [
        "stock_id", "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid", "margin_balance_change", "short_balance_change"
    ]
    keep = [c for c in keep if c in chip.columns]
    chip = chip[keep].drop_duplicates("stock_id", keep="last")

    d["stock_id"] = d["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False)
    d = d.merge(chip, on="stock_id", how="left", suffixes=("", "_chip"))

    for c in [
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid", "margin_balance_change", "short_balance_change"
    ]:
        if c not in d.columns:
            d[c] = 0
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)

    d["chip_source_valid_v300"] = (pd.to_numeric(d.get("inst_valid", 0), errors="coerce").fillna(0) >= 1).astype(int)
    return d


def apply_main_force_gate_v300(d, mode="ALPHA"):
    d = merge_chip_source_v300(d)
    mode = str(mode or "ALPHA").upper()

    close = _v300_num(d, "close", 0)
    high = _v300_num(d, "high", close)
    open_ = _v300_num(d, "open", close)
    volume = _v300_num(d, "volume", 0)
    turnover = _v300_num(d, "turnover", close * volume * 1000)
    volume_ratio = _v300_num(d, "volume_ratio", 1)

    mom5 = _v300_num(d, "mom5", 0)
    mom10 = _v300_num(d, "mom10", 0)
    mom20 = _v300_num(d, "mom20", 0)

    ma5 = _v300_num(d, "ma5", close)
    ma10 = _v300_num(d, "ma10", close)
    ma20 = _v300_num(d, "ma20", close)
    ma60 = _v300_num(d, "ma60", close)

    high20 = _v300_num(d, "high_20", close)
    high60 = _v300_num(d, "high_60", close)

    obv_mom5 = _v300_num(d, "obv_mom5", 0)
    obv_up5 = _v300_num(d, "obv_up_count_5", 0)
    low_hold = _v300_num(d, "low_non_down_count_5", 0)

    foreign = _v300_num(d, "foreign_net_buy", 0)
    trust = _v300_num(d, "trust_net_buy", 0)
    dealer = _v300_num(d, "dealer_net_buy", 0)
    inst = _v300_num(d, "inst_net_buy", 0)
    inst_valid = _v300_num(d, "inst_valid", 0)

    liq_level = d["liquidity_level"].astype(str).str.upper() if "liquidity_level" in d.columns else pd.Series("", index=d.index)

    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    # 流動性門票：不要 500 張內，不要成交金額過小。
    if mode == "ALPHA":
        liquidity_gate = (volume >= 2000) | (turnover >= 60_000_000) | liq_level.eq("HIGH")
    else:
        liquidity_gate = (volume >= 1000) | (turnover >= 30_000_000) | liq_level.isin(["MEDIUM", "HIGH"])

    # 籌碼/主力痕跡：每日法人資料優先；若當天資料缺，才用 OBV/低點墊高做代理。
    chip_buy = (
        (inst_valid >= 1) &
        ((inst > 0) | (foreign > 0) | (trust > 0))
    )

    obv_accum = (
        (obv_mom5 > 0) &
        (obv_up5 >= 2) &
        (low_hold >= 3)
    )

    main_force_trace = chip_buy | obv_accum

    # 攻擊結構：防禦股/牛皮股會被擋掉。
    attack_structure = (
        (close >= 15) &
        (close > ma20 * 0.985) &
        (ma5 >= ma10 * 0.985) &
        (ma10 >= ma20 * 0.965) &
        (ma20 >= ma60 * 0.94) &
        (mom10 > 0) &
        (mom20 > 0.025) &
        (close >= high60 * 0.86)
    )

    # 量能啟動：不追爆量末端。
    volume_start = volume_ratio.between(1.05, 4.2)

    # 未過熱 / 非出貨。
    not_overheat = (
        (mom20 <= 0.42) &
        (ma20_gap <= 0.24) &
        (volume_ratio <= 5.2) &
        ~((upper_shadow > 0.06) & (volume_ratio > 1.6)) &
        ~((intraday < -0.03) & (volume_ratio > 1.8))
    )

    main_force_gate = liquidity_gate & main_force_trace & attack_structure & volume_start & not_overheat

    # TOP 分數只用來標示與排序，不覆蓋原 entry_score。
    chip_score = (
        chip_buy.astype(int) * 35 +
        obv_accum.astype(int) * 25 +
        (trust > 0).astype(int) * 15 +
        (foreign > 0).astype(int) * 10 +
        _v300_band(volume_ratio, 1.0, 1.15, 2.8, 4.5) * 15 +
        _v300_band(mom20, 0.02, 0.05, 0.24, 0.42) * 12
    )

    d["main_force_gate_v300"] = main_force_gate.astype(int)
    d["main_force_score_v300"] = pd.Series(chip_score, index=d.index).round(1)
    d["chip_buy_v300"] = chip_buy.astype(int)
    d["obv_accum_v300"] = obv_accum.astype(int)
    d["attack_structure_v300"] = attack_structure.astype(int)
    d["volume_start_v300"] = volume_start.astype(int)
    d["not_overheat_v300"] = not_overheat.astype(int)

    d["top_opportunity"] = ""
    d["section_top_opportunity"] = ""
    d["opportunity_rank"] = ""
    d["top_reason"] = ""
    d["top_rank_v3066"] = 9999
    d["is_top_v3066"] = 0

    action = d["action"].astype(str).str.upper() if "action" in d.columns else pd.Series("", index=d.index)

    for label, mask in [
        ("TOP5_TEST", action.isin(["BUY", "TEST"]) & main_force_gate),
        ("TOP5_WATCH", action.eq("WATCH") & main_force_gate),
    ]:
        idx = (
            d.loc[mask]
            .sort_values(["main_force_score_v300", "entry_score", "mom20", "stock_id"], ascending=[False, False, False, True])
            .head(5)
            .index
        )
        if len(idx):
            ranks = [str(i) for i in range(1, len(idx) + 1)]
            d.loc[idx, "top_opportunity"] = [f"🔥TOP{i}" for i in range(1, len(idx) + 1)]
            d.loc[idx, "section_top_opportunity"] = [f"TOP{i}_" + label.split("_", 1)[-1] for i in range(1, len(idx) + 1)]
            d.loc[idx, "opportunity_rank"] = ranks
            d.loc[idx, "top_rank_v3066"] = [i for i in range(1, len(idx) + 1)]
            d.loc[idx, "is_top_v3066"] = 1
            d.loc[idx, "top_reason"] = "主力痕跡｜籌碼/OBV開始集中｜剛轉強｜量能啟動｜未過熱"

    return d


# ===== v306.6 TOP5 HARD ORDER / TOP5 硬排序 =====
def _top_rank_hard_v3066(row):
    """TOP1~TOP5 must stay above normal candidates. Lower is better."""
    try:
        if isinstance(row, pd.Series):
            vals = row.to_dict()
        else:
            vals = row if isinstance(row, dict) else {}
        for key in ["opportunity_rank", "section_opportunity_rank", "top_rank", "top_rank_v3066"]:
            v = vals.get(key, "")
            s = str(v).strip()
            if s and s not in ["--", "nan", "NaN", "None"]:
                m = re.search(r"(\d+)", s)
                if m:
                    return int(m.group(1))
        txt = " ".join(str(vals.get(k, "")) for k in [
            "section_top_opportunity", "top_opportunity", "execution_flag", "system_note", "note", "reason"
        ])
        m = re.search(r"TOP\s*([1-9]\d*)", txt, re.I)
        if m:
            return int(m.group(1))
        if "TOP" in txt.upper():
            return 99
    except Exception:
        pass
    return 9999

def _top_sort_columns_v3066(d):
    d = d.copy()
    d["top_rank_v3066"] = d.apply(_top_rank_hard_v3066, axis=1)
    d["is_top_v3066"] = (d["top_rank_v3066"] < 9999).astype(int)
    return d

def sort_candidates_top_first_v3066(d):
    if d is None or d.empty:
        return d
    d = _top_sort_columns_v3066(d)
    cols = ["is_top_v3066", "top_rank_v3066", "entry_score", "main_force_score_v300", "liquidity_score", "mom20", "mom10", "stock_id"]
    cols = [c for c in cols if c in d.columns]
    asc = []
    for c in cols:
        if c == "is_top_v3066": asc.append(False)
        elif c == "top_rank_v3066": asc.append(True)
        elif c == "stock_id": asc.append(True)
        else: asc.append(False)
    return d.sort_values(cols, ascending=asc)

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
    # v300：只補主力入口 Gate，不重寫原策略分數。
    d = apply_main_force_gate_v300(d, mode="CORE")

    core_liq_ok = (d["volume"] >= 1000) & d["liquidity_level"].isin(["MEDIUM", "HIGH"])
    low_liq = d["liquidity_level"].eq("LOW")
    main_force_ok = d["main_force_gate_v300"].eq(1)

    buy = (
        (d["entry_score"] >= 58)
        & (d["mom20"] > 0.05)
        & (d["close"] > d["ma20"])
        & (d["close"] >= 20)
        & core_liq_ok
        & main_force_ok
    )

    # 低流動性即使分數夠，也只允許試單，避免你資金被卡住。
    test = (
        (d["entry_score"] >= 44)
        & ~buy
        & (d["mom10"] > 0.01)
        & (d["close"] > d["ma20"] * 0.97)
        & (d["volume"] >= 1000)
        & main_force_ok
    )

    watch = (d["entry_score"] >= 34) & ~buy & ~test & main_force_ok

    set_action(d, buy, test, watch, "早期卡位", "低量試單", "早期觀察")

    d["note"] = (
        "CORE早期卡位｜剛轉強｜靠近MA20｜量能回溫｜"
        + d["liquidity_tag"].astype(str)
        + "｜" + d.get("ignition_note_v26669", "").astype(str)
    )

    return sort_candidates_top_first_v3066(d)


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
    # v300：只補主力入口 Gate，不重寫原策略分數。
    d = apply_main_force_gate_v300(d, mode="ALPHA")
    main_force_ok = d["main_force_gate_v300"].eq(1)

    buy = (
        (d["entry_score"] >= 70)
        & high_liq
        & (d["close"] > d["ma20"])
        & (d["ma20"] > d["ma60"])
        & (d["mom10"] > 0.03)
        & (d["volume_ratio"] >= 1.25)
        & main_force_ok
    )

    test = (
        (d["entry_score"] >= 58)
        & ~buy
        & mid_or_high
        & (d["close"] > d["ma20"])
        & (d["mom5"] > 0)
        & main_force_ok
    )

    watch = (d["entry_score"] >= 46) & ~buy & ~test & main_force_ok

    set_action(d, buy, test, watch, "高流動性強勢買進", "強勢試單", "高流動性觀察")

    d["note"] = (
        "ALPHA高流動性強勢延續｜成交量/成交金額優先｜突破/趨勢確認｜"
        + d["liquidity_tag"].astype(str)
        + "｜" + d.get("ignition_note_v26669", "").astype(str)
    )

    return sort_candidates_top_first_v3066(d)


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

    # v306.6：TOP1~TOP5 是硬規則；同 action 內 TOP 永遠排最上面，再比策略/分數/流動性。
    s = _top_sort_columns_v3066(s)
    s["priority"] = np.where(s["strategy_type"] == "ALPHA", 1, 2)
    sort_cols = ["action", "is_top_v3066", "top_rank_v3066", "priority", "entry_score", "liquidity_score"]
    action_order = {"BUY": 1, "TEST": 2, "WATCH": 3, "BLOCK": 4}
    s["action_order_v3066"] = s["action"].astype(str).str.upper().map(action_order).fillna(9)
    s = (
        s.sort_values(["action_order_v3066", "is_top_v3066", "top_rank_v3066", "priority", "entry_score", "liquidity_score"],
                      ascending=[True, False, True, True, False, False])
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
            "top_opportunity": r.get("top_opportunity", ""),
            "section_top_opportunity": r.get("section_top_opportunity", ""),
            "opportunity_rank": r.get("opportunity_rank", ""),
            "top_reason": r.get("top_reason", ""),
            "top_rank_v3066": r.get("top_rank_v3066", ""),
            "is_top_v3066": r.get("is_top_v3066", ""),
            "main_force_gate_v300": r.get("main_force_gate_v300", ""),
            "main_force_score_v300": r.get("main_force_score_v300", ""),
            "chip_buy_v300": r.get("chip_buy_v300", ""),
            "obv_accum_v300": r.get("obv_accum_v300", ""),
            "attack_structure_v300": r.get("attack_structure_v300", ""),
            "volume_start_v300": r.get("volume_start_v300", ""),
            "not_overheat_v300": r.get("not_overheat_v300", ""),
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
