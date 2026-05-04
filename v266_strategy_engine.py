"""
v266_strategy_engine.py
v266.33 策略進化版：CORE + ALPHA + IGNITION + EVOLUTION

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


def ignition_engine(x):
    """
    IGNITION：起漲 / 啟動節奏策略。
    目的：抓「低檔盤整 → 均線糾結 → 放量突破 → 準備啟動」的股票。
    只做獨立清單，不直接改動原本 final 操作邏輯，避免影響已穩定的主策略。
    """
    d = x.copy()
    d["strategy_type"] = "IGNITION"
    d["strategy_name"] = "IGNITION 起漲啟動"
    d["entry_score"] = 0.0

    mid_or_high = d["liquidity_level"].isin(["MEDIUM", "HIGH"])
    high_liq = d["liquidity_level"].eq("HIGH")

    # 1) 低檔 / 盤整 / 均線糾結
    d["entry_score"] += (d["ma_converge_pct"] <= 0.08).astype(int) * 14
    d["entry_score"] += (d["ma_converge_pct"] <= 0.12).astype(int) * 6
    d["entry_score"] += (d["range_20"] <= 0.22).astype(int) * 8
    d["entry_score"] += (d["close"] >= d["ma20"] * 0.98).astype(int) * 10
    d["entry_score"] += (d["close"] >= d["ma60"] * 0.95).astype(int) * 6

    # 2) 啟動 / 突破 / 量能回溫
    d["entry_score"] += (d["close"] > d["ma20"]).astype(int) * 10
    d["entry_score"] += (d["ma5"] >= d["ma20"] * 0.995).astype(int) * 8
    d["entry_score"] += (d["ma20_slope"] >= 0).astype(int) * 8
    d["entry_score"] += d["volume_ratio"].between(1.20, 4.80).astype(int) * 14
    d["entry_score"] += (d["volume"] >= 1000).astype(int) * 8
    d["entry_score"] += (d["volume"] >= 3000).astype(int) * 5

    # 3) 動能剛轉強，不追極端過熱
    d["entry_score"] += (d["mom5"] > 0).astype(int) * 8
    d["entry_score"] += (d["mom10"] > 0.01).astype(int) * 8
    d["entry_score"] += d["mom20"].between(-0.05, 0.22).astype(int) * 8
    d["entry_score"] += (d["close"] >= d["high_20"] * 0.965).astype(int) * 8
    d["entry_score"] += (d["low_non_down_count_5"] >= 3).astype(int) * 6

    # 4) KD / MACD / OBV 輔助
    d["entry_score"] += (d["kd_cross"] > 0).astype(int) * 6
    d["entry_score"] += (d["macd_cross"] > 0).astype(int) * 6
    d["entry_score"] += (d["macd_diff"] > 0).astype(int) * 5
    d["entry_score"] += (d["obv_mom5"] > 0).astype(int) * 5

    # 5) 流動性與風險控管
    d["entry_score"] += mid_or_high.astype(int) * 10
    d["entry_score"] += high_liq.astype(int) * 5

    d["entry_score"] -= (d["close"] < 15).astype(int) * 18
    d["entry_score"] -= (d["volume"] < 800).astype(int) * 22
    d["entry_score"] -= (d["liquidity_level"].eq("LOW")).astype(int) * 14
    d["entry_score"] -= (d["mom20"] > 0.35).astype(int) * 14
    d["entry_score"] -= (d["volume_ratio"] > 6.5).astype(int) * 10

    test = (
        (d["entry_score"] >= 64)
        & (d["close"] > d["ma20"] * 0.995)
        & (d["volume"] >= 1000)
        & mid_or_high
    )

    watch = (d["entry_score"] >= 50) & ~test

    # 起漲清單以 TEST / WATCH 呈現，不直接 BUY，避免假突破重倉。
    set_action(d, False, test, watch, "", "起漲試單", "起漲觀察")

    d["section_opportunity_rank"] = d["entry_score"].rank(method="first", ascending=False).astype(int)
    d["section_top_opportunity"] = np.where(
        d["section_opportunity_rank"] <= 5,
        "起漲TOP" + d["section_opportunity_rank"].astype(str),
        ""
    )

    d["ignition_phase"] = np.select(
        [
            (d["entry_score"] >= 72),
            (d["entry_score"] >= 64),
            (d["entry_score"] >= 50),
        ],
        ["啟動確認", "突破觀察", "預備布局"],
        default="條件不足"
    )

    d["note"] = (
        "IGNITION起漲啟動｜均線糾結/收斂｜放量轉強｜突破節奏｜"
        + d["ignition_phase"].astype(str)
        + "｜"
        + d["liquidity_tag"].astype(str)
    )
    d["reason"] = d["note"]
    d["system_note"] = "起漲清單：適合小量試單或優先觀察，不建議一次重倉。"

    return d.sort_values(["entry_score", "liquidity_score", "volume_ratio"], ascending=False)


def evolution_engine(core, alpha, ignition):
    """
    v266.33 EVOLUTION：策略進化鏈。
    目的：把 IGNITION → TEST → ALPHA → CORE 的升級路徑獨立成一張清單。
    不直接改變原本 final trade_plan；只提供決策提示與優先級。
    """
    parts = []

    def base_take(df, phase, promote_label, min_score=0, action_filter=None, n=40):
        if df is None or df.empty:
            return pd.DataFrame()
        d = df.copy()
        if action_filter is not None and 'action' in d.columns:
            d = d[d['action'].astype(str).str.upper().isin(action_filter)].copy()
        d = d[pd.to_numeric(d.get('entry_score', 0), errors='coerce').fillna(0) >= min_score].copy()
        if d.empty:
            return d
        d['evolution_phase'] = phase
        d['promote_label'] = promote_label
        d['strategy_type'] = d.get('strategy_type', phase.split('→')[-1])
        d['strategy_name'] = d.get('strategy_name', promote_label)
        return d.head(n)

    # 1) IGNITION → TEST：起漲條件已到位，適合小量試單。
    p1 = base_take(
        ignition,
        'IGNITION→TEST',
        '起漲轉試單',
        min_score=64,
        action_filter=['TEST'],
        n=30,
    )
    if not p1.empty:
        p1['evolution_score'] = pd.to_numeric(p1['entry_score'], errors='coerce').fillna(0) + 8
        p1['final_action'] = 'TEST'
        p1['action'] = 'TEST'
        p1['action_label'] = '試單'
        p1['action_sub'] = '起漲確認，允許小量試單'
        parts.append(p1)

    # 2) TEST → ALPHA：強勢動能與流動性更完整，可升級成主升段候選。
    p2 = base_take(
        alpha,
        'TEST→ALPHA',
        '試單轉強勢',
        min_score=58,
        action_filter=['TEST', 'BUY'],
        n=30,
    )
    if not p2.empty:
        p2['evolution_score'] = pd.to_numeric(p2['entry_score'], errors='coerce').fillna(0) + 14
        p2['final_action'] = np.where(p2['action'].astype(str).str.upper().eq('BUY'), 'BUY', 'TEST')
        p2['action'] = p2['final_action']
        p2['action_label'] = np.where(p2['final_action'].eq('BUY'), '買進', '試單')
        p2['action_sub'] = np.where(p2['final_action'].eq('BUY'), '強勢確認，可列主升段', '強勢試單，等待確認')
        parts.append(p2)

    # 3) ALPHA → CORE：趨勢站穩，進入核心持有/穩定觀察池。
    p3 = base_take(
        core,
        'ALPHA→CORE',
        '強勢轉核心',
        min_score=62,
        action_filter=['BUY', 'TEST', 'WATCH'],
        n=25,
    )
    if not p3.empty:
        p3['evolution_score'] = pd.to_numeric(p3['entry_score'], errors='coerce').fillna(0) + 10
        p3['final_action'] = p3['action'].astype(str).str.upper().replace({'WATCH':'WATCH', 'TEST':'TEST', 'BUY':'BUY'})
        p3['action'] = p3['final_action']
        p3['action_label'] = p3['final_action'].map({'BUY':'買進','TEST':'試單','WATCH':'觀察'}).fillna('觀察')
        p3['action_sub'] = '趨勢穩定，進入核心觀察/持有池'
        parts.append(p3)

    if not parts:
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True, sort=False)
    out['stock_id'] = out['stock_id'].astype(str).str.zfill(4)
    out['evolution_score'] = pd.to_numeric(out['evolution_score'], errors='coerce').fillna(pd.to_numeric(out.get('entry_score', 0), errors='coerce').fillna(0))

    # 同一檔只保留最高進化分數，避免重複洗版。
    phase_priority = {'TEST→ALPHA': 1, 'ALPHA→CORE': 2, 'IGNITION→TEST': 3}
    out['evolution_priority'] = out['evolution_phase'].map(phase_priority).fillna(9)
    out = (
        out.sort_values(['evolution_priority', 'evolution_score', 'liquidity_score'], ascending=[True, False, False])
           .drop_duplicates('stock_id')
           .head(80)
           .copy()
    )

    out['section_opportunity_rank'] = out['evolution_score'].rank(method='first', ascending=False).astype(int)
    out['section_top_opportunity'] = np.where(
        out['section_opportunity_rank'] <= 5,
        '進化TOP' + out['section_opportunity_rank'].astype(str),
        ''
    )
    out['source'] = 'EVOLUTION'
    out['bucket'] = 'EVOLUTION'
    out['strategy_type'] = 'EVOLUTION'
    out['strategy_name'] = 'EVOLUTION 策略進化鏈'
    out['reason'] = (
        '策略進化鏈｜' + out['evolution_phase'].astype(str) + '｜' +
        out.get('note', '').astype(str)
    )
    out['system_note'] = np.select(
        [
            out['evolution_phase'].eq('IGNITION→TEST'),
            out['evolution_phase'].eq('TEST→ALPHA'),
            out['evolution_phase'].eq('ALPHA→CORE'),
        ],
        [
            '起漲已成形，僅適合小量試單，不建議重倉。',
            '試單轉強勢，可優先追蹤是否進入主升段。',
            '強勢轉穩定，適合放入核心持有/觀察池。',
        ],
        default='策略進化提示：依分數與流動性分批確認。'
    )
    out['entry_type'] = out['evolution_phase']
    out['execution_flag'] = out['section_top_opportunity']
    out['score'] = out['evolution_score'].round(2)
    return out

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
    ignition = ignition_engine(latest).head(80)
    evolution = evolution_engine(core, alpha, ignition).head(80)

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
        "ignition_count": len(ignition),
        "ignition_test_count": int((ignition.action == "TEST").sum()),
        "ignition_watch_count": int((ignition.action == "WATCH").sum()),
        "evolution_count": len(evolution),
        "evolution_test_count": int((evolution.action == "TEST").sum()) if not evolution.empty else 0,
        "evolution_buy_count": int((evolution.action == "BUY").sum()) if not evolution.empty else 0,
        "evolution_count": len(evolution),
        "evolution_test_count": int((evolution.action == "TEST").sum()) if not evolution.empty else 0,
        "evolution_buy_count": int((evolution.action == "BUY").sum()) if not evolution.empty else 0,
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
        ignition.assign(engine="IGNITION"),
        evolution.assign(engine="EVOLUTION"),
    ], ignore_index=True)

    write_both(core, "core_candidates.csv")
    write_both(alpha, "alpha_candidates.csv")
    write_both(ignition, "ignition_candidates.csv")
    write_both(evolution, "strategy_evolution.csv")
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
        "ignition_count": len(ignition),
        "ignition_test_count": int((ignition.action == "TEST").sum()),
        "ignition_watch_count": int((ignition.action == "WATCH").sum()),
        "dual_strategy": {
            "CORE": "早期卡位 / 1000張以上小倉",
            "ALPHA": "高流動性強勢延續 / 3000張以上主力倉位",
            "IGNITION": "起漲啟動 / 均線糾結放量突破 / 小量試單",
            "EVOLUTION": "策略進化鏈 / IGNITION→TEST→ALPHA→CORE / 升級提示",
        },
    }

    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        with open(p, "w", encoding="utf-8-sig") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
