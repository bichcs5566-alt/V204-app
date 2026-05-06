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




# ===== v266.57.5 20/40/60 結構分數測試層（append-only，不改原本策略核心） =====
# 原則：
# 1. 先讓原本 main() + v266.57.2 續強提示完整跑完。
# 2. 只補 20/40/60 日結構欄位到既有輸出 CSV。
# 3. 不改 CORE / ALPHA / TEST / WATCH 條件。
# 4. 不改 entry_score、不改 action、不改資金配置、不改持倉。
def _read_json_v266575(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}


def _safe_float_v266575(v, default=np.nan):
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default


def _pct_range_v266575(high_s, low_s, ref):
    hi = pd.to_numeric(high_s, errors="coerce").max()
    lo = pd.to_numeric(low_s, errors="coerce").min()
    ref = _safe_float_v266575(ref)
    if not np.isfinite(hi) or not np.isfinite(lo) or not np.isfinite(ref) or ref <= 0:
        return np.nan
    return (hi - lo) / ref


def _volume_mild_expand_v266575(last_vol, recent_median):
    last_vol = _safe_float_v266575(last_vol)
    recent_median = _safe_float_v266575(recent_median)
    if not np.isfinite(last_vol) or not np.isfinite(recent_median) or recent_median <= 0:
        return False
    return 1.05 <= (last_vol / recent_median) <= 2.8


def _load_market_context_v266575():
    meta = {}
    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        meta.update(_read_json_v266575(p))

    macro = {}
    for p in [ROOT / "macro_regime.json", DATA_DIR / "macro_regime.json"]:
        macro.update(_read_json_v266575(p))

    market_regime = str(
        meta.get("market_regime")
        or meta.get("regime")
        or meta.get("market_label")
        or ""
    ).upper()

    macro_label = str(
        macro.get("macro_label")
        or macro.get("macro_regime")
        or meta.get("macro_label")
        or ""
    )

    # 權重只影響「結構提示分數」，不影響原策略進出。
    if "TREND" in market_regime or "BULL" in market_regime or "多" in market_regime:
        weights = {"20": 0.45, "40": 0.30, "60": 0.25}
        env = "多頭／趨勢盤：偏重20日主升延續"
    elif "BEAR" in market_regime or "空" in market_regime or "弱" in market_regime:
        weights = {"20": 0.25, "40": 0.30, "60": 0.45}
        env = "弱勢／空頭盤：偏重60日大結構防守"
    else:
        weights = {"20": 0.30, "40": 0.40, "60": 0.30}
        env = "盤整盤：偏重40日平台整理"

    macro_adj = 0.0
    if any(k in macro_label for k in ["偏多", "RISK_ON", "多頭"]):
        macro_adj = 0.5
        macro_note = "總經偏多：結構分數小幅加權"
    elif any(k in macro_label for k in ["偏空", "RISK_OFF", "空頭"]):
        macro_adj = -0.8
        macro_note = "總經偏空：結構分數小幅保守"
    else:
        macro_note = "總經中性或資料不足：不額外加權"

    return {
        "market_regime": market_regime or "--",
        "macro_label": macro_label or "--",
        "weights": weights,
        "macro_adj": macro_adj,
        "env_note": env,
        "macro_note": macro_note,
    }


def _calc_structure_map_v266575():
    try:
        df = load_feature()
    except Exception as e:
        print("v266.57.5 structure patch skip: load_feature failed", e)
        return {}, _load_market_context_v266575()

    if df.empty or "stock_id" not in df.columns or "date" not in df.columns:
        return {}, _load_market_context_v266575()

    need_cols = [
        "open", "high", "low", "close", "volume",
        "ma5", "ma10", "ma20", "ma60",
        "mom5", "mom10", "mom20", "mom60",
        "volume_ratio", "high_20", "low_20", "high_60", "low_60",
        "ma_converge_pct", "ma20_slope"
    ]
    for c in need_cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.copy()
    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})", expand=False).fillna(df["stock_id"].astype(str))
    df = df.dropna(subset=["date", "stock_id", "close"]).sort_values(["stock_id", "date"])

    ctx = _load_market_context_v266575()
    weights = ctx["weights"]
    macro_adj = float(ctx["macro_adj"])

    out = {}

    for sid, g in df.groupby("stock_id"):
        h = g.tail(70).copy()
        if len(h) < 20:
            continue

        last = h.iloc[-1]
        prev = h.iloc[-2] if len(h) >= 2 else last

        close = _safe_float_v266575(last.get("close"))
        open_ = _safe_float_v266575(last.get("open"))
        high = _safe_float_v266575(last.get("high"))
        low = _safe_float_v266575(last.get("low"))
        volume = _safe_float_v266575(last.get("volume"))
        ma5 = _safe_float_v266575(last.get("ma5"))
        ma10 = _safe_float_v266575(last.get("ma10"))
        ma20 = _safe_float_v266575(last.get("ma20"))
        ma60 = _safe_float_v266575(last.get("ma60"))
        ma20_slope = _safe_float_v266575(last.get("ma20_slope"))
        mom5 = _safe_float_v266575(last.get("mom5"))
        mom10 = _safe_float_v266575(last.get("mom10"))
        mom20 = _safe_float_v266575(last.get("mom20"))
        mom60 = _safe_float_v266575(last.get("mom60"))
        volume_ratio = _safe_float_v266575(last.get("volume_ratio"))

        if not np.isfinite(close) or close <= 0:
            continue

        last20 = h.tail(20)
        last40 = h.tail(40) if len(h) >= 40 else h
        last60 = h.tail(60) if len(h) >= 60 else h

        high20 = pd.to_numeric(last20["high"], errors="coerce").max()
        low20 = pd.to_numeric(last20["low"], errors="coerce").min()
        high40 = pd.to_numeric(last40["high"], errors="coerce").max()
        low40 = pd.to_numeric(last40["low"], errors="coerce").min()
        high60 = pd.to_numeric(last60["high"], errors="coerce").max()
        low60 = pd.to_numeric(last60["low"], errors="coerce").min()

        vol_med_20 = pd.to_numeric(last20["volume"], errors="coerce").median()
        vol_med_40 = pd.to_numeric(last40["volume"], errors="coerce").median()
        vol_med_60 = pd.to_numeric(last60["volume"], errors="coerce").median()

        range20 = _pct_range_v266575(last20["high"], last20["low"], close)
        range40 = _pct_range_v266575(last40["high"], last40["low"], close)
        range60 = _pct_range_v266575(last60["high"], last60["low"], close)

        ma_tight = (
            np.isfinite(ma5) and np.isfinite(ma10) and np.isfinite(ma20)
            and close > 0
            and (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / close <= 0.08
        )

        # 20D：短線轉強／主升初段
        s20 = 0
        r20 = []
        if np.isfinite(ma20) and close > ma20:
            s20 += 2; r20.append("收盤站上MA20")
        if np.isfinite(ma5) and np.isfinite(ma10) and ma5 >= ma10:
            s20 += 2; r20.append("MA5站上MA10")
        if np.isfinite(high20) and close >= high20 * 0.96:
            s20 += 2; r20.append("接近20日高點")
        if _volume_mild_expand_v266575(volume, vol_med_20):
            s20 += 2; r20.append("20日量能溫和回升")
        if np.isfinite(mom10) and mom10 > 0:
            s20 += 1; r20.append("10日動能轉正")
        if np.isfinite(ma20) and ma20 > 0 and close <= ma20 * 1.15:
            s20 += 1; r20.append("尚未嚴重乖離MA20")

        # 40D：中期平台整理／壓縮後待發動
        s40 = 0
        r40 = []
        if np.isfinite(range40) and range40 <= 0.35:
            s40 += 2; r40.append("40日區間收斂")
        if ma_tight:
            s40 += 2; r40.append("均線糾結壓縮")
        if np.isfinite(ma20) and close >= ma20 * 0.98:
            s40 += 2; r40.append("站回平台成本區")
        if len(last40) >= 30:
            prior_low = pd.to_numeric(last40.iloc[:20]["low"], errors="coerce").min()
            recent_low = pd.to_numeric(last40.iloc[-20:]["low"], errors="coerce").min()
            if np.isfinite(prior_low) and np.isfinite(recent_low) and recent_low >= prior_low * 0.96:
                s40 += 2; r40.append("中期低點未再破底")
        if _volume_mild_expand_v266575(volume, vol_med_40):
            s40 += 1; r40.append("40日量能開始回溫")
        if np.isfinite(ma20_slope) and ma20_slope >= 0:
            s40 += 1; r40.append("MA20走平翻揚")

        # 60D：長底翻多／大結構修復
        s60 = 0
        r60 = []
        if np.isfinite(ma60) and close >= ma60 * 0.96:
            s60 += 2; r60.append("接近或站回MA60")
        if np.isfinite(ma20) and np.isfinite(ma60) and ma20 >= ma60 * 0.95:
            s60 += 2; r60.append("MA20接近MA60")
        if np.isfinite(high60) and np.isfinite(low60) and high60 > low60:
            pos60 = (close - low60) / (high60 - low60)
            if pos60 >= 0.45:
                s60 += 2; r60.append("站回60日區間中上緣")
        if len(last60) >= 50:
            prior_low60 = pd.to_numeric(last60.iloc[:30]["low"], errors="coerce").min()
            recent_low60 = pd.to_numeric(last60.iloc[-30:]["low"], errors="coerce").min()
            if np.isfinite(prior_low60) and np.isfinite(recent_low60) and recent_low60 >= prior_low60 * 0.95:
                s60 += 2; r60.append("60日低點結構守住")
        if np.isfinite(mom60) and mom60 > -0.05:
            s60 += 1; r60.append("60日動能不再惡化")
        if _volume_mild_expand_v266575(volume, vol_med_60):
            s60 += 1; r60.append("60日量能溫和回補")

        # 過熱扣分：只扣結構提示分數，不影響原本動能名單。
        heat = 0
        heat_reason = []
        if np.isfinite(ma20) and ma20 > 0 and close > ma20 * 1.22:
            heat += 1.5; heat_reason.append("距MA20過遠")
        if np.isfinite(volume_ratio) and volume_ratio > 5.5:
            heat += 1.0; heat_reason.append("單日爆量偏高")
        if np.isfinite(mom20) and mom20 > 0.45:
            heat += 1.0; heat_reason.append("20日漲幅偏熱")
        if np.isfinite(open_) and np.isfinite(high) and np.isfinite(low) and np.isfinite(close) and high > low:
            upper_ratio = (high - max(open_, close)) / (high - low)
            if upper_ratio >= 0.45:
                heat += 1.0; heat_reason.append("長上影壓力")

        s20 = max(0, min(10, s20 - heat))
        s40 = max(0, min(10, s40 - heat * 0.5))
        s60 = max(0, min(10, s60 - heat * 0.3))

        composite = (
            s20 * weights["20"]
            + s40 * weights["40"]
            + s60 * weights["60"]
            + macro_adj
        )
        composite = max(0, min(10, composite))

        buckets = {
            "20D短線轉強": s20,
            "40D平台整理": s40,
            "60D長底翻多": s60,
        }
        best_type = max(buckets, key=buckets.get)
        best_score = buckets[best_type]

        if composite < 4:
            structure_type = "結構不足"
        elif heat >= 2 and s20 >= 6:
            structure_type = "過熱延續"
        else:
            structure_type = best_type

        if composite >= 7:
            grade = "強"
        elif composite >= 5:
            grade = "中"
        else:
            grade = "弱"

        reasons = []
        reasons.append(f"20D:{'、'.join(r20) if r20 else '短線結構不足'}")
        reasons.append(f"40D:{'、'.join(r40) if r40 else '平台結構不足'}")
        reasons.append(f"60D:{'、'.join(r60) if r60 else '長底結構不足'}")
        if heat_reason:
            reasons.append("過熱扣分:" + "、".join(heat_reason))

        if structure_type == "20D短線轉強":
            hint = "偏主升初段／延續觀察：適合搭配原本ALPHA動能，但避免追過熱。"
        elif structure_type == "40D平台整理":
            hint = "偏平台整理後轉強：適合觀察是否從WATCH/TEST升級。"
        elif structure_type == "60D長底翻多":
            hint = "偏長底翻多：適合CORE早期卡位，小倉觀察是否放量確認。"
        elif structure_type == "過熱延續":
            hint = "已有動能但乖離偏高：保留原動能判斷，操作上不追高。"
        else:
            hint = "結構證據不足：原策略若入選，仍需降低信心。"

        out[sid] = {
            "structure_20_score": round(float(s20), 2),
            "structure_40_score": round(float(s40), 2),
            "structure_60_score": round(float(s60), 2),
            "structure_score": round(float(composite), 2),
            "structure_grade": grade,
            "structure_type": structure_type,
            "structure_reason": "｜".join(reasons),
            "structure_market_fit": ctx["env_note"] + "｜" + ctx["macro_note"],
            "structure_hint": hint,
            "structure_patch_version": "v266.57.5",
        }

    return out, ctx


def _enrich_csv_v266575(name, structure_map):
    """只補 20/40/60 結構欄位，不刪列、不改 action、不改 entry_score。"""
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df = _safe_read_csv_v266572(p)
        if df.empty or "stock_id" not in df.columns:
            continue

        df = df.copy()
        sids = df["stock_id"].map(_sid_v266572)

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

        # 只追加提示，不覆蓋原 note/system_note。
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

        df.to_csv(p, index=False, encoding="utf-8-sig")
        print("v266.57.5 structure enriched:", p, len(df))


def apply_structure_score_patch_v266575():
    structure_map, ctx = _calc_structure_map_v266575()
    if not structure_map:
        print("v266.57.5 structure patch: no structure map, skip")
        return

    for name in [
        "core_candidates.csv",
        "alpha_candidates.csv",
        "candidates.csv",
        "trade_plan.csv",
        "ignition_candidates.csv",
        "strategy_evolution.csv",
        "final_action_plan.csv",
        "top_opportunities.csv",
    ]:
        _enrich_csv_v266575(name, structure_map)

    report = {
        "version": "v266.57.5",
        "mode": "append_only_20_40_60_structure_score",
        "changed_strategy_logic": False,
        "changed_actions": False,
        "changed_entry_score": False,
        "changed_position_sizing": False,
        "market_regime": ctx.get("market_regime", "--"),
        "macro_label": ctx.get("macro_label", "--"),
        "market_weighting": ctx.get("weights", {}),
        "macro_adjustment": ctx.get("macro_adj", 0),
        "enriched_stock_count": len(structure_map),
        "updated_at": taipei_now_str(),
        "description": "只補20/40/60日結構分數、結構型態、市場/總經適配提示；不改原本CORE/ALPHA動能與操作邏輯。",
    }

    for p in [ROOT / "structure_patch_report.json", DATA_DIR / "structure_patch_report.json"]:
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))


def main_v266575_structure_patch():
    main_v266572_continuation_patch()
    apply_structure_score_patch_v266575()


if __name__ == "__main__":
    main_v266575_structure_patch()
