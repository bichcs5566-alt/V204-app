"""
v265_3_dual_engine_core.py
v265.3 雙引擎可交易版

Market Regime → Core 強勢引擎 / Alpha 反轉引擎 → Router → trade_plan
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
CORE_BUY_TOP_N = 8
CORE_TEST_TOP_N = 8
ALPHA_TEST_TOP_N = 8
ALPHA_WATCH_TOP_N = 12
MAX_TRADE_PLAN_N = 36
CORE_CANDIDATE_N = 30
ALPHA_CANDIDATE_N = 30
FEE = 0.0015
SLIPPAGE = 0.001


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_common_stock_id(x):
    s = normalize_stock_id(x)
    return s.isdigit() and len(s) == 4 and not s.startswith(("00", "03", "04", "05", "06", "07", "08", "09"))


def price_tier(price):
    try:
        p = float(price)
    except Exception:
        return ""
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


def write_both(df, filename):
    df.to_csv(ROOT / filename, index=False, encoding="utf-8")
    df.to_csv(DATA_DIR / filename, index=False, encoding="utf-8")


def load_price():
    paths = [ROOT / "price_panel_daily.csv", ROOT / "data" / "price_panel_daily.csv", DATA_DIR / "price_panel_daily.csv"]
    src = next((p for p in paths if p.exists() and p.stat().st_size > 0), None)
    if src is None:
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv(src)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
        else:
            raise ValueError("missing date column")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        else:
            raise ValueError("missing stock_id column")

    if "close" not in df.columns:
        raise ValueError("missing close column")

    for c in ["open", "high", "low"]:
        if c not in df.columns:
            df[c] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = 0

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    df = df[df["stock_id"].apply(is_common_stock_id)].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df, str(src)


def build_features(df):
    out = df.copy()
    g = out.groupby("stock_id", group_keys=False)

    out["ret1"] = g["close"].pct_change()
    for n in [3, 5, 10, 20, 60]:
        out[f"mom{n}"] = g["close"].pct_change(n)

    for n in [5, 10, 20, 60]:
        out[f"ma{n}"] = (
            g["close"].rolling(n, min_periods=max(3, min(n, 20))).mean().reset_index(level=0, drop=True)
        )

    out["vol20"] = g["ret1"].rolling(20, min_periods=5).std().reset_index(level=0, drop=True)
    out["vol_ma5"] = g["volume"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    out["vol_ma20"] = g["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["vol_ma20"] + 1e-9)
    out["vol_dry_ratio"] = out["vol_ma5"] / (out["vol_ma20"] + 1e-9)

    out["high_20"] = g["high"].rolling(20, min_periods=5).max().reset_index(level=0, drop=True)
    out["low_20"] = g["low"].rolling(20, min_periods=5).min().reset_index(level=0, drop=True)
    out["high_60"] = g["high"].rolling(60, min_periods=10).max().reset_index(level=0, drop=True)
    out["low_60"] = g["low"].rolling(60, min_periods=10).min().reset_index(level=0, drop=True)

    out["range_20"] = (out["high_20"] - out["low_20"]) / (out["close"] + 1e-9)
    out["ma_max"] = out[["ma5", "ma10", "ma20"]].max(axis=1)
    out["ma_min"] = out[["ma5", "ma10", "ma20"]].min(axis=1)
    out["ma_converge_pct"] = (out["ma_max"] - out["ma_min"]) / (out["close"] + 1e-9)
    out["ma20_slope"] = g["ma20"].diff(5) / (g["ma20"].shift(5) + 1e-9)

    low9 = g["low"].rolling(9, min_periods=5).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9, min_periods=5).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_cross"] = ((out["kd_k"] > out["kd_d"]) & (g["kd_k"].shift(1) <= g["kd_d"].shift(1))).astype(int)

    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_cross"] = ((out["macd_diff"] > out["macd_signal"]) & (g["macd_diff"].shift(1) <= g["macd_signal"].shift(1))).astype(int)

    close_diff = g["close"].diff()
    signed_volume = np.where(close_diff > 0, out["volume"], np.where(close_diff < 0, -out["volume"], 0))
    out["obv_proxy"] = pd.Series(signed_volume, index=out.index).groupby(out["stock_id"]).cumsum()
    out["obv_mom5"] = g["obv_proxy"].pct_change(5).replace([np.inf, -np.inf], np.nan).fillna(0)

    for w in [3, 5, 10]:
        out[f"obv_up_count_{w}"] = out.groupby("stock_id")["obv_proxy"].transform(
            lambda s, ww=w: (s.diff() > 0).astype(float).rolling(ww, min_periods=max(2, ww // 2)).sum()
        )
        out[f"low_non_down_count_{w}"] = out.groupby("stock_id")["low"].transform(
            lambda s, ww=w: (s.diff() >= 0).astype(float).rolling(ww, min_periods=max(2, ww // 2)).sum()
        )

    return out


def latest_frame(feat):
    signal_date = pd.to_datetime(feat["date"].max())
    d = feat[feat["date"] == signal_date].dropna(subset=["close"]).copy()

    zero_cols = [
        "ret1", "mom3", "mom5", "mom10", "mom20", "mom60",
        "ma20_slope", "macd_diff", "macd_hist", "obv_mom5",
        "obv_up_count_3", "obv_up_count_5", "obv_up_count_10",
        "low_non_down_count_3", "low_non_down_count_5", "low_non_down_count_10",
    ]
    for c in zero_cols:
        if c not in d.columns:
            d[c] = 0
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)

    defaults = {
        "vol20": 0.03, "volume_ratio": 1.0, "vol_dry_ratio": 1.0,
        "range_20": 0.2, "ma_converge_pct": 0.1, "kd_cross": 0, "macd_cross": 0,
    }
    for c, default in defaults.items():
        if c not in d.columns:
            d[c] = default
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(default)

    for c in ["ma5", "ma10", "ma20", "ma60", "high_20", "low_20", "high_60", "low_60"]:
        if c not in d.columns:
            d[c] = d["close"]
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(d["close"])

    d["price_tier"] = d["close"].apply(price_tier)
    return signal_date, d


def detect_market_regime(latest):
    valid = latest[latest["close"] > 0].copy()
    if valid.empty:
        return "RANGE", {}

    pct_above_ma20 = float((valid["close"] >= valid["ma20"]).mean())
    pct_above_ma60 = float((valid["close"] >= valid["ma60"]).mean())
    pct_mom20_pos = float((valid["mom20"] > 0).mean())
    pct_strong = float(((valid["mom20"] > 0.08) & (valid["close"] >= valid["high_60"] * 0.92)).mean())
    median_mom20 = float(valid["mom20"].median())
    avg_volume_ratio = float(valid["volume_ratio"].replace([np.inf, -np.inf], np.nan).fillna(1).median())

    regime_score = 0
    regime_score += int(pct_above_ma20 >= 0.55)
    regime_score += int(pct_above_ma60 >= 0.50)
    regime_score += int(pct_mom20_pos >= 0.50)
    regime_score += int(pct_strong >= 0.08)
    regime_score += int(median_mom20 > 0.015)

    if pct_above_ma60 < 0.35 and pct_mom20_pos < 0.35:
        regime = "BEAR"
    elif regime_score >= 4:
        regime = "TREND"
    else:
        regime = "RANGE"

    info = {
        "pct_above_ma20": round(pct_above_ma20, 4),
        "pct_above_ma60": round(pct_above_ma60, 4),
        "pct_mom20_pos": round(pct_mom20_pos, 4),
        "pct_strong": round(pct_strong, 4),
        "median_mom20": round(median_mom20, 4),
        "avg_volume_ratio": round(avg_volume_ratio, 4),
        "regime_score": regime_score,
    }
    return regime, info


def score_core_engine(latest):
    x = latest.copy()

    x["core_momentum_score"] = 0.0
    x.loc[x["mom5"] > 0.02, "core_momentum_score"] += 6
    x.loc[x["mom10"] > 0.04, "core_momentum_score"] += 8
    x.loc[x["mom20"] > 0.08, "core_momentum_score"] += 10
    x.loc[x["mom60"] > 0.10, "core_momentum_score"] += 6
    x.loc[x["close"] >= x["high_60"] * 0.90, "core_momentum_score"] += 5

    x["core_trend_score"] = 0.0
    x.loc[x["close"] > x["ma20"], "core_trend_score"] += 8
    x.loc[x["ma20"] > x["ma60"], "core_trend_score"] += 8
    x.loc[x["ma20_slope"] > 0, "core_trend_score"] += 5
    x.loc[x["close"] > x["ma5"], "core_trend_score"] += 4

    x["core_volume_score"] = 0.0
    x.loc[x["volume_ratio"].between(1.05, 5.0), "core_volume_score"] += 8
    x.loc[x["volume_ratio"].between(1.30, 4.5), "core_volume_score"] += 7
    x.loc[x["volume"] > 1000, "core_volume_score"] += 5

    x["core_quality_score"] = 0.0
    x.loc[x["close"] >= 30, "core_quality_score"] += 6
    x.loc[x["close"] >= 50, "core_quality_score"] += 4
    x.loc[x["vol20"] <= 0.08, "core_quality_score"] += 4
    x.loc[x["macd_diff"] > 0, "core_quality_score"] += 3
    x.loc[x["obv_up_count_5"] >= 3, "core_quality_score"] += 3

    x["core_raw_score"] = x["core_momentum_score"] + x["core_trend_score"] + x["core_volume_score"] + x["core_quality_score"]

    x["core_penalty"] = 0.0
    x.loc[x["close"] < 20, "core_penalty"] += 12
    x.loc[x["mom20"] > 0.45, "core_penalty"] += 10
    x.loc[x["volume_ratio"] > 6, "core_penalty"] += 8
    x.loc[x["vol20"] > 0.14, "core_penalty"] += 5

    x["entry_score"] = x["core_raw_score"] - x["core_penalty"]
    x["strategy_type"] = "CORE"

    buy_cond = (x["entry_score"] >= 42) & (x["mom20"] > 0.06) & (x["close"] > x["ma20"]) & (x["close"] >= 30)
    test_cond = (x["entry_score"] >= 34) & ~buy_cond & (x["mom10"] > 0.015) & (x["close"] > x["ma20"] * 0.98)
    watch_cond = (x["entry_score"] >= 26) & ~buy_cond & ~test_cond

    set_action_cols(x, buy_cond, test_cond, watch_cond, "強勢主攻", "強勢試單", "強勢觀察")

    def note(r):
        parts = []
        if r["mom20"] > 0.10: parts.append("20日強勢")
        if r["close"] >= r["high_60"] * 0.90: parts.append("接近60日高")
        if r["ma20"] > r["ma60"]: parts.append("中期多頭")
        if r["volume_ratio"] >= 1.3: parts.append("量能放大")
        if r["close"] >= 50: parts.append("避開低價")
        if r["core_penalty"] > 0: parts.append(f"風險扣分{int(r['core_penalty'])}")
        return "｜".join(parts) if parts else "強勢條件不足"

    x["note"] = x.apply(note, axis=1)
    return x.sort_values(["entry_score", "core_momentum_score", "core_trend_score"], ascending=False)


def score_alpha_engine(latest):
    x = latest.copy()

    x["alpha_reversal_score"] = 0.0
    x.loc[x["mom3"] > 0, "alpha_reversal_score"] += 5
    x.loc[x["mom5"] > 0, "alpha_reversal_score"] += 6
    x.loc[x["mom10"] > -0.03, "alpha_reversal_score"] += 5
    x.loc[x["mom20"].between(-0.08, 0.08), "alpha_reversal_score"] += 5
    x.loc[x["close"] >= x["ma20"] * 0.97, "alpha_reversal_score"] += 6
    x.loc[x["close"] >= x["ma20"], "alpha_reversal_score"] += 4

    x["alpha_structure_score"] = 0.0
    x.loc[x["ma_converge_pct"] <= 0.10, "alpha_structure_score"] += 7
    x.loc[x["range_20"] <= 0.28, "alpha_structure_score"] += 6
    x.loc[x["low_non_down_count_5"] >= 3, "alpha_structure_score"] += 5
    x.loc[x["close"] >= x["low_20"] * 1.02, "alpha_structure_score"] += 4

    x["alpha_volume_score"] = 0.0
    x.loc[x["volume_ratio"].between(0.80, 4.0), "alpha_volume_score"] += 6
    x.loc[x["volume_ratio"].between(1.00, 3.5), "alpha_volume_score"] += 6
    x.loc[x["vol_dry_ratio"].between(0.45, 1.30), "alpha_volume_score"] += 4
    x.loc[x["obv_up_count_5"] >= 3, "alpha_volume_score"] += 4

    x["alpha_confirm_score"] = 0.0
    x.loc[x["kd_cross"] == 1, "alpha_confirm_score"] += 5
    x.loc[x["macd_cross"] == 1, "alpha_confirm_score"] += 5
    x.loc[x["macd_diff"] > 0, "alpha_confirm_score"] += 3
    x.loc[x["obv_mom5"] > 0, "alpha_confirm_score"] += 3

    x["alpha_raw_score"] = x["alpha_reversal_score"] + x["alpha_structure_score"] + x["alpha_volume_score"] + x["alpha_confirm_score"]

    x["alpha_penalty"] = 0.0
    x.loc[x["close"] < 8, "alpha_penalty"] += 10
    x.loc[x["mom20"] > 0.25, "alpha_penalty"] += 8
    x.loc[x["volume_ratio"] > 5.5, "alpha_penalty"] += 6
    x.loc[x["vol20"] > 0.14, "alpha_penalty"] += 5

    x["entry_score"] = x["alpha_raw_score"] - x["alpha_penalty"]
    x["strategy_type"] = "ALPHA"

    buy_cond = (x["entry_score"] >= 44) & (x["close"] > x["ma20"]) & (x["mom5"] > 0.025) & (x["volume_ratio"] >= 1.3)
    test_cond = (x["entry_score"] >= 34) & ~buy_cond & ((x["mom5"] > 0) | (x["kd_cross"] == 1) | (x["macd_cross"] == 1))
    watch_cond = (x["entry_score"] >= 26) & ~buy_cond & ~test_cond

    set_action_cols(x, buy_cond, test_cond, watch_cond, "反轉確認", "小倉試單", "反轉觀察")

    def note(r):
        parts = []
        if r["mom5"] > 0: parts.append("短線轉強")
        if r["close"] >= r["ma20"] * 0.97: parts.append("靠近MA20")
        if r["ma_converge_pct"] <= 0.10: parts.append("均線收斂")
        if r["volume_ratio"] >= 1.0: parts.append("量能回溫")
        if r["low_non_down_count_5"] >= 3: parts.append("低點不破")
        if r["alpha_penalty"] > 0: parts.append(f"風險扣分{int(r['alpha_penalty'])}")
        return "｜".join(parts) if parts else "反轉條件不足"

    x["note"] = x.apply(note, axis=1)
    return x.sort_values(["entry_score", "alpha_reversal_score", "alpha_structure_score"], ascending=False)


def set_action_cols(df, buy_cond, test_cond, watch_cond, buy_sub, test_sub, watch_sub):
    df["action"] = "SKIP"
    df.loc[watch_cond, "action"] = "WATCH"
    df.loc[test_cond, "action"] = "TEST"
    df.loc[buy_cond, "action"] = "BUY"
    df["action_label"] = "排除"
    df.loc[df["action"] == "WATCH", "action_label"] = "觀察"
    df.loc[df["action"] == "TEST", "action_label"] = "試單"
    df.loc[df["action"] == "BUY", "action_label"] = "買進"
    df["action_sub"] = "條件不足"
    df.loc[df["action"] == "WATCH", "action_sub"] = watch_sub
    df.loc[df["action"] == "TEST", "action_sub"] = test_sub
    df.loc[df["action"] == "BUY", "action_sub"] = buy_sub


def route_trade_plan(core, alpha, regime, signal_date):
    trade_date = next_trade_date(signal_date)

    if regime == "TREND":
        core_buy_n, core_test_n, alpha_test_n, alpha_watch_n = 8, 8, 3, 5
    elif regime == "BEAR":
        core_buy_n, core_test_n, alpha_test_n, alpha_watch_n = 0, 3, 8, 12
    else:
        core_buy_n, core_test_n, alpha_test_n, alpha_watch_n = 5, 6, 8, 10

    selected = pd.concat([
        core[core["action"] == "BUY"].head(core_buy_n),
        core[core["action"] == "TEST"].head(core_test_n),
        alpha[alpha["action"].isin(["BUY", "TEST"])].head(alpha_test_n),
        alpha[alpha["action"] == "WATCH"].head(alpha_watch_n),
    ], ignore_index=True)

    if not selected.empty:
        selected["strategy_priority"] = np.where(selected["strategy_type"] == "CORE", 1, 2)
        selected = selected.sort_values(["strategy_priority", "entry_score"], ascending=[True, False])
        selected = selected.drop_duplicates(subset=["stock_id"], keep="first")
    else:
        selected = alpha.head(ALPHA_WATCH_TOP_N).copy()
        selected["action"] = "WATCH"
        selected["action_label"] = "觀察"
        selected["action_sub"] = "低分觀察，不進場"
        selected["note"] = selected["note"].astype(str) + "｜保底觀察"

    selected = selected.head(MAX_TRADE_PLAN_N).copy()

    rows = []
    for _, r in selected.iterrows():
        action = str(r["action"])
        strategy_type = str(r["strategy_type"])
        score = float(r["entry_score"])
        px = float(r["close"]) * (1 + SLIPPAGE)

        if action == "BUY" and strategy_type == "CORE":
            weight = 0.02 if score >= 50 else 0.01
        elif action == "BUY" and strategy_type == "ALPHA":
            weight = 0.008
        elif action == "TEST":
            weight = 0.005
        else:
            weight = 0.0

        amount = INITIAL_CAPITAL * weight
        shares = amount / px if px > 0 else 0
        total_cost = shares * px * (1 + FEE)

        rows.append({
            "signal_date": str(pd.to_datetime(signal_date).date()),
            "trade_date": str(pd.to_datetime(trade_date).date()),
            "market_regime": regime,
            "strategy_type": strategy_type,
            "action": action,
            "action_label": r["action_label"],
            "action_sub": r["action_sub"],
            "stock_id": r["stock_id"],
            "price_tier": price_tier(px),
            "ref_price": round(px, 4),
            "target_weight": round(weight, 4),
            "suggested_amount": round(amount, 2),
            "suggested_shares": round(shares, 2),
            "estimated_total_cost": round(total_cost, 2),
            "entry_score": round(score, 2),
            "source": "V265_3_DUAL",
            "note": r["note"],
        })

    return pd.DataFrame(rows)


def ensure_support_files():
    support = {
        "current_positions.csv": ["stock_id", "shares", "avg_cost"],
        "position_monitor.csv": ["stock_id", "shares", "avg_cost", "note"],
        "watchlist_monitor.csv": ["stock_id", "note"],
        "full_summary.csv": ["return", "mdd", "sharpe_daily"],
        "daily_nav.csv": ["date", "nav", "ret"],
    }
    for name, cols in support.items():
        for p in [ROOT / name, DATA_DIR / name]:
            if not p.exists():
                pd.DataFrame(columns=cols).to_csv(p, index=False, encoding="utf-8")


def build_debug(raw, latest, core, alpha, trade_plan, signal_date, src, regime, regime_info):
    return pd.DataFrame([{
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "price_source": src,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "market_regime": regime,
        "regime_score": regime_info.get("regime_score", ""),
        "pct_above_ma20": regime_info.get("pct_above_ma20", ""),
        "pct_above_ma60": regime_info.get("pct_above_ma60", ""),
        "pct_mom20_pos": regime_info.get("pct_mom20_pos", ""),
        "pct_strong": regime_info.get("pct_strong", ""),
        "median_mom20": regime_info.get("median_mom20", ""),
        "total_input_rows": int(len(raw)),
        "latest_stock_count": int(len(latest)),
        "core_buy_count": int((core["action"] == "BUY").sum()),
        "core_test_count": int((core["action"] == "TEST").sum()),
        "core_watch_count": int((core["action"] == "WATCH").sum()),
        "alpha_buy_count": int((alpha["action"] == "BUY").sum()),
        "alpha_test_count": int((alpha["action"] == "TEST").sum()),
        "alpha_watch_count": int((alpha["action"] == "WATCH").sum()),
        "trade_buy_count": int((trade_plan["action"] == "BUY").sum()),
        "trade_test_count": int((trade_plan["action"] == "TEST").sum()),
        "trade_watch_count": int((trade_plan["action"] == "WATCH").sum()),
        "core_count": int(len(core)),
        "alpha_count": int(len(alpha)),
        "trade_plan_count": int(len(trade_plan)),
        "core_max_score": round(float(core["entry_score"].max()), 2) if len(core) else 0,
        "alpha_max_score": round(float(alpha["entry_score"].max()), 2) if len(alpha) else 0,
        "note": "v265.3 dual engine"
    }])


def main():
    raw, src = load_price()
    feat = build_features(raw)
    signal_date, latest = latest_frame(feat)
    regime, regime_info = detect_market_regime(latest)

    core_all = score_core_engine(latest)
    alpha_all = score_alpha_engine(latest)
    core_candidates = core_all.head(CORE_CANDIDATE_N).copy()
    alpha_candidates = alpha_all.head(ALPHA_CANDIDATE_N).copy()

    trade_plan = route_trade_plan(core_candidates, alpha_candidates, regime, signal_date)
    candidates = pd.concat([core_candidates.assign(engine="CORE"), alpha_candidates.assign(engine="ALPHA")], ignore_index=True)
    debug = build_debug(raw, latest, core_candidates, alpha_candidates, trade_plan, signal_date, src, regime, regime_info)

    write_both(trade_plan, "trade_plan.csv")
    write_both(core_candidates, "core_candidates.csv")
    write_both(alpha_candidates, "alpha_candidates.csv")
    write_both(candidates, "candidates.csv")
    write_both(debug, "selection_debug.csv")
    raw.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")
    ensure_support_files()

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v265_3_dual_engine_core",
        "price_source": src,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "data_state": "fresh",
        "market_regime": regime,
        "regime_info": regime_info,
        "trade_plan_count": int(len(trade_plan)),
        "buy_count": int((trade_plan["action"] == "BUY").sum()),
        "test_count": int((trade_plan["action"] == "TEST").sum()),
        "watch_count": int((trade_plan["action"] == "WATCH").sum()),
        "core_trade_count": int((trade_plan["strategy_type"] == "CORE").sum()) if "strategy_type" in trade_plan.columns else 0,
        "alpha_trade_count": int((trade_plan["strategy_type"] == "ALPHA").sum()) if "strategy_type" in trade_plan.columns else 0,
        "execution_rule": "Market Regime → Core/Alpha Router → T+1人工下單",
    }

    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v265.3 dual engine completed")
    print("market_regime:", regime, regime_info)
    print(debug.to_string(index=False))
    print(trade_plan.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
