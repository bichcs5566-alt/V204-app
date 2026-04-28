"""
v265_2_tradeable_strategy_core.py
v265.2 可實戰版

重點：
- 分數不再全部卡 59
- BUY / TEST / WATCH 依分數產生
- CSV 使用 utf-8，避免 iPhone Safari 亂碼
- 不接 sidecar、不接 v3_core
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
BUY_TOP_N = 8
TEST_TOP_N = 10
WATCH_TOP_N = 14
CORE_TOP_N = 25
ALPHA_TOP_N = 8
MAX_TRADE_PLAN_N = 32
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
    p = float(price)
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
    paths = [
        ROOT / "price_panel_daily.csv",
        ROOT / "data" / "price_panel_daily.csv",
        DATA_DIR / "price_panel_daily.csv",
    ]
    src = None
    for p in paths:
        if p.exists() and p.stat().st_size > 0:
            src = p
            break
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
        out[f"ma{n}"] = g["close"].rolling(n, min_periods=max(3, min(n, 20))).mean().reset_index(level=0, drop=True)

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

    zero_cols = ["ret1", "mom3", "mom5", "mom10", "mom20", "mom60", "ma20_slope", "macd_diff", "macd_hist",
                 "obv_up_count_3", "obv_up_count_5", "obv_up_count_10",
                 "low_non_down_count_3", "low_non_down_count_5", "low_non_down_count_10"]
    for c in zero_cols:
        d[c] = pd.to_numeric(d.get(c, 0), errors="coerce").fillna(0)

    d["vol20"] = pd.to_numeric(d.get("vol20", 0.03), errors="coerce").fillna(0.03)
    d["volume_ratio"] = pd.to_numeric(d.get("volume_ratio", 1.0), errors="coerce").fillna(1.0)
    d["vol_dry_ratio"] = pd.to_numeric(d.get("vol_dry_ratio", 1.0), errors="coerce").fillna(1.0)

    for c in ["ma5", "ma10", "ma20", "ma60", "high_20", "low_20", "high_60", "low_60"]:
        d[c] = pd.to_numeric(d.get(c, d["close"]), errors="coerce").fillna(d["close"])

    d["range_20"] = pd.to_numeric(d.get("range_20", 0.2), errors="coerce").fillna(0.2)
    d["ma_converge_pct"] = pd.to_numeric(d.get("ma_converge_pct", 0.1), errors="coerce").fillna(0.1)
    d["kd_cross"] = pd.to_numeric(d.get("kd_cross", 0), errors="coerce").fillna(0)
    d["macd_cross"] = pd.to_numeric(d.get("macd_cross", 0), errors="coerce").fillna(0)
    return signal_date, d


def score_stocks(d):
    x = d.copy()

    x["momentum_score"] = 0.0
    x.loc[x["mom3"] > 0, "momentum_score"] += 6
    x.loc[x["mom5"] > 0, "momentum_score"] += 8
    x.loc[x["mom10"] > -0.015, "momentum_score"] += 5
    x.loc[x["mom10"] > 0, "momentum_score"] += 5
    x.loc[x["mom20"] > -0.03, "momentum_score"] += 5
    x.loc[x["mom20"] > 0, "momentum_score"] += 4
    x.loc[x["mom60"] > 0, "momentum_score"] += 2

    x["trend_score"] = 0.0
    x.loc[x["close"] >= x["ma5"] * 0.98, "trend_score"] += 5
    x.loc[x["close"] >= x["ma10"] * 0.98, "trend_score"] += 5
    x.loc[x["close"] >= x["ma20"] * 0.97, "trend_score"] += 6
    x.loc[x["close"] >= x["ma20"], "trend_score"] += 5
    x.loc[x["ma20_slope"] >= -0.005, "trend_score"] += 4

    x["volume_score"] = 0.0
    x.loc[(x["volume_ratio"] >= 0.75) & (x["volume_ratio"] <= 4.5), "volume_score"] += 6
    x.loc[(x["volume_ratio"] >= 1.00) & (x["volume_ratio"] <= 4.0), "volume_score"] += 6
    x.loc[(x["vol_dry_ratio"] >= 0.45) & (x["vol_dry_ratio"] <= 1.25), "volume_score"] += 4
    x.loc[x["volume"] > 500, "volume_score"] += 4

    x["structure_score"] = 0.0
    x.loc[x["ma_converge_pct"] <= 0.10, "structure_score"] += 6
    x.loc[x["range_20"] <= 0.30, "structure_score"] += 5
    x.loc[x["close"] >= x["low_20"] * 1.02, "structure_score"] += 3
    x.loc[x["close"] >= x["high_20"] * 0.92, "structure_score"] += 4
    x.loc[(x["close"] / (x["high_60"] + 1e-9)).between(0.65, 1.02), "structure_score"] += 2

    x["confirm_score"] = 0.0
    x.loc[x["kd_cross"] == 1, "confirm_score"] += 5
    x.loc[x["macd_cross"] == 1, "confirm_score"] += 5
    x.loc[x["macd_diff"] > 0, "confirm_score"] += 3
    x.loc[x["obv_up_count_3"] >= 2, "confirm_score"] += 2
    x.loc[x["obv_up_count_5"] >= 3, "confirm_score"] += 2
    x.loc[x["low_non_down_count_5"] >= 3, "confirm_score"] += 3

    x["raw_score"] = x["momentum_score"] + x["trend_score"] + x["volume_score"] + x["structure_score"] + x["confirm_score"]

    x["risk_penalty"] = 0.0
    x.loc[x["close"] < 8, "risk_penalty"] += 12
    x.loc[x["mom20"] > 0.35, "risk_penalty"] += 10
    x.loc[x["volume_ratio"] > 6.0, "risk_penalty"] += 8
    x.loc[x["vol20"] > 0.13, "risk_penalty"] += 5

    x["entry_score"] = x["raw_score"] - x["risk_penalty"]

    buy_cond = (x["entry_score"] >= 72) & (x["momentum_score"] >= 18) & (x["trend_score"] >= 14)
    test_cond = (x["entry_score"] >= 58) & ~buy_cond & ((x["mom5"] > 0) | (x["mom10"] > 0) | (x["kd_cross"] == 1) | (x["macd_cross"] == 1))
    watch_cond = (x["entry_score"] >= 42) & ~buy_cond & ~test_cond

    x["action"] = "SKIP"
    x.loc[watch_cond, "action"] = "WATCH"
    x.loc[test_cond, "action"] = "TEST"
    x.loc[buy_cond, "action"] = "BUY"

    x["action_label"] = "排除"
    x.loc[x["action"] == "WATCH", "action_label"] = "觀察"
    x.loc[x["action"] == "TEST", "action_label"] = "試單"
    x.loc[x["action"] == "BUY", "action_label"] = "買進"

    x["action_sub"] = "條件不足"
    x.loc[x["action"] == "WATCH", "action_sub"] = "觀察等待確認"
    x.loc[x["action"] == "TEST", "action_sub"] = "小倉測試"
    x.loc[x["action"] == "BUY", "action_sub"] = "可分批進場"

    def make_note(r):
        parts = []
        if r["momentum_score"] >= 22: parts.append("動能轉強")
        elif r["momentum_score"] >= 14: parts.append("動能修復")
        if r["trend_score"] >= 16: parts.append("站回均線")
        elif r["trend_score"] >= 10: parts.append("趨勢修復")
        if r["volume_score"] >= 14: parts.append("量能回溫")
        elif r["volume_score"] >= 8: parts.append("量能正常")
        if r["structure_score"] >= 14: parts.append("結構收斂")
        elif r["structure_score"] >= 8: parts.append("結構尚可")
        if r["confirm_score"] >= 10: parts.append("指標確認")
        elif r["confirm_score"] >= 5: parts.append("指標初轉強")
        if r["risk_penalty"] > 0: parts.append(f"風險扣分{int(r['risk_penalty'])}")
        return "｜".join(parts) if parts else "條件不足"

    x["note"] = x.apply(make_note, axis=1)
    return x.sort_values(["entry_score", "momentum_score", "trend_score"], ascending=False)


def build_candidates(scored):
    core = scored.sort_values(["entry_score", "trend_score", "structure_score"], ascending=False).head(CORE_TOP_N).copy()
    alpha = scored.sort_values(["momentum_score", "confirm_score", "entry_score"], ascending=False).head(ALPHA_TOP_N).copy()
    return core, alpha


def target_weight(action, score):
    score = float(score)
    if action == "BUY":
        return 0.02 if score >= 82 else 0.01
    if action == "TEST":
        return 0.005
    return 0.0


def build_trade_plan(scored, signal_date):
    trade_date = next_trade_date(signal_date)
    buy = scored[scored["action"] == "BUY"].head(BUY_TOP_N)
    test = scored[scored["action"] == "TEST"].head(TEST_TOP_N)
    watch = scored[scored["action"] == "WATCH"].head(WATCH_TOP_N)

    selected = pd.concat([buy, test, watch], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(MAX_TRADE_PLAN_N)

    if selected.empty:
        selected = scored.head(WATCH_TOP_N).copy()
        selected["action"] = "WATCH"
        selected["action_label"] = "觀察"
        selected["action_sub"] = "低分觀察，不進場"
        selected["note"] = selected["note"].astype(str) + "｜保底觀察"

    rows = []
    for _, r in selected.iterrows():
        px = float(r["close"]) * (1 + SLIPPAGE)
        w = target_weight(r["action"], r["entry_score"])
        amount = INITIAL_CAPITAL * w
        shares = amount / px if px > 0 else 0
        total_cost = shares * px * (1 + FEE)

        rows.append({
            "signal_date": str(pd.to_datetime(signal_date).date()),
            "trade_date": str(pd.to_datetime(trade_date).date()),
            "action": r["action"],
            "action_label": r["action_label"],
            "action_sub": r["action_sub"],
            "stock_id": r["stock_id"],
            "price_tier": price_tier(px),
            "ref_price": round(px, 4),
            "target_weight": round(w, 4),
            "suggested_amount": round(amount, 2),
            "suggested_shares": round(shares, 2),
            "estimated_total_cost": round(total_cost, 2),
            "entry_score": round(float(r["entry_score"]), 2),
            "momentum_score": round(float(r["momentum_score"]), 2),
            "trend_score": round(float(r["trend_score"]), 2),
            "volume_score": round(float(r["volume_score"]), 2),
            "structure_score": round(float(r["structure_score"]), 2),
            "confirm_score": round(float(r["confirm_score"]), 2),
            "source": "V265_2",
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
        root_p = ROOT / name
        data_p = DATA_DIR / name
        if not root_p.exists():
            pd.DataFrame(columns=cols).to_csv(root_p, index=False, encoding="utf-8")
        if not data_p.exists():
            pd.DataFrame(columns=cols).to_csv(data_p, index=False, encoding="utf-8")


def build_debug(raw, latest, scored, core, alpha, trade_plan, signal_date, src):
    return pd.DataFrame([{
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "price_source": src,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "total_input_rows": int(len(raw)),
        "latest_stock_count": int(len(latest)),
        "scored_count": int(len(scored)),
        "buy_count": int((scored["action"] == "BUY").sum()),
        "test_count": int((scored["action"] == "TEST").sum()),
        "watch_count": int((scored["action"] == "WATCH").sum()),
        "skip_count": int((scored["action"] == "SKIP").sum()),
        "core_count": int(len(core)),
        "alpha_count": int(len(alpha)),
        "trade_plan_count": int(len(trade_plan)),
        "trade_buy_count": int((trade_plan["action"] == "BUY").sum()),
        "trade_test_count": int((trade_plan["action"] == "TEST").sum()),
        "trade_watch_count": int((trade_plan["action"] == "WATCH").sum()),
        "avg_score": round(float(scored["entry_score"].mean()), 2) if len(scored) else 0,
        "max_score": round(float(scored["entry_score"].max()), 2) if len(scored) else 0,
        "note": "v265.2 tradeable scoring"
    }])


def main():
    raw, src = load_price()
    feat = build_features(raw)
    signal_date, latest = latest_frame(feat)
    scored = score_stocks(latest)
    core, alpha = build_candidates(scored)
    trade_plan = build_trade_plan(scored, signal_date)
    debug = build_debug(raw, latest, scored, core, alpha, trade_plan, signal_date, src)

    write_both(trade_plan, "trade_plan.csv")
    write_both(scored, "candidates.csv")
    write_both(core, "core_candidates.csv")
    write_both(alpha, "alpha_candidates.csv")
    write_both(debug, "selection_debug.csv")
    raw.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")
    ensure_support_files()

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v265_2_tradeable_strategy_core",
        "price_source": src,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "data_state": "fresh",
        "trade_plan_count": int(len(trade_plan)),
        "buy_count": int((trade_plan["action"] == "BUY").sum()),
        "test_count": int((trade_plan["action"] == "TEST").sum()),
        "watch_count": int((trade_plan["action"] == "WATCH").sum()),
        "execution_rule": "T日產生訊號，下一交易日人工下單",
    }
    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v265.2 tradeable strategy core completed")
    print(debug.to_string(index=False))
    print(trade_plan.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
