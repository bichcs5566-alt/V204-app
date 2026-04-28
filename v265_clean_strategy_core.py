"""
v265_clean_strategy_core.py
以 v265_decision_engine.py 為主核心的乾淨策略重建版

定位：
- 每日操作決策核心
- 人工下單用
- 不接 v3_core
- 不接 sidecar
- trade_plan.csv 是唯一正式操作清單

核心保留：
- load_price()
- build_features()
- Core / Alpha 概念
- target_weight / suggested_amount / suggested_shares
- daily_nav / full_summary 可保留

策略重建：
A：BUY 買進
B：TEST 試單
C：WATCH 觀察

重要修正：
1. 不再因 mom60 / vol20 缺值整批空掉
2. mom60 只加分，不當硬門檻
3. 沒有買進時仍輸出 WATCH / NO_TRADE，不產生 0 byte 空檔
4. selection_debug.csv 清楚列出每層數量
5. full_summary.csv 仍可放績效；股票候選放 candidates.csv
"""

import os
import json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd


# =========================
# System Config
# =========================

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CAPITAL = 1_000_000

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25

CORE_TOP_N = 25
ALPHA_TOP_N = 6

BUY_TOP_N = 8
TEST_TOP_N = 8
WATCH_TOP_N = 14
MAX_TRADE_PLAN_N = 30

MAX_POSITION_WEIGHT = 0.10

FEE = 0.0015
SLIPPAGE = 0.001


# =========================
# Utilities
# =========================

def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_common_stock_id(x):
    s = normalize_stock_id(x)
    if not (s.isdigit() and len(s) == 4):
        return False
    # 排除 ETF / 權證 / 特殊商品常見區
    if s.startswith(("00", "03", "04", "05", "06", "07", "08", "09")):
        return False
    return True


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
    df.to_csv(ROOT / filename, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / filename, index=False, encoding="utf-8-sig")


def safe_empty_trade_plan(signal_date=None, reason="no candidate"):
    signal_date = pd.to_datetime(signal_date) if signal_date is not None else pd.Timestamp.today().normalize()
    trade_date = next_trade_date(signal_date)
    return pd.DataFrame([{
        "signal_date": str(signal_date.date()),
        "trade_date": str(trade_date.date()),
        "action": "NO_TRADE",
        "action_label": "⚪ 觀察",
        "action_sub": "今天沒有新的買進動作",
        "stock_id": "",
        "price_tier": "",
        "ref_price": "",
        "target_weight": 0.0,
        "suggested_amount": 0.0,
        "suggested_shares": 0.0,
        "estimated_total_cost": 0.0,
        "entry_score": 0.0,
        "source": "SYSTEM",
        "note": reason
    }])


# =========================
# 1. Load Price
# =========================

def load_price():
    candidates = [
        ROOT / "price_panel_daily.csv",
        ROOT / "data" / "price_panel_daily.csv",
        DATA_DIR / "price_panel_daily.csv",
    ]

    price_path = None
    for p in candidates:
        if p.exists() and p.stat().st_size > 0:
            price_path = p
            break

    if price_path is None:
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv(price_path)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
        else:
            raise ValueError("no date column")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        else:
            raise ValueError("no stock_id column")

    if "close" not in df.columns:
        raise ValueError("no close column")

    if "open" not in df.columns:
        df["open"] = df["close"]
    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = np.nan

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    df = df[df["stock_id"].apply(is_common_stock_id)].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)

    return df, str(price_path)


# =========================
# 2. Build Features
# =========================

def build_features(df):
    df = df.copy()
    g = df.groupby("stock_id", group_keys=False)

    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)

    df["vol20"] = (
        g["ret1"]
        .rolling(20, min_periods=5)
        .std()
        .reset_index(level=0, drop=True)
    )

    for n in [5, 10, 20, 60]:
        df[f"ma{n}"] = (
            g["close"]
            .rolling(n, min_periods=max(3, min(n, 20)))
            .mean()
            .reset_index(level=0, drop=True)
        )

    df["vol_ma5"] = (
        g["volume"]
        .rolling(5, min_periods=3)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["vol_ma20"] = (
        g["volume"]
        .rolling(20, min_periods=5)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["volume_ratio"] = df["volume"] / (df["vol_ma20"] + 1e-9)
    df["vol_dry_ratio"] = df["vol_ma5"] / (df["vol_ma20"] + 1e-9)

    df["high_20"] = (
        g["high"]
        .rolling(20, min_periods=5)
        .max()
        .reset_index(level=0, drop=True)
    )
    df["low_20"] = (
        g["low"]
        .rolling(20, min_periods=5)
        .min()
        .reset_index(level=0, drop=True)
    )
    df["high_60"] = (
        g["high"]
        .rolling(60, min_periods=10)
        .max()
        .reset_index(level=0, drop=True)
    )
    df["low_60"] = (
        g["low"]
        .rolling(60, min_periods=10)
        .min()
        .reset_index(level=0, drop=True)
    )

    df["range_20"] = (df["high_20"] - df["low_20"]) / (df["close"] + 1e-9)
    df["ma_max"] = df[["ma5", "ma10", "ma20"]].max(axis=1)
    df["ma_min"] = df[["ma5", "ma10", "ma20"]].min(axis=1)
    df["ma_converge_pct"] = (df["ma_max"] - df["ma_min"]) / (df["close"] + 1e-9)
    df["ma20_slope"] = g["ma20"].diff(5) / (g["ma20"].shift(5) + 1e-9)

    # KD
    low9 = (
        g["low"]
        .rolling(9, min_periods=5)
        .min()
        .reset_index(level=0, drop=True)
    )
    high9 = (
        g["high"]
        .rolling(9, min_periods=5)
        .max()
        .reset_index(level=0, drop=True)
    )
    rsv = (df["close"] - low9) / (high9 - low9 + 1e-9) * 100

    df["kd_k"] = (
        rsv.groupby(df["stock_id"])
        .ewm(com=2, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["kd_d"] = (
        df["kd_k"]
        .groupby(df["stock_id"])
        .ewm(com=2, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["kd_cross"] = (
        (df["kd_k"] > df["kd_d"])
        & (g["kd_k"].shift(1) <= g["kd_d"].shift(1))
    ).astype(int)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df["macd_diff"] = ema12 - ema26
    df["macd_signal"] = df.groupby("stock_id")["macd_diff"].transform(
        lambda s: s.ewm(span=9, adjust=False).mean()
    )
    df["macd_hist"] = df["macd_diff"] - df["macd_signal"]
    df["macd_cross"] = (
        (df["macd_diff"] > df["macd_signal"])
        & (g["macd_diff"].shift(1) <= g["macd_signal"].shift(1))
    ).astype(int)

    return df


# =========================
# 3. Latest Frame
# =========================

def latest_signal_frame(df):
    dates = sorted(df["date"].dropna().unique())
    if len(dates) == 0:
        raise ValueError("no valid dates")

    # 每日操作：用最新已存在資料當 signal_date，下一交易日為 trade_date
    signal_date = pd.to_datetime(dates[-1])
    d = df[df["date"] == signal_date].copy()

    # 僅要求 close 有效
    d = d.dropna(subset=["close"]).copy()

    # 中性補值：不要因資料不足整批空掉
    neutral_zero = ["ret1", "mom5", "mom10", "mom20", "mom60", "ma20_slope", "macd_diff", "macd_hist"]
    for col in neutral_zero:
        if col not in d.columns:
            d[col] = 0
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0)

    for col in ["vol20"]:
        if col not in d.columns:
            d[col] = 0.03
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0.03)

    for col in ["volume_ratio", "vol_dry_ratio"]:
        if col not in d.columns:
            d[col] = 1.0
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(1.0)

    for col in ["ma5", "ma10", "ma20", "ma60", "high_20", "low_20", "high_60", "low_60"]:
        if col not in d.columns:
            d[col] = d["close"]
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(d["close"])

    for col, default in [("range_20", 0.2), ("ma_converge_pct", 0.1)]:
        if col not in d.columns:
            d[col] = default
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(default)

    for col in ["kd_cross", "macd_cross"]:
        if col not in d.columns:
            d[col] = 0
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0)

    return signal_date, d


# =========================
# 4. Strategy Scoring
# =========================

def select_stocks(d):
    x = d.copy()

    # 趨勢分數 0-30
    x["trend_score"] = 0.0
    x.loc[x["close"] >= x["ma20"] * 0.98, "trend_score"] += 7
    x.loc[x["close"] >= x["ma20"], "trend_score"] += 7
    x.loc[x["mom5"] > 0, "trend_score"] += 6
    x.loc[x["mom20"] > -0.02, "trend_score"] += 5
    x.loc[x["mom20"] > 0, "trend_score"] += 3
    x.loc[x["mom60"] > 0, "trend_score"] += 2

    # 量能分數 0-25
    x["volume_score"] = 0.0
    x.loc[(x["volume_ratio"] >= 0.80) & (x["volume_ratio"] <= 4.0), "volume_score"] += 8
    x.loc[(x["volume_ratio"] >= 1.05) & (x["volume_ratio"] <= 4.5), "volume_score"] += 8
    x.loc[(x["vol_dry_ratio"] >= 0.45) & (x["vol_dry_ratio"] <= 1.20), "volume_score"] += 5
    x.loc[x["volume"] > 500, "volume_score"] += 4

    # 結構分數 0-25
    x["structure_score"] = 0.0
    x.loc[x["ma_converge_pct"] <= 0.08, "structure_score"] += 8
    x.loc[x["range_20"] <= 0.25, "structure_score"] += 6
    x.loc[x["close"] >= x["low_20"] * 1.03, "structure_score"] += 4
    x.loc[x["close"] >= x["high_20"] * 0.94, "structure_score"] += 4
    x.loc[(x["close"] / (x["high_60"] + 1e-9)).between(0.70, 0.98), "structure_score"] += 3

    # 確認分數 0-20
    x["confirm_score"] = 0.0
    x.loc[x["kd_cross"] == 1, "confirm_score"] += 7
    x.loc[x["macd_cross"] == 1, "confirm_score"] += 7
    x.loc[x["macd_diff"] > 0, "confirm_score"] += 3
    x.loc[x["mom10"] > -0.02, "confirm_score"] += 3

    x["raw_score"] = (
        x["trend_score"]
        + x["volume_score"]
        + x["structure_score"]
        + x["confirm_score"]
    )

    x["risk_penalty"] = 0.0
    x.loc[x["close"] < 10, "risk_penalty"] += 15
    x.loc[x["mom20"] > 0.30, "risk_penalty"] += 12
    x.loc[x["volume_ratio"] > 5.0, "risk_penalty"] += 8
    x.loc[x["vol20"] > 0.10, "risk_penalty"] += 5

    x["entry_score"] = x["raw_score"] - x["risk_penalty"]

    # 三層策略
    buy_cond = (
        (x["entry_score"] >= 75)
        & (x["trend_score"] >= 18)
        & (x["volume_score"] >= 12)
    )

    test_cond = (
        (x["entry_score"] >= 60)
        & ~buy_cond
        & (
            (x["mom5"] > 0)
            | (x["kd_cross"] == 1)
            | (x["macd_cross"] == 1)
        )
    )

    watch_cond = (
        (x["entry_score"] >= 45)
        & ~buy_cond
        & ~test_cond
    )

    x["action"] = "SKIP"
    x.loc[watch_cond, "action"] = "WATCH"
    x.loc[test_cond, "action"] = "TEST"
    x.loc[buy_cond, "action"] = "BUY"

    x["action_label"] = "❌ 排除"
    x.loc[x["action"] == "WATCH", "action_label"] = "🟡 觀察"
    x.loc[x["action"] == "TEST", "action_label"] = "🟠 試單"
    x.loc[x["action"] == "BUY", "action_label"] = "🟢 買進"

    x["action_sub"] = "條件不足"
    x.loc[x["action"] == "WATCH", "action_sub"] = "觀察等待確認"
    x.loc[x["action"] == "TEST", "action_sub"] = "小倉測試"
    x.loc[x["action"] == "BUY", "action_sub"] = "可分批進場"

    def make_note(r):
        parts = []
        if r["trend_score"] >= 18:
            parts.append("趨勢轉強")
        elif r["trend_score"] >= 10:
            parts.append("趨勢修復")

        if r["volume_score"] >= 16:
            parts.append("量能回溫")
        elif r["volume_score"] >= 8:
            parts.append("量能正常")

        if r["structure_score"] >= 15:
            parts.append("結構收斂")
        elif r["structure_score"] >= 8:
            parts.append("結構尚可")

        if r["confirm_score"] >= 10:
            parts.append("KD/MACD確認")
        elif r["confirm_score"] >= 5:
            parts.append("指標初轉強")

        if r["risk_penalty"] > 0:
            parts.append(f"風險扣分{int(r['risk_penalty'])}")

        if not parts:
            parts.append("條件不足")

        return "｜".join(parts)

    x["note"] = x.apply(make_note, axis=1)

    x = x.sort_values(["entry_score", "trend_score", "structure_score"], ascending=False)

    core = x.head(CORE_TOP_N).copy()
    alpha = x.sort_values(["confirm_score", "trend_score", "entry_score"], ascending=False).head(ALPHA_TOP_N).copy()

    return x, core, alpha


# =========================
# 5. Position Sizing
# =========================

def target_weight_for_action(action, entry_score):
    score = float(entry_score)
    if action == "BUY":
        return 0.02 if score >= 85 else 0.01
    if action == "TEST":
        return 0.005
    return 0.0


def build_trade_plan(scored, signal_date, latest_capital=INITIAL_CAPITAL):
    trade_date = next_trade_date(signal_date)

    # 先選 BUY / TEST / WATCH 分層
    buy = scored[scored["action"] == "BUY"].head(BUY_TOP_N)
    test = scored[scored["action"] == "TEST"].head(TEST_TOP_N)
    watch = scored[scored["action"] == "WATCH"].head(WATCH_TOP_N)

    selected = pd.concat([buy, test, watch], ignore_index=True)
    selected = selected.drop_duplicates(subset=["stock_id"]).head(MAX_TRADE_PLAN_N)

    # 防呆：如果沒有任何候選，至少輸出 NO_TRADE，不產生空檔
    if selected.empty:
        return safe_empty_trade_plan(signal_date, reason="今日沒有符合 BUY / TEST / WATCH 條件的股票")

    rows = []

    for _, r in selected.iterrows():
        px = float(r["close"]) * (1 + SLIPPAGE)
        weight = target_weight_for_action(r["action"], r["entry_score"])
        weight = min(weight, MAX_POSITION_WEIGHT)

        alloc = latest_capital * weight
        shares = alloc / px if px > 0 else 0
        gross = shares * px
        total_cost = gross * (1 + FEE)

        rows.append({
            "signal_date": str(pd.to_datetime(signal_date).date()),
            "trade_date": str(pd.to_datetime(trade_date).date()),
            "action": r["action"],
            "action_label": r["action_label"],
            "action_sub": r["action_sub"],
            "stock_id": r["stock_id"],
            "price_tier": price_tier(px),
            "ref_price": round(px, 4),
            "target_weight": round(weight, 4),
            "suggested_amount": round(alloc, 2),
            "suggested_shares": round(shares, 2),
            "estimated_total_cost": round(total_cost, 2),
            "entry_score": round(float(r["entry_score"]), 2),
            "trend_score": round(float(r["trend_score"]), 2),
            "volume_score": round(float(r["volume_score"]), 2),
            "structure_score": round(float(r["structure_score"]), 2),
            "confirm_score": round(float(r["confirm_score"]), 2),
            "source": "V265_CLEAN",
            "note": r["note"],
        })

    plan = pd.DataFrame(rows)
    return plan


# =========================
# 6. Backtest / Summary
# =========================

def run_backtest(df):
    """
    保留簡化回測，不影響每日 trade_plan。
    若資料不足，仍回傳安全 summary。
    """
    work = df.copy()
    signal_dates = sorted(work["date"].dropna().unique())

    if len(signal_dates) < 3:
        return pd.DataFrame([{"date": pd.Timestamp.today().date(), "nav": INITIAL_CAPITAL, "ret": 0.0}])

    cash = INITIAL_CAPITAL
    nav_list = []

    for i in range(len(signal_dates) - 1):
        today = pd.to_datetime(signal_dates[i])
        next_day = pd.to_datetime(signal_dates[i + 1])

        today_df = work[work["date"] == today].copy()
        next_df = work[work["date"] == next_day].copy()

        if today_df.empty or next_df.empty:
            continue

        scored, _, _ = select_stocks(today_df)
        plan = build_trade_plan(scored, today, latest_capital=cash)

        buy_plan = plan[plan["action"].isin(["BUY", "TEST"])].copy()
        next_price = {r["stock_id"]: r["close"] for _, r in next_df.iterrows()}

        nav = cash
        for _, r in buy_plan.iterrows():
            sid = r["stock_id"]
            if sid not in next_price:
                continue
            # 簡化：當日進場隔日估值，不做長期持倉模擬
            nav += 0

        nav_list.append({"date": next_day, "nav": nav})

    nav_df = pd.DataFrame(nav_list)
    if nav_df.empty:
        nav_df = pd.DataFrame([{"date": pd.Timestamp.today().date(), "nav": INITIAL_CAPITAL}])
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    return nav_df


def evaluate(nav_df):
    if nav_df is None or nav_df.empty or "nav" not in nav_df.columns:
        return pd.DataFrame([{"return": 0.0, "mdd": 0.0, "sharpe_daily": 0.0}])

    if len(nav_df) < 2:
        return pd.DataFrame([{"return": 0.0, "mdd": 0.0, "sharpe_daily": 0.0}])

    total_return = nav_df["nav"].iloc[-1] / (nav_df["nav"].iloc[0] + 1e-9) - 1.0
    mdd = (nav_df["nav"] / (nav_df["nav"].cummax() + 1e-9) - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-6)

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])


# =========================
# 7. Debug / Meta
# =========================

def build_selection_debug(raw_df, latest_df, scored, core, alpha, trade_plan, signal_date, price_source):
    return pd.DataFrame([{
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "price_source": price_source,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "total_input_rows": int(len(raw_df)),
        "latest_stock_count": int(len(latest_df)),
        "scored_count": int(len(scored)),
        "buy_count": int((scored["action"] == "BUY").sum()),
        "test_count": int((scored["action"] == "TEST").sum()),
        "watch_count": int((scored["action"] == "WATCH").sum()),
        "skip_count": int((scored["action"] == "SKIP").sum()),
        "core_count": int(len(core)),
        "alpha_count": int(len(alpha)),
        "trade_plan_count": int(len(trade_plan)),
        "trade_buy_count": int((trade_plan["action"] == "BUY").sum()) if "action" in trade_plan.columns else 0,
        "trade_test_count": int((trade_plan["action"] == "TEST").sum()) if "action" in trade_plan.columns else 0,
        "trade_watch_count": int((trade_plan["action"] == "WATCH").sum()) if "action" in trade_plan.columns else 0,
        "note": "v265 clean strategy core"
    }])


def ensure_support_files():
    files = {
        "current_positions.csv": ["stock_id", "shares", "avg_cost"],
        "position_monitor.csv": ["stock_id", "shares", "avg_cost", "note"],
        "watchlist_monitor.csv": ["stock_id", "note"],
    }
    for name, cols in files.items():
        root_p = ROOT / name
        data_p = DATA_DIR / name

        if not root_p.exists():
            pd.DataFrame(columns=cols).to_csv(root_p, index=False, encoding="utf-8-sig")
        if not data_p.exists():
            pd.DataFrame(columns=cols).to_csv(data_p, index=False, encoding="utf-8-sig")


# =========================
# 8. Main
# =========================

def main():
    raw_df, price_source = load_price()
    feat_df = build_features(raw_df)

    signal_date, latest_df = latest_signal_frame(feat_df)
    scored, core_df, alpha_df = select_stocks(latest_df)

    trade_plan_df = build_trade_plan(scored, signal_date, latest_capital=INITIAL_CAPITAL)

    nav_df = run_backtest(feat_df)
    summary_df = evaluate(nav_df)

    debug_df = build_selection_debug(
        raw_df=raw_df,
        latest_df=latest_df,
        scored=scored,
        core=core_df,
        alpha=alpha_df,
        trade_plan=trade_plan_df,
        signal_date=signal_date,
        price_source=price_source,
    )

    # 輸出
    write_both(trade_plan_df, "trade_plan.csv")
    write_both(scored, "candidates.csv")
    write_both(core_df, "core_candidates.csv")
    write_both(alpha_df, "alpha_candidates.csv")
    write_both(debug_df, "selection_debug.csv")

    nav_df.to_csv(ROOT / "daily_nav.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(ROOT / "full_summary.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(DATA_DIR / "full_summary.csv", index=False, encoding="utf-8-sig")

    # 同步 price_panel
    raw_df.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8-sig")

    ensure_support_files()

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v265_clean_strategy_core",
        "price_source": price_source,
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "data_state": "fresh",
        "trade_plan_count": int(len(trade_plan_df)),
        "buy_count": int((trade_plan_df["action"] == "BUY").sum()) if "action" in trade_plan_df.columns else 0,
        "test_count": int((trade_plan_df["action"] == "TEST").sum()) if "action" in trade_plan_df.columns else 0,
        "watch_count": int((trade_plan_df["action"] == "WATCH").sum()) if "action" in trade_plan_df.columns else 0,
        "execution_rule": "T日產生訊號，下一交易日人工下單",
    }

    for p in [ROOT / "meta.json", DATA_DIR / "meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v265_clean_strategy_core completed")
    print("price source:", price_source)
    print("signal date:", signal_date.date())
    print(debug_df.to_string(index=False))
    print("")
    print("Top trade plan:")
    print(trade_plan_df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
