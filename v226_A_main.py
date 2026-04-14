# v226_A_main.py
# v226 A 引擎（攻擊型短線引擎）
# 目標：
# 1. 與 B 引擎完全分開
# 2. 偏攻擊、少檔、高集中
# 3. 保留基本風控，避免單日爆炸
# 4. 輸出 nav / summary / trades / trade_stats

import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"
INITIAL_CAPITAL = 100000.0

df = pd.read_csv(PRICE_FILE)
df.columns = [c.strip().lower() for c in df.columns]

if "trade_date" not in df.columns:
    if "date" in df.columns:
        df["trade_date"] = df["date"]
    elif "datetime" in df.columns:
        df["trade_date"] = df["datetime"]
    else:
        raise Exception(f"❌ 沒有找到 trade_date/date/datetime 欄位，目前欄位: {df.columns.tolist()}")

for col in ["symbol", "close"]:
    if col not in df.columns:
        raise Exception(f"❌ 缺少基本欄位: {col}，目前欄位: {df.columns.tolist()}")

df["trade_date"] = pd.to_datetime(df["trade_date"])
df["symbol"] = df["symbol"].astype(str)
df["close"] = pd.to_numeric(df["close"], errors="coerce")
df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

g = df.groupby("symbol", group_keys=False)

# 無偷看：用當日以前資料形成訊號，隔日報酬作為持有收益
if "ret_1d" not in df.columns:
    df["ret_1d"] = g["close"].pct_change().shift(-1)

if "mom_3" not in df.columns:
    df["mom_3"] = g["close"].pct_change(3)

if "mom_5" not in df.columns:
    df["mom_5"] = g["close"].pct_change(5)

if "mom_10" not in df.columns:
    df["mom_10"] = g["close"].pct_change(10)

if "std_3" not in df.columns:
    raw_ret = g["close"].pct_change()
    df["std_3"] = raw_ret.groupby(df["symbol"]).rolling(3).std().reset_index(level=0, drop=True)

if "std_5" not in df.columns:
    raw_ret = g["close"].pct_change()
    df["std_5"] = raw_ret.groupby(df["symbol"]).rolling(5).std().reset_index(level=0, drop=True)

# 市場 regime：A 引擎也需要，但比 B 更偏進攻
market = (
    df.groupby("trade_date", as_index=False)["close"]
    .mean()
    .rename(columns={"close": "market_close"})
)
market["ma5"] = market["market_close"].rolling(5).mean()
market["ma10"] = market["market_close"].rolling(10).mean()
market["ma20"] = market["market_close"].rolling(20).mean()

# A 引擎：只要短期市場不差就可進攻
market["risk_on"] = np.where(
    market["ma20"].notna(),
    (market["ma5"] > market["ma10"]) & (market["ma10"] > market["ma20"]),
    (market["ma5"] > market["ma10"])
)

df = df.merge(market[["trade_date", "risk_on"]], on="trade_date", how="left")

def select_A(sub: pd.DataFrame):
    base = sub[
        sub["ret_1d"].notna() &
        sub["mom_3"].notna() &
        sub["mom_5"].notna()
    ].copy()

    if base.empty:
        return base, "empty"

    # 嚴格版：強短動能 + 波動不過高
    strict = base[
        (base["mom_3"] > 0.03) &
        (base["mom_5"] > 0.05) &
        (base["mom_10"].fillna(0) > 0.02) &
        (base["std_3"].fillna(999) < 0.12)
    ].copy()

    if len(strict) >= 3:
        strict["score"] = (
            strict["mom_3"].fillna(0) * 0.50 +
            strict["mom_5"].fillna(0) * 0.35 +
            strict["mom_10"].fillna(0) * 0.15
        )
        strict = strict.sort_values("score", ascending=False).head(4)
        return strict, "strict"

    # fallback：只看短動能排序，確保有交易
    ranked = base.copy()
    ranked["score"] = (
        ranked["mom_3"].fillna(0) * 0.60 +
        ranked["mom_5"].fillna(0) * 0.30 +
        ranked["mom_10"].fillna(0) * 0.10 -
        ranked["std_3"].fillna(0.2) * 0.10
    )
    ranked = ranked.sort_values("score", ascending=False).head(4)

    if len(ranked) >= 2:
        return ranked, "fallback_rank"

    min2 = base.sort_values(["mom_3", "mom_5"], ascending=False).head(min(2, len(base)))
    return min2, "fallback_min2"

dates = sorted(df["trade_date"].dropna().unique())
nav = INITIAL_CAPITAL
peak = INITIAL_CAPITAL

nav_rows = []
trade_rows = []

for d in dates:
    day = df[df["trade_date"] == d].copy()
    if day.empty:
        continue

    risk_on = bool(day["risk_on"].iloc[0]) if pd.notna(day["risk_on"].iloc[0]) else False

    if not risk_on:
        nav_rows.append({
            "trade_date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "risk_on": False,
            "select_mode": "cash",
            "nav": nav,
            "drawdown": nav / peak - 1.0,
        })
        continue

    picks, select_mode = select_A(day)

    if picks.empty:
        nav_rows.append({
            "trade_date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "risk_on": True,
            "select_mode": "empty",
            "nav": nav,
            "drawdown": nav / peak - 1.0,
        })
        continue

    # A 引擎保留攻擊性，但止損更明確
    picks["ret_1d"] = picks["ret_1d"].clip(lower=-0.07)

    if select_mode == "strict":
        gross_exposure = 0.90
    elif select_mode == "fallback_rank":
        gross_exposure = 0.70
    else:
        gross_exposure = 0.50

    # 集中持股：少檔高權重
    picks = picks.sort_values("score", ascending=False).reset_index(drop=True)
    n = len(picks)

    if n == 1:
        weights = [1.0]
    elif n == 2:
        weights = [0.6, 0.4]
    elif n == 3:
        weights = [0.5, 0.3, 0.2]
    else:
        weights = [0.4, 0.3, 0.2, 0.1]

    picks["weight_engine"] = weights[:n]
    picks["weight_portfolio"] = picks["weight_engine"] * gross_exposure
    picks["wret"] = picks["ret_1d"] * picks["weight_portfolio"]

    daily_ret = float(picks["wret"].sum())

    nav *= (1.0 + daily_ret)
    peak = max(peak, nav)
    drawdown = nav / peak - 1.0

    nav_rows.append({
        "trade_date": d,
        "daily_ret": daily_ret,
        "holdings": int(n),
        "gross_exposure": gross_exposure,
        "cash_mode": False,
        "risk_on": True,
        "select_mode": select_mode,
        "nav": nav,
        "drawdown": drawdown,
        "avg_trade_ret": float(picks["ret_1d"].mean()),
    })

    out = picks.copy()
    out["engine"] = "A"
    out["trade_date"] = d
    out["select_mode"] = select_mode
    out["capital_after_trade"] = nav

    keep_cols = [
        "engine", "trade_date", "symbol", "market", "close",
        "mom_3", "mom_5", "mom_10", "std_3", "std_5",
        "ret_1d", "score", "weight_engine", "weight_portfolio",
        "wret", "select_mode", "capital_after_trade"
    ]
    for c in keep_cols:
        if c not in out.columns:
            out[c] = np.nan

    trade_rows.append(out[keep_cols])

nav_df = pd.DataFrame(nav_rows)

if trade_rows:
    trades_df = pd.concat(trade_rows, ignore_index=True)
else:
    trades_df = pd.DataFrame(columns=[
        "engine", "trade_date", "symbol", "market", "close",
        "mom_3", "mom_5", "mom_10", "std_3", "std_5",
        "ret_1d", "score", "weight_engine", "weight_portfolio",
        "wret", "select_mode", "capital_after_trade"
    ])

if nav_df.empty:
    summary = pd.DataFrame([{
        "start_date": None,
        "end_date": None,
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": INITIAL_CAPITAL,
        "total_return": 0.0,
        "trading_days": 0,
        "nonzero_ret_days": 0,
        "avg_holdings": 0.0,
        "avg_exposure": 0.0,
        "cash_days": 0,
        "risk_on_days": 0,
        "strict_days": 0,
        "fallback_days": 0,
        "sharpe": 0.0,
        "mdd": 0.0,
    }])
else:
    daily = nav_df["daily_ret"].fillna(0.0)
    sharpe = 0.0
    if len(daily) > 1 and daily.std(ddof=1) > 0:
        sharpe = float((daily.mean() / daily.std(ddof=1)) * np.sqrt(252))

    summary = pd.DataFrame([{
        "start_date": str(pd.to_datetime(nav_df["trade_date"].min()).date()),
        "end_date": str(pd.to_datetime(nav_df["trade_date"].max()).date()),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": float(nav_df["nav"].iloc[-1]),
        "total_return": float(nav_df["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "trading_days": int(len(nav_df)),
        "nonzero_ret_days": int((nav_df["daily_ret"].abs() > 0).sum()),
        "avg_holdings": float(nav_df["holdings"].mean()),
        "avg_exposure": float(nav_df["gross_exposure"].mean()),
        "cash_days": int(nav_df["cash_mode"].sum()),
        "risk_on_days": int(nav_df["risk_on"].fillna(False).sum()),
        "strict_days": int((nav_df["select_mode"] == "strict").sum()),
        "fallback_days": int(nav_df["select_mode"].isin(["fallback_rank", "fallback_min2"]).sum()),
        "sharpe": sharpe,
        "mdd": float(nav_df["drawdown"].min()),
    }])

if not trades_df.empty:
    trade_stats = pd.DataFrame([{
        "rows": int(len(trades_df)),
        "symbols": int(trades_df["symbol"].nunique()),
        "avg_ret_1d": float(trades_df["ret_1d"].mean()),
        "avg_wret": float(trades_df["wret"].mean()),
        "win_rate": float((trades_df["ret_1d"] > 0).mean()),
        "strict_rows": int((trades_df["select_mode"] == "strict").sum()),
        "fallback_rows": int(trades_df["select_mode"].isin(["fallback_rank", "fallback_min2"]).sum()),
    }])
else:
    trade_stats = pd.DataFrame([{
        "rows": 0,
        "symbols": 0,
        "avg_ret_1d": 0.0,
        "avg_wret": 0.0,
        "win_rate": 0.0,
        "strict_rows": 0,
        "fallback_rows": 0
    }])

nav_df.to_csv("v226_A_nav.csv", index=False)
trades_df.to_csv("v226_A_trades.csv", index=False)
summary.to_csv("v226_A_summary.csv", index=False)
trade_stats.to_csv("v226_A_trade_stats.csv", index=False)

print("✅ DONE V226 A ENGINE")
print("Columns:", df.columns.tolist())
print(summary.to_string(index=False))
print(trade_stats.to_string(index=False))
