import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"

df = pd.read_csv(PRICE_FILE)

# 你的檔案是 date，不是 trade_date
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["date", "symbol"]).copy()

# 這份資料只有 close，沒有 open，所以先做最簡化版：
# 用隔日 close / 當日 close - 1 當作單日持有報酬
df["next_close"] = df.groupby("symbol")["close"].shift(-1)
df["ret_1d"] = df["next_close"] / df["close"] - 1

# 動能特徵
g = df.groupby("symbol")
df["mom_5"] = g["close"].pct_change(5)
df["mom_10"] = g["close"].pct_change(10)
df["mom_20"] = g["close"].pct_change(20)
df["std_5"] = g["ret_1d"].rolling(5).std().reset_index(level=0, drop=True)

dates = sorted(df["date"].dropna().unique())

capital = 100000.0
nav = capital
nav_rows = []
trade_rows = []

for d in dates[:-1]:
    day = df[df["date"] == d].copy()

    if day.empty:
        nav_rows.append({
            "date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "nav": nav,
        })
        continue

    # B-only 主系統：順勢
    candidates = day[
        day["mom_5"].notna() &
        day["mom_10"].notna() &
        day["std_5"].notna() &
        (day["mom_5"] > 0.03) &
        (day["mom_10"] > 0.05) &
        (day["std_5"] < 0.08)
    ].copy()

    # 避免 0 trades：如果太少，退化成純排序前 8
    if len(candidates) < 3:
        fallback = day[day["mom_5"].notna()].copy()
        candidates = fallback.sort_values("mom_5", ascending=False).head(8).copy()
    else:
        candidates = candidates.sort_values("mom_10", ascending=False).head(8).copy()

    candidates = candidates[candidates["ret_1d"].notna()].copy()

    if candidates.empty:
        nav_rows.append({
            "date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "nav": nav,
        })
        continue

    # 總曝險控制
    gross_exposure = 0.5 if len(candidates) >= 5 else 0.35
    weight = gross_exposure / len(candidates)

    candidates["weight"] = weight
    candidates["wret"] = candidates["ret_1d"] * candidates["weight"]

    daily_ret = candidates["wret"].sum()
    nav *= (1 + daily_ret)

    for _, r in candidates.iterrows():
        trade_rows.append({
            "engine": "B",
            "date": d,
            "symbol": r["symbol"],
            "market": r.get("market", ""),
            "close": r["close"],
            "mom_5": r["mom_5"],
            "mom_10": r["mom_10"],
            "std_5": r["std_5"],
            "ret_1d": r["ret_1d"],
            "weight": r["weight"],
            "wret": r["wret"],
        })

    nav_rows.append({
        "date": d,
        "daily_ret": daily_ret,
        "holdings": len(candidates),
        "gross_exposure": gross_exposure,
        "cash_mode": False,
        "nav": nav,
    })

nav_df = pd.DataFrame(nav_rows)
trades_df = pd.DataFrame(trade_rows)

if not nav_df.empty:
    nav_df["cum_return"] = nav_df["nav"] / capital - 1
    nav_df["peak"] = nav_df["nav"].cummax()
    nav_df["drawdown"] = nav_df["nav"] / nav_df["peak"] - 1
else:
    nav_df = pd.DataFrame(columns=["date", "daily_ret", "holdings", "gross_exposure", "cash_mode", "nav", "cum_return", "peak", "drawdown"])

# summary
if not nav_df.empty:
    daily = nav_df["daily_ret"].fillna(0.0)
    sharpe = 0.0
    if len(daily) > 1 and daily.std(ddof=1) > 0:
        sharpe = (daily.mean() / daily.std(ddof=1)) * np.sqrt(252)

    summary = pd.DataFrame([{
        "start_date": str(nav_df["date"].min().date()),
        "end_date": str(nav_df["date"].max().date()),
        "initial_capital": capital,
        "final_nav": float(nav_df["nav"].iloc[-1]),
        "total_return": float(nav_df["nav"].iloc[-1] / capital - 1),
        "trading_days": int(len(nav_df)),
        "nonzero_ret_days": int((nav_df["daily_ret"].abs() > 0).sum()),
        "avg_holdings": float(nav_df["holdings"].mean()),
        "avg_exposure": float(nav_df["gross_exposure"].mean()),
        "cash_days": int(nav_df["cash_mode"].sum()),
        "sharpe": float(sharpe),
        "mdd": float(nav_df["drawdown"].min()),
    }])
else:
    summary = pd.DataFrame([{
        "start_date": None,
        "end_date": None,
        "initial_capital": capital,
        "final_nav": capital,
        "total_return": 0.0,
        "trading_days": 0,
        "nonzero_ret_days": 0,
        "avg_holdings": 0.0,
        "avg_exposure": 0.0,
        "cash_days": 0,
        "sharpe": 0.0,
        "mdd": 0.0,
    }])

nav_df.to_csv("v223_1_nav.csv", index=False)
trades_df.to_csv("v223_1_B_trades.csv", index=False)
summary.to_csv("v223_1_summary.csv", index=False)

print("DONE B MAIN")
print(summary.to_string(index=False))
