# v250_engine_fixed.txt
# 覆蓋成 v250_engine.py 使用
# 修正：
# 1) 自動辨識 symbol / stock_id / 股票代號
# 2) 自動辨識 date / trade_date
# 3) workflow 需先產生 price_panel_daily.csv

import os
import pandas as pd
import numpy as np

PRICE_CANDIDATES = [
    "price_panel_daily.csv",
    "data/price_panel_daily.csv",
]

INITIAL_CAPITAL = 1.0

def load_price():
    df = None
    for p in PRICE_CANDIDATES:
        if os.path.exists(p):
            df = pd.read_csv(p)
            print(f"Loaded: {p}")
            break
    if df is None:
        raise FileNotFoundError("price_panel_daily.csv not found")

    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        else:
            for c in df.columns:
                if "date" in c:
                    df["date"] = df[c]
                    break
    if "date" not in df.columns:
        raise ValueError("缺少 date / trade_date 欄位")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "stockid" in df.columns:
            df["stock_id"] = df["stockid"]
        elif "ticker" in df.columns:
            df["stock_id"] = df["ticker"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        else:
            for c in df.columns:
                if "stock" in c or "symbol" in c or "ticker" in c or "code" in c:
                    df["stock_id"] = df[c]
                    break
    if "stock_id" not in df.columns:
        raise ValueError("缺少 stock_id / symbol / ticker / code 欄位")

    if "close" not in df.columns:
        raise ValueError("缺少 close 欄位")

    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    return df.sort_values(["date", "stock_id"]).reset_index(drop=True)

def build_regime(df):
    mkt = df.groupby("date")["close"].mean()
    ma20 = mkt.rolling(20).mean()
    ma60 = mkt.rolling(60).mean()

    regime = pd.Series("DEF", index=mkt.index)
    regime[(mkt > ma20)] = "AGG"
    regime[(mkt < ma20)] = "DEF"
    regime[(mkt < ma60)] = "RISK_OFF"
    return regime

def run_backtest(df):
    df = df.copy()
    df["ret"] = df.groupby("stock_id")["close"].pct_change()
    df["mom20"] = df.groupby("stock_id")["close"].pct_change(20)

    regime = build_regime(df)
    daily_nav = []
    nav = 1.0
    dates = sorted(df["date"].unique())

    for i in range(60, len(dates) - 1):
        today = dates[i]
        tomorrow = dates[i + 1]
        today_df = df[df["date"] == today].copy()
        reg = regime.loc[today]

        recent_ret = 0.0
        if i > 60:
            recent_ret = df[df["date"] == dates[i - 1]]["ret"].mean()
            if pd.isna(recent_ret):
                recent_ret = 0.0

        if recent_ret < -0.03:
            exposure = 0.0
            picks = []
        else:
            tmp = today_df.dropna(subset=["mom20"]).copy()
            if reg == "AGG":
                picks = tmp.nlargest(10, "mom20")
                exposure = 0.95
            elif reg == "DEF":
                picks = tmp[tmp["mom20"] > 0].nlargest(2, "mom20")
                exposure = 0.30
            else:
                picks = []
                exposure = 0.0

        if len(picks) > 0:
            next_day = df[(df["date"] == tomorrow) & (df["stock_id"].isin(picks["stock_id"]))]["ret"]
            next_ret = next_day.mean()
            if pd.isna(next_ret):
                next_ret = 0.0
        else:
            next_ret = 0.0

        nav *= (1 + exposure * next_ret)
        daily_nav.append({"date": today, "nav": nav, "exposure": exposure, "regime": reg})

    nav_df = pd.DataFrame(daily_nav)
    if nav_df.empty:
        raise ValueError("nav_df 為空，請檢查資料區間")

    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    total_return = nav_df["nav"].iloc[-1] - 1.0
    mdd = (nav_df["nav"] / nav_df["nav"].cummax() - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-9)

    summary = pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe,
        "avg_exposure": nav_df["exposure"].mean(),
        "agg_day_ratio": (nav_df["regime"] == "AGG").mean(),
        "def_day_ratio": (nav_df["regime"] == "DEF").mean(),
        "risk_off_day_ratio": (nav_df["regime"] == "RISK_OFF").mean(),
    }])

    summary.to_csv("summary.csv", index=False)
    nav_df.to_csv("nav.csv", index=False)
    print(summary.to_string(index=False))

if __name__ == "__main__":
    df = load_price()
    run_backtest(df)
