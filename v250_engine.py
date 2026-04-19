# v250_engine.py
# Execution-ready / strict no lookahead / robust loader

import pandas as pd
import numpy as np
import os

# =========================
# Robust loader（解你所有 date 問題）
# =========================
def load_price():
    paths = [
        "price_panel_daily.csv",
        "data/price_panel_daily.csv",
        "data/price_panel_parts/price_panel_daily_part_001.csv"
    ]

    for p in paths:
        if os.path.exists(p):
            df = pd.read_csv(p)
            print(f"Loaded: {p}")
            break
    else:
        raise FileNotFoundError("price_panel_daily.csv not found")

    # auto fix columns
    cols = [c.lower() for c in df.columns]
    df.columns = cols

    if "date" not in df.columns:
        for c in df.columns:
            if "date" in c:
                df = df.rename(columns={c: "date"})

    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "stock_id"])


# =========================
# Regime（提前判斷）
# =========================
def build_regime(df):
    mkt = df.groupby("date")["close"].mean()

    ma20 = mkt.rolling(20).mean()
    ma60 = mkt.rolling(60).mean()

    regime = pd.Series("DEF", index=mkt.index)

    regime[(mkt > ma20)] = "AGG"
    regime[(mkt < ma20)] = "DEF"
    regime[(mkt < ma60)] = "RISK_OFF"

    return regime


# =========================
# Strategy
# =========================
def run_backtest(df):

    df["ret"] = df.groupby("stock_id")["close"].pct_change()

    # momentum（不偷看）
    df["mom20"] = df.groupby("stock_id")["close"].pct_change(20)

    regime = build_regime(df)

    daily_nav = []
    nav = 1.0

    dates = sorted(df["date"].unique())

    for i in range(60, len(dates)-1):

        today = dates[i]
        tomorrow = dates[i+1]

        today_df = df[df["date"] == today].copy()

        reg = regime.loc[today]

        # =========================
        # 崩盤機制（關鍵）
        # =========================
        if i > 60:
            recent_ret = df[df["date"] == dates[i-1]]["ret"].mean()
            if recent_ret < -0.03:
                exposure = 0
                picks = []
            else:

                # =========================
                # Aggressive
                # =========================
                if reg == "AGG":
                    tmp = today_df.dropna(subset=["mom20"])
                    picks = tmp.nlargest(10, "mom20")
                    exposure = 0.95

                # =========================
                # Defensive（幾乎不做）
                # =========================
                elif reg == "DEF":
                    tmp = today_df.dropna(subset=["mom20"])
                    picks = tmp[tmp["mom20"] > 0].nlargest(2, "mom20")
                    exposure = 0.30

                # =========================
                # Risk-off（清倉）
                # =========================
                else:
                    picks = []
                    exposure = 0

        # =========================
        # 計算報酬（no lookahead）
        # =========================
        if len(picks) > 0:
            next_ret = df[
                (df["date"] == tomorrow) &
                (df["stock_id"].isin(picks["stock_id"]))
            ]["ret"].mean()
        else:
            next_ret = 0

        nav *= (1 + exposure * next_ret)
        daily_nav.append(nav)

    nav_series = pd.Series(daily_nav)

    # metrics
    ret = nav_series.iloc[-1] - 1
    mdd = (nav_series / nav_series.cummax() - 1).min()
    sharpe = nav_series.pct_change().mean() / (nav_series.pct_change().std() + 1e-9)

    summary = pd.DataFrame([{
        "return": ret,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])

    summary.to_csv("summary.csv", index=False)
    nav_series.to_csv("nav.csv", index=False)

    print(summary)


if __name__ == "__main__":
    df = load_price()
    run_backtest(df)
