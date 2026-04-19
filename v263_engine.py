import pandas as pd
import numpy as np
import os

CORE_WEIGHT = 0.8
ALPHA_WEIGHT = 0.2

CORE_TOP_N = 20
ALPHA_TOP_N = 6

MAX_POSITION_WEIGHT = 0.12

FEE = 0.0015
SLIPPAGE = 0.001

INITIAL_CAPITAL = 1_000_000


def load_price():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        df["date"] = df.get("trade_date", df.get("datetime"))

    if "stock_id" not in df.columns:
        df["stock_id"] = df.get("symbol", df.get("code"))

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    return df


def build_features(df):
    g = df.groupby("stock_id")

    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)

    return df


def select_stocks(d):

    core = d[d["mom20"] > -0.02].copy()
    core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
    core = core.sort_values("score", ascending=False).head(CORE_TOP_N)

    alpha = d[d["mom20"] > 0].copy()
    alpha["quality"] = (alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4) / (alpha["vol20"] + 1e-6)
    alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)

    return core, alpha


def run_backtest(df):

    df = df.dropna().copy()
    dates = sorted(df["date"].unique())

    capital = INITIAL_CAPITAL
    holdings = {}

    nav_list = []

    for i in range(len(dates) - 1):
        today = dates[i]
        next_day = dates[i + 1]

        today_df = df[df["date"] == today]
        next_df = df[df["date"] == next_day]

        next_price = {r["stock_id"]: r["close"] for _, r in next_df.iterrows()}

        core, alpha = select_stocks(today_df)

        target = {}

        for _, r in core.iterrows():
            target[r["stock_id"]] = CORE_WEIGHT / CORE_TOP_N

        for _, r in alpha.iterrows():
            target[r["stock_id"]] = target.get(r["stock_id"], 0) + ALPHA_WEIGHT / ALPHA_TOP_N

        nav = capital
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        new_holdings = {}

        for s, w in target.items():
            if s not in next_price:
                continue

            price = next_price[s] * (1 + SLIPPAGE)
            value = nav * min(w, MAX_POSITION_WEIGHT)
            shares = value / price

            cost = price * shares * (1 + FEE)

            if cost > capital:
                continue

            capital -= cost
            new_holdings[s] = {
                "shares": shares,
                "cost": price
            }

        holdings = new_holdings

        nav = capital
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        nav_list.append({
            "date": next_day,
            "nav": nav
        })

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0)

    return nav_df


def evaluate(nav):
    total_return = nav["nav"].iloc[-1] / nav["nav"].iloc[0] - 1
    mdd = (nav["nav"] / nav["nav"].cummax() - 1).min()
    sharpe = nav["ret"].mean() / (nav["ret"].std() + 1e-6)

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])


if __name__ == "__main__":
    df = load_price()
    df = build_features(df)

    nav = run_backtest(df)
    summary = evaluate(nav)

    nav.to_csv("daily_nav.csv", index=False)
    summary.to_csv("full_summary.csv", index=False)

    print(summary)
