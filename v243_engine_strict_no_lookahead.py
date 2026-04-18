import pandas as pd
import numpy as np
import argparse

TOP_N = 5
STOP_LOSS_1 = -0.07
STOP_LOSS_2 = -0.12
ADD_1 = 0.05
ADD_2 = 0.10

def load_data():
    price = pd.read_csv("price_panel_daily.csv", parse_dates=["date"])
    macro = pd.read_csv("macro_signal.csv", parse_dates=["date"])
    return price, macro

def prepare(price):
    price = price.sort_values(["symbol", "date"])
    price["ret"] = price.groupby("symbol")["close"].pct_change()
    price["mom10"] = price.groupby("symbol")["close"].pct_change(10)
    return price

def backtest(price, macro, start, end):
    dates = sorted(price["date"].unique())
    nav = 1.0
    nav_basis = nav

    portfolio = {}
    records = []

    for i in range(11, len(dates)-1):
        today = dates[i]
        next_day = dates[i+1]

        if today < pd.to_datetime(start) or today > pd.to_datetime(end):
            continue

        daily = price[price["date"] == today]
        macro_today = macro[macro["date"] == today]["risk_on"]

        if len(macro_today) == 0 or macro_today.values[0] == 0:
            portfolio = {}
            records.append({"date": today, "nav": nav, "count": 0})
            continue

        # === ranking（只用今天資料） ===
        ranked = daily.sort_values("mom10", ascending=False)
        candidates = ranked.head(20)["symbol"].tolist()

        # === signal（今天決定）===
        target = candidates[:TOP_N]

        # === execution（明天才做）===
        next_prices = price[price["date"] == next_day]

        # === 先賣 ===
        for s in list(portfolio.keys()):
            if s not in target:
                portfolio.pop(s)

        # === 再買 ===
        for s in target:
            if s not in portfolio:
                portfolio[s] = {
                    "weight": 1.0 / TOP_N,
                    "entry": next_prices[next_prices["symbol"] == s]["close"].values[0]
                }

        # === 報酬計算 ===
        pnl = 0
        for s in portfolio:
            row = next_prices[next_prices["symbol"] == s]
            if len(row) == 0:
                continue
            ret = row["close"].values[0] / portfolio[s]["entry"] - 1
            pnl += portfolio[s]["weight"] * ret

        nav *= (1 + pnl)

        if nav <= 0:
            raise ValueError("NAV 爆掉")

        records.append({
            "date": today,
            "nav": nav,
            "count": len(portfolio)
        })

    df = pd.DataFrame(records)
    summary = {
        "return": df["nav"].iloc[-1] - 1,
        "mdd": (df["nav"].cummax() - df["nav"]).max(),
        "avg_count": df["count"].mean()
    }

    pd.DataFrame([summary]).to_csv("summary.csv", index=False)
    df.to_csv("daily_nav.csv", index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    price, macro = load_data()
    price = prepare(price)

    backtest(price, macro, args.start, args.end)

if __name__ == "__main__":
    main()
