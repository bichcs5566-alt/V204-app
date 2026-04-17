# v241_6_main.py（實盤版）

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000
TOP_N = 5

STOP_LOSS_1 = -0.07
STOP_LOSS_2 = -0.12
ADD_LV1 = 0.05
ADD_LV2 = 0.10

FEE = 0.001425 * 2
SLIPPAGE = 0.001
MAX_DD_LIMIT = -0.15


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["symbol","trade_date"])
    df["ret"] = df.groupby("symbol")["close"].pct_change()
    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    return df


def get_exposure(macro):
    if macro > 0.5: return 1.3
    elif macro > 0: return 1.1
    elif macro > -0.6: return 0.8
    else: return 0


def simulate(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    holdings = {}
    stopped = False

    nav_rows = []
    trades = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]
        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)

        if stopped:
            nav_rows.append([date,nav,0,macro,exposure,nav/peak-1,0])
            continue

        ret = 0
        to_remove = []

        for sym, pos in list(holdings.items()):
            row = day[day["symbol"]==sym]
            if row.empty: continue

            row = row.iloc[-1]
            cur = row["close"] * (1 + SLIPPAGE)
            r = row["ret"]

            pnl = cur/pos["entry"] - 1

            if pnl <= STOP_LOSS_2:
                trades.append([date,"STOP_FULL",sym,cur])
                to_remove.append(sym)

            elif pnl <= STOP_LOSS_1:
                holdings[sym]["weight"] *= 0.5

            elif pnl >= ADD_LV2:
                holdings[sym]["weight"] *= 1.1

            elif pnl >= ADD_LV1:
                holdings[sym]["weight"] *= 1.05

            ret += r * pos["weight"]

        for sym in to_remove:
            holdings.pop(sym)

        # 補股
        if exposure > 0:
            ranked = day.sort_values("close", ascending=False)

            for _, r in ranked.iterrows():
                if len(holdings) >= TOP_N: break
                if r["symbol"] not in holdings:
                    holdings[r["symbol"]] = {
                        "entry": r["close"]*(1+SLIPPAGE),
                        "weight": 1/TOP_N
                    }
                    trades.append([date,"BUY",r["symbol"],r["close"]])

        # normalize
        if len(holdings)>0:
            total = sum([holdings[s]["weight"] for s in holdings])
            for s in holdings:
                holdings[s]["weight"] = holdings[s]["weight"]/total*exposure

        ret -= FEE
        nav *= (1+ret)

        peak = max(peak, nav)
        dd = nav/peak -1

        if dd < MAX_DD_LIMIT:
            holdings = {}
            stopped = True

        nav_rows.append([date,nav,ret,macro,exposure,dd,len(holdings)])

    nav = pd.DataFrame(nav_rows,columns=["date","nav","ret","macro","exposure","dd","count"])
    trades = pd.DataFrame(trades,columns=["date","action","symbol","price"])

    return nav, trades


def main():
    df = load_data()
    df = load_macro(df)

    nav, trades = simulate(df)

    summary = pd.DataFrame([{
        "return": nav["nav"].iloc[-1]/INITIAL_CAPITAL -1,
        "mdd": nav["dd"].min(),
        "avg_count": nav["count"].mean(),
        "trade_count": len(trades)
    }])

    nav.to_csv("v241_6_nav.csv",index=False)
    trades.to_csv("v241_6_trades.csv",index=False)
    summary.to_csv("v241_6_summary.csv",index=False)

    print(summary)


if __name__ == "__main__":
    main()
