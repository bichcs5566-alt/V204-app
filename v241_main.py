# v241_main.py（完整可覆蓋版｜已修復 ZeroDivisionError）

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5

STOP_LOSS_1 = -0.07
STOP_LOSS_2 = -0.12
ADD_WINNER = 0.05


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date","date","datetime","signal_date"] if c in df.columns), None)
    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date","symbol","close"])
    df = df.sort_values(["symbol","trade_date"])

    df["ret"] = df.groupby("symbol")["close"].pct_change()
    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)
    return df


def build_features(df):
    g = df.groupby("symbol")
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def get_exposure(macro):
    if macro > 0.5:
        return 1.3
    elif macro > 0:
        return 1.1
    elif macro > -0.6:
        return 0.8
    else:
        return 0.0


def rank(day):
    d = day.dropna(subset=["mom5","mom10"]).copy()
    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    return d.sort_values("score", ascending=False)


def simulate(df):
    df = build_features(df)

    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    holdings = {}
    nav_rows = []
    trade_rows = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]
        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)
        ranked = rank(day)

        ret = 0
        to_remove = []

        for sym, pos in list(holdings.items()):
            if sym in day["symbol"].values:
                row = day[day["symbol"]==sym].iloc[-1]
                cur = row["close"]
                r = row["ret"] if pd.notna(row["ret"]) else 0

                pnl = cur/pos["entry"] -1

                if pnl <= STOP_LOSS_2:
                    trade_rows.append([date,"STOP_FULL",sym,cur])
                    to_remove.append(sym)

                elif pnl <= STOP_LOSS_1:
                    holdings[sym]["weight"] *= 0.5
                    trade_rows.append([date,"STOP_PART",sym,cur])

                elif pnl >= ADD_WINNER:
                    holdings[sym]["weight"] *= 1.2
                    trade_rows.append([date,"ADD",sym,cur])

                ret += r * pos["weight"]
                holdings[sym]["last"] = cur

        for sym in to_remove:
            holdings.pop(sym)

        if exposure > 0:
            for _, r in ranked.iterrows():
                if len(holdings) >= TOP_N:
                    break
                if r["symbol"] not in holdings:
                    holdings[r["symbol"]] = {
                        "entry": r["close"],
                        "last": r["close"],
                        "weight": 1/TOP_N
                    }
                    trade_rows.append([date,"BUY",r["symbol"],r["close"]])

        # ⭐ 修正重點：避免 total_w = 0
        if len(holdings) > 0:
            total_w = sum(float(holdings[s]["weight"]) for s in holdings)

            if total_w <= 0:
                equal_w = exposure / len(holdings)
                for s in holdings:
                    holdings[s]["weight"] = equal_w
            else:
                for s in holdings:
                    holdings[s]["weight"] = (float(holdings[s]["weight"]) / total_w) * exposure

        ret = max(min(ret,0.12),-0.08)

        nav *= (1+ret)
        peak = max(peak, nav)
        dd = nav/peak -1

        nav_rows.append([date,nav,ret,macro,exposure,dd,len(holdings)])

    return pd.DataFrame(nav_rows,columns=["date","nav","ret","macro","exposure","dd","count"]), pd.DataFrame(trade_rows,columns=["date","action","symbol","price"])


def main():
    df = load_data()
    df = load_macro(df)

    nav, trades = simulate(df)

    summary = pd.DataFrame([{
        "return": nav["nav"].iloc[-1]/INITIAL_CAPITAL -1,
        "mdd": nav["dd"].min(),
        "avg_exposure": nav["exposure"].mean(),
        "avg_count": nav["count"].mean(),
        "trade_count": len(trades)
    }])

    nav.to_csv("v241_nav.csv",index=False)
    trades.to_csv("v241_trades.csv",index=False)
    summary.to_csv("v241_summary.csv",index=False)

    print(summary)


if __name__ == "__main__":
    main()
