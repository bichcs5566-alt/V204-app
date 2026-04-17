# v238 main (final capital scaling version)
import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5
STOP_LOSS = -0.08
COOLDOWN_DAYS = 3

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
    macro.columns = [str(c).lower().strip() for c in macro.columns]
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
    cooldown = {}

    nav_rows = []
    trade_rows = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]
        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)

        ranked = rank(day)

        # cooldown decrement
        for k in list(cooldown.keys()):
            cooldown[k] -= 1
            if cooldown[k] <= 0:
                cooldown.pop(k)

        # update holdings
        to_remove = []
        ret = 0.0

        for sym, pos in holdings.items():
            row = day[day["symbol"] == sym]
            if not row.empty:
                row = row.iloc[-1]
                r = row["ret"] if pd.notna(row["ret"]) else 0

                if row["close"]/pos["entry"] - 1 <= STOP_LOSS:
                    trade_rows.append([date,"STOP",sym,row["close"]])
                    cooldown[sym] = COOLDOWN_DAYS
                    to_remove.append(sym)
                else:
                    ret += r * pos["weight"]
                    holdings[sym]["last"] = row["close"]

        for sym in to_remove:
            holdings.pop(sym)

        if exposure > 0:
            top = ranked.head(10)

            # fill positions
            for _, r in top.iterrows():
                sym = r["symbol"]
                if sym in holdings or sym in cooldown:
                    continue
                if len(holdings) < TOP_N:
                    holdings[sym] = {"entry":r["close"],"last":r["close"],"weight":0,"score":r["score"]}
                    trade_rows.append([date,"BUY",sym,r["close"]])
                else:
                    break

        # dynamic weighting (core improvement)
        if len(holdings) > 0:
            scores = np.array([v["score"] for v in holdings.values()])
            scores = np.maximum(scores, 0.0001)
            weights = scores / scores.sum()

            for i, sym in enumerate(holdings.keys()):
                holdings[sym]["weight"] = weights[i] * exposure

        ret = max(min(ret,0.15),-0.1)

        nav *= (1+ret)
        peak = max(peak, nav)
        dd = nav/peak - 1

        nav_rows.append([date,nav,ret,macro,exposure,dd,len(holdings)])

    return pd.DataFrame(nav_rows,columns=["date","nav","ret","macro","exposure","dd","count"]), pd.DataFrame(trade_rows,columns=["date","action","symbol","price"])

def main():
    df = load_data()
    df = load_macro(df)

    nav, trades = simulate(df)

    summary = pd.DataFrame([{
        "return": nav["nav"].iloc[-1]/INITIAL_CAPITAL - 1,
        "mdd": nav["dd"].min(),
        "avg_exposure": nav["exposure"].mean()
    }])

    nav.to_csv("v238_nav.csv",index=False)
    trades.to_csv("v238_trades.csv",index=False)
    summary.to_csv("v238_summary.csv",index=False)

    print(summary)

if __name__ == "__main__":
    main()
