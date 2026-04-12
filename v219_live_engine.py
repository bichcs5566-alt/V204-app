# v219_live_engine.py
# v219（B-only + 動態倉位引擎）

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000

def load():
    return pd.read_csv("v216_positions.csv")

def prepare(df):
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df[df["engine"]=="B"].copy()
    if "wret" not in df.columns:
        df["wret"] = df["weight_portfolio"] * df["trade_ret"]
    return df.sort_values("trade_date")

def build(df):
    daily = df.groupby("trade_date")["wret"].sum().reset_index()

    daily["rolling_ret_10"] = daily["wret"].rolling(10).sum()

    daily["exposure"] = np.where(daily["rolling_ret_10"] > 0, 0.8, 0.3)

    daily["adj_ret"] = daily["wret"] * daily["exposure"]

    daily["nav"] = INITIAL_CAPITAL * (1 + daily["adj_ret"]).cumprod()

    daily["drawdown"] = daily["nav"]/daily["nav"].cummax() - 1

    return daily

def summary(daily):
    ret = daily["nav"].iloc[-1]/INITIAL_CAPITAL - 1
    sharpe = daily["adj_ret"].mean()/(daily["adj_ret"].std()+1e-9)*np.sqrt(252)
    mdd = daily["drawdown"].min()

    out = pd.DataFrame([{
        "return": ret,
        "sharpe": sharpe,
        "mdd": mdd
    }])

    out.to_csv("v219_summary.csv", index=False)
    return out

def main():
    df = prepare(load())
    daily = build(df)

    daily.to_csv("v219_daily.csv", index=False)

    s = summary(daily)

    print("==== v219 RESULT ====")
    print(s)

if __name__ == "__main__":
    main()
