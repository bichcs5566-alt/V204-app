import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [c.lower() for c in df.columns]

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["symbol", "trade_date"])

    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro["trade_date"] = pd.to_datetime(macro["trade_date"])

    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].fillna(0)

    return df


def backtest(df):
    nav = INITIAL_CAPITAL
    rows = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        ret = day["close"].pct_change().mean()
        if pd.isna(ret):
            ret = 0

        nav *= (1 + ret)

        rows.append([date, nav, ret])

    out = pd.DataFrame(rows, columns=["date", "nav", "ret"])
    return out


def main():
    df = load_data()
    df = load_macro(df)

    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1] / INITIAL_CAPITAL - 1,
        "mdd": (out["nav"] / out["nav"].cummax() - 1).min()
    }])

    out.to_csv("v235_nav.csv", index=False)
    summary.to_csv("v235_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
