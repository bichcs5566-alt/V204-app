import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5


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


def select_top(day):
    d = day.dropna(subset=["mom5","mom10"]).copy()

    if len(d) < TOP_N:
        return pd.DataFrame()

    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    top = d.sort_values("score", ascending=False).head(TOP_N)

    return top


def get_exposure(macro):
    if macro > 0.5:
        return 1.3
    elif macro > 0:
        return 1.1
    elif macro > -0.6:
        return 0.8
    else:
        return 0.0


def backtest_and_generate(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    nav_rows = []
    trade_rows = []

    df = build_features(df)

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)

        if exposure == 0:
            ret = 0.0
            picks = pd.DataFrame()
        else:
            picks = select_top(day)

            if picks.empty:
                ret = 0.0
            else:
                weight = 1.0 / len(picks)
                picks["weight"] = weight * exposure

                ret = (picks["ret"] * picks["weight"]).sum()

                # 🔥 記錄交易（實戰用）
                for _, row in picks.iterrows():
                    trade_rows.append([
                        date,
                        row["symbol"],
                        row["close"],
                        row["weight"]
                    ])

        # 風控
        ret = max(min(ret, 0.12), -0.08)

        nav *= (1 + ret)
        peak = max(peak, nav)

        dd = nav / peak - 1

        nav_rows.append([date, nav, ret, macro, exposure, dd])

    nav_df = pd.DataFrame(nav_rows, columns=[
        "date","nav","ret","macro_score","exposure","dd"
    ])

    trade_df = pd.DataFrame(trade_rows, columns=[
        "date","symbol","price","weight"
    ])

    return nav_df, trade_df


def main():
    df = load_data()
    df = load_macro(df)

    nav_df, trade_df = backtest_and_generate(df)

    summary = pd.DataFrame([{
        "return": nav_df["nav"].iloc[-1]/INITIAL_CAPITAL - 1,
        "mdd": nav_df["dd"].min(),
        "avg_exposure": nav_df["exposure"].mean()
    }])

    nav_df.to_csv("v237_nav.csv", index=False)
    trade_df.to_csv("v237_trades.csv", index=False)
    summary.to_csv("v237_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
