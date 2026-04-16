import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0


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


def select_stocks(day):
    d = day.dropna(subset=["ret","mom5","mom10"]).copy()

    if len(d) < 5:
        return 0.0

    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    top = d.sort_values("score", ascending=False).head(5)

    return float(top["ret"].mean())


def get_exposure(macro):
    if macro > 0.5:
        return 1.3
    elif macro > 0:
        return 1.1
    elif macro > -0.6:
        return 0.8
    else:
        return 0.0


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    rows = []
    ret_hist = []

    df = build_features(df)

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)

        if exposure == 0:
            ret = 0.0
        else:
            base_ret = select_stocks(day)
            ret = base_ret * exposure

            # 🔥 連虧控制
            if len(ret_hist) >= 3 and all(r < 0 for r in ret_hist[-3:]):
                ret *= 0.8

            # 🔥 回撤控制
            dd = nav / peak - 1
            if dd < -0.05:
                ret *= 0.7
            if dd < -0.08:
                ret *= 0.5

        # 🔥 真實波動
        ret = max(min(ret, 0.12), -0.08)

        nav *= (1 + ret)
        peak = max(peak, nav)

        dd = nav / peak - 1

        rows.append([date, nav, ret, macro, exposure, dd])
        ret_hist.append(ret)

    return pd.DataFrame(rows, columns=[
        "date","nav","ret","macro_score","exposure","dd"
    ])


def main():
    df = load_data()
    df = load_macro(df)

    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1]/INITIAL_CAPITAL - 1,
        "mdd": out["dd"].min(),
        "avg_exposure": out["exposure"].mean()
    }])

    out.to_csv("v236_2_nav.csv", index=False)
    summary.to_csv("v236_2_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
