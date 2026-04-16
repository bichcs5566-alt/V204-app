import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = None
    for c in ["trade_date", "date", "datetime", "signal_date"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise Exception(f"找不到日期欄位: {df.columns.tolist()}")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date", "symbol", "close"])
    df = df.sort_values(["symbol", "trade_date"])

    # 🔥 正確報酬（跨天）
    df["ret"] = df.groupby("symbol")["close"].pct_change()

    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    macro["trade_date"] = pd.to_datetime(macro["trade_date"])
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")

    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)

    return df


# 🔥 動能特徵
def build_features(df):
    g = df.groupby("symbol", group_keys=False)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


# 🔥 核心選股（v236）
def select_stocks(day):

    d = day.dropna(subset=["ret", "mom5", "mom10"]).copy()
    if len(d) < 10:
        return 0.0

    # 🔥 分數
    d["score"] = d["mom10"] * 0.6 + d["mom5"] * 0.4

    # 🔥 Top 10（集中）
    top = d.sort_values("score", ascending=False).head(10)

    return float(top["ret"].mean())


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    rows = []

    df = build_features(df)

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        if len(day) == 0:
            continue

        macro = float(day["macro_score"].iloc[0])

        # 🔥 v236：macro 控制「是否交易」
        if macro < -0.3:
            ret = 0.0
            exposure = 0.0

        else:
            base_ret = select_stocks(day)

            # 🔥 macro 控制倉位
            if macro > 0.5:
                exposure = 1.2
            elif macro > 0:
                exposure = 1.0
            else:
                exposure = 0.7

            ret = base_ret * exposure

        # 🔥 風控（避免異常）
        ret = max(min(ret, 0.15), -0.12)

        nav *= (1 + ret)
        peak = max(peak, nav)

        dd = nav / peak - 1

        rows.append([
            date,
            nav,
            ret,
            macro,
            exposure,
            dd
        ])

    out = pd.DataFrame(rows, columns=[
        "date", "nav", "ret", "macro_score", "exposure", "dd"
    ])

    return out


def main():
    df = load_data()
    df = load_macro(df)

    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1] / INITIAL_CAPITAL - 1,
        "mdd": out["dd"].min(),
        "avg_exposure": out["exposure"].mean()
    }])

    out.to_csv("v236_nav.csv", index=False)
    summary.to_csv("v236_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
