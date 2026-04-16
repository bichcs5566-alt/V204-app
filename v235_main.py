import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    # 🔥 自動找日期欄
    date_col = None
    for c in ["trade_date", "date", "datetime", "signal_date"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise Exception(f"❌ 找不到日期欄位，目前欄位: {df.columns.tolist()}")

    # 🔥 標準化
    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce")

    if "symbol" not in df.columns or "close" not in df.columns:
        raise Exception(f"❌ 缺少 symbol / close，目前欄位: {df.columns.tolist()}")

    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date", "symbol", "close"])
    df = df.sort_values(["symbol", "trade_date"])

    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    macro["trade_date"] = pd.to_datetime(macro["trade_date"], errors="coerce")

    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].fillna(0)

    return df


def backtest(df):
    nav = INITIAL_CAPITAL
    rows = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        ret = day.groupby("symbol")["close"].last().pct_change().mean()
        if pd.isna(ret):
            ret = 0.0

        nav *= (1 + ret)

        rows.append([date, nav, ret])

    return pd.DataFrame(rows, columns=["date", "nav", "ret"])


def main():
    df = load_data()
    df = load_macro(df)

    out = backtest(df)

    if out.empty:
        raise Exception("❌ 沒有產出結果")

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1] / INITIAL_CAPITAL - 1,
        "mdd": (out["nav"] / out["nav"].cummax() - 1).min()
    }])

    out.to_csv("v235_nav.csv", index=False)
    summary.to_csv("v235_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
