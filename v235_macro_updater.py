import pandas as pd
import yfinance as yf


def safe_download(ticker):
    data = yf.download(ticker, period="1y", progress=False)

    if data is None or len(data) == 0:
        raise Exception(f"{ticker} download failed")

    return data["Close"]


def fetch_data():
    spy = safe_download("SPY")
    vix = safe_download("^VIX")
    us10y = safe_download("^TNX")

    # 🔥 對齊 index（非常重要）
    df = pd.concat([spy, vix, us10y], axis=1)
    df.columns = ["spy", "vix", "us10y"]

    df = df.dropna().reset_index()
    df.rename(columns={"Date": "trade_date"}, inplace=True)

    return df


def build_macro(df):
    df["spy_ma5"] = df["spy"].rolling(5).mean()
    df["spy_ma20"] = df["spy"].rolling(20).mean()

    df["spy_score"] = (df["spy_ma5"] > df["spy_ma20"]).astype(int)

    df["vix_score"] = 0
    df.loc[df["vix"] < 18, "vix_score"] = 1
    df.loc[df["vix"] > 25, "vix_score"] = -1

    df["rate_change"] = df["us10y"].diff()

    df["rate_score"] = 0
    df.loc[df["rate_change"] < -0.05, "rate_score"] = 1
    df.loc[df["rate_change"] > 0.05, "rate_score"] = -1

    df["macro_score"] = (
        df["spy_score"] * 0.5 +
        df["vix_score"] * 0.3 +
        df["rate_score"] * 0.2
    )

    return df[["trade_date", "macro_score"]]


def main():
    df = fetch_data()
    macro = build_macro(df)

    macro.to_csv("macro_signal.csv", index=False)
    print("macro_signal.csv updated")


if __name__ == "__main__":
    main()
