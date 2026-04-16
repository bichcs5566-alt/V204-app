import pandas as pd
import yfinance as yf

def fetch_data():
    spy = yf.download("SPY", period="1y")["Close"]
    vix = yf.download("^VIX", period="1y")["Close"]
    us10y = yf.download("^TNX", period="1y")["Close"]

    df = pd.DataFrame({
        "spy": spy,
        "vix": vix,
        "us10y": us10y
    }).dropna()

    df = df.reset_index()
    df.rename(columns={"Date": "trade_date"}, inplace=True)

    return df


def build_macro(df):
    # === SPY 趨勢 ===
    df["spy_ma5"] = df["spy"].rolling(5).mean()
    df["spy_ma20"] = df["spy"].rolling(20).mean()

    df["spy_score"] = (df["spy_ma5"] > df["spy_ma20"]).astype(int)

    # === VIX（風險）===
    df["vix_score"] = 0
    df.loc[df["vix"] < 18, "vix_score"] = 1
    df.loc[df["vix"] > 25, "vix_score"] = -1

    # === 利率變化 ===
    df["rate_change"] = df["us10y"].diff()
    df["rate_score"] = 0
    df.loc[df["rate_change"] < -0.05, "rate_score"] = 1
    df.loc[df["rate_change"] > 0.05, "rate_score"] = -1

    # === 總分 ===
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
