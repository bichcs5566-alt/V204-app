import pandas as pd
import yfinance as yf


def safe_download(ticker):
    data = yf.download(
        ticker,
        period="1y",
        progress=False,
        auto_adjust=True,
        multi_level_index=False,
        threads=False,
    )

    if data is None or len(data) == 0:
        raise Exception(f"{ticker} download failed")

    if "Close" not in data.columns:
        raise Exception(f"{ticker} 缺少 Close 欄位，實際欄位: {list(data.columns)}")

    s = data["Close"].copy()
    s.name = ticker
    return s


def fetch_data():
    spy = safe_download("SPY")
    vix = safe_download("^VIX")
    us10y = safe_download("^TNX")

    df = pd.concat([spy, vix, us10y], axis=1).dropna().reset_index()
    df.columns = [str(c) for c in df.columns]

    if "Date" in df.columns:
        df = df.rename(columns={"Date": "trade_date"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "trade_date"})

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.rename(columns={
        "SPY": "spy",
        "^VIX": "vix",
        "^TNX": "us10y",
    })

    return df


def build_macro(df):
    df["spy_ma5"] = df["spy"].rolling(5).mean()
    df["spy_ma20"] = df["spy"].rolling(20).mean()

    df["spy_score"] = 0
    df.loc[df["spy_ma5"] > df["spy_ma20"], "spy_score"] = 1
    df.loc[df["spy_ma5"] < df["spy_ma20"], "spy_score"] = -1

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

    df["macro_score"] = df["macro_score"].clip(-1, 1)
    out = df[["trade_date", "macro_score"]].dropna().sort_values("trade_date").copy()
    return out


def main():
    df = fetch_data()
    macro = build_macro(df)
    macro.to_csv("macro_signal.csv", index=False)
    print("macro_signal.csv updated")
    print(macro.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()
