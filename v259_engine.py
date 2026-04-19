import pandas as pd
import numpy as np
import os

CORE_WEIGHT = 0.85
ALPHA_WEIGHT = 0.15

CORE_TOP_N = 10
ALPHA_TOP_N = 3

RET_CAP = 0.08
RET_FLOOR = -0.05


def load_price():
    if not os.path.exists("price_panel_daily.csv"):
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower() for c in df.columns]

    if "date" not in df.columns:
        if "datetime" in df.columns:
            df["date"] = df["datetime"]
        elif "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        else:
            raise ValueError("no date column")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        else:
            raise ValueError("no stock_id column")

    if "close" not in df.columns:
        raise ValueError("no close column")

    if "volume" not in df.columns:
        df["volume"] = np.nan

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str)

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)

    return df


def build_features(df):
    g = df.groupby("stock_id")

    df["ret1"] = g["close"].pct_change()
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)
    df["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["volume_spike"] = df["volume"] / (df["volume_ma20"] + 1e-6)

    return df


def run_strategy(df):
    df = df.dropna(subset=["ret1", "mom20", "mom60", "vol20"]).copy()

    nav = 1.0
    rows = []

    for date, d in df.groupby("date"):
        d = d.copy()

        # 避免極端異常股
        d = d[d["ret1"].abs() < 0.15].copy()

        core = d[d["mom20"] > 0].copy()
        core = core.sort_values("mom20", ascending=False).head(CORE_TOP_N)
        core_ret = core["ret1"].mean() if len(core) > 0 else 0.0

        d["trend"] = d["mom20"] * 0.6 + d["mom60"] * 0.4
        d["quality"] = d["trend"] / (d["vol20"] + 1e-6)

        alpha = d[(d["volume_spike"] > 1.3) & (d["mom20"] > 0)].copy()
        alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)
        alpha_ret = alpha["ret1"].mean() if len(alpha) > 0 else 0.0

        total_ret = CORE_WEIGHT * core_ret + ALPHA_WEIGHT * alpha_ret
        total_ret = min(total_ret, RET_CAP)
        total_ret = max(total_ret, RET_FLOOR)

        nav *= (1.0 + total_ret)

        rows.append(
            {
                "date": date,
                "nav": nav,
                "ret": total_ret,
                "core_ret": core_ret,
                "alpha_ret": alpha_ret,
            }
        )

    return pd.DataFrame(rows)


def evaluate(nav_df):
    total_return = nav_df["nav"].iloc[-1] - 1.0
    mdd = (nav_df["nav"] / nav_df["nav"].cummax() - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-6)

    return pd.DataFrame(
        [
            {
                "return": total_return,
                "mdd": mdd,
                "sharpe_daily": sharpe,
            }
        ]
    )


if __name__ == "__main__":
    df = load_price()
    df = build_features(df)
    nav_df = run_strategy(df)
    summary_df = evaluate(nav_df)

    nav_df.to_csv("daily_nav.csv", index=False)
    summary_df.to_csv("full_summary.csv", index=False)

    print(summary_df.to_string(index=False))
