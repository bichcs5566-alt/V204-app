# v214_dynamic_portfolio_engine.py
# 無偷看 + 動能門檻 + 不強制持股 + 空倉機制 + 簡化主力邏輯

import pandas as pd
import numpy as np

INPUT_PATH = "price_panel.csv"
OUTPUT_SUMMARY = "v214_summary.csv"
OUTPUT_LOG = "v214_daily_log.csv"

MOM_THRESHOLD = 0.15
MAX_POSITIONS = 20
INITIAL_CAPITAL = 100000

def load_data():
    df = pd.read_csv(INPUT_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])
    return df

def compute_features(df):
    df["ret"] = df.groupby("symbol")["close"].pct_change()
    df["mom_5d"] = df.groupby("symbol")["close"].pct_change(5)
    df["std_5d"] = df.groupby("symbol")["ret"].rolling(5).std().reset_index(level=0, drop=True)
    df["next_ret"] = df.groupby("symbol")["ret"].shift(-1)
    return df

def select_stocks(df_day):
    df_day = df_day.copy()

    # 條件1：動能
    cond1 = df_day["mom_5d"] > MOM_THRESHOLD

    # 條件2：不追高（避免出貨）
    cond2 = df_day["ret"] < 0.05

    # 條件3：波動收斂（主力proxy）
    cond3 = df_day["std_5d"] < df_day["std_5d"].median()

    df_sel = df_day[cond1 & cond2 & cond3]

    df_sel = df_sel.sort_values("mom_5d", ascending=False)

    return df_sel.head(MAX_POSITIONS)

def backtest(df):
    capital = INITIAL_CAPITAL
    daily_records = []

    dates = sorted(df["date"].unique())

    for i in range(len(dates) - 1):
        today = dates[i]
        next_day = dates[i + 1]

        df_today = df[df["date"] == today]
        df_next = df[df["date"] == next_day]

        selected = select_stocks(df_today)

        if len(selected) == 0:
            daily_ret = 0
        else:
            merged = pd.merge(
                selected[["symbol"]],
                df_next[["symbol", "next_ret"]],
                on="symbol",
                how="left"
            )

            merged["next_ret"] = merged["next_ret"].fillna(0)
            weight = 1 / len(merged)
            daily_ret = (merged["next_ret"] * weight).sum()

        capital *= (1 + daily_ret)

        daily_records.append({
            "date": today,
            "selected_count": len(selected),
            "daily_ret": daily_ret,
            "capital": capital
        })

    return pd.DataFrame(daily_records)

def summary(df_daily):
    total_return = df_daily["capital"].iloc[-1] / INITIAL_CAPITAL - 1
    sharpe = df_daily["daily_ret"].mean() / (df_daily["daily_ret"].std() + 1e-8) * np.sqrt(252)

    cummax = df_daily["capital"].cummax()
    drawdown = df_daily["capital"] / cummax - 1
    mdd = drawdown.min()

    return pd.DataFrame([{
        "total_return": total_return,
        "sharpe": sharpe,
        "mdd": mdd
    }])

def main():
    df = load_data()
    df = compute_features(df)

    df_daily = backtest(df)
    df_daily.to_csv(OUTPUT_LOG, index=False)

    df_summary = summary(df_daily)
    df_summary.to_csv(OUTPUT_SUMMARY, index=False)

    print("v214 完成")

if __name__ == "__main__":
    main()
