# v214_dynamic_portfolio_engine_fixed.txt

import pandas as pd

# ===== 修正這裡 =====
INPUT_PATH = "price_panel_daily.csv"   # ← 改成正確檔名

OUTPUT_SUMMARY = "v214_summary.csv"

def run():
    df = pd.read_csv(INPUT_PATH)

    # 基本欄位檢查
    required_cols = ["date", "symbol", "close"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    # 計算報酬
    df["ret"] = df.groupby("symbol")["close"].pct_change()

    # 每日平均報酬（簡化版）
    daily = df.groupby("date")["ret"].mean().dropna()

    # NAV
    nav = (1 + daily).cumprod()

    summary = {
        "start_date": str(daily.index.min().date()),
        "end_date": str(daily.index.max().date()),
        "initial_capital": 100000,
        "final_nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1),
        "trading_days": int(len(daily)),
    }

    pd.DataFrame([summary]).to_csv(OUTPUT_SUMMARY, index=False)

    print("✅ v214 fixed run complete")
    print(summary)

if __name__ == "__main__":
    run()
