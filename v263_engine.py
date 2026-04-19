import pandas as pd
import numpy as np
import os

# ===== 參數 =====
CORE_WEIGHT = 0.8
ALPHA_WEIGHT = 0.2

CORE_TOP_N = 15
ALPHA_TOP_N = 5

MAX_POSITION_WEIGHT = 0.15

STOP_LOSS_1 = -0.06
STOP_LOSS_2 = -0.10

FEE = 0.0015  # 手續費
SLIPPAGE = 0.001

INITIAL_CAPITAL = 1_000_000


# ===== 載入 =====
def load_price():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns:
        df["date"] = df.get("trade_date", df.get("datetime"))

    if "stock_id" not in df.columns:
        df["stock_id"] = df.get("symbol", df.get("code"))

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    return df


# ===== 特徵 =====
def build_features(df):
    g = df.groupby("stock_id")

    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)

    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)

    return df


# ===== 選股（訊號日 t）=====
def select_stocks(d):

    # Core
    core = d[d["mom20"] > 0].copy()
    core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
    core = core.sort_values("score", ascending=False).head(CORE_TOP_N)

    # Alpha（輕量）
    alpha = d[d["mom20"] > 0].copy()
    alpha["quality"] = (alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4) / (alpha["vol20"] + 1e-6)
    alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)

    return core, alpha


# ===== 主回測（嚴格 T+1）=====
def run_backtest(df):

    df = df.dropna().copy()
    dates = sorted(df["date"].unique())

    capital = INITIAL_CAPITAL
    holdings = {}  # stock_id -> dict(shares, cost)

    nav_list = []

    for i in range(len(dates) - 1):
        today = dates[i]
        next_day = dates[i + 1]

        today_df = df[df["date"] == today]
        next_df = df[df["date"] == next_day]

        next_price = {r["stock_id"]: r["close"] for _, r in next_df.iterrows()}

        # ===== 1. 用 today 選股（訊號）=====
        core, alpha = select_stocks(today_df)

        target = {}

        for _, r in core.iterrows():
            target[r["stock_id"]] = CORE_WEIGHT / CORE_TOP_N

        for _, r in alpha.iterrows():
            target[r["stock_id"]] = target.get(r["stock_id"], 0) + ALPHA_WEIGHT / ALPHA_TOP_N

        # ===== 2. 計算當前 NAV =====
        nav = capital
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        # ===== 3. 調整持倉（在 next_day 執行）=====
        new_holdings = {}

        for s, w in target.items():
            if s not in next_price:
                continue

            price = next_price[s] * (1 + SLIPPAGE)
            value = nav * w
            shares = value / price

            cost = price * shares * (1 + FEE)

            if cost > capital:
                continue

            capital -= cost
            new_holdings[s] = {
                "shares": shares,
                "cost": price
            }

        # ===== 4. 保留未賣出的（簡化：全部重建）=====
        holdings = new_holdings

        # ===== 5. 計算 NAV =====
        nav = capital
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        nav_list.append({
            "date": next_day,
            "nav": nav
        })

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0)

    return nav_df


# ===== 評估 =====
def evaluate(nav):
    total_return = nav["nav"].iloc[-1] / nav["nav"].iloc[0] - 1
    mdd = (nav["nav"] / nav["nav"].cummax() - 1).min()
    sharpe = nav["ret"].mean() / (nav["ret"].std() + 1e-6)

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])


# ===== 主程式 =====
if __name__ == "__main__":
    df = load_price()
    df = build_features(df)

    nav = run_backtest(df)
    summary = evaluate(nav)

    nav.to_csv("daily_nav.csv", index=False)
    summary.to_csv("full_summary.csv", index=False)

    print(summary)
