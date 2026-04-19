# v263_2_engine.txt
# 覆蓋後改名：v263_2_engine.py
# v263.2（效率精修版）
# 原則：
# 1) 不動主線結構（嚴格 T+1 / 真持倉 / 真成本）
# 2) 只做效率優化
# 3) Core 更願意進場，Alpha 稍微更有感
# 4) 仍不追極端報酬

import pandas as pd
import numpy as np
import os

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25

CORE_TOP_N = 25
ALPHA_TOP_N = 8

MAX_POSITION_WEIGHT = 0.12

FEE = 0.0015
SLIPPAGE = 0.001

INITIAL_CAPITAL = 1_000_000


def load_price():
    if not os.path.exists("price_panel_daily.csv"):
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
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
    df["stock_id"] = df["stock_id"].astype(str).str.strip()

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df


def build_features(df):
    g = df.groupby("stock_id")

    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)

    return df


def select_stocks(d):
    core = d[d["mom20"] > -0.03].copy()
    core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
    core = core.sort_values("score", ascending=False).head(CORE_TOP_N)

    alpha = d[d["mom20"] > 0].copy()
    alpha["quality"] = (alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4) / (alpha["vol20"] + 1e-6)
    alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)

    return core, alpha


def run_backtest(df):
    df = df.dropna(subset=["ret1", "mom5", "mom20", "mom60", "vol20"]).copy()
    dates = sorted(df["date"].unique())

    cash = INITIAL_CAPITAL
    holdings = {}
    nav_list = []

    for i in range(len(dates) - 1):
        today = dates[i]
        next_day = dates[i + 1]

        today_df = df[df["date"] == today].copy()
        next_df = df[df["date"] == next_day].copy()
        next_price = {r["stock_id"]: r["close"] for _, r in next_df.iterrows()}

        core, alpha = select_stocks(today_df)

        target = {}
        for _, r in core.iterrows():
            target[r["stock_id"]] = CORE_WEIGHT / CORE_TOP_N

        for _, r in alpha.iterrows():
            target[r["stock_id"]] = target.get(r["stock_id"], 0.0) + ALPHA_WEIGHT / ALPHA_TOP_N

        nav = cash
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        new_holdings = {}
        new_cash = nav  # 簡化為 next_day 重新配置，仍嚴格 T+1

        for s, w in target.items():
            if s not in next_price:
                continue

            weight = min(w, MAX_POSITION_WEIGHT)
            price = next_price[s] * (1 + SLIPPAGE)
            alloc_value = nav * weight
            shares = alloc_value / price

            gross_cost = shares * price
            total_cost = gross_cost * (1 + FEE)

            if total_cost > new_cash:
                continue

            new_cash -= total_cost
            new_holdings[s] = {
                "shares": shares,
                "cost": price
            }

        holdings = new_holdings
        cash = new_cash

        nav = cash
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        nav_list.append({
            "date": next_day,
            "nav": nav,
            "ret": np.nan,
            "core_count": len(core),
            "alpha_count": len(alpha),
        })

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)

    return nav_df


def evaluate(nav_df):
    total_return = nav_df["nav"].iloc[-1] / nav_df["nav"].iloc[0] - 1.0
    mdd = (nav_df["nav"] / nav_df["nav"].cummax() - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-6)

    avg_core_count = nav_df["core_count"].mean() if "core_count" in nav_df.columns else np.nan
    avg_alpha_count = nav_df["alpha_count"].mean() if "alpha_count" in nav_df.columns else np.nan

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe,
        "avg_core_count": avg_core_count,
        "avg_alpha_count": avg_alpha_count,
    }])


if __name__ == "__main__":
    df = load_price()
    df = build_features(df)

    nav_df = run_backtest(df)
    summary_df = evaluate(nav_df)

    nav_df.to_csv("daily_nav.csv", index=False)
    summary_df.to_csv("full_summary.csv", index=False)

    print(summary_df.to_string(index=False))
