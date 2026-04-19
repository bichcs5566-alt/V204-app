# v265_decision_engine.txt
# 覆蓋後改名：v265_decision_engine.py
# v265（決策輸出版）
# 核心：沿用 v264 / v263 主線邏輯
# 差異：輸出可直接手動下單的 trade_plan.csv

import pandas as pd
import numpy as np
import os

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25

CORE_TOP_N = 25
ALPHA_TOP_N = 6

MAX_POSITION_WEIGHT = 0.10

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
    core = d[d["mom20"] > -0.02].copy()
    core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
    core = core.sort_values("score", ascending=False).head(CORE_TOP_N)

    alpha = d[d["mom20"] > 0].copy()
    alpha["quality"] = (alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4) / (alpha["vol20"] + 1e-6)
    alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)

    return core, alpha


def build_target_weights(core, alpha):
    target = {}

    for _, r in core.iterrows():
        target[r["stock_id"]] = CORE_WEIGHT / CORE_TOP_N

    for _, r in alpha.iterrows():
        target[r["stock_id"]] = target.get(r["stock_id"], 0.0) + ALPHA_WEIGHT / ALPHA_TOP_N

    # 限制單檔上限
    for k in list(target.keys()):
        target[k] = min(target[k], MAX_POSITION_WEIGHT)

    return target


def build_trade_plan(df, latest_capital=INITIAL_CAPITAL):
    df = df.dropna(subset=["ret1", "mom5", "mom20", "mom60", "vol20"]).copy()
    dates = sorted(df["date"].unique())
    if len(dates) < 2:
        raise ValueError("not enough dates")

    signal_date = dates[-2]
    trade_date = dates[-1]

    signal_df = df[df["date"] == signal_date].copy()
    trade_df = df[df["date"] == trade_date].copy()

    trade_price = {r["stock_id"]: r["close"] for _, r in trade_df.iterrows()}

    core, alpha = select_stocks(signal_df)
    target = build_target_weights(core, alpha)

    rows = []
    for stock_id, weight in sorted(target.items(), key=lambda x: x[1], reverse=True):
        if stock_id not in trade_price:
            continue
        px = trade_price[stock_id] * (1 + SLIPPAGE)
        alloc = latest_capital * weight
        shares = alloc / px
        gross = shares * px
        total_cost = gross * (1 + FEE)

        source = []
        if stock_id in set(core["stock_id"]):
            source.append("CORE")
        if stock_id in set(alpha["stock_id"]):
            source.append("ALPHA")
        source_text = "+".join(source)

        rows.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "action": "BUY",
            "stock_id": stock_id,
            "target_weight": round(weight, 4),
            "ref_price": round(px, 4),
            "suggested_amount": round(alloc, 2),
            "suggested_shares": round(shares, 2),
            "estimated_total_cost": round(total_cost, 2),
            "source": source_text,
            "note": "T+1 signal; manual order candidate"
        })

    plan = pd.DataFrame(rows)
    return plan, core, alpha, signal_date, trade_date


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
        target = build_target_weights(core, alpha)

        nav = cash
        for s, pos in holdings.items():
            if s in next_price:
                nav += pos["shares"] * next_price[s]

        new_holdings = {}
        new_cash = nav

        for s, w in target.items():
            if s not in next_price:
                continue

            price = next_price[s] * (1 + SLIPPAGE)
            alloc_value = nav * w
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

        nav_list.append({"date": next_day, "nav": nav})

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)
    return nav_df


def evaluate(nav_df):
    total_return = nav_df["nav"].iloc[-1] / nav_df["nav"].iloc[0] - 1.0
    mdd = (nav_df["nav"] / nav_df["nav"].cummax() - 1.0).min()
    sharpe = nav_df["ret"].mean() / (nav_df["ret"].std() + 1e-6)

    return pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])


if __name__ == "__main__":
    df = load_price()
    df = build_features(df)

    nav_df = run_backtest(df)
    summary_df = evaluate(nav_df)

    trade_plan_df, core_df, alpha_df, signal_date, trade_date = build_trade_plan(df)

    nav_df.to_csv("daily_nav.csv", index=False)
    summary_df.to_csv("full_summary.csv", index=False)
    trade_plan_df.to_csv("trade_plan.csv", index=False)
    core_df.to_csv("core_candidates.csv", index=False)
    alpha_df.to_csv("alpha_candidates.csv", index=False)

    print("Signal date:", signal_date)
    print("Trade date:", trade_date)
    print(summary_df.to_string(index=False))
    print("\nTop trade plan:")
    print(trade_plan_df.head(10).to_string(index=False))
