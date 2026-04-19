# v264_engine.txt
# 覆蓋後改名：v264_engine.py
# v264（實盤準備版）
# 重點：
# - 嚴格 T+1
# - 真持倉延續（不再全重建）
# - 流動性限制（成交量限制）
# - 分批進出（簡化版）
# - 更真實滑價

import pandas as pd
import numpy as np
import os

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25

CORE_TOP_N = 25
ALPHA_TOP_N = 6

MAX_POSITION_WEIGHT = 0.10
MAX_DAILY_VOLUME_RATIO = 0.1   # 最多吃10%成交量

FEE = 0.0015
SLIPPAGE_BASE = 0.001

INITIAL_CAPITAL = 1_000_000


def load_price():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower() for c in df.columns]

    if "date" not in df.columns:
        df["date"] = df.get("trade_date", df.get("datetime"))

    if "stock_id" not in df.columns:
        df["stock_id"] = df.get("symbol", df.get("code"))

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    return df


def build_features(df):
    g = df.groupby("stock_id")

    df["ret1"] = g["close"].pct_change()
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)

    return df


def select_stocks(d):
    core = d[d["mom20"] > -0.02].copy()
    core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
    core = core.sort_values("score", ascending=False).head(CORE_TOP_N)

    alpha = d[d["mom20"] > 0].copy()
    alpha["quality"] = (alpha["mom20"]*0.6 + alpha["mom60"]*0.4)/(alpha["vol20"]+1e-6)
    alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)

    return core, alpha


def run_backtest(df):
    df = df.dropna().copy()
    dates = sorted(df["date"].unique())

    cash = INITIAL_CAPITAL
    holdings = {}  # stock -> shares
    nav_list = []

    for i in range(len(dates)-1):
        today = dates[i]
        next_day = dates[i+1]

        today_df = df[df["date"]==today]
        next_df = df[df["date"]==next_day]

        next_price = {r["stock_id"]: r["close"] for _,r in next_df.iterrows()}
        next_vol = {r["stock_id"]: r.get("volume",1e6) for _,r in next_df.iterrows()}

        core, alpha = select_stocks(today_df)

        target = {}

        for _,r in core.iterrows():
            target[r["stock_id"]] = CORE_WEIGHT/CORE_TOP_N

        for _,r in alpha.iterrows():
            target[r["stock_id"]] = target.get(r["stock_id"],0)+ALPHA_WEIGHT/ALPHA_TOP_N

        # 計算當前 NAV
        nav = cash
        for s,sh in holdings.items():
            if s in next_price:
                nav += sh * next_price[s]

        # 調整持倉（部分調整）
        new_holdings = holdings.copy()

        for s,w in target.items():
            if s not in next_price:
                continue

            price = next_price[s]
            vol = next_vol[s]

            max_shares = vol * MAX_DAILY_VOLUME_RATIO
            target_value = nav * min(w, MAX_POSITION_WEIGHT)
            target_shares = target_value / price

            current_shares = new_holdings.get(s,0)
            delta = target_shares - current_shares

            # 限制成交量
            delta = np.sign(delta) * min(abs(delta), max_shares)

            trade_value = delta * price
            cost = abs(trade_value) * (FEE + SLIPPAGE_BASE)

            if delta > 0:  # buy
                if cash < trade_value + cost:
                    continue
                cash -= (trade_value + cost)
                new_holdings[s] = current_shares + delta
            else:  # sell
                cash += abs(trade_value) - cost
                new_holdings[s] = current_shares + delta

        holdings = {k:v for k,v in new_holdings.items() if v>0}

        # 計算 NAV
        nav = cash
        for s,sh in holdings.items():
            if s in next_price:
                nav += sh * next_price[s]

        nav_list.append({"date": next_day, "nav": nav})

    nav_df = pd.DataFrame(nav_list)
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0)

    return nav_df


def evaluate(nav):
    ret = nav["nav"].iloc[-1]/nav["nav"].iloc[0]-1
    mdd = (nav["nav"]/nav["nav"].cummax()-1).min()
    sharpe = nav["ret"].mean()/(nav["ret"].std()+1e-6)

    return pd.DataFrame([{
        "return": ret,
        "mdd": mdd,
        "sharpe_daily": sharpe
    }])


if __name__=="__main__":
    df = load_price()
    df = build_features(df)

    nav = run_backtest(df)
    summary = evaluate(nav)

    nav.to_csv("daily_nav.csv",index=False)
    summary.to_csv("full_summary.csv",index=False)

    print(summary)
