import pandas as pd
import numpy as np
import os

# ===== 參數 =====
CORE_WEIGHT = 0.85
ALPHA_WEIGHT = 0.15

CORE_TOP_N = 10
ALPHA_TOP_N = 3

RET_CAP = 0.08
RET_FLOOR = -0.05

# ===== 載入 =====
def load_price():
    if not os.path.exists("price_panel_daily.csv"):
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [c.lower() for c in df.columns]

    # 自動修正欄位
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

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])

    return df


# ===== 特徵 =====
def build_features(df):
    df["ret1"] = df.groupby("stock_id")["close"].pct_change()

    df["mom20"] = df.groupby("stock_id")["close"].pct_change(20)
    df["mom60"] = df.groupby("stock_id")["close"].pct_change(60)

    df["vol20"] = df.groupby("stock_id")["ret1"].rolling(20).std().reset_index(0, drop=True)

    df["volume_ma20"] = df.groupby("stock_id")["volume"].rolling(20).mean().reset_index(0, drop=True)
    df["volume_spike"] = df["volume"] / df["volume_ma20"]

    return df


# ===== 策略 =====
def run_strategy(df):
    df = df.dropna()

    nav = 1.0
    result = []

    for date, d in df.groupby("date"):

        # 避免極端股（關鍵）
        d = d[d["ret1"].abs() < 0.15]

        # ===== CORE =====
        core = d[d["mom20"] > 0]
        core = core.sort_values("mom20", ascending=False).head(CORE_TOP_N)
        core_ret = core["ret1"].mean() if len(core) > 0 else 0

        # ===== ALPHA（輕量）=====
        d["trend"] = d["mom20"] * 0.6 + d["mom60"] * 0.4
        d["quality"] = d["trend"] / (d["vol20"] + 1e-6)

        alpha = d[
            (d["volume_spike"] > 1.3) &
            (d["mom20"] > 0)
        ]

        alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)
        alpha_ret = alpha["ret1"].mean() if len(alpha) > 0 else 0

        # ===== 合併 =====
        total_ret = CORE_WEIGHT * core_ret + ALPHA_WEIGHT * alpha_ret

        # 🔥 防爆
        total_ret = min(total_ret, RET_CAP)
        total_ret = max(total_ret, RET_FLOOR)

        nav *= (1 + total_ret)

        result.append({
            "date": date,
            "nav": nav,
            "ret": total_ret,
            "core_ret": core_ret,
            "alpha_ret": alpha_ret
        })

    return pd.DataFrame(result)


# ===== 評估 =====
def evaluate(df):
    ret = df["nav"].iloc[-1] - 1
    mdd = (df["nav"] / df["nav"].cummax() - 1).min()
    sharpe = df["ret"].mean() / (df["ret"].std() + 1e-6)

    return pd.DataFrame([{
       
