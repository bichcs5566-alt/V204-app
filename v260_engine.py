# v260_engine.txt
# 覆蓋後改名：v260_engine.py
# 方向：延續 v259 主線，只做溫和放大，不偏離原本規劃
# - Core 80% / Alpha 20%
# - Core TOP_N 15
# - Alpha TOP_N 5
# - Alpha 條件微放鬆
# - 報酬限幅保留，避免再次爆掉
# - 嚴格 T+1，不偷看

import os
import numpy as np
import pandas as pd

CORE_WEIGHT = 0.80
ALPHA_WEIGHT = 0.20

CORE_TOP_N = 15
ALPHA_TOP_N = 5

RET_CAP = 0.08
RET_FLOOR = -0.05


def load_price():
    if not os.path.exists("price_panel_daily.csv"):
        raise FileNotFoundError("price_panel_daily.csv not found")

    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

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
    df["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["volume_spike"] = df["volume"] / (df["volume_ma20"] + 1e-6)

    return df


def run_strategy(df):
    df = df.dropna(subset=["ret1", "mom5", "mom20", "mom60", "vol20"]).copy()

    nav = 1.0
    rows = []

    for date, d in df.groupby("date"):
        d = d.copy()

        # 避免妖股 / 異常值
        d = d[d["ret1"].abs() < 0.15].copy()
        d = d[d["close"] >= 10].copy()

        # ===== CORE：穩定主體 =====
        core = d[d["mom20"] > 0].copy()
        core["core_score"] = core["mom20"] * 0.7 + core["mom60"] * 0.3
        core = core.sort_values("core_score", ascending=False).head(CORE_TOP_N)
        core_ret = core["ret1"].mean() if len(core) > 0 else 0.0

        # ===== ALPHA：輕量放大 =====
        alpha = d[
            (d["volume_spike"] > 1.1) &
            (d["mom20"] > 0) &
            (d["mom5"] > -0.02)
        ].copy()

        alpha["trend"] = alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4
        alpha["quality"] = alpha["trend"] / (alpha["vol20"] + 1e-6)
        alpha["alpha_score"] = alpha["quality"] * 0.7 + alpha["mom5"] * 0.3
        alpha = alpha.sort_values("alpha_score", ascending=False).head(ALPHA_TOP_N)
        alpha_ret = alpha["ret1"].mean() if len(alpha) > 0 else 0.0

        total_ret = CORE_WEIGHT * core_ret + ALPHA_WEIGHT * alpha_ret

        # 防爆
        total_ret = min(total_ret, RET_CAP)
        total_ret = max(total_ret, RET_FLOOR)

        nav *= (1.0 + total_ret)

        rows.append({
            "date": date,
            "nav": nav,
            "ret": total_ret,
            "core_ret": core_ret,
            "alpha_ret": alpha_ret,
            "core_count": len(core),
            "alpha_count": len(alpha),
        })

    return pd.DataFrame(rows)


def evaluate(nav_df):
    total_return = nav_df["nav"].iloc[-1] - 1.0
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
    nav_df = run_strategy(df)
    summary_df = evaluate(nav_df)

    nav_df.to_csv("daily_nav.csv", index=False)
    summary_df.to_csv("full_summary.csv", index=False)

    print(summary_df.to_string(index=False))
