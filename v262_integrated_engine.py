import pandas as pd
import numpy as np
import os

# ===== 核心權重 =====
CORE_WEIGHT_BASE = 0.75
ALPHA_WEIGHT_BASE = 0.25

# ===== 持倉 =====
CORE_TOP_N = 20
ALPHA_TOP_N = 8

# ===== 風控 =====
RET_CAP = 0.08
RET_FLOOR = -0.05

# ===== 市場控制 =====
AGG_EXPOSURE = 1.0
DEF_EXPOSURE = 0.6
RISK_OFF_EXPOSURE = 0.1


# ===== 載入資料 =====
def load_price():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower() for c in df.columns]

    if "date" not in df.columns:
        df["date"] = df.get("trade_date", df.get("datetime"))

    if "stock_id" not in df.columns:
        df["stock_id"] = df.get("symbol", df.get("code"))

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce")

    df = df.dropna(subset=["date", "close"])
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

    df["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["volume_spike"] = df["volume"] / (df["volume_ma20"] + 1e-6)

    return df


# ===== 市場狀態 =====
def get_market_state(d):
    avg_ret = d["ret1"].mean()

    if avg_ret > 0.002:
        return "AGG"
    elif avg_ret > -0.002:
        return "DEF"
    else:
        return "RISK_OFF"


# ===== 主策略 =====
def run_strategy(df):
    df = df.dropna().copy()

    nav = 1.0
    peak = 1.0

    rows = []

    for date, d in df.groupby("date"):
        d = d.copy()

        # 避免異常股
        d = d[d["ret1"].abs() < 0.15]
        d = d[d["close"] >= 10]

        # ===== 市場層 =====
        market_state = get_market_state(d)

        if market_state == "AGG":
            exposure = AGG_EXPOSURE
            core_w = CORE_WEIGHT_BASE
            alpha_w = ALPHA_WEIGHT_BASE

        elif market_state == "DEF":
            exposure = DEF_EXPOSURE
            core_w = 0.85
            alpha_w = 0.15

        else:
            exposure = RISK_OFF_EXPOSURE
            core_w = 1.0
            alpha_w = 0.0

        # ===== Core =====
        core = d[d["mom20"] > -0.02].copy()
        core["score"] = core["mom20"] * 0.6 + core["mom60"] * 0.4
        core = core.sort_values("score", ascending=False).head(CORE_TOP_N)
        core_ret = core["ret1"].mean() if len(core) > 0 else 0

        # ===== Alpha =====
        alpha = d[
            (d["volume_spike"] > 1.0) &
            (d["mom20"] > 0)
        ].copy()

        alpha["trend"] = alpha["mom20"] * 0.6 + alpha["mom60"] * 0.4
        alpha["quality"] = alpha["trend"] / (alpha["vol20"] + 1e-6)

        alpha = alpha.sort_values("quality", ascending=False).head(ALPHA_TOP_N)
        alpha_ret = alpha["ret1"].mean() if len(alpha) > 0 else 0

        # ===== 合併 =====
        total_ret = exposure * (core_w * core_ret + alpha_w * alpha_ret)

        # ===== 風控 =====
        total_ret = min(total_ret, RET_CAP)
        total_ret = max(total_ret, RET_FLOOR)

        nav *= (1 + total_ret)
        peak = max(peak, nav)
        dd = nav / peak - 1

        rows.append({
            "date": date,
            "nav": nav,
            "ret": total_ret,
            "core_ret": core_ret,
            "alpha_ret": alpha_ret,
            "market": market_state,
            "exposure": exposure,
            "dd": dd
        })

    return pd.DataFrame(rows)


# ===== 評估 =====
def evaluate(df):
    return pd.DataFrame([{
        "return": df["nav"].iloc[-1] - 1,
        "mdd": df["dd"].min(),
        "sharpe_daily": df["ret"].mean() / (df["ret"].std() + 1e-6),
        "avg_exposure": df["exposure"].mean()
    }])


# ===== 主程式 =====
if __name__ == "__main__":
    df = load_price()
    df = build_features(df)

    nav = run_strategy(df)
    summary = evaluate(nav)

    nav.to_csv("daily_nav.csv", index=False)
    summary.to_csv("full_summary.csv", index=False)

    print(summary)
