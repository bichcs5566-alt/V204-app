import pandas as pd
import numpy as np

# ===== Load data =====
df = pd.read_csv("price_panel_daily.csv")

# Ensure required columns
required_cols = ["trade_date", "symbol", "close"]
for col in required_cols:
    if col not in df.columns:
        raise Exception(f"缺少欄位: {col}")

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values(["symbol", "trade_date"])

# ===== Feature engineering =====
df["ret"] = df.groupby("symbol")["close"].pct_change()
df["mom_5"] = df.groupby("symbol")["close"].pct_change(5)
df["mom_10"] = df.groupby("symbol")["close"].pct_change(10)

# ===== Engine A (short-term) =====
df["score_A"] = df["mom_5"]
df_A = df.copy()
df_A["rank_A"] = df_A.groupby("trade_date")["score_A"].rank(ascending=False)
df_A["pos_A"] = (df_A["rank_A"] <= 10).astype(int)

# ===== Engine B (swing) =====
df["score_B"] = df["mom_10"]
df_B = df.copy()
df_B["rank_B"] = df_B.groupby("trade_date")["score_B"].rank(ascending=False)
df_B["pos_B"] = (df_B["rank_B"] <= 10).astype(int)

# ===== Merge =====
df_all = df.merge(df_A[["trade_date","symbol","pos_A"]], on=["trade_date","symbol"])
df_all = df_all.merge(df_B[["trade_date","symbol","pos_B"]], on=["trade_date","symbol"])

# ===== Portfolio =====
df_all["ret_A"] = df_all["ret"] * df_all["pos_A"]
df_all["ret_B"] = df_all["ret"] * df_all["pos_B"]

daily_A = df_all.groupby("trade_date")["ret_A"].mean()
daily_B = df_all.groupby("trade_date")["ret_B"].mean()

# ===== Dynamic weighting =====
nav_A = (1 + daily_A.fillna(0)).cumprod()
nav_B = (1 + daily_B.fillna(0)).cumprod()

ret_5 = nav_A.pct_change(5)
ret_10 = nav_A.pct_change(10)

use_A = (ret_5 > ret_10).astype(int)

wA = 0.5 * use_A
wB = 1 - wA

combined_ret = wA * daily_A.fillna(0) + wB * daily_B.fillna(0)
nav = (1 + combined_ret).cumprod()

# ===== Risk control =====
rolling_max = nav.cummax()
dd = nav / rolling_max - 1

wA[dd < -0.08] = 0.2
wA[dd < -0.12] = 0.0
wB = 1 - wA

combined_ret = wA * daily_A.fillna(0) + wB * daily_B.fillna(0)
nav = (1 + combined_ret).cumprod()

# ===== Output =====
out = pd.DataFrame({
    "trade_date": nav.index,
    "nav": nav.values
})

summary = {
    "return": nav.iloc[-1] - 1,
    "mdd": dd.min(),
    "avg_wA": wA.mean(),
    "avg_wB": wB.mean(),
    "nav_weak_days": int((ret_5 < ret_10).sum())
}

pd.DataFrame(out).to_csv("v229_nav.csv", index=False)
pd.DataFrame([summary]).to_csv("v229_summary.csv", index=False)

print("v229 done")
