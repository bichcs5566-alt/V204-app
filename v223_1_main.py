# v223_1_main.py
# ==========================================
# v223.1
# - 主系統：B only（波段順勢）
# - A：完全隔離，只做觀察
# - 加入風控：最大曝險限制
# ==========================================

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000

# 👉 關鍵：不再滿倉
MAX_EXPOSURE = 0.5

# ==========================================
# 讀資料
# ==========================================
df = pd.read_csv("price_panel.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["symbol", "date"])

# ==========================================
# 特徵
# ==========================================
g = df.groupby("symbol")

df["ret1"] = g["close"].pct_change(1)
df["mom5"] = g["close"].pct_change(5)
df["mom20"] = g["close"].pct_change(20)
df["std5"] = g["ret1"].rolling(5).std().reset_index(level=0, drop=True)
df["ma5"] = g["close"].rolling(5).mean().reset_index(level=0, drop=True)
df["ma20"] = g["close"].rolling(20).mean().reset_index(level=0, drop=True)

dates = sorted(df["date"].unique())

records = []

# ==========================================
# 主引擎 B（順勢）
# ==========================================
for i in range(len(dates) - 6):
    signal = dates[i]
    trade = dates[i + 1]
    exit_d = dates[i + 6]

    snap = df[df["date"] == signal]

    cond = (
        (snap["mom5"] > 0.05) &
        (snap["mom20"] > 0.1) &
        (snap["close"] > snap["ma5"]) &
        (snap["ma5"] > snap["ma20"]) &
        (snap["std5"] < 0.12)
    )

    pick = snap[cond].sort_values("mom20", ascending=False).head(5)

    if len(pick) == 0:
        continue

    weight = MAX_EXPOSURE / len(pick)

    for _, r in pick.iterrows():
        try:
            buy = df[(df["symbol"] == r["symbol"]) & (df["date"] == trade)]["close"].values[0]
            sell = df[(df["symbol"] == r["symbol"]) & (df["date"] == exit_d)]["close"].values[0]
        except:
            continue

        ret = sell / buy - 1

        records.append({
            "engine": "B",
            "trade_date": trade,
            "ret": ret * weight
        })

# ==========================================
# NAV
# ==========================================
nav = INITIAL_CAPITAL
nav_list = []

for d in dates:
    day_ret = sum([r["ret"] for r in records if r["trade_date"] == d])
    nav *= (1 + day_ret)
    nav_list.append(nav)

nav_df = pd.DataFrame({
    "date": dates,
    "nav": nav_list
})

nav_df.to_csv("v223_1_nav.csv", index=False)

print("完成 v223.1（主系統）")
