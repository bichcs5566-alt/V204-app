# v223_1_A_observer.py

import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"

df = pd.read_csv(PRICE_FILE)

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values(["trade_date", "symbol"])

dates = sorted(df["trade_date"].unique())

records = []

for d in dates:
    g = df[df["trade_date"] == d]

    if len(g) == 0:
        continue

    # 🧪 A策略（不同邏輯：反轉）
    g = g.copy()
    g["score"] = g["open"] / g["close"]  # 反向

    selected = g.sort_values("score", ascending=False).head(10)

    selected["ret"] = selected["close"] / selected["open"] - 1

    if len(selected) > 0:
        avg_ret = selected["ret"].mean()
    else:
        avg_ret = 0

    records.append({
        "trade_date": d,
        "avg_ret": avg_ret,
        "count": len(selected)
    })

obs_df = pd.DataFrame(records)
obs_df.to_csv("v223_1_A_observer.csv", index=False)

print("DONE A OBSERVER")
