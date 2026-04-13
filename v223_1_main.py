# v223_1_main.py

import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"

df = pd.read_csv(PRICE_FILE)

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values(["trade_date", "symbol"])

dates = sorted(df["trade_date"].unique())

capital = 100000
nav_list = []
nav = capital

for d in dates:
    g = df[df["trade_date"] == d]

    # 🔥 強制一定有交易（避免0 trades）
    if len(g) == 0:
        nav_list.append({"trade_date": d, "nav": nav})
        continue

    # 👉 B主策略：用 momentum 排序
    g = g.copy()
    g["score"] = g["close"] / g["open"]  # 簡單動能

    selected = g.sort_values("score", ascending=False).head(10)

    # 報酬
    selected["ret"] = selected["close"] / selected["open"] - 1

    if len(selected) > 0:
        daily_ret = selected["ret"].mean()
    else:
        daily_ret = 0

    nav = nav * (1 + daily_ret)

    nav_list.append({
        "trade_date": d,
        "daily_ret": daily_ret,
        "nav": nav,
        "holdings": len(selected)
    })

nav_df = pd.DataFrame(nav_list)
nav_df.to_csv("v223_1_nav.csv", index=False)

# summary
total_return = nav / capital - 1

summary = pd.DataFrame([{
    "initial_capital": capital,
    "final_nav": nav,
    "total_return": total_return,
    "trading_days": len(nav_df)
}])

summary.to_csv("v223_1_summary.csv", index=False)

print("DONE B MAIN")
