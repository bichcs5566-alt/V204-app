# -*- coding: utf-8 -*-
import pandas as pd

print("=== POSITION OVERLAY START ===")

# 讀持倉
pos = pd.read_csv("position_monitor.csv")

# 讀價格
price = pd.read_csv("price_panel_daily.csv")

# 取最近20天
price = price.sort_values(["stock_id", "date"])
price["ma20"] = price.groupby("stock_id")["close"].rolling(20).mean().reset_index(0, drop=True)

latest = price.groupby("stock_id").tail(1)[["stock_id", "close", "ma20"]]

df = pos.merge(latest, on="stock_id", how="left")

results = []

for _, row in df.iterrows():
    stock = row["stock_id"]
    cost = row.get("avg_price", row.get("cost", 0))
    close = row["close"]
    ma20 = row["ma20"]

    action = "觀察"
    reason = ""

    if pd.isna(close):
        action = "觀察"
        reason = "無價格資料"

    else:
        pnl = (close - cost) / cost if cost > 0 else 0

        if pnl <= -0.08:
            action = "出場"
            reason = "跌破停損(-8%)"

        elif close < ma20:
            action = "觀察"
            reason = "跌破MA20"

        else:
            action = "抱住"
            reason = "趨勢未壞"

    results.append({
        "stock_id": stock,
        "position_action": action,
        "position_reason": reason
    })

out = pd.DataFrame(results)

out.to_csv("position_overlay.csv", index=False)
out.to_csv("mobile_dashboard_v1/data/position_overlay.csv", index=False)

print("=== POSITION OVERLAY DONE ===")
