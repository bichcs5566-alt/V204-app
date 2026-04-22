import pandas as pd
from datetime import datetime

# 模擬抓新資料（這裡先用當天日期補一筆）
today = datetime.now().strftime("%Y-%m-%d")

try:
    df = pd.read_csv("price_panel_daily.csv")
except:
    df = pd.DataFrame(columns=["date","stock","close"])

new_row = pd.DataFrame([{
    "date": today,
    "stock": "TEST",
    "close": 100
}])

df = pd.concat([df, new_row], ignore_index=True)
df.to_csv("price_panel_daily.csv", index=False, encoding="utf-8-sig")

print("updated price_panel_daily.csv with date:", today)
