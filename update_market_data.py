import requests
import pandas as pd
from datetime import datetime

OUTPUT = "price_panel_daily.csv"

today = datetime.now().strftime("%Y%m%d")

# 台股 TWSE API
url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={today}&type=ALL"

res = requests.get(url)

if res.status_code != 200:
    print("TWSE fetch failed")
    exit(1)

data = res.json()

if "data9" not in data:
    print("no data9 field")
    exit(1)

rows = data["data9"]

records = []

for r in rows:
    try:
        stock_id = r[0]
        close = r[8].replace(",", "")
        volume = r[2].replace(",", "")

        if close == "--":
            continue

        records.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "stock_id": stock_id,
            "close": float(close),
            "volume": int(volume)
        })
    except:
        continue

df = pd.DataFrame(records)

# 只保留最近 30 天（避免爆 GitHub 100MB）
try:
    old = pd.read_csv(OUTPUT)
    df = pd.concat([old, df], ignore_index=True)
except:
    pass

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values("date").drop_duplicates(["date", "stock_id"], keep="last")

df = df[df["date"] >= df["date"].max() - pd.Timedelta(days=30)]

df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")

print("update_market_data done")
print("latest date:", df["date"].max())
print("rows:", len(df))
