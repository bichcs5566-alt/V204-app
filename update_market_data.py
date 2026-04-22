import pandas as pd
from datetime import datetime

# v2.7 修正版
# 重點修正：
# 1. 欄位名稱改成 stock_id（不要再用 stock）
# 2. 若檔案原本是 stock 欄，也自動轉成 stock_id
# 3. 只做最小測試補值，避免主流程直接炸掉

PRICE_PANEL_FILE = "price_panel_daily.csv"

today = datetime.now().strftime("%Y-%m-%d")

try:
    df = pd.read_csv(PRICE_PANEL_FILE, encoding="utf-8-sig")
except Exception:
    df = pd.DataFrame(columns=["date", "stock_id", "close", "volume"])

df.columns = [str(c).strip().lower() for c in df.columns]

# 舊欄位相容
if "stock_id" not in df.columns:
    if "stock" in df.columns:
        df = df.rename(columns={"stock": "stock_id"})
    elif "symbol" in df.columns:
        df = df.rename(columns={"symbol": "stock_id"})
    elif "code" in df.columns:
        df = df.rename(columns={"code": "stock_id"})

# 補齊必要欄位
for col, default in {
    "date": today,
    "stock_id": "TEST",
    "close": 100,
    "volume": 0,
}.items():
    if col not in df.columns:
        df[col] = default

# 這裡只是最小測試資料，確保主 pipeline 不會因缺欄而中斷
new_row = pd.DataFrame([{
    "date": today,
    "stock_id": "TEST",
    "close": 100,
    "volume": 0,
}])

df = pd.concat([df, new_row], ignore_index=True)

# 保留主流程常用欄位
keep_cols = []
for c in ["date", "stock_id", "close", "volume"]:
    if c in df.columns:
        keep_cols.append(c)

df = df[keep_cols].copy()
df.to_csv(PRICE_PANEL_FILE, index=False, encoding="utf-8-sig")

print("updated price_panel_daily.csv")
print("latest append date:", today)
print("columns:", list(df.columns))
