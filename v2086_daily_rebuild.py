from pathlib import Path
from datetime import datetime
import pandas as pd

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"

# === 讀 NAV ===
nav = pd.read_csv(NAV_PATH)

if nav.empty:
    raise ValueError("NAV file is empty")

# === 轉日期 ===
nav["date"] = pd.to_datetime(nav["date"])

# === 取得最後一筆 ===
last_date = nav["date"].max()
last_nav = nav.iloc[-1]["nav"]

# === 今天 ===
today = pd.Timestamp.today().normalize()

# === 補缺日期 ===
missing_dates = pd.date_range(
    last_date + pd.Timedelta(days=1),
    today
)

rows = []

for d in missing_dates:
    daily_ret = 0.0
    last_nav = last_nav * (1 + daily_ret)

    rows.append({
        "date": d,
        "nav": last_nav,
        "ret": daily_ret
    })

# === 合併 ===
if rows:
    nav = pd.concat([nav, pd.DataFrame(rows)], ignore_index=True)

# === 存檔 ===
nav.to_csv(NAV_PATH, index=False)
