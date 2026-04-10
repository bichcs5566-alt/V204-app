from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def fetch_positions():
    pos = pd.read_csv(POS_PATH)

    for c in ["weight","day_ret"]:
        if c in pos.columns:
            pos[c] = to_num(pos[c])

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / len(pos)

    return pos

def rebuild_nav(pos):
    today = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)

    if NAV_PATH.exists():
        nav = pd.read_csv(NAV_PATH)
        nav["date"] = pd.to_datetime(nav["date"])
    else:
        nav = pd.DataFrame([{
            "date": pd.Timestamp("2015-03-27"),
            "nav": 100000
        }])

    last_date = nav["date"].iloc[-1]
    last_nav = float(nav["nav"].iloc[-1])

    # 🔥 核心：補齊所有缺的日期
    missing = pd.date_range(last_date + pd.Timedelta(days=1), today)

    avg_ret = pos["day_ret"].mean()

    cur = last_nav
    rows = []

    for d in missing:
        cur = cur * (1 + avg_ret)
        rows.append({"date": d, "nav": cur})

    if rows:
        nav = pd.concat([nav, pd.DataFrame(rows)])

    nav["date"] = nav["date"].dt.strftime("%Y-%m-%d")

    nav.to_csv(NAV_PATH, index=False)

    print("補齊完成")

def main():
    # === 讀 NAV ===
nav = pd.read_csv(NAV_PATH)

# 防呆（第一次跑）
if nav.empty:
    raise ValueError("NAV file is empty")

# 轉日期
nav["date"] = pd.to_datetime(nav["date"])

# === 取得最後狀態 ===
last_date = nav["date"].max()
last_nav = nav.iloc[-1]["nav"]

# === 今天 ===
today = pd.Timestamp.today().normalize()

# === 缺失日期 ===
missing_dates = pd.date_range(
    last_date + pd.Timedelta(days=1),
    today
)

rows = []

for d in missing_dates:
    daily_ret = 0.0  # 先用0，之後再接真實報酬
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

if rows:
    nav = pd.concat([nav, pd.DataFrame(rows)], ignore_index=True)

nav.to_csv(NAV_PATH, index=False)

if __name__ == "__main__":
    main()
