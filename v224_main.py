# v224_main_fixed.py
import pandas as pd
import numpy as np

df = pd.read_csv("price_panel_daily.csv")
df.columns = [c.strip().lower() for c in df.columns]

# 自動修復欄位名稱
if "date" in df.columns and "trade_date" not in df.columns:
    df["trade_date"] = df["date"]

if "datetime" in df.columns and "trade_date" not in df.columns:
    df["trade_date"] = df["datetime"]

if "trade_date" not in df.columns:
    raise Exception(f"❌ 沒有找到 trade_date / date / datetime 欄位，目前欄位: {df.columns.tolist()}")

required_cols = ["symbol", "close", "mom_5", "mom_10", "ret_1d"]
for col in required_cols:
    if col not in df.columns:
        raise Exception(f"缺少欄位: {col}")

df["trade_date"] = pd.to_datetime(df["trade_date"])

market = df.groupby("trade_date")["close"].mean().reset_index()
market["ma20"] = market["close"].rolling(20).mean()
market["ma60"] = market["close"].rolling(60).mean()
market["regime"] = market["ma20"] > market["ma60"]

df = df.merge(market[["trade_date", "regime"]], on="trade_date", how="left")

def select_B(sub):
    return sub[
        (sub["mom_5"] > 0.03) &
        (sub["mom_5"] < 0.20) &
        (sub["mom_10"] > 0.05)
    ].sort_values("mom_5", ascending=False).head(8)

dates = sorted(df["trade_date"].unique())
nav = 100000
peak = nav
records = []

for d in dates:
    sub = df[df["trade_date"] == d]
    regime = sub["regime"].iloc[0]

    if regime:
        picks = select_B(sub)
    else:
        picks = pd.DataFrame()

    if len(picks) == 0:
        daily_ret = 0
        cash_mode = True
    else:
        picks["ret_1d"] = picks["ret_1d"].clip(lower=-0.05)
        daily_ret = picks["ret_1d"].mean() * 0.5
        cash_mode = False

    nav = nav * (1 + daily_ret)
    peak = max(peak, nav)
    dd = (nav - peak) / peak

    records.append([d, nav, daily_ret, dd, cash_mode])

result = pd.DataFrame(records, columns=["date", "nav", "ret", "drawdown", "cash_mode"])
result.to_csv("v224_nav.csv", index=False)

print("✅ DONE")

