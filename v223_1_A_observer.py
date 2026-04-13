import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"

df = pd.read_csv(PRICE_FILE)

# 你的檔案是 date，不是 trade_date
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["date", "symbol"]).copy()

df["next_close"] = df.groupby("symbol")["close"].shift(-1)
df["ret_1d"] = df["next_close"] / df["close"] - 1

g = df.groupby("symbol")
df["mom_5"] = g["close"].pct_change(5)
df["std_5"] = g["ret_1d"].rolling(5).std().reset_index(level=0, drop=True)

dates = sorted(df["date"].dropna().unique())
rows = []
obs_rows = []

for d in dates[:-1]:
    day = df[df["date"] == d].copy()
    if day.empty:
        continue

    # A 觀察：反轉
    candidates = day[
        day["mom_5"].notna() &
        day["std_5"].notna() &
        (day["mom_5"] < -0.04) &
        (day["std_5"] < 0.08)
    ].copy()

    if len(candidates) < 3:
        fallback = day[day["mom_5"].notna()].copy()
        candidates = fallback.sort_values("mom_5", ascending=True).head(8).copy()
    else:
        candidates = candidates.sort_values("mom_5", ascending=True).head(8).copy()

    candidates = candidates[candidates["ret_1d"].notna()].copy()

    if candidates.empty:
        continue

    avg_ret = candidates["ret_1d"].mean()

    for _, r in candidates.iterrows():
        obs_rows.append({
            "engine": "A",
            "date": d,
            "symbol": r["symbol"],
            "market": r.get("market", ""),
            "close": r["close"],
            "mom_5": r["mom_5"],
            "std_5": r["std_5"],
            "ret_1d": r["ret_1d"],
        })

    rows.append({
        "date": d,
        "count": int(len(candidates)),
        "avg_ret": float(avg_ret),
    })

obs_df = pd.DataFrame(rows)
detail_df = pd.DataFrame(obs_rows)

if not obs_df.empty:
    summary = pd.DataFrame([{
        "days": int(len(obs_df)),
        "avg_count": float(obs_df["count"].mean()),
        "avg_ret": float(obs_df["avg_ret"].mean()),
        "win_rate": float((obs_df["avg_ret"] > 0).mean()),
    }])
else:
    summary = pd.DataFrame([{
        "days": 0,
        "avg_count": 0.0,
        "avg_ret": 0.0,
        "win_rate": 0.0,
    }])

obs_df.to_csv("v223_1_A_observer.csv", index=False)
detail_df.to_csv("v223_1_A_detail.csv", index=False)
summary.to_csv("v223_1_A_summary.csv", index=False)

print("DONE A OBSERVER")
print(summary.to_string(index=False))
