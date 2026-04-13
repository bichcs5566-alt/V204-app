# v223_1_A_observer.py
# ==========================================
# A 引擎觀察版（不進資金）
# ==========================================

import pandas as pd

df = pd.read_csv("price_panel.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["symbol", "date"])

g = df.groupby("symbol")

df["mom5"] = g["close"].pct_change(5)
df["std5"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)

dates = sorted(df["date"].unique())

rows = []

for i in range(len(dates) - 3):
    signal = dates[i]
    trade = dates[i + 1]
    exit_d = dates[i + 3]

    snap = df[df["date"] == signal]

    cond = (
        (snap["mom5"] < -0.08) &
        (snap["std5"] < 0.1)
    )

    pick = snap[cond].head(5)

    for _, r in pick.iterrows():
        try:
            buy = df[(df["symbol"] == r["symbol"]) & (df["date"] == trade)]["close"].values[0]
            sell = df[(df["symbol"] == r["symbol"]) & (df["date"] == exit_d)]["close"].values[0]
        except:
            continue

        rows.append({
            "symbol": r["symbol"],
            "ret": sell / buy - 1
        })

out = pd.DataFrame(rows)
out.to_csv("v223_1_A_observer.csv", index=False)

print("A觀察完成")
