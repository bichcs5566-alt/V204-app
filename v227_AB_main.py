# v227_AB_main.py

import pandas as pd
import numpy as np

FILE = "price_panel_daily.csv"
CAPITAL = 100000

df = pd.read_csv(FILE)
df.columns = [c.lower().strip() for c in df.columns]

# === 保證 trade_date 存在 ===
if "trade_date" not in df.columns:
    if "date" in df.columns:
        df["trade_date"] = df["date"]
    else:
        raise Exception("缺少 trade_date")

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values(["symbol", "trade_date"])

g = df.groupby("symbol")

# === 計算報酬 & 動能 ===
df["ret"] = g["close"].pct_change().shift(-1)
df["mom3"] = g["close"].pct_change(3)
df["mom5"] = g["close"].pct_change(5)
df["mom10"] = g["close"].pct_change(10)

# === 市場風險控制 ===
market = df.groupby("trade_date")["close"].mean().reset_index()
market["ma10"] = market["close"].rolling(10).mean()
market["ma20"] = market["close"].rolling(20).mean()
market["risk"] = market["ma10"] > market["ma20"]

df = df.merge(market[["trade_date", "risk"]], on="trade_date")

# === A 引擎（短線爆發）===
def engine_A(day):
    d = day.dropna(subset=["mom3", "mom5", "ret"])
    d = d[(d["mom3"] > 0.02) & (d["mom5"] > 0.03)]
    if d.empty:
        return 0
    d["score"] = d["mom3"] * 0.6 + d["mom5"] * 0.4
    d = d.sort_values("score", ascending=False).head(3)
    d["ret"] = d["ret"].clip(lower=-0.08)
    return d["ret"].mean() * 0.8

# === B 引擎（波段穩定）===
def engine_B(day):
    d = day.dropna(subset=["mom5", "mom10", "ret"])
    d = d[(d["mom5"] > 0) & (d["mom10"] > 0)]
    if d.empty:
        return 0
    d = d.sort_values("mom10", ascending=False).head(8)
    d["ret"] = d["ret"].clip(lower=-0.05)
    return d["ret"].mean() * 0.5

# === 回測 ===
nav = CAPITAL
peak = nav
rows = []

for d in sorted(df["trade_date"].unique()):
    day = df[df["trade_date"] == d]

    if not day["risk"].iloc[0]:
        rows.append([d, nav, 0, 0, 0])
        continue

    ra = engine_A(day)
    rb = engine_B(day)
    r = ra + rb

    nav *= (1 + r)
    peak = max(peak, nav)
    dd = nav / peak - 1

    rows.append([d, nav, ra, rb, dd])

out = pd.DataFrame(rows, columns=["date", "nav", "retA", "retB", "dd"])
out.to_csv("v227_nav.csv", index=False)

summary = pd.DataFrame([{
    "return": nav / CAPITAL - 1,
    "mdd": out["dd"].min(),
    "final_nav": nav
}])

summary.to_csv("v227_summary.csv", index=False)

print(summary)
