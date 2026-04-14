# v228_main.py
# 市場切換引擎（Regime Switching）

import pandas as pd
import numpy as np

FILE = "price_panel_daily.csv"
CAPITAL = 100000

df = pd.read_csv(FILE)
df.columns = [c.lower().strip() for c in df.columns]

# === 日期處理 ===
if "trade_date" not in df.columns:
    if "date" in df.columns:
        df["trade_date"] = df["date"]
    else:
        raise Exception("缺少 trade_date")

df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values(["symbol", "trade_date"])

g = df.groupby("symbol")

# === 指標 ===
df["ret"] = g["close"].pct_change().shift(-1)
df["mom3"] = g["close"].pct_change(3)
df["mom5"] = g["close"].pct_change(5)
df["mom10"] = g["close"].pct_change(10)

# === 市場 regime ===
market = df.groupby("trade_date")["close"].mean().reset_index()
market["ma5"] = market["close"].rolling(5).mean()
market["ma10"] = market["close"].rolling(10).mean()
market["ma20"] = market["close"].rolling(20).mean()

# 三種市場
def get_regime(row):
    if row["ma5"] > row["ma10"] and row["ma10"] > row["ma20"]:
        return "strong"
    elif row["ma10"] > row["ma20"]:
        return "normal"
    else:
        return "weak"

market["regime"] = market.apply(get_regime, axis=1)

df = df.merge(market[["trade_date","regime"]], on="trade_date")

# === A 引擎（攻擊）===
def engine_A(day):
    d = day.dropna(subset=["mom3","mom5","ret"])
    d = d[(d["mom3"] > 0.02) & (d["mom5"] > 0.03)]
    if d.empty:
        return 0
    d["score"] = d["mom3"]*0.6 + d["mom5"]*0.4
    d = d.sort_values("score", ascending=False).head(3)
    d["ret"] = d["ret"].clip(lower=-0.08)
    return d["ret"].mean()

# === B 引擎（防守）===
def engine_B(day):
    d = day.dropna(subset=["mom5","mom10","ret"])
    d = d[(d["mom5"] > 0) & (d["mom10"] > 0)]
    if d.empty:
        return 0
    d = d.sort_values("mom10", ascending=False).head(8)
    d["ret"] = d["ret"].clip(lower=-0.05)
    return d["ret"].mean()

# === 主回測 ===
nav = CAPITAL
peak = nav
rows = []

for d in sorted(df["trade_date"].unique()):
    day = df[df["trade_date"] == d]
    regime = day["regime"].iloc[0]

    # === 市場切換 ===
    if regime == "strong":
        wA, wB = 0.8, 0.6
    elif regime == "normal":
        wA, wB = 0.4, 0.6
    else:
        wA, wB = 0.0, 0.2   # 幾乎空倉

    rA = engine_A(day)
    rB = engine_B(day)

    total_ret = rA*wA + rB*wB

    nav *= (1 + total_ret)
    peak = max(peak, nav)
    dd = nav / peak - 1

    rows.append([d, nav, rA, rB, regime, total_ret, dd])

out = pd.DataFrame(rows, columns=[
    "date","nav","retA","retB","regime","total_ret","dd"
])

out.to_csv("v228_nav.csv", index=False)

summary = pd.DataFrame([{
    "return": nav/CAPITAL - 1,
    "mdd": out["dd"].min(),
    "final_nav": nav
}])

summary.to_csv("v228_summary.csv", index=False)

print("DONE v228")
print(summary)
