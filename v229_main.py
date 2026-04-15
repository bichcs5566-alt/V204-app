# v229_main.txt
# 修正版：自動辨識日期欄位（trade_date / date / datetime / signal_date）
# 並處理大小寫、空白、BOM 問題

import pandas as pd
import numpy as np

FILE = "price_panel_daily.csv"
CAPITAL = 100000.0

df = pd.read_csv(FILE)

# 欄位清理
df.columns = [
    str(c).replace("\ufeff", "").strip().lower()
    for c in df.columns
]

# 自動找日期欄位
date_candidates = ["trade_date", "date", "datetime", "signal_date"]
found_date_col = None
for c in date_candidates:
    if c in df.columns:
        found_date_col = c
        break

if found_date_col is None:
    raise Exception(f"缺少日期欄位，可接受欄位: {date_candidates}，目前欄位: {df.columns.tolist()}")

if "symbol" not in df.columns:
    raise Exception(f"缺少欄位: symbol，目前欄位: {df.columns.tolist()}")

if "close" not in df.columns:
    raise Exception(f"缺少欄位: close，目前欄位: {df.columns.tolist()}")

df["trade_date"] = pd.to_datetime(df[found_date_col], errors="coerce")
df["symbol"] = df["symbol"].astype(str)
df["close"] = pd.to_numeric(df["close"], errors="coerce")

df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

g = df.groupby("symbol", group_keys=False)

# 特徵
df["ret"] = g["close"].pct_change().shift(-1)
df["mom3"] = g["close"].pct_change(3)
df["mom5"] = g["close"].pct_change(5)
df["mom10"] = g["close"].pct_change(10)

# 市場 regime
market = (
    df.groupby("trade_date", as_index=False)["close"]
    .mean()
    .rename(columns={"close": "market_close"})
)
market["ma5"] = market["market_close"].rolling(5).mean()
market["ma10"] = market["market_close"].rolling(10).mean()
market["ma20"] = market["market_close"].rolling(20).mean()

def get_regime(row):
    if pd.notna(row["ma5"]) and pd.notna(row["ma10"]) and pd.notna(row["ma20"]):
        if row["ma5"] > row["ma10"] > row["ma20"]:
            return "strong"
        elif row["ma10"] > row["ma20"]:
            return "normal"
        else:
            return "weak"
    return "weak"

market["regime"] = market.apply(get_regime, axis=1)
df = df.merge(market[["trade_date", "regime"]], on="trade_date", how="left")

def engine_A(day):
    d = day.dropna(subset=["mom3", "mom5", "ret"]).copy()
    d = d[(d["mom3"] > 0.02) & (d["mom5"] > 0.03)]
    if d.empty:
        return 0.0
    d["score"] = d["mom3"] * 0.6 + d["mom5"] * 0.4
    d = d.sort_values("score", ascending=False).head(3).copy()
    d["ret"] = d["ret"].clip(lower=-0.08)
    return float(d["ret"].mean())

def engine_B(day):
    d = day.dropna(subset=["mom5", "mom10", "ret"]).copy()
    d = d[(d["mom5"] > 0) & (d["mom10"] > 0)]
    if d.empty:
        return 0.0
    d = d.sort_values("mom10", ascending=False).head(8).copy()
    d["ret"] = d["ret"].clip(lower=-0.05)
    return float(d["ret"].mean())

nav = CAPITAL
peak = CAPITAL
rows = []
nav_hist = []

for d in sorted(df["trade_date"].dropna().unique()):
    day = df[df["trade_date"] == d].copy()
    if day.empty:
        continue

    regime = str(day["regime"].iloc[0])

    # 基礎權重
    if regime == "strong":
        wA, wB = 0.8, 0.6
    elif regime == "normal":
        wA, wB = 0.2, 0.5
    else:
        # 弱市真空倉
        wA, wB = 0.0, 0.0

    # NAV 趨勢轉弱時停 A
    if len(nav_hist) >= 10:
        nav5 = pd.Series(nav_hist[-5:]).mean()
        nav10 = pd.Series(nav_hist[-10:]).mean()
        nav_weak = nav5 < nav10
    else:
        nav_weak = False

    if nav_weak:
        wA = 0.0

    # 組合回撤硬風控
    current_dd = nav / peak - 1.0
    if current_dd <= -0.08:
        wA *= 0.25
        wB *= 0.50
    if current_dd <= -0.12:
        wA = 0.0
        wB *= 0.20

    rA = engine_A(day) if wA > 0 else 0.0
    rB = engine_B(day) if wB > 0 else 0.0

    total_ret = rA * wA + rB * wB

    nav *= (1.0 + total_ret)
    peak = max(peak, nav)
    dd = nav / peak - 1.0
    nav_hist.append(nav)

    rows.append([
        d, nav, rA, rB, regime, wA, wB, nav_weak, total_ret, dd
    ])

out = pd.DataFrame(rows, columns=[
    "date", "nav", "retA", "retB", "regime", "wA", "wB", "nav_weak", "total_ret", "dd"
])

summary = pd.DataFrame([{
    "return": float(nav / CAPITAL - 1.0),
    "mdd": float(out["dd"].min()) if not out.empty else 0.0,
    "final_nav": float(nav),
    "strong_days": int((out["regime"] == "strong").sum()) if not out.empty else 0,
    "normal_days": int((out["regime"] == "normal").sum()) if not out.empty else 0,
    "weak_days": int((out["regime"] == "weak").sum()) if not out.empty else 0,
    "nav_weak_days": int(out["nav_weak"].fillna(False).sum()) if not out.empty else 0,
    "avg_wA": float(out["wA"].mean()) if not out.empty else 0.0,
    "avg_wB": float(out["wB"].mean()) if not out.empty else 0.0
}])

out.to_csv("v229_nav.csv", index=False)
summary.to_csv("v229_summary.csv", index=False)

print("DONE v229")
print(f"date column used: {found_date_col}")
print(summary.to_string(index=False))
