#v224_main.py
# 自動相容版：
# 1) price_panel_daily.csv 若只有 date/symbol/close/market 也能跑
# 2) 若沒有 mom_5 / mom_10 / ret_1d，會自動由 close 推算
# 3) Regime Engine + B策略 + 風控 + 空倉機制

import pandas as pd
import numpy as np

PRICE_FILE = "price_panel_daily.csv"
INITIAL_CAPITAL = 100000.0

df = pd.read_csv(PRICE_FILE)
df.columns = [c.strip().lower() for c in df.columns]

# ---- 日期欄位自動對齊 ----
if "trade_date" not in df.columns:
    if "date" in df.columns:
        df["trade_date"] = df["date"]
    elif "datetime" in df.columns:
        df["trade_date"] = df["datetime"]
    else:
        raise Exception(f"❌ 沒有找到 trade_date/date/datetime 欄位，目前欄位: {df.columns.tolist()}")

# ---- 基本欄位檢查 ----
base_required = ["symbol", "close"]
for col in base_required:
    if col not in df.columns:
        raise Exception(f"❌ 缺少基本欄位: {col}，目前欄位: {df.columns.tolist()}")

df["trade_date"] = pd.to_datetime(df["trade_date"])
df["symbol"] = df["symbol"].astype(str)
df["close"] = pd.to_numeric(df["close"], errors="coerce")
df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

# ---- 自動推算技術欄位 ----
g = df.groupby("symbol", group_keys=False)

if "ret_1d" not in df.columns:
    df["ret_1d"] = g["close"].pct_change().shift(-1)

if "mom_5" not in df.columns:
    df["mom_5"] = g["close"].pct_change(5)

if "mom_10" not in df.columns:
    df["mom_10"] = g["close"].pct_change(10)

# 額外風控欄位
if "std_5" not in df.columns:
    daily_ret = g["close"].pct_change()
    df["std_5"] = daily_ret.groupby(df["symbol"]).rolling(5).std().reset_index(level=0, drop=True)

# ---- 市場 regime：用全市場平均 close 近似 ----
market = df.groupby("trade_date", as_index=False)["close"].mean().rename(columns={"close": "market_close"})
market["ma20"] = market["market_close"].rolling(20).mean()
market["ma60"] = market["market_close"].rolling(60).mean()
market["risk_on"] = market["ma20"] > market["ma60"]

df = df.merge(market[["trade_date", "risk_on"]], on="trade_date", how="left")

def select_B(sub: pd.DataFrame) -> pd.DataFrame:
    cand = sub[
        sub["mom_5"].notna() &
        sub["mom_10"].notna() &
        sub["ret_1d"].notna() &
        (sub["mom_5"] > 0.03) &
        (sub["mom_5"] < 0.20) &
        (sub["mom_10"] > 0.05)
    ].copy()

    # 若條件太嚴導致沒交易，退化成排序制，避免空跑
    if len(cand) < 4:
        cand = sub[
            sub["mom_5"].notna() &
            sub["mom_10"].notna() &
            sub["ret_1d"].notna()
        ].copy().sort_values(["mom_10", "mom_5"], ascending=False).head(8)
    else:
        cand = cand.sort_values(["mom_10", "mom_5"], ascending=False).head(8)

    return cand

dates = sorted(df["trade_date"].dropna().unique())
nav = INITIAL_CAPITAL
peak = INITIAL_CAPITAL

nav_rows = []
trade_rows = []

for d in dates:
    day = df[df["trade_date"] == d].copy()
    if day.empty:
        continue

    risk_on = bool(day["risk_on"].iloc[0]) if pd.notna(day["risk_on"].iloc[0]) else False

    # 沒有足夠市場均線資料時，保守採空倉
    if not risk_on:
        nav_rows.append({
            "trade_date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "nav": nav,
        })
        continue

    picks = select_B(day)

    if picks.empty:
        nav_rows.append({
            "trade_date": d,
            "daily_ret": 0.0,
            "holdings": 0,
            "gross_exposure": 0.0,
            "cash_mode": True,
            "nav": nav,
        })
        continue

    # 風控：單日最大損失截斷
    picks["ret_1d"] = picks["ret_1d"].clip(lower=-0.05)

    # 曝險控制
    gross_exposure = 0.5 if len(picks) >= 6 else 0.35
    weight = gross_exposure / len(picks)

    picks["weight"] = weight
    picks["wret"] = picks["ret_1d"] * picks["weight"]
    daily_ret = float(picks["wret"].sum())

    nav *= (1.0 + daily_ret)
    peak = max(peak, nav)
    drawdown = nav / peak - 1.0

    nav_rows.append({
        "trade_date": d,
        "daily_ret": daily_ret,
        "holdings": int(len(picks)),
        "gross_exposure": gross_exposure,
        "cash_mode": False,
        "nav": nav,
        "drawdown": drawdown,
    })

    out = picks.copy()
    out["engine"] = "B"
    out["trade_date"] = d
    keep_cols = ["engine", "trade_date", "symbol", "market", "close", "mom_5", "mom_10", "std_5", "ret_1d", "weight", "wret"]
    for c in keep_cols:
        if c not in out.columns:
            out[c] = np.nan
    trade_rows.append(out[keep_cols])

nav_df = pd.DataFrame(nav_rows)
trades_df = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame(
    columns=["engine", "trade_date", "symbol", "market", "close", "mom_5", "mom_10", "std_5", "ret_1d", "weight", "wret"]
)

if nav_df.empty:
    summary = pd.DataFrame([{
        "start_date": None,
        "end_date": None,
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": INITIAL_CAPITAL,
        "total_return": 0.0,
        "trading_days": 0,
        "nonzero_ret_days": 0,
        "avg_holdings": 0.0,
        "avg_exposure": 0.0,
        "cash_days": 0,
        "sharpe": 0.0,
        "mdd": 0.0,
    }])
else:
    daily = nav_df["daily_ret"].fillna(0.0)
    sharpe = 0.0
    if len(daily) > 1 and daily.std(ddof=1) > 0:
        sharpe = float((daily.mean() / daily.std(ddof=1)) * np.sqrt(252))

    summary = pd.DataFrame([{
        "start_date": str(pd.to_datetime(nav_df["trade_date"].min()).date()),
        "end_date": str(pd.to_datetime(nav_df["trade_date"].max()).date()),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": float(nav_df["nav"].iloc[-1]),
        "total_return": float(nav_df["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "trading_days": int(len(nav_df)),
        "nonzero_ret_days": int((nav_df["daily_ret"].abs() > 0).sum()),
        "avg_holdings": float(nav_df["holdings"].mean()),
        "avg_exposure": float(nav_df["gross_exposure"].mean()),
        "cash_days": int(nav_df["cash_mode"].sum()),
        "sharpe": sharpe,
        "mdd": float(nav_df["drawdown"].min()) if "drawdown" in nav_df.columns else 0.0,
    }])

nav_df.to_csv("v224_nav.csv", index=False)
trades_df.to_csv("v224_B_trades.csv", index=False)
summary.to_csv("v224_summary.csv", index=False)

print("✅ DONE V224")
print("Columns:", df.columns.tolist())
print(summary.to_string(index=False))


