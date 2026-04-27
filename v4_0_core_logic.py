# v4_0_core_logic.py
# 目的：
# 重新建立「候選股 → 進場位置 → 防追高 → 風控 → 最終動作」核心邏輯
# 不改前端 app.js / index.html，只重新產出 mobile_dashboard_v1/data/trade_plan.csv
#
# 預期輸入：
# - price_panel_daily.csv
#   建議欄位：date, stock_id, close, volume
#   若有 open, high, low 更好；沒有也可跑基礎版
#
# 輸出：
# - mobile_dashboard_v1/data/trade_plan.csv
# - mobile_dashboard_v1/data/selection_debug.csv
#
# 重要觀念：
# BUY 不再代表直接買。
# v4 改成：
# BUY_PULLBACK = 回踩買點
# BUY_TEST     = 小倉試單
# WAIT         = 等回檔
# NO_CHASE     = 禁止追高
# WATCH        = 觀察
# HOLD         = 持有
# STOP_LOSS    = 停損

import os
import math
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CANDIDATES = [
    ROOT / "price_panel_daily.csv",
    ROOT / "data" / "price_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
]

OUT_TRADE = DATA_DIR / "trade_plan.csv"
OUT_DEBUG = DATA_DIR / "selection_debug.csv"

START_CAPITAL = 100000  # 可自行調整
MAX_NAMES = 20          # 每天最多輸出候選
MIN_PRICE = 10          # 排除太低價
MIN_AVG_VOLUME = 300000 # 20日均量門檻，可依台股狀況調整

BASE_COLUMNS = [
    "action",
    "stock_id",
    "price_tier",
    "ref_price",
    "target_weight",
    "suggested_amount",
    "note",
]


def find_input_file():
    for p in INPUT_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError(
        "找不到 price_panel_daily.csv，請放在 root、data/ 或 mobile_dashboard_v1/data/。"
    )


def price_tier(price):
    try:
        p = float(price)
    except Exception:
        return "unknown"
    if p < 50:
        return "lt_50"
    if p < 100:
        return "p50_100"
    if p < 300:
        return "p100_300"
    if p < 500:
        return "p300_500"
    if p < 1000:
        return "p500_1000"
    return "gt_1000"


def safe_pct(a, b):
    if b is None or b == 0 or pd.isna(b):
        return np.nan
    return a / b - 1


def normalize_columns(df):
    # 兼容不同欄位名稱
    rename_map = {}
    lower = {c.lower(): c for c in df.columns}

    candidates = {
        "date": ["date", "日期", "trade_date"],
        "stock_id": ["stock_id", "證券代號", "code", "symbol"],
        "close": ["close", "收盤價", "收盤", "price"],
        "volume": ["volume", "成交股數", "成交量", "vol"],
        "open": ["open", "開盤價", "開盤"],
        "high": ["high", "最高價", "最高"],
        "low": ["low", "最低價", "最低"],
    }

    for std, names in candidates.items():
        for n in names:
            if n.lower() in lower:
                rename_map[lower[n.lower()]] = std
                break

    df = df.rename(columns=rename_map)

    required = ["date", "stock_id", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"price_panel_daily.csv 缺少必要欄位：{missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    for c in ["open", "high", "low"]:
        if c not in df.columns:
            df[c] = df["close"]
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close", "volume"])
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df


def add_indicators(df):
    g = df.groupby("stock_id", group_keys=False)

    df["ma5"] = g["close"].rolling(5).mean().reset_index(level=0, drop=True)
    df["ma10"] = g["close"].rolling(10).mean().reset_index(level=0, drop=True)
    df["ma20"] = g["close"].rolling(20).mean().reset_index(level=0, drop=True)
    df["ma60"] = g["close"].rolling(60).mean().reset_index(level=0, drop=True)

    df["vol_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)

    df["ret_3d"] = g["close"].pct_change(3)
    df["ret_5d"] = g["close"].pct_change(5)
    df["ret_10d"] = g["close"].pct_change(10)
    df["ret_20d"] = g["close"].pct_change(20)

    df["high_20_prev"] = g["high"].rolling(20).max().shift(1).reset_index(level=0, drop=True)
    df["low_10_prev"] = g["low"].rolling(10).min().shift(1).reset_index(level=0, drop=True)

    df["dist_ma20"] = df["close"] / df["ma20"] - 1
    df["vol_ratio"] = df["volume"] / df["vol_ma20"]

    # 量增價弱：量大於20日均量1.5倍，但當日漲幅不強或收盤位置偏弱
    df["daily_ret"] = g["close"].pct_change(1)
    candle_range = (df["high"] - df["low"]).replace(0, np.nan)
    df["close_position"] = (df["close"] - df["low"]) / candle_range
    df["volume_up_price_weak"] = (
        (df["vol_ratio"] >= 1.5)
        & (df["daily_ret"] <= 0.01)
        & (df["close_position"].fillna(0.5) < 0.6)
    )

    return df


def classify_market(latest_rows):
    # 沒有大盤指數時，用全市場結構替代
    valid = latest_rows.dropna(subset=["ma20", "ma60", "ret_20d"])
    if valid.empty:
        return "neutral"

    above_ma20 = (valid["close"] > valid["ma20"]).mean()
    above_ma60 = (valid["close"] > valid["ma60"]).mean()
    avg_ret20 = valid["ret_20d"].mean()

    if above_ma20 >= 0.55 and above_ma60 >= 0.50 and avg_ret20 > 0:
        return "bull"
    if above_ma20 <= 0.40 and above_ma60 <= 0.45 and avg_ret20 < 0:
        return "bear"
    return "neutral"


def classify_entry(row):
    close = row["close"]
    ma10 = row["ma10"]
    ma20 = row["ma20"]
    high20 = row["high_20_prev"]

    if pd.isna(ma10) or pd.isna(ma20) or pd.isna(high20):
        return "資料不足"

    breakout = close > high20
    trend_ok = close > ma20 and ma10 > ma20
    pullback_ok = (
        trend_ok
        and abs(close / ma20 - 1) <= 0.05
        and row["ret_5d"] < 0.08
    )

    if breakout and row["ret_5d"] <= 0.12 and row["dist_ma20"] <= 0.12:
        return "剛突破"
    if pullback_ok:
        return "回踩不破"
    if trend_ok and row["ret_20d"] > 0.08 and row["dist_ma20"] <= 0.12:
        return "主升中"
    if row["dist_ma20"] > 0.15 or row["ret_5d"] > 0.15 or row["ret_10d"] > 0.22:
        return "過熱末段"
    if close < ma20:
        return "轉弱回檔"
    return "觀察"


def decide_action(row, market_state):
    stage = classify_entry(row)
    reasons = []

    # 大盤空頭：禁止新買
    if market_state == "bear":
        return "WATCH", "高", "大盤偏空，只觀察不新買"

    # 硬性禁止
    if bool(row["volume_up_price_weak"]):
        return "NO_CHASE", "高", "量增價弱，禁止新進場"

    if row["ret_5d"] > 0.15:
        return "NO_CHASE", "高", "5日漲幅過大，禁止追高"

    if row["ret_10d"] > 0.22:
        return "NO_CHASE", "高", "10日漲幅過大，禁止追高"

    if row["dist_ma20"] > 0.15:
        return "WAIT", "中高", "距離20日線過遠，等回檔"

    if stage == "回踩不破":
        if market_state == "bull":
            return "BUY_PULLBACK", "中", "回踩不破，較佳進場點"
        return "BUY_TEST", "中高", "震盪盤回踩不破，小倉試單"

    if stage == "剛突破":
        if market_state == "bull":
            return "BUY_TEST", "中", "剛突破，允許小倉試單"
        return "WAIT", "中高", "震盪盤剛突破，先等確認"

    if stage == "主升中":
        return "WAIT", "中", "主升中但非最佳買點，等回檔"

    if stage in ["過熱末段", "轉弱回檔"]:
        return "NO_CHASE", "高", f"{stage}，不新買"

    return "WATCH", "中", "未到進場位置"


def position_size(action, risk_level, market_state):
    if action in ["NO_CHASE", "WAIT", "WATCH"]:
        return 0, 0.0

    if action == "BUY_PULLBACK":
        weight = 0.08
    elif action == "BUY_TEST":
        weight = 0.04
    else:
        weight = 0.03

    if market_state == "neutral":
        weight *= 0.6
    if risk_level in ["中高", "高"]:
        weight *= 0.5

    amount = int(START_CAPITAL * weight)
    return amount, round(weight, 4)


def build_trade_plan(df):
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    debug = {
        "total_input": len(latest),
        "valid_after_na": 0,
        "core_primary_count": 0,
        "alpha_primary_count": 0,
        "core_final_count": 0,
        "alpha_final_count": 0,
        "market_state": "",
        "signal_date": str(latest_date.date()) if pd.notna(latest_date) else "",
    }

    latest = latest.dropna(subset=["ma20", "ma60", "vol_ma20", "ret_5d", "ret_10d", "ret_20d"])
    debug["valid_after_na"] = len(latest)

    market_state = classify_market(latest)
    debug["market_state"] = market_state

    # 候選池：先不要太嚴，避免又變空檔
    candidates = latest[
        (latest["close"] >= MIN_PRICE)
        & (latest["vol_ma20"] >= MIN_AVG_VOLUME)
        & (latest["close"] > latest["ma20"])
        & (latest["ma20"] >= latest["ma60"] * 0.95)
        & (latest["ret_20d"] > 0)
    ].copy()

    debug["core_primary_count"] = len(candidates)

    if candidates.empty:
        pd.DataFrame(columns=BASE_COLUMNS).to_csv(OUT_TRADE, index=False, encoding="utf-8-sig")
        pd.DataFrame([debug]).to_csv(OUT_DEBUG, index=False, encoding="utf-8-sig")
        return pd.DataFrame(columns=BASE_COLUMNS), pd.DataFrame([debug])

    # 分數：強度只是排序，不等於買進
    candidates["score"] = (
        candidates["ret_20d"].fillna(0) * 0.35
        + candidates["ret_10d"].fillna(0) * 0.25
        + candidates["ret_5d"].fillna(0) * 0.15
        + np.log1p(candidates["vol_ratio"].fillna(1)) * 0.10
        - candidates["dist_ma20"].clip(lower=0).fillna(0) * 0.15
    )

    candidates = candidates.sort_values("score", ascending=False).head(MAX_NAMES)
    debug["alpha_primary_count"] = len(candidates)

    out = []
    for _, r in candidates.iterrows():
        action, risk, reason = decide_action(r, market_state)
        amount, weight = position_size(action, risk, market_state)
        stage = classify_entry(r)

        note = (
            f"{stage}｜{reason}｜"
            f"市場:{market_state}｜"
            f"5日:{r['ret_5d']:.2%}｜10日:{r['ret_10d']:.2%}｜"
            f"距20MA:{r['dist_ma20']:.2%}｜量比:{r['vol_ratio']:.2f}"
        )

        out.append({
            "action": action,
            "stock_id": r["stock_id"],
            "price_tier": price_tier(r["close"]),
            "ref_price": round(float(r["close"]), 2),
            "target_weight": weight,
            "suggested_amount": amount,
            "note": note,
        })

    trade = pd.DataFrame(out, columns=BASE_COLUMNS)

    # 最終排序：真正可買優先，其次等待，再其次觀察/禁止
    rank = {
        "BUY_PULLBACK": 1,
        "BUY_TEST": 2,
        "WAIT": 3,
        "WATCH": 4,
        "NO_CHASE": 5,
    }
    trade["_rank"] = trade["action"].map(rank).fillna(9)
    trade = trade.sort_values(["_rank", "suggested_amount"], ascending=[True, False]).drop(columns=["_rank"])

    debug["core_final_count"] = int((trade["action"].isin(["BUY_PULLBACK", "BUY_TEST"])).sum())
    debug["alpha_final_count"] = len(trade)

    trade.to_csv(OUT_TRADE, index=False, encoding="utf-8-sig")
    pd.DataFrame([debug]).to_csv(OUT_DEBUG, index=False, encoding="utf-8-sig")

    return trade, pd.DataFrame([debug])


def main():
    in_file = find_input_file()
    print(f"[v4.0] input = {in_file}")

    raw = pd.read_csv(in_file)
    df = normalize_columns(raw)
    df = add_indicators(df)

    trade, debug = build_trade_plan(df)

    print(f"[v4.0] output trade_plan = {OUT_TRADE}")
    print(f"[v4.0] output debug = {OUT_DEBUG}")
    print(debug.to_string(index=False))
    print(trade.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
