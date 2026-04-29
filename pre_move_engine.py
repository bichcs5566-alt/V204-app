"""
pre_move_engine.py

主力佈局偵測引擎 Pre-Move Engine

定位：
- 不是追強
- 不是已啟動後才買
- 是偵測「主升段啟動前」的主力佈局痕跡

輸入：
- price_panel_daily.csv
  或 feature_panel_daily.csv

輸出：
- pre_move_candidates.csv
- pre_move_summary.json
- mobile_dashboard_v1/data/pre_move_candidates.csv
- mobile_dashboard_v1/data/pre_move_summary.json

分數：
- 80 以上：BUY / 小倉試單
- 65-79：TEST / 觀察試單
- 50-64：WATCH / 主力觀察
"""

from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CANDIDATES = [
    ROOT / "feature_panel_daily.csv",
    ROOT / "price_panel_daily.csv",
    DATA_DIR / "feature_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
]

OUTPUT_COLUMNS = [
    "date",
    "stock_id",
    "action",
    "pre_score",
    "close",
    "price_tier",
    "target_weight",
    "suggested_amount",
    "setup_type",
    "signal_tags",
    "vol_squeeze",
    "quiet_accumulation",
    "shakeout",
    "ma_compress",
    "volume_absorb",
    "near_breakout",
]


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def find_input_file():
    for p in INPUT_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError("找不到 feature_panel_daily.csv 或 price_panel_daily.csv")


def price_tier(close):
    try:
        x = float(close)
    except Exception:
        return "未知"
    if x < 50:
        return "50以下"
    if x < 100:
        return "50-100"
    if x < 500:
        return "100-500"
    if x < 1000:
        return "500-1000"
    return "1000以上"


def action_from_score(score):
    if score >= 80:
        return "BUY"
    if score >= 65:
        return "TEST"
    if score >= 50:
        return "WATCH"
    return "SKIP"


def weight_from_score(score):
    if score >= 80:
        return 0.01   # 主力佈局期只給 1%，不要重倉
    if score >= 65:
        return 0.005  # 試單 0.5%
    return 0.0


def amount_from_score(score):
    if score >= 80:
        return 10000
    if score >= 65:
        return 5000
    return 0


def safe_load_panel(path):
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
        else:
            raise ValueError("缺少 date / trade_date 欄位")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        elif "ticker" in df.columns:
            df["stock_id"] = df["ticker"].astype(str).str.extract(r"(\d{4})")[0]
        else:
            raise ValueError("缺少 stock_id / symbol / code / ticker 欄位")

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            if col in ["open", "high", "low"] and "close" in df.columns:
                df[col] = df["close"]
            elif col == "volume":
                df[col] = 0
            else:
                raise ValueError(f"缺少必要欄位：{col}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)].copy()
    df = df[df["close"] > 0].copy()
    df["volume"] = df["volume"].fillna(0)

    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)


def build_features(g):
    g = g.sort_values("date").copy()

    g["ret1"] = g["close"].pct_change()
    g["range_pct"] = (g["high"] - g["low"]) / g["close"].replace(0, np.nan)
    g["ma5"] = g["close"].rolling(5).mean()
    g["ma10"] = g["close"].rolling(10).mean()
    g["ma20"] = g["close"].rolling(20).mean()
    g["ma60"] = g["close"].rolling(60).mean()

    g["vol_ma20"] = g["volume"].rolling(20).mean()
    g["vol_ma60"] = g["volume"].rolling(60).mean()

    g["volatility_20"] = g["ret1"].rolling(20).std()
    g["volatility_60"] = g["ret1"].rolling(60).std()

    g["range20"] = g["range_pct"].rolling(20).mean()
    g["range60"] = g["range_pct"].rolling(60).mean()

    g["high20"] = g["high"].rolling(20).max()
    g["low20"] = g["low"].rolling(20).min()
    g["low10_prev"] = g["low"].shift(1).rolling(10).min()
    g["high10_prev"] = g["high"].shift(1).rolling(10).max()

    g["mom20"] = g["close"] / g["close"].shift(20) - 1
    g["mom60"] = g["close"] / g["close"].shift(60) - 1

    g["ma_gap_5_20"] = (g["ma5"] - g["ma20"]).abs() / g["close"]
    g["ma_gap_10_20"] = (g["ma10"] - g["ma20"]).abs() / g["close"]
    g["ma_compress_raw"] = g[["ma_gap_5_20", "ma_gap_10_20"]].mean(axis=1)

    return g


def score_latest(g):
    if len(g) < 80:
        return None

    g = build_features(g)
    r = g.iloc[-1].copy()

    close = r["close"]
    if not np.isfinite(close) or close <= 0:
        return None

    # 1. 波動壓縮：現在 20 日波動低於 60 日均態
    vol_squeeze = (
        pd.notna(r["volatility_20"]) and pd.notna(r["volatility_60"]) and
        r["volatility_20"] < r["volatility_60"] * 0.85
    )

    # 2. 安靜吸籌：量能回升，但價格沒有大漲
    quiet_accumulation = (
        pd.notna(r["vol_ma20"]) and r["vol_ma20"] > 0 and
        r["volume"] > r["vol_ma20"] * 1.25 and
        abs(r["ret1"]) < 0.025
    )

    # 3. 洗盤收回：跌破短低後收回
    shakeout = (
        pd.notna(r["low10_prev"]) and
        r["low"] < r["low10_prev"] and
        r["close"] > r["low10_prev"]
    )

    # 4. 均線收斂：5/10/20 貼近，表示壓縮
    ma_compress = (
        pd.notna(r["ma_compress_raw"]) and
        r["ma_compress_raw"] < 0.035
    )

    # 5. 吸收量：量放大但 K 線實體不大
    body_pct = abs(r["close"] - r["open"]) / close if pd.notna(r["open"]) else np.nan
    volume_absorb = (
        pd.notna(r["vol_ma20"]) and r["vol_ma20"] > 0 and
        r["volume"] > r["vol_ma20"] * 1.5 and
        pd.notna(body_pct) and body_pct < 0.025
    )

    # 6. 接近突破但尚未噴出：離 20 日高點近，但漲幅沒有過熱
    near_breakout = (
        pd.notna(r["high20"]) and r["high20"] > 0 and
        close >= r["high20"] * 0.94 and
        pd.notna(r["mom20"]) and r["mom20"] < 0.18
    )

    # 7. 中期不破底：60 日動能不能太差，避免抓到弱勢爛股
    structure_ok = (
        pd.notna(r["mom60"]) and r["mom60"] > -0.12 and
        pd.notna(r["ma60"]) and close > r["ma60"] * 0.88
    )

    score = 0
    score += 18 if vol_squeeze else 0
    score += 18 if quiet_accumulation else 0
    score += 14 if shakeout else 0
    score += 16 if ma_compress else 0
    score += 14 if volume_absorb else 0
    score += 14 if near_breakout else 0
    score += 6 if structure_ok else 0

    # 過熱扣分：主力佈局不是追高
    if pd.notna(r["mom20"]) and r["mom20"] > 0.22:
        score -= 18
    if pd.notna(r["ret1"]) and r["ret1"] > 0.07:
        score -= 12

    score = max(0, min(100, int(round(score))))

    tags = []
    if vol_squeeze:
        tags.append("波動壓縮")
    if quiet_accumulation:
        tags.append("安靜吸籌")
    if shakeout:
        tags.append("洗盤收回")
    if ma_compress:
        tags.append("均線收斂")
    if volume_absorb:
        tags.append("放量吸收")
    if near_breakout:
        tags.append("接近突破")
    if structure_ok:
        tags.append("結構尚可")

    if score >= 80:
        setup_type = "主力佈局完成"
    elif score >= 65:
        setup_type = "佈局高機率"
    elif score >= 50:
        setup_type = "佈局觀察"
    else:
        setup_type = "略過"

    return {
        "date": r["date"].strftime("%Y-%m-%d"),
        "stock_id": r["stock_id"],
        "action": action_from_score(score),
        "pre_score": score,
        "close": round(float(close), 2),
        "price_tier": price_tier(close),
        "target_weight": weight_from_score(score),
        "suggested_amount": amount_from_score(score),
        "setup_type": setup_type,
        "signal_tags": " | ".join(tags) if tags else "尚未形成",
        "vol_squeeze": bool(vol_squeeze),
        "quiet_accumulation": bool(quiet_accumulation),
        "shakeout": bool(shakeout),
        "ma_compress": bool(ma_compress),
        "volume_absorb": bool(volume_absorb),
        "near_breakout": bool(near_breakout),
    }


def main():
    src = find_input_file()
    print("pre_move input:", src)

    df = safe_load_panel(src)
    print("rows:", len(df), "stocks:", df["stock_id"].nunique())

    results = []
    for stock_id, g in df.groupby("stock_id", sort=False):
        item = score_latest(g)
        if item is not None:
            results.append(item)

    out = pd.DataFrame(results)
    if out.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        out = out[OUTPUT_COLUMNS]
        out = out[out["action"].isin(["BUY", "TEST", "WATCH"])].copy()
        out = out.sort_values(["pre_score", "stock_id"], ascending=[False, True]).reset_index(drop=True)

    # 控制 UI 數量：不要爆版
    ui_out = out.head(30).copy()

    out.to_csv(ROOT / "pre_move_candidates.csv", index=False, encoding="utf-8")
    ui_out.to_csv(DATA_DIR / "pre_move_candidates.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "pre_move_engine",
        "input_file": str(src),
        "total_candidates": int(len(out)),
        "ui_candidates": int(len(ui_out)),
        "buy_count": int((out["action"] == "BUY").sum()) if not out.empty else 0,
        "test_count": int((out["action"] == "TEST").sum()) if not out.empty else 0,
        "watch_count": int((out["action"] == "WATCH").sum()) if not out.empty else 0,
        "latest_date": str(df["date"].max().date()) if not df.empty else "",
        "note": "PRE 是主力佈局預判，只適合小倉試單，不等於正式重倉買進。",
    }

    for p in [ROOT / "pre_move_summary.json", DATA_DIR / "pre_move_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("pre_move completed")


if __name__ == "__main__":
    main()
