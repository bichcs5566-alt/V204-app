"""
market_regime.py
v266.11 市場濾網版

輸出：
- market_regime.json / market_regime.csv
- mobile_dashboard_v1/data/market_regime.json / market_regime.csv

邏輯：
1. 優先使用大盤指數檔：market_index_daily.csv / index_daily.csv / taiex_daily.csv / twii_daily.csv
2. 若沒有指數檔，使用 price_panel_daily.csv 或 feature_panel_daily.csv 推估市場廣度
3. 產生 market_regime：BULL / NEUTRAL / BEAR
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INDEX_FILES = [
    ROOT / "market_index_daily.csv",
    DATA_DIR / "market_index_daily.csv",
    ROOT / "index_daily.csv",
    DATA_DIR / "index_daily.csv",
    ROOT / "taiex_daily.csv",
    DATA_DIR / "taiex_daily.csv",
    ROOT / "twii_daily.csv",
    DATA_DIR / "twii_daily.csv",
]

PRICE_FILES = [
    ROOT / "price_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
    ROOT / "feature_panel_daily.csv",
    DATA_DIR / "feature_panel_daily.csv",
]


def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc)
                if not df.empty:
                    df.columns = [str(c).strip() for c in df.columns]
                    return df, p
            except Exception:
                continue
    return pd.DataFrame(), None


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def normalize_index_df(df):
    df = df.copy()
    alias = {
        "trade_date": "date",
        "Date": "date",
        "日期": "date",
        "Close": "close",
        "收盤價": "close",
        "index_close": "close",
        "taiex": "close",
        "加權指數": "close",
    }
    for old, new in alias.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    if "date" not in df.columns or "close" not in df.columns:
        raise ValueError("index data missing date/close")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    df = df[df["close"] > 0].copy()
    return df.sort_values("date")


def calc_index_regime():
    df, src = read_csv_any(INDEX_FILES)
    if df.empty:
        return None

    try:
        idx = normalize_index_df(df)
        if len(idx) < 2:
            return None

        latest = idx.iloc[-1]
        prev = idx.iloc[-2]

        close = float(latest["close"])
        prev_close = float(prev["close"])
        chg = close / prev_close - 1 if prev_close > 0 else 0

        ma20_series = idx["close"].rolling(20).mean()
        ma60_series = idx["close"].rolling(60).mean()
        ma20 = float(ma20_series.iloc[-1]) if len(idx) >= 20 else close
        ma60 = float(ma60_series.iloc[-1]) if len(idx) >= 60 else ma20

        above_ma20 = close >= ma20
        above_ma60 = close >= ma60

        ma20_prev = float(ma20_series.iloc[-6]) if len(idx) >= 26 and pd.notna(ma20_series.iloc[-6]) else ma20
        ma20_slope = ma20 / ma20_prev - 1 if ma20_prev > 0 else 0

        score = 50
        score += 20 if chg >= 0.012 else 0
        score += 10 if chg >= 0.004 else 0
        score -= 20 if chg <= -0.012 else 0
        score -= 10 if chg <= -0.004 else 0
        score += 15 if above_ma20 else -15
        score += 10 if above_ma60 else -10
        score += 10 if ma20_slope > 0 else -10

        if score >= 70:
            regime, label = "BULL", "大盤偏多"
        elif score <= 35:
            regime, label = "BEAR", "大盤偏弱"
        else:
            regime, label = "NEUTRAL", "大盤中性"

        return {
            "source": str(src),
            "method": "index_close",
            "date": pd.to_datetime(latest["date"]).strftime("%Y-%m-%d"),
            "index_close": round(close, 2),
            "index_change_pct": round(chg, 4),
            "index_change_pct_text": f"{chg * 100:.2f}%",
            "above_ma20": bool(above_ma20),
            "above_ma60": bool(above_ma60),
            "ma20_slope": round(float(ma20_slope), 4),
            "market_score": round(float(score), 2),
            "market_regime": regime,
            "market_label": label,
        }
    except Exception:
        return None


def calc_breadth_regime():
    df, src = read_csv_any(PRICE_FILES)
    if df.empty:
        raise FileNotFoundError("missing price_panel_daily.csv / feature_panel_daily.csv")

    df = df.copy()
    alias = {
        "trade_date": "date",
        "Date": "date",
        "Close": "close",
        "收盤價": "close",
        "pct_change": "return",
        "mom1": "return",
        "漲跌幅": "return",
    }
    for old, new in alias.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    if "stock_id" not in df.columns:
        for alt in ["symbol", "code", "ticker"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break

    if "date" not in df.columns or "stock_id" not in df.columns or "close" not in df.columns:
        raise ValueError("price data missing date/stock_id/close")

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["stock_id", "date"])

    if "return" not in df.columns:
        df["return"] = df.groupby("stock_id")["close"].pct_change()
    else:
        df["return"] = pd.to_numeric(df["return"], errors="coerce")

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy().dropna(subset=["return"])

    if latest.empty:
        raise ValueError("latest market breadth data empty")

    adv = float((latest["return"] > 0).mean())
    dec = float((latest["return"] < 0).mean())
    avg_ret = float(latest["return"].mean())
    med_ret = float(latest["return"].median())

    score = 50 + (adv - 0.5) * 80 + avg_ret * 500

    if adv >= 0.58 and avg_ret > 0.003:
        regime, label = "BULL", "大盤偏多"
    elif adv <= 0.42 and avg_ret < -0.003:
        regime, label = "BEAR", "大盤偏弱"
    else:
        regime, label = "NEUTRAL", "大盤中性"

    return {
        "source": str(src),
        "method": "market_breadth",
        "date": pd.to_datetime(latest_date).strftime("%Y-%m-%d"),
        "index_close": "",
        "index_change_pct": round(avg_ret, 4),
        "index_change_pct_text": f"{avg_ret * 100:.2f}%",
        "advance_ratio": round(adv, 4),
        "decline_ratio": round(dec, 4),
        "median_return": round(med_ret, 4),
        "market_score": round(float(score), 2),
        "market_regime": regime,
        "market_label": label,
    }


def main():
    result = calc_index_regime()
    if result is None:
        result = calc_breadth_regime()

    regime = result.get("market_regime", "NEUTRAL")
    if regime == "BULL":
        policy = "允許 BUY / TEST，偏向順勢分批"
    elif regime == "BEAR":
        policy = "禁止追高，BUY/TEST 降級觀察"
    else:
        policy = "控制追高，BUY 降級 TEST"

    result.update({
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "system": "market_regime_v266_11",
        "action_policy": policy,
    })

    for p in [ROOT / "market_regime.json", DATA_DIR / "market_regime.json"]:
        p.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    pd.DataFrame([result]).to_csv(ROOT / "market_regime.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([result]).to_csv(DATA_DIR / "market_regime.csv", index=False, encoding="utf-8-sig")

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
