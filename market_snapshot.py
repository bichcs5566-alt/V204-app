"""
market_snapshot.py
v266.10：全市場主表（含股票名稱）

輸出：
- market_snapshot.csv
- mobile_dashboard_v1/data/market_snapshot.csv

欄位：
stock_id, stock_name, close, volume, turnover, date, liquidity_level, liquidity_tag, liquidity_score
"""

from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRICE_FILES = [ROOT / "price_panel_daily.csv", DATA_DIR / "price_panel_daily.csv"]
FEATURE_FILES = [ROOT / "feature_panel_daily.csv", DATA_DIR / "feature_panel_daily.csv"]
NAME_FILES = [
    ROOT / "stock_basic.csv", DATA_DIR / "stock_basic.csv",
    ROOT / "stock_list.csv", DATA_DIR / "stock_list.csv",
    ROOT / "company_list.csv", DATA_DIR / "company_list.csv",
    ROOT / "tw_stock_list.csv", DATA_DIR / "tw_stock_list.csv",
]

OUTPUT_COLUMNS = [
    "stock_id", "stock_name", "close", "volume", "turnover", "date",
    "liquidity_level", "liquidity_tag", "liquidity_score"
]


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc, dtype={"stock_id": str})
                if not df.empty:
                    df.columns = [str(c).strip() for c in df.columns]
                    if "stock_id" in df.columns:
                        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
                    return df, p
            except Exception:
                continue
    return pd.DataFrame(), None


def apply_alias(df):
    df = df.copy()
    alias = {
        "symbol": "stock_id", "code": "stock_id", "ticker": "stock_id",
        "trade_date": "date",
        "name": "stock_name", "證券名稱": "stock_name", "股票名稱": "stock_name", "公司名稱": "stock_name",
        "收盤價": "close", "Close": "close", "成交張數": "volume", "成交股數": "volume", "Volume": "volume",
        "ref_price": "close", "price": "close",
    }
    for old, new in alias.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]
    return df


def standardize_market_df(df):
    df = apply_alias(df)

    if "stock_id" not in df.columns:
        raise ValueError("missing stock_id")
    if "date" not in df.columns:
        df["date"] = datetime.now().strftime("%Y-%m-%d")
    if "stock_name" not in df.columns:
        df["stock_name"] = ""
    if "close" not in df.columns:
        df["close"] = 0
    if "volume" not in df.columns:
        df["volume"] = 0

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

    # 若 volume 像股數，轉成張數
    if len(df) and df["volume"].median() > 200000:
        df["volume"] = df["volume"] / 1000

    if "turnover" not in df.columns:
        df["turnover"] = df["close"] * df["volume"] * 1000
    else:
        raw = pd.to_numeric(df["turnover"], errors="coerce")
        fallback = df["close"] * df["volume"] * 1000
        df["turnover"] = raw.fillna(fallback)

    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)]
    df = df[df["close"] > 0].copy()
    return df


def load_name_map():
    out = {}
    for p in NAME_FILES:
        df, _ = read_csv_any([p])
        if df.empty:
            continue
        df = apply_alias(df)
        if "stock_id" not in df.columns or "stock_name" not in df.columns:
            continue
        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
        for _, r in df.iterrows():
            sid = str(r.get("stock_id", "")).strip()
            name = str(r.get("stock_name", "")).strip()
            if sid and name and name.lower() not in ["nan", "none", "null"]:
                out[sid] = name
    return out


def calc_liquidity(df):
    df = df.copy()
    vol_rank = df["volume"].rank(pct=True).fillna(0)
    turnover_rank = df["turnover"].rank(pct=True).fillna(0)
    df["liquidity_score"] = (vol_rank * 50 + turnover_rank * 50).round(2)

    high = (df["volume"] >= 3000) | (df["turnover"] >= 80_000_000) | (df["liquidity_score"] >= 75)
    medium = (df["volume"] >= 1000) | (df["turnover"] >= 30_000_000) | (df["liquidity_score"] >= 45)
    low = df["volume"] >= 500

    df["liquidity_level"] = np.select([high, medium, low], ["HIGH", "MEDIUM", "LOW"], default="BLOCK")
    df["liquidity_tag"] = df["liquidity_level"].map({
        "HIGH": "高流動性", "MEDIUM": "中流動性", "LOW": "低流動性", "BLOCK": "流動性不足"
    })
    return df


def main():
    price, price_src = read_csv_any(PRICE_FILES)
    feature, feature_src = read_csv_any(FEATURE_FILES)

    if not price.empty:
        base = standardize_market_df(price)
        source = str(price_src)
    elif not feature.empty:
        base = standardize_market_df(feature)
        source = str(feature_src)
    else:
        raise FileNotFoundError("missing price_panel_daily.csv and feature_panel_daily.csv")

    base = base.sort_values(["stock_id", "date"])
    latest = base.groupby("stock_id", as_index=False).tail(1).copy()

    # 從 feature 補名稱
    if not feature.empty:
        f = standardize_market_df(feature)
        fmap = {}
        if "stock_name" in f.columns:
            f = f.sort_values(["stock_id", "date"]).groupby("stock_id", as_index=False).tail(1)
            fmap = {
                str(r["stock_id"]): str(r.get("stock_name", "")).strip()
                for _, r in f.iterrows()
                if str(r.get("stock_name", "")).strip().lower() not in ["", "nan", "none", "null"]
            }
        latest["stock_name"] = latest.apply(
            lambda r: fmap.get(str(r["stock_id"]), str(r.get("stock_name", "")).strip()),
            axis=1
        )

    name_map = load_name_map()
    latest["stock_name"] = latest.apply(
        lambda r: name_map.get(str(r["stock_id"]), str(r.get("stock_name", "")).strip()),
        axis=1
    )
    latest["stock_name"] = latest["stock_name"].replace(["nan", "None", "null"], "")

    latest = calc_liquidity(latest)
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")

    out = latest[OUTPUT_COLUMNS].copy().sort_values("stock_id")
    out.to_csv(ROOT / "market_snapshot.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "market_snapshot.csv", index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "market_snapshot_v266_10",
        "market_source": source,
        "rows": int(len(out)),
        "with_name_count": int((out["stock_name"].astype(str).str.strip() != "").sum()),
        "high_liquidity_count": int((out["liquidity_level"] == "HIGH").sum()),
        "medium_liquidity_count": int((out["liquidity_level"] == "MEDIUM").sum()),
        "low_liquidity_count": int((out["liquidity_level"] == "LOW").sum()),
        "block_liquidity_count": int((out["liquidity_level"] == "BLOCK").sum()),
        "encoding": "utf-8-sig",
    }

    for p in [ROOT / "market_snapshot_summary.json", DATA_DIR / "market_snapshot_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
