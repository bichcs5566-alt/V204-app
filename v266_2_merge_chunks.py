"""
v266_2_merge_chunks.py
v266.2 分段資料層：合併 data_chunks/price_panel_YYYY.csv → price_panel_daily.csv

用途：
- 將分年 chunk 合併為完整母檔
- 不重新抓資料
- 去重、排序、檢查大小
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np

ROOT = Path(".")
CHUNK_DIR = ROOT / "data_chunks"
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MIN_TOTAL_ROWS = 10000
MIN_UNIQUE_DATES = 200


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def load_chunks():
    files = sorted(CHUNK_DIR.glob("price_panel_*.csv"))
    files = [p for p in files if not p.name.endswith("_meta.csv")]

    if not files:
        raise FileNotFoundError("no data_chunks/price_panel_YYYY.csv found")

    parts = []
    loaded = []
    for p in files:
        try:
            df = pd.read_csv(p)
            if df.empty:
                continue
            df["source_chunk"] = p.name
            parts.append(df)
            loaded.append(str(p))
            print("loaded:", p, "rows=", len(df))
        except Exception as e:
            print("skip broken chunk:", p, e)

    if not parts:
        raise RuntimeError("all chunks empty or broken")

    return pd.concat(parts, ignore_index=True), loaded


def finalize(df):
    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]

    for col in ["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]:
        if col not in out.columns:
            out[col] = ""

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date", "stock_id", "close"])
    out = out[out["close"] > 0].copy()

    for c in ["open", "high", "low"]:
        out[c] = out[c].fillna(out["close"])

    out["volume"] = out["volume"].fillna(0)
    out = out.drop_duplicates(["date", "stock_id"], keep="last")
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")

    return out[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]]


def main():
    raw, loaded = load_chunks()
    panel = finalize(raw)

    if len(panel) < MIN_TOTAL_ROWS:
        raise RuntimeError(f"merged panel too small: {len(panel)} rows")

    unique_dates = panel["date"].nunique()
    if unique_dates < MIN_UNIQUE_DATES:
        raise RuntimeError(f"unique_dates too small: {unique_dates}")

    panel.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v266_2_merge_chunks",
        "loaded_chunks": loaded,
        "chunk_count": len(loaded),
        "rows": int(len(panel)),
        "stock_count": int(panel["stock_id"].nunique()),
        "unique_dates": int(unique_dates),
        "start_date": str(panel["date"].min()),
        "end_date": str(panel["date"].max()),
        "market_counts": panel["market"].value_counts().to_dict(),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v266.2 merge chunks completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
