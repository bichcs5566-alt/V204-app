"""
merge_chunks.py

合併 data_chunks/price_panel_YYYY.csv → price_panel_daily.csv
然後同步 mobile_dashboard_v1/data/price_panel_daily.csv
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

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


def main():
    files = sorted(CHUNK_DIR.glob("price_panel_*.csv"))
    files = [p for p in files if "_meta" not in p.name]

    if not files:
        raise FileNotFoundError("no data_chunks/price_panel_YYYY.csv found")

    parts = []
    loaded = []
    for p in files:
        df = pd.read_csv(p)
        if df.empty:
            continue
        df["source_chunk"] = p.name
        parts.append(df)
        loaded.append(str(p))
        print("loaded", p, "rows", len(df))

    if not parts:
        raise RuntimeError("all chunks empty")

    out = pd.concat(parts, ignore_index=True)
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

    out = out[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]]

    if len(out) < MIN_TOTAL_ROWS:
        raise RuntimeError(f"merged panel too small: {len(out)}")

    unique_dates = out["date"].nunique()
    if unique_dates < MIN_UNIQUE_DATES:
        raise RuntimeError(f"unique_dates too small: {unique_dates}")

    out.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "merge_chunks",
        "chunk_count": len(loaded),
        "loaded_chunks": loaded,
        "rows": int(len(out)),
        "stock_count": int(out["stock_id"].nunique()),
        "unique_dates": int(unique_dates),
        "start_date": str(out["date"].min()),
        "end_date": str(out["date"].max()),
        "market_counts": out["market"].value_counts().to_dict(),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("merge chunks completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
