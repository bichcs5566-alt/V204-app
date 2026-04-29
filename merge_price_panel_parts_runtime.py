"""
merge_price_panel_parts_runtime.py

讀取：
data/price_panel_parts/manifest.json
data/price_panel_parts/price_panel_daily_part_001.csv ... part_006.csv

輸出 runtime：
price_panel_daily.csv
mobile_dashboard_v1/data/price_panel_daily.csv
data_meta.json

注意：完整 price_panel_daily.csv 只在 runner 暫存，不要 commit 回 repo。
"""
from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
PART_DIR = ROOT / "data" / "price_panel_parts"
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST = PART_DIR / "manifest.json"

def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s

def resolve_part_path(file_value):
    p = Path(file_value)
    for candidate in [p, PART_DIR / p.name, ROOT / file_value]:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"part file not found: {file_value}")

def normalize_df(df):
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns:
        if "trade_date" in df.columns:
            df["date"] = df["trade_date"]
        elif "datetime" in df.columns:
            df["date"] = df["datetime"]
        else:
            raise ValueError("missing date/trade_date")

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]
        elif "ticker" in df.columns:
            df["stock_id"] = df["ticker"].astype(str).str.extract(r"(\d{4})")[0]
        else:
            raise ValueError("missing stock_id/symbol/code/ticker")

    if "name" not in df.columns:
        df["name"] = ""
    if "market" not in df.columns:
        df["market"] = ""

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            if c in ["open", "high", "low"] and "close" in df.columns:
                df[c] = df["close"]
            elif c == "volume":
                df[c] = 0
            else:
                raise ValueError(f"missing required column: {c}")

    out = df[["date","stock_id","name","market","open","high","low","close","volume"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

    for c in ["open","high","low","close","volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date","stock_id","close"])
    out = out[out["close"] > 0].copy()

    for c in ["open","high","low"]:
        out[c] = out[c].fillna(out["close"])
    out["volume"] = out["volume"].fillna(0)

    out = out.drop_duplicates(["date","stock_id"], keep="last")
    out = out.sort_values(["stock_id","date"]).reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date","stock_id","name","market","open","high","low","close","volume"]]

def main():
    if not MANIFEST.exists():
        raise FileNotFoundError("missing data/price_panel_parts/manifest.json")

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    parts = manifest.get("parts", [])
    if not parts:
        raise ValueError("manifest.json has no parts")

    dfs = []
    loaded = []
    for i, item in enumerate(parts, 1):
        p = resolve_part_path(item["file"])
        df = pd.read_csv(p)
        dfs.append(df)
        loaded.append({"file": str(p), "rows": int(len(df)), "bytes": int(p.stat().st_size)})
        print(f"loaded part {i}: {p} rows={len(df)}")

    raw = pd.concat(dfs, ignore_index=True)
    panel = normalize_df(raw)

    if len(panel) < 10000:
        raise RuntimeError(f"merged panel too small: {len(panel)}")
    if panel["date"].nunique() < 200:
        raise RuntimeError(f"unique dates too small: {panel['date'].nunique()}")

    panel.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "merge_price_panel_parts_runtime",
        "manifest_file": str(MANIFEST),
        "manifest_created_at": manifest.get("created_at"),
        "manifest_start": manifest.get("start"),
        "manifest_end": manifest.get("end"),
        "manifest_part_count": manifest.get("part_count"),
        "manifest_total_rows": manifest.get("total_rows"),
        "manifest_total_symbols": manifest.get("total_symbols"),
        "loaded_part_count": len(loaded),
        "loaded_parts": loaded,
        "rows_before_dedup": int(len(raw)),
        "rows_after_dedup": int(len(panel)),
        "stock_count": int(panel["stock_id"].nunique()),
        "unique_dates": int(panel["date"].nunique()),
        "start_date": str(panel["date"].min()),
        "end_date": str(panel["date"].max()),
    }
    for out in [ROOT/"data_meta.json", DATA_DIR/"data_meta.json"]:
        out.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
