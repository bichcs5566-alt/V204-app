"""
import_existing_price_panel.py

用途：
把舊 10 年母檔轉成新資料層標準格式。

支援舊欄位：
- trade_date -> date
- symbol -> stock_id
- ticker 可保留但不必使用

輸出：
- price_panel_daily.csv
- mobile_dashboard_v1/data/price_panel_daily.csv
- data_meta.json

使用時機：
1. 你已有舊版 10 年 price_panel_daily.csv
2. 但欄位是 trade_date / symbol
3. 先跑這支轉換成新標準，再跑 backfill_missing_days.py
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_CANDIDATES = [
    ROOT / "price_panel_daily.csv",
    ROOT / "price_panel_daily_old.csv",
    DATA_DIR / "price_panel_daily.csv",
]


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_common_stock_id(x):
    s = normalize_stock_id(x)
    return s.isdigit() and len(s) == 4 and not s.startswith(("00", "03", "04", "05", "06", "07", "08", "09"))


def find_source():
    for p in SOURCE_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError("找不到可用的 price_panel_daily.csv / price_panel_daily_old.csv")


def main():
    src = find_source()
    print("source:", src)

    df = pd.read_csv(src)
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

    if "market" not in df.columns:
        df["market"] = ""

    if "name" not in df.columns:
        df["name"] = ""

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            if c in ["open", "high", "low"] and "close" in df.columns:
                df[c] = df["close"]
            elif c == "volume":
                df[c] = 0
            else:
                raise ValueError(f"缺少必要欄位：{c}")

    out = df[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date", "stock_id", "close"])
    out = out[out["stock_id"].apply(is_common_stock_id)].copy()
    out = out[out["close"] > 0].copy()

    for c in ["open", "high", "low"]:
        out[c] = out[c].fillna(out["close"])

    out["volume"] = out["volume"].fillna(0)
    out = out.drop_duplicates(["date", "stock_id"], keep="last")
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")

    out.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "import_existing_price_panel",
        "input_file": str(src),
        "rows": int(len(out)),
        "stock_count": int(out["stock_id"].nunique()),
        "unique_dates": int(out["date"].nunique()),
        "start_date": str(out["date"].min()),
        "end_date": str(out["date"].max()),
        "market_counts": out["market"].value_counts(dropna=False).to_dict(),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("import completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
