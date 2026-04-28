"""
daily_update.py

每日更新：
- 只 append 今日資料到 price_panel_daily.csv
- 不砍 10 年歷史資料
"""

from pathlib import Path
from datetime import datetime
import json
import requests
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TODAY_DT = datetime.now()
TODAY = TODAY_DT.strftime("%Y-%m-%d")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}


def clean_text(x):
    return str(x).strip().replace(",", "").replace("--", "").replace("—", "").replace("X", "")


def to_number(x):
    s = clean_text(x)
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def to_int(x):
    s = clean_text(x)
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def normalize_stock_id(x):
    s = clean_text(x)
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_common_stock_id(x):
    s = normalize_stock_id(x)
    return s.isdigit() and len(s) == 4 and not s.startswith(("00", "03", "04", "05", "06", "07", "08", "09"))


def candidate_tables(payload):
    out = []
    for t in payload.get("tables", []):
        fields = [str(c).strip() for c in t.get("fields", [])]
        data = t.get("data", [])
        if fields and data:
            out.append((fields, data))

    for key in ["data9", "data8", "data7", "data6", "data5", "data4", "data3", "data2", "data1"]:
        data = payload.get(key)
        fields = payload.get(f"fields{key[-1]}")
        if isinstance(data, list) and data and isinstance(fields, list) and fields:
            out.append(([str(c).strip() for c in fields], data))
    return out


def field_index(fields, keywords):
    for i, f in enumerate(fields):
        for kw in keywords:
            if kw in f:
                return i
    return None


def parse_twse(payload):
    for fields, rows in candidate_tables(payload):
        joined = " | ".join(fields)
        if not ("收盤" in joined and ("證券代號" in joined or "股票代號" in joined or "代號" in joined)):
            continue

        idx_stock = field_index(fields, ["證券代號", "股票代號", "代號"])
        idx_name = field_index(fields, ["證券名稱", "股票名稱", "名稱"])
        idx_close = field_index(fields, ["收盤價", "收盤"])
        idx_volume = field_index(fields, ["成交股數", "成交量"])
        idx_open = field_index(fields, ["開盤價", "開盤"])
        idx_high = field_index(fields, ["最高價", "最高"])
        idx_low = field_index(fields, ["最低價", "最低"])

        if idx_stock is None or idx_close is None:
            continue

        records = []
        for r in rows:
            stock_id = normalize_stock_id(r[idx_stock])
            close = to_number(r[idx_close])
            if not is_common_stock_id(stock_id) or close is None or close <= 0:
                continue
            records.append({
                "date": TODAY,
                "stock_id": stock_id,
                "name": clean_text(r[idx_name]) if idx_name is not None and idx_name < len(r) else "",
                "market": "TWSE",
                "open": to_number(r[idx_open]) if idx_open is not None and idx_open < len(r) else close,
                "high": to_number(r[idx_high]) if idx_high is not None and idx_high < len(r) else close,
                "low": to_number(r[idx_low]) if idx_low is not None and idx_low < len(r) else close,
                "close": close,
                "volume": to_int(r[idx_volume]) if idx_volume is not None and idx_volume < len(r) else 0,
            })
        if records:
            return pd.DataFrame(records)

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def fetch_today():
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "date": TODAY_DT.strftime("%Y%m%d"), "type": "ALL"}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return parse_twse(resp.json())


def load_existing():
    p = ROOT / "price_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR / "price_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        raise FileNotFoundError("price_panel_daily.csv missing. Run build_chunk_year + merge_chunks first.")
    return pd.read_csv(p)


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
    old = load_existing()

    if TODAY_DT.weekday() >= 5:
        print("weekend, keep existing data")
        panel = finalize(old)
    else:
        today = fetch_today()
        if today.empty:
            print("today data empty, keep existing data")
            panel = finalize(old)
        else:
            panel = finalize(pd.concat([old, today], ignore_index=True))

    panel.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "daily_update",
        "rows": int(len(panel)),
        "stock_count": int(panel["stock_id"].nunique()),
        "unique_dates": int(panel["date"].nunique()),
        "start_date": str(panel["date"].min()),
        "end_date": str(panel["date"].max()),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("daily update completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
