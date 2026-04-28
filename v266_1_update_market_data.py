"""
v266_1_update_market_data.py
每日增量更新版

來源：你提供的 update_market_data.py
修正：
1. 不再只保留最近 30 天
2. 保留 10 年歷史母檔
3. 新增 open/high/low 欄位兼容
4. 同步寫入 mobile_dashboard_v1/data
"""

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import json

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT = Path("price_panel_daily.csv")
TODAY_RAW = datetime.now().strftime("%Y%m%d")
TODAY = datetime.now().strftime("%Y-%m-%d")

TWSE_URL = (
    "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    f"?response=json&date={TODAY_RAW}&type=ALL"
)

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


def normalize_columns(cols):
    return [str(c).strip() for c in cols]


def candidate_tables(payload):
    out = []
    tables = payload.get("tables", [])
    for t in tables:
        fields = normalize_columns(t.get("fields", []))
        data = t.get("data", [])
        if fields and data:
            out.append((fields, data, "tables"))

    for key in ["data9", "data8", "data7", "data6", "data5", "data4", "data3", "data2", "data1"]:
        data = payload.get(key)
        fields = payload.get(f"fields{key[-1]}")
        if isinstance(data, list) and data and isinstance(fields, list) and fields:
            out.append((normalize_columns(fields), data, key))

    return out


def match_table(fields):
    joined = " | ".join(fields)
    has_stock = any(k in joined for k in ["證券代號", "股票代號", "代號"])
    has_close = any(k in joined for k in ["收盤價", "收盤"])
    has_volume = any(k in joined for k in ["成交股數", "成交量"])
    return has_stock and has_close and has_volume


def field_index(fields, keywords):
    for i, f in enumerate(fields):
        for kw in keywords:
            if kw in f:
                return i
    return None


def parse_twse_rows(payload):
    for fields, rows, source_key in candidate_tables(payload):
        if not match_table(fields):
            continue

        idx_stock = field_index(fields, ["證券代號", "股票代號", "代號"])
        idx_name = field_index(fields, ["證券名稱", "股票名稱", "名稱"])
        idx_close = field_index(fields, ["收盤價", "收盤"])
        idx_volume = field_index(fields, ["成交股數", "成交量"])
        idx_open = field_index(fields, ["開盤價", "開盤"])
        idx_high = field_index(fields, ["最高價", "最高"])
        idx_low = field_index(fields, ["最低價", "最低"])

        if idx_stock is None or idx_close is None or idx_volume is None:
            continue

        records = []
        for r in rows:
            try:
                stock_id = normalize_stock_id(r[idx_stock])
                close = to_number(r[idx_close])
                volume = to_int(r[idx_volume])

                if not is_common_stock_id(stock_id):
                    continue
                if close is None or close <= 0:
                    continue

                open_p = to_number(r[idx_open]) if idx_open is not None and idx_open < len(r) else close
                high_p = to_number(r[idx_high]) if idx_high is not None and idx_high < len(r) else close
                low_p = to_number(r[idx_low]) if idx_low is not None and idx_low < len(r) else close
                name = clean_text(r[idx_name]) if idx_name is not None and idx_name < len(r) else ""

                records.append({
                    "date": TODAY,
                    "stock_id": stock_id,
                    "name": name,
                    "market": "TWSE",
                    "open": open_p if open_p is not None else close,
                    "high": high_p if high_p is not None else close,
                    "low": low_p if low_p is not None else close,
                    "close": close,
                    "volume": volume if volume is not None else 0,
                })
            except Exception:
                continue

        if records:
            print(f"parsed source: {source_key}")
            print(f"matched fields: {fields}")
            return pd.DataFrame(records)

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def load_existing():
    if not OUTPUT.exists():
        return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])

    try:
        old = pd.read_csv(OUTPUT, encoding="utf-8")
    except Exception:
        old = pd.read_csv(OUTPUT, encoding="utf-8-sig")

    old.columns = [str(c).strip().lower() for c in old.columns]

    if "stock_id" not in old.columns:
        for alt in ["stock", "symbol", "code"]:
            if alt in old.columns:
                old["stock_id"] = old[alt]
                break

    for col in ["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]:
        if col not in old.columns:
            old[col] = None

    old = old[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]].copy()
    old["date"] = pd.to_datetime(old["date"], errors="coerce")
    old["stock_id"] = old["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        old[c] = pd.to_numeric(old[c], errors="coerce")

    old = old.dropna(subset=["date", "stock_id", "close"])
    return old


def main():
    print("fetch:", TWSE_URL)
    resp = requests.get(TWSE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    stat = str(payload.get("stat", ""))
    if stat and ("很抱歉" in stat or "查詢失敗" in stat or "沒有符合條件" in stat):
        print("twse stat:", stat)

    new_df = parse_twse_rows(payload)
    old_df = load_existing()

    if new_df.empty:
        print("warning: today fetch returned no tradable rows")
        print("keep existing price_panel_daily.csv unchanged")
        if old_df.empty:
            raise RuntimeError("today fetch empty and no existing price_panel_daily.csv available")
        return

    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged["stock_id"] = merged["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce")

    merged = merged.dropna(subset=["date", "stock_id", "close"])
    merged = merged[merged["stock_id"].apply(is_common_stock_id)].copy()

    for c in ["open", "high", "low"]:
        merged[c] = merged[c].fillna(merged["close"])

    merged["volume"] = merged["volume"].fillna(0)
    merged = merged.sort_values(["stock_id", "date"])
    merged = merged.drop_duplicates(["date", "stock_id"], keep="last")

    # v266.1 關鍵：不再只保留 30 天
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged = merged[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]]

    merged.to_csv(OUTPUT, index=False, encoding="utf-8")
    merged.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v266_1_update_market_data",
        "latest_date": TODAY,
        "rows": int(len(merged)),
        "stock_count": int(merged["stock_id"].nunique()),
        "start_date": str(merged["date"].min()),
        "end_date": str(merged["date"].max()),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v266.1 update_market_data done")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
