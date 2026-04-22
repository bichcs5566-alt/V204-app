import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# ==========================================
# update_market_data.py
# v2.8 真實行情抓取修正版（可直接覆蓋）
# 修正重點：
# 1. 不再硬吃 data9
# 2. 改成自動掃描 tables / data9 / data8 / data6
# 3. 找到含「證券代號 / 收盤價 / 成交股數」的表才解析
# 4. 若當天抓不到，保留舊資料，不直接毀掉 price_panel_daily.csv
# ==========================================

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
    return str(x).strip().replace(",", "").replace("--", "").replace("—", "")


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


def normalize_columns(cols):
    return [str(c).strip() for c in cols]


def candidate_tables(payload):
    out = []

    # 新版常見：tables
    tables = payload.get("tables", [])
    for t in tables:
        fields = normalize_columns(t.get("fields", []))
        data = t.get("data", [])
        if fields and data:
            out.append((fields, data, "tables"))

    # 舊版/其他變體：data9 / data8 / data6...
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
        idx_close = field_index(fields, ["收盤價", "收盤"])
        idx_volume = field_index(fields, ["成交股數", "成交量"])

        if idx_stock is None or idx_close is None or idx_volume is None:
            continue

        records = []
        for r in rows:
            try:
                stock_id = clean_text(r[idx_stock])
                close = to_number(r[idx_close])
                volume = to_int(r[idx_volume])

                if not stock_id:
                    continue
                if close is None or close <= 0:
                    continue
                if volume is None:
                    volume = 0

                records.append({
                    "date": TODAY,
                    "stock_id": stock_id,
                    "close": close,
                    "volume": volume,
                })
            except Exception:
                continue

        if records:
            print(f"parsed source: {source_key}")
            print(f"matched fields: {fields}")
            return pd.DataFrame(records)

    return pd.DataFrame(columns=["date", "stock_id", "close", "volume"])


def load_existing():
    if not OUTPUT.exists():
        return pd.DataFrame(columns=["date", "stock_id", "close", "volume"])

    try:
        old = pd.read_csv(OUTPUT, encoding="utf-8-sig")
    except Exception:
        old = pd.read_csv(OUTPUT)

    old.columns = [str(c).strip().lower() for c in old.columns]

    if "stock_id" not in old.columns:
        for alt in ["stock", "symbol", "code"]:
            if alt in old.columns:
                old["stock_id"] = old[alt]
                break

    for col in ["date", "stock_id", "close", "volume"]:
        if col not in old.columns:
            old[col] = None

    old = old[["date", "stock_id", "close", "volume"]].copy()
    old["date"] = pd.to_datetime(old["date"], errors="coerce")
    old["close"] = pd.to_numeric(old["close"], errors="coerce")
    old["volume"] = pd.to_numeric(old["volume"], errors="coerce")
    old = old.dropna(subset=["date", "stock_id", "close"])
    return old


def main():
    print("fetch:", TWSE_URL)
    resp = requests.get(TWSE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    payload = resp.json()

    # 若官方回應非成功，也印出來方便 debug
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
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["volume"] = pd.to_numeric(merged["volume"], errors="coerce").fillna(0)

    merged = merged.dropna(subset=["date", "stock_id", "close"])
    merged = merged.sort_values(["date", "stock_id"])
    merged = merged.drop_duplicates(["date", "stock_id"], keep="last")

    # 只保留最近 30 天，避免檔案太大
    max_date = merged["date"].max()
    merged = merged[merged["date"] >= max_date - pd.Timedelta(days=30)].copy()

    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged.to_csv(OUTPUT, index=False, encoding="utf-8-sig")

    print("update_market_data done")
    print("latest date:", max_date.strftime("%Y-%m-%d"))
    print("rows:", len(merged))


if __name__ == "__main__":
    main()
