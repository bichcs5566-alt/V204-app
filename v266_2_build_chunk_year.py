"""
v266_2_build_chunk_year.py
v266.2 分段資料層：單一年份 chunk 建置

用途：
- 每次只抓一個年份
- 輸出 data_chunks/price_panel_YYYY.csv
- 避免 GitHub Actions 一次抓 10 年卡死
- chunk 檔可長期保存，再由 merge 合併成完整 price_panel_daily.csv

執行：
python v266_2_build_chunk_year.py 2015
"""

from pathlib import Path
from datetime import datetime, timedelta
import sys
import time
import json
import requests
import pandas as pd
import numpy as np

ROOT = Path(".")
CHUNK_DIR = ROOT / "data_chunks"
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_SECONDS = 0.25
MIN_YEAR_ROWS = 1000

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

    for t in payload.get("tables", []):
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


def parse_rows(payload, date_str, market):
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
                    "date": date_str,
                    "stock_id": stock_id,
                    "name": name,
                    "market": market,
                    "open": open_p if open_p is not None else close,
                    "high": high_p if high_p is not None else close,
                    "low": low_p if low_p is not None else close,
                    "close": close,
                    "volume": volume if volume is not None else 0,
                })
            except Exception:
                continue

        if records:
            print(f"{date_str} {market} parsed {source_key} rows={len(records)}")
            return pd.DataFrame(records)

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def fetch_twse_day(dt):
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "date": dt.strftime("%Y%m%d"), "type": "ALL"}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return parse_rows(payload, dt.strftime("%Y-%m-%d"), "TWSE")


def fetch_tpex_day(dt):
    date_str = dt.strftime("%Y-%m-%d")
    roc_year = dt.year - 1911
    roc_date = f"{roc_year}/{dt.month:02d}/{dt.day:02d}"

    endpoints = [
        (
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "d": roc_date, "s": "0,asc,0"},
        ),
        (
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc",
            {"date": dt.strftime("%Y/%m/%d"), "type": "EW", "response": "json"},
        ),
    ]

    for url, params in endpoints:
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            parsed = parse_rows(payload, date_str, "TPEX")
            if not parsed.empty:
                return parsed

            data = payload.get("aaData") or payload.get("data") or []
            if data:
                records = []
                for r in data:
                    if len(r) < 8:
                        continue
                    stock_id = normalize_stock_id(r[0])
                    close = to_number(r[2])
                    if not is_common_stock_id(stock_id) or close is None or close <= 0:
                        continue
                    records.append({
                        "date": date_str,
                        "stock_id": stock_id,
                        "name": clean_text(r[1]) if len(r) > 1 else "",
                        "market": "TPEX",
                        "open": to_number(r[4]) or close,
                        "high": to_number(r[5]) or close,
                        "low": to_number(r[6]) or close,
                        "close": close,
                        "volume": to_int(r[7]) or 0,
                    })
                if records:
                    print(f"{date_str} TPEX parsed fallback rows={len(records)}")
                    return pd.DataFrame(records)
        except Exception as e:
            print("TPEX failed:", date_str, str(e)[:120])

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def finalize(df):
    if df.empty:
        return df

    out = df.copy()
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

    return out[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]]


def build_year(year):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)

    # 當年只抓到今天
    today = datetime.now()
    if year == today.year:
        end = min(end, today)

    parts = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            print("fetch day:", cur.strftime("%Y-%m-%d"))

            try:
                twse = fetch_twse_day(cur)
                if not twse.empty:
                    parts.append(twse)
            except Exception as e:
                print("TWSE failed:", cur.strftime("%Y-%m-%d"), str(e)[:150])

            try:
                tpex = fetch_tpex_day(cur)
                if not tpex.empty:
                    parts.append(tpex)
            except Exception as e:
                print("TPEX hard failed:", cur.strftime("%Y-%m-%d"), str(e)[:150])

            time.sleep(SLEEP_SECONDS)

        cur += timedelta(days=1)

    if not parts:
        raise RuntimeError(f"no data fetched for year {year}")

    return finalize(pd.concat(parts, ignore_index=True))


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: python v266_2_build_chunk_year.py 2015")

    year = int(sys.argv[1])
    if year < 2010 or year > datetime.now().year:
        raise ValueError(f"invalid year: {year}")

    print(f"v266.2 build chunk year start: {year}")

    panel = build_year(year)

    if len(panel) < MIN_YEAR_ROWS:
        raise RuntimeError(f"year chunk too small: {year}, rows={len(panel)}")

    out = CHUNK_DIR / f"price_panel_{year}.csv"
    panel.to_csv(out, index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v266_2_build_chunk_year",
        "year": year,
        "file": str(out),
        "rows": int(len(panel)),
        "stock_count": int(panel["stock_id"].nunique()),
        "start_date": str(panel["date"].min()),
        "end_date": str(panel["date"].max()),
        "market_counts": panel["market"].value_counts().to_dict(),
        "file_size_mb": round(out.stat().st_size / 1024 / 1024, 2),
    }

    meta_path = CHUNK_DIR / f"price_panel_{year}_meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v266.2 build chunk completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
