"""
v266_1_build_price_panel_10y.py
10 年歷史資料重建版

用途：
- 重新建立 10 年 price_panel_daily.csv
- 沿用你原本 update_market_data.py 的 TWSE parser 思路
- 不會只保留 30 天
- 產出 root 與 mobile_dashboard_v1/data 兩份

注意：
- 這支是歷史建檔，不是每日更新
- 第一次跑會比較久
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT = ROOT / "price_panel_daily.csv"
RAW_OUTPUT = ROOT / "raw_market_daily.csv"

LOOKBACK_YEARS = 10
SLEEP_SECONDS = 0.25
MIN_TOTAL_ROWS = 10000

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


def parse_rows(payload, date_str, market="TWSE"):
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
            print(f"{date_str} {market} parsed source: {source_key}, rows={len(records)}")
            return pd.DataFrame(records)

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def fetch_twse_day(dt):
    ymd = dt.strftime("%Y%m%d")
    date_str = dt.strftime("%Y-%m-%d")
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "date": ymd, "type": "ALL"}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    stat = str(payload.get("stat", ""))
    if stat and ("很抱歉" in stat or "查詢失敗" in stat or "沒有符合條件" in stat):
        print("twse stat:", date_str, stat)
    return parse_rows(payload, date_str, "TWSE")


def fetch_tpex_day(dt):
    """
    TPEX API 經常變動，因此這裡做成可失敗，不讓上櫃資料卡死整條歷史建置。
    TWSE 成功就能先建立主資料層。
    """
    date_str = dt.strftime("%Y-%m-%d")
    roc_year = dt.year - 1911
    roc_date = f"{roc_year}/{dt.month:02d}/{dt.day:02d}"

    urls = [
        (
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",
            {"l": "zh-tw", "d": roc_date, "s": "0,asc,0"}
        ),
        (
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc",
            {"date": dt.strftime("%Y/%m/%d"), "type": "EW", "response": "json"}
        ),
    ]

    for url, params in urls:
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            # 新式 tables
            parsed = parse_rows(payload, date_str, "TPEX")
            if not parsed.empty:
                return parsed

            # 舊式 aaData
            data = payload.get("aaData") or payload.get("data") or []
            if data:
                df = pd.DataFrame(data)
                records = []
                for _, r in df.iterrows():
                    vals = list(r.values)
                    if len(vals) < 8:
                        continue
                    stock_id = normalize_stock_id(vals[0])
                    if not is_common_stock_id(stock_id):
                        continue
                    close = to_number(vals[2])
                    if close is None or close <= 0:
                        continue
                    records.append({
                        "date": date_str,
                        "stock_id": stock_id,
                        "name": clean_text(vals[1]) if len(vals) > 1 else "",
                        "market": "TPEX",
                        "open": to_number(vals[4]) or close,
                        "high": to_number(vals[5]) or close,
                        "low": to_number(vals[6]) or close,
                        "close": close,
                        "volume": to_int(vals[7]) or 0,
                    })
                if records:
                    print(f"{date_str} TPEX parsed fallback rows={len(records)}")
                    return pd.DataFrame(records)
        except Exception as e:
            print("TPEX failed:", date_str, str(e)[:120])

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def build_history():
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_YEARS * 365 + 30)

    all_parts = []
    cur = start
    total_days = 0

    while cur <= end:
        if cur.weekday() < 5:
            total_days += 1
            print("fetch day:", cur.strftime("%Y-%m-%d"))

            try:
                twse = fetch_twse_day(cur)
                if not twse.empty:
                    all_parts.append(twse)
            except Exception as e:
                print("TWSE failed:", cur.strftime("%Y-%m-%d"), str(e)[:150])

            try:
                tpex = fetch_tpex_day(cur)
                if not tpex.empty:
                    all_parts.append(tpex)
            except Exception as e:
                print("TPEX hard failed:", cur.strftime("%Y-%m-%d"), str(e)[:150])

            time.sleep(SLEEP_SECONDS)

        cur += timedelta(days=1)

    if not all_parts:
        raise RuntimeError("no historical data fetched")

    raw = pd.concat(all_parts, ignore_index=True)
    return raw


def finalize_panel(raw):
    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["stock_id"].apply(is_common_stock_id)].copy()
    df = df[df["close"] > 0].copy()

    for c in ["open", "high", "low"]:
        df[c] = df[c].fillna(df["close"])

    df["volume"] = df["volume"].fillna(0)
    df = df.drop_duplicates(["date", "stock_id"], keep="last")
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]]


def main():
    print("v266.1 build 10y history start")

    raw = build_history()
    panel = finalize_panel(raw)

    if len(panel) < MIN_TOTAL_ROWS:
        raise RuntimeError(f"history too small, rows={len(panel)}")

    raw.to_csv(RAW_OUTPUT, index=False, encoding="utf-8")
    raw.to_csv(DATA_DIR / "raw_market_daily.csv", index=False, encoding="utf-8")
    panel.to_csv(OUTPUT, index=False, encoding="utf-8")
    panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "v266_1_build_price_panel_10y",
        "rows": int(len(panel)),
        "stock_count": int(panel["stock_id"].nunique()),
        "start_date": str(panel["date"].min()),
        "end_date": str(panel["date"].max()),
        "market_counts": panel["market"].value_counts().to_dict(),
    }

    for p in [ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v266.1 10y history completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
