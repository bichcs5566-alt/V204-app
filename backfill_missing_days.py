"""
backfill_missing_days.py

用途：
從目前 price_panel_daily.csv 的最後日期開始，
自動補到目前可取得的最新交易日。

流程：
price_panel_daily.csv
→ 找 max(date)
→ 從 max(date)+1 開始逐日抓 TWSE/TPEX
→ 合併回 price_panel_daily.csv
→ 同步 mobile_dashboard_v1/data

注意：
- 這支只補缺口，不重抓 10 年
- 如果今天還沒收盤或 API 沒資料，會自動跳過
"""

from pathlib import Path
from datetime import datetime, timedelta
import time
import json
import requests
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_SECONDS = 0.35

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
            out.append((fields, data, "tables"))

    for key in ["data9", "data8", "data7", "data6", "data5", "data4", "data3", "data2", "data1"]:
        data = payload.get(key)
        fields = payload.get(f"fields{key[-1]}")
        if isinstance(data, list) and data and isinstance(fields, list) and fields:
            out.append(([str(c).strip() for c in fields], data, key))

    return out


def field_index(fields, keywords):
    for i, f in enumerate(fields):
        for kw in keywords:
            if kw in f:
                return i
    return None


def parse_rows(payload, date_str, market):
    for fields, rows, source_key in candidate_tables(payload):
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
            try:
                stock_id = normalize_stock_id(r[idx_stock])
                close = to_number(r[idx_close])

                if not is_common_stock_id(stock_id) or close is None or close <= 0:
                    continue

                open_p = to_number(r[idx_open]) if idx_open is not None and idx_open < len(r) else close
                high_p = to_number(r[idx_high]) if idx_high is not None and idx_high < len(r) else close
                low_p = to_number(r[idx_low]) if idx_low is not None and idx_low < len(r) else close
                volume = to_int(r[idx_volume]) if idx_volume is not None and idx_volume < len(r) else 0

                records.append({
                    "date": date_str,
                    "stock_id": stock_id,
                    "name": clean_text(r[idx_name]) if idx_name is not None and idx_name < len(r) else "",
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
    return parse_rows(resp.json(), dt.strftime("%Y-%m-%d"), "TWSE")


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
                    print(f"{date_str} TPEX fallback rows={len(records)}")
                    return pd.DataFrame(records)
        except Exception as e:
            print("TPEX failed:", date_str, str(e)[:120])

    return pd.DataFrame(columns=["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"])


def load_panel():
    p = ROOT / "price_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR / "price_panel_daily.csv"
    if not p.exists() or p.stat().st_size == 0:
        raise FileNotFoundError("price_panel_daily.csv missing")

    df = pd.read_csv(p)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns and "trade_date" in df.columns:
        df["date"] = df["trade_date"]

    if "stock_id" not in df.columns and "symbol" in df.columns:
        df["stock_id"] = df["symbol"]

    for col in ["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            if col == "name" or col == "market":
                df[col] = ""
            elif col == "volume":
                df[col] = 0
            elif col in ["open", "high", "low"] and "close" in df.columns:
                df[col] = df["close"]
            else:
                raise ValueError(f"missing column: {col}")

    return df[["date", "stock_id", "name", "market", "open", "high", "low", "close", "volume"]].copy()


def finalize(df):
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


def main():
    old = finalize(load_panel())
    last_date = pd.to_datetime(old["date"]).max()
    today = datetime.now()

    start = last_date + timedelta(days=1)
    end = today

    print("current last_date:", last_date.date())
    print("backfill start:", start.date())
    print("backfill end:", end.date())

    parts = [old]
    filled_days = []
    skipped_days = []

    cur = start
    while cur <= end:
        if cur.weekday() >= 5:
            skipped_days.append({"date": cur.strftime("%Y-%m-%d"), "reason": "weekend"})
            cur += timedelta(days=1)
            continue

        print("fetch missing day:", cur.strftime("%Y-%m-%d"))

        day_parts = []

        try:
            twse = fetch_twse_day(cur)
            if not twse.empty:
                day_parts.append(twse)
        except Exception as e:
            print("TWSE failed:", cur.strftime("%Y-%m-%d"), str(e)[:160])

        try:
            tpex = fetch_tpex_day(cur)
            if not tpex.empty:
                day_parts.append(tpex)
        except Exception as e:
            print("TPEX hard failed:", cur.strftime("%Y-%m-%d"), str(e)[:160])

        if day_parts:
            ddf = pd.concat(day_parts, ignore_index=True)
            parts.append(ddf)
            filled_days.append({
                "date": cur.strftime("%Y-%m-%d"),
                "rows": int(len(ddf)),
                "symbols": int(ddf["stock_id"].nunique())
            })
        else:
            skipped_days.append({"date": cur.strftime("%Y-%m-%d"), "reason": "no data returned"})

        time.sleep(SLEEP_SECONDS)
        cur += timedelta(days=1)

    merged = finalize(pd.concat(parts, ignore_index=True))

    merged.to_csv(ROOT / "price_panel_daily.csv", index=False, encoding="utf-8")
    merged.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "backfill_missing_days",
        "before_last_date": str(last_date.date()),
        "after_last_date": str(pd.to_datetime(merged["date"]).max().date()),
        "rows": int(len(merged)),
        "stock_count": int(merged["stock_id"].nunique()),
        "unique_dates": int(merged["date"].nunique()),
        "filled_days": filled_days,
        "skipped_days": skipped_days,
    }

    for p in [ROOT / "backfill_report.json", DATA_DIR / "backfill_report.json", ROOT / "data_meta.json", DATA_DIR / "data_meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("backfill completed")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
