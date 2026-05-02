# -*- coding: utf-8 -*-
"""
twse_chip_data_v26623.py
v266.23.9 TWSE 籌碼資料層正式修正版

核心修正：
1. T86 三大法人「全市場」正確 selectType 使用 ALLBUT0999，不是 ALL。
2. 若 ALLBUT0999 失敗，才改逐類別備援抓取。
3. 輸出 chip_source_twse.csv 與 mobile_dashboard_v1/data/chip_source_twse.csv。
4. summary 會顯示 version=v266.23.9、fetch_mode、rows。
5. 沒有資料不讓 pipeline 崩潰，但 rows 太少會明確寫進 summary。
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import json
import re
import time
import pandas as pd
import requests


DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}

# 備援分類。主路徑會先用 ALLBUT0999。
T86_SELECT_TYPES = [
    "01", "02", "03", "04", "05", "06", "07", "08",
    "09", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "20", "21", "22", "23", "24",
    "25", "26", "27", "28", "29", "30", "31", "32",
]


def log(msg: str):
    print(f"[v266.23.9 TWSE CHIP] {msg}", flush=True)


def yyyymmdd(d) -> str:
    return d.strftime("%Y%m%d")


def to_num(v, default=0.0) -> float:
    try:
        if v is None:
            return default
        s = str(v).replace(",", "").replace("+", "").replace("--", "").strip()
        if s in ("", "-", "nan", "NaN", "None", "null"):
            return default
        return float(s)
    except Exception:
        return default


def fetch_json(url: str, name: str) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        log(f"{name} HTTP={r.status_code} URL={url}")
        if r.status_code != 200:
            log(f"{name} HTTP body={r.text[:300]}")
            return {}
        try:
            js = r.json()
        except Exception:
            log(f"{name} JSON parse failed body={r.text[:300]}")
            return {}
        if "data" not in js:
            log(f"{name} no data key, keys={list(js.keys())}, stat={js.get('stat')}, msg={js.get('msg') or js.get('message')}")
        return js
    except Exception as e:
        log(f"{name} exception={e}")
        return {}


def recent_trade_dates(max_days=14):
    today = datetime.now().date()
    for i in range(max_days):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        yield d


def parse_t86_rows(data) -> pd.DataFrame:
    rows = []
    for r in data or []:
        try:
            stock_id = str(r[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            # T86 常見位置：
            # 0 證券代號, 1 證券名稱, 4 外陸資買賣超, 10 投信買賣超, 16 自營商買賣超
            foreign = to_num(r[4]) if len(r) > 4 else 0.0
            trust = to_num(r[10]) if len(r) > 10 else 0.0
            dealer = to_num(r[16]) if len(r) > 16 else 0.0
            inst = foreign + trust + dealer

            rows.append({
                "stock_id": stock_id,
                "stock_name": str(r[1]).strip() if len(r) > 1 else "",
                "foreign_net_buy": foreign,
                "trust_net_buy": trust,
                "dealer_net_buy": dealer,
                "inst_net_buy": inst,
                "inst_buy_days": 1 if inst > 0 else 0,
                "inst_valid": 1,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


def fetch_t86_by_type(date: str, select_type: str) -> pd.DataFrame:
    urls = [
        f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType={select_type}&response=json",
        f"https://www.twse.com.tw/fund/T86?date={date}&selectType={select_type}&response=json",
    ]

    for url in urls:
        js = fetch_json(url, f"T86 date={date} selectType={select_type}")
        data = js.get("data", [])
        df = parse_t86_rows(data)
        log(f"T86 date={date} selectType={select_type} parsed_rows={len(df)}")
        if len(df) > 0:
            return df
        time.sleep(0.3)

    return pd.DataFrame()


def fetch_institutional():
    for d in recent_trade_dates(14):
        date = yyyymmdd(d)

        # 正確全市場代碼：ALLBUT0999
        for all_type in ["ALLBUT0999", "ALL"]:
            df_all = fetch_t86_by_type(date, all_type)
            if len(df_all) >= 100:
                df_all = df_all.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id")
                return df_all, date, all_type

        # 備援：逐類別合併
        frames = []
        for st in T86_SELECT_TYPES:
            df = fetch_t86_by_type(date, st)
            if len(df) > 0:
                frames.append(df)
            time.sleep(0.2)

        if frames:
            out = pd.concat(frames, ignore_index=True)
            out = out.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id")
            log(f"T86 BY_TYPE merged date={date} rows={len(out)}")
            if len(out) >= 100:
                return out, date, "BY_TYPE"

        log(f"T86 date={date} rows too few, try previous trade day")

    log("T86 failed, output empty institutional table")
    return pd.DataFrame(columns=[
        "stock_id", "stock_name", "foreign_net_buy", "trust_net_buy",
        "dealer_net_buy", "inst_net_buy", "inst_buy_days", "inst_valid"
    ]), "", "FAILED"


def parse_margin_rows(data) -> pd.DataFrame:
    rows = []
    for r in data or []:
        try:
            stock_id = str(r[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            rows.append({
                "stock_id": stock_id,
                "margin_balance": to_num(r[5]) if len(r) > 5 else 0.0,
                "short_balance": to_num(r[9]) if len(r) > 9 else 0.0,
                "margin_balance_change": 0.0,
                "short_balance_change": 0.0,
                "margin_valid": 1,
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_margin():
    for d in recent_trade_dates(14):
        date = yyyymmdd(d)
        urls = [
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&response=json",
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?date={date}&response=json",
        ]

        for url in urls:
            js = fetch_json(url, f"MI_MARGN date={date}")
            df = parse_margin_rows(js.get("data", []))
            log(f"MI_MARGN date={date} parsed_rows={len(df)}")
            if len(df) >= 100:
                df = df.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id")
                return df, date
            time.sleep(0.3)

    return pd.DataFrame(columns=[
        "stock_id", "margin_balance", "short_balance",
        "margin_balance_change", "short_balance_change", "margin_valid"
    ]), ""


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        "stock_id": "",
        "stock_name": "",
        "foreign_net_buy": 0.0,
        "trust_net_buy": 0.0,
        "dealer_net_buy": 0.0,
        "inst_net_buy": 0.0,
        "inst_buy_days": 0,
        "inst_valid": 0,
        "margin_balance": 0.0,
        "short_balance": 0.0,
        "margin_balance_change": 0.0,
        "short_balance_change": 0.0,
        "margin_valid": 0,
    }

    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v

    df = df[list(defaults.keys())].copy()
    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})")[0]
    df = df.dropna(subset=["stock_id"])
    df = df.drop_duplicates(subset=["stock_id"], keep="last")
    df = df.sort_values("stock_id")

    for c in defaults:
        if c not in ("stock_id", "stock_name"):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def save_outputs(df: pd.DataFrame, inst_date: str, margin_date: str, fetch_mode: str):
    df = ensure_columns(df)

    for p in [Path("chip_source_twse.csv"), DATA_DIR / "chip_source_twse.csv"]:
        df.to_csv(p, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "TWSE",
        "version": "v266.23.9",
        "fetch_mode": fetch_mode,
        "inst_date": inst_date,
        "margin_date": margin_date,
        "rows": int(len(df)),
        "inst_valid_count": int(df["inst_valid"].sum()) if "inst_valid" in df.columns else 0,
        "margin_valid_count": int(df["margin_valid"].sum()) if "margin_valid" in df.columns else 0,
        "first_30_stock_id": df["stock_id"].head(30).tolist(),
        "important_check": {
            "has_2330": bool((df["stock_id"].astype(str) == "2330").any()),
            "has_2409": bool((df["stock_id"].astype(str) == "2409").any()),
            "has_3707": bool((df["stock_id"].astype(str) == "3707").any()),
        },
        "encoding": "utf-8-sig",
    }

    for p in [Path("chip_source_twse_summary.json"), DATA_DIR / "chip_source_twse_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"SAVED rows={summary['rows']} fetch_mode={fetch_mode} has_2330={summary['important_check']['has_2330']}")


def main():
    inst, inst_date, fetch_mode = fetch_institutional()
    margin, margin_date = fetch_margin()

    if inst.empty and margin.empty:
        out = pd.DataFrame()
    elif inst.empty:
        out = margin.copy()
    elif margin.empty:
        out = inst.copy()
    else:
        out = inst.merge(margin, on="stock_id", how="outer")

    save_outputs(out, inst_date, margin_date, fetch_mode)


if __name__ == "__main__":
    main()
