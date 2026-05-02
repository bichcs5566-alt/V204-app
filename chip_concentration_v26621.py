# -*- coding: utf-8 -*-
"""
v266.23.5 TWSE 籌碼資料層完整穩定版

修正重點：
1. T86 三大法人加入 selectType=ALL，不會只抓到單一產業類別。
2. 自動往前找最近有資料的交易日。
3. MI_MARGN 融資融券安全抓取，沒有資料不會讓 pipeline 失敗。
4. 全部輸出成系統標準欄位：
   stock_id, stock_name,
   foreign_net_buy, trust_net_buy, dealer_net_buy, inst_net_buy,
   inst_buy_days, inst_valid,
   margin_balance, short_balance, margin_balance_change, short_balance_change, margin_valid
5. 同步輸出：
   chip_source_twse.csv
   mobile_dashboard_v1/data/chip_source_twse.csv
   chip_source_twse_summary.json
   mobile_dashboard_v1/data/chip_source_twse_summary.json
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


def log(msg: str):
    print(f"[v266.23.5 TWSE CHIP] {msg}")


def yyyymmdd(dt) -> str:
    return dt.strftime("%Y%m%d")


def to_num(v, default=0.0):
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
        log(f"{name} HTTP {r.status_code} url={url}")
        if r.status_code != 200:
            log(f"{name} failed body={r.text[:250]}")
            return {}
        try:
            js = r.json()
        except Exception:
            log(f"{name} not json body={r.text[:250]}")
            return {}
        if "data" not in js:
            log(f"{name} no data key keys={list(js.keys())} stat={js.get('stat')} msg={js.get('msg') or js.get('message')}")
        return js
    except Exception as e:
        log(f"{name} exception={e}")
        return {}


def recent_dates(max_days: int = 12):
    today = datetime.now().date()
    for i in range(max_days):
        d = today - timedelta(days=i)
        # 週六週日略過，但保留最近幾天安全
        if d.weekday() >= 5:
            continue
        yield d


def parse_t86_row(d):
    """
    TWSE T86 常見欄位：
    0 證券代號
    1 證券名稱
    4 外陸資買賣超股數
    10 投信買賣超股數
    16 自營商買賣超股數
    """
    stock_id = str(d[0]).strip()
    if not re.fullmatch(r"\d{4}", stock_id):
        return None

    foreign = to_num(d[4]) if len(d) > 4 else 0.0
    trust = to_num(d[10]) if len(d) > 10 else 0.0
    dealer = to_num(d[16]) if len(d) > 16 else 0.0

    inst = foreign + trust + dealer

    return {
        "stock_id": stock_id,
        "stock_name": str(d[1]).strip() if len(d) > 1 else "",
        "foreign_net_buy": foreign,
        "trust_net_buy": trust,
        "dealer_net_buy": dealer,
        "inst_net_buy": inst,
        # 單日資料先保守處理：單日買超 = 1，未買超 = 0
        "inst_buy_days": 1 if inst > 0 else 0,
        "inst_valid": 1,
    }


def fetch_institutional() -> tuple[pd.DataFrame, str]:
    """
    三大法人：一定要 selectType=ALL。
    沒加 selectType=ALL 時，TWSE 可能只回單一產業，常見就只剩 1101~1110 這種少量資料。
    """
    for d in recent_dates(12):
        date = yyyymmdd(d)

        urls = [
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/fund/T86?date={date}&selectType=ALL&response=json",
        ]

        for url in urls:
            js = fetch_json(url, f"T86 三大法人 {date}")
            data = js.get("data", [])
            if not data:
                time.sleep(0.5)
                continue

            rows = []
            for row in data:
                parsed = parse_t86_row(row)
                if parsed:
                    rows.append(parsed)

            df = pd.DataFrame(rows)
            if len(df) >= 100:
                log(f"T86 成功 date={date} rows={len(df)}")
                return df, date

            log(f"T86 rows too few date={date} rows={len(df)}，繼續往前找")
            time.sleep(0.5)

    log("T86 三大法人抓不到足夠資料，回空表")
    return pd.DataFrame(columns=[
        "stock_id", "stock_name",
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid"
    ]), ""


def parse_margin_rows(data):
    rows = []
    for d in data:
        try:
            stock_id = str(d[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            # TWSE MI_MARGN 欄位格式可能變動，先用常見位置，不足就填 0。
            margin_balance = to_num(d[5]) if len(d) > 5 else 0.0
            short_balance = to_num(d[9]) if len(d) > 9 else 0.0

            rows.append({
                "stock_id": stock_id,
                "margin_balance": margin_balance,
                "short_balance": short_balance,
                "margin_balance_change": 0.0,
                "short_balance_change": 0.0,
                "margin_valid": 1,
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_margin() -> tuple[pd.DataFrame, str]:
    """
    融資融券：TWSE 有時 date/selectType 參數不同會回無 data。
    所以用多 URL fallback，不足時不中斷。
    """
    for d in recent_dates(12):
        date = yyyymmdd(d)

        urls = [
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&response=json",
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?date={date}&response=json",
        ]

        for url in urls:
            js = fetch_json(url, f"MI_MARGN 融資融券 {date}")
            data = js.get("data", [])
            if not data:
                time.sleep(0.5)
                continue

            df = parse_margin_rows(data)
            if len(df) >= 100:
                log(f"MI_MARGN 成功 date={date} rows={len(df)}")
                return df, date

            log(f"MI_MARGN rows too few date={date} rows={len(df)}，繼續往前找")
            time.sleep(0.5)

    log("MI_MARGN 融資融券抓不到足夠資料，回空表")
    return pd.DataFrame(columns=[
        "stock_id", "margin_balance", "short_balance",
        "margin_balance_change", "short_balance_change", "margin_valid"
    ]), ""


def attach_default_columns(df: pd.DataFrame) -> pd.DataFrame:
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
    return df[list(defaults.keys())]


def save_outputs(df: pd.DataFrame, inst_date: str, margin_date: str):
    df = attach_default_columns(df)

    # 數字欄位清理
    num_cols = [
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid",
        "margin_balance", "short_balance",
        "margin_balance_change", "short_balance_change", "margin_valid",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["stock_id"] = df["stock_id"].astype(str).str.extract(r"(\d{4})")[0]
    df = df.dropna(subset=["stock_id"]).drop_duplicates(subset=["stock_id"], keep="last")
    df = df.sort_values("stock_id")

    for p in [Path("chip_source_twse.csv"), DATA_DIR / "chip_source_twse.csv"]:
        df.to_csv(p, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "v266.23.5",
        "source": "TWSE",
        "inst_date": inst_date,
        "margin_date": margin_date,
        "rows": int(len(df)),
        "inst_valid_count": int(df["inst_valid"].sum()) if "inst_valid" in df.columns else 0,
        "margin_valid_count": int(df["margin_valid"].sum()) if "margin_valid" in df.columns else 0,
        "note": "T86 uses selectType=ALL. If rows are very small, selectType/date failed.",
        "encoding": "utf-8-sig",
    }

    for p in [Path("chip_source_twse_summary.json"), DATA_DIR / "chip_source_twse_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"saved rows={len(df)} inst_valid={summary['inst_valid_count']} margin_valid={summary['margin_valid_count']}")


def main():
    inst, inst_date = fetch_institutional()
    margin, margin_date = fetch_margin()

    if inst.empty and margin.empty:
        out = pd.DataFrame()
    elif inst.empty:
        out = margin.copy()
    elif margin.empty:
        out = inst.copy()
    else:
        out = inst.merge(margin, on="stock_id", how="outer")

    save_outputs(out, inst_date, margin_date)


if __name__ == "__main__":
    main()
