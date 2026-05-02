# -*- coding: utf-8 -*-
"""
twse_chip_data_v26623.py
v266.23.7 TWSE 籌碼資料層逐類別完整修正版

修正核心：
- T86 三大法人不能只依賴 selectType=ALL，因為實測可能只回水泥類。
- 改成：
  1. 先嘗試 ALL
  2. 若筆數太少，改逐類別抓取
  3. 合併全部類別
  4. 以 stock_id 去重
- 產出 chip_source_twse.csv 給 chip_concentration_v26621.py 使用。

輸出：
- chip_source_twse.csv
- chip_source_twse_summary.json
- mobile_dashboard_v1/data/chip_source_twse.csv
- mobile_dashboard_v1/data/chip_source_twse_summary.json
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

# TWSE T86 常見類別代碼。
# 如果 ALL 只回 7 筆，就用這些類別逐一抓。
T86_SELECT_TYPES = [
    "01",  # 水泥
    "02",  # 食品
    "03",  # 塑膠
    "04",  # 紡織
    "05",  # 電機機械
    "06",  # 電器電纜
    "07",  # 化學
    "08",  # 生技醫療
    "09",  # 玻璃陶瓷
    "10",  # 造紙
    "11",  # 鋼鐵
    "12",  # 橡膠
    "13",  # 汽車
    "14",  # 半導體
    "15",  # 電腦及週邊
    "16",  # 光電
    "17",  # 通信網路
    "18",  # 電子零組件
    "19",  # 電子通路
    "20",  # 資訊服務
    "21",  # 其他電子
    "22",  # 建材營造
    "23",  # 航運
    "24",  # 觀光
    "25",  # 金融保險
    "26",  # 貿易百貨
    "27",  # 油電燃氣
    "28",  # 其他
    "29",  # 存託憑證 / 其他分類
    "30",
    "31",
    "32",
]


def log(msg: str):
    print(f"[v266.23.7 TWSE CHIP] {msg}")


def yyyymmdd(dt) -> str:
    return dt.strftime("%Y%m%d")


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
        log(f"{name} HTTP {r.status_code}")
        if r.status_code != 200:
            log(f"{name} failed body={r.text[:250]}")
            return {}
        try:
            js = r.json()
        except Exception:
            log(f"{name} not json body={r.text[:250]}")
            return {}
        return js
    except Exception as e:
        log(f"{name} exception={e}")
        return {}


def recent_trade_dates(max_days: int = 14):
    today = datetime.now().date()
    for i in range(max_days):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        yield d


def parse_t86_rows(data) -> pd.DataFrame:
    rows = []
    for d in data or []:
        try:
            stock_id = str(d[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            # T86 欄位常見位置：
            # 0 證券代號
            # 1 證券名稱
            # 4 外陸資買賣超股數
            # 10 投信買賣超股數
            # 16 自營商買賣超股數
            foreign = to_num(d[4]) if len(d) > 4 else 0.0
            trust = to_num(d[10]) if len(d) > 10 else 0.0
            dealer = to_num(d[16]) if len(d) > 16 else 0.0
            inst = foreign + trust + dealer

            rows.append({
                "stock_id": stock_id,
                "stock_name": str(d[1]).strip() if len(d) > 1 else "",
                "foreign_net_buy": foreign,
                "trust_net_buy": trust,
                "dealer_net_buy": dealer,
                "inst_net_buy": inst,
                "inst_buy_days": 1 if inst > 0 else 0,
                "inst_valid": 1,
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=[
            "stock_id", "stock_name",
            "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
            "inst_buy_days", "inst_valid"
        ])

    return pd.DataFrame(rows)


def fetch_t86_by_type(date: str, select_type: str) -> pd.DataFrame:
    urls = [
        f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType={select_type}&response=json",
        f"https://www.twse.com.tw/fund/T86?date={date}&selectType={select_type}&response=json",
    ]

    for url in urls:
        js = fetch_json(url, f"T86 date={date} type={select_type}")
        data = js.get("data", [])
        if data:
            df = parse_t86_rows(data)
            if not df.empty:
                return df
        time.sleep(0.25)

    return pd.DataFrame()


def fetch_institutional() -> tuple[pd.DataFrame, str, str]:
    """
    回傳：
    df, data_date, fetch_mode
    """
    for d in recent_trade_dates(14):
        date = yyyymmdd(d)

        # 先試 ALL
        df_all = fetch_t86_by_type(date, "ALL")
        log(f"T86 ALL date={date} rows={len(df_all)}")

        if len(df_all) >= 100:
            df_all = df_all.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id")
            return df_all, date, "ALL"

        # ALL 不足，改逐類別
        log(f"T86 ALL 筆數不足，改逐類別抓取 date={date}")
        frames = []

        for st in T86_SELECT_TYPES:
            df = fetch_t86_by_type(date, st)
            if not df.empty:
                log(f"T86 type={st} rows={len(df)}")
                frames.append(df)
            time.sleep(0.2)

        if frames:
            out = pd.concat(frames, ignore_index=True)
            out = out.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id")
            log(f"T86 merged date={date} rows={len(out)}")
            if len(out) >= 100:
                return out, date, "BY_TYPE"

        log(f"T86 date={date} 仍不足，往前找上一個交易日")

    log("T86 抓不到足夠資料，回空表")
    return pd.DataFrame(columns=[
        "stock_id", "stock_name",
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid"
    ]), "", "FAILED"


def parse_margin_rows(data) -> pd.DataFrame:
    rows = []
    for d in data or []:
        try:
            stock_id = str(d[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

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
            data = js.get("data", [])
            if not data:
                time.sleep(0.25)
                continue

            df = parse_margin_rows(data)
            log(f"MI_MARGN date={date} rows={len(df)}")

            if len(df) >= 100:
                return df.drop_duplicates(subset=["stock_id"], keep="last").sort_values("stock_id"), date

        log(f"MI_MARGN date={date} 無足夠資料，往前找")

    log("MI_MARGN 抓不到足夠資料，回空表")
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

    num_cols = [c for c in defaults.keys() if c not in ("stock_id", "stock_name")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def save_outputs(df: pd.DataFrame, inst_date: str, margin_date: str, mode: str):
    df = ensure_columns(df)

    for p in [
        Path("chip_source_twse.csv"),
        DATA_DIR / "chip_source_twse.csv",
    ]:
        df.to_csv(p, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "v266.23.7",
        "source": "TWSE",
        "fetch_mode": mode,
        "inst_date": inst_date,
        "margin_date": margin_date,
        "rows": int(len(df)),
        "inst_valid_count": int(df["inst_valid"].sum()) if "inst_valid" in df.columns else 0,
        "margin_valid_count": int(df["margin_valid"].sum()) if "margin_valid" in df.columns else 0,
        "first_20_stock_id": df["stock_id"].head(20).tolist(),
        "note": "If ALL returns too few rows, script merges T86 by selectType.",
        "encoding": "utf-8-sig",
    }

    for p in [
        Path("chip_source_twse_summary.json"),
        DATA_DIR / "chip_source_twse_summary.json",
    ]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"saved chip_source_twse rows={summary['rows']} inst_valid={summary['inst_valid_count']} margin_valid={summary['margin_valid_count']} mode={mode}")


def main():
    inst, inst_date, mode = fetch_institutional()
    margin, margin_date = fetch_margin()

    if inst.empty and margin.empty:
        out = pd.DataFrame()
    elif inst.empty:
        out = margin.copy()
    elif margin.empty:
        out = inst.copy()
    else:
        out = inst.merge(margin, on="stock_id", how="outer")

    save_outputs(out, inst_date, margin_date, mode)


if __name__ == "__main__":
    main()
