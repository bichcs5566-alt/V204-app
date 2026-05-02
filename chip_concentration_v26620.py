# -*- coding: utf-8 -*-
"""
v266.23.1 TWSE 籌碼資料層穩定修正版

修正重點：
- TWSE 回傳沒有 data 時不讓 pipeline 失敗
- 三大法人抓得到就先輸出
- 融資融券抓不到時保留欄位，不中斷
- 同步輸出 root 與 mobile_dashboard_v1/data
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
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
}


def log(msg: str):
    print(f"[v266.23.1 TWSE CHIP] {msg}")


def to_num(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).replace(",", "").replace("+", "").strip()
        if s in ("", "--", "-", "nan", "None", "null"):
            return default
        return float(s)
    except Exception:
        return default


def fetch_json(url: str, name: str) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        log(f"{name} HTTP {r.status_code}")
        if r.status_code != 200:
            log(f"{name} failed body: {r.text[:300]}")
            return {}

        try:
            js = r.json()
        except Exception:
            log(f"{name} not json: {r.text[:300]}")
            return {}

        if "data" not in js:
            log(f"{name} no data key. keys={list(js.keys())} stat={js.get('stat')} msg={js.get('msg') or js.get('message')}")
            return js

        return js
    except Exception as e:
        log(f"{name} exception: {e}")
        return {}


def fetch_institutional() -> pd.DataFrame:
    """
    TWSE 三大法人 T86
    endpoint: /rwd/zh/fund/T86?response=json
    """
    urls = [
        "https://www.twse.com.tw/rwd/zh/fund/T86?response=json",
        "https://www.twse.com.tw/fund/T86?response=json",
    ]

    js = {}
    for u in urls:
        js = fetch_json(u, "T86 三大法人")
        if js.get("data"):
            break
        time.sleep(1)

    data = js.get("data", [])
    fields = js.get("fields", [])

    if not data:
        log("三大法人無資料，輸出空表")
        return pd.DataFrame(columns=[
            "stock_id", "foreign_net_buy", "trust_net_buy", "dealer_net_buy",
            "inst_net_buy", "inst_buy_days", "inst_valid"
        ])

    rows = []

    # T86 欄位常見：
    # 0證券代號, 1證券名稱, 4外陸資買賣超股數, 10投信買賣超股數, 16自營商買賣超股數
    for d in data:
        try:
            stock_id = str(d[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            foreign = to_num(d[4]) if len(d) > 4 else 0.0
            trust = to_num(d[10]) if len(d) > 10 else 0.0
            dealer = to_num(d[16]) if len(d) > 16 else 0.0

            rows.append({
                "stock_id": stock_id,
                "stock_name": str(d[1]).strip() if len(d) > 1 else "",
                "foreign_net_buy": foreign,
                "trust_net_buy": trust,
                "dealer_net_buy": dealer,
                "inst_net_buy": foreign + trust + dealer,
                # 目前只抓單日，連買天數先保守處理：單日買超視為 1，否則 0
                "inst_buy_days": 1 if (foreign + trust + dealer) > 0 else 0,
                "inst_valid": 1,
            })
        except Exception:
            continue

    out = pd.DataFrame(rows)
    log(f"三大法人筆數：{len(out)}")
    return out


def fetch_margin() -> pd.DataFrame:
    """
    TWSE 融資融券。
    MI_MARGN 有時沒有 data，這裡不讓它中斷。
    """
    urls = [
        "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json",
        "https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json",
    ]

    js = {}
    for u in urls:
        js = fetch_json(u, "MI_MARGN 融資融券")
        if js.get("data"):
            break
        time.sleep(1)

    data = js.get("data", [])
    if not data:
        log("融資融券無資料，保留空欄位不中斷")
        return pd.DataFrame(columns=[
            "stock_id", "margin_balance", "short_balance",
            "margin_balance_change", "short_balance_change", "margin_valid"
        ])

    rows = []

    for d in data:
        try:
            stock_id = str(d[0]).strip()
            if not re.fullmatch(r"\d{4}", stock_id):
                continue

            # 不同格式欄位位置可能不同，先用較穩的 fallback：
            # 嘗試從整列找數字欄位，若格式不符就留 0
            nums = [to_num(x, None) for x in d]
            nums = [x for x in nums if x is not None]

            margin_balance = 0.0
            short_balance = 0.0

            # 原本預估位置
            if len(d) > 5:
                margin_balance = to_num(d[5])
            if len(d) > 9:
                short_balance = to_num(d[9])

            rows.append({
                "stock_id": stock_id,
                "margin_balance": margin_balance,
                "short_balance": short_balance,
                # v266.23.1 尚未接昨日快照，先填 0，下一版可用歷史檔算差額
                "margin_balance_change": 0.0,
                "short_balance_change": 0.0,
                "margin_valid": 1,
            })
        except Exception:
            continue

    out = pd.DataFrame(rows)
    log(f"融資融券筆數：{len(out)}")
    return out


def save_outputs(df: pd.DataFrame):
    for p in [
        Path("chip_source_twse.csv"),
        DATA_DIR / "chip_source_twse.csv",
    ]:
        df.to_csv(p, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "TWSE",
        "version": "v266.23.1",
        "rows": int(len(df)),
        "inst_valid_count": int(pd.to_numeric(df.get("inst_valid", 0), errors="coerce").fillna(0).sum()) if not df.empty else 0,
        "margin_valid_count": int(pd.to_numeric(df.get("margin_valid", 0), errors="coerce").fillna(0).sum()) if not df.empty else 0,
        "encoding": "utf-8-sig",
    }

    for p in [
        Path("chip_source_twse_summary.json"),
        DATA_DIR / "chip_source_twse_summary.json",
    ]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"saved chip_source_twse.csv rows={len(df)}")


def main():
    inst = fetch_institutional()
    margin = fetch_margin()

    if inst.empty and margin.empty:
        log("三大法人與融資融券都無資料，輸出空表但不中斷")
        df = pd.DataFrame(columns=[
            "stock_id", "stock_name",
            "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy", "inst_buy_days", "inst_valid",
            "margin_balance", "short_balance", "margin_balance_change", "short_balance_change", "margin_valid",
        ])
    elif inst.empty:
        df = margin.copy()
        for c in ["stock_name", "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy", "inst_buy_days", "inst_valid"]:
            if c not in df.columns:
                df[c] = "" if c == "stock_name" else 0
    elif margin.empty:
        df = inst.copy()
        for c in ["margin_balance", "short_balance", "margin_balance_change", "short_balance_change", "margin_valid"]:
            if c not in df.columns:
                df[c] = 0
    else:
        df = inst.merge(margin, on="stock_id", how="left")
        for c in ["margin_balance", "short_balance", "margin_balance_change", "short_balance_change", "margin_valid"]:
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    save_outputs(df)


if __name__ == "__main__":
    main()
