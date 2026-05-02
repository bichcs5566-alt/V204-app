# -*- coding: utf-8 -*-
"""
twse_chip_data_v26623.py
v266.24 完整籌碼資料層

目的：
1. 抓 TWSE 上市三大法人 T86，全市場 selectType=ALLBUT0999。
2. 嘗試抓 TPEX 上櫃三大法人資料。
3. 嘗試抓 TWSE 融資融券。
4. 以 stock_basic_tw_full.csv / candidates / final_action_plan / price_panel 建立 universe。
5. 所有 universe 股票都輸出；沒有法人資料不代表缺資料，補 0。
6. 輸出：
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


VERSION = "v266.24"
DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}


def log(msg: str):
    print(f"[{VERSION} CHIP] {msg}", flush=True)


def yyyymmdd(d) -> str:
    return d.strftime("%Y%m%d")


def roc_date_slash(d) -> str:
    return f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"


def to_num(v, default=0.0) -> float:
    try:
        if v is None:
            return default
        s = str(v).replace(",", "").replace("+", "").replace("--", "").replace("%", "").strip()
        if s in ("", "-", "nan", "NaN", "None", "null"):
            return default
        return float(s)
    except Exception:
        return default


def stock_id(v) -> str:
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else ""


def read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path)


def recent_trade_dates(max_days=14):
    today = datetime.now().date()
    for i in range(max_days):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        yield d


def fetch_json(url: str, name: str, params=None) -> dict:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        log(f"{name} HTTP={r.status_code} URL={r.url}")
        if r.status_code != 200:
            log(f"{name} body={r.text[:300]}")
            return {}
        try:
            return r.json()
        except Exception:
            log(f"{name} json parse failed body={r.text[:300]}")
            return {}
    except Exception as e:
        log(f"{name} exception={e}")
        return {}


def build_universe() -> pd.DataFrame:
    frames = []

    candidates = [
        Path("mobile_dashboard_v1/data/stock_basic_tw_full.csv"),
        Path("stock_basic_tw_full.csv"),
        Path("mobile_dashboard_v1/data/final_action_plan.csv"),
        Path("final_action_plan.csv"),
        Path("mobile_dashboard_v1/data/candidates.csv"),
        Path("candidates.csv"),
        Path("mobile_dashboard_v1/data/trading_system_plan.csv"),
        Path("trading_system_plan.csv"),
        Path("mobile_dashboard_v1/data/price_panel_daily.csv"),
        Path("price_panel_daily.csv"),
    ]

    for p in candidates:
        if not p.exists():
            continue
        try:
            df = read_csv_safe(p)
            sid_col = None
            name_col = None
            for c in ["stock_id", "symbol", "code", "個股", "股票代號"]:
                if c in df.columns:
                    sid_col = c
                    break
            for c in ["stock_name", "name", "股票名稱", "證券名稱"]:
                if c in df.columns:
                    name_col = c
                    break
            if sid_col:
                tmp = pd.DataFrame()
                tmp["stock_id"] = df[sid_col].astype(str).map(stock_id)
                tmp["stock_name"] = df[name_col].astype(str) if name_col else ""
                tmp = tmp[tmp["stock_id"].str.len() == 4]
                frames.append(tmp)
                log(f"universe source {p} rows={len(tmp)}")
        except Exception as e:
            log(f"universe read failed {p}: {e}")

    if not frames:
        return pd.DataFrame(columns=["stock_id", "stock_name"])

    uni = pd.concat(frames, ignore_index=True)
    uni = uni.dropna(subset=["stock_id"])
    uni = uni[uni["stock_id"].astype(str).str.fullmatch(r"\d{4}")]
    uni = uni.drop_duplicates(subset=["stock_id"], keep="first")
    uni = uni.sort_values("stock_id")
    log(f"universe total rows={len(uni)}")
    return uni


def parse_twse_t86(data) -> pd.DataFrame:
    rows = []
    for r in data or []:
        try:
            sid = stock_id(r[0])
            if not sid:
                continue

            foreign = to_num(r[4]) if len(r) > 4 else 0.0
            trust = to_num(r[10]) if len(r) > 10 else 0.0
            dealer = to_num(r[16]) if len(r) > 16 else 0.0
            inst = foreign + trust + dealer

            rows.append({
                "stock_id": sid,
                "stock_name": str(r[1]).strip() if len(r) > 1 else "",
                "foreign_net_buy": foreign,
                "trust_net_buy": trust,
                "dealer_net_buy": dealer,
                "inst_net_buy": inst,
                "inst_buy_days": 1 if inst > 0 else 0,
                "inst_valid": 1,
                "chip_market": "TWSE",
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_twse_t86() -> tuple[pd.DataFrame, str, str]:
    for d in recent_trade_dates(14):
        date = yyyymmdd(d)

        # 正確全市場上市普通股：ALLBUT0999
        for select_type in ["ALLBUT0999", "ALL"]:
            urls = [
                f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType={select_type}&response=json",
                f"https://www.twse.com.tw/fund/T86?date={date}&selectType={select_type}&response=json",
            ]
            for url in urls:
                js = fetch_json(url, f"TWSE T86 {date} {select_type}")
                df = parse_twse_t86(js.get("data", []))
                log(f"TWSE T86 date={date} select={select_type} rows={len(df)}")
                if len(df) >= 100:
                    return df.drop_duplicates("stock_id", keep="last"), date, select_type
                time.sleep(0.3)

        log(f"TWSE T86 {date} insufficient, try previous day")

    return pd.DataFrame(), "", "FAILED"


def parse_tpex_3inst_json(js) -> pd.DataFrame:
    """
    TPEX endpoint 欄位可能變動，所以採用彈性解析：
    先找 tables/data，再從 row 中抓 4碼代號與買賣超數字。
    """
    data_candidates = []

    if isinstance(js, dict):
        for key in ["aaData", "data"]:
            if isinstance(js.get(key), list):
                data_candidates.append(js[key])
        if isinstance(js.get("tables"), list):
            for t in js["tables"]:
                if isinstance(t, dict) and isinstance(t.get("data"), list):
                    data_candidates.append(t["data"])

    rows = []
    for data in data_candidates:
        for r in data:
            try:
                if not isinstance(r, (list, tuple)):
                    continue
                sid = ""
                name = ""
                for item in r[:4]:
                    maybe = stock_id(item)
                    if maybe:
                        sid = maybe
                        break
                if not sid:
                    continue

                # 名稱通常在代號旁邊，找第一個非數字中文欄位
                for item in r[:5]:
                    s = str(item).strip()
                    if s and not re.fullmatch(r"[\d,.\-+]+", s) and not stock_id(s):
                        name = s
                        break

                nums = [to_num(x, None) for x in r]
                nums = [x for x in nums if x is not None]

                # 保守取最後幾個買賣超欄位；若解析不到，仍保留 stock_id 並補 0。
                foreign = nums[-6] if len(nums) >= 6 else 0.0
                trust = nums[-4] if len(nums) >= 4 else 0.0
                dealer = nums[-2] if len(nums) >= 2 else 0.0
                inst = foreign + trust + dealer

                rows.append({
                    "stock_id": sid,
                    "stock_name": name,
                    "foreign_net_buy": foreign,
                    "trust_net_buy": trust,
                    "dealer_net_buy": dealer,
                    "inst_net_buy": inst,
                    "inst_buy_days": 1 if inst > 0 else 0,
                    "inst_valid": 1,
                    "chip_market": "TPEX",
                })
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).drop_duplicates("stock_id", keep="last")


def fetch_tpex_3inst() -> tuple[pd.DataFrame, str, str]:
    for d in recent_trade_dates(14):
        roc = roc_date_slash(d)
        # TPEX 端點偶爾調整，這裡使用常見 JSON 端點，多組 fallback。
        endpoints = [
            (
                "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php",
                {"l": "zh-tw", "se": "EW", "t": "D", "d": roc, "s": "0,asc", "o": "json"},
            ),
            (
                "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php",
                {"l": "zh-tw", "d": roc, "se": "EW", "o": "json"},
            ),
        ]

        for url, params in endpoints:
            js = fetch_json(url, f"TPEX 3INST {roc}", params=params)
            df = parse_tpex_3inst_json(js)
            log(f"TPEX 3INST date={roc} rows={len(df)}")
            if len(df) >= 50:
                return df, roc, "TPEX_3INST"
            time.sleep(0.5)

    return pd.DataFrame(), "", "FAILED"


def parse_twse_margin(data) -> pd.DataFrame:
    rows = []
    for r in data or []:
        try:
            sid = stock_id(r[0])
            if not sid:
                continue
            rows.append({
                "stock_id": sid,
                "margin_balance": to_num(r[5]) if len(r) > 5 else 0.0,
                "short_balance": to_num(r[9]) if len(r) > 9 else 0.0,
                "margin_balance_change": 0.0,
                "short_balance_change": 0.0,
                "margin_valid": 1,
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_twse_margin() -> tuple[pd.DataFrame, str]:
    for d in recent_trade_dates(14):
        date = yyyymmdd(d)
        urls = [
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?date={date}&selectType=ALL&response=json",
            f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&response=json",
        ]
        for url in urls:
            js = fetch_json(url, f"TWSE MARGIN {date}")
            df = parse_twse_margin(js.get("data", []))
            log(f"TWSE MARGIN date={date} rows={len(df)}")
            if len(df) >= 100:
                return df.drop_duplicates("stock_id", keep="last"), date
            time.sleep(0.3)
    return pd.DataFrame(), ""


def ensure_final_columns(df: pd.DataFrame) -> pd.DataFrame:
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
        "chip_market": "",
    }
    for c, v in defaults.items():
        if c not in df.columns:
            df[c] = v

    df = df[list(defaults.keys())].copy()
    df["stock_id"] = df["stock_id"].astype(str).map(stock_id)
    df = df[df["stock_id"].str.fullmatch(r"\d{4}")]
    df = df.drop_duplicates("stock_id", keep="last")
    df = df.sort_values("stock_id")

    num_cols = [c for c in defaults if c not in ("stock_id", "stock_name", "chip_market")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df


def main():
    universe = build_universe()

    twse_inst, twse_date, twse_mode = fetch_twse_t86()
    tpex_inst, tpex_date, tpex_mode = fetch_tpex_3inst()
    twse_margin, margin_date = fetch_twse_margin()

    inst_frames = []
    if not twse_inst.empty:
        inst_frames.append(twse_inst)
    if not tpex_inst.empty:
        inst_frames.append(tpex_inst)

    if inst_frames:
        inst_all = pd.concat(inst_frames, ignore_index=True)
        inst_all = inst_all.drop_duplicates("stock_id", keep="last")
    else:
        inst_all = pd.DataFrame(columns=[
            "stock_id", "stock_name", "foreign_net_buy", "trust_net_buy",
            "dealer_net_buy", "inst_net_buy", "inst_buy_days", "inst_valid", "chip_market"
        ])

    if universe.empty:
        base = inst_all.copy()
    else:
        base = universe.merge(inst_all, on="stock_id", how="left", suffixes=("", "_inst"))
        if "stock_name_inst" in base.columns:
            base["stock_name"] = base["stock_name"].where(base["stock_name"].astype(str).str.strip() != "", base["stock_name_inst"])
            base.drop(columns=["stock_name_inst"], inplace=True, errors="ignore")

    if not twse_margin.empty:
        base = base.merge(twse_margin, on="stock_id", how="left")

    out = ensure_final_columns(base)

    # 重要：全市場補齊後，沒有法人交易資料視為 0；但 inst_valid=0 保留作為信心判斷
    for c in [
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "margin_balance", "short_balance",
        "margin_balance_change", "short_balance_change"
    ]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    for c in ["inst_valid", "margin_valid"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    out["chip_market"] = out["chip_market"].fillna("")

    for p in [Path("chip_source_twse.csv"), DATA_DIR / "chip_source_twse.csv"]:
        out.to_csv(p, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "TWSE+TPEX+UNIVERSE",
        "version": VERSION,
        "rows": int(len(out)),
        "universe_rows": int(len(universe)),
        "twse_inst_rows": int(len(twse_inst)),
        "tpex_inst_rows": int(len(tpex_inst)),
        "inst_valid_count": int(out["inst_valid"].sum()),
        "margin_valid_count": int(out["margin_valid"].sum()),
        "twse_date": twse_date,
        "twse_mode": twse_mode,
        "tpex_date": tpex_date,
        "tpex_mode": tpex_mode,
        "margin_date": margin_date,
        "important_check": {
            "has_2330": bool((out["stock_id"] == "2330").any()),
            "has_2409": bool((out["stock_id"] == "2409").any()),
            "has_3707": bool((out["stock_id"] == "3707").any()),
            "has_6239": bool((out["stock_id"] == "6239").any()),
        },
        "note": "Universe stocks are all kept. Missing institutional rows are treated as 0 but lower confidence.",
        "encoding": "utf-8-sig",
    }

    for p in [Path("chip_source_twse_summary.json"), DATA_DIR / "chip_source_twse_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
