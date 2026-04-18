# build_full_price_panel_10y.py
from __future__ import annotations

import argparse
import math
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

ROOT = Path(".")
OUTPUT_PANEL = ROOT / "price_panel_daily.csv"
OUTPUT_UNIVERSE = ROOT / "symbol_universe.csv"
OUTPUT_SUMMARY = ROOT / "build_price_panel_summary.csv"
OUTPUT_LOG = ROOT / "build_price_panel_log.txt"
CHECKPOINT_DIR = ROOT / "_price_panel_checkpoints"

DEFAULT_START = "2015-01-01"
REQUEST_TIMEOUT = 30
CHUNK_SIZE = 80
SLEEP_BETWEEN_CHUNKS = 1.5
MIN_VALID_ROWS_PER_SYMBOL = 20

TWSE_SYMBOL_URLS = [
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_O",
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_P",
]
TPEX_SYMBOL_URLS = [
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    "https://www.tpex.org.tw/openapi/v1/mkt/stock/aftertrading/daily_close_quotes",
]
USER_AGENT = {"User-Agent": "Mozilla/5.0 (compatible; build_full_price_panel_10y/1.0)"}

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with OUTPUT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def clean_code(x: object) -> str:
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)
    return s

def is_taiwan_stock_code(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4,6}", s))

def find_existing_symbol_candidates() -> pd.DataFrame:
    records = []
    for p in ROOT.rglob("*.csv"):
        if p.name.startswith("build_price_panel_"):
            continue
        try:
            head = pd.read_csv(p, nrows=200)
        except Exception:
            continue
        cols = {str(c).strip().lower(): c for c in head.columns}
        symbol_col = None
        for cand in ["symbol", "stock_id", "code", "ticker", "è­å¸ä»£è", "è¡ç¥¨ä»£è"]:
            if cand.lower() in cols:
                symbol_col = cols[cand.lower()]
                break
        if symbol_col is not None:
            vals = head[symbol_col].dropna().astype(str).map(clean_code)
            vals = sorted({v for v in vals if is_taiwan_stock_code(v)})
            for v in vals:
                records.append({"symbol": v, "source": str(p), "market_hint": None})
        if "symbol" in cols and ("market" in cols or "exchange" in cols):
            market_col = cols["market"] if "market" in cols else cols["exchange"]
            tmp = head[[cols["symbol"], market_col]].dropna().copy()
            tmp.columns = ["symbol", "market_hint"]
            tmp["symbol"] = tmp["symbol"].astype(str).map(clean_code)
            tmp["market_hint"] = tmp["market_hint"].astype(str).str.upper().str.strip()
            tmp = tmp[tmp["symbol"].map(is_taiwan_stock_code)]
            records.extend({"symbol": r.symbol, "source": str(p), "market_hint": r.market_hint} for r in tmp.itertuples(index=False))
    return pd.DataFrame(records).drop_duplicates() if records else pd.DataFrame(columns=["symbol","market_hint","source"])

def fetch_json(url: str):
    r = requests.get(url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    text = r.text.strip()
    if "application/json" in r.headers.get("Content-Type","") or text.startswith("[") or text.startswith("{"):
        return r.json()
    raise ValueError(f"Not JSON: {url}")

def _extract_code_frame(data, market, url):
    if isinstance(data, dict):
        data = data.get("data", [])
    if not isinstance(data, list):
        return pd.DataFrame()
    df = pd.DataFrame(data)
    possible_cols = [c for c in df.columns if str(c).lower() in {"companycode","securitiescompanycode","scode","code","å¬å¸ä»£è","è¡ç¥¨ä»£è","è­å¸ä»£è"}]
    if not possible_cols:
        for c in df.columns:
            if "ä»£è" in str(c) or "code" in str(c).lower():
                possible_cols.append(c)
        possible_cols = possible_cols[:1]
    if not possible_cols:
        return pd.DataFrame()
    code_col = possible_cols[0]
    name_col = None
    for c in df.columns:
        if "åç¨±" in str(c) or "name" in str(c).lower():
            name_col = c
            break
    out = pd.DataFrame({
        "symbol": df[code_col].astype(str).map(clean_code),
        "name": df[name_col].astype(str) if name_col is not None else "",
        "market": market,
        "source": url,
    })
    out = out[out["symbol"].map(is_taiwan_stock_code)].drop_duplicates(subset=["symbol"])
    return out

def fetch_symbols(urls, market) -> pd.DataFrame:
    for url in urls:
        try:
            out = _extract_code_frame(fetch_json(url), market, url)
            if not out.empty:
                log(f"{market} symbols loaded from {url}: {len(out)}")
                return out
        except Exception as e:
            log(f"{market} symbol source failed: {url} | {e}")
    return pd.DataFrame(columns=["symbol","name","market","source"])

def build_symbol_universe(limit_symbols=None) -> pd.DataFrame:
    existing = find_existing_symbol_candidates()
    if not existing.empty:
        log(f"Recovered existing symbol candidates from repo CSVs: {existing['symbol'].nunique()}")
    twse = fetch_symbols(TWSE_SYMBOL_URLS, "TWSE")
    tpex = fetch_symbols(TPEX_SYMBOL_URLS, "TPEX")
    parts = []
    if not existing.empty:
        tmp = existing.rename(columns={"market_hint":"market"})
        tmp["name"] = ""
        parts.append(tmp[["symbol","name","market","source"]])
    if not twse.empty:
        parts.append(twse[["symbol","name","market","source"]])
    if not tpex.empty:
        parts.append(tpex[["symbol","name","market","source"]])
    if not parts:
        raise RuntimeError("ç¡æ³å»ºç« symbol universeï¼repo å§æªæ¾å°å¯ç¨ä»£ç¢¼ï¼å®æ¹ä»£ç¢¼ä¾æºä¹æä¸å°")
    universe = pd.concat(parts, ignore_index=True)
    universe["symbol"] = universe["symbol"].astype(str).map(clean_code)
    universe = universe[universe["symbol"].map(is_taiwan_stock_code)].copy()
    def choose_market(s):
        vals = [str(x).upper().strip() for x in s if str(x).strip()]
        for pref in ["TWSE","TPEX"]:
            if pref in vals:
                return pref
        return vals[0] if vals else ""
    consolidated = universe.groupby("symbol", as_index=False).agg(
        name=("name", lambda s: next((x for x in s if str(x).strip()), "")),
        market=("market", choose_market),
        source=("source", lambda s: " | ".join(sorted({str(x) for x in s if str(x).strip()})[:3])),
    ).sort_values(["market","symbol"]).reset_index(drop=True)
    if limit_symbols:
        consolidated = consolidated.head(limit_symbols).copy()
    consolidated["yf_ticker"] = np.where(consolidated["market"].eq("TPEX"), consolidated["symbol"] + ".TWO", consolidated["symbol"] + ".TW")
    return consolidated

def batch_download_yfinance(tickers, start, end):
    if not tickers:
        return pd.DataFrame()
    data = yf.download(tickers=tickers, start=start, end=end, auto_adjust=False, actions=False, progress=False, group_by="ticker", threads=True)
    if data is None or len(data) == 0:
        return pd.DataFrame()
    frames = []
    if not isinstance(data.columns, pd.MultiIndex):
        ticker = tickers[0]
        tmp = data.reset_index().copy()
        tmp.columns = [str(c) for c in tmp.columns]
        tmp["ticker"] = ticker
        frames.append(tmp)
    else:
        for ticker in list(dict.fromkeys([c[0] for c in data.columns])):
            try:
                tmp = data[ticker].reset_index().copy()
                tmp.columns = [str(c) for c in tmp.columns]
                tmp["ticker"] = ticker
                frames.append(tmp)
            except Exception:
                pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def normalize_yf_frame(df, universe_map):
    if df.empty:
        return df
    col_map = {str(c).strip().lower(): c for c in df.columns}
    date_col = col_map.get("date") or col_map.get("datetime")
    if date_col is None:
        raise ValueError("yfinance output missing date column")
    def get_col(*names):
        for n in names:
            if n.lower() in col_map:
                return col_map[n.lower()]
        return None
    out = pd.DataFrame({
        "trade_date": pd.to_datetime(df[date_col]).dt.normalize(),
        "ticker": df[get_col("ticker")].astype(str),
        "open": pd.to_numeric(df[get_col("open")], errors="coerce") if get_col("open") else np.nan,
        "high": pd.to_numeric(df[get_col("high")], errors="coerce") if get_col("high") else np.nan,
        "low": pd.to_numeric(df[get_col("low")], errors="coerce") if get_col("low") else np.nan,
        "close": pd.to_numeric(df[get_col("close","adj close")], errors="coerce"),
        "volume": pd.to_numeric(df[get_col("volume")], errors="coerce").fillna(0) if get_col("volume") else 0,
    }).dropna(subset=["trade_date","close"])
    out["symbol"] = out["ticker"].map(lambda t: t.split(".")[0] if "." in t else str(t))
    out["market"] = out["ticker"].map(lambda t: universe_map.get(t, {}).get("market",""))
    return out[["trade_date","symbol","market","open","high","low","close","volume","ticker"]]

def save_checkpoint(df, idx):
    CHECKPOINT_DIR.mkdir(exist_ok=True, parents=True)
    p = CHECKPOINT_DIR / f"chunk_{idx:04d}.csv"
    df.to_csv(p, index=False)
    return p

def load_existing_checkpoints():
    if not CHECKPOINT_DIR.exists():
        return pd.DataFrame()
    parts = []
    for p in sorted(CHECKPOINT_DIR.glob("chunk_*.csv")):
        try:
            parts.append(pd.read_csv(p, parse_dates=["trade_date"]))
        except Exception:
            pass
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def infer_missing_market_and_retry(universe, start, end):
    uncertain = universe[universe["market"].fillna("").eq("")].copy()
    if uncertain.empty:
        return pd.DataFrame()
    rows = []
    for suffix in [".TW",".TWO"]:
        tickers = [s + suffix for s in uncertain["symbol"].tolist()]
        raw = batch_download_yfinance(tickers, start, end)
        if raw.empty:
            continue
        alt_map = {t: {"market": "TWSE" if t.endswith(".TW") else "TPEX"} for t in tickers}
        norm = normalize_yf_frame(raw, alt_map)
        if not norm.empty:
            rows.append(norm)
    if rows:
        out = pd.concat(rows, ignore_index=True)
        return out.sort_values(["symbol","trade_date"]).drop_duplicates(subset=["symbol","trade_date"], keep="last")
    return pd.DataFrame()

def build_panel(start, end, limit_symbols=None):
    universe = build_symbol_universe(limit_symbols=limit_symbols)
    universe.to_csv(OUTPUT_UNIVERSE, index=False)
    log(f"Universe saved: {OUTPUT_UNIVERSE} | symbols={len(universe)}")
    existing = load_existing_checkpoints()
    completed_symbols = set(existing["symbol"].astype(str).unique()) if not existing.empty else set()
    if completed_symbols:
        log(f"Loaded checkpoints | rows={len(existing)} symbols={len(completed_symbols)}")
    todo = universe[~universe["symbol"].isin(completed_symbols)].copy()
    universe_map = {row.yf_ticker: {"symbol": row.symbol, "market": row.market} for row in universe.itertuples(index=False)}
    chunks = math.ceil(max(len(todo), 1) / CHUNK_SIZE)
    all_parts = [existing] if not existing.empty else []
    for i in range(chunks):
        sub = todo.iloc[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE].copy()
        if sub.empty:
            continue
        tickers = sub["yf_ticker"].tolist()
        log(f"Downloading chunk {i+1}/{chunks} | tickers={len(tickers)}")
        try:
            raw = batch_download_yfinance(tickers, start, end)
            norm = normalize_yf_frame(raw, universe_map) if not raw.empty else pd.DataFrame()
        except Exception as e:
            log(f"Chunk failed {i+1}/{chunks}: {e}")
            norm = pd.DataFrame()
        if not norm.empty:
            save_checkpoint(norm, i+1)
            all_parts.append(norm)
            log(f"Chunk {i+1}/{chunks} saved | rows={len(norm)} | symbols={norm['symbol'].nunique()}")
        else:
            log(f"Chunk {i+1}/{chunks} returned empty")
        time.sleep(SLEEP_BETWEEN_CHUNKS)
    alt = infer_missing_market_and_retry(universe, start, end)
    if not alt.empty:
        all_parts.append(alt)
        log(f"Alternate market retry rows={len(alt)} symbols={alt['symbol'].nunique()}")
    if not all_parts:
        raise RuntimeError("å®å¨ç¡æ³ä¸è¼ä»»ä½å¹æ ¼è³æ")
    panel = pd.concat(all_parts, ignore_index=True)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.normalize()
    panel["symbol"] = panel["symbol"].astype(str).map(clean_code)
    panel = panel[panel["symbol"].map(is_taiwan_stock_code)].copy()
    for c in ["open","high","low","close","volume"]:
        panel[c] = pd.to_numeric(panel[c], errors="coerce")
    panel = panel.dropna(subset=["trade_date","symbol","close"])
    panel = panel[panel["close"] > 0].copy()
    panel["volume"] = panel["volume"].fillna(0)
    panel = panel.sort_values(["symbol","trade_date"]).drop_duplicates(subset=["symbol","trade_date"], keep="last")
    valid_counts = panel.groupby("symbol")["trade_date"].size()
    keep = valid_counts[valid_counts >= MIN_VALID_ROWS_PER_SYMBOL].index
    panel = panel[panel["symbol"].isin(keep)].copy()
    market_map = universe.set_index("symbol")["market"].to_dict()
    panel["market"] = panel["symbol"].map(market_map).fillna(panel.get("market",""))
    return panel, universe

def build_summary(panel, universe, start, end):
    if panel.empty:
        return pd.DataFrame([{"start": start, "end": end, "rows": 0, "symbols": 0, "min_date": "", "max_date": "", "twse_symbols": 0, "tpex_symbols": 0, "covers_target": False, "universe_symbols": int(len(universe))}])
    min_date = panel["trade_date"].min().date().isoformat()
    max_date = panel["trade_date"].max().date().isoformat()
    covers_target = (min_date <= start) and (max_date >= (pd.Timestamp(end) - pd.Timedelta(days=1)).date().isoformat())
    return pd.DataFrame([{
        "start": start,
        "end": end,
        "rows": int(len(panel)),
        "symbols": int(panel["symbol"].nunique()),
        "min_date": min_date,
        "max_date": max_date,
        "twse_symbols": int(panel.loc[panel["market"].eq("TWSE"), "symbol"].nunique()),
        "tpex_symbols": int(panel.loc[panel["market"].eq("TPEX"), "symbol"].nunique()),
        "covers_target": bool(covers_target),
        "universe_symbols": int(len(universe)),
    }])

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=(datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat())
    p.add_argument("--limit-symbols", type=int, default=None)
    return p.parse_args()

def main():
    if OUTPUT_LOG.exists():
        OUTPUT_LOG.unlink()
    args = parse_args()
    log("=== build_full_price_panel_10y start ===")
    log(f"start={args.start} end={args.end} limit_symbols={args.limit_symbols}")
    panel, universe = build_panel(args.start, args.end, args.limit_symbols)
    panel = panel.sort_values(["trade_date","symbol"]).reset_index(drop=True)
    panel.to_csv(OUTPUT_PANEL, index=False)
    summary = build_summary(panel, universe, args.start, args.end)
    summary.to_csv(OUTPUT_SUMMARY, index=False)
    log(f"Panel saved: {OUTPUT_PANEL} | rows={len(panel)} | symbols={panel['symbol'].nunique() if not panel.empty else 0}")
    log(f"Summary saved: {OUTPUT_SUMMARY}")
    log(summary.to_string(index=False))
    log("=== build_full_price_panel_10y done ===")

if __name__ == "__main__":
    main()
