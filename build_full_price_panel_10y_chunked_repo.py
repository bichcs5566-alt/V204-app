# build_full_price_panel_10y_chunked_repo.py
# 建立 10 年台股 price panel，切成多個 <100MB CSV 分片後提交到 repo
# 產出：
# - data/price_panel_parts/price_panel_daily_part_XXX.csv
# - data/price_panel_parts/manifest.json
# - build_price_panel_summary.csv
# - build_price_panel_log.txt
# - symbol_universe.csv

import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

ROOT = Path(".")
PARTS_DIR = ROOT / "data" / "price_panel_parts"
OUT_SUMMARY = ROOT / "build_price_panel_summary.csv"
OUT_LOG = ROOT / "build_price_panel_log.txt"
OUT_UNIVERSE = ROOT / "symbol_universe.csv"
OUT_MANIFEST = PARTS_DIR / "manifest.json"

START = os.getenv("PANEL_START", "2015-01-01")
END = os.getenv("PANEL_END", datetime.utcnow().strftime("%Y-%m-%d"))
LIMIT_SYMBOLS = os.getenv("LIMIT_SYMBOLS", "").strip()
LIMIT_SYMBOLS = int(LIMIT_SYMBOLS) if LIMIT_SYMBOLS else None

CHUNK_SIZE = 80
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CHUNKS = 1.5
MIN_VALID_ROWS_PER_SYMBOL = 20

# GitHub 單檔限制 100MB，保守抓 85MB
MAX_PART_BYTES = int(float(os.getenv("MAX_PART_MB", "85")) * 1024 * 1024)

TWSE_URLS = [
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_O",
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "https://openapi.twse.com.tw/v1/opendata/t187ap03_P",
]

TPEX_URLS = [
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    "https://www.tpex.org.tw/openapi/v1/mkt/stock_aftertrading/daily_close_quotes",
]

USER_AGENT = {"User-Agent": "Mozilla/5.0"}

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with OUT_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def clean_code(x) -> str:
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)
    return s

def safe_json_get(url: str):
    r = requests.get(url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        raise ValueError(f"Not JSON: {url}")

def find_existing_symbol_candidates() -> set:
    found = set()
    for p in ROOT.rglob("*.csv"):
        try:
            df = pd.read_csv(p, nrows=500)
        except Exception:
            continue
        cols = {str(c).lower().strip(): c for c in df.columns}
        if "symbol" in cols:
            raw = df[cols["symbol"]].astype(str).tolist()
            for x in raw:
                code = clean_code(x)
                if 4 <= len(code) <= 6:
                    found.add(code)
    return found

def load_twse_symbols() -> set:
    out = set()
    for url in TWSE_URLS:
        try:
            data = safe_json_get(url)
            if isinstance(data, list):
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    for v in list(row.values())[:3]:
                        code = clean_code(v)
                        if len(code) == 4:
                            out.add(code)
                if out:
                    log(f"TWSE symbols loaded from {url}: {len(out)}")
                    return out
        except Exception as e:
            log(f"TWSE symbol source failed: {url} | {e}")
    return out

def load_tpex_symbols() -> set:
    out = set()
    for url in TPEX_URLS:
        try:
            data = safe_json_get(url)
            if isinstance(data, list):
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    for v in list(row.values())[:3]:
                        code = clean_code(v)
                        if len(code) == 4:
                            out.add(code)
                if out:
                    log(f"TPEX symbols loaded from {url}: {len(out)}")
                    return out
        except Exception as e:
            log(f"TPEX symbol source failed: {url} | {e}")
    return out

def build_universe() -> pd.DataFrame:
    existing = find_existing_symbol_candidates()
    log(f"Recovered existing symbol candidates from repo CSVs: {len(existing)}")

    twse = load_twse_symbols()
    tpex = load_tpex_symbols()

    universe = sorted(existing | twse | tpex)
    df = pd.DataFrame({"symbol": universe})
    df["market_guess"] = np.where(
        df["symbol"].isin(sorted(twse)), "TWSE",
        np.where(df["symbol"].isin(sorted(tpex)), "TPEX", "UNKNOWN")
    )

    if LIMIT_SYMBOLS:
        df = df.head(LIMIT_SYMBOLS).copy()

    df.to_csv(OUT_UNIVERSE, index=False)
    log(f"Universe saved: {OUT_UNIVERSE.name} | symbols={len(df)}")
    return df

def to_yf_ticker(symbol: str, market_guess: str) -> str:
    return f"{symbol}.TWO" if market_guess == "TPEX" else f"{symbol}.TW"

def download_chunk(tickers):
    data = yf.download(
        tickers=tickers,
        start=START,
        end=END,
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    if data is None or len(data) == 0:
        return pd.DataFrame()

    frames = []
    if isinstance(data.columns, pd.MultiIndex):
        for t in tickers:
            if t not in data.columns.get_level_values(0):
                continue
            one = data[t].copy()
            if len(one) == 0:
                continue
            one = one.reset_index()
            one.columns = [str(c).lower().strip() for c in one.columns]
            if "date" not in one.columns or "close" not in one.columns:
                continue
            one["trade_date"] = pd.to_datetime(one["date"])
            one["symbol"] = t.split(".")[0]
            keep = [c for c in ["trade_date", "symbol", "open", "high", "low", "close", "adj close", "volume"] if c in one.columns]
            one = one[keep].copy()
            frames.append(one)
    else:
        one = data.reset_index().copy()
        one.columns = [str(c).lower().strip() for c in one.columns]
        if "date" in one.columns and "close" in one.columns and len(tickers) == 1:
            one["trade_date"] = pd.to_datetime(one["date"])
            one["symbol"] = tickers[0].split(".")[0]
            keep = [c for c in ["trade_date", "symbol", "open", "high", "low", "close", "adj close", "volume"] if c in one.columns]
            frames.append(one[keep].copy())

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out.columns = [c.replace("adj close", "adj_close") for c in out.columns]
    return out

def build_panel(universe_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    yf_tickers = [to_yf_ticker(r.symbol, r.market_guess) for r in universe_df.itertuples(index=False)]
    chunks = [yf_tickers[i:i+CHUNK_SIZE] for i in range(0, len(yf_tickers), CHUNK_SIZE)]

    for i, chunk in enumerate(chunks, start=1):
        log(f"Downloading chunk {i}/{len(chunks)} | tickers={len(chunk)}")
        try:
            df = download_chunk(chunk)
            if df.empty:
                log(f"Chunk {i}/{len(chunks)} returned empty")
            else:
                rows.append(df)
                log(f"Chunk {i}/{len(chunks)} saved | rows={len(df)} | symbols={df['symbol'].nunique()}")
        except Exception as e:
            log(f"Chunk {i}/{len(chunks)} failed | {e}")
        time.sleep(SLEEP_BETWEEN_CHUNKS)

    if not rows:
        raise ValueError("所有 chunk 都沒有抓到資料")

    panel = pd.concat(rows, ignore_index=True)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    panel["symbol"] = panel["symbol"].astype(str).str.strip()
    panel["close"] = pd.to_numeric(panel["close"], errors="coerce")
    if "volume" in panel.columns:
        panel["volume"] = pd.to_numeric(panel["volume"], errors="coerce")
    else:
        panel["volume"] = np.nan

    panel = panel.dropna(subset=["trade_date", "symbol", "close"])
    panel = panel[panel["close"] > 0].copy()

    counts = panel.groupby("symbol").size().rename("n").reset_index()
    good = set(counts.loc[counts["n"] >= MIN_VALID_ROWS_PER_SYMBOL, "symbol"])
    panel = panel[panel["symbol"].isin(good)].copy()

    panel = panel.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    log(f"Panel built in memory | rows={len(panel)} | symbols={panel['symbol'].nunique()}")
    return panel

def split_panel_to_parts(panel: pd.DataFrame) -> dict:
    if PARTS_DIR.exists():
        shutil.rmtree(PARTS_DIR)
    PARTS_DIR.mkdir(parents=True, exist_ok=True)

    sample_n = min(5000, len(panel))
    sample_bytes = len(panel.head(sample_n).to_csv(index=False).encode("utf-8"))
    bytes_per_row = max(sample_bytes / max(sample_n, 1), 1.0)
    rows_per_part = max(int(MAX_PART_BYTES / bytes_per_row), 10000)

    parts = []
    start_idx = 0
    part_no = 1

    while start_idx < len(panel):
        end_idx = min(start_idx + rows_per_part, len(panel))
        chunk = panel.iloc[start_idx:end_idx].copy()

        # 保守二分，確保不超過上限
        while True:
            data = chunk.to_csv(index=False).encode("utf-8")
            if len(data) <= MAX_PART_BYTES or len(chunk) <= 10000:
                break
            end_idx = start_idx + max(len(chunk) // 2, 10000)
            chunk = panel.iloc[start_idx:end_idx].copy()

        fname = f"price_panel_daily_part_{part_no:03d}.csv"
        fpath = PARTS_DIR / fname
        fpath.write_bytes(data)

        part_info = {
            "file": str(fpath.as_posix()),
            "rows": int(len(chunk)),
            "bytes": int(fpath.stat().st_size),
            "min_date": str(pd.to_datetime(chunk["trade_date"]).min().date()),
            "max_date": str(pd.to_datetime(chunk["trade_date"]).max().date()),
            "symbols": int(chunk["symbol"].nunique()),
        }
        parts.append(part_info)
        log(f"Saved {fname} | rows={part_info['rows']} | bytes={part_info['bytes']}")

        start_idx = end_idx
        part_no += 1

    manifest = {
        "created_at": datetime.utcnow().isoformat(),
        "start": START,
        "end": END,
        "part_count": len(parts),
        "total_rows": int(len(panel)),
        "total_symbols": int(panel["symbol"].nunique()),
        "parts": parts,
    }
    OUT_MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Manifest saved: {OUT_MANIFEST.as_posix()} | parts={len(parts)}")
    return manifest

def build_summary(panel: pd.DataFrame, universe_df: pd.DataFrame, manifest: dict) -> None:
    twse_n = int((universe_df["market_guess"] == "TWSE").sum())
    tpex_n = int((universe_df["market_guess"] == "TPEX").sum())
    covers_target = (
        pd.Timestamp("2022-01-03") >= panel["trade_date"].min()
        and pd.Timestamp("2025-12-31") <= panel["trade_date"].max()
    )

    summary = pd.DataFrame([{
        "start": START,
        "end": END,
        "rows": int(len(panel)),
        "symbols": int(panel["symbol"].nunique()),
        "min_date": str(panel["trade_date"].min().date()),
        "max_date": str(panel["trade_date"].max().date()),
        "twse_symbols": twse_n,
        "tpex_symbols": tpex_n,
        "covers_target": bool(covers_target),
        "universe_symbols": int(len(universe_df)),
        "part_count": int(manifest["part_count"]),
    }])
    summary.to_csv(OUT_SUMMARY, index=False)
    log(f"Summary saved: {OUT_SUMMARY.name}")
    log(summary.to_string(index=False))

def main():
    if OUT_LOG.exists():
        OUT_LOG.unlink()

    log("=== build_full_price_panel_10y_chunked_repo start ===")
    log(f"start={START} end={END} limit_symbols={LIMIT_SYMBOLS} max_part_bytes={MAX_PART_BYTES}")

    universe_df = build_universe()
    panel = build_panel(universe_df)
    manifest = split_panel_to_parts(panel)
    build_summary(panel, universe_df, manifest)

    log("=== build_full_price_panel_10y_chunked_repo done ===")

if __name__ == "__main__":
    main()
