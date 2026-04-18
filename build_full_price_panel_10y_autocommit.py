# build_full_price_panel_10y_autocommit.py
# 建立 10 年台股資料後，自動 commit 回 repo（預設直接推目前 branch）
# 若 push 失敗，會輸出較清楚訊息，方便改成 PR 流程

import os
import re
import time
import json
import subprocess
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

ROOT = Path(".")
OUT_PANEL = ROOT / "price_panel_daily.csv"
OUT_SUMMARY = ROOT / "build_price_panel_summary.csv"
OUT_LOG = ROOT / "build_price_panel_log.txt"
OUT_UNIVERSE = ROOT / "symbol_universe.csv"

START = os.getenv("PANEL_START", "2015-01-01")
END = os.getenv("PANEL_END", datetime.utcnow().strftime("%Y-%m-%d"))
LIMIT_SYMBOLS = os.getenv("LIMIT_SYMBOLS", "").strip()
LIMIT_SYMBOLS = int(LIMIT_SYMBOLS) if LIMIT_SYMBOLS else None

CHUNK_SIZE = 80
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CHUNKS = 1.5
MIN_VALID_ROWS_PER_SYMBOL = 20

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

def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    print(f"$ {cmd}", flush=True)
    return subprocess.run(cmd, shell=True, text=True, check=check)

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
                    vals = list(row.values())
                    for v in vals[:3]:
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
                    vals = list(row.values())
                    for v in vals[:3]:
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
    df["market_guess"] = np.where(df["symbol"].isin(sorted(twse)), "TWSE",
                          np.where(df["symbol"].isin(sorted(tpex)), "TPEX", "UNKNOWN"))

    if LIMIT_SYMBOLS:
        df = df.head(LIMIT_SYMBOLS).copy()

    df.to_csv(OUT_UNIVERSE, index=False)
    log(f"Universe saved: {OUT_UNIVERSE.name} | symbols={len(df)}")
    return df

def to_yf_ticker(symbol: str, market_guess: str) -> str:
    if market_guess == "TPEX":
        return f"{symbol}.TWO"
    return f"{symbol}.TW"

def download_chunk(tickers: list[str]) -> pd.DataFrame:
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
    panel.to_csv(OUT_PANEL, index=False)
    log(f"Panel saved: {OUT_PANEL.name} | rows={len(panel)} | symbols={panel['symbol'].nunique()}")
    return panel

def build_summary(panel: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
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
    }])
    summary.to_csv(OUT_SUMMARY, index=False)
    log(f"Summary saved: {OUT_SUMMARY.name}")
    log(summary.to_string(index=False))
    return summary

def commit_back_to_repo():
    actor = os.getenv("GITHUB_ACTOR", "github-actions[bot]")
    email = f"{actor}@users.noreply.github.com"
    branch = os.getenv("GITHUB_REF_NAME", "main")

    run(f'git config user.name "{actor}"')
    run(f'git config user.email "{email}"')
    run("git status --short", check=False)
    run(f"git add {OUT_PANEL.name} {OUT_SUMMARY.name} {OUT_LOG.name} {OUT_UNIVERSE.name}")

    diff = subprocess.run("git diff --cached --quiet", shell=True)
    if diff.returncode == 0:
        log("沒有檔案變更，略過 commit / push。")
        return

    run('git commit -m "auto update full price panel 10y"')
    try:
        run(f"git push origin HEAD:{branch}")
        log(f"已成功 push 回 repo branch: {branch}")
    except subprocess.CalledProcessError as e:
        log("git push 失敗。常見原因：")
        log("1) main branch protection")
        log("2) Actions 權限不是 Read and write")
        log("3) repo 不允許 workflow 直接 push")
        raise e

def main():
    if OUT_LOG.exists():
        OUT_LOG.unlink()

    log("=== build_full_price_panel_10y_autocommit start ===")
    log(f"start={START} end={END} limit_symbols={LIMIT_SYMBOLS}")

    universe_df = build_universe()
    panel = build_panel(universe_df)
    build_summary(panel, universe_df)
    commit_back_to_repo()

    log("=== build_full_price_panel_10y_autocommit done ===")

if __name__ == "__main__":
    main()
