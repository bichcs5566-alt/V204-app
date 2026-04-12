#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v216.1 權重引擎驗證版
目的：
1. 讀取 v216 產生的逐筆持倉資料
2. 依不同時間區間做穩定性驗證
3. 輸出 summary / window_summary / daily_nav / trade_stats
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent

INPUT_CANDIDATES = [
    "v216_positions.csv",
    "v216_positions.txt",
    "positions.csv",
]

OUTPUT_WINDOW = ROOT / "v2161_window_summary.csv"
OUTPUT_DAILY = ROOT / "v2161_daily_nav.csv"
OUTPUT_TRADES = ROOT / "v2161_trade_stats.csv"
OUTPUT_SUMMARY = ROOT / "v2161_summary.csv"


def find_input_file() -> Path:
    for name in INPUT_CANDIDATES:
        path = ROOT / name
        if path.exists():
            return path
    raise FileNotFoundError(
        "找不到輸入檔。請把 v216 產出的持倉檔放在同資料夾，檔名建議為 v216_positions.csv"
    )


def safe_to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def annualized_return(gross_nav: float, periods: int, periods_per_year: int = 252) -> float:
    if periods <= 0 or pd.isna(gross_nav) or gross_nav <= 0:
        return np.nan
    years = periods / periods_per_year
    if years <= 0:
        return np.nan
    return gross_nav ** (1 / years) - 1


def sharpe_ratio(rets: pd.Series, periods_per_year: int = 252) -> float:
    rets = pd.to_numeric(rets, errors="coerce").dropna()
    if len(rets) < 2:
        return np.nan
    std = rets.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return (rets.mean() / std) * math.sqrt(periods_per_year)


def max_drawdown(nav: pd.Series) -> float:
    nav = pd.to_numeric(nav, errors="coerce").dropna()
    if nav.empty:
        return np.nan
    peak = nav.cummax()
    dd = nav / peak - 1.0
    return dd.min()


def build_daily_nav(df: pd.DataFrame, initial_capital: float = 100000.0) -> pd.DataFrame:
    required = ["trade_date", "wret"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"缺少必要欄位: {missing}")

    agg_map = {"daily_ret": ("wret", "sum")}
    agg_map["holdings"] = ("symbol", "nunique") if "symbol" in df.columns else ("wret", "size")
    agg_map["gross_exposure"] = ("weight_portfolio", "sum") if "weight_portfolio" in df.columns else ("wret", "size")
    agg_map["cash_mode"] = ("cash_mode", "max") if "cash_mode" in df.columns else ("wret", lambda x: False)

    daily = (
        df.groupby("trade_date", as_index=False)
        .agg(**agg_map)
        .sort_values("trade_date")
        .reset_index(drop=True)
    )

    daily["nav"] = (1.0 + daily["daily_ret"].fillna(0)).cumprod() * initial_capital
    daily["cum_return"] = daily["nav"] / initial_capital - 1.0
    daily["drawdown"] = daily["nav"] / daily["nav"].cummax() - 1.0
    return daily


def trade_level_stats(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        c for c in [
            "engine", "signal_date", "trade_date", "symbol", "market",
            "mom_5d", "std_5d", "trade_ret", "weight_engine",
            "weight_portfolio", "wret"
        ] if c in df.columns
    ]
    trades = df[cols].copy()

    if "trade_ret" in trades.columns:
        trades["is_win"] = trades["trade_ret"] > 0
        trades["is_loss"] = trades["trade_ret"] < 0
    else:
        trades["is_win"] = False
        trades["is_loss"] = False

    if "engine" in trades.columns:
        stats = (
            trades.groupby("engine", as_index=False)
            .agg(
                rows=("engine", "size"),
                symbols=("symbol", "nunique") if "symbol" in trades.columns else ("engine", "size"),
                avg_trade_ret=("trade_ret", "mean") if "trade_ret" in trades.columns else ("engine", lambda x: np.nan),
                win_rate=("is_win", "mean"),
                loss_rate=("is_loss", "mean"),
                avg_wret=("wret", "mean") if "wret" in trades.columns else ("engine", lambda x: np.nan),
            )
            .sort_values("engine")
            .reset_index(drop=True)
        )
    else:
        stats = pd.DataFrame([{
            "engine": "ALL",
            "rows": len(trades),
            "symbols": trades["symbol"].nunique() if "symbol" in trades.columns else len(trades),
            "avg_trade_ret": trades["trade_ret"].mean() if "trade_ret" in trades.columns else np.nan,
            "win_rate": trades["is_win"].mean() if len(trades) else np.nan,
            "loss_rate": trades["is_loss"].mean() if len(trades) else np.nan,
            "avg_wret": trades["wret"].mean() if "wret" in trades.columns else np.nan,
        }])

    return stats


def make_windows(min_date: pd.Timestamp, max_date: pd.Timestamp):
    windows = []
    preset = [
        ("2022_bear", pd.Timestamp("2022-01-01"), pd.Timestamp("2022-12-31")),
        ("2023_range", pd.Timestamp("2023-01-01"), pd.Timestamp("2023-12-31")),
        ("2024_bull", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")),
        ("full_sample", min_date, max_date),
    ]
    for name, s, e in preset:
        if e >= min_date and s <= max_date:
            windows.append((name, max(s, min_date), min(e, max_date)))

    windows.append(("last_60d", max_date - pd.Timedelta(days=90), max_date))
    return windows


def evaluate_windows(daily: pd.DataFrame) -> pd.DataFrame:
    min_date = daily["trade_date"].min()
    max_date = daily["trade_date"].max()
    windows = make_windows(min_date, max_date)
    rows = []

    for name, start, end in windows:
        part = daily[(daily["trade_date"] >= start) & (daily["trade_date"] <= end)].copy()
        part = part.sort_values("trade_date")
        if part.empty:
            continue

        first_ret = float(part["daily_ret"].iloc[0]) if len(part) else 0.0
        initial_nav = float(part["nav"].iloc[0] / (1 + first_ret)) if (1 + first_ret) != 0 else np.nan
        final_nav = float(part["nav"].iloc[-1])
        total_return = final_nav / initial_nav - 1 if initial_nav and initial_nav > 0 else np.nan
        sr = sharpe_ratio(part["daily_ret"])
        mdd = max_drawdown(part["nav"])

        rows.append({
            "window_name": name,
            "start_date": part["trade_date"].min().date().isoformat(),
            "end_date": part["trade_date"].max().date().isoformat(),
            "trading_days": len(part),
            "nonzero_ret_days": int((part["daily_ret"].abs() > 0).sum()),
            "avg_holdings": float(part["holdings"].mean()),
            "avg_gross_exposure": float(part["gross_exposure"].mean()),
            "cash_days": int(part["cash_mode"].fillna(False).astype(bool).sum()),
            "initial_nav": initial_nav,
            "final_nav": final_nav,
            "total_return": total_return,
            "ann_return": annualized_return(
                final_nav / initial_nav if initial_nav and initial_nav > 0 else np.nan,
                len(part)
            ),
            "sharpe": sr,
            "mdd": mdd,
            "pass_sharpe_gt_1": bool(pd.notna(sr) and sr > 1),
            "pass_mdd_gt_-0.15": bool(pd.notna(mdd) and mdd > -0.15),
        })

    return pd.DataFrame(rows)


def overall_summary(
    daily: pd.DataFrame,
    windows: pd.DataFrame,
    trades: pd.DataFrame,
    initial_capital: float = 100000.0
) -> pd.DataFrame:
    final_nav = float(daily["nav"].iloc[-1]) if not daily.empty else np.nan
    total_return = final_nav / initial_capital - 1 if initial_capital > 0 and pd.notna(final_nav) else np.nan

    row = {
        "start_date": daily["trade_date"].min().date().isoformat() if not daily.empty else "",
        "end_date": daily["trade_date"].max().date().isoformat() if not daily.empty else "",
        "initial_capital": initial_capital,
        "final_nav": final_nav,
        "total_return": total_return,
        "trading_days": int(len(daily)),
        "nonzero_ret_days": int((daily["daily_ret"].abs() > 0).sum()) if not daily.empty else 0,
        "avg_holdings": float(daily["holdings"].mean()) if not daily.empty else np.nan,
        "avg_exposure": float(daily["gross_exposure"].mean()) if not daily.empty else np.nan,
        "cash_days": int(daily["cash_mode"].fillna(False).astype(bool).sum()) if not daily.empty else 0,
        "sharpe": sharpe_ratio(daily["daily_ret"]) if not daily.empty else np.nan,
        "mdd": max_drawdown(daily["nav"]) if not daily.empty else np.nan,
        "trade_rows": int(trades["rows"].sum()) if not trades.empty and "rows" in trades.columns else 0,
        "engines": ",".join(sorted(trades["engine"].astype(str).unique())) if not trades.empty and "engine" in trades.columns else "ALL",
        "windows_tested": int(len(windows)),
        "windows_pass_sharpe": int(windows["pass_sharpe_gt_1"].sum()) if not windows.empty else 0,
        "windows_pass_mdd": int(windows["pass_mdd_gt_-0.15"].sum()) if not windows.empty else 0,
    }
    return pd.DataFrame([row])


def main() -> None:
    input_path = find_input_file()
    df = pd.read_csv(input_path)

    if "trade_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    if "wret" not in df.columns and {"trade_ret", "weight_portfolio"}.issubset(df.columns):
        df["wret"] = (
            pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0)
            * pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0)
        )

    df["trade_date"] = safe_to_datetime(df["trade_date"])
    if "signal_date" in df.columns:
        df["signal_date"] = safe_to_datetime(df["signal_date"])

    df = df.dropna(subset=["trade_date"]).copy()
    sort_cols = ["trade_date"] + [c for c in ["engine", "symbol"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    daily = build_daily_nav(df)
    windows = evaluate_windows(daily)
    trades = trade_level_stats(df)
    summary = overall_summary(daily, windows, trades)

    daily_out = daily.copy()
    daily_out["trade_date"] = daily_out["trade_date"].dt.date.astype(str)

    daily_out.to_csv(OUTPUT_DAILY, index=False, encoding="utf-8-sig")
    windows.to_csv(OUTPUT_WINDOW, index=False, encoding="utf-8-sig")
    trades.to_csv(OUTPUT_TRADES, index=False, encoding="utf-8-sig")
    summary.to_csv(OUTPUT_SUMMARY, index=False, encoding="utf-8-sig")

    print(f"input_file: {input_path.name}")
    print(f"daily_rows: {len(daily_out)}")
    print(f"window_rows: {len(windows)}")
    print(f"trade_stats_rows: {len(trades)}")
    print(f"summary_rows: {len(summary)}")
    print("done")


if __name__ == "__main__":
    main()
