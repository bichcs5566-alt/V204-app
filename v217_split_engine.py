# v217_split_engine.py
# 版本：v217（拆分驗證版）
# 說明：
# 1. 讀取含 engine 欄位的持股明細，例如 v216_positions.csv
# 2. 分別回測 A_only / B_only / AB
# 3. 產出各模式 summary、daily、trade_stats、window_summary
# 4. 主要用途：驗證 A、B、AB 到底誰是真 alpha

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

INITIAL_CAPITAL = 100000.0
DEFAULT_INPUT = "v216_positions.csv"
MODES = {
    "A_only": ["A"],
    "B_only": ["B"],
    "AB": ["A", "B"],
}


def safe_read_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"找不到檔案: {path}")
    return pd.read_csv(path)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = ["engine", "trade_date"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"缺少必要欄位: {col}")

    if "wret" not in df.columns:
        if "weight_portfolio" in df.columns and "trade_ret" in df.columns:
            df["wret"] = (
                pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0.0)
                * pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0.0)
            )
        elif "weight" in df.columns and "trade_ret" in df.columns:
            df["wret"] = (
                pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
                * pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0.0)
            )
        else:
            raise ValueError("缺少 wret，且無法由 weight_portfolio*trade_ret 或 weight*trade_ret 補出")

    if "weight_portfolio" not in df.columns:
        if "weight" in df.columns:
            df["weight_portfolio"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
        else:
            df["weight_portfolio"] = 0.0

    if "trade_ret" not in df.columns:
        df["trade_ret"] = 0.0

    if "symbol" not in df.columns:
        df["symbol"] = "NA"

    df["engine"] = df["engine"].astype(str).str.strip()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["trade_ret"] = pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0.0)
    df["weight_portfolio"] = pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0.0)
    df["wret"] = pd.to_numeric(df["wret"], errors="coerce").fillna(0.0)

    df = df.dropna(subset=["trade_date"]).copy()
    df = df.sort_values(["trade_date", "engine", "symbol"]).reset_index(drop=True)
    return df


def annualized_return(total_return: float, trading_days: int) -> float:
    if trading_days <= 0:
        return 0.0
    return float((1.0 + total_return) ** (252.0 / trading_days) - 1.0)


def sharpe_ratio(daily_ret: pd.Series) -> float:
    daily_ret = pd.to_numeric(daily_ret, errors="coerce").fillna(0.0)
    std = daily_ret.std(ddof=0)
    if std == 0 or np.isnan(std):
        return 0.0
    return float((daily_ret.mean() / std) * np.sqrt(252.0))


def max_drawdown(nav: pd.Series) -> float:
    nav = pd.to_numeric(nav, errors="coerce").ffill()
    if nav.empty:
        return 0.0
    roll_max = nav.cummax()
    dd = nav / roll_max - 1.0
    return float(dd.min())


def build_daily(df_mode: pd.DataFrame) -> pd.DataFrame:
    if df_mode.empty:
        return pd.DataFrame(columns=[
            "trade_date", "daily_ret", "holdings", "gross_exposure",
            "nav", "cum_return", "drawdown"
        ])

    daily = (
        df_mode.groupby("trade_date", as_index=False)
        .agg(
            daily_ret=("wret", "sum"),
            holdings=("symbol", "nunique"),
            gross_exposure=("weight_portfolio", "sum"),
        )
        .sort_values("trade_date")
        .reset_index(drop=True)
    )

    daily["nav"] = INITIAL_CAPITAL * (1.0 + daily["daily_ret"]).cumprod()
    daily["cum_return"] = daily["nav"] / INITIAL_CAPITAL - 1.0
    daily["drawdown"] = daily["nav"] / daily["nav"].cummax() - 1.0
    return daily


def build_trade_stats(df_mode: pd.DataFrame) -> pd.DataFrame:
    if df_mode.empty:
        return pd.DataFrame(columns=[
            "engine", "rows", "symbols", "avg_trade_ret",
            "win_rate", "loss_rate", "avg_wret"
        ])

    rows = []
    for engine, sub in df_mode.groupby("engine"):
        tr = pd.to_numeric(sub["trade_ret"], errors="coerce").fillna(0.0)
        wr = pd.to_numeric(sub["wret"], errors="coerce").fillna(0.0)
        rows.append({
            "engine": engine,
            "rows": int(len(sub)),
            "symbols": int(sub["symbol"].nunique()),
            "avg_trade_ret": float(tr.mean()) if len(tr) else 0.0,
            "win_rate": float((tr > 0).mean()) if len(tr) else 0.0,
            "loss_rate": float((tr < 0).mean()) if len(tr) else 0.0,
            "avg_wret": float(wr.mean()) if len(wr) else 0.0,
        })
    return pd.DataFrame(rows)


def build_summary(mode_name: str, engines: List[str], df_mode: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame([{
            "mode": mode_name,
            "engines": ",".join(engines),
            "start_date": pd.NaT,
            "end_date": pd.NaT,
            "initial_capital": INITIAL_CAPITAL,
            "final_nav": INITIAL_CAPITAL,
            "total_return": 0.0,
            "ann_return": 0.0,
            "trading_days": 0,
            "nonzero_ret_days": 0,
            "avg_holdings": 0.0,
            "avg_exposure": 0.0,
            "cash_days": 0,
            "sharpe": 0.0,
            "mdd": 0.0,
            "trade_rows": int(len(df_mode)),
        }])

    total_return = float(daily["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0)
    trading_days = int(len(daily))
    avg_exposure = float(daily["gross_exposure"].mean())
    cash_days = int((daily["gross_exposure"] < 0.5).sum())

    return pd.DataFrame([{
        "mode": mode_name,
        "engines": ",".join(engines),
        "start_date": daily["trade_date"].min().date(),
        "end_date": daily["trade_date"].max().date(),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": float(daily["nav"].iloc[-1]),
        "total_return": total_return,
        "ann_return": annualized_return(total_return, trading_days),
        "trading_days": trading_days,
        "nonzero_ret_days": int((daily["daily_ret"] != 0).sum()),
        "avg_holdings": float(daily["holdings"].mean()),
        "avg_exposure": avg_exposure,
        "cash_days": cash_days,
        "sharpe": sharpe_ratio(daily["daily_ret"]),
        "mdd": max_drawdown(daily["nav"]),
        "trade_rows": int(len(df_mode)),
    }])


def build_window_summary(mode_name: str, engines: List[str], daily: pd.DataFrame) -> pd.DataFrame:
    windows = {
        "full_sample": daily.copy(),
        "last_60d": daily.tail(60).copy(),
    }

    rows = []
    for window_name, sub in windows.items():
        if sub.empty:
            rows.append({
                "mode": mode_name,
                "engines": ",".join(engines),
                "window_name": window_name,
                "start_date": pd.NaT,
                "end_date": pd.NaT,
                "trading_days": 0,
                "nonzero_ret_days": 0,
                "avg_holdings": 0.0,
                "avg_gross_exposure": 0.0,
                "cash_days": 0,
                "initial_nav": INITIAL_CAPITAL,
                "final_nav": INITIAL_CAPITAL,
                "total_return": 0.0,
                "ann_return": 0.0,
                "sharpe": 0.0,
                "mdd": 0.0,
                "pass_sharpe_gt_1": False,
                "pass_mdd_gt_-0.15": True,
            })
            continue

        total_return = float(sub["nav"].iloc[-1] / sub["nav"].iloc[0] - 1.0) if len(sub) > 1 else float(sub["daily_ret"].iloc[0])
        shp = sharpe_ratio(sub["daily_ret"])
        mdd = max_drawdown(sub["nav"])

        rows.append({
            "mode": mode_name,
            "engines": ",".join(engines),
            "window_name": window_name,
            "start_date": sub["trade_date"].min().date(),
            "end_date": sub["trade_date"].max().date(),
            "trading_days": int(len(sub)),
            "nonzero_ret_days": int((sub["daily_ret"] != 0).sum()),
            "avg_holdings": float(sub["holdings"].mean()),
            "avg_gross_exposure": float(sub["gross_exposure"].mean()),
            "cash_days": int((sub["gross_exposure"] < 0.5).sum()),
            "initial_nav": float(sub["nav"].iloc[0]),
            "final_nav": float(sub["nav"].iloc[-1]),
            "total_return": total_return,
            "ann_return": annualized_return(total_return, int(len(sub))),
            "sharpe": shp,
            "mdd": mdd,
            "pass_sharpe_gt_1": bool(shp > 1.0),
            "pass_mdd_gt_-0.15": bool(mdd > -0.15),
        })

    return pd.DataFrame(rows)


def run_one_mode(mode_name: str, engines: List[str], df: pd.DataFrame, out_dir: Path) -> Dict[str, pd.DataFrame]:
    df_mode = df[df["engine"].isin(engines)].copy()
    daily = build_daily(df_mode)
    summary = build_summary(mode_name, engines, df_mode, daily)
    trade_stats = build_trade_stats(df_mode)
    window_summary = build_window_summary(mode_name, engines, daily)

    prefix = f"v217_{mode_name}"
    df_mode.to_csv(out_dir / f"{prefix}_positions.csv", index=False)
    daily.to_csv(out_dir / f"{prefix}_daily.csv", index=False)
    summary.to_csv(out_dir / f"{prefix}_summary.csv", index=False)
    trade_stats.to_csv(out_dir / f"{prefix}_trade_stats.csv", index=False)
    window_summary.to_csv(out_dir / f"{prefix}_window_summary.csv", index=False)

    return {
        "positions": df_mode,
        "daily": daily,
        "summary": summary,
        "trade_stats": trade_stats,
        "window_summary": window_summary,
    }


def build_compare_table(results: Dict[str, Dict[str, pd.DataFrame]], out_dir: Path) -> pd.DataFrame:
    rows = []
    for mode_name, pack in results.items():
        rows.append(pack["summary"].iloc[0].to_dict())
    comp = pd.DataFrame(rows)
    comp.to_csv(out_dir / "v217_compare_summary.csv", index=False)
    return comp


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT, help="來源持股檔，預設 v216_positions.csv")
    parser.add_argument("--outdir", default=".", help="輸出資料夾，預設目前目錄")
    args = parser.parse_args()

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = safe_read_csv(args.input)
    df = normalize_columns(raw)

    results: Dict[str, Dict[str, pd.DataFrame]] = {}
    for mode_name, engines in MODES.items():
        results[mode_name] = run_one_mode(mode_name, engines, df, out_dir)

    comp = build_compare_table(results, out_dir)

    print("=" * 80)
    print("v217 split engine validation done")
    print("=" * 80)
    print(comp[[
        "mode", "engines", "start_date", "end_date",
        "final_nav", "total_return", "ann_return",
        "trading_days", "sharpe", "mdd", "trade_rows"
    ]].to_string(index=False))


if __name__ == "__main__":
    main()
