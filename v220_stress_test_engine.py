# v220_stress_test_engine.py
# v220（壓力測試完整版）
# 功能：
# 1. 只使用已驗證的 B engine
# 2. 對 v216_positions.csv 做多種壓力測試
# 3. 輸出多區間 / 多模式 summary
# 4. 輸出最終比較與建議

from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

INITIAL_CAPITAL = 100000.0
INPUT_FILE = "v216_positions.csv"

# 測試模式
MODES = {
    "base_03_08": {"type": "two_level", "low": 0.3, "high": 0.8},
    "full_10": {"type": "fixed", "exposure": 1.0},
    "fixed_08": {"type": "fixed", "exposure": 0.8},
    "fixed_05": {"type": "fixed", "exposure": 0.5},
    "fixed_03": {"type": "fixed", "exposure": 0.3},
    "tri_02_05_08": {"type": "three_level", "low": 0.2, "mid": 0.5, "high": 0.8},
}

WINDOWS = [
    ("2022", "2022-01-01", "2022-12-31"),
    ("2023", "2023-01-01", "2023-12-31"),
    ("2024", "2024-01-01", "2024-12-31"),
    ("2025", "2025-01-01", "2025-12-31"),
]

def load_positions(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"找不到檔案: {path}")

    df = pd.read_csv(p)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["engine", "trade_date"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"缺少必要欄位: {c}")

    if "trade_ret" not in df.columns:
        df["trade_ret"] = 0.0

    if "weight_portfolio" not in df.columns:
        if "weight" in df.columns:
            df["weight_portfolio"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
        else:
            df["weight_portfolio"] = 0.0

    if "wret" not in df.columns:
        df["wret"] = (
            pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0.0)
            * pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0.0)
        )

    if "symbol" not in df.columns:
        df["symbol"] = "NA"

    df["engine"] = df["engine"].astype(str).str.strip()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["trade_ret"] = pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0.0)
    df["weight_portfolio"] = pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0.0)
    df["wret"] = pd.to_numeric(df["wret"], errors="coerce").fillna(0.0)

    df = df.dropna(subset=["trade_date"]).copy()
    df = df[df["engine"] == "B"].copy()
    df = df.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    return df


def sharpe_ratio(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").fillna(0.0)
    std = x.std(ddof=0)
    if std == 0 or np.isnan(std):
        return 0.0
    return float((x.mean() / std) * np.sqrt(252.0))


def max_drawdown(nav: pd.Series) -> float:
    nav = pd.to_numeric(nav, errors="coerce").ffill()
    if nav.empty:
        return 0.0
    return float((nav / nav.cummax() - 1.0).min())


def annualized_return(total_return: float, trading_days: int) -> float:
    if trading_days <= 0:
        return 0.0
    return float((1.0 + total_return) ** (252.0 / trading_days) - 1.0)


def build_b_daily(df_b: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df_b.groupby("trade_date", as_index=False)
        .agg(
            raw_ret=("wret", "sum"),
            holdings=("symbol", "nunique"),
            gross_exposure=("weight_portfolio", "sum"),
        )
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    daily["rolling_ret_10"] = daily["raw_ret"].rolling(10).sum()
    daily["rolling_ret_20"] = daily["raw_ret"].rolling(20).sum()
    return daily


def apply_mode(daily: pd.DataFrame, mode_name: str, cfg: Dict) -> pd.DataFrame:
    d = daily.copy()

    if cfg["type"] == "fixed":
        d["exposure"] = float(cfg["exposure"])

    elif cfg["type"] == "two_level":
        low = float(cfg["low"])
        high = float(cfg["high"])
        d["exposure"] = np.where(d["rolling_ret_10"] > 0, high, low)

    elif cfg["type"] == "three_level":
        low = float(cfg["low"])
        mid = float(cfg["mid"])
        high = float(cfg["high"])

        # 三段式：
        # rolling_ret_20 > 0 且 rolling_ret_10 > 0 => high
        # rolling_ret_20 > 0 且 rolling_ret_10 <= 0 => mid
        # rolling_ret_20 <= 0 => low
        d["exposure"] = np.where(
            (d["rolling_ret_20"] > 0) & (d["rolling_ret_10"] > 0), high,
            np.where(d["rolling_ret_20"] > 0, mid, low)
        )
    else:
        raise ValueError(f"未知 mode type: {cfg['type']}")

    d["adj_ret"] = d["raw_ret"] * d["exposure"]
    d["nav"] = INITIAL_CAPITAL * (1.0 + d["adj_ret"]).cumprod()
    d["cum_return"] = d["nav"] / INITIAL_CAPITAL - 1.0
    d["drawdown"] = d["nav"] / d["nav"].cummax() - 1.0
    d["mode"] = mode_name
    return d


def summarize(d: pd.DataFrame, mode_name: str, window_name: str = "full_sample") -> Dict:
    if d.empty:
        return {
            "mode": mode_name,
            "window": window_name,
            "start_date": "",
            "end_date": "",
            "return": 0.0,
            "ann_return": 0.0,
            "sharpe": 0.0,
            "mdd": 0.0,
            "trading_days": 0,
            "avg_holdings": 0.0,
            "avg_raw_exposure": 0.0,
            "avg_exposure": 0.0,
        }

    total_return = float(d["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0)
    trading_days = int(len(d))
    return {
        "mode": mode_name,
        "window": window_name,
        "start_date": d["trade_date"].min().date(),
        "end_date": d["trade_date"].max().date(),
        "return": total_return,
        "ann_return": annualized_return(total_return, trading_days),
        "sharpe": sharpe_ratio(d["adj_ret"]),
        "mdd": max_drawdown(d["nav"]),
        "trading_days": trading_days,
        "avg_holdings": float(d["holdings"].mean()),
        "avg_raw_exposure": float(d["gross_exposure"].mean()),
        "avg_exposure": float(d["exposure"].mean()),
    }


def score_row(r: pd.Series) -> float:
    # 偏重品質，其次才是報酬
    return float(r["sharpe"] * 2.0 + r["return"] - abs(r["mdd"]))


def judge(compare_df: pd.DataFrame) -> Tuple[str, str]:
    ranked = compare_df.sort_values(["score", "sharpe", "return"], ascending=False).reset_index(drop=True)
    winner = str(ranked.iloc[0]["mode"])

    if winner == "tri_02_05_08":
        decision = "USE_TRI_LEVEL"
    elif winner == "base_03_08":
        decision = "KEEP_TWO_LEVEL"
    elif winner == "full_10":
        decision = "FULL_EXPOSURE_OK"
    else:
        decision = f"USE_{winner.upper()}"

    return winner, decision


def main():
    df_b = load_positions(INPUT_FILE)
    daily_b = build_b_daily(df_b)

    all_daily = []
    compare_rows = []
    window_rows = []

    for mode_name, cfg in MODES.items():
        d = apply_mode(daily_b, mode_name, cfg)
        all_daily.append(d)

        # full summary
        compare_rows.append(summarize(d, mode_name, "full_sample"))

        # year windows
        for window_name, start, end in WINDOWS:
            sub = d[(d["trade_date"] >= start) & (d["trade_date"] <= end)].copy()
            if not sub.empty:
                window_rows.append(summarize(sub, mode_name, window_name))

    all_daily_df = pd.concat(all_daily, ignore_index=True)
    compare_df = pd.DataFrame(compare_rows)
    window_df = pd.DataFrame(window_rows)

    compare_df["score"] = compare_df.apply(score_row, axis=1)

    winner, decision = judge(compare_df)

    all_daily_df.to_csv("v220_all_daily.csv", index=False)
    compare_df.to_csv("v220_compare_summary.csv", index=False)
    window_df.to_csv("v220_window_summary.csv", index=False)

    with open("v220_decision.txt", "w", encoding="utf-8") as f:
        f.write("=== v220 Stress Test Decision ===\n")
        f.write(f"WINNER: {winner}\n")
        f.write(f"DECISION: {decision}\n\n")
        f.write("=== Compare Summary ===\n")
        f.write(compare_df.to_string(index=False))
        f.write("\n\n=== Window Summary ===\n")
        if not window_df.empty:
            f.write(window_df.to_string(index=False))
        else:
            f.write("No year-window data found in current sample.\n")

    print("==== v220 compare summary ====")
    print(compare_df.sort_values('score', ascending=False).to_string(index=False))
    print("\nWINNER:", winner)
    print("DECISION:", decision)


if __name__ == "__main__":
    main()
