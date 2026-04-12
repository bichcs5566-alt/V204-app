#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v216_weight_engine.py

用途
----
以「前一交易日產生訊號、下一交易日進場」的方式，建立 v216 權重引擎版組合回測。

核心改版
--------
1. 不再平均配置全部股票
2. 採用 Top N 濃縮
3. 採用排名加權（平方放大）
4. 支援 A / B 雙引擎權重
5. 支援現金模式（市場不好時降倉）
6. 嚴格無偷看：signal_date -> trade_date

預期輸入檔
----------
放在 repo 根目錄：
- price_panel.csv
    必要欄位至少包含：
    [symbol, date, close]
    可選欄位：
    [market]

輸出檔
------
- v216_positions.csv
- v216_daily_nav.csv
- v216_signal_log.csv
- v216_summary.csv
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# =========================
# 路徑設定
# =========================
BASE_DIR = Path(".")
PRICE_PANEL_PATH = BASE_DIR / "price_panel.csv"

POSITIONS_OUT = BASE_DIR / "v216_positions.csv"
DAILY_NAV_OUT = BASE_DIR / "v216_daily_nav.csv"
SIGNAL_LOG_OUT = BASE_DIR / "v216_signal_log.csv"
SUMMARY_OUT = BASE_DIR / "v216_summary.csv"


# =========================
# 參數設定
# =========================
INITIAL_CAPITAL = 100000.0

# A / B 引擎名額
A_TOP_N = 8
B_TOP_N = 4

# A / B 引擎總資金比重（剩下保留現金）
ENGINE_A_CAP = 0.60
ENGINE_B_CAP = 0.20
MAX_GROSS_EXPOSURE = 1.00

# 權重模式
# 可選: "linear", "square"
WEIGHT_MODE = "square"

# 市場濾網
USE_MARKET_FILTER = True
MARKET_MA_WINDOW = 20

# 現金模式設定
REDUCED_RISK_EXPOSURE = 0.35
CASH_IF_BAD_MARKET = True

# A 引擎：偏強勢動能
A_MIN_MOM_5D = 0.03
A_MAX_STD_5D = 0.08

# B 引擎：偏穩健低波動 + 正報酬
B_MIN_MOM_5D = 0.01
B_MAX_STD_5D = 0.05

# 避免太小價格或異常值
MIN_PRICE = 5.0

# 嚴格：signal_date 下一個交易日才 trade
STRICT_NEXT_TRADING_DAY = True


# =========================
# 工具函式
# =========================
def require_columns(df: pd.DataFrame, cols: List[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} 缺少必要欄位: {missing}")


def safe_read_price_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"找不到檔案: {path}")

    df = pd.read_csv(path)
    require_columns(df, ["symbol", "date", "close"], "price_panel.csv")

    df = df.copy()
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["symbol", "date", "close"])
    df = df[df["close"] > 0].sort_values(["symbol", "date"]).reset_index(drop=True)

    if "market" not in df.columns:
        df["market"] = "UNKNOWN"

    return df


def build_market_proxy(price: pd.DataFrame) -> pd.DataFrame:
    """
    若沒有大盤指數資料，就用全市場每日平均 close 當 proxy。
    """
    proxy = (
        price.groupby("date", as_index=False)["close"]
        .mean()
        .rename(columns={"close": "market_close"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    proxy["market_ma"] = proxy["market_close"].rolling(MARKET_MA_WINDOW, min_periods=MARKET_MA_WINDOW).mean()
    proxy["risk_on"] = proxy["market_close"] >= proxy["market_ma"]
    proxy["risk_on"] = proxy["risk_on"].fillna(False)
    return proxy


def add_features(price: pd.DataFrame) -> pd.DataFrame:
    df = price.copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    g = df.groupby("symbol", group_keys=False)
    df["ret_1d"] = g["close"].pct_change()
    df["mom_5d"] = g["close"].pct_change(5)
    df["std_5d"] = g["ret_1d"].rolling(5).std().reset_index(level=0, drop=True)
    df["next_close"] = g["close"].shift(-1)
    df["trade_ret"] = df["next_close"] / df["close"] - 1.0

    return df


def next_trade_mapping(all_dates: List[pd.Timestamp]) -> Dict[pd.Timestamp, pd.Timestamp]:
    mapping: Dict[pd.Timestamp, pd.Timestamp] = {}
    for i in range(len(all_dates) - 1):
        mapping[all_dates[i]] = all_dates[i + 1]
    return mapping


def rank_to_weight(scores: pd.Series, mode: str = "square") -> pd.Series:
    s = scores.clip(lower=0).astype(float)

    if s.sum() <= 0:
        return pd.Series(np.zeros(len(s)), index=s.index)

    if mode == "linear":
        raw = s
    elif mode == "square":
        raw = s ** 2
    else:
        raise ValueError(f"不支援的 WEIGHT_MODE: {mode}")

    total = raw.sum()
    if total <= 0:
        return pd.Series(np.zeros(len(raw)), index=raw.index)

    return raw / total


def build_a_candidates(day_df: pd.DataFrame) -> pd.DataFrame:
    """
    A 引擎：高動能、波動不過大。
    """
    x = day_df.copy()
    x = x[
        (x["close"] >= MIN_PRICE) &
        (x["mom_5d"] >= A_MIN_MOM_5D) &
        (x["std_5d"] <= A_MAX_STD_5D)
    ].copy()

    if x.empty:
        return x

    x["score"] = x["mom_5d"] / (x["std_5d"] + 1e-9)
    x = x.sort_values(["score", "mom_5d"], ascending=[False, False]).head(A_TOP_N).copy()
    return x


def build_b_candidates(day_df: pd.DataFrame) -> pd.DataFrame:
    """
    B 引擎：低波動、正動能、較穩定。
    """
    x = day_df.copy()
    x = x[
        (x["close"] >= MIN_PRICE) &
        (x["mom_5d"] >= B_MIN_MOM_5D) &
        (x["std_5d"] <= B_MAX_STD_5D)
    ].copy()

    if x.empty:
        return x

    # B 偏穩健：動能 / 波動，但更強調低波
    x["score"] = x["mom_5d"] / ((x["std_5d"] + 1e-9) ** 1.5)
    x = x.sort_values(["score", "mom_5d"], ascending=[False, False]).head(B_TOP_N).copy()
    return x


def allocate_engine(
    candidates: pd.DataFrame,
    engine_name: str,
    engine_cap: float,
    weight_mode: str,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()

    out = candidates.copy().reset_index(drop=True)
    out["engine"] = engine_name

    score_clean = out["score"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    min_positive = float(score_clean[score_clean > 0].min()) if (score_clean > 0).any() else 1e-9
    out["score_pos"] = score_clean.clip(lower=min_positive)

    out["weight_engine"] = rank_to_weight(out["score_pos"], mode=weight_mode)
    out["weight_portfolio"] = out["weight_engine"] * engine_cap
    return out


def apply_market_filter(exposure: float, risk_on: bool) -> Tuple[float, bool]:
    if not USE_MARKET_FILTER:
        return exposure, False

    if risk_on:
        return exposure, False

    if CASH_IF_BAD_MARKET:
        return min(exposure, REDUCED_RISK_EXPOSURE), True

    return exposure, False


def summarize_daily_nav(nav: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    if nav.empty:
        return pd.DataFrame([{
            "start_date": np.nan,
            "end_date": np.nan,
            "initial_capital": initial_capital,
            "final_nav": initial_capital,
            "total_return": 0.0,
            "trading_days": 0,
            "nonzero_ret_days": 0,
            "avg_holdings": 0.0,
            "avg_exposure": 0.0,
            "cash_days": 0,
            "sharpe": np.nan,
            "mdd": 0.0,
        }])

    x = nav.copy().sort_values("trade_date").reset_index(drop=True)
    x["cummax"] = x["nav"].cummax()
    x["drawdown"] = x["nav"] / x["cummax"] - 1.0

    daily_ret = x["daily_ret"].fillna(0.0)
    mean_ret = daily_ret.mean()
    std_ret = daily_ret.std(ddof=0)

    sharpe = np.nan
    if std_ret > 0:
        sharpe = mean_ret / std_ret * math.sqrt(252)

    return pd.DataFrame([{
        "start_date": str(x["trade_date"].min().date()),
        "end_date": str(x["trade_date"].max().date()),
        "initial_capital": initial_capital,
        "final_nav": float(x["nav"].iloc[-1]),
        "total_return": float(x["nav"].iloc[-1] / initial_capital - 1.0),
        "trading_days": int(len(x)),
        "nonzero_ret_days": int((daily_ret.abs() > 0).sum()),
        "avg_holdings": float(x["total_count"].mean()),
        "avg_exposure": float(x["gross_exposure"].mean()),
        "cash_days": int(x["cash_mode"].sum()),
        "sharpe": float(sharpe) if pd.notna(sharpe) else np.nan,
        "mdd": float(x["drawdown"].min()),
    }])


# =========================
# 主流程
# =========================
def main() -> None:
    price = safe_read_price_panel(PRICE_PANEL_PATH)
    feat = add_features(price)

    market_proxy = build_market_proxy(price)
    market_risk_map = dict(zip(market_proxy["date"], market_proxy["risk_on"]))

    all_signal_dates = sorted(feat["date"].drop_duplicates().tolist())
    next_map = next_trade_mapping(all_signal_dates)

    positions_list = []
    signal_log_list = []
    daily_nav_list = []

    capital = INITIAL_CAPITAL

    for signal_date in all_signal_dates:
        if signal_date not in next_map:
            continue

        trade_date = next_map[signal_date] if STRICT_NEXT_TRADING_DAY else signal_date

        day_df = feat[feat["date"] == signal_date].copy()
        day_df = day_df.dropna(subset=["trade_ret", "mom_5d", "std_5d"]).copy()

        if day_df.empty:
            continue

        # A / B 候選
        a_cand = build_a_candidates(day_df)
        b_cand = build_b_candidates(day_df)

        # A / B 配權
        a_alloc = allocate_engine(a_cand, "A", ENGINE_A_CAP, WEIGHT_MODE)
        b_alloc = allocate_engine(b_cand, "B", ENGINE_B_CAP, WEIGHT_MODE)

        selected = pd.concat([a_alloc, b_alloc], ignore_index=True)

        # 市場濾網 + 現金模式
        risk_on = bool(market_risk_map.get(signal_date, False))
        planned_exposure = float(selected["weight_portfolio"].sum()) if not selected.empty else 0.0
        target_exposure = min(planned_exposure, MAX_GROSS_EXPOSURE)
        target_exposure, cash_mode = apply_market_filter(target_exposure, risk_on)

        # 沒有部位 => 現金
        if selected.empty or target_exposure <= 0:
            daily_ret = 0.0
            capital = capital * (1.0 + daily_ret)

            daily_nav_list.append({
                "signal_date": signal_date,
                "trade_date": trade_date,
                "market_rows": int(len(day_df)),
                "a_count": 0,
                "b_count": 0,
                "total_count": 0,
                "gross_exposure": 0.0,
                "daily_ret": daily_ret,
                "nav": capital,
                "cash_mode": True,
            })

            signal_log_list.append({
                "signal_date": signal_date,
                "trade_date": trade_date,
                "market_rows": int(len(day_df)),
                "a_count": 0,
                "b_count": 0,
                "total_count": 0,
                "gross_exposure": 0.0,
                "avg_trade_ret": np.nan,
                "risk_on": risk_on,
                "cash_mode": True,
            })
            continue

        # Normalize 成 target exposure
        selected = selected.copy()
        base_sum = selected["weight_portfolio"].sum()
        if base_sum > 0:
            selected["weight_portfolio"] = selected["weight_portfolio"] / base_sum * target_exposure
        else:
            selected["weight_portfolio"] = 0.0

        # 當日組合報酬
        selected["wret"] = selected["trade_ret"] * selected["weight_portfolio"]
        portfolio_ret = float(selected["wret"].sum())

        capital = capital * (1.0 + portfolio_ret)

        selected["signal_date"] = signal_date
        selected["trade_date"] = trade_date
        selected["capital_after_trade"] = capital
        selected["cash_mode"] = cash_mode

        positions_list.append(selected[[
            "engine",
            "signal_date",
            "trade_date",
            "symbol",
            "market",
            "close",
            "mom_5d",
            "std_5d",
            "trade_ret",
            "weight_engine",
            "weight_portfolio",
            "wret",
            "capital_after_trade",
            "cash_mode",
        ]].rename(columns={"close": "signal_close"}))

        signal_log_list.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "market_rows": int(len(day_df)),
            "a_count": int(len(a_alloc)),
            "b_count": int(len(b_alloc)),
            "total_count": int(len(selected)),
            "gross_exposure": float(selected["weight_portfolio"].sum()),
            "avg_trade_ret": float(selected["trade_ret"].mean()),
            "risk_on": risk_on,
            "cash_mode": cash_mode,
        })

        daily_nav_list.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "market_rows": int(len(day_df)),
            "a_count": int(len(a_alloc)),
            "b_count": int(len(b_alloc)),
            "total_count": int(len(selected)),
            "gross_exposure": float(selected["weight_portfolio"].sum()),
            "daily_ret": portfolio_ret,
            "nav": capital,
            "cash_mode": cash_mode,
        })

    positions = pd.concat(positions_list, ignore_index=True) if positions_list else pd.DataFrame(columns=[
        "engine", "signal_date", "trade_date", "symbol", "market", "signal_close",
        "mom_5d", "std_5d", "trade_ret", "weight_engine", "weight_portfolio", "wret",
        "capital_after_trade", "cash_mode"
    ])

    signal_log = pd.DataFrame(signal_log_list)
    daily_nav = pd.DataFrame(daily_nav_list)
    summary = summarize_daily_nav(daily_nav, INITIAL_CAPITAL)

    if not positions.empty:
        positions = positions.sort_values(["trade_date", "engine", "symbol"]).reset_index(drop=True)
    if not signal_log.empty:
        signal_log = signal_log.sort_values(["trade_date"]).reset_index(drop=True)
    if not daily_nav.empty:
        daily_nav = daily_nav.sort_values(["trade_date"]).reset_index(drop=True)

    positions.to_csv(POSITIONS_OUT, index=False)
    signal_log.to_csv(SIGNAL_LOG_OUT, index=False)
    daily_nav.to_csv(DAILY_NAV_OUT, index=False)
    summary.to_csv(SUMMARY_OUT, index=False)

    print("v216 完成")
    print("positions rows:", len(positions))
    print("signal days:", len(signal_log))
    if not summary.empty:
        print(summary.to_dict(orient="records")[0])


if __name__ == "__main__":
    main()
