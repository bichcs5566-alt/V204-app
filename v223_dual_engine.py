# v223_dual_engine.py
# ============================================================
# v223 雙引擎版
# - Engine A: 短線反轉（買弱等反彈）
# - Engine B: 波段順勢（買強等續漲）
# - 兩個引擎完全分開，最後只在資金層合併，不混邏輯
# - 無偷看：signal_date 選股，trade_date 才進場
# ============================================================

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


OUT_PREFIX = "v223"

INITIAL_CAPITAL = 100000.0

SHORT_CAPITAL_PCT = 0.30
SWING_CAPITAL_PCT = 0.70

A_TOP_N = 8
A_MIN_PRICE = 10.0
A_MAX_PRICE = 250.0
A_HOLD_DAYS = 2
A_MOM5_MAX = -0.03
A_STD5_MAX = 0.10
A_MIN_RET1 = -0.08
A_MAX_RET1 = 0.03

B_TOP_N = 6
B_MIN_PRICE = 10.0
B_MAX_PRICE = 350.0
B_HOLD_DAYS = 5
B_MOM5_MIN = 0.05
B_MOM20_MIN = 0.10
B_STD5_MAX = 0.12

PRICE_CANDIDATES = [
    "price_panel.csv",
    "twse_tpex_price_panel.csv",
    "v212_twse_tpex_price_panel.csv",
    "v212_twse_tpex_price_panel_v2.csv",
    "v212_price_panel.csv",
    "price_panel.parquet",
    "twse_tpex_price_panel.parquet",
]


def find_existing_file(candidates: Iterable[str]) -> Optional[Path]:
    for name in candidates:
        p = Path(name)
        if p.exists():
            return p
    for ext in ("*.csv", "*.parquet", "*.zip"):
        for p in sorted(Path(".").glob(ext)):
            low = p.name.lower()
            if "price" in low and "panel" in low:
                return p
    return None


def read_any_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".zip":
        return pd.read_csv(path, compression="zip")
    raise ValueError(f"不支援的檔案格式: {path}")


def normalize_price_panel(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        low = str(c).lower().strip()
        if low in {"date", "trade_date", "tradedate", "datetime"}:
            rename_map[c] = "date"
        elif low in {"symbol", "stock_id", "stockid", "code", "ticker"}:
            rename_map[c] = "symbol"
        elif low in {"close", "close_price", "closing_price", "收盤價"}:
            rename_map[c] = "close"
        elif low in {"market", "exchange"}:
            rename_map[c] = "market"

    df = df.rename(columns=rename_map)

    required = ["date", "symbol", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"價格資料缺少必要欄位: {missing}")

    if "market" not in df.columns:
        df["market"] = "UNKNOWN"

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["symbol"] = (
        df["symbol"].astype(str).str.extract(r"(\d+)")[0].fillna(df["symbol"].astype(str))
    )
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["market"] = df["market"].astype(str)

    df = df.dropna(subset=["date", "symbol", "close"]).copy()
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df[["date", "symbol", "close", "market"]]


def build_features(price: pd.DataFrame) -> pd.DataFrame:
    px = price.copy()
    g = px.groupby("symbol", group_keys=False)

    px["ret_1d"] = g["close"].pct_change(1)
    px["mom_5d"] = g["close"].pct_change(5)
    px["mom_10d"] = g["close"].pct_change(10)
    px["mom_20d"] = g["close"].pct_change(20)
    px["std_5d"] = g["ret_1d"].rolling(5).std().reset_index(level=0, drop=True)
    px["ma_5"] = g["close"].rolling(5).mean().reset_index(level=0, drop=True)
    px["ma_10"] = g["close"].rolling(10).mean().reset_index(level=0, drop=True)
    px["ma_20"] = g["close"].rolling(20).mean().reset_index(level=0, drop=True)
    return px


def build_trade_calendar(trading_dates: list[str]) -> tuple[dict[str, Optional[str]], dict[str, Optional[str]]]:
    next_map = {}
    prev_map = {}
    for i, d in enumerate(trading_dates):
        prev_map[d] = trading_dates[i - 1] if i > 0 else None
        next_map[d] = trading_dates[i + 1] if i + 1 < len(trading_dates) else None
    return next_map, prev_map


def nth_next_date(next_map: dict[str, Optional[str]], start_date: str, n: int) -> Optional[str]:
    cur = start_date
    for _ in range(n):
        cur = next_map.get(cur)
        if cur is None:
            return None
    return cur


def add_trade_paths(selected: pd.DataFrame, feat: pd.DataFrame, trade_date: str, exit_date: Optional[str]) -> pd.DataFrame:
    trade_px = feat.loc[feat["date"] == trade_date, ["symbol", "close"]].rename(columns={"close": "trade_close"})
    out = selected.merge(trade_px, on="symbol", how="left")

    if exit_date is not None:
        exit_px = feat.loc[feat["date"] == exit_date, ["symbol", "close"]].rename(columns={"close": "exit_close"})
        out = out.merge(exit_px, on="symbol", how="left")
        out["trade_ret"] = np.where(
            out["trade_close"].notna() & out["exit_close"].notna() & (out["trade_close"] > 0),
            out["exit_close"] / out["trade_close"] - 1.0,
            np.nan,
        )
    else:
        out["exit_close"] = np.nan
        out["trade_ret"] = np.nan

    return out


def run_engine_a(feat: pd.DataFrame, next_map: dict[str, Optional[str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    trading_dates = sorted(feat["date"].unique().tolist())
    pos_rows = []
    log_rows = []

    for signal_date in trading_dates:
        trade_date = next_map.get(signal_date)
        if trade_date is None:
            continue
        exit_date = nth_next_date(next_map, trade_date, A_HOLD_DAYS)

        snap = feat[feat["date"] == signal_date].copy()
        if snap.empty:
            continue

        candidates = snap[
            snap["mom_5d"].notna()
            & snap["std_5d"].notna()
            & snap["ret_1d"].notna()
            & (snap["close"] >= A_MIN_PRICE)
            & (snap["close"] <= A_MAX_PRICE)
            & (snap["mom_5d"] <= A_MOM5_MAX)
            & (snap["std_5d"] <= A_STD5_MAX)
            & (snap["ret_1d"] >= A_MIN_RET1)
            & (snap["ret_1d"] <= A_MAX_RET1)
        ].copy()

        if candidates.empty:
            log_rows.append(
                {
                    "engine": "A",
                    "signal_date": signal_date,
                    "trade_date": trade_date,
                    "candidate_rows": 0,
                    "selected_count": 0,
                    "avg_mom_5d": np.nan,
                    "hold_days": A_HOLD_DAYS,
                }
            )
            continue

        selected = candidates.sort_values(
            ["mom_5d", "std_5d", "ret_1d"],
            ascending=[True, True, False],
        ).head(A_TOP_N).copy()

        selected["engine"] = "A"
        selected["signal_date"] = signal_date
        selected["trade_date"] = trade_date
        selected["exit_date"] = exit_date

        selected = add_trade_paths(selected, feat, trade_date, exit_date)

        n = len(selected)
        selected["weight_engine"] = 1.0 / n if n > 0 else 0.0
        selected["weight_portfolio"] = selected["weight_engine"] * SHORT_CAPITAL_PCT
        selected["wret"] = selected["weight_portfolio"] * selected["trade_ret"]

        pos_rows.append(selected)

        log_rows.append(
            {
                "engine": "A",
                "signal_date": signal_date,
                "trade_date": trade_date,
                "candidate_rows": int(len(candidates)),
                "selected_count": int(n),
                "avg_mom_5d": float(selected["mom_5d"].mean()),
                "hold_days": A_HOLD_DAYS,
            }
        )

    positions = pd.concat(pos_rows, ignore_index=True) if pos_rows else pd.DataFrame()
    signal_log = pd.DataFrame(log_rows)
    return positions, signal_log


def run_engine_b(feat: pd.DataFrame, next_map: dict[str, Optional[str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    trading_dates = sorted(feat["date"].unique().tolist())
    pos_rows = []
    log_rows = []

    for signal_date in trading_dates:
        trade_date = next_map.get(signal_date)
        if trade_date is None:
            continue
        exit_date = nth_next_date(next_map, trade_date, B_HOLD_DAYS)

        snap = feat[feat["date"] == signal_date].copy()
        if snap.empty:
            continue

        candidates = snap[
            snap["mom_5d"].notna()
            & snap["mom_20d"].notna()
            & snap["std_5d"].notna()
            & snap["ma_5"].notna()
            & snap["ma_20"].notna()
            & (snap["close"] >= B_MIN_PRICE)
            & (snap["close"] <= B_MAX_PRICE)
            & (snap["mom_5d"] >= B_MOM5_MIN)
            & (snap["mom_20d"] >= B_MOM20_MIN)
            & (snap["std_5d"] <= B_STD5_MAX)
            & (snap["close"] >= snap["ma_5"])
            & (snap["ma_5"] >= snap["ma_20"])
        ].copy()

        if candidates.empty:
            log_rows.append(
                {
                    "engine": "B",
                    "signal_date": signal_date,
                    "trade_date": trade_date,
                    "candidate_rows": 0,
                    "selected_count": 0,
                    "avg_mom_5d": np.nan,
                    "hold_days": B_HOLD_DAYS,
                }
            )
            continue

        selected = candidates.sort_values(
            ["mom_20d", "mom_5d", "std_5d"],
            ascending=[False, False, True],
        ).head(B_TOP_N).copy()

        selected["engine"] = "B"
        selected["signal_date"] = signal_date
        selected["trade_date"] = trade_date
        selected["exit_date"] = exit_date

        selected = add_trade_paths(selected, feat, trade_date, exit_date)

        n = len(selected)
        selected["weight_engine"] = 1.0 / n if n > 0 else 0.0
        selected["weight_portfolio"] = selected["weight_engine"] * SWING_CAPITAL_PCT
        selected["wret"] = selected["weight_portfolio"] * selected["trade_ret"]

        pos_rows.append(selected)

        log_rows.append(
            {
                "engine": "B",
                "signal_date": signal_date,
                "trade_date": trade_date,
                "candidate_rows": int(len(candidates)),
                "selected_count": int(n),
                "avg_mom_5d": float(selected["mom_5d"].mean()),
                "hold_days": B_HOLD_DAYS,
            }
        )

    positions = pd.concat(pos_rows, ignore_index=True) if pos_rows else pd.DataFrame()
    signal_log = pd.DataFrame(log_rows)
    return positions, signal_log


def build_daily_nav(all_positions: pd.DataFrame, trading_dates: list[str]) -> pd.DataFrame:
    if all_positions.empty:
        return pd.DataFrame(
            columns=["trade_date", "daily_ret", "holdings", "gross_exposure", "nav", "cum_return", "drawdown"]
        )

    agg = all_positions.groupby("trade_date", as_index=False).agg(
        daily_ret=("wret", "sum"),
        holdings=("symbol", "nunique"),
        gross_exposure=("weight_portfolio", "sum"),
    )

    daily = pd.DataFrame({"trade_date": trading_dates[1:]})
    daily = daily.merge(agg, on="trade_date", how="left")
    daily["daily_ret"] = daily["daily_ret"].fillna(0.0)
    daily["holdings"] = daily["holdings"].fillna(0).astype(int)
    daily["gross_exposure"] = daily["gross_exposure"].fillna(0.0)

    nav_vals = []
    capital = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    for r in daily.itertuples():
        capital *= 1.0 + float(r.daily_ret)
        peak = max(peak, capital)
        nav_vals.append((capital, capital / INITIAL_CAPITAL - 1.0, capital / peak - 1.0))

    daily[["nav", "cum_return", "drawdown"]] = pd.DataFrame(nav_vals, index=daily.index)
    return daily


def sharpe_ratio(daily_ret: pd.Series) -> float:
    x = pd.to_numeric(daily_ret, errors="coerce").dropna()
    if len(x) < 2 or float(x.std(ddof=1)) == 0.0:
        return 0.0
    return float((x.mean() / x.std(ddof=1)) * math.sqrt(252.0))


def annualized_return(final_nav: float, n_days: int, initial_nav: float) -> float:
    if n_days <= 0 or final_nav <= 0 or initial_nav <= 0:
        return 0.0
    years = n_days / 252.0
    if years <= 0:
        return 0.0
    return (final_nav / initial_nav) ** (1.0 / years) - 1.0


def build_summary(price: pd.DataFrame, positions: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            [
                {
                    "start_date": None,
                    "end_date": None,
                    "initial_capital": INITIAL_CAPITAL,
                    "final_nav": INITIAL_CAPITAL,
                    "total_return": 0.0,
                    "trading_days": 0,
                    "nonzero_ret_days": 0,
                    "avg_holdings": 0.0,
                    "avg_exposure": 0.0,
                    "ann_return": 0.0,
                    "sharpe": 0.0,
                    "mdd": 0.0,
                    "trade_rows": 0,
                    "engines": "A,B",
                }
            ]
        )

    final_nav = float(daily["nav"].iloc[-1])
    return pd.DataFrame(
        [
            {
                "start_date": str(price["date"].min()),
                "end_date": str(price["date"].max()),
                "initial_capital": INITIAL_CAPITAL,
                "final_nav": final_nav,
                "total_return": final_nav / INITIAL_CAPITAL - 1.0,
                "trading_days": int(len(daily)),
                "nonzero_ret_days": int((daily["daily_ret"].abs() > 0).sum()),
                "avg_holdings": float(daily["holdings"].mean()),
                "avg_exposure": float(daily["gross_exposure"].mean()),
                "ann_return": annualized_return(final_nav, len(daily), INITIAL_CAPITAL),
                "sharpe": sharpe_ratio(daily["daily_ret"]),
                "mdd": float(daily["drawdown"].min()),
                "trade_rows": int(len(positions)),
                "engines": "A,B",
            }
        ]
    )


def build_engine_stats(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(
            columns=["engine", "rows", "symbols", "avg_trade_ret", "win_rate", "loss_rate", "avg_wret"]
        )

    rows = []
    for eng, df in positions.groupby("engine"):
        x = df.copy()
        rows.append(
            {
                "engine": eng,
                "rows": int(len(x)),
                "symbols": int(x["symbol"].nunique()),
                "avg_trade_ret": float(pd.to_numeric(x["trade_ret"], errors="coerce").mean()),
                "win_rate": float((pd.to_numeric(x["trade_ret"], errors="coerce") > 0).mean()),
                "loss_rate": float((pd.to_numeric(x["trade_ret"], errors="coerce") < 0).mean()),
                "avg_wret": float(pd.to_numeric(x["wret"], errors="coerce").mean()),
            }
        )
    return pd.DataFrame(rows)


def build_window_summary(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "window_name",
                "start_date",
                "end_date",
                "trading_days",
                "nonzero_ret_days",
                "avg_holdings",
                "avg_gross_exposure",
                "initial_nav",
                "final_nav",
                "total_return",
                "ann_return",
                "sharpe",
                "mdd",
                "pass_sharpe_gt_1",
                "pass_mdd_gt_-0.20",
            ]
        )

    windows = {
        "full_sample": daily.copy(),
        "last_60d": daily.tail(60).copy(),
    }

    rows = []
    for name, df in windows.items():
        if df.empty:
            continue
        if name == "full_sample":
            init_nav = INITIAL_CAPITAL
        else:
            first_ret = float(df["daily_ret"].iloc[0])
            init_nav = float(df["nav"].iloc[0] / (1.0 + first_ret)) if (1.0 + first_ret) != 0 else float(df["nav"].iloc[0])

        final_nav = float(df["nav"].iloc[-1])
        total_return = final_nav / init_nav - 1.0 if init_nav > 0 else 0.0
        ann_return = annualized_return(final_nav, len(df), init_nav)
        shp = sharpe_ratio(df["daily_ret"])
        mdd = float(df["drawdown"].min())

        rows.append(
            {
                "window_name": name,
                "start_date": str(df["trade_date"].min()),
                "end_date": str(df["trade_date"].max()),
                "trading_days": int(len(df)),
                "nonzero_ret_days": int((df["daily_ret"].abs() > 0).sum()),
                "avg_holdings": float(df["holdings"].mean()),
                "avg_gross_exposure": float(df["gross_exposure"].mean()),
                "initial_nav": init_nav,
                "final_nav": final_nav,
                "total_return": total_return,
                "ann_return": ann_return,
                "sharpe": shp,
                "mdd": mdd,
                "pass_sharpe_gt_1": bool(shp > 1.0),
                "pass_mdd_gt_-0.20": bool(mdd > -0.20),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    src = find_existing_file(PRICE_CANDIDATES)
    if src is None:
        raise FileNotFoundError("找不到價格資料檔，請放入 price_panel.csv 或 v212_twse_tpex_price_panel.csv 在 repo 根目錄。")

    raw = read_any_table(src)
    price = normalize_price_panel(raw)
    feat = build_features(price)

    trading_dates = sorted(feat["date"].unique().tolist())
    next_map, _ = build_trade_calendar(trading_dates)

    a_pos, a_log = run_engine_a(feat, next_map)
    b_pos, b_log = run_engine_b(feat, next_map)

    positions = (
        pd.concat([a_pos, b_pos], ignore_index=True)
        if (not a_pos.empty or not b_pos.empty)
        else pd.DataFrame()
    )
    signal_log = (
        pd.concat([a_log, b_log], ignore_index=True)
        if (not a_log.empty or not b_log.empty)
        else pd.DataFrame()
    )

    if not positions.empty:
        positions = positions.rename(columns={"close": "signal_close"})
        ordered_cols = [
            "engine",
            "signal_date",
            "trade_date",
            "exit_date",
            "symbol",
            "market",
            "signal_close",
            "mom_5d",
            "mom_20d",
            "std_5d",
            "trade_close",
            "exit_close",
            "trade_ret",
            "weight_engine",
            "weight_portfolio",
            "wret",
        ]
        positions = positions[[c for c in ordered_cols if c in positions.columns]].copy()

    daily = build_daily_nav(positions, trading_dates)
    summary = build_summary(price, positions, daily)
    engine_stats = build_engine_stats(positions)
    window_summary = build_window_summary(daily)

    signal_log.to_csv(f"{OUT_PREFIX}_signal_log.csv", index=False)
    positions.to_csv(f"{OUT_PREFIX}_positions.csv", index=False)
    daily.to_csv(f"{OUT_PREFIX}_daily_nav.csv", index=False)
    summary.to_csv(f"{OUT_PREFIX}_summary.csv", index=False)
    engine_stats.to_csv(f"{OUT_PREFIX}_engine_stats.csv", index=False)
    window_summary.to_csv(f"{OUT_PREFIX}_window_summary.csv", index=False)

    print("source file:", src.name)
    print("price rows:", len(price))
    print("A rows:", len(a_pos))
    print("B rows:", len(b_pos))
    print(summary.to_string(index=False))
    if not engine_stats.empty:
        print(engine_stats.to_string(index=False))


if __name__ == "__main__":
    main()
