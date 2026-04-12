# v222_auto_pipeline.py
# ------------------------------------------------------------
# v222 自動資料 + 策略 + 輸出版
# 功能：
# 1) 自動尋找 repo 內的價格資料檔
# 2) 標準化欄位 date / symbol / close / market
# 3) 建立無偷看 signal（用 signal_date 選股，trade_date 才交易）
# 4) 輸出 positions / daily_nav / summary / trade_stats / window_summary
# ------------------------------------------------------------

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


OUT_PREFIX = "v222"

INITIAL_CAPITAL = 100000.0
TOP_N = 12
VOL_MAX = 0.12          # 5日波動上限
MIN_PRICE = 10.0
MAX_PRICE = 300.0

# 風控
RISK_ON_EXPOSURE = 0.80
RISK_OFF_EXPOSURE = 0.35
CASH_MOM_THRESHOLD = 0.03
BREADTH_THRESHOLD = 8

# 尋找資料檔優先順序
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
        low = c.lower().strip()

        if low in {"date", "tradedate", "trade_date", "datetime"}:
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
    df["symbol"] = df["symbol"].astype(str).str.extract(r"(\d+)")[0].fillna(df["symbol"].astype(str))
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["market"] = df["market"].astype(str)

    df = df.dropna(subset=["date", "symbol", "close"]).copy()
    df = df[df["close"] > 0].copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    return df[["date", "symbol", "close", "market"]]


def build_features(price: pd.DataFrame) -> pd.DataFrame:
    px = price.copy()
    grp = px.groupby("symbol", group_keys=False)

    px["ret"] = grp["close"].pct_change()
    px["mom_5d"] = grp["close"].pct_change(5)
    px["std_5d"] = grp["ret"].rolling(5).std().reset_index(level=0, drop=True)

    return px


def build_next_trade_map(trading_dates: list[str]) -> dict[str, Optional[str]]:
    nxt = {}
    for i, d in enumerate(trading_dates):
        nxt[d] = trading_dates[i + 1] if i + 1 < len(trading_dates) else None
    return nxt


def build_positions(feat: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    trading_dates = sorted(feat["date"].unique().tolist())
    next_map = build_next_trade_map(trading_dates)

    positions = []
    daily_meta = []

    for signal_date in trading_dates:
        trade_date = next_map.get(signal_date)
        if trade_date is None:
            continue

        snap = feat[feat["date"] == signal_date].copy()
        if snap.empty:
            continue

        candidates = snap[
            snap["mom_5d"].notna()
            & snap["std_5d"].notna()
            & (snap["close"] >= MIN_PRICE)
            & (snap["close"] <= MAX_PRICE)
            & (snap["std_5d"] <= VOL_MAX)
        ].copy()

        market_rows = int(len(snap))
        breadth = int((candidates["mom_5d"] > 0).sum()) if not candidates.empty else 0
        avg_mom = float(candidates["mom_5d"].mean()) if not candidates.empty else 0.0

        risk_on = (breadth >= BREADTH_THRESHOLD) and (avg_mom >= CASH_MOM_THRESHOLD)
        gross_exposure = RISK_ON_EXPOSURE if risk_on else RISK_OFF_EXPOSURE
        cash_mode = not risk_on

        if candidates.empty:
            daily_meta.append({
                "signal_date": signal_date,
                "trade_date": trade_date,
                "market_rows": market_rows,
                "candidate_rows": 0,
                "selected_count": 0,
                "breadth": breadth,
                "avg_mom_5d": avg_mom,
                "gross_exposure": 0.0,
                "cash_mode": True,
                "risk_on": False,
            })
            continue

        selected = candidates.sort_values(["mom_5d", "std_5d", "close"], ascending=[False, True, True]).head(TOP_N).copy()
        selected_count = int(len(selected))
        weight = gross_exposure / selected_count if selected_count > 0 else 0.0
        selected["weight"] = weight
        selected["signal_date"] = signal_date
        selected["trade_date"] = trade_date

        # 用 trade_date 的 close 算下一個交易日報酬：無偷看
        trade_px = feat.loc[feat["date"] == trade_date, ["symbol", "close"]].rename(columns={"close": "trade_close"})
        next_trade = next_map.get(trade_date)
        exit_px = None
        if next_trade is not None:
            exit_px = feat.loc[feat["date"] == next_trade, ["symbol", "close"]].rename(columns={"close": "exit_close"})

        selected = selected.merge(trade_px, on="symbol", how="left")
        if exit_px is not None:
            selected = selected.merge(exit_px, on="symbol", how="left")
            selected["trade_ret"] = np.where(
                selected["trade_close"].notna() & selected["exit_close"].notna() & (selected["trade_close"] > 0),
                selected["exit_close"] / selected["trade_close"] - 1.0,
                np.nan,
            )
        else:
            selected["trade_ret"] = np.nan

        selected["wret"] = selected["weight"] * selected["trade_ret"]

        daily_meta.append({
            "signal_date": signal_date,
            "trade_date": trade_date,
            "market_rows": market_rows,
            "candidate_rows": int(len(candidates)),
            "selected_count": selected_count,
            "breadth": breadth,
            "avg_mom_5d": avg_mom,
            "gross_exposure": float(gross_exposure),
            "cash_mode": bool(cash_mode),
            "risk_on": bool(risk_on),
        })

        positions.append(selected)

    pos = pd.concat(positions, ignore_index=True) if positions else pd.DataFrame()
    meta = pd.DataFrame(daily_meta)

    return pos, meta


def build_nav(positions: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if meta.empty:
        return pd.DataFrame(columns=[
            "trade_date", "daily_ret", "holdings", "gross_exposure", "cash_mode",
            "risk_on", "nav", "cum_return", "drawdown"
        ])

    if positions.empty:
        daily = meta[["trade_date", "gross_exposure", "cash_mode", "risk_on"]].copy()
        daily["daily_ret"] = 0.0
        daily["holdings"] = 0
    else:
        agg = positions.groupby("trade_date", as_index=False).agg(
            daily_ret=("wret", "sum"),
            holdings=("symbol", "nunique"),
        )
        daily = meta.merge(agg, on="trade_date", how="left")
        daily["daily_ret"] = daily["daily_ret"].fillna(0.0)
        daily["holdings"] = daily["holdings"].fillna(0).astype(int)

    daily = daily.sort_values("trade_date").reset_index(drop=True)
    nav = []
    capital = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    for r in daily.itertuples():
        capital *= (1.0 + float(r.daily_ret))
        peak = max(peak, capital)
        nav.append((capital, capital / INITIAL_CAPITAL - 1.0, capital / peak - 1.0))

    daily[["nav", "cum_return", "drawdown"]] = pd.DataFrame(nav, index=daily.index)
    return daily


def annualized_return(final_nav: float, n_days: int, initial: float) -> float:
    if n_days <= 0 or final_nav <= 0 or initial <= 0:
        return 0.0
    years = n_days / 252.0
    if years <= 0:
        return 0.0
    return (final_nav / initial) ** (1.0 / years) - 1.0


def sharpe_ratio(daily_ret: pd.Series) -> float:
    x = pd.to_numeric(daily_ret, errors="coerce").dropna()
    if len(x) < 2 or float(x.std(ddof=1)) == 0.0:
        return 0.0
    return float((x.mean() / x.std(ddof=1)) * math.sqrt(252.0))


def build_summary(price: pd.DataFrame, positions: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame([{
            "start_date": None,
            "end_date": None,
            "initial_capital": INITIAL_CAPITAL,
            "final_nav": INITIAL_CAPITAL,
            "total_return": 0.0,
            "trading_days": 0,
            "nonzero_ret_days": 0,
            "avg_holdings": 0.0,
            "avg_exposure": 0.0,
            "cash_days": 0,
            "ann_return": 0.0,
            "sharpe": 0.0,
            "mdd": 0.0,
            "trade_rows": 0,
            "symbols": 0,
        }])

    final_nav = float(daily["nav"].iloc[-1])
    total_return = final_nav / INITIAL_CAPITAL - 1.0
    summary = pd.DataFrame([{
        "start_date": str(price["date"].min()),
        "end_date": str(price["date"].max()),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": final_nav,
        "total_return": total_return,
        "trading_days": int(len(daily)),
        "nonzero_ret_days": int((daily["daily_ret"].abs() > 0).sum()),
        "avg_holdings": float(daily["holdings"].mean()),
        "avg_exposure": float(daily["gross_exposure"].mean()),
        "cash_days": int(daily["cash_mode"].sum()),
        "ann_return": annualized_return(final_nav, len(daily), INITIAL_CAPITAL),
        "sharpe": sharpe_ratio(daily["daily_ret"]),
        "mdd": float(daily["drawdown"].min()),
        "trade_rows": int(len(positions)),
        "symbols": int(positions["symbol"].nunique()) if not positions.empty else 0,
    }])
    return summary


def build_trade_stats(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(columns=["rows", "symbols", "win_rate", "loss_rate", "avg_trade_ret", "avg_wret"])
    x = positions.copy()
    x["trade_ret"] = pd.to_numeric(x["trade_ret"], errors="coerce")
    x["wret"] = pd.to_numeric(x["wret"], errors="coerce")
    return pd.DataFrame([{
        "rows": int(len(x)),
        "symbols": int(x["symbol"].nunique()),
        "win_rate": float((x["trade_ret"] > 0).mean()),
        "loss_rate": float((x["trade_ret"] < 0).mean()),
        "avg_trade_ret": float(x["trade_ret"].mean()),
        "avg_wret": float(x["wret"].mean()),
    }])


def build_window_summary(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=[
            "window_name", "start_date", "end_date", "trading_days", "nonzero_ret_days",
            "avg_holdings", "avg_gross_exposure", "cash_days", "initial_nav", "final_nav",
            "total_return", "ann_return", "sharpe", "mdd", "pass_sharpe_gt_1", "pass_mdd_gt_-0.15"
        ])

    windows = {
        "full_sample": daily.copy(),
        "last_60d": daily.tail(60).copy(),
    }

    rows = []
    for name, df in windows.items():
        if df.empty:
            continue
        init_nav = INITIAL_CAPITAL if name == "full_sample" else float(df["nav"].iloc[0] / (1.0 + float(df["daily_ret"].iloc[0]))) if float(1.0 + df["daily_ret"].iloc[0]) != 0 else float(df["nav"].iloc[0])
        final_nav = float(df["nav"].iloc[-1])
        total_ret = final_nav / init_nav - 1.0 if init_nav > 0 else 0.0
        ann_ret = annualized_return(final_nav, len(df), init_nav)
        shp = sharpe_ratio(df["daily_ret"])
        mdd = float(df["drawdown"].min())
        rows.append({
            "window_name": name,
            "start_date": str(df["trade_date"].min()),
            "end_date": str(df["trade_date"].max()),
            "trading_days": int(len(df)),
            "nonzero_ret_days": int((df["daily_ret"].abs() > 0).sum()),
            "avg_holdings": float(df["holdings"].mean()),
            "avg_gross_exposure": float(df["gross_exposure"].mean()),
            "cash_days": int(df["cash_mode"].sum()),
            "initial_nav": init_nav,
            "final_nav": final_nav,
            "total_return": total_ret,
            "ann_return": ann_ret,
            "sharpe": shp,
            "mdd": mdd,
            "pass_sharpe_gt_1": bool(shp > 1.0),
            "pass_mdd_gt_-0.15": bool(mdd > -0.15),
        })
    return pd.DataFrame(rows)


def main() -> None:
    src = find_existing_file(PRICE_CANDIDATES)
    if src is None:
        raise FileNotFoundError(
            "找不到價格資料檔。請把 price_panel.csv 或 v212_twse_tpex_price_panel.csv 放在 repo 根目錄。"
        )

    raw = read_any_table(src)
    price = normalize_price_panel(raw)
    feat = build_features(price)
    positions, signal_log = build_positions(feat)
    daily = build_nav(positions, signal_log)
    summary = build_summary(price, positions, daily)
    trade_stats = build_trade_stats(positions)
    window_summary = build_window_summary(daily)

    if not positions.empty:
        keep_cols = [
            "signal_date", "trade_date", "symbol", "market", "signal_close",
            "mom_5d", "std_5d", "trade_close", "exit_close", "trade_ret", "weight", "wret"
        ]
        positions = positions.rename(columns={"close": "signal_close"})
        positions = positions[[c for c in keep_cols if c in positions.columns]].copy()

    signal_log.to_csv(f"{OUT_PREFIX}_signal_log.csv", index=False)
    positions.to_csv(f"{OUT_PREFIX}_positions.csv", index=False)
    daily.to_csv(f"{OUT_PREFIX}_daily_nav.csv", index=False)
    summary.to_csv(f"{OUT_PREFIX}_summary.csv", index=False)
    trade_stats.to_csv(f"{OUT_PREFIX}_trade_stats.csv", index=False)
    window_summary.to_csv(f"{OUT_PREFIX}_window_summary.csv", index=False)

    print("source file:", src.name)
    print("price rows:", len(price))
    print("positions rows:", len(positions))
    print("daily rows:", len(daily))
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
