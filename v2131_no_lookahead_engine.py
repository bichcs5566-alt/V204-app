# v213.1 無偷看正式版（TXT 版）
# 存成：v2131_no_lookahead_engine.py
# workflow：.github/workflows/v2131.yml

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent

PRICE_PANEL_PATH = ROOT / "price_panel_daily.csv"
BASE_NAV_PATH = ROOT / "v202_nav.csv"

OUT_POSITIONS = ROOT / "v2131_daily_positions.csv"
OUT_NAV = ROOT / "v2131_daily_nav.csv"
OUT_SUMMARY = ROOT / "v2131_summary.csv"
OUT_SIGNAL = ROOT / "v2131_signal_log.csv"

LOOKBACK_DAYS = 5
MIN_PRICE = 10.0
TOP_N = 20
MAX_DAILY_RET_CLIP = 0.10


def load_price_panel():
    px = pd.read_csv(PRICE_PANEL_PATH)
    px.columns = [str(c).strip() for c in px.columns]

    required = {"symbol", "date", "close"}
    missing = required - set(px.columns)
    if missing:
        raise ValueError(f"price_panel_daily.csv 缺少欄位: {sorted(missing)}")

    px["symbol"] = px["symbol"].astype(str).str.strip()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    px["close"] = pd.to_numeric(px["close"], errors="coerce")

    px = px.dropna(subset=["symbol", "date", "close"]).copy()
    px = px.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last").reset_index(drop=True)
    return px


def load_initial_capital():
    nav = pd.read_csv(BASE_NAV_PATH)
    nav.columns = [str(c).strip() for c in nav.columns]

    if "nav" not in nav.columns:
        raise ValueError("v202_nav.csv 缺少 nav 欄位")

    nav["nav"] = pd.to_numeric(nav["nav"], errors="coerce")
    nav = nav.dropna(subset=["nav"])

    if nav.empty:
        raise ValueError("v202_nav.csv 沒有有效 nav")

    return float(nav.iloc[0]["nav"])


def prepare_features(px):
    px = px.copy()
    px["ret_1d"] = px.groupby("symbol")["close"].pct_change()
    px["ret_1d"] = px["ret_1d"].clip(-MAX_DAILY_RET_CLIP, MAX_DAILY_RET_CLIP)
    px["mom_5d"] = px.groupby("symbol")["close"].pct_change(LOOKBACK_DAYS)

    # 無偷看：今天產生信號，只能吃下一交易日報酬
    px["next_ret_1d"] = px.groupby("symbol")["ret_1d"].shift(-1)
    px["next_date"] = px.groupby("symbol")["date"].shift(-1)

    return px


def select_daily_portfolio(signal_day_df):
    candidates = signal_day_df.copy()
    candidates = candidates[candidates["close"] >= MIN_PRICE]
    candidates = candidates[candidates["mom_5d"].notna()]
    candidates = candidates[candidates["mom_5d"] > 0]
    candidates = candidates[candidates["next_ret_1d"].notna()]
    candidates = candidates[candidates["next_date"].notna()]

    if candidates.empty:
        return candidates.assign(weight=np.nan)

    selected = candidates.sort_values(["mom_5d", "symbol"], ascending=[False, True]).head(TOP_N).copy()
    selected["weight"] = 1.0 / len(selected)
    return selected


def build_backtest(px, initial_capital):
    signal_dates = sorted(px["date"].dropna().unique())

    signal_rows = []
    position_rows = []
    nav_rows = []

    nav_value = float(initial_capital)

    for signal_dt in signal_dates:
        signal_day_df = px[px["date"] == signal_dt].copy()
        selected = select_daily_portfolio(signal_day_df)

        selected_count = int(len(selected))
        avg_mom = float(selected["mom_5d"].mean()) if selected_count > 0 else np.nan

        mode_next = selected["next_date"].mode() if selected_count > 0 else pd.Series(dtype="datetime64[ns]")
        trade_dt = mode_next.iloc[0] if not mode_next.empty else pd.NaT

        signal_rows.append({
            "signal_date": pd.Timestamp(signal_dt).strftime("%Y-%m-%d"),
            "trade_date": pd.Timestamp(trade_dt).strftime("%Y-%m-%d") if pd.notna(trade_dt) else None,
            "candidate_rows": int(len(signal_day_df)),
            "selected_count": selected_count,
            "avg_mom_5d": avg_mom,
        })

        if selected_count == 0:
            continue

        selected["trade_date"] = pd.to_datetime(selected["next_date"])
        selected["trade_ret"] = selected["next_ret_1d"].fillna(0.0).clip(-MAX_DAILY_RET_CLIP, MAX_DAILY_RET_CLIP)
        selected["wret"] = selected["trade_ret"] * selected["weight"]

        day_ret_df = (
            selected.groupby("trade_date", as_index=False)
            .agg(
                daily_ret=("wret", "sum"),
                holdings=("symbol", "nunique"),
                avg_exposure=("weight", lambda x: float(pd.Series(x).abs().sum()))
            )
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

        for _, rr in day_ret_df.iterrows():
            daily_ret = float(np.clip(rr["daily_ret"], -MAX_DAILY_RET_CLIP, MAX_DAILY_RET_CLIP))
            nav_value = nav_value * (1.0 + daily_ret)

            nav_rows.append({
                "signal_date": pd.Timestamp(signal_dt).strftime("%Y-%m-%d"),
                "date": pd.Timestamp(rr["trade_date"]).strftime("%Y-%m-%d"),
                "daily_ret": daily_ret,
                "nav": nav_value,
                "holdings": int(rr["holdings"]),
                "avg_exposure": float(rr["avg_exposure"]),
            })

        for _, row in selected.iterrows():
            position_rows.append({
                "signal_date": pd.Timestamp(signal_dt).strftime("%Y-%m-%d"),
                "trade_date": pd.Timestamp(row["trade_date"]).strftime("%Y-%m-%d"),
                "symbol": row["symbol"],
                "signal_close": float(row["close"]),
                "mom_5d": float(row["mom_5d"]),
                "trade_ret": float(row["trade_ret"]),
                "weight": float(row["weight"]),
                "wret": float(row["wret"]),
            })

    positions = pd.DataFrame(position_rows)
    nav_df = pd.DataFrame(nav_rows)
    signal_df = pd.DataFrame(signal_rows)

    if not nav_df.empty:
        nav_df = nav_df.sort_values(["date", "signal_date"]).drop_duplicates(["date"], keep="last").reset_index(drop=True)

    return positions, nav_df, signal_df


def build_summary(nav_df, initial_capital):
    if nav_df.empty:
        return pd.DataFrame([{
            "start_date": None,
            "end_date": None,
            "initial_capital": initial_capital,
            "final_nav": initial_capital,
            "total_return": 0.0,
            "trading_days": 0,
            "nonzero_ret_days": 0,
            "avg_holdings": 0.0,
            "avg_exposure": 0.0,
            "sharpe": 0.0,
            "mdd": 0.0,
        }])

    nav_series = nav_df["nav"].astype(float)
    ret_series = nav_df["daily_ret"].astype(float)

    final_nav = float(nav_series.iloc[-1])
    total_return = (final_nav / initial_capital - 1.0) if initial_capital != 0 else 0.0
    nonzero_ret_days = int((ret_series.abs() > 0).sum())
    avg_holdings = float(nav_df["holdings"].astype(float).mean())
    avg_exposure = float(nav_df["avg_exposure"].astype(float).mean())

    ret_std = float(ret_series.std(ddof=0))
    sharpe = float((ret_series.mean() / ret_std) * np.sqrt(252)) if ret_std > 0 else 0.0

    running_max = nav_series.cummax()
    drawdown = nav_series / running_max - 1.0
    mdd = float(drawdown.min()) if len(drawdown) else 0.0

    return pd.DataFrame([{
        "start_date": str(nav_df.iloc[0]["date"]),
        "end_date": str(nav_df.iloc[-1]["date"]),
        "initial_capital": initial_capital,
        "final_nav": final_nav,
        "total_return": total_return,
        "trading_days": int(len(nav_df)),
        "nonzero_ret_days": nonzero_ret_days,
        "avg_holdings": avg_holdings,
        "avg_exposure": avg_exposure,
        "sharpe": sharpe,
        "mdd": mdd,
    }])


def main():
    px = load_price_panel()
    initial_capital = load_initial_capital()
    px = prepare_features(px)

    positions, nav_df, signal_df = build_backtest(px, initial_capital)
    summary = build_summary(nav_df, initial_capital)

    positions.to_csv(OUT_POSITIONS, index=False)
    nav_df.to_csv(OUT_NAV, index=False)
    signal_df.to_csv(OUT_SIGNAL, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print("v213.1 DONE")
    print("price rows:", len(px))
    print("positions rows:", len(positions))
    print("nav rows:", len(nav_df))
    print("signal rows:", len(signal_df))
    print("nonzero ret days:", int((nav_df["daily_ret"].abs() > 0).sum()) if not nav_df.empty else 0)
    if not summary.empty:
        print("final nav:", float(summary.iloc[0]["final_nav"]))
        print("total return:", float(summary.iloc[0]["total_return"]))


if __name__ == "__main__":
    main()
