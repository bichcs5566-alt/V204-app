import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent

INPUT_PATH = ROOT / "price_panel_daily.csv"
OUTPUT_DAILY = ROOT / "v2141_daily_log.csv"
OUTPUT_POSITIONS = ROOT / "v2141_positions.csv"
OUTPUT_SUMMARY = ROOT / "v2141_summary.csv"

INITIAL_CAPITAL = 100000.0
MAX_POSITIONS = 20
MOM_THRESHOLD = 0.15
NO_CHASE_RET = 0.05
VOL_LOOKBACK = 5
MOM_LOOKBACK = 5
RET_CLIP = 0.10


def load_data():
    df = pd.read_csv(INPUT_PATH)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"symbol", "date", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"price_panel_daily.csv 缺少欄位: {sorted(missing)}")

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    if "market" not in df.columns:
        df["market"] = ""

    df = df.dropna(subset=["symbol", "date", "close"]).copy()
    df = df.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
    df.reset_index(drop=True, inplace=True)
    return df


def add_features(df):
    out = df.copy()

    out["ret_1d"] = out.groupby("symbol")["close"].pct_change()
    out["ret_1d"] = out["ret_1d"].clip(-RET_CLIP, RET_CLIP)

    out["mom_5d"] = out.groupby("symbol")["close"].pct_change(MOM_LOOKBACK)

    out["std_5d"] = (
        out.groupby("symbol")["ret_1d"]
        .rolling(VOL_LOOKBACK)
        .std()
        .reset_index(level=0, drop=True)
    )

    out["next_ret_1d"] = out.groupby("symbol")["ret_1d"].shift(-1)
    out["next_date"] = out.groupby("symbol")["date"].shift(-1)

    return out


def select_stocks(df_day):
    day = df_day.copy()

    raw_count = len(day)

    day = day[day["mom_5d"].notna()].copy()
    after_mom_na = len(day)

    day = day[day["mom_5d"] > MOM_THRESHOLD].copy()
    after_mom_threshold = len(day)

    day = day[day["ret_1d"].fillna(0) < NO_CHASE_RET].copy()
    after_no_chase = len(day)

    if not day.empty:
        vol_med = day["std_5d"].median()
        day = day[day["std_5d"].fillna(np.inf) < vol_med].copy()
    after_vol_contract = len(day)

    day = day[day["next_ret_1d"].notna() & day["next_date"].notna()].copy()
    after_next_day = len(day)

    day = day.sort_values(["mom_5d", "symbol"], ascending=[False, True]).head(MAX_POSITIONS).copy()

    if not day.empty:
        day["weight"] = 1.0 / len(day)
    else:
        day["weight"] = np.nan

    stats = {
        "candidate_rows": raw_count,
        "after_mom_na": after_mom_na,
        "after_mom_threshold": after_mom_threshold,
        "after_no_chase": after_no_chase,
        "after_vol_contract": after_vol_contract,
        "after_next_day": after_next_day,
        "selected_count": len(day),
    }
    return day, stats


def run_backtest(df):
    signal_dates = sorted(df["date"].dropna().unique())

    capital = INITIAL_CAPITAL
    daily_rows = []
    position_rows = []

    for signal_dt in signal_dates:
        df_day = df[df["date"] == signal_dt].copy()
        selected, stats = select_stocks(df_day)

        signal_date_str = pd.Timestamp(signal_dt).strftime("%Y-%m-%d")

        if selected.empty:
            daily_rows.append({
                "signal_date": signal_date_str,
                "trade_date": None,
                "candidate_rows": stats["candidate_rows"],
                "after_mom_na": stats["after_mom_na"],
                "after_mom_threshold": stats["after_mom_threshold"],
                "after_no_chase": stats["after_no_chase"],
                "after_vol_contract": stats["after_vol_contract"],
                "after_next_day": stats["after_next_day"],
                "selected_count": 0,
                "avg_mom_5d": np.nan,
                "avg_std_5d": np.nan,
                "daily_ret": 0.0,
                "capital": capital,
                "cash_mode": True,
            })
            continue

        selected["trade_ret"] = selected["next_ret_1d"].clip(-RET_CLIP, RET_CLIP)
        selected["wret"] = selected["trade_ret"] * selected["weight"]
        selected["signal_date"] = pd.Timestamp(signal_dt)
        selected["trade_date"] = pd.to_datetime(selected["next_date"])

        grouped = (
            selected.groupby("trade_date", as_index=False)
            .agg(
                daily_ret=("wret", "sum"),
                holdings=("symbol", "nunique"),
                avg_mom_5d=("mom_5d", "mean"),
                avg_std_5d=("std_5d", "mean"),
            )
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

        for _, g in grouped.iterrows():
            daily_ret = float(g["daily_ret"])
            capital = capital * (1.0 + daily_ret)

            daily_rows.append({
                "signal_date": signal_date_str,
                "trade_date": pd.Timestamp(g["trade_date"]).strftime("%Y-%m-%d"),
                "candidate_rows": stats["candidate_rows"],
                "after_mom_na": stats["after_mom_na"],
                "after_mom_threshold": stats["after_mom_threshold"],
                "after_no_chase": stats["after_no_chase"],
                "after_vol_contract": stats["after_vol_contract"],
                "after_next_day": stats["after_next_day"],
                "selected_count": int(g["holdings"]),
                "avg_mom_5d": float(g["avg_mom_5d"]),
                "avg_std_5d": float(g["avg_std_5d"]),
                "daily_ret": daily_ret,
                "capital": capital,
                "cash_mode": False,
            })

        keep_cols = [
            "signal_date", "trade_date", "symbol", "market", "close",
            "mom_5d", "std_5d", "trade_ret", "weight", "wret"
        ]
        pos = selected[keep_cols].copy()
        pos["signal_date"] = pos["signal_date"].dt.strftime("%Y-%m-%d")
        pos["trade_date"] = pos["trade_date"].dt.strftime("%Y-%m-%d")
        pos.rename(columns={"close": "signal_close"}, inplace=True)
        position_rows.extend(pos.to_dict("records"))

    daily_df = pd.DataFrame(daily_rows)
    pos_df = pd.DataFrame(position_rows)

    if not daily_df.empty:
        daily_df = daily_df.sort_values(["trade_date", "signal_date"], na_position="last").reset_index(drop=True)

    return daily_df, pos_df


def build_summary(daily_df):
    if daily_df.empty:
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
            "sharpe": 0.0,
            "mdd": 0.0,
        }])

    nav = daily_df["capital"].astype(float)
    ret = daily_df["daily_ret"].astype(float)

    final_nav = float(nav.iloc[-1])
    total_return = final_nav / INITIAL_CAPITAL - 1.0

    ret_std = float(ret.std(ddof=0))
    sharpe = float(ret.mean() / ret_std * np.sqrt(252)) if ret_std > 0 else 0.0

    running_max = nav.cummax()
    drawdown = nav / running_max - 1.0
    mdd = float(drawdown.min()) if len(drawdown) else 0.0

    avg_holdings = float(daily_df["selected_count"].astype(float).mean())
    avg_exposure = float((daily_df["selected_count"].astype(float) / MAX_POSITIONS).mean())
    cash_days = int(daily_df["cash_mode"].fillna(False).sum())

    start_value = daily_df.iloc[0]["trade_date"] if pd.notna(daily_df.iloc[0]["trade_date"]) else daily_df.iloc[0]["signal_date"]
    end_value = daily_df.iloc[-1]["trade_date"] if pd.notna(daily_df.iloc[-1]["trade_date"]) else daily_df.iloc[-1]["signal_date"]

    return pd.DataFrame([{
        "start_date": str(start_value),
        "end_date": str(end_value),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": final_nav,
        "total_return": total_return,
        "trading_days": int(len(daily_df)),
        "nonzero_ret_days": int((ret.abs() > 0).sum()),
        "avg_holdings": avg_holdings,
        "avg_exposure": avg_exposure,
        "cash_days": cash_days,
        "sharpe": sharpe,
        "mdd": mdd,
    }])


def main():
    df = load_data()
    df = add_features(df)

    daily_df, pos_df = run_backtest(df)
    summary_df = build_summary(daily_df)

    daily_df.to_csv(OUTPUT_DAILY, index=False)
    pos_df.to_csv(OUTPUT_POSITIONS, index=False)
    summary_df.to_csv(OUTPUT_SUMMARY, index=False)

    print("v214.1 done")
    print(summary_df.to_dict("records")[0])


if __name__ == "__main__":
    main()
