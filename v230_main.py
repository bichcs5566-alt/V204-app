import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
INITIAL_CAPITAL = 100000.0


def load_base_panel(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).replace("\ufeff", "").strip().lower() for c in df.columns]

    date_candidates = ["trade_date", "date", "datetime", "signal_date"]
    date_col = None
    for c in date_candidates:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise Exception(
            f"缺少日期欄位，可接受欄位: {date_candidates}，目前欄位: {df.columns.tolist()}"
        )

    for c in ["symbol", "close"]:
        if c not in df.columns:
            raise Exception(f"缺少欄位: {c}，目前欄位: {df.columns.tolist()}")

    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    if "market" not in df.columns:
        df["market"] = ""

    df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("symbol", group_keys=False)

    df["ret_1d"] = g["close"].pct_change().shift(-1)
    df["mom3"] = g["close"].pct_change(3)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["vol5"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)

    market = (
        df.groupby("trade_date", as_index=False)["close"]
        .mean()
        .rename(columns={"close": "market_close"})
    )
    market["ma5"] = market["market_close"].rolling(5).mean()
    market["ma10"] = market["market_close"].rolling(10).mean()
    market["ma20"] = market["market_close"].rolling(20).mean()

    def regime_fn(row):
        if pd.notna(row["ma5"]) and pd.notna(row["ma10"]) and pd.notna(row["ma20"]):
            if row["ma5"] > row["ma10"] > row["ma20"]:
                return "strong"
            elif row["ma10"] > row["ma20"]:
                return "normal"
            else:
                return "weak"
        return "weak"

    market["regime"] = market.apply(regime_fn, axis=1)
    df = df.merge(
        market[["trade_date", "regime", "ma5", "ma10", "ma20"]],
        on="trade_date",
        how="left"
    )
    return df


def run_engine_a(day: pd.DataFrame):
    d = day.dropna(subset=["mom3", "mom5", "ret_1d"]).copy()
    d = d[(d["mom3"] > 0.02) & (d["mom5"] > 0.03)]

    if d.empty:
        return 0.0, 0, 0.0

    d["score"] = d["mom3"] * 0.6 + d["mom5"] * 0.4
    d = d.sort_values("score", ascending=False).head(3).copy()
    d["ret_1d"] = d["ret_1d"].clip(lower=-0.08)

    return float(d["ret_1d"].mean()), int(len(d)), float(d["score"].mean())


def run_engine_b(day: pd.DataFrame):
    d = day.dropna(subset=["mom5", "mom10", "ret_1d"]).copy()
    d = d[(d["mom5"] > -0.01) & (d["mom10"] > -0.02)]

    if d.empty:
        return 0.0, 0, 0.0

    d["score"] = d["mom10"] * 0.7 + d["mom5"] * 0.3
    d = d.sort_values("score", ascending=False).head(8).copy()
    d["ret_1d"] = d["ret_1d"].clip(lower=-0.05)

    return float(d["ret_1d"].mean()), int(len(d)), float(d["score"].mean())


def backtest_portfolio(df: pd.DataFrame):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    nav_hist = []
    rows = []

    for d in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == d].copy()
        if day.empty:
            continue

        regime = str(day["regime"].iloc[0])

        if regime == "strong":
            w_a, w_b = 0.75, 0.55
        elif regime == "normal":
            w_a, w_b = 0.25, 0.50
        else:
            w_a, w_b = 0.0, 0.35

        if len(nav_hist) >= 10:
            nav5 = float(pd.Series(nav_hist[-5:]).mean())
            nav10 = float(pd.Series(nav_hist[-10:]).mean())
            nav_weak = nav5 < nav10 * 0.995
        else:
            nav_weak = False

        if nav_weak:
            w_a *= 0.5

        current_dd = nav / peak - 1.0
        if current_dd <= -0.08:
            w_a *= 0.5
            w_b *= 0.7

        ret_a, a_count, a_score = run_engine_a(day) if w_a > 0 else (0.0, 0, 0.0)
        ret_b, b_count, b_score = run_engine_b(day) if w_b > 0 else (0.0, 0, 0.0)

        if pd.isna(ret_a):
            ret_a = 0.0
        if pd.isna(ret_b):
            ret_b = 0.0

        total_ret = ret_a * w_a + ret_b * w_b
        if pd.isna(total_ret):
            total_ret = 0.0

        nav *= (1.0 + total_ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0
        nav_hist.append(nav)

        rows.append([
            d, nav, regime, w_a, w_b, ret_a, ret_b, total_ret,
            a_count, b_count, a_score, b_score, nav_weak, dd
        ])

    return pd.DataFrame(rows, columns=[
        "trade_date", "nav", "regime", "w_a", "w_b",
        "ret_a", "ret_b", "total_ret",
        "a_count", "b_count", "a_score", "b_score",
        "nav_weak", "dd"
    ])


def write_reports(nav_df: pd.DataFrame, raw_df: pd.DataFrame):
    if nav_df.empty:
        summary = pd.DataFrame([{
            "start_date": None,
            "end_date": None,
            "return": 0.0,
            "mdd": 0.0,
            "final_nav": INITIAL_CAPITAL,
            "sharpe": 0.0,
            "avg_w_a": 0.0,
            "avg_w_b": 0.0,
            "avg_a_count": 0.0,
            "avg_b_count": 0.0,
            "strong_days": 0,
            "normal_days": 0,
            "weak_days": 0,
            "nav_weak_days": 0,
            "nan_days": 0,
            "data_rows": int(len(raw_df)),
            "symbols": int(raw_df["symbol"].nunique()) if not raw_df.empty else 0,
        }])
    else:
        daily = nav_df["total_ret"].fillna(0.0)
        sharpe = 0.0
        if len(daily) > 1 and daily.std(ddof=1) > 0:
            sharpe = float((daily.mean() / daily.std(ddof=1)) * np.sqrt(252))

        summary = pd.DataFrame([{
            "start_date": str(pd.to_datetime(nav_df["trade_date"].min()).date()),
            "end_date": str(pd.to_datetime(nav_df["trade_date"].max()).date()),
            "return": float(nav_df["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
            "mdd": float(nav_df["dd"].min()),
            "final_nav": float(nav_df["nav"].iloc[-1]),
            "sharpe": sharpe,
            "avg_w_a": float(nav_df["w_a"].mean()),
            "avg_w_b": float(nav_df["w_b"].mean()),
            "avg_a_count": float(nav_df["a_count"].mean()),
            "avg_b_count": float(nav_df["b_count"].mean()),
            "strong_days": int((nav_df["regime"] == "strong").sum()),
            "normal_days": int((nav_df["regime"] == "normal").sum()),
            "weak_days": int((nav_df["regime"] == "weak").sum()),
            "nav_weak_days": int(nav_df["nav_weak"].fillna(False).sum()),
            "nan_days": int(nav_df["total_ret"].isna().sum()),
            "data_rows": int(len(raw_df)),
            "symbols": int(raw_df["symbol"].nunique()),
        }])

    diagnostics = pd.DataFrame([{
        "input_file": INPUT_FILE,
        "input_columns": ",".join(raw_df.columns.tolist()),
        "has_trade_date": "trade_date" in raw_df.columns,
        "has_symbol": "symbol" in raw_df.columns,
        "has_close": "close" in raw_df.columns,
        "nav_has_nan": bool(nav_df.isna().any().any()) if not nav_df.empty else False,
        "ret_a_nan": int(nav_df["ret_a"].isna().sum()) if not nav_df.empty else 0,
        "ret_b_nan": int(nav_df["ret_b"].isna().sum()) if not nav_df.empty else 0,
    }])

    nav_df.to_csv("v230_nav.csv", index=False)
    summary.to_csv("v230_summary.csv", index=False)
    diagnostics.to_csv("v230_diagnostics.csv", index=False)

    print("DONE v230")
    print(summary.to_string(index=False))
    print(diagnostics.to_string(index=False))


def main():
    base_df = load_base_panel(INPUT_FILE)
    feat_df = build_features(base_df)
    nav_df = backtest_portfolio(feat_df)
    write_reports(nav_df, feat_df)


if __name__ == "__main__":
    main()
