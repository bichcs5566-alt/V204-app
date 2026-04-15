import os
import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"   # 選填：trade_date,macro_score
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
        raise Exception(f"缺少日期欄位，可接受欄位: {date_candidates}，目前欄位: {df.columns.tolist()}")

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
    df["mom20"] = g["close"].pct_change(20)
    df["vol5"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)

    market = (
        df.groupby("trade_date", as_index=False)["close"]
        .mean()
        .rename(columns={"close": "market_close"})
    )
    market["ma5"] = market["market_close"].rolling(5).mean()
    market["ma10"] = market["market_close"].rolling(10).mean()
    market["ma20"] = market["market_close"].rolling(20).mean()

    def price_regime_fn(row):
        if pd.notna(row["ma5"]) and pd.notna(row["ma10"]) and pd.notna(row["ma20"]):
            if row["ma5"] > row["ma10"] > row["ma20"]:
                return "strong"
            elif row["ma10"] > row["ma20"]:
                return "normal"
            else:
                return "weak"
        return "weak"

    market["price_regime"] = market.apply(price_regime_fn, axis=1)
    df = df.merge(
        market[["trade_date", "price_regime", "ma5", "ma10", "ma20"]],
        on="trade_date",
        how="left"
    )
    return df


def load_macro_layer(df: pd.DataFrame) -> pd.DataFrame:
    unique_dates = pd.DataFrame({"trade_date": sorted(df["trade_date"].dropna().unique())})

    if os.path.exists(MACRO_FILE):
        macro = pd.read_csv(MACRO_FILE)
        macro.columns = [str(c).replace("\ufeff", "").strip().lower() for c in macro.columns]

        if "trade_date" not in macro.columns:
            if "date" in macro.columns:
                macro["trade_date"] = macro["date"]
            else:
                raise Exception("macro_signal.csv 缺少 trade_date/date")

        if "macro_score" not in macro.columns:
            raise Exception("macro_signal.csv 缺少 macro_score")

        macro["trade_date"] = pd.to_datetime(macro["trade_date"], errors="coerce")
        macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
        macro = macro.dropna(subset=["trade_date", "macro_score"]).copy()
        macro = macro[["trade_date", "macro_score"]].sort_values("trade_date").drop_duplicates("trade_date")

        merged = unique_dates.merge(macro, on="trade_date", how="left").sort_values("trade_date")
        merged["macro_score"] = merged["macro_score"].ffill().fillna(0.0)
        merged["macro_source"] = "file"
    else:
        price_map = {"strong": 0.5, "normal": 0.0, "weak": -0.5}
        fallback = (
            df.groupby("trade_date", as_index=False)["price_regime"]
            .first()
            .rename(columns={"price_regime": "tmp_regime"})
        )
        fallback["macro_score"] = fallback["tmp_regime"].map(price_map).fillna(0.0)
        fallback["macro_source"] = "fallback_price_regime"
        merged = fallback[["trade_date", "macro_score", "macro_source"]]

    def macro_regime_fn(x):
        if x >= 0.5:
            return "macro_strong"
        elif x >= 0.0:
            return "macro_normal"
        else:
            return "macro_weak"

    merged["macro_regime"] = merged["macro_score"].apply(macro_regime_fn)
    return merged


def run_engine_a(day: pd.DataFrame):
    d = day.dropna(subset=["mom3", "mom5", "mom10", "ret_1d"]).copy()
    d = d[(d["mom3"] > 0.01) & (d["mom5"] > 0.015) & (d["mom10"] > -0.03)]

    if d.empty:
        return 0.0, 0, 0.0

    d["score"] = (
        d["mom3"].fillna(0) * 0.45 +
        d["mom5"].fillna(0) * 0.30 +
        d["mom10"].fillna(0) * 0.15 -
        d["vol5"].fillna(0.2) * 0.10
    )

    d = d.sort_values("score", ascending=False).head(5).copy()
    d["ret_1d"] = d["ret_1d"].clip(lower=-0.08)
    return float(d["ret_1d"].mean()), int(len(d)), float(d["score"].mean())


def run_engine_b(day: pd.DataFrame):
    d = day.dropna(subset=["mom5", "mom10", "mom20", "ret_1d"]).copy()
    if d.empty:
        return 0.0, 0, 0.0

    d["score"] = (
        d["mom10"].fillna(0) * 0.50 +
        d["mom5"].fillna(0) * 0.20 +
        d["mom20"].fillna(0) * 0.20 -
        d["vol5"].fillna(0.2) * 0.10
    )

    d = d.sort_values("score", ascending=False).head(10).copy()
    d = d[(d["mom10"] > -0.06) & (d["mom5"] > -0.06)]

    if d.empty:
        return 0.0, 0, 0.0

    d["ret_1d"] = d["ret_1d"].clip(lower=-0.05)
    return float(d["ret_1d"].mean()), int(len(d)), float(d["score"].mean())


def backtest_portfolio(df: pd.DataFrame):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    nav_hist = []
    rows = []

    prev_w_a = 0.0
    prev_w_b = 0.0

    for d in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == d].copy()
        if day.empty:
            continue

        price_regime = str(day["price_regime"].iloc[0])
        macro_regime = str(day["macro_regime"].iloc[0])
        macro_score = float(day["macro_score"].iloc[0])

        if price_regime == "strong":
            base_w_a, base_w_b = 0.70, 0.60
        elif price_regime == "normal":
            base_w_a, base_w_b = 0.35, 0.50
        else:
            base_w_a, base_w_b = 0.05, 0.30

        if macro_regime == "macro_strong":
            macro_mult_a, macro_mult_b = 1.10, 1.05
        elif macro_regime == "macro_normal":
            macro_mult_a, macro_mult_b = 1.00, 1.00
        else:
            macro_mult_a, macro_mult_b = 0.60, 0.80

        target_w_a = base_w_a * macro_mult_a
        target_w_b = base_w_b * macro_mult_b

        if len(nav_hist) >= 10:
            nav5 = float(pd.Series(nav_hist[-5:]).mean())
            nav10 = float(pd.Series(nav_hist[-10:]).mean())
            nav_weak = nav5 < nav10 * 0.997
        else:
            nav_weak = False

        if nav_weak:
            target_w_a *= 0.70

        current_dd = nav / peak - 1.0
        if current_dd <= -0.05:
            target_w_a *= 0.80
            target_w_b *= 0.90
        if current_dd <= -0.08:
            target_w_a *= 0.60
            target_w_b *= 0.75

        w_a = prev_w_a * 0.50 + target_w_a * 0.50
        w_b = prev_w_b * 0.50 + target_w_b * 0.50

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
            d, nav, price_regime, macro_regime, macro_score,
            w_a, w_b, ret_a, ret_b, total_ret,
            a_count, b_count, a_score, b_score,
            nav_weak, dd
        ])

        prev_w_a = w_a
        prev_w_b = w_b

    return pd.DataFrame(rows, columns=[
        "trade_date", "nav", "price_regime", "macro_regime", "macro_score",
        "w_a", "w_b", "ret_a", "ret_b", "total_ret",
        "a_count", "b_count", "a_score", "b_score",
        "nav_weak", "dd"
    ])


def write_reports(nav_df: pd.DataFrame, raw_df: pd.DataFrame, macro_df: pd.DataFrame):
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
            "macro_source": macro_df["macro_source"].iloc[0] if not macro_df.empty else "unknown",
            "nav_weak_days": 0,
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
            "avg_macro_score": float(nav_df["macro_score"].mean()),
            "price_strong_days": int((nav_df["price_regime"] == "strong").sum()),
            "price_normal_days": int((nav_df["price_regime"] == "normal").sum()),
            "price_weak_days": int((nav_df["price_regime"] == "weak").sum()),
            "macro_strong_days": int((nav_df["macro_regime"] == "macro_strong").sum()),
            "macro_normal_days": int((nav_df["macro_regime"] == "macro_normal").sum()),
            "macro_weak_days": int((nav_df["macro_regime"] == "macro_weak").sum()),
            "nav_weak_days": int(nav_df["nav_weak"].fillna(False).sum()),
            "macro_source": macro_df["macro_source"].iloc[0] if not macro_df.empty else "unknown",
            "data_rows": int(len(raw_df)),
            "symbols": int(raw_df["symbol"].nunique()),
        }])

    diagnostics = pd.DataFrame([{
        "input_file": INPUT_FILE,
        "macro_file_exists": os.path.exists(MACRO_FILE),
        "macro_file_used": macro_df["macro_source"].iloc[0] if not macro_df.empty else "unknown",
        "input_columns": ",".join(raw_df.columns.tolist()),
        "nav_has_nan": bool(nav_df.isna().any().any()) if not nav_df.empty else False,
        "ret_a_nan": int(nav_df["ret_a"].isna().sum()) if not nav_df.empty else 0,
        "ret_b_nan": int(nav_df["ret_b"].isna().sum()) if not nav_df.empty else 0,
    }])

    nav_df.to_csv("v231_nav.csv", index=False)
    summary.to_csv("v231_summary.csv", index=False)
    diagnostics.to_csv("v231_diagnostics.csv", index=False)

    print("DONE v231")
    print(summary.to_string(index=False))
    print(diagnostics.to_string(index=False))


def main():
    base_df = load_base_panel(INPUT_FILE)
    feat_df = build_features(base_df)
    macro_df = load_macro_layer(feat_df)
    feat_df = feat_df.merge(
        macro_df[["trade_date", "macro_score", "macro_regime", "macro_source"]],
        on="trade_date",
        how="left"
    )
    nav_df = backtest_portfolio(feat_df)
    write_reports(nav_df, feat_df, macro_df)


if __name__ == "__main__":
    main()
