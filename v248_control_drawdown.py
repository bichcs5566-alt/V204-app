# v248_control_drawdown.txt
# 直接覆蓋用
# 使用方式：
# 1. 上傳到 repo 後改名為 v248_control_drawdown.py
# 2. workflow 先跑 merge_chunked_price_panel.py，再跑本檔

import pandas as pd
import numpy as np
import argparse
import json

TOP_N = 6
MAX_GROSS_EXPOSURE = 0.90
MIN_ACTIVE_EXPOSURE = 0.35

STOP_LOSS_BASE = -0.06
STOP_LOSS_STRONG = -0.09

ADD_THRESHOLD = 0.04
ADD_SIZE = 0.30

INITIAL_CAPITAL = 1.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2022-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--label", default="full")
    return parser.parse_args()


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "trade_date" in df.columns:
        df["date"] = pd.to_datetime(df["trade_date"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    else:
        raise ValueError("price_panel_daily.csv 缺少 trade_date / date 欄位")

    required = ["symbol", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"price_panel_daily.csv 缺少必要欄位: {missing}")

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"])
    df = df[df["close"] > 0].copy()

    return df.sort_values(["symbol", "date"]).reset_index(drop=True)


def compute_features(df):
    g = df.groupby("symbol")
    df["ret"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["score"] = df["mom20"] * 0.45 + df["mom10"] * 0.35 + df["mom5"] * 0.20
    return df


def filter_date_range(df, start, end):
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    out = df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].copy()
    if out.empty:
        raise ValueError(
            f"篩選後資料為空 | requested={start_ts.date()}~{end_ts.date()} | "
            f"available={df['date'].min().date()}~{df['date'].max().date()}"
        )
    meta = {
        "requested_start": str(start_ts.date()),
        "requested_end": str(end_ts.date()),
        "available_start": str(df["date"].min().date()),
        "available_end": str(df["date"].max().date()),
        "filtered_rows": int(len(out)),
        "filtered_symbols": int(out["symbol"].nunique()),
    }
    return out, meta


def run_backtest(df):
    dates = sorted(df["date"].unique())
    capital = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    positions = {}
    nav_records = []
    trade_records = []
    signal_records = []

    for d in dates:
        day = df[df["date"] == d].copy()
        day = day.dropna(subset=["score"])
        if day.empty:
            nav_records.append({"date": d, "nav": capital, "count": len(positions), "exposure": 0.0, "dd": capital / peak - 1.0})
            continue

        day_map = {row["symbol"]: row for _, row in day.iterrows()}

        # === Exit / update existing positions ===
        live_symbols = []
        gross_exposure = 0.0

        for s in list(positions.keys()):
            if s not in day_map:
                continue

            row = day_map[s]
            daily_ret = row["ret"]
            if pd.isna(daily_ret):
                daily_ret = 0.0

            pnl = daily_ret
            pos_score = positions[s]["score"]

            stop = STOP_LOSS_STRONG if pos_score > 0 else STOP_LOSS_BASE

            # stop loss
            if pnl < stop:
                trade_records.append({
                    "date": d, "action": "SELL", "symbol": s,
                    "reason": "stop_loss", "ret": float(pnl)
                })
                del positions[s]
                continue

            # add-on only for strong continuation
            if pnl > ADD_THRESHOLD:
                positions[s]["weight"] *= (1 + ADD_SIZE)
                positions[s]["weight"] = min(positions[s]["weight"], 1.0 / TOP_N * 1.8)
                signal_records.append({
                    "date": d, "signal": "ADD_ON", "symbol": s,
                    "ret": float(pnl), "score": float(pos_score)
                })

            positions[s]["score"] = row["score"]
            live_symbols.append(s)
            gross_exposure += positions[s]["weight"]

        # === Entry ===
        ranked = day.sort_values("score", ascending=False).head(20)
        target_symbols = ranked["symbol"].tolist()

        for s in target_symbols:
            if s in positions:
                continue
            if len(positions) >= TOP_N:
                break
            if gross_exposure >= MAX_GROSS_EXPOSURE:
                break

            base_weight = MIN_ACTIVE_EXPOSURE / TOP_N
            positions[s] = {
                "weight": base_weight,
                "score": float(day_map[s]["score"]),
            }
            gross_exposure += base_weight
            trade_records.append({
                "date": d, "action": "BUY", "symbol": s,
                "reason": "rank_entry", "ret": np.nan
            })

        # hard cap on total exposure
        if positions:
            total_w = sum(p["weight"] for p in positions.values())
            if total_w > MAX_GROSS_EXPOSURE:
                scale = MAX_GROSS_EXPOSURE / total_w
                for s in positions:
                    positions[s]["weight"] *= scale
                gross_exposure = MAX_GROSS_EXPOSURE
            else:
                gross_exposure = total_w
        else:
            gross_exposure = 0.0

        # === Compute portfolio return ===
        portfolio_ret = 0.0
        for s in list(positions.keys()):
            if s not in day_map:
                continue
            daily_ret = day_map[s]["ret"]
            if pd.isna(daily_ret):
                daily_ret = 0.0
            portfolio_ret += positions[s]["weight"] * daily_ret

        capital *= (1.0 + portfolio_ret)
        capital = max(capital, 1e-9)
        peak = max(peak, capital)
        dd = capital / peak - 1.0

        nav_records.append({
            "date": d,
            "nav": capital,
            "count": len(positions),
            "exposure": gross_exposure,
            "dd": dd
        })

    nav_df = pd.DataFrame(nav_records)
    trades_df = pd.DataFrame(trade_records)
    signals_df = pd.DataFrame(signal_records)
    return nav_df, trades_df, signals_df


def build_summary(nav_df, trades_df, meta):
    nav_df = nav_df.copy()
    nav_df["ret"] = nav_df["nav"].pct_change().fillna(0.0)

    total_return = nav_df["nav"].iloc[-1] - 1.0
    mdd = nav_df["dd"].min()
    avg_exposure = nav_df["exposure"].mean()
    avg_count = nav_df["count"].mean()
    trade_count = len(trades_df)

    vol = nav_df["ret"].std(ddof=0)
    sharpe = nav_df["ret"].mean() / vol * np.sqrt(252) if vol > 0 else np.nan
    win_day_ratio = (nav_df["ret"] > 0).mean()

    summary = pd.DataFrame([{
        "return": total_return,
        "mdd": mdd,
        "avg_exposure": avg_exposure,
        "avg_count": avg_count,
        "trade_count": trade_count,
        "sharpe_daily": sharpe,
        "win_day_ratio": win_day_ratio,
        "requested_start": meta["requested_start"],
        "requested_end": meta["requested_end"],
        "available_start": meta["available_start"],
        "available_end": meta["available_end"],
        "filtered_rows": meta["filtered_rows"],
        "filtered_symbols": meta["filtered_symbols"],
    }])
    return summary


def main():
    args = parse_args()

    df = load_data()
    df = compute_features(df)
    df, meta = filter_date_range(df, args.start, args.end)

    nav_df, trades_df, signals_df = run_backtest(df)
    summary_df = build_summary(nav_df, trades_df, meta)

    label = args.label
    nav_df.to_csv(f"{label}_nav.csv", index=False)
    trades_df.to_csv(f"{label}_trades.csv", index=False)
    signals_df.to_csv(f"{label}_signals.csv", index=False)
    summary_df.to_csv(f"{label}_summary.csv", index=False)

    with open(f"{label}_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
