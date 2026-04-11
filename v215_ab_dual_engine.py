# v215_ab_dual_engine.py
# 雙引擎 A+B 正式版
# A = 穩定底盤層（低波動動能）
# B = 進攻突破層（高動能突破）
# 使用資料：price_panel_daily.csv
# 輸出：
#   v215_a_positions.csv
#   v215_b_positions.csv
#   v215_daily_log.csv
#   v215_summary.csv

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent

INPUT_PATH = ROOT / "price_panel_daily.csv"

OUT_A_POS = ROOT / "v215_a_positions.csv"
OUT_B_POS = ROOT / "v215_b_positions.csv"
OUT_DAILY = ROOT / "v215_daily_log.csv"
OUT_SUMMARY = ROOT / "v215_summary.csv"

INITIAL_CAPITAL = 100000.0
RET_CLIP = 0.10

# ===== A 引擎：穩定底盤 =====
A_ALLOC = 0.70
A_MOM_LOOKBACK = 5
A_MOM_MIN = 0.03
A_MAX_POSITIONS = 12
A_MIN_POSITIONS = 6
A_NOCHASE_MAX = 0.06
A_WEIGHT_CAP = 0.12
A_VOL_LOOKBACK = 5
A_VOL_Q = 0.80   # 只排除最極端波動 20%

# ===== B 引擎：進攻突破 =====
B_ALLOC = 0.30
B_MOM_LOOKBACK = 5
B_MOM_MIN = 0.10
B_MAX_POSITIONS = 6
B_MIN_POSITIONS = 2
B_NOCHASE_MAX = 0.095
B_WEIGHT_CAP = 0.18
B_VOL_LOOKBACK = 5
B_VOL_MIN_Q = 0.20
B_VOL_MAX_Q = 0.95

# 市場濾網：若當日可交易股票太少，視為資料異常/不可交易日
MIN_MARKET_ROWS = 50


def load_data() -> pd.DataFrame:
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


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()

    x["ret_1d"] = x.groupby("symbol")["close"].pct_change().clip(-RET_CLIP, RET_CLIP)
    x["mom_5d"] = x.groupby("symbol")["close"].pct_change(5)
    x["std_5d"] = (
        x.groupby("symbol")["ret_1d"]
        .rolling(5)
        .std()
        .reset_index(level=0, drop=True)
    )
    x["next_ret_1d"] = x.groupby("symbol")["ret_1d"].shift(-1)
    x["next_date"] = x.groupby("symbol")["date"].shift(-1)
    return x


def capped_equal_weights(n: int, cap: float) -> np.ndarray:
    if n <= 0:
        return np.array([])
    w = np.repeat(1.0 / n, n)
    if (1.0 / n) <= cap:
        return w

    # 如果等權超過 cap，先全部 cap，剩餘權重再平均分配給未達 cap 的位置
    w = np.zeros(n)
    remaining = 1.0
    open_idx = list(range(n))

    while open_idx and remaining > 1e-12:
        eq = remaining / len(open_idx)
        if eq <= cap:
            for i in open_idx:
                w[i] += eq
            remaining = 0.0
        else:
            for i in open_idx:
                w[i] += cap
            remaining = 1.0 - w.sum()
            open_idx = [i for i in open_idx if w[i] + 1e-12 < cap]

        # 避免理論上因 cap 太小導致無法分配
        if len(open_idx) == 0 and remaining > 1e-10:
            w = w / w.sum()

    if w.sum() > 0:
        w = w / w.sum()
    return w


def select_a(df_day: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    d = df_day.copy()
    raw = len(d)

    d = d[d["mom_5d"].notna() & d["std_5d"].notna()].copy()
    after_na = len(d)

    d = d[d["mom_5d"] >= A_MOM_MIN].copy()
    after_mom = len(d)

    d = d[d["ret_1d"].fillna(0) <= A_NOCHASE_MAX].copy()
    after_nochase = len(d)

    if not d.empty:
        vol_hi = d["std_5d"].quantile(A_VOL_Q)
        d = d[d["std_5d"] <= vol_hi].copy()
    after_vol = len(d)

    d = d[d["next_ret_1d"].notna() & d["next_date"].notna()].copy()
    after_next = len(d)

    # A：偏好高動能、低波動
    d["a_score"] = d["mom_5d"] / (d["std_5d"] + 1e-8)
    d = d.sort_values(["a_score", "mom_5d", "symbol"], ascending=[False, False, True]).head(A_MAX_POSITIONS).copy()

    if len(d) < A_MIN_POSITIONS:
        d = d.iloc[0:0].copy()

    if not d.empty:
        w = capped_equal_weights(len(d), A_WEIGHT_CAP)
        d["weight_engine"] = w
    else:
        d["weight_engine"] = np.nan

    stats = {
        "raw": raw,
        "after_na": after_na,
        "after_mom": after_mom,
        "after_nochase": after_nochase,
        "after_vol": after_vol,
        "after_next": after_next,
        "selected": len(d),
    }
    return d, stats


def select_b(df_day: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    d = df_day.copy()
    raw = len(d)

    d = d[d["mom_5d"].notna() & d["std_5d"].notna()].copy()
    after_na = len(d)

    d = d[d["mom_5d"] >= B_MOM_MIN].copy()
    after_mom = len(d)

    d = d[d["ret_1d"].fillna(0) <= B_NOCHASE_MAX].copy()
    after_nochase = len(d)

    if not d.empty:
        vol_lo = d["std_5d"].quantile(B_VOL_MIN_Q)
        vol_hi = d["std_5d"].quantile(B_VOL_MAX_Q)
        d = d[(d["std_5d"] >= vol_lo) & (d["std_5d"] <= vol_hi)].copy()
    after_vol = len(d)

    d = d[d["next_ret_1d"].notna() & d["next_date"].notna()].copy()
    after_next = len(d)

    # B：偏好高動能、允許中高波動，但不碰最亂的尾部
    d["b_score"] = d["mom_5d"]
    d = d.sort_values(["b_score", "symbol"], ascending=[False, True]).head(B_MAX_POSITIONS).copy()

    if len(d) < B_MIN_POSITIONS:
        d = d.iloc[0:0].copy()

    if not d.empty:
        w = capped_equal_weights(len(d), B_WEIGHT_CAP)
        d["weight_engine"] = w
    else:
        d["weight_engine"] = np.nan

    stats = {
        "raw": raw,
        "after_na": after_na,
        "after_mom": after_mom,
        "after_nochase": after_nochase,
        "after_vol": after_vol,
        "after_next": after_next,
        "selected": len(d),
    }
    return d, stats


def finalize_positions(d: pd.DataFrame, engine_name: str, alloc: float, signal_dt) -> pd.DataFrame:
    if d.empty:
        return d.copy()

    out = d.copy()
    out["engine"] = engine_name
    out["signal_date"] = pd.Timestamp(signal_dt)
    out["trade_date"] = pd.to_datetime(out["next_date"])
    out["trade_ret"] = out["next_ret_1d"].clip(-RET_CLIP, RET_CLIP)

    # engine 內權重 × 引擎配置 = 最終投組權重
    out["weight_portfolio"] = out["weight_engine"] * alloc
    out["wret"] = out["trade_ret"] * out["weight_portfolio"]

    keep = [
        "engine", "signal_date", "trade_date", "symbol", "market", "close",
        "mom_5d", "std_5d", "trade_ret", "weight_engine", "weight_portfolio", "wret"
    ]
    out = out[keep].copy()
    out.rename(columns={"close": "signal_close"}, inplace=True)
    out["signal_date"] = out["signal_date"].dt.strftime("%Y-%m-%d")
    out["trade_date"] = out["trade_date"].dt.strftime("%Y-%m-%d")
    return out


def build_summary(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame([{
            "start_date": None,
            "end_date": None,
            "initial_capital": INITIAL_CAPITAL,
            "final_nav": INITIAL_CAPITAL,
            "total_return": 0.0,
            "trading_days": 0,
            "nonzero_ret_days": 0,
            "avg_a_count": 0.0,
            "avg_b_count": 0.0,
            "avg_total_count": 0.0,
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
    mdd = float(drawdown.min())

    return pd.DataFrame([{
        "start_date": str(daily_df.iloc[0]["trade_date"]),
        "end_date": str(daily_df.iloc[-1]["trade_date"]),
        "initial_capital": INITIAL_CAPITAL,
        "final_nav": final_nav,
        "total_return": total_return,
        "trading_days": int(len(daily_df)),
        "nonzero_ret_days": int((ret.abs() > 0).sum()),
        "avg_a_count": float(daily_df["a_count"].mean()),
        "avg_b_count": float(daily_df["b_count"].mean()),
        "avg_total_count": float(daily_df["total_count"].mean()),
        "avg_exposure": float(daily_df["gross_exposure"].mean()),
        "cash_days": int((daily_df["gross_exposure"] <= 0).sum()),
        "sharpe": sharpe,
        "mdd": mdd,
    }])


def run_backtest(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = sorted(df["date"].dropna().unique())

    capital = INITIAL_CAPITAL
    daily_rows = []
    a_rows = []
    b_rows = []

    for signal_dt in dates:
        df_day = df[df["date"] == signal_dt].copy()
        market_rows = len(df_day)

        signal_date_str = pd.Timestamp(signal_dt).strftime("%Y-%m-%d")

        # 資料不足 / 非正常交易日，直接跳過
        if market_rows < MIN_MARKET_ROWS:
            daily_rows.append({
                "signal_date": signal_date_str,
                "trade_date": None,
                "market_rows": market_rows,
                "a_count": 0,
                "b_count": 0,
                "total_count": 0,
                "gross_exposure": 0.0,
                "daily_ret": 0.0,
                "capital": capital,
                "cash_mode": True,
            })
            continue

        a_sel, a_stats = select_a(df_day)
        b_sel, b_stats = select_b(df_day)

        a_pos = finalize_positions(a_sel, "A", A_ALLOC, signal_dt)
        b_pos = finalize_positions(b_sel, "B", B_ALLOC, signal_dt)

        # 同 symbol 若同日同時被 A、B 選到，保留兩筆沒問題，因為代表兩引擎同意加碼
        merged_pos = pd.concat([a_pos, b_pos], ignore_index=True)

        if merged_pos.empty:
            daily_rows.append({
                "signal_date": signal_date_str,
                "trade_date": None,
                "market_rows": market_rows,
                "a_count": 0,
                "b_count": 0,
                "total_count": 0,
                "gross_exposure": 0.0,
                "daily_ret": 0.0,
                "capital": capital,
                "cash_mode": True,
            })
            continue

        grouped = (
            merged_pos.groupby("trade_date", as_index=False)
            .agg(
                daily_ret=("wret", "sum"),
                a_count=("engine", lambda s: int((pd.Series(s) == "A").sum())),
                b_count=("engine", lambda s: int((pd.Series(s) == "B").sum())),
                total_count=("symbol", "count"),
                gross_exposure=("weight_portfolio", "sum"),
            )
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

        for _, g in grouped.iterrows():
            daily_ret = float(g["daily_ret"])
            capital = capital * (1.0 + daily_ret)

            daily_rows.append({
                "signal_date": signal_date_str,
                "trade_date": str(g["trade_date"]),
                "market_rows": market_rows,
                "a_count": int(g["a_count"]),
                "b_count": int(g["b_count"]),
                "total_count": int(g["total_count"]),
                "gross_exposure": float(g["gross_exposure"]),
                "daily_ret": daily_ret,
                "capital": capital,
                "cash_mode": False,
            })

        if not a_pos.empty:
            a_rows.extend(a_pos.to_dict("records"))
        if not b_pos.empty:
            b_rows.extend(b_pos.to_dict("records"))

    daily_df = pd.DataFrame(daily_rows)
    if not daily_df.empty:
        daily_df = daily_df.sort_values(["trade_date", "signal_date"], na_position="last").reset_index(drop=True)

    a_df = pd.DataFrame(a_rows)
    b_df = pd.DataFrame(b_rows)
    return daily_df, a_df, b_df


def main():
    df = load_data()
    df = add_features(df)

    daily_df, a_df, b_df = run_backtest(df)
    summary_df = build_summary(daily_df)

    a_df.to_csv(OUT_A_POS, index=False)
    b_df.to_csv(OUT_B_POS, index=False)
    daily_df.to_csv(OUT_DAILY, index=False)
    summary_df.to_csv(OUT_SUMMARY, index=False)

    print("v215 dual engine done")
    print("price rows:", len(df))
    print("a positions:", len(a_df))
    print("b positions:", len(b_df))
    print(summary_df.to_dict("records")[0])


if __name__ == "__main__":
    main()
