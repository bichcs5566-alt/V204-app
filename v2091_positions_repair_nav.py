# =========================================================
# 檔案 2：v2091_positions_repair_nav.py
# =========================================================
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
SUM_PATH = ROOT / "v202_summary.csv"
UPDATE_LOG_PATH = ROOT / "update_log.csv"
REPAIR_LOG_PATH = ROOT / "v2091_repair_log.csv"

TIER_MAP = {
    "all": ROOT / "tier_all_positions.csv",
    "lt10": ROOT / "tier_lt10_positions.csv",
    "10_30": ROOT / "tier_10_30_positions.csv",
    "30_50": ROOT / "tier_30_50_positions.csv",
    "50_100": ROOT / "tier_50_100_positions.csv",
    "100p": ROOT / "tier_100p_positions.csv",
    "unknown": ROOT / "tier_unknown_positions.csv",
}
OVERVIEW_PATH = ROOT / "tier_overview_summary.csv"


def to_num(series):
    return pd.to_numeric(series, errors="coerce")


def classify_price(x):
    if pd.isna(x):
        return "unknown"
    if x < 10:
        return "lt10"
    if 10 <= x < 30:
        return "10_30"
    if 30 <= x < 50:
        return "30_50"
    if 50 <= x < 100:
        return "50_100"
    if x >= 100:
        return "100p"
    return "unknown"


def load_existing_nav():
    if not NAV_PATH.exists():
        raise FileNotFoundError(f"找不到 {NAV_PATH}")

    nav = pd.read_csv(NAV_PATH)
    nav.columns = [str(c).strip() for c in nav.columns]

    if "date" not in nav.columns or "nav" not in nav.columns:
        raise ValueError("v202_nav.csv 必須包含 date 與 nav 欄位")

    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = to_num(nav["nav"])

    if "ret" in nav.columns:
        nav["ret"] = to_num(nav["ret"])
    else:
        nav["ret"] = np.nan

    if "holdings" in nav.columns:
        nav["holdings"] = to_num(nav["holdings"])
    else:
        nav["holdings"] = np.nan

    if "avg_exposure" in nav.columns:
        nav["avg_exposure"] = to_num(nav["avg_exposure"])
    else:
        nav["avg_exposure"] = np.nan

    nav = nav.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)

    if nav.empty:
        raise ValueError("v202_nav.csv 沒有有效資料")

    return nav


def load_and_repair_positions():
    if not POS_PATH.exists():
        raise FileNotFoundError(f"找不到 {POS_PATH}")

    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    raw_rows = len(pos)

    # 基本欄位
    if "weight" not in pos.columns:
        pos["weight"] = np.nan
    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0
    if "trade_date" not in pos.columns:
        pos["trade_date"] = pd.NaT
    if "symbol" not in pos.columns:
        pos["symbol"] = np.arange(1, len(pos) + 1).astype(str)

    # 型別轉換
    pos["weight"] = to_num(pos["weight"])
    pos["day_ret"] = to_num(pos["day_ret"])
    pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")

    if "signal_date" in pos.columns:
        pos["signal_date"] = pd.to_datetime(pos["signal_date"], errors="coerce")

    # 僅補空值，不整欄覆蓋
    latest_day = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)
    pos["trade_date"] = pos["trade_date"].fillna(latest_day)

    if "signal_date" in pos.columns:
        pos["signal_date"] = pos["signal_date"].fillna(pos["trade_date"])

    # 填補空值
    pos["day_ret"] = pos["day_ret"].fillna(0.0)

    if pos["weight"].isna().all():
        if len(pos) > 0:
            pos["weight"] = 1.0 / len(pos)
        else:
            pos["weight"] = pd.Series(dtype=float)
    else:
        pos["weight"] = pos["weight"].fillna(0.0)

    # 清洗異常值
    # 1) 單檔 weight 限制
    pos["weight_raw"] = pos["weight"]
    pos["weight"] = pos["weight"].clip(lower=-1.0, upper=1.0)

    # 2) 單檔 day_ret 限制（避免污染爆衝）
    pos["day_ret_raw"] = pos["day_ret"]
    pos["day_ret"] = pos["day_ret"].clip(lower=-0.30, upper=0.30)

    # 3) 每日權重過大時正規化
    repair_logs = []

 def normalize_group(g):
    g = g.copy()

    trade_date_val = g.name if hasattr(g, "name") else None
    if pd.isna(trade_date_val):
        trade_date_str = ""
    else:
        trade_date_str = str(pd.to_datetime(trade_date_val).date())

    total_abs_weight = g["weight"].abs().sum()
    total_weight = g["weight"].sum()

    repaired = False
    old_total_weight = float(total_weight)
    old_total_abs_weight = float(total_abs_weight)

    if total_abs_weight > 1.5 and total_abs_weight > 0:
        g["weight"] = g["weight"] / total_abs_weight
        repaired = True

    repair_logs.append({
        "trade_date": trade_date_str,
        "rows": int(len(g)),
        "old_total_weight": old_total_weight,
        "old_total_abs_weight": old_total_abs_weight,
        "new_total_weight": float(g["weight"].sum()),
        "new_total_abs_weight": float(g["weight"].abs().sum()),
        "normalized": repaired
    })

    return g


def build_daily_return_from_positions(pos):
    daily_rows = []

    for d, g in pos.groupby("trade_date"):
        holdings = int(g["symbol"].nunique()) if "symbol" in g.columns else int(len(g))
        avg_exposure = float(g["weight"].abs().sum())

        # 防呆：過高 holdings / exposure 直接標記縮限
        if holdings > 200:
            holdings = 200

        if avg_exposure > 1.5:
            avg_exposure = 1.5

        ret = float((g["weight"].fillna(0) * g["day_ret"].fillna(0)).sum())

        # 每日策略報酬再做最後一層防呆
        ret = max(min(ret, 0.10), -0.10)

        daily_rows.append({
            "date": pd.to_datetime(d),
            "ret": ret,
            "holdings": holdings,
            "avg_exposure": avg_exposure
        })

    daily = pd.DataFrame(daily_rows).sort_values("date").reset_index(drop=True)
    return daily


def rebuild_nav_from_returns(base_nav, daily_ret_df):
    start_date = pd.to_datetime(base_nav["date"].min())
    end_date = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)

    full_dates = pd.DataFrame({"date": pd.date_range(start=start_date, end=end_date, freq="D")})
    nav = full_dates.merge(daily_ret_df, on="date", how="left")

    nav["ret"] = nav["ret"].fillna(0.0)

    if "holdings" not in nav.columns:
        nav["holdings"] = np.nan
    if "avg_exposure" not in nav.columns:
        nav["avg_exposure"] = np.nan

    # 既有值往前補
    if "holdings" in base_nav.columns:
        old_hold = base_nav[["date", "holdings"]].copy()
        old_hold["date"] = pd.to_datetime(old_hold["date"])
        nav = nav.merge(old_hold, on="date", how="left", suffixes=("", "_old"))
        nav["holdings"] = nav["holdings"].combine_first(nav["holdings_old"])
        nav = nav.drop(columns=["holdings_old"])

    if "avg_exposure" in base_nav.columns:
        old_exp = base_nav[["date", "avg_exposure"]].copy()
        old_exp["date"] = pd.to_datetime(old_exp["date"])
        nav = nav.merge(old_exp, on="date", how="left", suffixes=("", "_old"))
        nav["avg_exposure"] = nav["avg_exposure"].combine_first(nav["avg_exposure_old"])
        nav = nav.drop(columns=["avg_exposure_old"])

    nav["holdings"] = nav["holdings"].ffill().fillna(0)
    nav["avg_exposure"] = nav["avg_exposure"].ffill().fillna(0.0)

    initial_nav = float(base_nav.iloc[0]["nav"])
    nav.loc[0, "nav"] = initial_nav

    for i in range(1, len(nav)):
        prev_nav = float(nav.loc[i - 1, "nav"])
        today_ret = float(nav.loc[i, "ret"])
        nav.loc[i, "nav"] = prev_nav * (1 + today_ret)

    return nav


def build_summary(nav):
    initial_capital = float(nav["nav"].iloc[0])
    final_nav = float(nav["nav"].iloc[-1])

    total_return = (final_nav / initial_capital) - 1.0 if initial_capital != 0 else 0.0
    daily_ret = nav["nav"].pct_change().dropna()

    sharpe = 0.0
    if len(daily_ret) > 0 and float(daily_ret.std()) != 0:
        sharpe = float((daily_ret.mean() / daily_ret.std()) * np.sqrt(252))

    mdd = float((nav["nav"] / nav["nav"].cummax() - 1.0).min()) if len(nav) else 0.0

    summary = pd.DataFrame([{
        "start_date": str(pd.to_datetime(nav["date"]).min().date()),
        "end_date": str(pd.to_datetime(nav["date"]).max().date()),
        "initial_capital": initial_capital,
        "final_nav": final_nav,
        "total_return": total_return,
        "sharpe": sharpe,
        "mdd": mdd,
        "trading_days": int(len(nav)),
        "avg_holdings": float(pd.to_numeric(nav["holdings"], errors="coerce").mean()),
        "avg_exposure": float(pd.to_numeric(nav["avg_exposure"], errors="coerce").mean())
    }])

    return summary


def build_tiers(pos):
    df = pos.copy()

    price_col = None
    for c in ["price", "entry_price", "close", "close_used", "last_price", "px"]:
        if c in df.columns:
            price_col = c
            break

    if price_col is None:
        df["__price__"] = np.nan
    else:
        df["__price__"] = to_num(df[price_col])

    df["price_tier"] = df["__price__"].apply(classify_price)

    df_save = df.copy()
    df_save["trade_date"] = pd.to_datetime(df_save["trade_date"]).dt.strftime("%Y-%m-%d")
    if "signal_date" in df_save.columns:
        df_save["signal_date"] = pd.to_datetime(df_save["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_save.to_csv(TIER_MAP["all"], index=False)

    overview_rows = []
    for tier in ["lt10", "10_30", "30_50", "50_100", "100p", "unknown"]:
        sub = df[df["price_tier"] == tier].copy()

        sub_save = sub.copy()
        sub_save["trade_date"] = pd.to_datetime(sub_save["trade_date"]).dt.strftime("%Y-%m-%d")
        if "signal_date" in sub_save.columns:
            sub_save["signal_date"] = pd.to_datetime(sub_save["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        sub_save.to_csv(TIER_MAP[tier], index=False)

        overview_rows.append({
            "price_tier": tier,
            "rows": len(sub),
            "symbol_count": sub["symbol"].nunique() if "symbol" in sub.columns else np.nan,
            "avg_weight": float(sub["weight"].mean()) if "weight" in sub.columns and len(sub) else np.nan,
            "avg_day_ret": float(sub["day_ret"].mean()) if "day_ret" in sub.columns and len(sub) else np.nan,
            "avg_cum_ret": float(sub["cum_ret"].mean()) if "cum_ret" in sub.columns and len(sub) else np.nan,
            "latest_trade_date": str(pd.to_datetime(sub["trade_date"]).max().date()) if "trade_date" in sub.columns and len(sub) else "",
            "latest_signal_date": (
                str(pd.to_datetime(sub["signal_date"], errors="coerce").max().date())
                if "signal_date" in sub.columns and len(sub) else ""
            )
        })

    pd.DataFrame(overview_rows).to_csv(OVERVIEW_PATH, index=False)


def write_update_log(nav, pos, summary, raw_rows):
    row = pd.DataFrame([{
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "latest_nav_date": str(pd.to_datetime(nav["date"]).max().date()),
        "latest_trade_date": str(pd.to_datetime(pos["trade_date"]).max().date()) if "trade_date" in pos.columns and len(pos) else "",
        "latest_signal_date": (
            str(pd.to_datetime(pos["signal_date"], errors="coerce").max().date())
            if "signal_date" in pos.columns and len(pos) else ""
        ),
        "final_nav": float(summary.iloc[0]["final_nav"]),
        "sharpe": float(summary.iloc[0]["sharpe"]),
        "total_return": float(summary.iloc[0]["total_return"]),
        "positions_rows_raw": int(raw_rows),
        "positions_rows_clean": int(len(pos))
    }])

    if UPDATE_LOG_PATH.exists():
        old = pd.read_csv(UPDATE_LOG_PATH)
        out = pd.concat([old, row], ignore_index=True)
    else:
        out = row

    out.to_csv(UPDATE_LOG_PATH, index=False)


def main():
    base_nav = load_existing_nav()
    pos, raw_rows = load_and_repair_positions()
    daily_ret_df = build_daily_return_from_positions(pos)
    nav = rebuild_nav_from_returns(base_nav, daily_ret_df)
    summary = build_summary(nav)

    nav_save = nav.copy()
    nav_save["date"] = pd.to_datetime(nav_save["date"]).dt.strftime("%Y-%m-%d")
    nav_save.to_csv(NAV_PATH, index=False)

    pos_save = pos.copy()
    pos_save["trade_date"] = pd.to_datetime(pos_save["trade_date"]).dt.strftime("%Y-%m-%d")
    if "signal_date" in pos_save.columns:
        pos_save["signal_date"] = pd.to_datetime(pos_save["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    pos_save.to_csv(POS_PATH, index=False)

    summary.to_csv(SUM_PATH, index=False)

    build_tiers(pos)
    write_update_log(nav, pos, summary, raw_rows)

    print("v209.1 DONE")
    print("已修復 positions 污染並重建真實 NAV")
    print(f"NAV -> {NAV_PATH}")
    print(f"SUMMARY -> {SUM_PATH}")
    print(f"UPDATE LOG -> {UPDATE_LOG_PATH}")
    print(f"REPAIR LOG -> {REPAIR_LOG_PATH}")


if __name__ == "__main__":
    main()
