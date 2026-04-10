# =========================================================
# 檔案 2：v2088_daily_rebuild.py
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


def load_positions():
    if not POS_PATH.exists():
        raise FileNotFoundError(f"找不到 {POS_PATH}")

    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    for c in ["weight", "day_ret", "cum_ret", "price", "entry_price", "close", "close_used", "last_price", "px"]:
        if c in pos.columns:
            pos[c] = to_num(pos[c])

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0

    if "weight" not in pos.columns or pos["weight"].isna().all():
        if len(pos) > 0:
            pos["weight"] = 1.0 / len(pos)
        else:
            pos["weight"] = pd.Series(dtype=float)

    if "symbol" not in pos.columns:
        pos["symbol"] = np.arange(1, len(pos) + 1).astype(str)

    latest_day = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)
    if "trade_date" in pos.columns:
        pos["trade_date"] = latest_day.strftime("%Y-%m-%d")
    if "signal_date" in pos.columns:
        pos["signal_date"] = latest_day.strftime("%Y-%m-%d")

    return pos


def rebuild_nav():
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

    start_date = nav["date"].min()
    end_date = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)

    full_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    full_nav = pd.DataFrame({"date": full_dates})

    nav = full_nav.merge(nav, on="date", how="left")

    nav["ret"] = nav["ret"].fillna(0.0)
    nav["nav"] = nav["nav"].ffill()

    if nav["nav"].isna().any():
        first_valid = nav["nav"].dropna()
        if len(first_valid) == 0:
            nav["nav"] = 100000.0
        else:
            nav["nav"] = nav["nav"].fillna(first_valid.iloc[0])

    return nav


def apply_position_metrics(nav, pos):
    holdings_val = int(pos["symbol"].nunique()) if len(pos) else 0
    exposure_val = float(pos["weight"].fillna(0).sum()) if "weight" in pos.columns and len(pos) else 0.0

    nav["holdings"] = nav["holdings"].ffill().fillna(holdings_val)
    nav["avg_exposure"] = nav["avg_exposure"].ffill().fillna(exposure_val)

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
        "avg_holdings": float(nav["holdings"].mean()),
        "avg_exposure": float(nav["avg_exposure"].mean())
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
    df.to_csv(TIER_MAP["all"], index=False)

    overview_rows = []
    for tier in ["lt10", "10_30", "30_50", "50_100", "100p", "unknown"]:
        sub = df[df["price_tier"] == tier].copy()
        sub.to_csv(TIER_MAP[tier], index=False)

        overview_rows.append({
            "price_tier": tier,
            "rows": len(sub),
            "symbol_count": sub["symbol"].nunique() if "symbol" in sub.columns else np.nan,
            "avg_weight": float(sub["weight"].mean()) if "weight" in sub.columns and len(sub) else np.nan,
            "avg_day_ret": float(sub["day_ret"].mean()) if "day_ret" in sub.columns and len(sub) else np.nan,
            "avg_cum_ret": float(sub["cum_ret"].mean()) if "cum_ret" in sub.columns and len(sub) else np.nan,
            "latest_trade_date": str(sub["trade_date"].max()) if "trade_date" in sub.columns and len(sub) else "",
            "latest_signal_date": str(sub["signal_date"].max()) if "signal_date" in sub.columns and len(sub) else ""
        })

    overview = pd.DataFrame(overview_rows)
    overview.to_csv(OVERVIEW_PATH, index=False)


def write_update_log(nav, pos, summary):
    row = pd.DataFrame([{
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "latest_nav_date": str(pd.to_datetime(nav["date"]).max().date()),
        "latest_trade_date": str(pos["trade_date"].max()) if "trade_date" in pos.columns and len(pos) else "",
        "latest_signal_date": str(pos["signal_date"].max()) if "signal_date" in pos.columns and len(pos) else "",
        "final_nav": float(summary.iloc[0]["final_nav"]),
        "sharpe": float(summary.iloc[0]["sharpe"]),
        "total_return": float(summary.iloc[0]["total_return"]),
        "positions_rows": int(len(pos))
    }])

    if UPDATE_LOG_PATH.exists():
        old = pd.read_csv(UPDATE_LOG_PATH)
        out = pd.concat([old, row], ignore_index=True)
    else:
        out = row

    out.to_csv(UPDATE_LOG_PATH, index=False)


def main():
    pos = load_positions()
    nav = rebuild_nav()
    nav = apply_position_metrics(nav, pos)
    summary = build_summary(nav)

    nav["date"] = pd.to_datetime(nav["date"]).dt.strftime("%Y-%m-%d")
    nav.to_csv(NAV_PATH, index=False)
    pos.to_csv(POS_PATH, index=False)
    summary.to_csv(SUM_PATH, index=False)

    build_tiers(pos)
    write_update_log(nav, pos, summary)

    print("v208.8 DONE")
    print("已補齊歷史缺口並重建主輸出")
    print(f"NAV -> {NAV_PATH}")
    print(f"SUMMARY -> {SUM_PATH}")
    print(f"UPDATE LOG -> {UPDATE_LOG_PATH}")


if __name__ == "__main__":
    main()
