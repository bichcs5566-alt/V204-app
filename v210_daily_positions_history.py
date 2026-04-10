# =========================================================
# 檔案 2：v210_daily_positions_history.py
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
EXPAND_LOG_PATH = ROOT / "v210_expand_log.csv"
DAILY_SAMPLE_PATH = ROOT / "v210_daily_positions_sample.csv"

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
    if x < 30:
        return "10_30"
    if x < 50:
        return "30_50"
    if x < 100:
        return "50_100"
    return "100p"


def load_nav():
    if not NAV_PATH.exists():
        raise FileNotFoundError(f"找不到 {NAV_PATH}")

    nav = pd.read_csv(NAV_PATH)
    nav.columns = [str(c).strip() for c in nav.columns]

    if "date" not in nav.columns or "nav" not in nav.columns:
        raise ValueError("v202_nav.csv 必須包含 date 與 nav 欄位")

    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = to_num(nav["nav"])

    nav = nav.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)
    if nav.empty:
        raise ValueError("v202_nav.csv 沒有有效資料")

    return nav


def load_snapshot_positions():
    if not POS_PATH.exists():
        raise FileNotFoundError(f"找不到 {POS_PATH}")

    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    if len(pos) == 0:
        raise ValueError("v202_positions.csv 是空的")

    if "symbol" not in pos.columns:
        pos["symbol"] = np.arange(1, len(pos) + 1).astype(str)

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / len(pos)

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0

    pos["weight"] = to_num(pos["weight"]).fillna(0.0)
    pos["day_ret"] = to_num(pos["day_ret"]).fillna(0.0)

    if "cum_ret" in pos.columns:
        pos["cum_ret"] = to_num(pos["cum_ret"])

    for c in ["price", "entry_price", "close", "close_used", "last_price", "px"]:
        if c in pos.columns:
            pos[c] = to_num(pos[c])

    if "trade_date" in pos.columns:
        pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")

    if "signal_date" in pos.columns:
        pos["signal_date"] = pd.to_datetime(pos["signal_date"], errors="coerce")

    # 快照版：只取最新那批持倉當模板
    if "trade_date" in pos.columns and pos["trade_date"].notna().any():
        latest_trade_date = pos["trade_date"].max()
        snapshot = pos[pos["trade_date"] == latest_trade_date].copy()
        if len(snapshot) == 0:
            snapshot = pos.copy()
    else:
        latest_trade_date = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)
        snapshot = pos.copy()

    # 單筆防呆
    snapshot["weight"] = snapshot["weight"].clip(-1.0, 1.0)
    snapshot["day_ret"] = snapshot["day_ret"].clip(-0.30, 0.30)

    abs_sum = snapshot["weight"].abs().sum()
    if abs_sum > 1.5 and abs_sum > 0:
        snapshot["weight"] = snapshot["weight"] / abs_sum

    return snapshot, latest_trade_date


def expand_positions_daily(snapshot, nav):
    full_dates = pd.date_range(
        start=nav["date"].min(),
        end=nav["date"].max(),
        freq="D"
    )

    rows = []
    snapshot = snapshot.copy()

    for d in full_dates:
        g = snapshot.copy()
        g["trade_date"] = d
        if "signal_date" in g.columns:
            g["signal_date"] = g["signal_date"].fillna(d)
        else:
            g["signal_date"] = d

        # 先做可運行歷史版：除最新日外，其餘 day_ret = 0
        # 最新日保留原始快照 day_ret
        if d != full_dates[-1]:
            g["day_ret"] = 0.0

        rows.append(g)

    out = pd.concat(rows, ignore_index=True)

    # 加入 price_tier
    price_col = None
    for c in ["price", "entry_price", "close", "close_used", "last_price", "px"]:
        if c in out.columns:
            price_col = c
            break

    if price_col is None:
        out["__price__"] = np.nan
    else:
        out["__price__"] = to_num(out[price_col])

    out["price_tier"] = out["__price__"].apply(classify_price)

    return out


def build_daily_returns_from_positions(pos):
    daily_rows = []

    for d, g in pos.groupby("trade_date", sort=True):
        ret = float((g["weight"].fillna(0.0) * g["day_ret"].fillna(0.0)).sum())
        ret = max(min(ret, 0.10), -0.10)

        holdings = int(g["symbol"].nunique()) if "symbol" in g.columns else int(len(g))
        avg_exposure = float(g["weight"].abs().sum())

        daily_rows.append({
            "date": pd.to_datetime(d),
            "ret": ret,
            "holdings": holdings,
            "avg_exposure": avg_exposure
        })

    daily = pd.DataFrame(daily_rows)
    daily = daily.sort_values("date").reset_index(drop=True)
    return daily


def rebuild_nav(nav0, daily):
    full_dates = pd.DataFrame({
        "date": pd.date_range(nav0["date"].min(), nav0["date"].max(), freq="D")
    })

    nav = full_dates.merge(daily, on="date", how="left")
    nav["ret"] = nav["ret"].fillna(0.0)
    nav["holdings"] = nav["holdings"].ffill().fillna(0)
    nav["avg_exposure"] = nav["avg_exposure"].ffill().fillna(0.0)

    nav.loc[0, "nav"] = float(nav0.iloc[0]["nav"])

    for i in range(1, len(nav)):
        prev_nav = float(nav.loc[i - 1, "nav"])
        today_ret = float(nav.loc[i, "ret"])
        nav.loc[i, "nav"] = prev_nav * (1.0 + today_ret)

    return nav


def build_summary(nav):
    initial_capital = float(nav["nav"].iloc[0])
    final_nav = float(nav["nav"].iloc[-1])

    total_return = (final_nav / initial_capital) - 1.0 if initial_capital != 0 else 0.0
    daily_ret = nav["nav"].pct_change().dropna()

    sharpe = 0.0
    if len(daily_ret) > 1 and float(daily_ret.std()) != 0:
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
    # all
    save_all = pos.copy()
    save_all["trade_date"] = pd.to_datetime(save_all["trade_date"]).dt.strftime("%Y-%m-%d")
    if "signal_date" in save_all.columns:
        save_all["signal_date"] = pd.to_datetime(save_all["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    save_all.to_csv(TIER_MAP["all"], index=False)

    overview_rows = []
    for tier in ["lt10", "10_30", "30_50", "50_100", "100p", "unknown"]:
        sub = pos[pos["price_tier"] == tier].copy()

        sub_save = sub.copy()
        sub_save["trade_date"] = pd.to_datetime(sub_save["trade_date"]).dt.strftime("%Y-%m-%d")
        if "signal_date" in sub_save.columns:
            sub_save["signal_date"] = pd.to_datetime(sub_save["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        sub_save.to_csv(TIER_MAP[tier], index=False)

        overview_rows.append({
            "price_tier": tier,
            "rows": len(sub),
            "symbol_count": sub["symbol"].nunique() if "symbol" in sub.columns else np.nan,
            "avg_weight": float(sub["weight"].mean()) if len(sub) else np.nan,
            "avg_day_ret": float(sub["day_ret"].mean()) if len(sub) else np.nan,
            "avg_cum_ret": float(sub["cum_ret"].mean()) if "cum_ret" in sub.columns and len(sub) else np.nan,
            "latest_trade_date": str(pd.to_datetime(sub["trade_date"]).max().date()) if len(sub) else "",
            "latest_signal_date": (
                str(pd.to_datetime(sub["signal_date"], errors="coerce").max().date())
                if "signal_date" in sub.columns and len(sub) else ""
            )
        })

    pd.DataFrame(overview_rows).to_csv(OVERVIEW_PATH, index=False)


def write_logs(snapshot, latest_trade_date, pos, nav):
    expand_log = pd.DataFrame([{
        "snapshot_rows": int(len(snapshot)),
        "snapshot_latest_trade_date": str(pd.to_datetime(latest_trade_date).date()),
        "expanded_rows": int(len(pos)),
        "expanded_unique_trade_dates": int(pos["trade_date"].nunique()),
        "expanded_unique_symbols": int(pos["symbol"].nunique()) if "symbol" in pos.columns else 0,
        "nav_start_date": str(pd.to_datetime(nav["date"]).min().date()),
        "nav_end_date": str(pd.to_datetime(nav["date"]).max().date())
    }])
    expand_log.to_csv(EXPAND_LOG_PATH, index=False)

    sample = pos.head(200).copy()
    sample["trade_date"] = pd.to_datetime(sample["trade_date"]).dt.strftime("%Y-%m-%d")
    if "signal_date" in sample.columns:
        sample["signal_date"] = pd.to_datetime(sample["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    sample.to_csv(DAILY_SAMPLE_PATH, index=False)

    update_row = pd.DataFrame([{
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "latest_nav_date": str(pd.to_datetime(nav["date"]).max().date()),
        "positions_rows": int(len(pos)),
        "unique_trade_dates": int(pos["trade_date"].nunique()),
        "unique_symbols": int(pos["symbol"].nunique()) if "symbol" in pos.columns else 0
    }])

    if UPDATE_LOG_PATH.exists():
        old = pd.read_csv(UPDATE_LOG_PATH)
        out = pd.concat([old, update_row], ignore_index=True)
    else:
        out = update_row

    out.to_csv(UPDATE_LOG_PATH, index=False)


def main():
    nav0 = load_nav()
    snapshot, latest_trade_date = load_snapshot_positions()
    pos = expand_positions_daily(snapshot, nav0)
    daily = build_daily_returns_from_positions(pos)
    nav = rebuild_nav(nav0, daily)
    summary = build_summary(nav)

    # 存主檔
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
    write_logs(snapshot, latest_trade_date, pos, nav)

    print("v210 DONE")
    print(f"snapshot rows = {len(snapshot)}")
    print(f"expanded rows = {len(pos)}")
    print(f"unique trade dates = {pos['trade_date'].nunique()}")
    print(f"NAV -> {NAV_PATH}")
    print(f"POSITIONS -> {POS_PATH}")
    print(f"SUMMARY -> {SUM_PATH}")
    print(f"EXPAND LOG -> {EXPAND_LOG_PATH}")


if __name__ == "__main__":
    main()
