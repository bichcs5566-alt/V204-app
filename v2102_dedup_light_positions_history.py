# =========================================================
# 檔案 2：v2102_dedup_light_positions_history.py
# =========================================================
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
LOG_PATH = ROOT / "v2102_dedup_log.csv"

WINDOW_DAYS = 45


def to_num(s):
    return pd.to_numeric(s, errors="coerce")


def load_nav():
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

    nav = nav.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)
    return nav


def load_positions():
    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    if "symbol" not in pos.columns:
        pos["symbol"] = np.arange(1, len(pos) + 1).astype(str)

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / max(len(pos), 1)

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0

    if "trade_date" not in pos.columns:
        pos["trade_date"] = pd.NaT

    pos["symbol"] = pos["symbol"].astype(str)
    pos["weight"] = to_num(pos["weight"]).fillna(0.0)
    pos["day_ret"] = to_num(pos["day_ret"]).fillna(0.0)
    pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")

    return pos


def choose_snapshot(pos):
    if pos["trade_date"].notna().any():
        latest_day = pos["trade_date"].max()
        snapshot = pos[pos["trade_date"] == latest_day].copy()
        if len(snapshot) == 0:
            snapshot = pos.copy()
    else:
        latest_day = pd.Timestamp.utcnow().normalize()
        snapshot = pos.copy()

    snapshot = snapshot.sort_values(["symbol"]).drop_duplicates(
        subset=["symbol"], keep="last"
    ).reset_index(drop=True)

    abs_sum = snapshot["weight"].abs().sum()
    if abs_sum > 1.5 and abs_sum > 0:
        snapshot["weight"] = snapshot["weight"] / abs_sum

    return snapshot, latest_day


def expand_light(snapshot, nav):
    end_date = nav["date"].max()
    start_date = end_date - pd.Timedelta(days=WINDOW_DAYS)
    dates = pd.date_range(start_date, end_date, freq="D")

    rows = []
    for d in dates:
        g = snapshot.copy()
        g["trade_date"] = d
        if d != end_date:
            g["day_ret"] = 0.0
        rows.append(g)

    out = pd.concat(rows, ignore_index=True)
    return out, start_date, end_date


def drop_old_window_rows(pos, start_date):
    mask_old_keep = pos["trade_date"].isna() | (pos["trade_date"] < start_date)
    return pos[mask_old_keep].copy()


def combine_and_dedup(old_pos_keep, new_pos):
    pos = pd.concat([old_pos_keep, new_pos], ignore_index=True)
    pos = pos.sort_values(["trade_date", "symbol"]).drop_duplicates(
        subset=["trade_date", "symbol"], keep="last"
    ).reset_index(drop=True)
    return pos


def build_daily(pos):
    daily_base = (
        pos.groupby("trade_date", as_index=False)
        .agg(
            holdings=("symbol", "nunique"),
            avg_exposure=("weight", lambda x: float(pd.Series(x).abs().sum()))
        )
    )

    weighted = (
        pos.assign(wret=pos["weight"].fillna(0.0) * pos["day_ret"].fillna(0.0))
        .groupby("trade_date", as_index=False)["wret"]
        .sum()
        .rename(columns={"wret": "ret"})
    )

    daily = daily_base.merge(weighted, on="trade_date", how="left")
    daily["ret"] = daily["ret"].fillna(0.0).clip(-0.10, 0.10)
    return daily.sort_values("trade_date").reset_index(drop=True)


def rebuild_nav(nav, daily):
    nav2 = nav.copy()
    nav2 = (
        nav2.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    nav2 = nav2.merge(daily, left_on="date", right_on="trade_date", how="left", suffixes=("", "_new"))

    if "ret_new" in nav2.columns:
        nav2["ret"] = nav2["ret_new"].combine_first(nav2["ret"]).fillna(0.0)
    elif "ret" not in nav2.columns:
        nav2["ret"] = 0.0
    else:
        nav2["ret"] = nav2["ret"].fillna(0.0)

    if "holdings_new" in nav2.columns:
        if "holdings" in nav2.columns:
            nav2["holdings"] = nav2["holdings_new"].combine_first(nav2["holdings"])
        else:
            nav2["holdings"] = nav2["holdings_new"]

    if "avg_exposure_new" in nav2.columns:
        if "avg_exposure" in nav2.columns:
            nav2["avg_exposure"] = nav2["avg_exposure_new"].combine_first(nav2["avg_exposure"])
        else:
            nav2["avg_exposure"] = nav2["avg_exposure_new"]

    for c in ["trade_date", "ret_new", "holdings_new", "avg_exposure_new"]:
        if c in nav2.columns:
            nav2 = nav2.drop(columns=[c])

    nav2 = nav2.sort_values("date").reset_index(drop=True)
    nav2.loc[0, "nav"] = float(nav2.loc[0, "nav"])

    for i in range(1, len(nav2)):
        nav2.loc[i, "nav"] = float(nav2.loc[i - 1, "nav"]) * (1.0 + float(nav2.loc[i, "ret"]))

    return nav2


def write_log(old_pos, snapshot, new_pos, final_pos, start_date, end_date, nav2):
    log = pd.DataFrame([{
        "window_start": str(start_date.date()),
        "window_end": str(end_date.date()),
        "old_positions_rows": int(len(old_pos)),
        "snapshot_rows": int(len(snapshot)),
        "new_window_rows": int(len(new_pos)),
        "final_positions_rows": int(len(final_pos)),
        "final_unique_trade_dates": int(final_pos["trade_date"].nunique()),
        "final_unique_trade_symbol_pairs": int(final_pos[["trade_date", "symbol"]].drop_duplicates().shape[0]),
        "nav_unique_dates": int(nav2["date"].nunique()),
        "nav_last_date": str(nav2["date"].max().date())
    }])
    log.to_csv(LOG_PATH, index=False)


def main():
    nav = load_nav()
    old_pos = load_positions()

    snapshot, snapshot_day = choose_snapshot(old_pos)
    new_pos, start_date, end_date = expand_light(snapshot, nav)
    old_pos_keep = drop_old_window_rows(old_pos, start_date)
    final_pos = combine_and_dedup(old_pos_keep, new_pos)

    daily = build_daily(final_pos)
    nav2 = rebuild_nav(nav, daily)

    pos_save = final_pos.copy()
    pos_save["trade_date"] = pd.to_datetime(pos_save["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    nav_save = nav2.copy()
    nav_save["date"] = pd.to_datetime(nav_save["date"]).dt.strftime("%Y-%m-%d")

    pos_save.to_csv(POS_PATH, index=False)
    nav_save.to_csv(NAV_PATH, index=False)

    write_log(old_pos, snapshot, new_pos, final_pos, start_date, end_date, nav2)

    print("v210.2 DONE")
    print("window:", start_date.date(), "->", end_date.date())
    print("snapshot rows:", len(snapshot))
    print("new window rows:", len(new_pos))
    print("final positions rows:", len(final_pos))


if __name__ == "__main__":
    main()
