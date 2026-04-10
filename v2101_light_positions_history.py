# =========================
# 檔案 2：v2101_light_positions_history.py
# =========================
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
LOG_PATH = ROOT / "v2101_light_log.csv"

WINDOW_DAYS = 45

def load_nav():
    nav = pd.read_csv(NAV_PATH)
    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = pd.to_numeric(nav["nav"], errors="coerce")
    nav = nav.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)
    return nav

def load_snapshot():
    pos = pd.read_csv(POS_PATH)

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / max(len(pos), 1)

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0

    pos["weight"] = pd.to_numeric(pos["weight"], errors="coerce").fillna(0.0)
    pos["day_ret"] = pd.to_numeric(pos["day_ret"], errors="coerce").fillna(0.0)
    return pos

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

def rebuild_nav(nav, pos_new):
    daily = (
        pos_new.groupby("trade_date", as_index=False)
        .apply(lambda g: pd.Series({"ret": float((g["weight"] * g["day_ret"]).sum())}))
        .reset_index(drop=True)
    )

    nav2 = nav.merge(daily, left_on="date", right_on="trade_date", how="left")

    if "ret" not in nav2.columns:
        nav2["ret"] = 0.0
    else:
        nav2["ret"] = pd.to_numeric(nav2["ret"], errors="coerce").fillna(0.0)

    for i in range(1, len(nav2)):
        nav2.loc[i, "nav"] = nav2.loc[i - 1, "nav"] * (1.0 + nav2.loc[i, "ret"])

    if "trade_date" in nav2.columns:
        nav2 = nav2.drop(columns=["trade_date"])

    return nav2

def main():
    nav = load_nav()
    snapshot = load_snapshot()

    pos_new, start, end = expand_light(snapshot, nav)
    nav_new = rebuild_nav(nav.copy(), pos_new)

    pos_save = pos_new.copy()
    pos_save["trade_date"] = pd.to_datetime(pos_save["trade_date"]).dt.strftime("%Y-%m-%d")
    nav_new["date"] = pd.to_datetime(nav_new["date"]).dt.strftime("%Y-%m-%d")

    pos_save.to_csv(POS_PATH, index=False)
    nav_new.to_csv(NAV_PATH, index=False)

    pd.DataFrame([{
        "start": str(start.date()),
        "end": str(end.date()),
        "rows": int(len(pos_new))
    }]).to_csv(LOG_PATH, index=False)

    print("v210.1 DONE")
    print("rows:", len(pos_new))

if __name__ == "__main__":
    main()
