from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
REPAIR_LOG_PATH = ROOT / "v2091_repair_log.csv"

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def load_nav():
    nav = pd.read_csv(NAV_PATH)
    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = to_num(nav["nav"])
    nav = nav.dropna(subset=["date","nav"])
    nav = nav.sort_values("date").reset_index(drop=True)
    return nav

def load_pos():
    pos = pd.read_csv(POS_PATH)

    if "trade_date" not in pos.columns:
        pos["trade_date"] = pd.NaT

    if "weight" not in pos.columns:
        pos["weight"] = 0

    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0

    pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")
    pos["weight"] = to_num(pos["weight"]).fillna(0)
    pos["day_ret"] = to_num(pos["day_ret"]).fillna(0)

    pos["trade_date"] = pos["trade_date"].fillna(
        pd.Timestamp(datetime.utcnow().date())
    )

    return pos

def normalize(pos):
    out = []
    log = []

    for d, g in pos.groupby("trade_date"):
        g = g.copy()

        total = g["weight"].abs().sum()
        normalized = False

        if total > 1.5 and total > 0:
            g["weight"] = g["weight"] / total
            normalized = True

        log.append({
            "date": str(d.date()),
            "rows": len(g),
            "normalized": normalized
        })

        out.append(g)

    pos = pd.concat(out).reset_index(drop=True)
    log = pd.DataFrame(log)

    return pos, log

def build_daily(pos):
    rows = []

    for d, g in pos.groupby("trade_date"):
        r = (g["weight"] * g["day_ret"]).sum()

        # 防爆
        r = max(min(r, 0.1), -0.1)

        rows.append({
            "date": d,
            "ret": r
        })

    return pd.DataFrame(rows)

def rebuild(nav, daily):
    full_dates = pd.date_range(
        nav["date"].min(),
        datetime.utcnow().date(),
        freq="D"
    )

    df = pd.DataFrame({"date": full_dates})
    df = df.merge(daily, on="date", how="left")

    df["ret"] = df["ret"].fillna(0)

    df.loc[0, "nav"] = nav.iloc[0]["nav"]

    for i in range(1, len(df)):
        prev = df.loc[i-1, "nav"]
        df.loc[i, "nav"] = prev * (1 + df.loc[i, "ret"])

    return df

def main():
    nav = load_nav()
    pos = load_pos()

    pos, log = normalize(pos)
    daily = build_daily(pos)
    nav = rebuild(nav, daily)

    nav["date"] = nav["date"].dt.strftime("%Y-%m-%d")
    nav.to_csv(NAV_PATH, index=False)

    pos["trade_date"] = pos["trade_date"].dt.strftime("%Y-%m-%d")
    pos.to_csv(POS_PATH, index=False)

    log.to_csv(REPAIR_LOG_PATH, index=False)

    print("v209.1 FINAL OK")

if __name__ == "__main__":
    main()
