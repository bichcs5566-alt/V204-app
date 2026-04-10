from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
SUM_PATH = ROOT / "v202_summary.csv"
UPDATE_LOG_PATH = ROOT / "update_log.csv"

# ========= 抓資料 =========
def fetch_data():
    pos = pd.read_csv(POS_PATH)

    today = pd.Timestamp(datetime.utcnow().date())
    latest_day = today - pd.Timedelta(days=1)

    if "trade_date" in pos.columns:
        pos["trade_date"] = latest_day.strftime("%Y-%m-%d")

    return pos

# ========= 主策略 =========
def run_strategy(pos):
    today = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=1)

    # NAV
    if NAV_PATH.exists():
        nav = pd.read_csv(NAV_PATH)
    else:
        nav = pd.DataFrame(columns=["date", "nav"])

    last_nav = nav["nav"].iloc[-1] if len(nav) else 100000

    new_row = {
        "date": today.strftime("%Y-%m-%d"),
        "nav": last_nav * 1.001
    }

    nav = pd.concat([nav, pd.DataFrame([new_row])], ignore_index=True)

    # SUMMARY
    summary = pd.DataFrame([{
        "start_date": nav["date"].iloc[0],
        "end_date": nav["date"].iloc[-1],
        "final_nav": nav["nav"].iloc[-1]
    }])

    return nav, pos, summary

# ========= 分層 =========
def build_tiers(pos):
    pos.to_csv(ROOT / "tier_all_positions.csv", index=False)

# ========= 更新紀錄 =========
def log_update(nav):
    row = pd.DataFrame([{
        "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_nav_date": nav["date"].iloc[-1]
    }])

    if UPDATE_LOG_PATH.exists():
        old = pd.read_csv(UPDATE_LOG_PATH)
        row = pd.concat([old, row])

    row.to_csv(UPDATE_LOG_PATH, index=False)

# ========= 主流程 =========
def main():
    pos = fetch_data()
    nav, pos, summary = run_strategy(pos)

    nav.to_csv(NAV_PATH, index=False)
    pos.to_csv(POS_PATH, index=False)
    summary.to_csv(SUM_PATH, index=False)

    build_tiers(pos)
    log_update(nav)

    print("v208.5 DONE")

if __name__ == "__main__":
    main()
