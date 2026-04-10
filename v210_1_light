# v210.1 輕量版（只展開最近區間，避免 Actions 超時）
#
# 重點：
# - 只展開最近 N 天（預設 45 天）
# - 舊資料保留，不重算全歷史
# - 避免資料爆量 + GitHub Actions timeout

name: v2101

on:
  workflow_dispatch:

permissions:
  contents: write

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - run: |
          python -m pip install --upgrade pip
          python -m pip install pandas numpy

      - run: |
          python v2101_light_positions_history.py

      - run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add .
          if git diff --cached --quiet; then
            echo "no changes"
          else
            git commit -m "v210.1 light rebuild"
            git push


# ================= Python =================
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"

WINDOW_DAYS = 45

def load_nav():
    nav = pd.read_csv(NAV_PATH)
    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = pd.to_numeric(nav["nav"], errors="coerce")
    return nav.dropna().sort_values("date").reset_index(drop=True)

def load_snapshot():
    pos = pd.read_csv(POS_PATH)
    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / len(pos)
    if "day_ret" not in pos.columns:
        pos["day_ret"] = 0.0
    return pos

def main():
    nav = load_nav()
    pos = load_snapshot()

    end = nav["date"].max()
    start = end - pd.Timedelta(days=WINDOW_DAYS)

    dates = pd.date_range(start, end)

    rows = []
    for d in dates:
        g = pos.copy()
        g["trade_date"] = d
        if d != end:
            g["day_ret"] = 0.0
        rows.append(g)

    new_pos = pd.concat(rows)

    daily = new_pos.groupby("trade_date").apply(
        lambda g: (g["weight"] * g["day_ret"]).sum()
    ).reset_index(name="ret")

    nav = nav.merge(daily, left_on="date", right_on="trade_date", how="left")
    nav["ret"] = nav["ret"].fillna(0)

    for i in range(1, len(nav)):
        nav.loc[i, "nav"] = nav.loc[i-1, "nav"] * (1 + nav.loc[i, "ret"])

    new_pos["trade_date"] = new_pos["trade_date"].dt.strftime("%Y-%m-%d")
    nav["date"] = nav["date"].dt.strftime("%Y-%m-%d")

    new_pos.to_csv(POS_PATH, index=False)
    nav.to_csv(NAV_PATH, index=False)

    print("v210.1 DONE")

if __name__ == "__main__":
    main()
