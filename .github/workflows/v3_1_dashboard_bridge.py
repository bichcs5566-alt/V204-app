name: v3_1_tradeable_main_force

on:
  workflow_dispatch:
  schedule:
    # 台灣時間 19:10 = UTC 11:10
    - cron: "10 11 * * 1-5"

permissions:
  contents: write

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas numpy requests

      - name: Run v3.1 tradeable pipeline
        run: |
          python v1_stable_pipeline.py

      - name: Commit results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add trade_plan.csv v3_core_candidates.csv v3_core_debug.csv mobile_dashboard_v1/data/ || true
          if git diff --cached --quiet; then
            echo "No changes to commit"
          else
            git commit -m "v3.1 tradeable main force update"
            git push
          fi
