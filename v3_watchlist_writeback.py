name: v3_watchlist_writeback

on:
  workflow_dispatch:
    inputs:
      action:
        description: "add 或 remove"
        required: true
        type: choice
        options:
          - add
          - remove
      stock_id:
        description: "股票代號"
        required: true
        type: string

permissions:
  contents: write
  actions: write

concurrency:
  group: v3-watchlist-writeback
  cancel-in-progress: false

jobs:
  writeback:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          persist-credentials: true

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run watchlist writeback
        run: |
          python v3_watchlist_writeback.py \
            --action "${{ github.event.inputs.action }}" \
            --stock_id "${{ github.event.inputs.stock_id }}"

      - name: Commit & Push (safe mode)
        run: |
          set -e
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

          git add watchlist.csv

          if git diff --cached --quiet; then
            echo "no watchlist.csv changes"
            exit 0
          fi

          git commit -m "v3.2.2 watchlist writeback: ${{ github.event.inputs.action }} ${{ github.event.inputs.stock_id }}"

          git pull --rebase origin main || git rebase --abort || true
          git push origin HEAD:main || (
            git pull origin main --no-rebase
            git push origin HEAD:main
          )

      - name: Trigger main pipeline
        if: success()
        uses: actions/github-script@v7
        with:
          script: |
            await github.rest.actions.createWorkflowDispatch({
              owner: context.repo.owner,
              repo: context.repo.repo,
              workflow_id: 'v3_1_auto_update.yml',
              ref: 'main'
            })
