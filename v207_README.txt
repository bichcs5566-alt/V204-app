
v207 真自動更新版

這版比 v206 多了：
1. GitHub Actions 每天自動執行
2. 自動產生 tier 檔案
3. 自動寫 update_log.csv
4. 手機端可看到最近自動更新紀錄

你要上傳：
- v207_fixed_url_app.html
- v202_nav.csv
- v202_positions.csv
- v202_summary.csv

再建立：
- .github/workflows/v207_daily_update.yml
- scripts/v207_mock_update.py
