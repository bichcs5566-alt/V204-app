
v206 自動更新版 使用說明

這版新增：
1. 自動更新頻率（下拉式）
2. 價格層篩選（下拉式）
3. 手動更新按鈕
4. 保留 v205 固定網址架構

重要：
- 這版的「自動更新」是前端自動重新抓 GitHub Pages 上最新 csv。
- 它不是替你在後端產生新 csv。
- 如果你要真正每天自動把最新 csv 推上 GitHub，還要再接 GitHub Actions 或雲端排程。

GitHub Pages 要放的檔案：
- v206_fixed_url_app.html
- v202_nav.csv
- v202_positions.csv
- v202_summary.csv

第一次使用：
1. 用 Safari / Chrome 打開 v206_fixed_url_app.html 的網址
2. 輸入 Base URL，例如：
   https://你的帳號.github.io/V204-app
3. 按「儲存網址」
4. 按「立即更新」
5. 設定自動更新頻率
6. 選擇價格層
