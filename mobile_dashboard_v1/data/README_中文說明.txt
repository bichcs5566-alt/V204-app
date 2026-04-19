手機操作介面 v1（中文）

資料夾用途：
- index.html：手機主介面
- styles.css：樣式
- app.js：讀取 CSV 並顯示
- data/：放每日更新後的 CSV

你每天要更新到 data/ 的檔案：
- trade_plan.csv
- current_positions.csv
- core_candidates.csv
- alpha_candidates.csv

建議使用方式：
1. 把整個 mobile_dashboard_v1 資料夾放到 repo
2. 每日 workflow 跑完後，把上面四個 CSV 複製到 data/
3. 用 GitHub Pages 或本機 http server 開啟 index.html

若要本機快速預覽：
在資料夾內執行
python -m http.server 8000

然後手機或電腦打開：
http://127.0.0.1:8000/

如果要讓 GitHub Pages 直接用：
- 把這個資料夾內容放在 docs/ 或 repo root
- 確保 data/ 內每天更新 CSV
