v266.8 雙按鈕版（前端刷新 + 後端更新）

這版新增：
1. 即時時間（像手錶一樣每秒更新）
2. 重新整理頁面（前端重讀目前 data）
3. 更新資料與策略（後端觸發 GitHub Actions workflow_dispatch）
4. 台灣時間 meta.json
5. 按鈕狀態回饋
6. 保留目前完整操作 UI

你要覆蓋 / 新增的檔案：

A. repo 根目錄
- v266_8_pipeline.txt → 改名 v266_8_pipeline.py
- v266_8_pipeline.yml.txt → 改名 .github/workflows/v266_8_pipeline.yml

B. mobile_dashboard_v1/
- index.html
- app.js
- styles.css
- README_中文說明.txt
- github_config.example.js  （參考檔）
- 可選：github_config.js   （你自己複製 example 後填入）

說明：
1. 「重新整理頁面」只會重抓目前 Pages 上的資料
2. 「更新資料與策略」會呼叫 GitHub API 觸發 workflow_dispatch
3. 要讓第二顆按鈕真正能用，你需要設定 GitHub Token
4. 這版預設會優先讀 mobile_dashboard_v1/github_config.js
5. 如果沒有 github_config.js，按更新資料與策略時會跳 prompt 讓你填一次，並存到此裝置 localStorage

建議：
- 用 Fine-grained PAT，權限只給這個 repo 的 Actions: Read and Write、Contents: Read
- 這個 token 只會存在你這台裝置瀏覽器 localStorage
