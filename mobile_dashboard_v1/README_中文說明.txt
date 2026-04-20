v266.7 資料一致性修正版

這版只處理三件事：
1. 舊資料問題：workflow 強制把最新輸出寫回 mobile_dashboard_v1/data/
2. 價格分層亂碼：後端統一輸出英文 key，前端轉中文
3. 資料狀態問題：新增 meta.json，UI 顯示真正的最後更新、訊號日、交易日、資料新舊

安裝方式：
1. v266_7_consistency_fix.txt 改名為 v266_7_consistency_fix.py
2. v266_7_consistency_fix.yml.txt 放到 .github/workflows/ 並改名為 v266_7_consistency_fix.yml
3. 覆蓋 mobile_dashboard_v1/index.html、app.js、styles.css、README_中文說明.txt
4. 不要覆蓋現有 data 內真實資料
5. 手動 Run workflow 一次
