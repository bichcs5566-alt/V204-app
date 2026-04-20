v266.7.2 完整覆蓋版（資料修正專用）

這版修正：
1. meta.json 強制輸出，解決日期顯示為 --
2. trade/position/watch/sum/debug 空資料時顯示提示，不再空白
3. stock_id 全部強制轉字串，避免 merge 對不到造成 價格=0 / 價格分層=未知
4. price_tier 後端統一輸出英文 key，前端轉中文
5. 備註 / 狀態欄舊錯碼時顯示中文提示
6. 今日操作「加入持倉」預設股數改為 1000
7. 保留完整操作 UI：輸入欄、移除鍵、加入持倉

你要覆蓋的檔案：
A. repo 根目錄
- v266_7_2_consistency_fix.py   （由 txt 改名）

B. .github/workflows/
- v266_7_2_consistency_fix.yml  （由 txt 改名）

C. mobile_dashboard_v1/
- index.html
- app.js
- styles.css
- README_中文說明.txt

注意：
1. 不要覆蓋你現有 data 內真實 CSV
2. 只放入新的 meta.json（若你要也可保留現有，workflow 會重寫）
3. 上傳後請手動 Run v266.7.2 workflow 一次
