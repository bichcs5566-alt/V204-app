v203 手機桌面 App 版 使用說明

一、你會拿到這些檔：
1. v203_mobile_app.html
2. manifest.json
3. service-worker.js
4. icon-192.png
5. icon-512.png

二、怎麼用
方法A：最簡單
- 把 v203_mobile_app.html 用手機瀏覽器打開
- 選 v202_nav.csv / v202_positions.csv / v202_summary.csv
- 按「載入 / Load」

方法B：像 App 一樣
- 把整包檔案放到同一個資料夾或簡單靜態網站
- iPhone 用 Safari 打開
- 按分享 → 加入主畫面
- Android 用 Chrome 打開
- 按選單 → 安裝應用程式 / 加入主畫面

三、注意
- 這版不需要 Streamlit
- 不需要網址也能當本地 HTML 開啟
- 若要完整 PWA 體驗，最好把 html / manifest / service worker / icon 放同一個位置

四、資料來源
- 讀取 v202 或 v106 產出的 csv
- 目前是手動選檔
- 下一版可做成自動讀最新每日檔

五、下一步可做
- v204：自動抓最新 csv
- v205：加入今日選股提醒
- v300：進階資料版（三大法人 / 融資融券 / 籌碼）
