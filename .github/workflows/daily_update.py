"""
daily_update.py

每日更新：
- 只補 price_panel_daily.csv 最後日期之後的缺口
- 不刪歷史資料
- 每天跑也安全
"""

# 直接沿用 backfill_missing_days.py 的邏輯
# 因為每日更新本質就是補 last_date+1 到 today

from backfill_missing_days import main

if __name__ == "__main__":
    main()
