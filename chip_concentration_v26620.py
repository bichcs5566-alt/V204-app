# -*- coding: utf-8 -*-
"""
v266.20 籌碼資料檢查工具

用途：
檢查 final_action_plan.csv 是否真的有吃到籌碼資料。
"""

from pathlib import Path
import pandas as pd

CANDIDATES = [
    Path("mobile_dashboard_v1/data/final_action_plan.csv"),
    Path("final_action_plan.csv"),
    Path("mobile_dashboard_v1/data/full_summary.csv"),
    Path("full_summary.csv"),
]

def main():
    target = None
    for p in CANDIDATES:
        if p.exists():
            target = p
            break

    if target is None:
        print("❌ 找不到 final_action_plan.csv / full_summary.csv")
        return

    print(f"✅ 讀取檔案：{target}")

    df = pd.read_csv(target, encoding="utf-8-sig")
    print(f"總筆數：{len(df)}")

    need_cols = [
        "chip_score",
        "chip_label",
        "chip_display",
        "chip_reason",
        "chip_hint",
        "chip_valid_count",
        "chip_missing",
    ]

    missing_cols = [c for c in need_cols if c not in df.columns]

    if missing_cols:
        print("\n❌ 籌碼欄位尚未成功輸出")
        print("缺少欄位：", ", ".join(missing_cols))
        print("\n判斷：final_decision_engine.py 可能還沒接上 add_chip_columns(out)。")
        return

    valid = pd.to_numeric(df["chip_valid_count"], errors="coerce").fillna(0)
    score = pd.to_numeric(df["chip_score"], errors="coerce").fillna(0)

    no_data_count = int((valid == 0).sum())
    partial_count = int(((valid > 0) & (valid < 5)).sum())
    full_count = int((valid >= 5).sum())

    print("\n📊 籌碼資料狀態")
    print(f"完全沒資料：{no_data_count}")
    print(f"部分有資料：{partial_count}")
    print(f"五類都有資料：{full_count}")

    print("\n📈 籌碼分數分布")
    print(f"高度集中 80+：{int((score >= 80).sum())}")
    print(f"偏集中 60~79：{int(((score >= 60) & (score < 80)).sum())}")
    print(f"普通 40~59：{int(((score >= 40) & (score < 60)).sum())}")
    print(f"分散 20~39：{int(((score >= 20) & (score < 40)).sum())}")
    print(f"極度分散 <20：{int((score < 20).sum())}")

    if no_data_count == len(df):
        print("\n⚠️ 結論：目前全部籌碼資料都沒吃到。")
        print("可能原因：原始 CSV 沒有法人、外資、投信、融資、大戶欄位。")
    elif no_data_count > 0:
        print("\n⚠️ 結論：部分股票籌碼資料沒吃到。")
    else:
        print("\n✅ 結論：籌碼模組有成功吃到資料。")

    print("\n🔎 前 10 筆檢查")
    show_cols = []
    for c in ["symbol", "stock_id", "code", "name", "stock_name", "chip_display", "chip_valid_count", "chip_missing", "chip_reason"]:
        if c in df.columns:
            show_cols.append(c)
    print(df[show_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
