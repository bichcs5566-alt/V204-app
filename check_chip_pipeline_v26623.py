# -*- coding: utf-8 -*-
"""
check_chip_pipeline_v26623.py

檢查籌碼資料卡在哪一層。
"""

from pathlib import Path
import json
import pandas as pd


def read_csv(path):
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path)


def check_file(path):
    p = Path(path)
    print("\n==============================")
    print(f"檢查：{path}")
    print("==============================")

    if not p.exists():
        print("❌ 檔案不存在")
        return None

    print(f"✅ 檔案存在：{p.stat().st_size} bytes")

    if p.suffix.lower() == ".json":
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            print(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            print("❌ JSON 讀取失敗：", e)
        return None

    try:
        df = read_csv(p)
    except Exception as e:
        print("❌ CSV 讀取失敗：", e)
        return None

    print(f"✅ 筆數：{len(df)}")
    print(f"✅ 欄位：{list(df.columns)}")

    if "stock_id" in df.columns:
        ids = df["stock_id"].astype(str).head(30).tolist()
        print(f"前30個 stock_id：{ids}")

    if len(df) <= 20:
        print("\n⚠️ 筆數偏少，前幾列：")
        print(df.head(20).to_string(index=False))

    chip_cols = [c for c in df.columns if "chip" in c.lower() or "籌碼" in c]
    if chip_cols:
        print(f"\n✅ 籌碼欄位：{chip_cols}")
        for c in chip_cols:
            try:
                print(f"{c} 前10筆：{df[c].head(10).tolist()}")
            except Exception:
                pass

    if "chip_valid_count" in df.columns:
        try:
            print("chip_valid_count 統計：")
            print(df["chip_valid_count"].value_counts(dropna=False).to_string())
        except Exception:
            pass

    if "chip_reason" in df.columns:
        print("chip_reason 前10筆：")
        print(df["chip_reason"].head(10).tolist())

    return df


def main():
    files = [
        "chip_source_twse_summary.json",
        "mobile_dashboard_v1/data/chip_source_twse_summary.json",
        "chip_source_twse.csv",
        "mobile_dashboard_v1/data/chip_source_twse.csv",
        "final_action_plan.csv",
        "mobile_dashboard_v1/data/final_action_plan.csv",
    ]

    for f in files:
        check_file(f)


if __name__ == "__main__":
    main()
