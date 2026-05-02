# -*- coding: utf-8 -*-
"""
check_chip_debug.py
v266 籌碼資料鏈路檢查器

用途：
單獨檢查目前 repo 裡：
1. TWSE 籌碼來源檔是否存在
2. 筆數是否正常
3. 是否仍只抓到 1101~1110
4. final_action_plan 是否已經有 chip 欄位
5. chip_valid_count 是否全部為 0
"""

from pathlib import Path
import json
import pandas as pd


def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path)


def check_json(path: Path):
    print("\n==============================")
    print(f"CHECK JSON: {path}")
    print("==============================")

    if not path.exists():
        print("❌ NOT FOUND")
        return

    print(f"✅ FOUND size={path.stat().st_size} bytes")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        print(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"❌ JSON READ ERROR: {e}")


def check_csv(path: Path):
    print("\n==============================")
    print(f"CHECK CSV: {path}")
    print("==============================")

    if not path.exists():
        print("❌ NOT FOUND")
        return

    print(f"✅ FOUND size={path.stat().st_size} bytes")

    try:
        df = read_csv(path)
    except Exception as e:
        print(f"❌ CSV READ ERROR: {e}")
        return

    print(f"rows: {len(df)}")
    print(f"columns: {list(df.columns)}")

    if "stock_id" in df.columns:
        ids = df["stock_id"].astype(str).head(30).tolist()
        print(f"sample stock_id first30: {ids}")

        only_cement = set(df["stock_id"].astype(str).str.extract(r"(\d{4})")[0].dropna().tolist()).issubset(
            {"1101", "1102", "1103", "1104", "1108", "1109", "1110"}
        )
        print(f"only cement 1101~1110: {only_cement}")

    if len(df) <= 30:
        print("\n⚠️ rows <= 30, preview:")
        print(df.head(30).to_string(index=False))

    chip_cols = [c for c in df.columns if "chip" in str(c).lower() or "籌碼" in str(c)]
    print(f"chip columns: {chip_cols}")

    if "chip_valid_count" in df.columns:
        print("\nchip_valid_count value_counts:")
        try:
            print(df["chip_valid_count"].value_counts(dropna=False).to_string())
        except Exception as e:
            print(f"❌ chip_valid_count count error: {e}")

    for c in ["chip_score", "chip_display", "chip_reason", "chip_hint", "chip_missing", "chip_confidence"]:
        if c in df.columns:
            print(f"\n{c} first10:")
            print(df[c].head(10).tolist())

    # 檢查幾個常用股票是否存在於籌碼源
    if "stock_id" in df.columns:
        for sid in ["2330", "2409", "3707", "6239", "3680"]:
            exists = sid in set(df["stock_id"].astype(str).str.extract(r"(\d{4})")[0].dropna().tolist())
            print(f"stock_id {sid} exists: {exists}")


def main():
    print("========== CHIP PIPELINE DEBUG START ==========")
    print(f"cwd: {Path.cwd()}")

    print("\n========== ROOT FILES ==========")
    for p in sorted(Path(".").glob("*")):
        if p.is_file():
            print(f"{p} | {p.stat().st_size} bytes")

    print("\n========== DATA FILES ==========")
    data_dir = Path("mobile_dashboard_v1/data")
    if data_dir.exists():
        for p in sorted(data_dir.glob("*")):
            if p.is_file():
                print(f"{p} | {p.stat().st_size} bytes")
    else:
        print("mobile_dashboard_v1/data NOT FOUND")

    json_files = [
        Path("chip_source_twse_summary.json"),
        Path("mobile_dashboard_v1/data/chip_source_twse_summary.json"),
    ]

    csv_files = [
        Path("chip_source_twse.csv"),
        Path("mobile_dashboard_v1/data/chip_source_twse.csv"),
        Path("final_action_plan.csv"),
        Path("mobile_dashboard_v1/data/final_action_plan.csv"),
        Path("trade_plan.csv"),
        Path("mobile_dashboard_v1/data/trade_plan.csv"),
    ]

    for p in json_files:
        check_json(p)

    for p in csv_files:
        check_csv(p)

    print("\n========== CHIP PIPELINE DEBUG END ==========")


if __name__ == "__main__":
    main()
