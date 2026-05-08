# trigger_quality_patch.py
# v266.67
# 主力發動前夜模型補丁
# 不覆蓋原策略
# 只修正 TEST / WATCH 排序品質

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"

FILES = [
    ROOT / "trade_plan.csv",
    DATA_DIR / "trade_plan.csv"
]

def calc_compression(row):
    score = 0

    k = str(row.get("k_structure", ""))
    ma5 = str(row.get("ma5_status", ""))
    ma10 = str(row.get("ma10_status", ""))
    ma20 = str(row.get("ma20_status", ""))

    if "收斂" in k:
        score += 25

    if "整理" in k:
        score += 20

    if "站上" in ma5:
        score += 15

    if "站上" in ma10:
        score += 10

    if "站上" in ma20:
        score += 10

    return score


def calc_trigger_quality(row):

    score = 0

    reason = str(row.get("reason", ""))
    chip_reason = str(row.get("chip_reason", ""))
    system_tip = str(row.get("system_tip", ""))
    k = str(row.get("k_structure", ""))
    kbar = str(row.get("kbar_pattern", ""))

    liquidity = float(row.get("liquidity_score", 0))
    chip = float(row.get("chip_score", 0))

    compression = calc_compression(row)

    score += compression * 0.9

    # 初放量
    if "溫和放量" in reason:
        score += 30

    if "量縮" in reason:
        score += 20

    if "爆量" in reason:
        score -= 25

    # 籌碼剛轉強
    if chip >= 20 and chip <= 45:
        score += 35

    if chip >= 45 and chip <= 65:
        score += 15

    if chip > 80:
        score -= 30

    # 結構
    if "平台" in reason:
        score += 20

    if "收斂" in k:
        score += 25

    if "多頭排列" in k:
        score += 10

    # 避免追高
    if "突破確認" in kbar:
        score -= 10

    if "強勢延續" in system_tip:
        score -= 15

    # 流動性過熱
    if liquidity > 98:
        score -= 15

    return round(score, 2)


def patch(df):

    if len(df) == 0:
        return df

    df["trigger_quality"] = df.apply(calc_trigger_quality, axis=1)

    # 狀態分層
    conditions = [
        df["trigger_quality"] >= 80,
        df["trigger_quality"] >= 60,
        df["trigger_quality"] >= 40,
    ]

    values = [
        "🔥 主力點火前夜",
        "🟢 早期轉強",
        "🟡 觀察收斂"
    ]

    df["trigger_phase"] = np.select(
        conditions,
        values,
        default="⚪ 雜訊"
    )

    # TEST / WATCH 重分類
    new_mode = []

    for _, row in df.iterrows():

        tq = row["trigger_quality"]

        if tq >= 60:
            new_mode.append("TEST")
        else:
            new_mode.append("WATCH")

    df["watch_mode"] = new_mode

    # 排序
    df = df.sort_values(
        by=[
            "watch_mode",
            "trigger_quality"
        ],
        ascending=[True, False]
    )

    return df


for file in FILES:

    if not file.exists():
        continue

    try:

        df = pd.read_csv(file)

        if len(df) == 0:
            continue

        df = patch(df)

        df.to_csv(file, index=False)

        print(f"[v266.67] patched: {file}")

    except Exception as e:
        print(f"[ERROR] {file} -> {e}")
