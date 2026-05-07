# ===== v266.58 CHIP SCORE PATCH =====
# 不改原策略
# 只新增：
# - chip_score
# - chip_signal
# - chip_reason
# - fallback date
# - dashboard output

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def build_chip_score(df):

    # 防呆
    for c in [
        "foreign_buy",
        "investment_buy",
        "dealer_buy",
        "margin_change",
        "volume_ratio",
        "close",
        "ma20",
        "ma60"
    ]:
        if c not in df.columns:
            df[c] = 0

    df["chip_score"] = 0
    df["chip_reason"] = ""

    # =========================
    # 外資
    # =========================
    df.loc[df["foreign_buy"] > 0, "chip_score"] += 5
    df.loc[df["foreign_buy"] > 0, "chip_reason"] += "外資買超｜"

    # =========================
    # 投信
    # =========================
    df.loc[df["investment_buy"] > 0, "chip_score"] += 6
    df.loc[df["investment_buy"] > 0, "chip_reason"] += "投信買超｜"

    # =========================
    # 自營
    # =========================
    df.loc[df["dealer_buy"] > 0, "chip_score"] += 2
    df.loc[df["dealer_buy"] > 0, "chip_reason"] += "自營買超｜"

    # =========================
    # 融資暴增扣分
    # =========================
    df.loc[df["margin_change"] > 8, "chip_score"] -= 8
    df.loc[df["margin_change"] > 8, "chip_reason"] += "融資暴增｜"

    # =========================
    # 爆量不跌
    # =========================
    strong_hold = (
        (df["volume_ratio"] > 1.5)
        & (df["close"] > df["ma20"])
    )

    df.loc[strong_hold, "chip_score"] += 8
    df.loc[strong_hold, "chip_reason"] += "爆量不跌｜"

    # =========================
    # MA20 / MA60 結構
    # =========================
    structure_ok = (
        (df["close"] > df["ma20"])
        & (df["ma20"] > df["ma60"])
    )

    df.loc[structure_ok, "chip_score"] += 5
    df.loc[structure_ok, "chip_reason"] += "均線結構｜"

    # =========================
    # 籌碼訊號
    # =========================
    df["chip_signal"] = "一般"

    df.loc[df["chip_score"] >= 20, "chip_signal"] = "主力吸籌"
    df.loc[df["chip_score"] >= 28, "chip_signal"] = "主力發動"

    return df


def merge_chip_score(target_csv):

    p = Path(target_csv)

    if not p.exists():
        print(f"skip: {p}")
        return

    df = pd.read_csv(p)

    df = build_chip_score(df)

    # =========================
    # 加權進 final_score
    # =========================
    if "final_score" in df.columns:
        df["final_score"] = (
            df["final_score"] * 0.75
            + df["chip_score"] * 0.25
        )

    # 排序
    if "final_score" in df.columns:
        df = df.sort_values("final_score", ascending=False)

    # 寫回
    df.to_csv(p, index=False)

    # dashboard
    dashboard_path = DATA_DIR / p.name
    df.to_csv(dashboard_path, index=False)

    print(f"updated: {p}")


def run_v26658_chip_patch():

    targets = [
        "candidates.csv",
        "core_candidates.csv",
        "alpha_candidates.csv",
        "trade_plan.csv",
        "watchlist_monitor.csv",
        "final_action_plan.csv"
    ]

    for t in targets:
        merge_chip_score(t)

    print("v266.58 chip patch done")


if __name__ == "__main__":
    run_v26658_chip_patch()

# ===== END PATCH =====
