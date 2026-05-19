# -*- coding: utf-8 -*-
"""
v266.63
first_trigger_event_patch.py

核心：
真正抓：
「第一次異常事件」

不是：
已經強很久。

目標：
縮小：
強點火事件數量

讓：
81 檔
→
10~20 檔左右

真正接近：
主力第一波發動。

新增：
- first_trigger_score_v26663
- first_trigger_label_v26663
- first_trigger_reason_v26663
- first_trigger_rank_v26663
"""

from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"

TARGETS = [
    "timing_candidates.csv",
    "final_action_plan.csv",
    "top_opportunities.csv",
]

# =========================
# safe
# =========================

def safe_num(df, col, default=0):

    if col not in df.columns:

        return pd.Series(
            default,
            index=df.index,
            dtype=float
        )

    return pd.to_numeric(
        df[col],
        errors="coerce"
    ).fillna(default)

# =========================
# first trigger
# =========================

def build_first_trigger(df):

    df = df.copy()

    close = safe_num(df, "close")
    ma20 = safe_num(df, "ma20")
    vr = safe_num(df, "volume_ratio", 1)

    chip = safe_num(df, "chip_score")

    trigger = safe_num(
        df,
        "trigger_event_score_v26662"
    )

    state_rank = safe_num(
        df,
        "state_rank_today_v26660"
    )

    score = np.zeros(len(df))

    reason = []

    for i in range(len(df)):

        r = []

        s = 0

        # =========================
        # 第一次站上 ma20
        # =========================

        if (
            close.iloc[i] > ma20.iloc[i]
            and close.iloc[i] < ma20.iloc[i] * 1.08
        ):

            s += 10

            r.append("第一次站上MA20")

        # =========================
        # 第一次溫和放量
        # =========================

        if (
            vr.iloc[i] >= 1.3
            and vr.iloc[i] <= 2.5
        ):

            s += 10

            r.append("第一次溫和放量")

        # =========================
        # 籌碼剛轉強
        # =========================

        if (
            chip.iloc[i] >= 8
            and chip.iloc[i] <= 16
        ):

            s += 8

            r.append("籌碼剛轉強")

        # =========================
        # trigger 有事件
        # =========================

        if (
            trigger.iloc[i] >= 18
            and trigger.iloc[i] <= 40
        ):

            s += 8

            r.append("trigger事件成立")

        # =========================
        # 狀態剛轉強
        # =========================

        if (
            state_rank.iloc[i] >= 3
            and state_rank.iloc[i] <= 5
        ):

            s += 6

            r.append("狀態剛升級")

        # =========================
        # 過熱懲罰
        # =========================

        if vr.iloc[i] > 4:

            s -= 15

            r.append("爆量過熱")

        if (
            ma20.iloc[i] > 0
            and close.iloc[i] > ma20.iloc[i] * 1.18
        ):

            s -= 20

            r.append("乖離過大")

        # =========================
        # 已經太強扣分
        # =========================

        if trigger.iloc[i] > 50:

            s -= 12

            r.append("已經強很久")

        score[i] = s

        reason.append("｜".join(r))

    df["first_trigger_score_v26663"] = score

    df["first_trigger_reason_v26663"] = reason

    # =========================
    # label
    # =========================

    label = []

    for s in score:

        if s >= 32:

            label.append("FIRST_IGNITION")

        elif s >= 22:

            label.append("EARLY_BREAK")

        elif s >= 12:

            label.append("WATCH")

        else:

            label.append("NONE")

    df["first_trigger_label_v26663"] = label

    # =========================
    # lifecycle clean:
    # 只補 first trigger 分數與排名，不重新排序、不改 bucket、不改 action。
    # 原因：
    # 這支若 sort_values 後覆蓋原檔，後段會把它當成新的 ranking pool，
    # 造成 IGNITION / EVOLUTION 同質化。
    # =========================

    order = (
        pd.Series(score, index=df.index)
        .rank(method="first", ascending=False)
        .astype(int)
    )
    df["first_trigger_rank_v26663"] = order

    return df

# =========================
# patch
# =========================

def patch_one(path):

    p = Path(path)

    if not p.exists():

        print("missing:", p)

        return

    try:

        df = pd.read_csv(p)

        if df.empty:

            print("empty:", p)

            return

        out = build_first_trigger(df)

        out.to_csv(
            p,
            index=False,
            encoding="utf-8-sig"
        )

        print("patched:", p, len(out))

    except Exception as e:

        print("failed:", p, e)

# =========================
# main
# =========================

def main():

    for name in TARGETS:

        patch_one(ROOT / name)

        patch_one(DATA_DIR / name)

    print("v266.63 first trigger patch done")

if __name__ == "__main__":
