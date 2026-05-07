# -*- coding: utf-8 -*-
"""
v266.59 主力發動狀態機補丁
不覆蓋原策略
只新增：
- evolution_state
- ignition_state
- 主力行為分類
- 狀態升級追蹤
"""

from pathlib import Path
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"

TARGETS = [
    "trade_plan.csv",
    "watchlist_monitor.csv",
    "final_action_plan.csv",
    "candidates.csv",
]

def safe_num(df, col, default=0):
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)

    return pd.to_numeric(
        df[col].astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False),
        errors="coerce"
    ).fillna(default)

def apply_state_engine(df):

    df = df.copy()

    score = safe_num(df, "score")
    chip = safe_num(df, "chip_score")
    vol = safe_num(df, "volume_ratio", 1)
    close = safe_num(df, "close")
    ma20 = safe_num(df, "ma20")
    ma60 = safe_num(df, "ma60")

    evolution_state = []
    ignition_state = []
    evolution_reason = []

    for i in range(len(df)):

        s = score.iloc[i]
        c = chip.iloc[i]
        v = vol.iloc[i]
        cl = close.iloc[i]
        m20 = ma20.iloc[i]
        m60 = ma60.iloc[i]

        state = "觀察"
        ignite = "否"
        reason = []

        if c >= 8:
            state = "吸籌中"
            reason.append("法人偏多")

        if c >= 12 and v > 1.2:
            state = "試盤中"
            reason.append("量能放大")

        if cl > m20 and m20 > m60:
            state = "突破中"
            reason.append("均線轉強")

        if s >= 75 and c >= 15 and v >= 1.5:
            state = "發動中"
            ignite = "準IGNITION"
            reason.append("主力發動")

        if s >= 85 and c >= 18 and cl > m20:
            state = "延續中"
            ignite = "IGNITION"
            reason.append("趨勢延續")

        if c <= -5:
            state = "出貨疑慮"
            ignite = "否"
            reason.append("籌碼轉弱")

        evolution_state.append(state)
        ignition_state.append(ignite)
        evolution_reason.append("｜".join(reason))

    df["evolution_state"] = evolution_state
    df["ignition_state"] = ignition_state
    df["evolution_reason"] = evolution_reason

    return df

def patch_file(path):

    p = Path(path)

    if not p.exists():
        print("missing:", p)
        return

    try:
        df = pd.read_csv(p)

        if df.empty:
            print("empty:", p)
            return

        out = apply_state_engine(df)

        out.to_csv(p, index=False, encoding="utf-8-sig")

        print("patched:", p, len(out))

    except Exception as e:
        print("failed:", p, e)

def main():

    for name in TARGETS:

        patch_file(ROOT / name)
        patch_file(DATA_DIR / name)

    print("v266.59 state engine patch done")

if __name__ == "__main__":
    main()
