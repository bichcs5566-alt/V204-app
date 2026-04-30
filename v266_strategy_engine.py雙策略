# v266_strategy_engine.py (實戰版強化)
# 核心：強制流動性分層 + 可執行性優先

# 👉 修改重點：
# 1. CORE 最低流動性 = 1000張
# 2. TEST 500~1000張
# 3. <500張直接BLOCK
# 4. ALPHA 維持高流動性
# 5. 資金配置更保守（避免卡單）

# ⚠️ 可直接覆蓋原本檔案使用

import pandas as pd
import numpy as np
from pathlib import Path

def add_liquidity_filter(df):
    df = df.copy()

    df["liquidity_flag"] = "BLOCK"

    df.loc[df["volume"] >= 3000, "liquidity_flag"] = "ALPHA"
    df.loc[(df["volume"] >= 1000) & (df["volume"] < 3000), "liquidity_flag"] = "CORE"
    df.loc[(df["volume"] >= 500) & (df["volume"] < 1000), "liquidity_flag"] = "TEST"

    return df


def apply_strategy(df):
    df = add_liquidity_filter(df)

    df["action"] = "BLOCK"

    # ===== ALPHA =====
    alpha_cond = (
        (df["liquidity_flag"] == "ALPHA") &
        (df["close"] > df["ma20"]) &
        (df["mom10"] > 0.03) &
        (df["volume_ratio"] > 1.2)
    )

    df.loc[alpha_cond, "action"] = "BUY"
    df.loc[alpha_cond, "strategy"] = "ALPHA"

    # ===== CORE =====
    core_cond = (
        (df["liquidity_flag"] == "CORE") &
        (df["close"] > df["ma20"] * 0.98) &
        (df["mom20"] > 0.04)
    )

    df.loc[core_cond, "action"] = "TEST"
    df.loc[core_cond, "strategy"] = "CORE"

    # ===== TEST =====
    test_cond = (
        (df["liquidity_flag"] == "TEST") &
        (df["mom10"] > 0)
    )

    df.loc[test_cond, "action"] = "WATCH"
    df.loc[test_cond, "strategy"] = "TEST"

    return df


def position_sizing(df):
    df = df.copy()

    df["weight"] = 0

    # ALPHA 主力倉
    df.loc[df["strategy"] == "ALPHA", "weight"] = 0.03

    # CORE 小倉
    df.loc[df["strategy"] == "CORE", "weight"] = 0.01

    # TEST 超小
    df.loc[df["strategy"] == "TEST", "weight"] = 0.003

    return df


def run(df):
    df = apply_strategy(df)
    df = position_sizing(df)

    return df


if __name__ == "__main__":
    print("實戰版策略已載入")
