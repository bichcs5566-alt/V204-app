# =========================================================
# v266.58_market_regime_dynamic_weight_patch
# =========================================================

import pandas as pd

def build_market_regime(df):

    regime = {
        "market_mode": "NEUTRAL",
        "market_score": 0,
        "ignition_weight": 1.0,
        "core_weight": 1.0,
        "watch_weight": 1.0
    }

    try:

        up_ratio = (df["close"] > df["ma20"]).mean()

        strong_ratio = (
            (df["mom20"] > 0.15) &
            (df["volume_ratio"] > 1.2)
        ).mean()

        breakout_ratio = (
            (df["close"] >= df["rolling_20_high"] * 0.98)
        ).mean()

        weak_ratio = (
            (df["close"] < df["ma60"])
        ).mean()

        market_score = 0

        if up_ratio > 0.62:
            market_score += 2

        if strong_ratio > 0.18:
            market_score += 2

        if breakout_ratio > 0.12:
            market_score += 2

        if weak_ratio > 0.45:
            market_score -= 3

        if market_score >= 4:

            regime["market_mode"] = "IGNITION"

            regime["ignition_weight"] = 1.35
            regime["core_weight"] = 0.95
            regime["watch_weight"] = 0.85

        elif market_score <= 0:

            regime["market_mode"] = "CORE"

            regime["ignition_weight"] = 0.75
            regime["core_weight"] = 1.35
            regime["watch_weight"] = 1.15

        else:

            regime["market_mode"] = "WATCH"

            regime["ignition_weight"] = 0.95
            regime["core_weight"] = 1.05
            regime["watch_weight"] = 1.35

        regime["market_score"] = market_score

    except Exception as e:

        print("market regime failed:", e)

    return regime


def apply_market_regime_patch(df):

    regime = build_market_regime(df)

    if "ignition_power_score" in df.columns:

        df["ignition_power_score_v26658"] = (
            df["ignition_power_score"] *
            regime["ignition_weight"]
        )

    if "structure_pre_score" in df.columns:

        df["structure_pre_score_v26658"] = (
            df["structure_pre_score"] *
            regime["core_weight"]
        )

    if "continuation_quality_score" in df.columns:

        df["continuation_quality_score_v26658"] = (
            df["continuation_quality_score"] *
            regime["watch_weight"]
        )

    score_cols = []

    for c in [
        "ignition_power_score_v26658",
        "structure_pre_score_v26658",
        "continuation_quality_score_v26658"
    ]:

        if c in df.columns:
            score_cols.append(c)

    if len(score_cols) > 0:

        df["market_dynamic_score_v26658"] = (
            df[score_cols].sum(axis=1)
        )

    df["market_mode_v26658"] = regime["market_mode"]
    df["market_score_v26658"] = regime["market_score"]

    return df, regime
