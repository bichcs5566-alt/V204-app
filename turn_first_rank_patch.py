# -*- coding: utf-8 -*-
"""
v266.64.1 turn_first_rank_patch.py
收斂版 TURN_FIRST 引擎

目標：
把 TURN_FIRST 從 55 檔
壓縮到：
10~20 檔左右

核心：
必須同時符合：
1. MA20剛站回
2. 溫和放量
3. 籌碼初升 或 狀態剛升級
4. 不得過熱/假突破

不改：
- 原策略
- action
- 持倉
- 停損
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import json
import re

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"

TARGETS = [
    "final_action_plan.csv",
    "timing_candidates.csv",
    "trade_plan.csv",
]

def now_tw():
    return datetime.now(
        timezone(timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")

def read_csv_safe(path):

    p = Path(path)

    if not p.exists():
        return pd.DataFrame(), "missing"

    try:

        df = pd.read_csv(
            p,
            encoding="utf-8-sig"
        )

        if df.empty:
            return pd.DataFrame(), "empty"

        return df, "ok"

    except Exception as e:

        return pd.DataFrame(), str(e)

def write_csv(df, path):

    p = Path(path)

    p.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        p,
        index=False,
        encoding="utf-8-sig"
    )

def num(df, col, default=0):

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

def txt(df, col):

    if col not in df.columns:

        return pd.Series(
            "",
            index=df.index,
            dtype=str
        )

    return df[col].fillna("").astype(str)

def calc(df):

    df = df.copy()

    close = num(df, "close")
    ref_price = num(df, "ref_price")

    close = close.where(
        close != 0,
        ref_price
    )

    ma20 = num(df, "ma20")

    vr = num(
        df,
        "volume_ratio",
        1
    )

    chip = num(
        df,
        "chip_score"
    )

    state_rank = num(
        df,
        "state_rank_today_v26660"
    )

    state_transition = num(
        df,
        "state_transition_score_v26660"
    )

    trigger = num(
        df,
        "trigger_event_score_v26662"
    )

    first_trigger = num(
        df,
        "first_trigger_score_v26663"
    )

    continuation = num(
        df,
        "continuation_quality_score"
    )

    ignition_power = num(
        df,
        "ignition_power_score"
    )

    blob = (
        txt(df, "reason")
        + " "
        + txt(df, "system_note")
        + " "
        + txt(df, "chip_reason")
        + " "
        + txt(df, "trigger_event_reason_v26662")
        + " "
        + txt(df, "first_trigger_reason_v26663")
        + " "
        + txt(df, "state_transition_reason_v26660")
    )

    score = []
    label = []
    reason = []

    for i in range(len(df)):

        s = 0
        r = []

        ma20_reclaim = (
            ma20.iloc[i] > 0
            and close.iloc[i] > ma20.iloc[i]
            and close.iloc[i] <= ma20.iloc[i] * 1.06
        )

        mild_volume = (
            vr.iloc[i] >= 1.2
            and vr.iloc[i] <= 2.8
        )

        chip_early = (
            chip.iloc[i] >= 6
            and chip.iloc[i] <= 18
        )

        state_upgrade = (
            state_rank.iloc[i] >= 2
            and state_rank.iloc[i] <= 5
            and state_transition.iloc[i] >= 6
        )

        trigger_ok = (
            trigger.iloc[i] >= 10
            and trigger.iloc[i] <= 35
        )

        first_ok = (
            first_trigger.iloc[i] >= 12
            and first_trigger.iloc[i] <= 38
        )

        hot = (
            vr.iloc[i] > 3.5
            or (
                ma20.iloc[i] > 0
                and close.iloc[i] > ma20.iloc[i] * 1.12
            )
            or continuation.iloc[i] >= 8
            or ignition_power.iloc[i] >= 8
        )

        risk_text = bool(
            re.search(
                r"高檔出貨|假突破|長上影|誘多|出貨疑慮|追高|過熱",
                blob.iloc[i]
            )
        )

        # =========================
        # TURN_FIRST 必要條件
        # =========================

        if ma20_reclaim:

            s += 20
            r.append("MA20剛站回")

        if mild_volume:

            s += 18
            r.append("溫和放量")

        if chip_early:

            s += 20
            r.append("籌碼初升")

        if state_upgrade:

            s += 18
            r.append("狀態剛升級")

        if trigger_ok:

            s += 10
            r.append("trigger合理")

        if first_ok:

            s += 14
            r.append("first trigger合理")

        if "平台壓縮" in blob.iloc[i] or "收斂" in blob.iloc[i]:

            s += 12
            r.append("平台收斂")

        if "回檔不破" in blob.iloc[i] or "洗盤收回" in blob.iloc[i]:

            s += 10
            r.append("洗盤承接")

        # =========================
        # 過熱扣分
        # =========================

        if hot:

            s -= 25
            r.append("過熱扣分")

        if risk_text:

            s -= 30
            r.append("風險文字")

        # =========================
        # label
        # =========================

        is_turn_first = (
            ma20_reclaim
            and mild_volume
            and (
                chip_early
                or state_upgrade
            )
            and not hot
            and not risk_text
            and s >= 55
        )

        is_early_turn = (
            ma20_reclaim
            and mild_volume
            and s >= 42
        )

        if is_turn_first:

            lb = "TURN_FIRST"

        elif is_early_turn:

            lb = "EARLY_TURN"

        elif s >= 28:

            lb = "WATCH_TURN"

        elif s <= 0:

            lb = "AVOID_CHASE"

        else:

            lb = "NONE"

        score.append(round(s, 2))
        label.append(lb)
        reason.append("｜".join(r))

    df["turn_event_score_v26664_1"] = score

    df["turn_event_label_v26664_1"] = label

    df["turn_event_reason_v26664_1"] = reason

    df = df.sort_values(
        "turn_event_score_v26664_1",
        ascending=False
    )

    df["turn_first_rank_v26664_1"] = (
        range(1, len(df) + 1)
    )

    return df

def patch_one(name):

    report = {}

    for base in [ROOT, DATA_DIR]:

        p = base / name

        df, status = read_csv_safe(p)

        if status != "ok":

            report[str(p)] = status
            continue

        try:

            out = calc(df)

            write_csv(out, p)

            if base == ROOT:

                write_csv(
                    out,
                    DATA_DIR / name
                )

            report[str(p)] = len(out)

        except Exception as e:

            report[str(p)] = str(e)

    return report

def main():

    report = {
        "version": "v266.64.1",
        "updated_at": now_tw(),
        "files": {}
    }

    for name in TARGETS:

        report["files"][name] = patch_one(name)

    for p in [
        ROOT / "turn_first_report.json",
        DATA_DIR / "turn_first_report.json"
    ]:

        p.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8-sig"
        )

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2
        )
    )

if __name__ == "__main__":
    main()
