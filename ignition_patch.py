# -*- coding: utf-8 -*-
"""
v266.68 ignition_patch.py
主力發動前夕辨識補丁

核心：
找「主力剛準備發動」，不是已經發動完。

新增欄位：
- compression_score_v26668
- first_volume_trigger_v26668
- accumulation_score_v26668
- fake_breakout_risk_v26668
- ignition_score_v26668
- ignition_phase_v26668
- ignition_reason_v26668
- ignition_rank_v26668
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import re
import pandas as pd
import numpy as np
from pandas.errors import EmptyDataError

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = [
    "trade_plan.csv",
    "final_action_plan.csv",
    "timing_candidates.csv",
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
    "pre_move_candidates.csv",
    "top_opportunities.csv",
]


def now_tw():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def read_csv_safe(path):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(), "missing"
    if p.stat().st_size == 0:
        return pd.DataFrame(), "empty_file"
    last = ""
    for enc in ["utf-8-sig", "utf-8"]:
        try:
            df = pd.read_csv(p, dtype=str, encoding=enc)
            if df is None or df.empty:
                return pd.DataFrame(), "empty_rows"
            return df, "ok"
        except EmptyDataError:
            return pd.DataFrame(), "empty_data_error"
        except Exception as e:
            last = str(e)
    return pd.DataFrame(), "read_failed:" + last


def write_csv(df, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding="utf-8-sig")


def num(df, col, default=0.0):
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce"
    ).fillna(default)


def txt(df, col):
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[col].fillna("").astype(str)


def clean_sid(v):
    s = "" if v is None else str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s


def calc_ignition(df):
    df = df.copy()

    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].map(clean_sid)

    close = num(df, "close")
    ref_price = num(df, "ref_price")
    price = num(df, "price")
    close = close.where(close != 0, ref_price)
    close = close.where(close != 0, price)

    ma5 = num(df, "ma5")
    ma10 = num(df, "ma10")
    ma20 = num(df, "ma20")
    volume_ratio = num(df, "volume_ratio", 1.0)
    chip = num(df, "chip_score")
    score = num(df, "score")
    state_rank = num(df, "state_rank_today_v26660")
    state_transition = num(df, "state_transition_score_v26660")
    trigger_score = num(df, "trigger_event_score_v26662")
    first_trigger = num(df, "first_trigger_score_v26663")
    turn_score = num(df, "turn_event_score_v26664_1")
    trigger_quality = num(df, "trigger_quality")

    blob = (
        txt(df, "reason") + " " +
        txt(df, "system_note") + " " +
        txt(df, "tech_reason") + " " +
        txt(df, "chip_reason") + " " +
        txt(df, "k_structure") + " " +
        txt(df, "kline_structure") + " " +
        txt(df, "kbar_type") + " " +
        txt(df, "behavior_hint") + " " +
        txt(df, "trigger_event_reason_v26662") + " " +
        txt(df, "first_trigger_reason_v26663") + " " +
        txt(df, "turn_event_reason_v26664_1") + " " +
        txt(df, "state_transition_reason_v26660")
    )

    n = len(df)
    compression = pd.Series([0.0] * n, index=df.index)
    first_vol = pd.Series([0.0] * n, index=df.index)
    accumulation = pd.Series([0.0] * n, index=df.index)
    fake_risk = pd.Series([0.0] * n, index=df.index)
    reasons = pd.Series([""] * n, index=df.index, dtype=object)

    def add(series, mask, points, reason):
        nonlocal reasons
        mask = mask.fillna(False)
        series.loc[mask] += points
        reasons.loc[mask] = reasons.loc[mask] + reason + "｜"

    add(compression, blob.str.contains("平台壓縮|40D平台收斂|收斂|整理收斂|波動壓縮|短中均線糾結", regex=True, na=False), 25, "平台/波動壓縮")
    add(compression, (ma20 > 0) & (close > ma20 * 0.97) & (close < ma20 * 1.06), 18, "靠近MA20臨界區")
    add(compression, (ma5 > 0) & (ma10 > 0) & (ma20 > 0) & ((ma5 - ma20).abs() / ma20 <= 0.06) & ((ma10 - ma20).abs() / ma20 <= 0.06), 15, "均線收斂")
    add(compression, volume_ratio.between(0.65, 1.25), 12, "量能安靜")
    add(compression, blob.str.contains("量縮|洗盤|整理|換手|低點墊高|回檔不破", regex=True, na=False), 12, "洗盤整理不破")

    add(first_vol, volume_ratio.between(1.20, 1.85), 28, "第一次溫和放量")
    add(first_vol, volume_ratio.between(1.86, 2.50), 14, "放量偏明顯")
    add(first_vol, volume_ratio > 2.50, -18, "量能偏大非潛伏")
    add(first_vol, volume_ratio > 4.00, -35, "爆量追高風險")
    add(first_vol, blob.str.contains("溫和放量|初放量|第一次放量|量能回溫", regex=True, na=False), 18, "初放量文字確認")
    add(first_vol, blob.str.contains("爆量|大量|巨量", regex=True, na=False), -20, "爆量文字扣分")

    add(accumulation, chip.between(20, 45), 28, "籌碼20-45初升")
    add(accumulation, chip.between(46, 60), 18, "籌碼46-60佈局中")
    add(accumulation, chip.between(61, 72), 8, "籌碼偏高需確認")
    add(accumulation, chip > 75, -20, "籌碼過高疑似後段")
    add(accumulation, blob.str.contains("吸籌|籌碼初升|法人偏多|主力佈局|鎖籌|集中|承接", regex=True, na=False), 18, "主力吸籌文字確認")
    add(accumulation, state_transition.between(6, 18), 12, "狀態轉移剛加速")
    add(accumulation, state_rank.between(2, 5), 10, "狀態剛升級")

    add(fake_risk, blob.str.contains("假突破|長上影|高檔出貨|誘多|出貨疑慮|追高|過熱|爆量不漲|開高走低", regex=True, na=False), 35, "假突破/出貨文字風險")
    add(fake_risk, (ma20 > 0) & (close / ma20 > 1.12), 18, "離MA20過遠")
    add(fake_risk, (ma20 > 0) & (close / ma20 > 1.18), 32, "MA20乖離過熱")
    add(fake_risk, volume_ratio > 3.5, 18, "量能過熱")
    add(fake_risk, score >= 80, 10, "原始強度偏高")
    add(fake_risk, blob.str.contains("強勢延續|續強|多頭排列很久|突破確認K", regex=True, na=False), 12, "偏續強非前夜")

    ignition = (
        compression * 0.28 +
        first_vol * 0.26 +
        accumulation * 0.26 +
        np.minimum(trigger_score, 35) * 0.06 +
        np.minimum(first_trigger, 38) * 0.06 +
        np.minimum(turn_score, 55) * 0.06 +
        np.minimum(trigger_quality, 85) * 0.02 -
        fake_risk * 0.55
    )

    data_quality_penalty = ((ma20 <= 0).astype(int) + (volume_ratio <= 0).astype(int) + (chip <= 0).astype(int)) * 8
    ignition = ignition - data_quality_penalty

    df["compression_score_v26668"] = compression.round(2)
    df["first_volume_trigger_v26668"] = first_vol.round(2)
    df["accumulation_score_v26668"] = accumulation.round(2)
    df["fake_breakout_risk_v26668"] = fake_risk.round(2)
    df["ignition_score_v26668"] = ignition.round(2)
    df["ignition_reason_v26668"] = reasons.str.rstrip("｜")
    df["ignition_patch_version"] = "v266.68"
    df["ignition_updated_at"] = now_tw()

    phase = pd.Series(["NOISE"] * n, index=df.index, dtype=object)
    phase.loc[(ignition >= 28) & (fake_risk <= 20)] = "WATCH_BUILDING"
    phase.loc[(ignition >= 38) & (fake_risk <= 18)] = "PRE_IGNITION"
    phase.loc[(ignition >= 48) & (fake_risk <= 15)] = "IGNITION_READY"
    phase.loc[(fake_risk >= 35)] = "FAKE_BREAKOUT_RISK"
    phase.loc[(compression >= 40) & (first_vol < 12) & (fake_risk <= 20)] = "BUILDING_WAIT"
    df["ignition_phase_v26668"] = phase

    hint_map = {
        "IGNITION_READY": "主力發動前夕：可優先試單，隔日看不破MA5/MA10。",
        "PRE_IGNITION": "接近點火：放入TEST/優先觀察，等待第一根確認。",
        "WATCH_BUILDING": "收斂佈局：放入WATCH，等待第一次溫和放量。",
        "BUILDING_WAIT": "主力潛伏：尚未放量，不急進場。",
        "FAKE_BREAKOUT_RISK": "假突破/追高風險：避免追價。",
        "NOISE": "雜訊：條件不足。"
    }
    df["ignition_hint_v26668"] = df["ignition_phase_v26668"].map(hint_map).fillna("依原策略觀察。")

    suggested = pd.Series(["WATCH"] * n, index=df.index, dtype=object)
    suggested.loc[df["ignition_phase_v26668"].isin(["IGNITION_READY", "PRE_IGNITION"])] = "TEST"
    suggested.loc[df["ignition_phase_v26668"].isin(["FAKE_BREAKOUT_RISK", "NOISE"])] = "WATCH"
    df["ignition_suggested_mode_v26668"] = suggested

    if "watch_mode" in df.columns:
        df["watch_mode"] = df["ignition_suggested_mode_v26668"]

    df = df.sort_values(
        by=["ignition_suggested_mode_v26668", "ignition_score_v26668"],
        ascending=[True, False]
    ).copy()
    df["ignition_rank_v26668"] = range(1, len(df) + 1)

    return df


def patch_one(name):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)
        if status != "ok":
            report[str(p)] = {"status": status, "rows": 0}
            print(f"[v266.68] skip {p}: {status}")
            continue

        try:
            out = calc_ignition(df)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)
            report[str(p)] = {"status": "updated", "rows": int(len(out))}
            print(f"[v266.68] updated {p}: {len(out)}")
        except Exception as e:
            report[str(p)] = {"status": "failed", "rows": int(len(df)), "error": str(e)}
            print(f"[v266.68] failed {p}: {e}")
    return report


def main():
    report = {
        "version": "v266.68",
        "mode": "ignition_prelaunch_patch",
        "changed_strategy_logic": False,
        "changed_position": False,
        "updated_at": now_tw(),
        "files": {}
    }

    for name in TARGETS:
        report["files"][name] = patch_one(name)

    summary_df = pd.DataFrame()
    for p in [ROOT / "trade_plan.csv", ROOT / "final_action_plan.csv", DATA_DIR / "trade_plan.csv"]:
        df, status = read_csv_safe(p)
        if status == "ok":
            summary_df = df
            break

    if not summary_df.empty and "ignition_phase_v26668" in summary_df.columns:
        report["ignition_phase_counts"] = summary_df["ignition_phase_v26668"].fillna("").astype(str).value_counts().to_dict()
        report["suggested_mode_counts"] = summary_df["ignition_suggested_mode_v26668"].fillna("").astype(str).value_counts().to_dict()
    else:
        report["ignition_phase_counts"] = {}
        report["suggested_mode_counts"] = {}

    for p in [ROOT / "ignition_report_v26668.json", DATA_DIR / "ignition_report_v26668.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
