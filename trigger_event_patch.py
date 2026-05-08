# -*- coding: utf-8 -*-
"""
v266.62 trigger_event_patch.py
主力異常事件偵測引擎

目的：
把系統從「強度排序」補成「事件偵測」。
專抓：
- 第一次站上 MA20
- 第一次爆量不跌
- 平台壓縮後放量
- 洗盤不破
- 突破近高但未過熱
- 籌碼/狀態 READY 確認

不改：
- 原策略
- action
- 持倉
- 停損
- 原分數
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import re
import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = [
    "timing_candidates.csv",
    "trade_plan.csv",
    "final_action_plan.csv",
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
    "pre_move_candidates.csv",
    "top_opportunities.csv",
]

def now_tw():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

def sid(v):
    s = "" if v is None else str(v).strip()
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else s

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

def calc_trigger_events(df):
    df = df.copy()

    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].map(sid)

    close = num(df, "close")
    ref_price = num(df, "ref_price")
    close = close.where(close != 0, ref_price)

    ma20 = num(df, "ma20")
    volume_ratio = num(df, "volume_ratio", 1.0)
    chip_score = num(df, "chip_score")
    structure_score = num(df, "structure_pre_score")
    continuation_score = num(df, "continuation_quality_score")
    ignition_power = num(df, "ignition_power_score")

    blob = (
        txt(df, "reason") + " " +
        txt(df, "system_note") + " " +
        txt(df, "chip_reason") + " " +
        txt(df, "structure_pre_reason") + " " +
        txt(df, "continuation_quality_reason") + " " +
        txt(df, "ignition_power_reason") + " " +
        txt(df, "state_transition_reason_v26660") + " " +
        txt(df, "behavior_hint") + " " +
        txt(df, "k_structure") + " " +
        txt(df, "kbar_type")
    )

    trigger_score = pd.Series([0.0] * len(df), index=df.index)
    reasons = pd.Series([""] * len(df), index=df.index, dtype=object)

    def add(mask, points, reason):
        nonlocal trigger_score, reasons
        mask = mask.fillna(False)
        trigger_score.loc[mask] += points
        reasons.loc[mask] = reasons.loc[mask] + reason + "｜"

    add((close > ma20) & (ma20 > 0) & (structure_score >= 4), 8, "站上MA20且結構成形")
    add((volume_ratio >= 1.4) & (volume_ratio <= 3.5) & (close >= ma20) & (ma20 > 0), 10, "溫和放量不跌")
    add(blob.str.contains("40D平台收斂|短中均線糾結|平台壓縮|收斂", regex=True, na=False) & (volume_ratio >= 1.2), 10, "平台壓縮後放量")
    add(blob.str.contains("回檔不破|低點墊高|洗盤收回|回檔量縮|承接", regex=True, na=False), 8, "洗盤不破承接")
    add((ignition_power >= 4) & (volume_ratio <= 4.5), 7, "突破品質成立但未過熱")
    add((chip_score >= 8) & (chip_score <= 24), 8, "籌碼偏多未過熱")
    add(txt(df, "ignition_upgrade_flag_v26660").str.contains("READY|WATCH_UP|YES", regex=True, na=False), 8, "狀態升級觸發")
    add(txt(df, "state_transition_label_v26660").str.contains("準IGNITION|升級觀察|IGNITION", regex=True, na=False), 8, "狀態轉移轉強")
    add(txt(df, "entry_signal").str.contains("WAIT", regex=True, na=False) & (structure_score >= 6) & (chip_score >= 8), 6, "WAIT但結構籌碼提前轉強")
    add(txt(df, "lane").str.contains("PRE", regex=True, na=False) & (structure_score >= 5) & (volume_ratio >= 1.1), 6, "PRE階段出現點火事件")

    add((volume_ratio > 5.0), -10, "爆量過熱扣分")
    add((ma20 > 0) & (close / ma20 > 1.18), -12, "遠離MA20過熱扣分")
    add(blob.str.contains("高檔出貨|假突破|長上影|誘多|出貨疑慮|籌碼風險", regex=True, na=False), -12, "假突破/出貨風險扣分")

    df["trigger_event_score_v26662"] = trigger_score.round(2)
    df["trigger_event_reason_v26662"] = reasons.str.rstrip("｜")

    label = pd.Series(["無事件"] * len(df), index=df.index, dtype=object)
    label.loc[df["trigger_event_score_v26662"] >= 28] = "強點火事件"
    label.loc[(df["trigger_event_score_v26662"] >= 18) & (df["trigger_event_score_v26662"] < 28)] = "準點火事件"
    label.loc[(df["trigger_event_score_v26662"] >= 10) & (df["trigger_event_score_v26662"] < 18)] = "早期轉強事件"
    label.loc[df["trigger_event_score_v26662"] <= -8] = "過熱/風險事件"
    df["trigger_event_label_v26662"] = label

    hint = pd.Series(["等待下一次確認。"] * len(df), index=df.index, dtype=object)
    hint.loc[label == "強點火事件"] = "可列入優先追蹤，觀察隔日不破MA5/MA10。"
    hint.loc[label == "準點火事件"] = "具備起漲條件，可放入TEST/準IGNITION觀察。"
    hint.loc[label == "早期轉強事件"] = "偏早期，可列入WATCH等待二次確認。"
    hint.loc[label == "過熱/風險事件"] = "避免追高，等回檔不破再評估。"
    df["trigger_action_hint_v26662"] = hint

    base_col = next((c for c in [
        "state_adjusted_score_v26660",
        "chip_adjusted_score_v26658",
        "adjusted_signal_score_v26657_9",
        "score",
        "final_score",
        "entry_score",
    ] if c in df.columns), None)

    if base_col:
        base = num(df, base_col)
        df["trigger_adjusted_score_v26662"] = (base * 0.70 + df["trigger_event_score_v26662"] * 0.30).round(3)
    else:
        df["trigger_adjusted_score_v26662"] = df["trigger_event_score_v26662"]

    df = df.sort_values("trigger_adjusted_score_v26662", ascending=False).copy()
    df["trigger_rank_v26662"] = range(1, len(df) + 1)
    df["trigger_patch_version"] = "v266.62"
    df["trigger_updated_at"] = now_tw()

    return df

def patch_one(name):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)

        if status != "ok":
            report[str(p)] = {"status": status, "rows": 0}
            print(f"[v266.62] skip {p}: {status}")
            continue

        try:
            out = calc_trigger_events(df)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)
            report[str(p)] = {"status": "updated", "rows": int(len(out))}
            print(f"[v266.62] updated {p}: {len(out)}")
        except Exception as e:
            report[str(p)] = {"status": "failed", "error": str(e), "rows": int(len(df))}
            print(f"[v266.62] failed {p}: {e}")

    return report

def main():
    report = {
        "version": "v266.62",
        "mode": "trigger_event_engine_patch",
        "changed_strategy_logic": False,
        "changed_action": False,
        "changed_position": False,
        "updated_at": now_tw(),
        "files": {},
    }

    for name in TARGETS:
        report["files"][name] = patch_one(name)

    summary_df = pd.DataFrame()
    for p in [ROOT / "final_action_plan.csv", ROOT / "timing_candidates.csv", DATA_DIR / "final_action_plan.csv"]:
        df, status = read_csv_safe(p)
        if status == "ok":
            summary_df = df
            break

    if not summary_df.empty and "trigger_event_label_v26662" in summary_df.columns:
        report["trigger_labels"] = summary_df["trigger_event_label_v26662"].fillna("").astype(str).value_counts().to_dict()
    else:
        report["trigger_labels"] = {}

    for p in [ROOT / "trigger_event_report.json", DATA_DIR / "trigger_event_report.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
