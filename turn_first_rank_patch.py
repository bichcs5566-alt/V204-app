# -*- coding: utf-8 -*-
"""
v266.64 turn_first_rank_patch.py
轉折優先排序補丁：把排序從「已經很強」改成「剛開始轉強」。

不改原策略 / action / position / stoploss，只新增轉折排序欄位：
- turn_event_score_v26664
- turn_event_label_v26664
- turn_event_reason_v26664
- turn_first_rank_v26664
- turn_priority_hint_v26664
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
    "final_action_plan.csv",
    "timing_candidates.csv",
    "trade_plan.csv",
    "top_opportunities.csv",
    "pre_move_candidates.csv",
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
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

def calc_turn_first(df):
    df = df.copy()
    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].map(sid)

    close = num(df, "close")
    ref_price = num(df, "ref_price")
    close = close.where(close != 0, ref_price)

    ma5 = num(df, "ma5")
    ma10 = num(df, "ma10")
    ma20 = num(df, "ma20")
    vr = num(df, "volume_ratio", 1.0)
    chip = num(df, "chip_score")
    trigger = num(df, "trigger_event_score_v26662")
    first_trigger = num(df, "first_trigger_score_v26663")
    state_rank = num(df, "state_rank_today_v26660")
    state_transition = num(df, "state_transition_score_v26660")
    structure = num(df, "structure_pre_score")
    continuation = num(df, "continuation_quality_score")
    ignition_power = num(df, "ignition_power_score")

    blob = (
        txt(df, "reason") + " " +
        txt(df, "system_note") + " " +
        txt(df, "chip_reason") + " " +
        txt(df, "trigger_event_reason_v26662") + " " +
        txt(df, "first_trigger_reason_v26663") + " " +
        txt(df, "state_transition_reason_v26660") + " " +
        txt(df, "structure_pre_reason") + " " +
        txt(df, "continuation_quality_reason") + " " +
        txt(df, "k_structure") + " " +
        txt(df, "kbar_type") + " " +
        txt(df, "behavior_hint")
    )

    score = pd.Series([0.0] * len(df), index=df.index)
    reason = pd.Series([""] * len(df), index=df.index, dtype=object)

    def add(mask, points, why):
        nonlocal score, reason
        mask = mask.fillna(False)
        score.loc[mask] += points
        reason.loc[mask] = reason.loc[mask] + why + "｜"

    # 轉折事件：優先加分
    add((ma20 > 0) & (close > ma20) & (close / ma20 <= 1.08), 18, "MA20剛站回/未過熱")
    add((ma5 > 0) & (ma10 > 0) & (close > ma5) & (ma5 >= ma10) & ((ma20 <= 0) | (close / ma20 <= 1.10)), 10, "短均剛轉強")
    add(blob.str.contains("平台壓縮|40D平台收斂|短中均線糾結|收斂|波動壓縮", regex=True, na=False) & (vr >= 1.1) & (vr <= 3.0), 20, "平台壓縮後第一次釋放")
    add((vr >= 1.25) & (vr <= 2.8), 16, "第一次溫和放量")
    add((chip >= 6) & (chip <= 18), 18, "籌碼初升區")
    add((chip > 18) & (chip <= 28), 8, "籌碼已強但未失控")
    add((state_rank >= 2) & (state_rank <= 5), 14, "狀態剛升級")
    add((state_transition >= 6) & (state_transition <= 18), 12, "轉移分剛加速")
    add((trigger >= 10) & (trigger <= 32), 10, "trigger事件在合理區")
    add((first_trigger >= 12) & (first_trigger <= 38), 16, "first trigger成立")
    add(blob.str.contains("洗盤收回|回檔不破|低點墊高|回檔量縮|承接|整理換手|安靜吸籌", regex=True, na=False), 14, "洗盤/承接轉強")

    # 扣分：避免買到已經噴完
    add((ma20 > 0) & (close / ma20 > 1.12), -14, "離MA20偏遠")
    add((ma20 > 0) & (close / ma20 > 1.18), -24, "離MA20過熱")
    add(vr > 3.5, -10, "量能偏過熱")
    add(vr > 5.0, -20, "爆量過熱")
    add(continuation >= 8, -8, "延續分過高偏後段")
    add(ignition_power >= 8, -6, "點火已明顯偏追價")
    add(blob.str.contains("高檔出貨|假突破|長上影|誘多|出貨疑慮|籌碼風險|過熱|追高", regex=True, na=False), -25, "假突破/過熱風險")
    add(trigger > 45, -12, "trigger過高疑似已發動一段")
    add(first_trigger > 45, -10, "first trigger過高疑似非第一天")

    df["turn_event_score_v26664"] = score.round(2)
    df["turn_event_reason_v26664"] = reason.str.rstrip("｜")

    label = pd.Series(["NONE"] * len(df), index=df.index, dtype=object)
    label.loc[df["turn_event_score_v26664"] >= 55] = "TURN_FIRST"
    label.loc[(df["turn_event_score_v26664"] >= 42) & (df["turn_event_score_v26664"] < 55)] = "EARLY_TURN"
    label.loc[(df["turn_event_score_v26664"] >= 28) & (df["turn_event_score_v26664"] < 42)] = "WATCH_TURN"
    label.loc[df["turn_event_score_v26664"] <= 0] = "AVOID_CHASE"
    df["turn_event_label_v26664"] = label

    hint = pd.Series(["無明確轉折，暫不追。"] * len(df), index=df.index, dtype=object)
    hint.loc[label == "TURN_FIRST"] = "第一優先：轉折剛出現，隔日看不破MA5/MA10。"
    hint.loc[label == "EARLY_TURN"] = "早期轉強：可列入試單/優先觀察。"
    hint.loc[label == "WATCH_TURN"] = "觀察轉強：等待第二根確認。"
    hint.loc[label == "AVOID_CHASE"] = "避免追高：等回檔不破再看。"
    df["turn_priority_hint_v26664"] = hint

    base_col = next((c for c in [
        "first_trigger_score_v26663",
        "trigger_event_score_v26662",
        "state_adjusted_score_v26660",
        "chip_adjusted_score_v26658",
        "score",
        "final_score",
    ] if c in df.columns), None)

    if base_col:
        base = num(df, base_col)
        df["turn_adjusted_score_v26664"] = (score * 0.72 + base * 0.28).round(3)
    else:
        df["turn_adjusted_score_v26664"] = score.round(3)

    df = df.sort_values("turn_adjusted_score_v26664", ascending=False).copy()
    df["turn_first_rank_v26664"] = range(1, len(df) + 1)
    df["turn_patch_version"] = "v266.64"
    df["turn_updated_at"] = now_tw()
    return df

def patch_one(name):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)
        if status != "ok":
            report[str(p)] = {"status": status, "rows": 0}
            print(f"[v266.64] skip {p}: {status}")
            continue
        try:
            out = calc_turn_first(df)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)
            report[str(p)] = {"status": "updated", "rows": int(len(out))}
            print(f"[v266.64] updated {p}: {len(out)}")
        except Exception as e:
            report[str(p)] = {"status": "failed", "error": str(e), "rows": int(len(df))}
            print(f"[v266.64] failed {p}: {e}")
    return report

def main():
    report = {
        "version": "v266.64",
        "mode": "turn_first_rank_patch",
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

    if not summary_df.empty and "turn_event_label_v26664" in summary_df.columns:
        report["turn_labels"] = summary_df["turn_event_label_v26664"].fillna("").astype(str).value_counts().to_dict()
    else:
        report["turn_labels"] = {}

    for p in [ROOT / "turn_first_report.json", DATA_DIR / "turn_first_report.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
