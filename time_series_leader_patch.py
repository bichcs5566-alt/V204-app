# -*- coding: utf-8 -*-
"""
v266.61 time_series_leader_patch.py
時間序列主導排序補丁

不改原策略 / action / position / stoploss。
新增時間序列主力佈局排序分，降低已漲一波、過熱、爆量股權重。
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
    "trade_plan.csv",
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

def n(df, col, default=0.0):
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce"
    ).fillna(default)

def ss(df, col):
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[col].fillna("").astype(str)

def add_reason(current, mask, reason):
    out = current.copy()
    mask = mask.fillna(False)
    out.loc[mask] = out.loc[mask] + reason + "｜"
    return out

def calc_time_series_leader(df):
    df = df.copy()
    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].map(sid)

    structure = n(df, "structure_pre_score")
    continuation = n(df, "continuation_quality_score")
    chip = n(df, "chip_score")
    ignition = n(df, "ignition_power_score")
    state_score = n(df, "state_transition_score_v26660")
    state_adj = n(df, "state_adjusted_score_v26660")
    chip_adj = n(df, "chip_adjusted_score_v26658")

    base_score = n(df, "score")
    if base_score.eq(0).all():
        base_score = n(df, "final_score")
    if base_score.eq(0).all():
        base_score = n(df, "entry_score")

    close = n(df, "close")
    if close.eq(0).all():
        close = n(df, "ref_price")
    ma5 = n(df, "ma5")
    ma10 = n(df, "ma10")
    ma20 = n(df, "ma20")
    ma60 = n(df, "ma60")
    mom5 = n(df, "mom5")
    mom10 = n(df, "mom10")
    mom20 = n(df, "mom20")
    volume_ratio = n(df, "volume_ratio", 1.0)

    blob = (
        ss(df, "state_today_v26660") + " " +
        ss(df, "state_transition_label_v26660") + " " +
        ss(df, "ignition_upgrade_flag_v26660") + " " +
        ss(df, "chip_signal") + " " +
        ss(df, "chip_reason") + " " +
        ss(df, "structure_pre_reason") + " " +
        ss(df, "structure_pre_hint") + " " +
        ss(df, "continuation_quality_reason") + " " +
        ss(df, "ignition_power_reason") + " " +
        ss(df, "behavior_hint") + " " +
        ss(df, "reason") + " " +
        ss(df, "system_note")
    )

    reasons = pd.Series([""] * len(df), index=df.index, dtype=object)

    absorption = pd.Series([0.0] * len(df), index=df.index)
    mask = structure >= 6
    absorption.loc[mask] += 12
    reasons = add_reason(reasons, mask, "20/40/60結構佳")

    mask = chip >= 12
    absorption.loc[mask] += 12
    reasons = add_reason(reasons, mask, "籌碼偏多")

    mask = continuation >= 5
    absorption.loc[mask] += 8
    reasons = add_reason(reasons, mask, "續強品質佳")

    mask = blob.str.contains("吸籌|洗盤|整理換手|平台壓縮|安靜吸籌|放量吸收|爆量不跌", regex=True, na=False)
    absorption.loc[mask] += 10
    reasons = add_reason(reasons, mask, "主力佈局語意")

    upgrade = pd.Series([0.0] * len(df), index=df.index)
    mask = state_score >= 12
    upgrade.loc[mask] += 14
    reasons = add_reason(reasons, mask, "狀態升級強")

    mask = ss(df, "ignition_upgrade_flag_v26660").isin(["READY", "WATCH_UP", "YES"])
    upgrade.loc[mask] += 10
    reasons = add_reason(reasons, mask, "具升級旗標")

    mask = ss(df, "state_transition_label_v26660").str.contains("準IGNITION|升級觀察|IGNITION", regex=True, na=False)
    upgrade.loc[mask] += 8
    reasons = add_reason(reasons, mask, "升級標籤")

    pre_ignition = pd.Series([0.0] * len(df), index=df.index)

    close_ma20_ratio = pd.Series([0.0] * len(df), index=df.index)
    valid_ma20 = ma20 > 0
    close_ma20_ratio.loc[valid_ma20] = close.loc[valid_ma20] / ma20.loc[valid_ma20]

    close_ma60_ratio = pd.Series([0.0] * len(df), index=df.index)
    valid_ma60 = ma60 > 0
    close_ma60_ratio.loc[valid_ma60] = close.loc[valid_ma60] / ma60.loc[valid_ma60]

    mask = valid_ma20 & (close_ma20_ratio >= 0.98) & (close_ma20_ratio <= 1.12)
    pre_ignition.loc[mask] += 10
    reasons = add_reason(reasons, mask, "距MA20合理")

    mask = valid_ma60 & (close_ma60_ratio >= 0.95) & (close_ma60_ratio <= 1.20)
    pre_ignition.loc[mask] += 6
    reasons = add_reason(reasons, mask, "距MA60合理")

    mask = (ma5 >= ma10) & (ma10 >= ma20) & (ma20 > 0)
    pre_ignition.loc[mask] += 5
    reasons = add_reason(reasons, mask, "短均轉強")

    mask = (mom5 > 0) & (mom10 > 0) & (mom20 < 0.22)
    pre_ignition.loc[mask] += 6
    reasons = add_reason(reasons, mask, "動能初段未過熱")

    penalty = pd.Series([0.0] * len(df), index=df.index)
    mask = mom5 > 0.18
    penalty.loc[mask] += 8
    reasons = add_reason(reasons, mask, "5日漲幅過熱扣分")

    mask = mom10 > 0.30
    penalty.loc[mask] += 8
    reasons = add_reason(reasons, mask, "10日漲幅過熱扣分")

    mask = mom20 > 0.45
    penalty.loc[mask] += 12
    reasons = add_reason(reasons, mask, "20日漲幅過熱扣分")

    mask = valid_ma20 & (close_ma20_ratio > 1.18)
    penalty.loc[mask] += 12
    reasons = add_reason(reasons, mask, "乖離MA20過大扣分")

    mask = volume_ratio > 4.0
    penalty.loc[mask] += 6
    reasons = add_reason(reasons, mask, "爆量過大扣分")

    mask = blob.str.contains("出貨疑慮|高檔出貨|假突破|籌碼風險|長上影|過熱|降級風險", regex=True, na=False)
    penalty.loc[mask] += 15
    reasons = add_reason(reasons, mask, "風險語意扣分")

    breakout_assist = pd.Series([0.0] * len(df), index=df.index)
    mask = ignition >= 4
    breakout_assist.loc[mask] += 5
    reasons = add_reason(reasons, mask, "突破品質輔助")

    ts_score = (
        absorption * 0.35 +
        upgrade * 0.28 +
        pre_ignition * 0.25 +
        breakout_assist * 0.12 +
        chip_adj * 0.05 +
        state_adj * 0.05 +
        base_score * 0.03 -
        penalty * 0.42
    )

    df["absorption_score_v26661"] = absorption.round(2)
    df["upgrade_persistence_score_v26661"] = upgrade.round(2)
    df["pre_ignition_score_v26661"] = pre_ignition.round(2)
    df["overheated_penalty_v26661"] = penalty.round(2)
    df["ts_leader_score_v26661"] = ts_score.round(3)
    df["ts_leader_rank_v26661"] = pd.to_numeric(df["ts_leader_score_v26661"], errors="coerce").rank(ascending=False, method="min")
    df["ts_leader_reason_v26661"] = reasons.str.rstrip("｜")

    df["ts_leader_grade_v26661"] = "觀察"
    df.loc[df["ts_leader_score_v26661"] >= 8, "ts_leader_grade_v26661"] = "起漲前優先"
    df.loc[df["ts_leader_score_v26661"] >= 12, "ts_leader_grade_v26661"] = "主力佈局優先"
    df.loc[df["ts_leader_score_v26661"] >= 16, "ts_leader_grade_v26661"] = "高優先準點火"
    df.loc[df["overheated_penalty_v26661"] >= 15, "ts_leader_grade_v26661"] = "過熱降權"

    df["ts_leader_patch_version"] = "v266.61"
    df["ts_leader_updated_at"] = now_tw()

    df = df.sort_values(
        ["ts_leader_score_v26661", "absorption_score_v26661", "upgrade_persistence_score_v26661"],
        ascending=[False, False, False]
    )

    return df

def patch_file(name):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)
        if status != "ok":
            report[str(p)] = {"status": status, "rows": 0}
            print(f"[v266.61] skip {p}: {status}")
            continue
        try:
            out = calc_time_series_leader(df)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)
            report[str(p)] = {"status": "updated", "rows": int(len(out))}
            print(f"[v266.61] updated {p}: {len(out)}")
        except Exception as e:
            report[str(p)] = {"status": "failed", "rows": int(len(df)), "error": str(e)}
            print(f"[v266.61] failed {p}: {e}")
    return report

def main():
    report = {
        "version": "v266.61",
        "mode": "time_series_leader_sort_patch",
        "changed_strategy_logic": False,
        "changed_action": False,
        "changed_position": False,
        "changed_stoploss": False,
        "updated_at": now_tw(),
        "description": "時間序列主導排序：讓20/40/60結構、籌碼吸籌、狀態升級主導top ranking，降低已漲一波/過熱/爆量股權重。",
        "files": {},
    }

    for name in TARGETS:
        report["files"][name] = patch_file(name)

    df, status = read_csv_safe(ROOT / "final_action_plan.csv")
    if status == "ok":
        if "ts_leader_grade_v26661" in df.columns:
            report["grade_counts"] = df["ts_leader_grade_v26661"].fillna("").astype(str).value_counts().to_dict()
        if all(c in df.columns for c in ["stock_id", "ts_leader_score_v26661", "ts_leader_grade_v26661", "ts_leader_reason_v26661"]):
            report["top10"] = df[["stock_id", "ts_leader_score_v26661", "ts_leader_grade_v26661", "ts_leader_reason_v26661"]].head(10).to_dict(orient="records")

    for p in [ROOT / "time_series_leader_report.json", DATA_DIR / "time_series_leader_report.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
