# -*- coding: utf-8 -*-
"""
v266.60.2 state_transition_patch.py
狀態連續追蹤引擎 + 正確 report 統計欄位修正版

修正重點：
- report 不再抓 strategy_layer
- 改抓 state_today_v26660
- 同時統計 state_transition_label_v26660
- 同時統計 ignition_upgrade_flag_v26660

不改：
- 原策略
- action
- position
- stoploss
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
    "trade_plan.csv",
    "watchlist_monitor.csv",
    "final_action_plan.csv",
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
    "pre_move_candidates.csv",
    "top_opportunities.csv",
]

STATE_MEMORY = ROOT / "state_transition_memory.csv"
STATE_MEMORY_DATA = DATA_DIR / "state_transition_memory.csv"

STATE_RANK = {
    "出貨疑慮": -2,
    "觀察": 0,
    "吸籌中": 1,
    "洗盤吸籌": 2,
    "試盤中": 3,
    "突破中": 4,
    "發動中": 5,
    "延續中": 6,
}

def now_tw():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

def today_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

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

def nval(row, keys, default=0.0):
    for k in keys:
        try:
            if k in row and str(row.get(k, "")).strip() not in ["", "nan", "None", "--"]:
                return float(str(row.get(k)).replace(",", "").replace("%", ""))
        except Exception:
            pass
    return default

def infer_state(row):
    ev = str(row.get("evolution_state", "")).strip()
    if ev and ev.lower() not in ["nan", "none", "null", "--", "-"]:
        return ev

    blob = " ".join(str(row.get(c, "")) for c in [
        "behavior_hint", "behavior_action_hint", "reason", "system_note",
        "chip_signal", "chip_reason", "structure_pre_hint",
        "continuation_quality_hint", "ignition_power_hint",
        "kbar_type", "k_structure", "kline_structure",
        "action", "final_action", "status"
    ])

    score = nval(row, ["chip_adjusted_score_v26658", "adjusted_signal_score_v26657_9", "score", "final_score", "entry_score"])
    chip = nval(row, ["chip_score"])
    ip = nval(row, ["ignition_power_score"])
    st = nval(row, ["structure_pre_score"])
    cq = nval(row, ["continuation_quality_score"])

    if any(x in blob for x in ["出貨", "高檔出貨", "假突破", "籌碼風險", "跌破", "停損"]):
        return "出貨疑慮"
    if any(x in blob for x in ["主力拉升", "趨勢延續", "延續中"]) or (score >= 85 and chip >= 16 and cq >= 5):
        return "延續中"
    if any(x in blob for x in ["主力發動", "準IGNITION", "TEST有升級IGNITION條件"]) or (score >= 75 and chip >= 14 and ip >= 4):
        return "發動中"
    if any(x in blob for x in ["突破確認", "突破中", "突破品質"]) or ip >= 4:
        return "突破中"
    if any(x in blob for x in ["試盤中", "放量吸收", "量能放大"]) or (chip >= 10 and cq >= 3):
        return "試盤中"
    if any(x in blob for x in ["洗盤吸籌", "整理換手", "洗盤收回", "安靜吸籌"]) or (st >= 5 and chip >= 6):
        return "洗盤吸籌"
    if any(x in blob for x in ["主力吸籌", "吸籌中", "法人偏多", "偏集中"]) or chip >= 6:
        return "吸籌中"
    return "觀察"

def load_memory():
    for p in [STATE_MEMORY, STATE_MEMORY_DATA]:
        df, status = read_csv_safe(p)
        if status == "ok":
            if "stock_id" in df.columns:
                df["stock_id"] = df["stock_id"].map(sid)
            return df
    return pd.DataFrame(columns=[
        "stock_id", "last_date", "prev_state", "prev_rank",
        "streak_up_days", "streak_state_days", "last_seen_at"
    ])

def save_memory(mem):
    write_csv(mem, STATE_MEMORY)
    write_csv(mem, STATE_MEMORY_DATA)

def apply_transition(df, mem):
    df = df.copy()
    if "stock_id" not in df.columns:
        return df, mem

    df["stock_id"] = df["stock_id"].map(sid)
    mem = mem.copy()
    if "stock_id" not in mem.columns:
        mem["stock_id"] = ""
    mem["stock_id"] = mem["stock_id"].map(sid)

    mem_map = {sid(r.get("stock_id", "")): r.to_dict() for _, r in mem.iterrows() if sid(r.get("stock_id", ""))}

    today = today_str()
    now = now_tw()
    scores, labels, reasons, flags, today_states, today_ranks = [], [], [], [], [], []
    new_rows = []

    for _, row in df.iterrows():
        code = sid(row.get("stock_id", ""))
        state = infer_state(row)
        rank = int(STATE_RANK.get(state, 0))

        old = mem_map.get(code, {})
        prev_state = str(old.get("prev_state", ""))
        try:
            prev_rank = int(float(old.get("prev_rank", 0)))
        except Exception:
            prev_rank = 0
        try:
            streak_up = int(float(old.get("streak_up_days", 0)))
        except Exception:
            streak_up = 0
        try:
            streak_state = int(float(old.get("streak_state_days", 0)))
        except Exception:
            streak_state = 0

        delta = rank - prev_rank
        score = 0
        reason = [f"前狀態:{prev_state}→今日:{state}" if prev_state else f"首次追蹤:{state}"]

        if delta >= 2:
            score += 8
            streak_up += 1
            reason.append("狀態跳升")
        elif delta == 1:
            score += 5
            streak_up += 1
            reason.append("狀態升級")
        elif delta == 0 and rank >= 3:
            score += 3
            reason.append("強狀態維持")
        elif delta < 0:
            score -= 5
            streak_up = 0
            reason.append("狀態降級")

        streak_state = streak_state + 1 if state == prev_state else 1

        if streak_up >= 2:
            score += 5
            reason.append("連續升級")
        if streak_state >= 3 and rank >= 3:
            score += 4
            reason.append("強狀態連續維持")

        if state in ["發動中", "延續中"]:
            score += 8
        elif state == "突破中":
            score += 5
        elif state in ["試盤中", "洗盤吸籌"]:
            score += 3
        elif state == "出貨疑慮":
            score -= 10

        if score >= 18 or (state == "延續中" and streak_state >= 2):
            label, flag = "IGNITION", "YES"
            reason.append("連續狀態達IGNITION")
        elif score >= 12 or state == "發動中":
            label, flag = "準IGNITION", "READY"
            reason.append("具備點火條件")
        elif score >= 7:
            label, flag = "升級觀察", "WATCH_UP"
            reason.append("狀態轉強")
        elif score <= -5:
            label, flag = "降級風險", "RISK"
        else:
            label, flag = "持續觀察", "NO"

        today_states.append(state)
        today_ranks.append(rank)
        scores.append(score)
        labels.append(label)
        reasons.append("｜".join(reason))
        flags.append(flag)

        new_rows.append({
            "stock_id": code,
            "last_date": today,
            "prev_state": state,
            "prev_rank": rank,
            "streak_up_days": streak_up,
            "streak_state_days": streak_state,
            "last_seen_at": now,
        })

    df["state_today_v26660"] = today_states
    df["state_rank_today_v26660"] = today_ranks
    df["state_transition_score_v26660"] = scores
    df["state_transition_label_v26660"] = labels
    df["state_transition_reason_v26660"] = reasons
    df["ignition_upgrade_flag_v26660"] = flags
    df["state_patch_version"] = "v266.60.2"

    base_col = next((c for c in ["chip_adjusted_score_v26658", "adjusted_signal_score_v26657_9", "score", "final_score", "entry_score"] if c in df.columns), None)
    if base_col:
        base = pd.to_numeric(df[base_col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
        df["state_adjusted_score_v26660"] = (base + df["state_transition_score_v26660"] * 0.35).round(3)
    else:
        df["state_adjusted_score_v26660"] = df["state_transition_score_v26660"]

    update_df = pd.DataFrame(new_rows)
    old_keep = mem[~mem["stock_id"].astype(str).isin(update_df["stock_id"].astype(str))] if not mem.empty and "stock_id" in mem.columns else pd.DataFrame()
    new_mem = pd.concat([old_keep, update_df], ignore_index=True)

    return df, new_mem

def patch_file(name, mem):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)
        if status != "ok":
            report[str(p)] = {"status": status, "rows": 0}
            print(f"[v266.60.2] skip {p}: {status}")
            continue
        try:
            out, mem = apply_transition(df, mem)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)
            report[str(p)] = {"status": "updated", "rows": len(out)}
            print(f"[v266.60.2] updated {p}: {len(out)}")
        except Exception as e:
            report[str(p)] = {"status": "failed", "error": str(e), "rows": len(df)}
            print(f"[v266.60.2] failed {p}: {e}")
    return report, mem

def safe_counts(df, col):
    if df is None or df.empty or col not in df.columns:
        return {}
    return df[col].fillna("").astype(str).replace({"nan": "", "None": ""}).value_counts().to_dict()

def build_export_memory_from_final():
    """
    用 final_action_plan.csv 重新輸出 state_transition_memory.csv。
    修正舊版抓 strategy_layer 導致 states 全空的問題。
    """
    df, status = read_csv_safe(ROOT / "final_action_plan.csv")
    if status != "ok":
        df, status = read_csv_safe(DATA_DIR / "final_action_plan.csv")
    if status != "ok":
        return pd.DataFrame()

    df = df.copy()
    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].map(sid)

    keep_cols = [
        "stock_id", "stock_name", "action", "final_action",
        "state_today_v26660", "state_rank_today_v26660",
        "state_transition_score_v26660",
        "state_transition_label_v26660",
        "state_transition_reason_v26660",
        "ignition_upgrade_flag_v26660",
        "chip_score", "chip_signal", "chip_reason",
        "state_adjusted_score_v26660",
    ]

    out = pd.DataFrame()
    for c in keep_cols:
        out[c] = df[c] if c in df.columns else ""

    out["memory_updated_at"] = now_tw()
    return out

def main():
    mem = load_memory()
    report = {
        "version": "v266.60.2",
        "mode": "state_transition_memory_patch_fixed_report",
        "changed_strategy_logic": False,
        "changed_action": False,
        "changed_position": False,
        "updated_at": now_tw(),
        "files": {},
    }

    for name in TARGETS:
        r, mem = patch_file(name, mem)
        report["files"][name] = r

    save_memory(mem)

    # 另外輸出可讀記憶檔，讓 Code / dashboard 看得到正確欄位
    export_mem = build_export_memory_from_final()
    if not export_mem.empty:
        write_csv(export_mem, STATE_MEMORY)
        write_csv(export_mem, STATE_MEMORY_DATA)
        report["memory_rows"] = int(len(export_mem))
        report["states"] = safe_counts(export_mem, "state_today_v26660")
        report["transition_labels"] = safe_counts(export_mem, "state_transition_label_v26660")
        report["ignition_flags"] = safe_counts(export_mem, "ignition_upgrade_flag_v26660")
    else:
        report["memory_rows"] = int(len(mem))
        report["states"] = safe_counts(mem, "prev_state")
        report["transition_labels"] = {}
        report["ignition_flags"] = {}

    for p in [ROOT / "state_transition_report.json", DATA_DIR / "state_transition_report.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
