# -*- coding: utf-8 -*-
"""
v266.58.1 chip_score_patch.py
籌碼權重補丁：防空檔、防缺欄位、防 pandas EmptyDataError。

不改：
- 原策略核心
- 原 action
- 持倉
- 停損
- structure_post_patch.py

只補：
- chip_score
- chip_signal
- chip_reason
- chip_adjusted_score_v26658
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import pandas as pd
from pandas.errors import EmptyDataError


ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = [
    "candidates.csv",
    "core_candidates.csv",
    "alpha_candidates.csv",
    "trade_plan.csv",
    "watchlist_monitor.csv",
    "final_action_plan.csv",
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


def num_series(df, col, default=0.0):
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce"
    ).fillna(default)


def text_series(df, col):
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[col].fillna("").astype(str)


def build_chip_score(df):
    df = df.copy()

    foreign_buy = num_series(df, "foreign_buy")
    investment_buy = num_series(df, "investment_buy")
    dealer_buy = num_series(df, "dealer_buy")
    margin_change = num_series(df, "margin_change")
    volume_ratio = num_series(df, "volume_ratio", 1.0)

    close = num_series(df, "close")
    if close.eq(0).all():
        close = num_series(df, "ref_price")

    ma20 = num_series(df, "ma20")
    ma60 = num_series(df, "ma60")

    chip_score = pd.Series([0.0] * len(df), index=df.index)
    reasons = pd.Series([""] * len(df), index=df.index, dtype=object)

    def add(mask, points, reason):
        nonlocal chip_score, reasons
        mask = mask.fillna(False)
        chip_score.loc[mask] += points
        reasons.loc[mask] = reasons.loc[mask] + reason + "｜"

    add(foreign_buy > 0, 5, "外資買超")
    add(investment_buy > 0, 6, "投信買超")
    add(dealer_buy > 0, 2, "自營買超")

    if "margin_change" in df.columns:
        add(margin_change > 8, -8, "融資暴增")

    add((volume_ratio > 1.5) & (close > ma20) & (ma20 > 0), 8, "爆量不跌")
    add((close > ma20) & (ma20 > ma60) & (ma60 > 0), 5, "均線結構")

    chip_concentration = num_series(df, "chip_concentration")
    if "chip_concentration" in df.columns:
        add(chip_concentration >= 70, 6, "籌碼高度集中")
        add((chip_concentration >= 55) & (chip_concentration < 70), 3, "籌碼偏集中")

    txt = (
        text_series(df, "chip_reason") + " " +
        text_series(df, "reason") + " " +
        text_series(df, "system_note")
    )
    add(txt.str.contains("三大法人買超|投信買超|外資買超", regex=True, na=False), 4, "法人偏多")
    add(txt.str.contains("主力拉升|主力吸籌|偏集中", regex=True, na=False), 5, "主力痕跡")
    add(txt.str.contains("低信心|融資暴增|高檔出貨|假突破", regex=True, na=False), -6, "籌碼風險")

    df["chip_score"] = chip_score.round(2)
    df["chip_reason"] = reasons.str.rstrip("｜")

    df["chip_signal"] = "一般"
    df.loc[df["chip_score"] >= 16, "chip_signal"] = "主力吸籌"
    df.loc[df["chip_score"] >= 24, "chip_signal"] = "主力發動"
    df.loc[df["chip_score"] <= -4, "chip_signal"] = "籌碼風險"

    base_col = None
    for c in ["adjusted_signal_score_v26657_9", "adjusted_signal_score_v26657_7", "final_score", "entry_score", "score"]:
        if c in df.columns:
            base_col = c
            break

    if base_col:
        base = num_series(df, base_col)
        df["chip_adjusted_score_v26658"] = (base * 0.82 + df["chip_score"] * 0.18).round(3)
    else:
        df["chip_adjusted_score_v26658"] = df["chip_score"].round(3)

    df["chip_patch_version"] = "v266.58.1"
    df["chip_updated_at"] = now_tw()
    return df


def patch_one(name):
    report = {}
    for base in [ROOT, DATA_DIR]:
        p = base / name
        df, status = read_csv_safe(p)
        key = str(p)

        if status != "ok":
            report[key] = {"status": status, "rows": 0}
            print(f"[chip v266.58.1] skip {p}: {status}")
            continue

        try:
            out = build_chip_score(df)
            write_csv(out, p)
            if base == ROOT:
                write_csv(out, DATA_DIR / name)

            report[key] = {"status": "updated", "rows": int(len(out))}
            print(f"[chip v266.58.1] updated {p}: {len(out)}")
        except Exception as e:
            report[key] = {"status": "failed", "error": str(e), "rows": int(len(df))}
            print(f"[chip v266.58.1] failed {p}: {e}")

    return report


def main():
    full_report = {
        "version": "v266.58.1",
        "mode": "chip_score_safe_patch",
        "changed_strategy_logic": False,
        "changed_action": False,
        "changed_position": False,
        "updated_at": now_tw(),
        "files": {},
    }

    for name in TARGETS:
        full_report["files"][name] = patch_one(name)

    for p in [ROOT / "chip_score_patch_report.json", DATA_DIR / "chip_score_patch_report.json"]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(full_report, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(json.dumps(full_report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
