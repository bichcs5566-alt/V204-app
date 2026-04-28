"""
v1_stable_pipeline_v3_1_tradeable.py

v3.1 主力同步可交易版

重點：
- 不改 v3.0 主力行為核心引擎
- 只修「交易決策層」
- LATENT_STRONG / TEST / BREAKOUT_READY 不再全部只觀察
- 用小倉、試單、加碼三段式處理

需要同目錄：
- v3_0_core_engine.py
"""

import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd

from v3_0_core_engine import run_v3_core_engine


ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CAPITAL = 1_000_000

# v3.1 可交易倉位設定
WEIGHT_LATENT_STRONG = 0.005    # 強潛伏：先卡位 0.5%
WEIGHT_TEST = 0.003             # 試單：試單 0.3%
WEIGHT_BREAKOUT_READY = 0.010   # 發動前：小倉 1.0%
WEIGHT_BREAKOUT_HIGH = 0.015    # 高分發動前：1.5%


def find_price_panel():
    candidates = [
        ROOT / "price_panel_daily.csv",
        ROOT / "data" / "price_panel_daily.csv",
        DATA_DIR / "price_panel_daily.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("找不到 price_panel_daily.csv")


def next_trade_date(signal_date):
    d = pd.to_datetime(signal_date) + pd.Timedelta(days=1)
    # 簡易週末修正：六日往後推到週一
    if d.weekday() == 5:
        d += pd.Timedelta(days=2)
    elif d.weekday() == 6:
        d += pd.Timedelta(days=1)
    return d


def price_tier(price):
    try:
        p = float(price)
    except Exception:
        return ""
    if p < 50:
        return "50以下"
    if p < 100:
        return "50-100"
    if p < 300:
        return "100-300"
    if p < 500:
        return "300-500"
    if p < 1000:
        return "500-1000"
    return "1000以上"


def action_from_stage(stage, score):
    """
    v3.1 可交易決策層

    注意：
    - 不是所有入選都買
    - 只有主力行為達標才給倉位
    - WATCH 仍只觀察
    """
    stage = str(stage or "").upper()

    if stage == "BREAKOUT_READY":
        if score >= 80:
            return "BUY", "🟢 加碼", "發動前高分，分批加碼", WEIGHT_BREAKOUT_HIGH
        return "BUY", "🟢 買進", "發動前，先小倉", WEIGHT_BREAKOUT_READY

    if stage == "LATENT_STRONG":
        return "BUILD", "🟡 佈局", "強潛伏，先卡位", WEIGHT_LATENT_STRONG

    if stage == "TEST":
        return "TEST", "🟠 試單", "主力試單，輕倉測試", WEIGHT_TEST

    if stage == "LATENT":
        return "READY", "🟡 追蹤", "潛伏中，等試單", 0.0

    if stage == "WATCH":
        return "WATCH", "⚪ 觀察", "條件未完整", 0.0

    return "SKIP", "❌ 排除", "無主力行為", 0.0


def build_trade_plan(scored, signal_date):
    rows = []
    trade_date = next_trade_date(signal_date)

    for _, r in scored.iterrows():
        score = float(r.get("main_force_score", 0))
        raw_action, label, sub, target_weight = action_from_stage(r.get("stage", "WATCH"), score)

        # 嚴格版：SKIP 不輸出
        if raw_action == "SKIP":
            continue

        ref_price = float(r.get("close", 0)) if pd.notna(r.get("close", None)) else 0

        note = r.get("note", "")
        if raw_action == "BUILD":
            note = f"強潛伏卡位｜{note}"
        elif raw_action == "TEST":
            note = f"試單輕倉｜{note}"
        elif raw_action == "BUY":
            note = f"發動前進場｜{note}"

        rows.append({
            "signal_date": str(pd.to_datetime(signal_date).date()),
            "trade_date": str(trade_date.date()),
            "action": raw_action,
            "action_label": label,
            "action_sub": sub,
            "raw_action": raw_action,
            "stock_id": str(r.get("stock_id")),
            "price_tier": price_tier(ref_price),
            "target_weight": round(target_weight, 4),
            "ref_price": round(ref_price, 4),
            "suggested_amount": round(INITIAL_CAPITAL * target_weight, 2),
            "entry_score": round(score, 2),
            "stage": r.get("stage", ""),
            "accumulation_score": r.get("accumulation_score", ""),
            "control_score": r.get("control_score", ""),
            "test_move_score": r.get("test_move_score", ""),
            "pre_breakout_score": r.get("pre_breakout_score", ""),
            "note": note,
            "detail_note": (
                f"吸籌:{r.get('accumulation_score','')}｜"
                f"控盤:{r.get('control_score','')}｜"
                f"試單:{r.get('test_move_score','')}｜"
                f"前兆:{r.get('pre_breakout_score','')}｜"
                f"{r.get('accumulation_reason','')} / {r.get('control_reason','')} / "
                f"{r.get('test_move_reason','')} / {r.get('pre_breakout_reason','')}"
            )
        })

    cols = [
        "signal_date", "trade_date", "action", "action_label", "action_sub",
        "raw_action", "stock_id", "price_tier", "target_weight", "ref_price",
        "suggested_amount", "entry_score", "stage",
        "accumulation_score", "control_score", "test_move_score", "pre_breakout_score",
        "note", "detail_note"
    ]
    return pd.DataFrame(rows, columns=cols)


def write_empty_support_files():
    # 避免前端讀不到檔案
    support_files = {
        "current_positions.csv": ["stock_id", "shares", "cost"],
        "position_monitor.csv": ["stock_id", "shares", "cost", "note"],
        "watchlist_monitor.csv": ["stock_id", "note"],
        "full_summary.csv": ["metric", "value"],
        "price_panel_daily.csv": None,
    }

    for name, cols in support_files.items():
        p = DATA_DIR / name
        if not p.exists() and cols is not None:
            pd.DataFrame(columns=cols).to_csv(p, index=False, encoding="utf-8-sig")


def main():
    price_panel_path = find_price_panel()
    df = pd.read_csv(price_panel_path)

    scored, debug = run_v3_core_engine(df, top_n=40)

    if len(scored):
        signal_date = pd.to_datetime(scored["date"].max())
    else:
        signal_date = pd.to_datetime(df["date"].max()) if "date" in df.columns else pd.Timestamp.today()

    trade_plan = build_trade_plan(scored, signal_date)

    # root outputs
    scored.to_csv(ROOT / "v3_core_candidates.csv", index=False, encoding="utf-8-sig")
    debug.to_csv(ROOT / "v3_core_debug.csv", index=False, encoding="utf-8-sig")
    trade_plan.to_csv(ROOT / "trade_plan.csv", index=False, encoding="utf-8-sig")

    # dashboard outputs
    scored.to_csv(DATA_DIR / "v3_core_candidates.csv", index=False, encoding="utf-8-sig")
    debug.to_csv(DATA_DIR / "v3_core_debug.csv", index=False, encoding="utf-8-sig")
    trade_plan.to_csv(DATA_DIR / "trade_plan.csv", index=False, encoding="utf-8-sig")

    write_empty_support_files()

    # 若 price_panel 在 root，順手同步一份到 dashboard
    try:
        src_panel = Path(price_panel_path)
        if src_panel.exists():
            pd.read_csv(src_panel).to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "now_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "price_panel_latest_date": str(pd.to_datetime(df["date"].max()).date()) if "date" in df.columns else "",
        "data_state": "fresh",
        "source": "v3_2_main_force_sensing",
        "execution_rule": "T日盤後產生訊號，T+1交易",
        "trade_plan_count": int(len(trade_plan)),
        "v3_selected_count": int(len(scored)),
        "breakout_ready_count": int((scored["stage"] == "BREAKOUT_READY").sum()) if len(scored) else 0,
        "latent_strong_count": int((scored["stage"] == "LATENT_STRONG").sum()) if len(scored) else 0,
        "test_count": int((scored["stage"] == "TEST").sum()) if len(scored) else 0,
        "position_writeback_state": "idle",
        "position_source": "current_positions.csv"
    }

    with open(DATA_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v3.2 sensing tradeable completed")
    print(debug.to_string(index=False))
    if len(trade_plan):
        print(trade_plan[["action_label", "stock_id", "entry_score", "target_weight", "suggested_amount", "note"]].head(40).to_string(index=False))
    else:
        print("trade_plan is empty: no tradeable stage today")


if __name__ == "__main__":
    main()
