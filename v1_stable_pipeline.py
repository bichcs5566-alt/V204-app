"""
v1_stable_pipeline_v3_0_core_strict.py

v3.0 嚴格版測試用主程式。
用途：
- 不取代你正式交易系統
- 先輸出 v3_core_candidates.csv / trade_plan.csv / dashboard data
- 讓你檢查：南亞科、旺宏、力特這種「收斂潛伏」是否被正確抓出來

需要同目錄：
- v3_0_core_engine.py
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from v3_0_core_engine import run_v3_core_engine


ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CAPITAL = 1_000_000


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


def action_from_stage(stage):
    if stage == "BREAKOUT_READY":
        return "BUY", "🟢 發動前", "可小倉分批"
    if stage == "LATENT_STRONG":
        return "READY", "🟡 強潛伏", "優先追蹤"
    if stage == "LATENT":
        return "READY", "🟡 潛伏", "等待試單"
    if stage == "TEST":
        return "TEST", "🟠 試單", "隔日確認"
    return "WATCH", "⚪ 觀察", "僅追蹤"


def build_trade_plan(scored, signal_date):
    rows = []
    trade_date = pd.to_datetime(signal_date) + pd.Timedelta(days=1)

    for _, r in scored.iterrows():
        raw_action, label, sub = action_from_stage(r.get("stage", "WATCH"))
        score = float(r.get("main_force_score", 0))

        # 嚴格版：只有發動前才給小倉；其他全部 0
        if raw_action == "BUY":
            target_weight = 0.01 if score < 80 else 0.015
        else:
            target_weight = 0.0

        ref_price = float(r.get("close", 0)) if pd.notna(r.get("close", None)) else 0
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
            "note": r.get("note", ""),
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
        "suggested_amount", "entry_score", "stage", "note", "detail_note"
    ]
    return pd.DataFrame(rows, columns=cols)


def main():
    price_panel_path = find_price_panel()
    df = pd.read_csv(price_panel_path)

    scored, debug = run_v3_core_engine(df, top_n=30)

    if len(scored):
        signal_date = pd.to_datetime(scored["date"].max())
    else:
        # fallback 只為 meta 顯示，不補交易名單
        signal_date = pd.to_datetime(df["date"].max()) if "date" in df.columns else pd.Timestamp.today()

    trade_plan = build_trade_plan(scored, signal_date)

    # 輸出 root
    scored.to_csv(ROOT / "v3_core_candidates.csv", index=False, encoding="utf-8-sig")
    debug.to_csv(ROOT / "v3_core_debug.csv", index=False, encoding="utf-8-sig")
    trade_plan.to_csv(ROOT / "trade_plan.csv", index=False, encoding="utf-8-sig")

    # 輸出 dashboard data
    scored.to_csv(DATA_DIR / "v3_core_candidates.csv", index=False, encoding="utf-8-sig")
    debug.to_csv(DATA_DIR / "v3_core_debug.csv", index=False, encoding="utf-8-sig")
    trade_plan.to_csv(DATA_DIR / "trade_plan.csv", index=False, encoding="utf-8-sig")

    # 基本空檔補齊，避免前端炸
    for name in ["current_positions.csv", "position_monitor.csv", "watchlist_monitor.csv"]:
        p = DATA_DIR / name
        if not p.exists():
            pd.DataFrame().to_csv(p, index=False, encoding="utf-8-sig")

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "now_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str((pd.to_datetime(signal_date) + pd.Timedelta(days=1)).date()),
        "price_panel_latest_date": str(pd.to_datetime(df["date"].max()).date()) if "date" in df.columns else "",
        "data_state": "fresh",
        "source": "v3_0_core_engine_strict",
        "execution_rule": "T日盤後產生訊號，T+1交易",
        "trade_plan_count": int(len(trade_plan)),
        "v3_selected_count": int(len(scored)),
        "position_writeback_state": "idle",
        "position_source": "current_positions.csv"
    }
    with open(DATA_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v3.0 core strict completed")
    print(debug.to_string(index=False))
    print(trade_plan[["action_label", "stock_id", "entry_score", "note"]].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
