"""
v1_stable_pipeline.py
v3.4 Data Pipeline 修復版

目的：
- full_summary.csv 永遠是股票明細，不再被 return/mdd/sharpe 覆蓋
- performance_summary.csv 才放績效
- trade_plan.csv 直接由 v3_core_candidates 建立
- READY / WATCH / STRUCTURE 也會顯示，不再整個消失
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from v3_0_core_engine import run_v3_core_engine

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INITIAL_CAPITAL = 1_000_000

WEIGHT = {
    "BREAKOUT_READY": 0.010,
    "LATENT_STRONG": 0.005,
    "STRUCTURE_READY": 0.003,
    "TEST": 0.003,
    "LATENT": 0.0,
    "WATCH": 0.0,
}


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


def write_csv_both(df, filename):
    df.to_csv(ROOT / filename, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / filename, index=False, encoding="utf-8-sig")


def ensure_stock_level_df(df, name):
    cols = set([str(c).lower() for c in df.columns])
    perf_cols = {"return", "mdd", "sharpe", "sharpe_daily"}
    if perf_cols.intersection(cols) and not {"stock_id", "close"}.issubset(cols):
        raise ValueError(f"{name} 被績效統計覆蓋，不是股票明細")
    if not {"stock_id", "close"}.issubset(cols):
        raise ValueError(f"{name} 缺少 stock_id / close")
    if len(df) < 50:
        raise ValueError(f"{name} 股票列數過少：{len(df)}，疑似資料被洗掉")


def build_full_summary(price_panel, scored):
    if scored is not None and len(scored) > 0 and {"stock_id", "close"}.issubset(scored.columns):
        fs = scored.copy()
        fs["source_type"] = "v3_scored_candidates"
    else:
        df = price_panel.copy()
        df.columns = [str(c).lower().strip() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            fs = df[df["date"] == df["date"].max()].copy()
        else:
            fs = df.copy()
        fs["source_type"] = "latest_price_panel_fallback"

    for col in [
        "stage", "main_force_score", "accumulation_score", "control_score",
        "structure_score", "test_move_score", "pre_breakout_score", "note"
    ]:
        if col not in fs.columns:
            fs[col] = ""
    return fs


def performance_summary():
    return pd.DataFrame([{
        "return": 0,
        "mdd": 0,
        "sharpe_daily": 0,
        "note": "performance separated from full_summary"
    }])


def action_from_stage(stage, score):
    stage = str(stage or "").upper()
    if stage == "BREAKOUT_READY":
        if score >= 80:
            return "BUY", "🟢 加碼", "發動前高分，分批加碼", 0.015
        return "BUY", "🟢 買進", "發動前，先小倉", WEIGHT[stage]
    if stage == "LATENT_STRONG":
        return "BUILD", "🟡 佈局", "強潛伏，先卡位", WEIGHT[stage]
    if stage == "STRUCTURE_READY":
        return "STRUCTURE", "🔵 結構", "結構待發，極小倉觀察", WEIGHT[stage]
    if stage == "TEST":
        return "TEST", "🟠 試單", "主力試單，輕倉測試", WEIGHT[stage]
    if stage == "LATENT":
        return "READY", "🟡 追蹤", "潛伏中，等試單", 0.0
    if stage == "WATCH":
        return "WATCH", "⚪ 觀察", "條件未完整", 0.0
    return "SKIP", "❌ 排除", "無主力行為", 0.0


def build_trade_plan(scored, signal_date):
    cols = [
        "signal_date","trade_date","action","action_label","action_sub","raw_action",
        "stock_id","price_tier","target_weight","ref_price","suggested_amount",
        "entry_score","stage","accumulation_score","control_score","structure_score",
        "test_move_score","pre_breakout_score","note","detail_note"
    ]
    if scored is None or scored.empty:
        return pd.DataFrame(columns=cols)

    rows = []
    trade_date = next_trade_date(signal_date)

    for _, r in scored.iterrows():
        score = float(r.get("main_force_score", 0) or 0)
        raw_action, label, sub, target_weight = action_from_stage(r.get("stage", "WATCH"), score)
        if raw_action == "SKIP":
            continue

        ref_price = float(r.get("close", 0) or 0)
        note = str(r.get("note", ""))

        if raw_action == "BUILD":
            note = f"強潛伏卡位｜{note}"
        elif raw_action == "STRUCTURE":
            note = f"結構待發｜{note}"
        elif raw_action == "TEST":
            note = f"試單輕倉｜{note}"
        elif raw_action == "BUY":
            note = f"發動前進場｜{note}"
        else:
            note = f"先追蹤不進場｜{note}"

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
            "structure_score": r.get("structure_score", ""),
            "test_move_score": r.get("test_move_score", ""),
            "pre_breakout_score": r.get("pre_breakout_score", ""),
            "note": note,
            "detail_note": (
                f"吸籌:{r.get('accumulation_score','')}｜"
                f"控盤:{r.get('control_score','')}｜"
                f"結構:{r.get('structure_score','')}｜"
                f"試單:{r.get('test_move_score','')}｜"
                f"前兆:{r.get('pre_breakout_score','')}"
            ),
        })
    return pd.DataFrame(rows, columns=cols)


def write_empty_support_files():
    files = {
        "current_positions.csv": ["stock_id", "shares", "avg_cost"],
        "position_monitor.csv": ["stock_id", "shares", "avg_cost", "note"],
        "watchlist_monitor.csv": ["stock_id", "note"],
    }
    for name, cols in files.items():
        p = DATA_DIR / name
        if not p.exists():
            pd.DataFrame(columns=cols).to_csv(p, index=False, encoding="utf-8-sig")


def main():
    errors = []
    price_panel_path = find_price_panel()
    price_panel = pd.read_csv(price_panel_path)
    price_panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8-sig")

    scored, debug = run_v3_core_engine(price_panel, top_n=40)

    if len(scored) and "date" in scored.columns:
        signal_date = pd.to_datetime(scored["date"].max())
    else:
        tmp = price_panel.copy()
        tmp.columns = [str(c).lower().strip() for c in tmp.columns]
        signal_date = pd.to_datetime(tmp["date"].max()) if "date" in tmp.columns else pd.Timestamp.today()

    full_summary = build_full_summary(price_panel, scored)
    try:
        ensure_stock_level_df(full_summary, "full_summary")
    except Exception as e:
        errors.append(str(e))

    perf = performance_summary()
    trade_plan = build_trade_plan(scored, signal_date)

    write_csv_both(scored, "v3_core_candidates.csv")
    write_csv_both(debug, "v3_core_debug.csv")
    write_csv_both(full_summary, "full_summary.csv")
    write_csv_both(perf, "performance_summary.csv")
    write_csv_both(trade_plan, "trade_plan.csv")
    write_csv_both(scored, "core_candidates.csv")
    write_csv_both(scored.head(25), "alpha_candidates.csv")
    write_empty_support_files()

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "now_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": str(pd.to_datetime(signal_date).date()),
        "trade_date": str(next_trade_date(signal_date).date()),
        "price_panel_latest_date": str(pd.to_datetime(signal_date).date()),
        "data_state": "warning" if errors else "fresh",
        "source": "v3_4_data_pipeline_fixed",
        "execution_rule": "T日盤後產生訊號，T+1交易",
        "trade_plan_count": int(len(trade_plan)),
        "full_summary_count": int(len(full_summary)),
        "v3_selected_count": int(len(scored)),
        "errors": errors,
        "position_writeback_state": "idle",
        "position_source": "current_positions.csv",
    }

    for p in [DATA_DIR / "meta.json", ROOT / "meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v3.4 data pipeline fixed completed")
    print("price_panel rows:", len(price_panel))
    print("scored rows:", len(scored))
    print("full_summary rows:", len(full_summary))
    print("trade_plan rows:", len(trade_plan))
    if errors:
        print("WARNINGS:", errors)


if __name__ == "__main__":
    main()
