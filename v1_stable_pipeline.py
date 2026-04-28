"""
v1_stable_pipeline.py
v3.4.1 接線修正版

修正目的：
1. full_summary.csv 永遠輸出「股票明細」，不再被 return/mdd/sharpe 覆蓋
2. trade_plan.csv 不再只有標題列
3. v3_core_candidates 若有內容，用它產生今日操作
4. v3_core_candidates 若空，先用 latest price_panel 產生 WATCH 觀察列，避免前端空白
5. 但 WATCH 權重 = 0，不會誤導成買進
6. performance_summary.csv 才放績效統計
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd

try:
    from v3_0_core_engine import run_v3_core_engine
except Exception as e:
    run_v3_core_engine = None
    IMPORT_ERROR = str(e)
else:
    IMPORT_ERROR = ""


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


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


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


def latest_stock_snapshot(price_panel):
    df = price_panel.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        latest_date = df["date"].max()
        df = df[df["date"] == latest_date].copy()
    else:
        latest_date = pd.Timestamp.today().normalize()
        df["date"] = latest_date

    if "stock_id" not in df.columns:
        df["stock_id"] = ""
    if "close" not in df.columns:
        df["close"] = 0

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["source_type"] = "latest_price_panel_stock_snapshot"

    # 排除明顯非個股商品，保留一般四碼個股
    df = df[df["stock_id"].astype(str).str.match(r"^[1-9][0-9]{3}$", na=False)].copy()

    for col in [
        "stage", "main_force_score", "accumulation_score", "control_score",
        "structure_score", "test_move_score", "pre_breakout_score", "note"
    ]:
        if col not in df.columns:
            df[col] = ""

    return df, latest_date


def merge_scores_into_full_summary(full_summary, scored):
    fs = full_summary.copy()
    if scored is None or scored.empty or "stock_id" not in scored.columns:
        return fs

    score_cols = [
        "stock_id", "stage", "main_force_score", "accumulation_score",
        "control_score", "structure_score", "test_move_score",
        "pre_breakout_score", "note"
    ]
    exist_cols = [c for c in score_cols if c in scored.columns]
    s = scored[exist_cols].copy()
    s["stock_id"] = s["stock_id"].apply(normalize_stock_id)

    # 避免欄位重複
    drop_cols = [c for c in exist_cols if c != "stock_id" and c in fs.columns]
    fs = fs.drop(columns=drop_cols, errors="ignore")
    fs = fs.merge(s, on="stock_id", how="left", suffixes=("", "_score"))

    for col in score_cols:
        if col != "stock_id" and col not in fs.columns:
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
        return "BUY", "🟢 買進", "發動前，先小倉", 0.010

    if stage == "LATENT_STRONG":
        return "BUILD", "🟡 佈局", "強潛伏，先卡位", 0.005

    if stage == "STRUCTURE_READY":
        return "STRUCTURE", "🔵 結構", "結構待發，極小倉觀察", 0.003

    if stage == "TEST":
        return "TEST", "🟠 試單", "主力試單，輕倉測試", 0.003

    if stage == "LATENT":
        return "READY", "🟡 追蹤", "潛伏中，等試單", 0.0

    if stage == "WATCH":
        return "WATCH", "⚪ 觀察", "條件未完整", 0.0

    return "WATCH", "⚪ 觀察", "資料接線觀察", 0.0


def build_trade_plan(scored, full_summary, signal_date):
    cols = [
        "signal_date", "trade_date", "action", "action_label", "action_sub",
        "raw_action", "stock_id", "price_tier", "target_weight", "ref_price",
        "suggested_amount", "entry_score", "stage",
        "accumulation_score", "control_score", "structure_score",
        "test_move_score", "pre_breakout_score", "note", "detail_note"
    ]

    trade_date = next_trade_date(signal_date)

    # 優先使用 v3 scored
    if scored is not None and not scored.empty:
        src = scored.copy()
        src["trade_source"] = "v3_core_candidates"
    else:
        # 接線防呆：若策略候選空，至少讓前端看到最新股票快照中的觀察名單
        src = full_summary.copy().head(25)
        src["stage"] = "WATCH"
        src["main_force_score"] = 0
        src["note"] = "策略候選空，資料接線觀察列，不作為買進依據"
        src["trade_source"] = "price_panel_watch_fallback"

    rows = []
    for _, r in src.iterrows():
        score = float(r.get("main_force_score", 0) or 0)
        raw_action, label, sub, target_weight = action_from_stage(r.get("stage", "WATCH"), score)

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
                f"來源:{r.get('trade_source','')}｜"
                f"吸籌:{r.get('accumulation_score','')}｜"
                f"控盤:{r.get('control_score','')}｜"
                f"結構:{r.get('structure_score','')}｜"
                f"試單:{r.get('test_move_score','')}｜"
                f"前兆:{r.get('pre_breakout_score','')}"
            )
        })

    return pd.DataFrame(rows, columns=cols)


def write_empty_support_files():
    support_files = {
        "current_positions.csv": ["stock_id", "shares", "avg_cost"],
        "position_monitor.csv": ["stock_id", "shares", "avg_cost", "note"],
        "watchlist_monitor.csv": ["stock_id", "note"],
    }
    for name, cols in support_files.items():
        p = DATA_DIR / name
        if not p.exists():
            pd.DataFrame(columns=cols).to_csv(p, index=False, encoding="utf-8-sig")


def main():
    errors = []

    price_panel_path = find_price_panel()
    price_panel = pd.read_csv(price_panel_path)
    price_panel.to_csv(DATA_DIR / "price_panel_daily.csv", index=False, encoding="utf-8-sig")

    full_summary_base, latest_date = latest_stock_snapshot(price_panel)

    if run_v3_core_engine is None:
        errors.append(f"v3_0_core_engine import failed: {IMPORT_ERROR}")
        scored = pd.DataFrame()
        debug = pd.DataFrame([{
            "state": "engine_import_failed",
            "error": IMPORT_ERROR,
            "selected_count": 0
        }])
    else:
        try:
            scored, debug = run_v3_core_engine(price_panel, top_n=40)
        except Exception as e:
            errors.append(f"run_v3_core_engine failed: {e}")
            scored = pd.DataFrame()
            debug = pd.DataFrame([{
                "state": "engine_failed",
                "error": str(e),
                "selected_count": 0
            }])

    full_summary = merge_scores_into_full_summary(full_summary_base, scored)
    perf = performance_summary()
    trade_plan = build_trade_plan(scored, full_summary, latest_date)

    write_csv_both(full_summary, "full_summary.csv")
    write_csv_both(perf, "performance_summary.csv")
    write_csv_both(trade_plan, "trade_plan.csv")
    write_csv_both(scored, "v3_core_candidates.csv")
    write_csv_both(debug, "v3_core_debug.csv")
    write_csv_both(scored if not scored.empty else full_summary.head(25), "core_candidates.csv")
    write_csv_both(scored.head(25) if not scored.empty else full_summary.head(25), "alpha_candidates.csv")

    write_empty_support_files()

    meta = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "now_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "signal_date": str(pd.to_datetime(latest_date).date()),
        "trade_date": str(next_trade_date(latest_date).date()),
        "price_panel_latest_date": str(pd.to_datetime(latest_date).date()),
        "data_state": "warning" if errors else "fresh",
        "source": "v3_4_1_connection_fixed",
        "execution_rule": "T日盤後產生訊號，T+1交易",
        "price_panel_rows": int(len(price_panel)),
        "full_summary_count": int(len(full_summary)),
        "v3_selected_count": int(len(scored)),
        "trade_plan_count": int(len(trade_plan)),
        "errors": errors,
        "position_writeback_state": "idle",
        "position_source": "current_positions.csv"
    }

    for p in [DATA_DIR / "meta.json", ROOT / "meta.json"]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    print("v3.4.1 connection fixed completed")
    print("price_panel rows:", len(price_panel))
    print("full_summary rows:", len(full_summary))
    print("scored rows:", len(scored))
    print("trade_plan rows:", len(trade_plan))
    if errors:
        print("errors:", errors)


if __name__ == "__main__":
    main()
