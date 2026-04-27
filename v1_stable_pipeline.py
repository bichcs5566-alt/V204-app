import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

try:
    from decision_modules import add_decision_features, entry_score, position_stage
    DECISION_MODULES_AVAILABLE = True
except Exception:
    DECISION_MODULES_AVAILABLE = False
    def add_decision_features(df): return df
    def entry_score(row, market_row=None):
        return {"entry_score":0,"entry_action":"SKIP","market_score":0,"trend_score":0,"momentum_score":0,"chip_score":0,"risk_penalty":0,"entry_reason":"decision_modules.py 未載入"}
    def position_stage(row): return "未載入"

# =========================================================
# v1_stable_pipeline.py
# v1.4 signal/trade date + history version
#
# 核心修正：
# 1. current_positions.csv 是唯一持倉來源。
# 2. position_monitor.csv 只能由 current_positions.csv 產生。
# 3. 策略可以產生 trade_plan.csv，但不能自動復活持倉。
# 4. dashboard/data/current_positions.csv 只做同步鏡像，不再優先讀取。
#
# 可直接覆蓋 repo 根目錄的 v1_stable_pipeline.py
# =========================================================

CORE_WEIGHT = 0.75
ALPHA_WEIGHT = 0.25
CORE_TOP_N = 25
ALPHA_TOP_N = 6
MIN_CORE_FILL = 5
MIN_ALPHA_FILL = 2
MAX_POSITION_WEIGHT = 0.10
BUY_BAND = 0.015
REDUCE_BAND = -0.015
SLIPPAGE = 0.001
INITIAL_CAPITAL = 1000000
STOP_LOSS_2 = -0.10
DEFAULT_SHARES = 1000

PRICE_PANEL_FILE = "price_panel_daily.csv"
MERGE_SCRIPT = "merge_chunked_price_panel.py"

DASHBOARD_DIR = Path("mobile_dashboard_v1")
DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"

POSITIONS_FILE = "current_positions.csv"
WATCHLIST_FILE = "watchlist.csv"

PREV_TRADE_PLAN_FILE = "prev_trade_plan.csv"
TRADE_PLAN_HISTORY_DIR = Path("trade_plan_history")
DASHBOARD_TRADE_PLAN_HISTORY_DIR = DASHBOARD_DATA_DIR / "trade_plan_history"


# ---------- basic utilities ----------

def now_taipei():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)


def read_csv_auto(path):
    path = Path(path)
    for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def write_csv(path, df):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def read_csv_optional(path, default_columns=None):
    path = Path(path)
    default_columns = default_columns or []
    if not path.exists():
        return pd.DataFrame(columns=default_columns)
    try:
        return read_csv_auto(path)
    except Exception:
        return pd.DataFrame(columns=default_columns)


def load_previous_trade_plan():
    """
    交易節奏定義：
    T日盤後產生 trade_plan.csv，T+1 交易。
    pipeline 每次重跑前，先把目前已存在的 trade_plan.csv 視為「前一份交易計畫」。
    如果根目錄 trade_plan.csv 不存在，才讀 prev_trade_plan.csv。
    """
    cols = [
        "signal_date", "trade_date", "action", "stock_id", "price_tier",
        "target_weight", "ref_price", "suggested_amount", "entry_score", "stage", "note"
    ]
    current = read_csv_optional("trade_plan.csv", cols)
    if len(current):
        return current
    return read_csv_optional(PREV_TRADE_PLAN_FILE, cols)


def normalize_trade_plan_for_state(prev_df):
    if prev_df is None or prev_df.empty:
        return pd.DataFrame(columns=[
            "signal_date", "trade_date", "action", "stock_id", "price_tier",
            "target_weight", "ref_price", "suggested_amount", "entry_score", "stage", "note"
        ])
    out = prev_df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    if "stock_id" not in out.columns:
        if "stock" in out.columns:
            out = out.rename(columns={"stock": "stock_id"})
        elif "code" in out.columns:
            out = out.rename(columns={"code": "stock_id"})
        elif "symbol" in out.columns:
            out = out.rename(columns={"symbol": "stock_id"})
    if "stock_id" not in out.columns:
        out["stock_id"] = ""
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)
    if "action" not in out.columns:
        out["action"] = ""
    out["action"] = out["action"].astype(str).str.upper().str.strip()
    return out[out["stock_id"] != ""].drop_duplicates(subset=["stock_id"], keep="last").reset_index(drop=True)


def save_trade_plan_state(prev_trade_df, new_trade_df, signal_date, trade_date):
    """
    產生三層狀態：
    1. prev_trade_plan.csv：前一份交易計畫，給下一次策略比對。
    2. trade_plan_history/YYYY-MM-DD.csv：以 trade_date 存檔，方便隔天直接呼叫交易清單。
    3. dashboard/data 同步鏡像。
    """
    prev_norm = normalize_trade_plan_for_state(prev_trade_df)
    if len(prev_norm):
        write_csv(PREV_TRADE_PLAN_FILE, prev_norm)
        write_csv(DASHBOARD_DATA_DIR / PREV_TRADE_PLAN_FILE, prev_norm)

    TRADE_PLAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_TRADE_PLAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    trade_key = str(pd.Timestamp(trade_date).date())
    signal_key = str(pd.Timestamp(signal_date).date())

    # 主要用 trade_date 取檔：今天盤後產生，隔天交易。
    write_csv(TRADE_PLAN_HISTORY_DIR / f"{trade_key}.csv", new_trade_df)
    write_csv(DASHBOARD_TRADE_PLAN_HISTORY_DIR / f"{trade_key}.csv", new_trade_df)

    # 額外保留 signal_date，方便排查當天盤後產生的訊號。
    write_csv(TRADE_PLAN_HISTORY_DIR / f"signal_{signal_key}.csv", new_trade_df)
    write_csv(DASHBOARD_TRADE_PLAN_HISTORY_DIR / f"signal_{signal_key}.csv", new_trade_df)



def normalize_stock_id(s):
    if pd.isna(s):
        return ""
    return str(s).strip().replace(".0", "")


def to_num(v, default=np.nan):
    n = pd.to_numeric(v, errors="coerce")
    if pd.isna(n):
        return default
    return n


def price_tier_key(price):
    if pd.isna(price):
        return "unknown"
    p = float(price)
    if p < 50:
        return "lt_50"
    if p < 100:
        return "p50_100"
    if p < 300:
        return "p100_300"
    if p < 500:
        return "p300_500"
    if p < 1000:
        return "p500_1000"
    return "gt_1000"


def next_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    d = pd.Timestamp(ts).normalize() + pd.Timedelta(days=1)
    while d.weekday() >= 5:
        d += pd.Timedelta(days=1)
    return d


# ---------- file bootstrap ----------

def ensure_dashboard_files():
    DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    TRADE_PLAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_TRADE_PLAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    templates = {
        POSITIONS_FILE: ["stock_id", "shares", "avg_cost", "last_action_date", "note"],
        WATCHLIST_FILE: ["stock_id"],
        "selection_debug.csv": [
            "date", "total_input", "valid_after_na",
            "core_primary_count", "alpha_primary_count",
            "core_final_count", "alpha_final_count"
        ],
        "position_monitor.csv": [
            "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
            "shares", "avg_cost", "pnl_pct", "target_weight",
            "current_weight_est", "stage", "action", "note"
        ],
        "watchlist_monitor.csv": [
            "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
            "holding_status", "strategy_bucket", "action", "pnl_pct"
        ],
        "trade_plan.csv": [
            "signal_date", "trade_date", "action", "stock_id", "price_tier",
            "target_weight", "ref_price", "suggested_amount", "entry_score", "stage", "note"
        ],
        "full_summary.csv": ["return", "mdd", "sharpe_daily"],
    }

    for name, cols in templates.items():
        root_path = Path(name)
        dash_path = DASHBOARD_DATA_DIR / name

        # root current_positions.csv / watchlist.csv 是主要來源；如果沒有才建立空檔。
        if name in [POSITIONS_FILE, WATCHLIST_FILE]:
            if not root_path.exists():
                write_csv(root_path, pd.DataFrame(columns=cols))
            if not dash_path.exists():
                write_csv(dash_path, pd.DataFrame(columns=cols))
        else:
            if not root_path.exists():
                write_csv(root_path, pd.DataFrame(columns=cols))
            if not dash_path.exists():
                write_csv(dash_path, pd.DataFrame(columns=cols))

    meta = DASHBOARD_DATA_DIR / "meta.json"
    if not meta.exists():
        meta.write_text(json.dumps({}, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_price_panel():
    panel = Path(PRICE_PANEL_FILE)
    if panel.exists():
        return

    merge = Path(MERGE_SCRIPT)
    if not merge.exists():
        raise FileNotFoundError(f"找不到 {PRICE_PANEL_FILE}，且 {MERGE_SCRIPT} 也不存在")

    print(f"{PRICE_PANEL_FILE} 不存在，先執行 {MERGE_SCRIPT}")
    subprocess.run([sys.executable, str(merge)], check=True)

    if not panel.exists():
        raise FileNotFoundError(f"執行 {MERGE_SCRIPT} 後仍找不到 {PRICE_PANEL_FILE}")


# ---------- data loading ----------

def load_price():
    ensure_price_panel()
    df = read_csv_auto(PRICE_PANEL_FILE)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        for alt in ["trade_date", "datetime"]:
            if alt in df.columns:
                df["date"] = df[alt]
                break

    if "stock_id" not in df.columns:
        for alt in ["symbol", "code", "stock"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break

    if "stock_id" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 stock_id 欄位")
    if "close" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 close 欄位")
    if "volume" not in df.columns:
        df["volume"] = np.nan

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    df = df.dropna(subset=["date", "stock_id", "close"]).copy()
    df = df[(df["stock_id"] != "") & (df["close"] > 0)]
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df


def load_positions():
    """
    v3.6 關鍵修正：
    root/current_positions.csv 是唯一真實來源。
    mobile_dashboard_v1/data/current_positions.csv 只能當顯示鏡像，不能優先讀。
    """
    root_path = Path(POSITIONS_FILE)

    if not root_path.exists():
        return pd.DataFrame(columns=["stock_id", "shares", "avg_cost", "last_action_date", "note"])

    pos = read_csv_auto(root_path)
    pos.columns = [str(c).lower().strip() for c in pos.columns]

    if "stock_id" not in pos.columns:
        return pd.DataFrame(columns=["stock_id", "shares", "avg_cost", "last_action_date", "note"])

    for col in ["shares", "avg_cost", "last_action_date", "note"]:
        if col not in pos.columns:
            pos[col] = ""

    pos["stock_id"] = pos["stock_id"].apply(normalize_stock_id)
    pos = pos[pos["stock_id"] != ""].copy()

    pos["shares"] = pd.to_numeric(pos["shares"], errors="coerce").fillna(DEFAULT_SHARES)
    pos["avg_cost"] = pd.to_numeric(pos["avg_cost"], errors="coerce")

    # 同一股票只保留最後一筆，避免重複列造成前端看似刪不掉。
    pos = pos.drop_duplicates(subset=["stock_id"], keep="last")

    return pos[["stock_id", "shares", "avg_cost", "last_action_date", "note"]].reset_index(drop=True)


def load_watchlist():
    root_path = Path(WATCHLIST_FILE)
    dash_path = DASHBOARD_DATA_DIR / WATCHLIST_FILE
    path = root_path if root_path.exists() else dash_path

    if not path.exists():
        return set()

    try:
        df = read_csv_auto(path)
        df.columns = [str(c).lower().strip() for c in df.columns]
        if "stock_id" not in df.columns:
            return set()
        return set(df["stock_id"].apply(normalize_stock_id).replace("", np.nan).dropna())
    except Exception:
        return set()


# ---------- strategy ----------

def build_features(df):
    g = df.groupby("stock_id")
    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)
    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)
    df = add_decision_features(df)
    return df


def select_stocks(day_df):
    total_input = len(day_df)
    valid = day_df.dropna(subset=["mom20", "mom60", "vol20"]).copy()
    valid_count = len(valid)

    core_primary = valid[valid["mom20"] > -0.02].copy()
    core_primary["score"] = core_primary["mom20"] * 0.6 + core_primary["mom60"] * 0.4
    core_primary = core_primary.sort_values("score", ascending=False)

    core_fallback = valid.copy()
    core_fallback["score"] = core_fallback["mom20"] * 0.6 + core_fallback["mom60"] * 0.4
    core_fallback = core_fallback.sort_values("score", ascending=False)

    core = core_primary.head(CORE_TOP_N).copy()
    if len(core) < MIN_CORE_FILL:
        extra = core_fallback[~core_fallback["stock_id"].isin(core["stock_id"])].head(MIN_CORE_FILL - len(core))
        core = pd.concat([core, extra], ignore_index=True)

    if len(core) < CORE_TOP_N:
        extra = core_fallback[~core_fallback["stock_id"].isin(core["stock_id"])].head(CORE_TOP_N - len(core))
        core = pd.concat([core, extra], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(CORE_TOP_N)

    alpha_primary = valid[valid["mom20"] > 0].copy()
    alpha_primary["quality"] = (
        alpha_primary["mom20"] * 0.6 + alpha_primary["mom60"] * 0.4
    ) / (alpha_primary["vol20"] + 1e-6)
    alpha_primary = alpha_primary.sort_values("quality", ascending=False)

    alpha_fallback = valid.copy()
    alpha_fallback["quality"] = (
        alpha_fallback["mom20"] * 0.6 + alpha_fallback["mom60"] * 0.4
    ) / (alpha_fallback["vol20"] + 1e-6)
    alpha_fallback = alpha_fallback.sort_values("quality", ascending=False)

    alpha = alpha_primary.head(ALPHA_TOP_N).copy()
    if len(alpha) < MIN_ALPHA_FILL:
        extra = alpha_fallback[~alpha_fallback["stock_id"].isin(alpha["stock_id"])].head(MIN_ALPHA_FILL - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True)

    if len(alpha) < ALPHA_TOP_N:
        extra = alpha_fallback[~alpha_fallback["stock_id"].isin(alpha["stock_id"])].head(ALPHA_TOP_N - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True).drop_duplicates(subset=["stock_id"]).head(ALPHA_TOP_N)

    debug = pd.DataFrame([{
        "date": str(day_df["date"].iloc[0].date()) if len(day_df) else "",
        "total_input": total_input,
        "valid_after_na": valid_count,
        "core_primary_count": len(core_primary),
        "alpha_primary_count": len(alpha_primary),
        "core_final_count": len(core),
        "alpha_final_count": len(alpha),
    }])
    return core, alpha, debug


def build_target_weights(core, alpha):
    target = {}
    core_n = max(len(core), 1)
    alpha_n = max(len(alpha), 1)

    for _, r in core.iterrows():
        target[r["stock_id"]] = CORE_WEIGHT / core_n

    for _, r in alpha.iterrows():
        target[r["stock_id"]] = target.get(r["stock_id"], 0) + ALPHA_WEIGHT / alpha_n

    for k in list(target.keys()):
        target[k] = min(float(target[k]), MAX_POSITION_WEIGHT)

    return target


def latest_signal_date(df):
    dates = sorted(pd.Series(df["date"].dt.normalize().unique()))
    if not dates:
        raise ValueError("交易日不足，無法產生訊號")
    return pd.Timestamp(dates[-1]).normalize()


# ---------- outputs ----------

def build_position_monitor(positions, price_map, target, signal_date, trade_date, signal_row_map=None):
    """
    鐵規則：position_monitor.csv 只能包含 current_positions.csv 裡存在的股票。
    """
    signal_row_map = signal_row_map or {}
    rows = []

    for _, current in positions.iterrows():
        stock_id = normalize_stock_id(current.get("stock_id", ""))
        if not stock_id:
            continue

        px_raw = price_map.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        tier = price_tier_key(ref_price)

        shares = to_num(current.get("shares"), DEFAULT_SHARES)
        avg_cost = to_num(current.get("avg_cost"), np.nan)
        target_weight = float(target.get(stock_id, 0))

        pnl_pct = np.nan
        if pd.notna(px_raw) and pd.notna(avg_cost) and float(avg_cost) > 0:
            pnl_pct = px_raw / avg_cost - 1.0

        current_value = shares * px_raw if pd.notna(shares) and pd.notna(px_raw) else np.nan
        current_weight_est = current_value / INITIAL_CAPITAL if pd.notna(current_value) else np.nan

        row = signal_row_map.get(stock_id, {})
        stage = position_stage(row)

        if pd.notna(pnl_pct) and pnl_pct <= STOP_LOSS_2:
            action = "STOP_LOSS"
            note = "達停損條件"
        elif target_weight <= 0:
            action = "SELL"
            note = "不在目標池"
        elif pd.notna(current_weight_est):
            diff = target_weight - current_weight_est
            if diff > BUY_BAND:
                action = "ADD"
                note = "低於目標權重"
            elif diff < REDUCE_BAND:
                action = "REDUCE"
                note = "高於目標權重"
            else:
                action = "HOLD"
                note = "權重在容許範圍"
        else:
            action = "HOLD"
            note = "缺少目前權重"

        rows.append({
            "signal_date": str(signal_date.date()),
            "trade_date": str(trade_date.date()),
            "stock_id": stock_id,
            "price_tier": tier,
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
            "shares": int(shares) if pd.notna(shares) else DEFAULT_SHARES,
            "avg_cost": round(avg_cost, 4) if pd.notna(avg_cost) else "",
            "pnl_pct": round(pnl_pct, 4) if pd.notna(pnl_pct) else "",
            "target_weight": round(target_weight, 4),
            "current_weight_est": round(current_weight_est, 4) if pd.notna(current_weight_est) else "",
            "stage": stage,
            "action": action,
            "note": note,
        })

    cols = [
        "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
        "shares", "avg_cost", "pnl_pct", "target_weight",
        "current_weight_est", "stage", "action", "note"
    ]
    return pd.DataFrame(rows, columns=cols)


def build_trade_plan(positions, price_map, target, signal_date, trade_date, signal_row_map=None, prev_trade_plan=None):
    """
    trade_plan 可以出現策略新選股，但不會寫入 current_positions.csv。
    v3.7：原本策略只給候選池，是否 BUY / WATCH 改由 decision_modules.entry_score 判斷。
    """
    signal_row_map = signal_row_map or {}
    prev_trade_plan = normalize_trade_plan_for_state(prev_trade_plan)
    prev_map = {
        normalize_stock_id(r.get("stock_id", "")): r
        for _, r in prev_trade_plan.iterrows()
        if normalize_stock_id(r.get("stock_id", ""))
    }
    held = set(positions["stock_id"].apply(normalize_stock_id)) if len(positions) else set()
    rows = []
    for stock_id in sorted(target.keys()):
        if stock_id in held:
            continue
        target_weight = float(target.get(stock_id, 0))
        if target_weight <= 0:
            continue
        px_raw = price_map.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        tier = price_tier_key(ref_price)
        row = signal_row_map.get(stock_id, {})
        score_info = entry_score(row, market_row=None)
        raw_action = score_info.get("entry_action", "SKIP")
        action = raw_action
        score = score_info.get("entry_score", 0)
        stage = position_stage(row)

        prev_row = prev_map.get(stock_id)
        prev_action = ""
        prev_score = ""
        prev_trade_date = ""
        state_note = ""
        if prev_row is not None:
            prev_action = str(prev_row.get("action", "")).upper().strip()
            prev_score = prev_row.get("entry_score", "")
            prev_trade_date = prev_row.get("trade_date", "")
            if prev_action == "WATCH" and score >= 80:
                action = "BUY"
                state_note = "昨日觀察轉今日買進"
            elif prev_action == "BUY" and score >= 60 and raw_action != "BUY":
                action = "WATCH"
                state_note = "前次買進後仍在觀察區"
            elif prev_action in ["WATCH", "BUY"] and score < 60:
                action = "SKIP"
                state_note = "前次名單轉弱"

        if action == "SKIP":
            continue
        suggested_amount = INITIAL_CAPITAL * target_weight if action == "BUY" else 0
        final_weight = target_weight if action == "BUY" else 0
        rows.append({
            "signal_date": str(signal_date.date()),
            "trade_date": str(trade_date.date()),
            "action": action,
            "stock_id": stock_id,
            "price_tier": tier,
            "target_weight": round(final_weight, 4),
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
            "suggested_amount": round(suggested_amount, 2),
            "entry_score": score,
            "stage": stage,
            "prev_action": prev_action,
            "prev_entry_score": prev_score,
            "prev_trade_date": prev_trade_date,
            "note": (state_note + "；" if state_note else "") + score_info.get("entry_reason", "模組化進場判斷"),
        })
    cols = ["signal_date", "trade_date", "action", "stock_id", "price_tier", "target_weight", "ref_price", "suggested_amount", "entry_score", "stage", "prev_action", "prev_entry_score", "prev_trade_date", "note"]
    return pd.DataFrame(rows, columns=cols)

def build_watchlist_monitor(watchlist, positions, price_map, core, alpha, signal_date, trade_date):
    held = set(positions["stock_id"].apply(normalize_stock_id)) if len(positions) else set()
    core_set = set(core["stock_id"].apply(normalize_stock_id)) if len(core) else set()
    alpha_set = set(alpha["stock_id"].apply(normalize_stock_id)) if len(alpha) else set()
    avg_cost_map = positions.set_index("stock_id")["avg_cost"].to_dict() if len(positions) else {}

    rows = []
    for stock_id in sorted(watchlist):
        px_raw = price_map.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        tier = price_tier_key(ref_price)

        bucket = "NONE"
        if stock_id in alpha_set:
            bucket = "BUY_READY"
        elif stock_id in core_set:
            bucket = "CANDIDATE"

        action = "WATCH"
        if stock_id in held:
            action = "HOLD_MONITOR"
        elif stock_id in alpha_set:
            action = "BUY_READY"
        elif stock_id in core_set:
            action = "CANDIDATE"

        pnl_pct = np.nan
        avg_cost = avg_cost_map.get(stock_id, np.nan)
        if pd.notna(px_raw) and pd.notna(avg_cost) and float(avg_cost) > 0:
            pnl_pct = px_raw / avg_cost - 1.0

        rows.append({
            "signal_date": str(signal_date.date()),
            "trade_date": str(trade_date.date()),
            "stock_id": stock_id,
            "price_tier": tier,
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
            "holding_status": "已持有" if stock_id in held else "未持有",
            "strategy_bucket": bucket,
            "action": action,
            "pnl_pct": round(pnl_pct, 4) if pd.notna(pnl_pct) else "",
        })

    cols = [
        "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
        "holding_status", "strategy_bucket", "action", "pnl_pct"
    ]
    return pd.DataFrame(rows, columns=cols)


def build_outputs(df, prev_trade_plan=None):
    signal_date = latest_signal_date(df)
    trade_date = next_business_day(signal_date)
    price_panel_latest_date = signal_date

    signal_df = df[df["date"].dt.normalize() == signal_date].copy()

    latest_close_df = (
        df.sort_values(["stock_id", "date"])
        .groupby("stock_id", as_index=False)
        .tail(1)
        .copy()
    )
    price_map = {normalize_stock_id(r["stock_id"]): r["close"] for _, r in latest_close_df.iterrows()}

    positions = load_positions()
    watchlist = load_watchlist()

    core, alpha, debug = select_stocks(signal_df)
    target = build_target_weights(core, alpha)
    signal_row_map = {normalize_stock_id(r["stock_id"]): r for _, r in signal_df.iterrows()}

    trade_df = build_trade_plan(positions, price_map, target, signal_date, trade_date, signal_row_map, prev_trade_plan)
    pos_df = build_position_monitor(positions, price_map, target, signal_date, trade_date, signal_row_map)
    watch_df = build_watchlist_monitor(watchlist, positions, price_map, core, alpha, signal_date, trade_date)

    summary = pd.DataFrame([{
        "return": 0,
        "mdd": 0,
        "sharpe_daily": 0,
    }])

    now_str = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "generated_at": now_str,
        "now_time": now_str,
        "signal_date": str(signal_date.date()),
        "trade_date": str(trade_date.date()),
        "price_panel_latest_date": str(price_panel_latest_date.date()),
        "data_state": "fresh",
        "source": "v1.4_signal_trade_history",
        "execution_rule": "T日盤後產生訊號，T+1交易",
        "prev_trade_plan_file": PREV_TRADE_PLAN_FILE,
        "trade_plan_history_file": f"trade_plan_history/{str(trade_date.date())}.csv",
        "trade_plan_batch": now_str,
        "position_writeback_state": "idle",
        "position_source": POSITIONS_FILE,
        "position_count": int(len(positions)),
        "trade_plan_count": int(len(trade_df)),
        "decision_modules_available": bool(DECISION_MODULES_AVAILABLE),
    }

    return trade_df, pos_df, watch_df, summary, debug, positions, meta


def write_csv_both(df, filename, write_root=True):
    if write_root:
        write_csv(filename, df)
    write_csv(DASHBOARD_DATA_DIR / filename, df)


def main():
    ensure_dashboard_files()

    # 在覆蓋今天新 trade_plan 前，先讀取目前存在的清單作為「前一份交易計畫」。
    prev_trade_df = load_previous_trade_plan()

    df = build_features(load_price())
    trade_df, pos_df, watch_df, summary_df, debug_df, positions_df, meta = build_outputs(df, prev_trade_df)

    # 保存狀態與歷史：讓系統可以呼叫前一份名單，也可以用 trade_date 找回隔天要交易的清單。
    signal_date = pd.Timestamp(meta.get("signal_date"))
    trade_date = pd.Timestamp(meta.get("trade_date"))
    save_trade_plan_state(prev_trade_df, trade_df, signal_date, trade_date)

    # 輸出策略與監控檔
    write_csv_both(trade_df, "trade_plan.csv", write_root=True)
    write_csv_both(pos_df, "position_monitor.csv", write_root=True)
    write_csv_both(watch_df, "watchlist_monitor.csv", write_root=True)
    write_csv_both(summary_df, "full_summary.csv", write_root=True)
    write_csv_both(debug_df, "selection_debug.csv", write_root=True)

    # v3.6：root current_positions.csv 是唯一來源；這裡只做鏡像同步，不讓 dashboard 舊檔反寫回 root。
    write_csv(POSITIONS_FILE, positions_df)
    write_csv(DASHBOARD_DATA_DIR / POSITIONS_FILE, positions_df)

    # watchlist 也同步鏡像，避免 dashboard 顯示舊資料。
    watch_root = Path(WATCHLIST_FILE)
    if watch_root.exists():
        try:
            watch_df_raw = read_csv_auto(watch_root)
            write_csv(DASHBOARD_DATA_DIR / WATCHLIST_FILE, watch_df_raw)
        except Exception:
            pass

    meta_text = json.dumps(meta, ensure_ascii=False, indent=2)
    (DASHBOARD_DATA_DIR / "meta.json").write_text(meta_text, encoding="utf-8")
    Path("meta.json").write_text(meta_text, encoding="utf-8")

    print("完成 v3.7 module decision pipeline")
    print(meta_text)


if __name__ == "__main__":
    main()
