import json
import subprocess
import sys
import re
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
# v2.6 behavior validation version
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


def is_common_stock_id(stock_id):
    """
    台股普通股保護濾網：
    只保留 4 碼純數字普通股，例如 1101 / 2330 / 3037。
    排除：
    - 00 開頭 ETF
    - 01 / 02 / 03 開頭權證、牛熊、槓反商品
    - 含英文字母代號，例如 01004T / 02001L / 03003T
    """
    sid = normalize_stock_id(stock_id)
    return bool(re.fullmatch(r"[1-9]\d{3}", sid))


def score_bridge(row):
    """
    v1.9.2 分數接線修復：
    不完全依賴 decision_modules.entry_score。
    只要 row 裡有 close / ma / momentum，就在 pipeline 端直接補一個可用分數。
    """
    if row is None:
        row = {}

    try:
        score_info = entry_score(row, market_row=None)
    except Exception:
        score_info = {"entry_score": 0, "entry_action": "SKIP", "entry_reason": "entry_score失敗，使用bridge"}

    base_score = to_num(score_info.get("entry_score", 0), 0)

    close = to_num(row.get("close"), np.nan) if hasattr(row, "get") else np.nan
    ma5 = to_num(row.get("ma5"), np.nan) if hasattr(row, "get") else np.nan
    ma10 = to_num(row.get("ma10"), np.nan) if hasattr(row, "get") else np.nan
    ma20 = to_num(row.get("ma20"), np.nan) if hasattr(row, "get") else np.nan
    ma60 = to_num(row.get("ma60"), np.nan) if hasattr(row, "get") else np.nan
    mom5 = to_num(row.get("mom5"), np.nan) if hasattr(row, "get") else np.nan
    mom20 = to_num(row.get("mom20"), np.nan) if hasattr(row, "get") else np.nan
    kd_k = to_num(row.get("kd_k"), np.nan) if hasattr(row, "get") else np.nan
    kd_d = to_num(row.get("kd_d"), np.nan) if hasattr(row, "get") else np.nan
    macd_diff = to_num(row.get("macd_diff"), np.nan) if hasattr(row, "get") else np.nan
    volume_ratio = to_num(row.get("volume_ratio"), np.nan) if hasattr(row, "get") else np.nan
    prev_high_20 = to_num(row.get("prev_high_20"), np.nan) if hasattr(row, "get") else np.nan
    candidate_score = to_num(row.get("score"), np.nan) if hasattr(row, "get") else np.nan

    bridge_score = 0
    reasons = []

    if pd.notna(close) and pd.notna(ma20):
        if close > ma20:
            bridge_score += 18
            reasons.append("站上MA20")
        elif close >= ma20 * 0.98:
            bridge_score += 10
            reasons.append("貼近MA20")

    if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
        if ma5 > ma10 > ma20:
            bridge_score += 18
            reasons.append("短均多排")
        elif ma5 >= ma10 * 0.995 and ma10 >= ma20 * 0.995:
            bridge_score += 10
            reasons.append("均線靠攏")

    if pd.notna(close) and pd.notna(ma60) and close > ma60:
        bridge_score += 6
        reasons.append("站上MA60")

    if pd.notna(mom5):
        if mom5 > 0:
            bridge_score += 10
            reasons.append("5日動能正")
        elif mom5 > -0.02:
            bridge_score += 5
            reasons.append("5日動能未破壞")

    if pd.notna(mom20):
        if mom20 > 0:
            bridge_score += 10
            reasons.append("20日動能正")
        elif mom20 > -0.03:
            bridge_score += 4
            reasons.append("20日動能中性")

    if pd.notna(kd_k) and pd.notna(kd_d):
        if kd_k > kd_d and kd_k < 88:
            bridge_score += 8
            reasons.append("KD偏多")
        elif 35 <= kd_k <= 75:
            bridge_score += 4
            reasons.append("KD中性")

    if pd.notna(macd_diff):
        if macd_diff > 0:
            bridge_score += 8
            reasons.append("MACD偏多")

    if pd.notna(volume_ratio):
        if 1.0 <= volume_ratio <= 3.5:
            bridge_score += 6
            reasons.append("量能配合")
        elif volume_ratio > 0.7:
            bridge_score += 3
            reasons.append("量能尚可")

    if pd.notna(close) and pd.notna(prev_high_20) and close >= prev_high_20 * 0.98:
        bridge_score += 8
        reasons.append("接近20日高")

    if pd.notna(candidate_score):
        # select_stocks 的 score 通常是 0~1，轉成 0~12 補分
        bridge_score += max(0, min(12, candidate_score * 12))
        reasons.append("候選池排序加分")

    final_score = int(round(max(base_score, bridge_score)))

    if final_score >= 65:
        action = "BUY"
    elif final_score >= 50:
        action = "TEST"
    elif final_score >= 35:
        action = "READY"
    else:
        action = "SKIP"

    reason = "；".join(reasons) if reasons else score_info.get("entry_reason", "分數資料不足")
    return {
        "entry_score": final_score,
        "entry_action": action,
        "entry_reason": reason,
        "base_score": base_score,
        "bridge_score": bridge_score,
    }


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
    """
    v2.0 完整資料層：
    不依賴 price_panel_daily.csv 事先有指標欄位。
    每次 pipeline 會自行補齊：
    - MA5 / MA10 / MA20 / MA60
    - MOM5 / MOM20 / MOM60
    - VOL20 / volume_ma20 / volume_ratio
    - KD
    - MACD
    - 前10/20日高點
    """
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = np.nan

    for c in ["close", "high", "low", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df = df[df["stock_id"].apply(is_common_stock_id)].copy()
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)

    g = df.groupby("stock_id", group_keys=False)

    df["ret1"] = g["close"].pct_change()
    df["mom5"] = g["close"].pct_change(5)
    df["mom20"] = g["close"].pct_change(20)
    df["mom60"] = g["close"].pct_change(60)

    for n in [5, 10, 20, 60, 120]:
        df[f"ma{n}"] = g["close"].rolling(n).mean().reset_index(level=0, drop=True)

    df["vol20"] = g["ret1"].rolling(20).std().reset_index(level=0, drop=True)
    df["volume_ma20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["volume_ratio"] = df["volume"] / (df["volume_ma20"] + 1e-9)

    df["low_20d"] = g["low"].rolling(20, min_periods=5).min().reset_index(level=0, drop=True)
    df["low_60d"] = g["low"].rolling(60, min_periods=10).min().reset_index(level=0, drop=True)
    df["prev_low_20"] = g["close"].shift(1).rolling(20).min().reset_index(level=0, drop=True)
    df["prev_high_10"] = g["close"].shift(1).rolling(10).max().reset_index(level=0, drop=True)
    df["prev_high_20"] = g["close"].shift(1).rolling(20).max().reset_index(level=0, drop=True)

    df["ma20_slope"] = g["ma20"].diff(3) / (g["ma20"].shift(3) + 1e-9)

    # KD
    low9 = g["low"].rolling(9).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9).max().reset_index(level=0, drop=True)
    rsv = (df["close"] - low9) / (high9 - low9 + 1e-9) * 100
    df["kd_k"] = rsv.groupby(df["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    df["kd_d"] = df["kd_k"].groupby(df["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    df["kd_k_prev"] = g["kd_k"].shift(1)
    df["kd_d_prev"] = g["kd_d"].shift(1)
    df["kd_cross_up"] = ((df["kd_k"] > df["kd_d"]) & (df["kd_k_prev"] <= df["kd_d_prev"])).astype(int)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df["macd_diff"] = ema12 - ema26
    df["macd_signal"] = df.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = df["macd_diff"] - df["macd_signal"]
    df["macd_diff_prev"] = g["macd_diff"].shift(1)
    df["macd_signal_prev"] = g["macd_signal"].shift(1)
    df["macd_cross_up"] = ((df["macd_diff"] > df["macd_signal"]) & (df["macd_diff_prev"] <= df["macd_signal_prev"])).astype(int)

    # 給 decision_modules 額外補欄，若模組存在會再補一次也沒關係
    df = add_decision_features(df)
    return df


def select_stocks(day_df):
    """
    v2.5 主力結構 + 動能保留：
    先建立主力候選池，再跑動能策略。
    目的：
    - 保留原本動能選股
    - 避免低價股、代號前段、單一族群洗版
    - 讓名單像市場真正主軸
    """
    total_input = len(day_df)

    required = [
        "close", "volume", "ma5", "ma10", "ma20", "ma60", "ma120",
        "mom5", "mom20", "mom60", "vol20", "volume_ratio",
        "prev_high_10", "prev_high_20", "prev_low_20",
        "low_20d", "low_60d", "ma20_slope",
        "kd_cross_up", "macd_cross_up", "macd_diff", "macd_hist"
    ]

    valid = day_df.copy()
    for col in required:
        if col not in valid.columns:
            valid[col] = np.nan

    valid["stock_id"] = valid["stock_id"].apply(normalize_stock_id)
    valid = valid[valid["stock_id"].apply(is_common_stock_id)].copy()

    valid["close"] = pd.to_numeric(valid["close"], errors="coerce")
    valid = valid.dropna(subset=["close"]).copy()

    # 基本數值防空
    if "volume" not in valid.columns:
        valid["volume"] = np.nan
    valid["volume"] = pd.to_numeric(valid["volume"], errors="coerce").fillna(0)

    valid["turnover_value"] = valid["close"] * valid["volume"]

    for col in ["ma5", "ma10", "ma20", "ma60", "ma120"]:
        valid[col] = pd.to_numeric(valid[col], errors="coerce").fillna(valid["close"])

    for col in ["mom5", "mom20", "mom60", "vol20", "volume_ratio", "macd_diff", "macd_hist", "ma20_slope"]:
        valid[col] = pd.to_numeric(valid[col], errors="coerce")
    valid["mom5"] = valid["mom5"].fillna(0)
    valid["mom20"] = valid["mom20"].fillna(0)
    valid["mom60"] = valid["mom60"].fillna(0)
    valid["vol20"] = valid["vol20"].fillna(0.03)
    valid["volume_ratio"] = valid["volume_ratio"].fillna(1.0)
    valid["macd_diff"] = valid["macd_diff"].fillna(0)
    valid["macd_hist"] = valid["macd_hist"].fillna(0)
    valid["ma20_slope"] = valid["ma20_slope"].fillna(0)

    for col in ["prev_high_10", "prev_high_20", "prev_low_20", "low_20d", "low_60d"]:
        valid[col] = pd.to_numeric(valid[col], errors="coerce").fillna(valid["close"])

    for col in ["kd_cross_up", "macd_cross_up"]:
        valid[col] = pd.to_numeric(valid[col], errors="coerce").fillna(0)

    valid_count = len(valid)
    if valid_count == 0:
        debug = pd.DataFrame([{
            "date": str(day_df["date"].iloc[0].date()) if len(day_df) else "",
            "total_input": total_input,
            "valid_after_na": 0,
            "structure_pool_count": 0,
            "core_primary_count": 0,
            "alpha_primary_count": 0,
            "core_final_count": 0,
            "alpha_final_count": 0,
            "data_layer_state": "no_close_data",
        }])
        return pd.DataFrame(), pd.DataFrame(), debug

    # ========= STEP 1 主力候選池：結構先行 =========
    price_rank = valid["close"].rank(pct=True)
    volume_rank = valid["volume"].rank(pct=True)
    turnover_rank = valid["turnover_value"].rank(pct=True)

    valid["price_structure_score"] = np.select(
        [
            valid["close"] >= 100,
            valid["close"] >= 50,
            valid["close"] >= 20,
            valid["close"] >= 10,
        ],
        [1.00, 0.90, 0.72, 0.45],
        default=0.15
    )

    valid["liquidity_score"] = (
        volume_rank.fillna(0) * 0.55 +
        turnover_rank.fillna(0) * 0.45
    )

    valid["structure_score"] = (
        valid["price_structure_score"] * 0.45 +
        valid["liquidity_score"] * 0.45 +
        ((valid["volume_ratio"] >= 0.8) & (valid["volume_ratio"] <= 4.5)).astype(int) * 0.10
    )

    # 不是硬砍低價，而是降權；主力池用結構分數取前段
    structure_pool = valid[
        (valid["close"] >= 10) &
        (valid["structure_score"] >= valid["structure_score"].quantile(0.35))
    ].copy()

    if len(structure_pool) < 80:
        structure_pool = valid.sort_values("structure_score", ascending=False).head(min(300, len(valid))).copy()

    # ========= STEP 2 動能保留 =========
    x = structure_pool.copy()

    x["ma_max"] = x[["ma5", "ma10", "ma20"]].max(axis=1)
    x["ma_min"] = x[["ma5", "ma10", "ma20"]].min(axis=1)
    x["ma_converge_pct"] = ((x["ma_max"] - x["ma_min"]) / x["close"]).replace([np.inf, -np.inf], np.nan).fillna(0)

    x["trend_ok"] = (
        (x["close"] >= x["ma20"] * 0.97) &
        (x["mom5"] > -0.06)
    ).astype(int)

    x["ma_bull"] = (
        (x["ma5"] >= x["ma10"] * 0.995) &
        (x["ma10"] >= x["ma20"] * 0.995)
    ).astype(int)

    x["breakout_20"] = (x["close"] > x["prev_high_20"] * 1.003).astype(int)
    x["breakout_10"] = (x["close"] > x["prev_high_10"] * 1.003).astype(int)
    x["near_high_20"] = (x["close"] >= x["prev_high_20"] * 0.97).astype(int)

    x["momentum_ok"] = (
        (x["mom5"] > -0.01) |
        (x["mom20"] > 0) |
        (x["macd_diff"] > 0) |
        (x["kd_cross_up"] == 1) |
        (x["macd_cross_up"] == 1)
    ).astype(int)

    # v2.6：死股排除 + 佈局雙條件
    x["dead_stock"] = (
        ((x["volume_ratio"] < 0.85) & (x["mom5"].abs() < 0.018) & (x["mom20"] <= 0.015)) |
        ((x["volume"] < 300) & (x["mom5"].abs() < 0.018))
    ).astype(int)

    x["structure_improving"] = (
        (x["low_20d"] >= x["low_60d"] * 0.97) |
        (x["ma20_slope"] > -0.002) |
        (x["close"] >= x["ma20"] * 0.96)
    ).astype(int)

    x["behavior_evidence"] = (
        (x["volume_ratio"] > 1.08) |
        ((x["mom5"] > 0) & (x["volume_ratio"] > 0.9)) |
        (x["kd_cross_up"] == 1) |
        (x["macd_cross_up"] == 1)
    ).astype(int)

    x["main_force_setup"] = (
        (x["structure_improving"] == 1) &
        (x["behavior_evidence"] == 1) &
        (x["dead_stock"] == 0)
    ).astype(int)

    # ========= STEP 3 分數：結構 + 動能 + 主力階段 =========
    x["momentum_score"] = (
        x["breakout_20"] * 0.22
        + x["breakout_10"] * 0.14
        + x["near_high_20"] * 0.10
        + x["ma_bull"] * 0.12
        + x["trend_ok"] * 0.12
        + x["momentum_ok"] * 0.15
        + x["mom5"].clip(-0.1, 0.2) * 0.08
        + x["mom20"].clip(-0.1, 0.3) * 0.07
    )

    x["main_force_score"] = (
        x["main_force_setup"] * 0.22
        + x["structure_improving"] * 0.06
        + x["behavior_evidence"] * 0.08
        + ((x["volume_ratio"] >= 1.05) & (x["volume_ratio"] <= 4.5)).astype(int) * 0.05
        + (1 - x["ma_converge_pct"].clip(0, 0.25)) * 0.05
        - x["dead_stock"] * 0.30
    )

    x["score"] = (
        x["structure_score"] * 0.40
        + x["momentum_score"] * 0.42
        + x["main_force_score"] * 0.18
    )

    # 低價過度降權，避免全變低價股
    x.loc[x["close"] < 20, "score"] *= 0.82
    x.loc[x["close"] < 10, "score"] *= 0.55

    # 價格分層，避免單一低價層洗版
    def bucket_price(p):
        if p < 20:
            return "20以下"
        if p < 50:
            return "20-50"
        if p < 100:
            return "50-100"
        if p < 300:
            return "100-300"
        return "300以上"

    x["price_bucket_v25"] = x["close"].apply(bucket_price)

    # ========= STEP 4 名單控制：分層配額 =========
    quota = {
        "300以上": 5,
        "100-300": 7,
        "50-100": 6,
        "20-50": 5,
        "20以下": 2,
    }

    selected_parts = []
    for b, q in quota.items():
        part = x[x["price_bucket_v25"] == b].sort_values(["score", "structure_score", "momentum_score"], ascending=False).head(q)
        selected_parts.append(part)

    selected = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()

    # v2.6：優先排除死股。若太少，再允許低分觀察補位。
    if "dead_stock" in selected.columns:
        selected = selected[selected["dead_stock"] == 0].copy()

    if len(selected) < CORE_TOP_N:
        pool_extra = x[x["dead_stock"] == 0] if "dead_stock" in x.columns else x
        extra = pool_extra[~pool_extra["stock_id"].isin(selected["stock_id"])].sort_values(["score", "structure_score"], ascending=False).head(CORE_TOP_N - len(selected))
        selected = pd.concat([selected, extra], ignore_index=True)

    if len(selected) < 8:
        extra = x[~x["stock_id"].isin(selected["stock_id"])].sort_values(["score", "structure_score"], ascending=False).head(8 - len(selected))
        selected = pd.concat([selected, extra], ignore_index=True)

    selected = selected.drop_duplicates(subset=["stock_id"]).sort_values(["score", "structure_score", "momentum_score"], ascending=False)

    # candidate bucket
    selected["candidate_bucket"] = "structure_momentum"
    selected.loc[selected["breakout_20"] == 1, "candidate_bucket"] = "breakout"
    selected.loc[(selected["momentum_ok"] == 1) & (selected["breakout_20"] == 0), "candidate_bucket"] = "pullback"
    selected.loc[selected["main_force_setup"] == 1, "candidate_bucket"] = "setup"

    core = selected.head(CORE_TOP_N).copy()

    alpha_source = core.copy()
    if len(alpha_source):
        alpha = alpha_source.sort_values(["momentum_score", "score", "structure_score"], ascending=False).head(ALPHA_TOP_N).copy()
    else:
        alpha = pd.DataFrame()

    if len(alpha) < MIN_ALPHA_FILL and len(core):
        extra = core[~core["stock_id"].isin(alpha["stock_id"])].head(MIN_ALPHA_FILL - len(alpha))
        alpha = pd.concat([alpha, extra], ignore_index=True)

    debug = pd.DataFrame([{
        "date": str(day_df["date"].iloc[0].date()) if len(day_df) else "",
        "total_input": total_input,
        "valid_after_na": valid_count,
        "structure_pool_count": int(len(structure_pool)),
        "core_primary_count": int(len(selected)),
        "alpha_primary_count": int(len(alpha_source)) if 'alpha_source' in locals() else 0,
        "core_final_count": int(len(core)),
        "alpha_final_count": int(len(alpha)),
        "dead_stock_count": int(x["dead_stock"].sum()) if "dead_stock" in x.columns else 0,
        "data_layer_state": "v2_6_behavior_validation",
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




def action_split_fields(action_display, score):
    """
    v1.8 給前端 action 欄位分上下兩行：
    BUY = 突破型
    TEST / WATCH_A = 試單型
    READY / WATCH_B = 準備型
    """
    action_display = str(action_display or "").upper()
    score_num = to_num(score, 0)

    if action_display == "BUY":
        return "🟢 買進", "結構動能"
    if action_display in ["TEST", "BUY_SMALL", "WATCH_A"]:
        return "🟠 試盤", "結構轉強"
    if action_display in ["READY", "WATCH_B"]:
        if score_num >= 50:
            return "🟡 佈局", "低檔吸籌"
        return "🟡 佈局", "等待轉強"
    if action_display == "WATCH":
        return "🟣 觀察", "等待確認"
    if action_display == "OBSERVE":
        return "⚪ 觀察", "弱訊號"
    return "⚪ 不碰", "條件不足"


def human_action_note(action, score, stage, raw_reason="", prev_action=""):
    """
    v1.8 把技術理由轉成手機上可以一眼理解的交易語言。
    """
    action = str(action or "").upper()
    prev_action = str(prev_action or "").upper()
    score_num = to_num(score, 0)

    if action == "BUY":
        return f"突破確認｜分數{int(score_num)}｜可分批進場"

    if action in ["TEST", "BUY_SMALL"]:
        return f"試單候選｜分數{int(score_num)}｜先小倉，隔日確認"

    if action == "READY":
        return f"準備觀察｜分數{int(score_num)}｜等突破，不追高"

    if action == "WATCH":
        if score_num >= 60:
            return f"接近突破｜分數{int(score_num)}｜可小倉試單"
        return f"等待方向｜分數{int(score_num)}｜先觀察"

    if action == "SELL":
        return f"轉弱出場｜分數{int(score_num)}"

    return f"暫不操作｜分數{int(score_num)}"


def action_weight_multiplier(action, score):
    """
    v1.8 權重分級：
    BUY   = 主倉
    TEST  = 試單
    READY = 準備觀察，不配置金額
    """
    action = str(action or "").upper()
    score_num = to_num(score, 0)

    if action == "BUY":
        return 1.0
    if action in ["TEST", "BUY_SMALL"]:
        return 0.35 if score_num < 60 else 0.5
    if action == "WATCH":
        return 0.3
    if action == "READY":
        return 0.0
    if action == "OBSERVE":
        return 0.0
    return 0.0


def is_breakout_signal(row, score):
    """
    v1.6 突破進場：
    - 收盤突破前10日高點：初步突破
    - 收盤突破前20日高點：強突破
    """
    close = to_num(row.get("close"), np.nan) if hasattr(row, "get") else np.nan
    prev_high_10 = to_num(row.get("prev_high_10"), np.nan) if hasattr(row, "get") else np.nan
    prev_high_20 = to_num(row.get("prev_high_20"), np.nan) if hasattr(row, "get") else np.nan
    score_num = to_num(score, 0)

    if pd.notna(close) and pd.notna(prev_high_20) and close > prev_high_20 * 1.005 and score_num >= 55:
        return True, "突破20日高點"
    if pd.notna(close) and pd.notna(prev_high_10) and close > prev_high_10 * 1.005 and score_num >= 60:
        return True, "突破10日高點"
    return False, ""


def normalize_watch_grade(action, score):
    """
    不收斂名單，但把 WATCH 分成強弱。
    """
    action = str(action or "").upper()
    score_num = to_num(score, 0)

    if action == "BUY":
        return "BUY"
    if action == "WATCH":
        if score_num >= 60:
            return "WATCH_A"
        return "WATCH_B"
    return action


def build_trade_plan(positions, price_map, target, signal_date, trade_date, signal_row_map=None, prev_trade_plan=None):
    """
    v2.4：主力同步 + 不空單保護。
    原則：
    1. 優先輸出策略正式訊號 BUY / TEST / READY。
    2. 若正式訊號太少，不從全市場亂補，只從 select_stocks 推出的候選池中補 OBSERVE。
    3. OBSERVE 不給建議金額，只作為弱訊號觀察，不等於買進。
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
    observe_pool = []

    cols = [
        "signal_date", "trade_date", "action", "action_label", "action_sub", "raw_action",
        "stock_id", "price_tier", "target_weight", "ref_price", "suggested_amount",
        "entry_score", "stage", "prev_action", "prev_entry_score", "prev_trade_date",
        "note", "detail_note"
    ]

    def append_trade_row(stock_id, action, score, row, target_weight, state_note="", prev_action="", prev_score="", prev_trade_date=""):
        px_raw = price_map.get(stock_id, np.nan)
        ref_price = px_raw * (1 + SLIPPAGE) if pd.notna(px_raw) else np.nan
        tier = price_tier_key(ref_price)
        stage = position_stage(row)

        action = str(action or "OBSERVE").upper().strip()
        if action not in ["BUY", "TEST", "READY", "WATCH", "WATCH_A", "WATCH_B", "OBSERVE"]:
            action = "OBSERVE"

        action_display = normalize_watch_grade(action, score)
        if action in ["TEST", "READY", "OBSERVE"]:
            action_display = action

        action_label, action_sub = action_split_fields(action_display, score)
        multiplier = action_weight_multiplier(action, score)
        final_weight = float(target_weight or 0) * multiplier
        suggested_amount = INITIAL_CAPITAL * final_weight

        score_info = score_bridge(row)
        raw_reason = (state_note + "；" if state_note else "") + score_info.get("entry_reason", "主力同步判斷")

        if action == "OBSERVE":
            simple_note = f"弱觀察｜分數{int(to_num(score, 0))}｜候選池前排，不追高"
        else:
            simple_note = human_action_note(action, score, stage, raw_reason, prev_action)

        rows.append({
            "signal_date": str(signal_date.date()),
            "trade_date": str(trade_date.date()),
            "action": action_display,
            "action_label": action_label,
            "action_sub": action_sub,
            "raw_action": action,
            "stock_id": stock_id,
            "price_tier": tier,
            "target_weight": round(final_weight, 4),
            "ref_price": round(ref_price, 4) if pd.notna(ref_price) else "",
            "suggested_amount": round(suggested_amount, 2),
            "entry_score": int(round(to_num(score, 0))),
            "stage": stage,
            "prev_action": prev_action,
            "prev_entry_score": prev_score,
            "prev_trade_date": prev_trade_date,
            "note": simple_note,
            "detail_note": raw_reason,
        })

    for stock_id in sorted(target.keys()):
        stock_id = normalize_stock_id(stock_id)
        if not is_common_stock_id(stock_id):
            continue
        if stock_id in held:
            continue

        target_weight = float(target.get(stock_id, 0))
        if target_weight <= 0:
            continue

        row = signal_row_map.get(stock_id, {})
        score_info = score_bridge(row)
        raw_action = str(score_info.get("entry_action", "OBSERVE")).upper()
        action = raw_action
        score = to_num(score_info.get("entry_score", 0), 0)

        prev_row = prev_map.get(stock_id)
        prev_action = ""
        prev_score = ""
        prev_trade_date = ""
        state_note = ""

        if prev_row is not None:
            prev_action = str(prev_row.get("action", "")).upper().strip()
            prev_score = prev_row.get("entry_score", "")
            prev_trade_date = prev_row.get("trade_date", "")

            if prev_action in ["WATCH", "WATCH_A", "TEST", "READY", "OBSERVE"] and score >= 60:
                action = "BUY"
                state_note = "前次觀察轉強"
            elif prev_action == "BUY" and score >= 48 and raw_action != "BUY":
                action = "TEST"
                state_note = "前次買進後仍在試盤區"

        bucket = str(row.get("candidate_bucket", "")).lower() if hasattr(row, "get") else ""
        breakout_ok, breakout_reason = is_breakout_signal(row, score)

        if breakout_ok or bucket == "breakout" or action == "BUY":
            action = "BUY"
            state_note = (state_note + "；" if state_note else "") + (breakout_reason or "主升發動")
        elif action == "TEST" or bucket == "pullback" or score >= 50:
            action = "TEST"
            state_note = (state_note + "；" if state_note else "") + "試盤候選"
        elif action == "READY" or bucket in ["squeeze", "fallback"] or score >= 28:
            action = "READY"
            state_note = (state_note + "；" if state_note else "") + "佈局候選"
        else:
            # 不丟掉，放入弱觀察池；只從策略候選池補，不從全市場補
            observe_pool.append((score, stock_id, row, target_weight))
            continue

        append_trade_row(stock_id, action, score, row, target_weight, state_note, prev_action, prev_score, prev_trade_date)

    # v2.4 最低輸出保護：不是硬保底，只補候選池前排 OBSERVE
    MIN_OUTPUT = 8
    MAX_OBSERVE = 12

    existing = set([r["stock_id"] for r in rows])
    if len(rows) < MIN_OUTPUT:
        observe_pool = sorted(observe_pool, key=lambda x: x[0], reverse=True)
        need = min(MAX_OBSERVE, MIN_OUTPUT - len(rows))
        added = 0
        for score, stock_id, row, target_weight in observe_pool:
            if stock_id in existing:
                continue
            append_trade_row(
                stock_id=stock_id,
                action="OBSERVE",
                score=max(score, 10),
                row=row,
                target_weight=0.0,
                state_note="策略候選池弱訊號補位"
            )
            existing.add(stock_id)
            added += 1
            if added >= need:
                break

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

    # v2.0：score / candidate_bucket 來自 select_stocks，因此 signal_row_map 必須用候選池資料，
    # 不能只用原始 signal_df，否則 action / score 會斷線。
    candidate_rows = pd.concat([core, alpha], ignore_index=True) if (len(core) or len(alpha)) else signal_df.copy()
    candidate_rows = candidate_rows.drop_duplicates(subset=["stock_id"], keep="first")
    signal_row_map = {normalize_stock_id(r["stock_id"]): r for _, r in candidate_rows.iterrows()}

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
        "source": "v2.6_behavior_validation",
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
    # v2.0：輸出最新技術指標快照，方便檢查分數來源
    latest_feature_df = df.sort_values(["stock_id", "date"]).groupby("stock_id", as_index=False).tail(1).copy()
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
