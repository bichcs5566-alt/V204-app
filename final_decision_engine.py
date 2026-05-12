"""
final_decision_engine.py
v266.10 market_snapshot 主表版：多來源補資料層

基於 v266.9.2：
1. 保留所有 UI 欄位。
2. 除了 feature_panel_daily.csv，也從 pre_move_candidates.csv / timing_candidates.csv 補資料。
3. 不改策略判斷，只做資料補齊。

原 v266.9.2 說明：
完整補流動性資料

目的：
1. 保留 UI 欄位，不再因為缺資料就空白。
2. 從 feature_panel_daily.csv 補每檔最新：
   close / volume / turnover / liquidity_level / liquidity_tag / liquidity_score
3. trade_plan / candidates / alpha / core 有資料優先，沒有才用 feature_panel 補。
4. 持倉 EXIT 優先，進場策略不改。
5. CSV 輸出 utf-8-sig。
"""

from pathlib import Path
from datetime import datetime, timedelta
import json
import math
import pandas as pd
from chip_concentration_v26621 import add_chip_columns
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# v302.2：所有 final decision 會被 loc 指派的欄位統一鎖 object。
V302_MUTABLE_COLS = [
    "final_action", "entry_type", "execution_flag", "allowed",
    "suggested_amount", "target_weight", "priority",
    "system_note", "reason", "source", "bucket", "strategy_type",
    "top_opportunity", "section_top_opportunity",
    "opportunity_rank", "section_opportunity_rank",
    "stock_name", "liquidity_level", "liquidity_tag",
    "signal_stage_v303", "signal_stage_rank_v303", "stage_reason_v303",
    "test_rank_score_v304", "test_rank_v304", "test_top_tag_v304", "test_rank_reason_v304",
]


OUTPUT_COLUMNS = [
    "final_action", "signal_date", "trade_date", "stock_id", "stock_name", "source", "bucket", "strategy_type", "score", "entry_type",
    "execution_flag", "allowed", "close", "suggested_amount", "target_weight",
    "priority", "reason", "system_note",
    "opportunity_score", "opportunity_rank", "top_opportunity",
    "liquidity_level", "liquidity_tag", "liquidity_score", "volume", "turnover",
    "chip_score", "chip_label", "chip_display", "chip_reason", "chip_hint", "chip_valid_count", "chip_missing", "chip_confidence",
    "main_force_gate_v302",
    "main_force_score_v302",
    "chip_trace_v302",
    "obv_trace_v302",
    "attack_structure_v302",
    "volume_start_v302",
    "not_overheat_v302",
    "signal_stage_v303",
    "signal_stage_rank_v303",
    "ignition_score_v303",
    "evolution_score_v303",
    "stage_reason_v303",
    "test_rank_score_v304",
    "test_rank_v304",
    "test_top_tag_v304",
    "test_rank_reason_v304",
    "section_opportunity_rank", "section_top_opportunity",
    "attack_score_v309", "final_attack_score_v309", "final_sort_score_v309", "hard_reject_v309",
    "short_turn_weak_v309", "ma_sticky_no_attack_v309", "box_middle_v309", "liquidity_only_v309",
    "final_attack_score_final_v309", "final_sort_score_final_v309", "hard_reject_final_v309",
]

def clean_text(v, default=""):
    if v is None:
        return default
    if isinstance(v, float) and math.isnan(v):
        return default
    s = str(v)
    if s.lower() in ["nan", "none", "null"]:
        return default
    return s

def normalize_stock_id(x):
    s = clean_text(x).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s

def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc, dtype={"stock_id": str})
                if not df.empty:
                    df.columns = [str(c).strip() for c in df.columns]
                    if "stock_id" in df.columns:
                        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
                    return df
            except Exception:
                continue
    return pd.DataFrame()

def write_csv_both(df, name):
    df.to_csv(ROOT / name, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / name, index=False, encoding="utf-8-sig")

def is_true(x):
    return str(x).strip().lower() in ["true", "1", "yes"] or x is True


# v266.32 台股交易日引擎：只補日期邏輯，不動策略、分數、排序、資金控管。
TW_MARKET_HOLIDAYS = {
    "2026-01-01",
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-02-27",
    "2026-04-03", "2026-04-06",
    "2026-05-01",
    "2026-06-19",
    "2026-09-25",
    "2026-10-09",
}


def _date_text(v):
    s = str(v).strip()
    if not s or s.lower() in ["nan", "none", "null"]:
        return ""
    if len(s) >= 10:
        s = s[:10]
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except Exception:
        return ""


def next_tw_trading_day(signal_date):
    """
    訊號日後的下一個台股交易日。
    例如 2026-05-01（五，勞動節休市）→ 2026-05-04。
    """
    s = _date_text(signal_date)
    if not s:
        return ""

    d = datetime.strptime(s, "%Y-%m-%d").date()
    while True:
        d = d + timedelta(days=1)
        ds = d.strftime("%Y-%m-%d")
        if d.weekday() >= 5:
            continue
        if ds in TW_MARKET_HOLIDAYS:
            continue
        return ds


def pick_signal_date(row, fallback=""):
    """
    從來源資料抓訊號日。
    不用 trade_date 當 signal_date，避免把已算出的交易日反污染回訊號日。
    """
    for c in ["signal_date", "date", "asof_date", "run_date", "generated_date"]:
        if c in row.index:
            v = _date_text(row.get(c, ""))
            if v:
                return v
    return _date_text(fallback)


def pct_text(x):
    try:
        return f"{round(float(x) * 100, 2)}%"
    except Exception:
        return ""

def calc_liquidity(df):
    df = df.copy()

    for c in ["close", "volume"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["turnover"] = df["close"] * df["volume"] * 1000

    vol_rank = df["volume"].rank(pct=True).fillna(0)
    turnover_rank = df["turnover"].rank(pct=True).fillna(0)
    df["liquidity_score"] = (vol_rank * 50 + turnover_rank * 50).round(2)

    high = (df["volume"] >= 3000) | (df["turnover"] >= 80_000_000) | (df["liquidity_score"] >= 75)
    medium = (df["volume"] >= 1000) | (df["turnover"] >= 30_000_000) | (df["liquidity_score"] >= 45)
    low = df["volume"] >= 500

    df["liquidity_level"] = np.select(
        [high, medium, low],
        ["HIGH", "MEDIUM", "LOW"],
        default="BLOCK"
    )

    df["liquidity_tag"] = df["liquidity_level"].map({
        "HIGH": "高流動性",
        "MEDIUM": "中流動性",
        "LOW": "低流動性",
        "BLOCK": "流動性不足",
    })

    return df

def load_feature_lookup():
    df = read_csv_any([ROOT / "feature_panel_daily.csv", DATA_DIR / "feature_panel_daily.csv"])
    if df.empty or "stock_id" not in df.columns:
        return {}

    df = df.copy()
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(["stock_id", "date"])
        latest = df.groupby("stock_id", as_index=False).tail(1).copy()
    else:
        latest = df.drop_duplicates("stock_id", keep="last").copy()

    latest = calc_liquidity(latest)

    keep = ["stock_id", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]
    for c in keep:
        if c not in latest.columns:
            latest[c] = ""

    return {str(r["stock_id"]): r.to_dict() for _, r in latest[keep].iterrows()}

def make_lookup():
    frames = []

    for name in [
        "trade_plan.csv",
        "trading_system_plan.csv",
        "candidates.csv",
        "alpha_candidates.csv",
        "core_candidates.csv",
        "pre_move_candidates.csv",
        "timing_candidates.csv",
    ]:
        df = read_csv_any([ROOT / name, DATA_DIR / name])
        if not df.empty and "stock_id" in df.columns:
            df = df.copy()
            df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
            frames.append(df)

    feature_lookup = load_feature_lookup()

    if not frames:
        return feature_lookup

    all_df = pd.concat(frames, ignore_index=True)
    all_df["stock_id"] = all_df["stock_id"].apply(normalize_stock_id)

    # 如果主表沒有流動性欄位，先補空欄位，再用 feature_lookup 填補。
    for c in ["stock_name", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]:
        if c not in all_df.columns:
            all_df[c] = ""

    for idx, row in all_df.iterrows():
        sid = str(row["stock_id"])
        f = feature_lookup.get(sid, {})
        for c in ["stock_name", "close", "volume", "turnover", "liquidity_level", "liquidity_tag", "liquidity_score"]:
            v = clean_text(all_df.at[idx, c], "")
            if v == "" or v == "0" or v == "0.0":
                if c in f:
                    all_df.at[idx, c] = f[c]

    all_df = all_df.drop_duplicates("stock_id", keep="first")
    out = {str(r["stock_id"]): r.to_dict() for _, r in all_df.iterrows()}

    # feature lookup 也要補進來，讓 PRE/WATCH/BLOCK 都有資料
    for sid, data in feature_lookup.items():
        if sid not in out:
            out[sid] = data

    return out

def pick(row, lookup, col, default=""):
    v = row.get(col, default) if hasattr(row, "get") else default
    v = clean_text(v, "")
    if v != "" and v != "0" and v != "0.0":
        return v

    sid = normalize_stock_id(row.get("stock_id", "")) if hasattr(row, "get") else ""
    src = lookup.get(sid, {})
    return clean_text(src.get(col, default), default)

def norm_action(v):
    s = clean_text(v).strip().upper()
    mapping = {
        "買進": "BUY",
        "試單": "TEST",
        "觀察": "WATCH",
        "禁止": "BLOCK",
        "賣出": "SELL",
        "減碼": "REDUCE",
    }
    return mapping.get(s, s)



def load_macro_guard():
    """
    v266.12：總經最高層風控。
    讀 macro_regime.json，決定總經環境：
    RISK_ON：可進攻
    NEUTRAL：中性
    RISK_OFF：降低進攻
    """
    data = {}
    for p in [ROOT / "macro_regime.json", DATA_DIR / "macro_regime.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                data = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    if not data:
        return {
            "macro_regime": "NEUTRAL",
            "macro_label": "總經中性",
            "macro_policy": "總經資料不足，暫用中性模式",
            "macro_score": 0,
            "macro_score_ratio": 0,
            "valid_indicator_count": 0,
            "total_indicator_count": 0,
        }

    return {
        "macro_regime": str(data.get("macro_regime", "NEUTRAL")).upper(),
        "macro_label": str(data.get("macro_label", "總經中性")),
        "macro_policy": str(data.get("macro_policy", "")),
        "macro_score": data.get("macro_score", 0),
        "macro_score_ratio": data.get("macro_score_ratio", 0),
        "valid_indicator_count": data.get("valid_indicator_count", 0),
        "total_indicator_count": data.get("total_indicator_count", 0),
    }

def load_market_guard():
    """
    v266.11 市場濾網：
    優先讀 market_regime.json 的大盤狀態。
    若沒有，再 fallback 到 market_snapshot_summary.json 的流動性市場分數。

    BULL / STRONG：BUY / TEST / WATCH 全開
    NEUTRAL / MID：BUY 降級 TEST
    BEAR / WEAK：BUY / TEST 降級 WATCH
    """
    regime_data = {}
    for p in [ROOT / "market_regime.json", DATA_DIR / "market_regime.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                regime_data = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    if regime_data:
        regime = str(regime_data.get("market_regime", "NEUTRAL")).upper()
        label = str(regime_data.get("market_label", "大盤中性"))
        change_text = str(regime_data.get("index_change_pct_text", ""))
        score = float(regime_data.get("market_score", 50) or 50)

        if regime == "BULL":
            mode = "STRONG"
            guard_label = f"{label} {change_text}：BUY / TEST / WATCH 全開"
        elif regime == "BEAR":
            mode = "WEAK"
            guard_label = f"{label} {change_text}：BUY / TEST 降級 WATCH，只觀察"
        else:
            mode = "MID"
            guard_label = f"{label} {change_text}：BUY 降級 TEST，控制追高"

        return {
            "market_guard_mode": mode,
            "market_guard_score": round(score, 2),
            "market_guard_label": guard_label,
            "market_regime": regime,
            "market_label": label,
            "index_change_pct_text": change_text,
            "market_regime_source": regime_data.get("source", ""),
            "market_regime_method": regime_data.get("method", ""),
        }

    # fallback：沒有 market_regime 時，使用流動性市場分數
    summary = {}
    for p in [ROOT / "market_snapshot_summary.json", DATA_DIR / "market_snapshot_summary.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                summary = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    high = int(float(summary.get("high_liquidity_count", 0) or 0))
    mid = int(float(summary.get("medium_liquidity_count", 0) or 0))
    block = int(float(summary.get("block_liquidity_count", 0) or 0))
    score = high * 1.0 + mid * 0.5 - block * 0.7

    if score >= 300:
        mode = "STRONG"
        label = "市場流動性強：BUY / TEST / WATCH 全開"
        regime = "BULL"
    elif score >= 150:
        mode = "MID"
        label = "市場流動性中性：BUY 降級 TEST"
        regime = "NEUTRAL"
    else:
        mode = "WEAK"
        label = "市場流動性弱：BUY / TEST 降級 WATCH"
        regime = "BEAR"

    return {
        "market_guard_mode": mode,
        "market_guard_score": round(score, 2),
        "market_guard_label": label,
        "market_regime": regime,
        "market_label": label,
        "index_change_pct_text": "",
        "market_regime_source": "market_snapshot_summary",
        "market_regime_method": "liquidity_fallback",
    }

def apply_market_guard(out):
    """
    v266.12：三層風控
    1) 總經 macro：決定大方向
    2) 市場 market：決定當天節奏
    3) 個股 final_action：決定標的

    持倉 EXIT / SELL / REDUCE 不受降級影響，仍優先處理。
    """
    guard = load_market_guard()
    macro = load_macro_guard()

    guard.update(macro)

    if out.empty:
        return out, guard

    out = out.copy()
    market_mode = guard.get("market_guard_mode", "MID")
    market_label = guard.get("market_guard_label", "")
    macro_regime = guard.get("macro_regime", "NEUTRAL")
    macro_label = guard.get("macro_label", "總經中性")
    macro_policy = guard.get("macro_policy", "")

    protected = (
        out["source"].astype(str).str.upper().eq("EXIT")
        | out["final_action"].astype(str).str.upper().isin(["SELL", "REDUCE"])
    )

    strategy_upper = out["strategy_type"].astype(str).str.upper()
    final_upper = out["final_action"].astype(str).str.upper()

    # === 總經層：先決定大方向 ===
    if macro_regime == "RISK_OFF":
        # 總經偏空：ALPHA 不做 BUY；一般 BUY 降 TEST；TEST 降 WATCH
        buy_mask = (~protected) & final_upper.eq("BUY")
        test_mask = (~protected) & final_upper.eq("TEST")

        out.loc[buy_mask, "final_action"] = "TEST"
        out.loc[buy_mask, "priority"] = "3"
        out.loc[test_mask, "final_action"] = "WATCH"
        out.loc[test_mask, "priority"] = "8"
        out.loc[test_mask, "suggested_amount"] = "0"
        out.loc[test_mask, "target_weight"] = "0"

        macro_note = f"{macro_label}：{macro_policy}"
        affected = buy_mask | test_mask
        out.loc[affected, "system_note"] = _v302_append_note(out.loc[affected, "system_note"], macro_note)

    elif macro_regime == "NEUTRAL":
        # 總經中性：ALPHA BUY 降 TEST；CORE 小倉可以保留
        alpha_buy = (~protected) & final_upper.eq("BUY") & strategy_upper.str.contains("ALPHA", na=False)
        out.loc[alpha_buy, "final_action"] = "TEST"
        out.loc[alpha_buy, "priority"] = "3"
        out.loc[alpha_buy, "system_note"] = _v302_append_note(out.loc[alpha_buy, "system_note"], f"{macro_label}：ALPHA 降級 TEST")

    # 重新抓一次 final_action，避免前面已改動
    final_upper = out["final_action"].astype(str).str.upper()

    # === 市場層：再控制當天節奏 ===
    if market_mode == "MID":
        mask = (~protected) & final_upper.eq("BUY")
        out.loc[mask, "final_action"] = "TEST"
        out.loc[mask, "priority"] = "3"
        out.loc[mask, "system_note"] = _v302_append_note(out.loc[mask, "system_note"], market_label)

    elif market_mode == "WEAK":
        mask = (~protected) & final_upper.isin(["BUY", "TEST"])
        out.loc[mask, "final_action"] = "WATCH"
        out.loc[mask, "priority"] = "8"
        out.loc[mask, "suggested_amount"] = "0"
        out.loc[mask, "target_weight"] = "0"
        out.loc[mask, "system_note"] = _v302_append_note(out.loc[mask, "system_note"], market_label)

    return out, guard



def macro_confidence_level(valid_count, total_count):
    try:
        valid = float(valid_count or 0)
        total = float(total_count or 0)
        ratio = valid / total if total > 0 else 0
    except Exception:
        ratio = 0

    if ratio >= 0.70:
        return "HIGH", "高信心", ratio
    if ratio >= 0.40:
        return "MID", "中信心", ratio
    return "LOW", "低信心", ratio


def adjusted_macro_score(raw_score, valid_count, total_count):
    _, _, ratio = macro_confidence_level(valid_count, total_count)
    try:
        return round(float(raw_score or 0) * ratio, 2)
    except Exception:
        return 0.0

def load_macro_regime_for_v26614():
    data = {}
    for p in [ROOT / "macro_regime.json", DATA_DIR / "macro_regime.json"]:
        try:
            if p.exists() and p.stat().st_size > 0:
                data = json.loads(p.read_text(encoding="utf-8"))
                break
        except Exception:
            pass

    regime = str(data.get("macro_regime", "NEUTRAL")).upper()
    label = str(data.get("macro_label", "總經中性"))
    score = float(data.get("macro_score", 0) or 0)
    ratio = float(data.get("macro_score_ratio", 0) or 0)

    valid_count = int(float(data.get("valid_indicator_count", 0) or 0))
    total_count = int(float(data.get("total_indicator_count", 0) or 0))
    unknown_count = int(float(data.get("unknown_count", 0) or 0))
    conf_code, conf_label, conf_ratio = macro_confidence_level(valid_count, total_count)
    adj_score = adjusted_macro_score(score, valid_count, total_count)

    # 低信心時不讓總經過度影響操作，避免 2/7 指標就判成強多
    effective_regime = regime
    effective_label = label
    if conf_code == "LOW":
        effective_regime = "NEUTRAL"
        effective_label = f"{label}（低信心）"

    return {
        "macro_regime": effective_regime,
        "macro_raw_regime": regime,
        "macro_label": effective_label,
        "macro_raw_label": label,
        "macro_score": score,
        "macro_adjusted_score": adj_score,
        "macro_score_ratio": ratio,
        "macro_confidence": conf_code,
        "macro_confidence_label": conf_label,
        "macro_confidence_ratio": round(conf_ratio, 4),
        "macro_policy": data.get("macro_policy", ""),
        "valid_indicator_count": valid_count,
        "total_indicator_count": total_count,
        "unknown_count": unknown_count,
    }


def calc_opportunity_score(row):
    """
    v266.15 機會分數：
    給 TEST / WATCH 清單排序，不改原本策略邏輯。
    """
    def f(x, default=0):
        try:
            if x is None:
                return default
            s = str(x).replace(",", "").replace("億", "").strip()
            if s in ["", "--", "nan", "None", "null"]:
                return default
            return float(s)
        except Exception:
            return default

    action = str(row.get("final_action", "")).upper()
    source = str(row.get("source", "")).upper()
    bucket = str(row.get("bucket", row.get("strategy_type", ""))).upper()
    entry = str(row.get("entry_type", "")).upper()
    note = str(row.get("system_note", ""))
    reason = str(row.get("reason", ""))

    liq = f(row.get("liquidity_score", 0))
    score = f(row.get("score", 0))
    volume = f(row.get("volume", 0))
    turnover = f(row.get("turnover", 0))

    op = 0.0

    # 原始策略分數
    op += score * 0.55

    # 流動性越高越優先
    op += liq * 0.35

    # 成交量 / 成交金額加分，但避免極端放大
    if volume > 0:
        op += min(np.log10(volume + 1) * 4, 25)
    if turnover > 0:
        op += min(np.log10(turnover + 1) * 2, 18)

    # 型態加權
    if "BREAK" in entry or "突破" in entry:
        op += 12
    if "PULLBACK" in entry or "回檔" in entry:
        op += 7
    if "WAIT" in entry or "等待" in entry:
        op += 2

    # 策略層加權
    if "ALPHA" in bucket or "主力" in bucket:
        op += 10
    if "CORE" in bucket or "核心" in bucket:
        op += 6
    if "PRE" in bucket or "預備" in bucket:
        op += 2

    # 只針對進場/觀察類做 TOP 評測；出場類不排名
    if action in ["SELL", "REDUCE"] or source == "EXIT":
        return 0.0

    return round(float(op), 2)


def apply_macro_strength_v26614(out):
    """
    總經只調整攻擊強度：
    RISK_ON：不壓 BUY
    NEUTRAL：BUY 降 TEST
    RISK_OFF：BUY/TEST 降 WATCH
    但 SELL/REDUCE 不受影響。
    """
    macro = load_macro_regime_for_v26614()

    if out.empty:
        return out, macro

    out = out.copy()
    regime = macro.get("macro_regime", "NEUTRAL")
    label = macro.get("macro_label", "總經中性")
    policy = macro.get("macro_policy", "")

    protected = (
        out["source"].astype(str).str.upper().eq("EXIT")
        | out["final_action"].astype(str).str.upper().isin(["SELL", "REDUCE"])
    )

    if regime in ["RISK_OFF", "BEAR", "BAD"]:
        mask = (~protected) & out["final_action"].astype(str).str.upper().isin(["BUY", "TEST"])
        out.loc[mask, "final_action"] = "WATCH"
        out.loc[mask, "priority"] = "8"
        out.loc[mask, "suggested_amount"] = "0"
        out.loc[mask, "target_weight"] = "0"
        out.loc[mask, "system_note"] = _v302_append_note(out.loc[mask, "system_note"], f"{label}：總經偏保守，降級觀察")

    elif regime in ["NEUTRAL", "MID"]:
        mask = (~protected) & out["final_action"].astype(str).str.upper().eq("BUY")
        out.loc[mask, "final_action"] = "TEST"
        out.loc[mask, "priority"] = "3"
        out.loc[mask, "system_note"] = _v302_append_note(out.loc[mask, "system_note"], f"{label}：BUY 降級 TEST，控制追高")

    else:
        # RISK_ON：保留進攻
        pass

    return out, macro



# ===== v302 FINAL MAIN-FORCE HARD GATE / 最後決策主力硬門檻 =====
# 目的：
# - 修正 final_decision_engine 用舊 opportunity_score 把垃圾高流動股重新放回 TEST/WATCH。
# - 不改 UI / pipeline / 持倉出場。
# - 持倉 SELL / REDUCE / WATCH 不動。
# - 只處理新進場 BUY / TEST / WATCH。
# - 不符合「主力痕跡 + 攻擊結構 + 量能啟動 + 未過熱」者，直接 BLOCK。

def _v302_num(df, col, default=0.0):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index, dtype="float64")


def _v302_load_feature_latest():
    for p in [
        ROOT / "feature_panel_daily.csv",
        DATA_DIR / "feature_panel_daily.csv",
        ROOT / "mobile_dashboard_v1" / "data" / "feature_panel_daily.csv",
    ]:
        df = read_csv_any([p])
        if df.empty or "stock_id" not in df.columns:
            continue
        df = df.copy()
        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

        if "date" in df.columns:
            df["_date_key"] = df["date"].astype(str)
            df = df.sort_values("_date_key").drop_duplicates("stock_id", keep="last").drop(columns=["_date_key"], errors="ignore")
        elif "signal_date" in df.columns:
            df["_date_key"] = df["signal_date"].astype(str)
            df = df.sort_values("_date_key").drop_duplicates("stock_id", keep="last").drop(columns=["_date_key"], errors="ignore")
        else:
            df = df.drop_duplicates("stock_id", keep="last")
        return df
    return pd.DataFrame()


def _v302_load_chip_source():
    for p in [
        ROOT / "chip_source_twse.csv",
        DATA_DIR / "chip_source_twse.csv",
        ROOT / "mobile_dashboard_v1" / "data" / "chip_source_twse.csv",
    ]:
        df = read_csv_any([p])
        if df.empty or "stock_id" not in df.columns:
            continue
        df = df.copy()
        df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
        return df.drop_duplicates("stock_id", keep="last")
    return pd.DataFrame()


def _v302_force_object_cols(df, cols):
    """pandas string dtype 安全鎖定，避免 loc 指派 BLOCK / note / tag / 數值補欄時爆 Invalid value for dtype 'str'。"""
    if df is None or df.empty:
        return df
    df = df.astype("object")
    for c in cols:
        if c not in df.columns:
            df[c] = pd.Series([""] * len(df), index=df.index, dtype="object")
        else:
            df[c] = df[c].astype("object").where(df[c].notna(), "")
    return df


def _v302_force_all_object(df):
    """v302.3：整張表鎖成 object，防止 pandas string dtype 在 loc 指派數字/布林時爆炸。"""
    if df is None or df.empty:
        return df
    return df.astype("object")


def _v302_append_note(series, msg):
    """安全附加 system_note，全部轉 object/string，避免 pandas string dtype 報錯。"""
    return (
        series.astype("object")
        .where(series.notna(), "")
        .astype(str)
        .replace(["nan", "None", "null"], "")
        .apply(lambda x: (x + "｜" if x else "") + str(msg))
    )


def apply_final_main_force_gate_v302(out):
    """
    v303.2 主力 Gate 分級修正版：
    TEST = 主力早期佈局 Gate，不要求已經點火。
    IGNITION / EVOLUTION 交給 apply_test_ignition_evolution_v303() 升級。
    """
    if out is None or out.empty:
        return out

    out = out.copy()
    out = _v302_force_all_object(out)
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

    out = _v302_force_object_cols(out, [
        "final_action", "entry_type", "execution_flag", "system_note", "reason",
        "source", "bucket", "strategy_type", "top_opportunity",
        "section_top_opportunity", "opportunity_rank", "section_opportunity_rank",
    ])

    feature = _v302_load_feature_latest()
    chip = _v302_load_chip_source()

    if not feature.empty:
        need = [
            "stock_id", "close", "open", "high", "low", "volume", "turnover", "volume_ratio",
            "mom5", "mom10", "mom20", "mom60",
            "ma5", "ma10", "ma20", "ma60", "ma20_slope",
            "high_20", "high_60", "low_20",
            "ma_converge_pct", "range_20",
            "obv_mom5", "obv_up_count_5", "low_non_down_count_5",
            "vol20", "liquidity_score", "liquidity_level", "liquidity_tag",
        ]
        need = [c for c in need if c in feature.columns]
        out = out.merge(feature[need], on="stock_id", how="left", suffixes=("", "_fx"))
        out = _v302_force_all_object(out)

        for c in ["close", "volume", "turnover", "liquidity_score", "liquidity_level", "liquidity_tag"]:
            fx = c + "_fx"
            if fx in out.columns:
                if c not in out.columns:
                    out[c] = out[fx].astype("object")
                else:
                    out[c] = out[c].astype("object")
                    out[fx] = out[fx].astype("object")
                    empty = out[c].astype(str).str.strip().isin(["", "nan", "None", "null", "0", "0.0"])
                    out.loc[empty, c] = out.loc[empty, fx].astype("object")
                out = out.drop(columns=[fx], errors="ignore")
        out = _v302_force_all_object(out)

    if not chip.empty:
        need = [
            "stock_id", "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
            "inst_buy_days", "inst_valid", "margin_balance_change", "short_balance_change",
        ]
        need = [c for c in need if c in chip.columns]
        out = out.merge(chip[need], on="stock_id", how="left")
        out = _v302_force_all_object(out)

    for c in [
        "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy",
        "inst_buy_days", "inst_valid", "margin_balance_change", "short_balance_change",
    ]:
        if c not in out.columns:
            out[c] = 0

    close = _v302_num(out, "close", 0)
    open_ = _v302_num(out, "open", close)
    high = _v302_num(out, "high", close)
    volume = _v302_num(out, "volume", 0)
    turnover = _v302_num(out, "turnover", close * volume * 1000)
    volume_ratio = _v302_num(out, "volume_ratio", 1)

    mom10 = _v302_num(out, "mom10", 0)
    mom20 = _v302_num(out, "mom20", 0)

    ma5 = _v302_num(out, "ma5", close)
    ma10 = _v302_num(out, "ma10", close)
    ma20 = _v302_num(out, "ma20", close)
    ma60 = _v302_num(out, "ma60", close)
    high60 = _v302_num(out, "high_60", close)

    obv_mom5 = _v302_num(out, "obv_mom5", 0)
    obv_up5 = _v302_num(out, "obv_up_count_5", 0)
    low_hold5 = _v302_num(out, "low_non_down_count_5", 0)

    foreign = _v302_num(out, "foreign_net_buy", 0)
    trust = _v302_num(out, "trust_net_buy", 0)
    inst = _v302_num(out, "inst_net_buy", 0)
    inst_days = _v302_num(out, "inst_buy_days", 0)
    inst_valid = _v302_num(out, "inst_valid", 0)
    liq_score = _v302_num(out, "liquidity_score", 0)

    liq_level = out["liquidity_level"].astype(str).str.upper() if "liquidity_level" in out.columns else pd.Series("", index=out.index)
    final_upper = out["final_action"].astype(str).str.upper()
    source_upper = out["source"].astype(str).str.upper() if "source" in out.columns else pd.Series("", index=out.index)

    protected = source_upper.eq("EXIT") | final_upper.isin(["SELL", "REDUCE"])

    # TEST 流動性門票：避免 500 張內雜訊，但不只收大型高流動股。
    liquidity_gate = (
        (volume >= 800)
        | (turnover >= 25_000_000)
        | (liq_score >= 45)
        | liq_level.isin(["MEDIUM", "HIGH"])
    )

    # TEST 主力早期痕跡：法人開始動，或 OBV/低點墊高做代理。
    chip_trace = (
        (inst_valid >= 1)
        & (
            (inst > 0)
            | (foreign > 0)
            | (trust > 0)
            | (inst_days >= 1)
        )
    )

    obv_trace = (
        (obv_mom5 > 0)
        & (obv_up5 >= 1)
        & (low_hold5 >= 2)
    )

    main_force_trace = chip_trace | obv_trace

    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high60_pos = (close / high60).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    # TEST 結構：只要求「沒壞、開始轉強」，不要求已經攻擊。
    basic_structure = (
        (close >= 12)
        & (close > ma20 * 0.955)
        & (ma5 >= ma10 * 0.955)
        & (ma10 >= ma20 * 0.930)
        & (ma20 >= ma60 * 0.900)
        & (mom10 > -0.020)
        & (mom20 > -0.015)
        & (high60_pos >= 0.78)
    )

    # TEST 量能：微增即可；過熱爆量交給排除。
    early_volume = volume_ratio.between(0.82, 4.80)

    not_overheat = (
        (mom20 <= 0.48)
        & (ma20_gap <= 0.30)
        & (volume_ratio <= 5.8)
        & ~((upper_shadow > 0.095) & (volume_ratio > 1.8))
        & ~((intraday < -0.045) & (volume_ratio > 2.0))
    )

    main_force_gate = liquidity_gate & main_force_trace & basic_structure & early_volume & not_overheat

    # 這兩個是後面升級用，不是 TEST 基本門檻。
    attack_structure = (
        (close > ma20 * 0.985)
        & (ma5 >= ma10 * 0.975)
        & (mom10 > 0.005)
        & (mom20 > 0.025)
        & (high60_pos >= 0.84)
    )

    volume_start = volume_ratio.between(1.03, 4.20)

    score = (
        liquidity_gate.astype(int) * 8
        + chip_trace.astype(int) * 34
        + obv_trace.astype(int) * 26
        + basic_structure.astype(int) * 18
        + early_volume.astype(int) * 10
        + attack_structure.astype(int) * 10
        + volume_start.astype(int) * 8
        + (trust > 0).astype(int) * 10
        + (foreign > 0).astype(int) * 6
        - (~not_overheat).astype(int) * 35
    )

    out["main_force_gate_v302"] = main_force_gate.astype(int)
    out["main_force_score_v302"] = pd.Series(score, index=out.index).round(1)
    out["chip_trace_v302"] = chip_trace.astype(int)
    out["obv_trace_v302"] = obv_trace.astype(int)
    out["attack_structure_v302"] = attack_structure.astype(int)
    out["volume_start_v302"] = volume_start.astype(int)
    out["not_overheat_v302"] = not_overheat.astype(int)

    target = (~protected) & final_upper.isin(["BUY", "TEST", "WATCH", "IGNITION", "EVOLUTION"]) & (~main_force_gate)

    if target.any():
        out.loc[target, "final_action"] = "BLOCK"
        out.loc[target, "priority"] = "9"
        out.loc[target, "allowed"] = "False"
        out.loc[target, "suggested_amount"] = "0"
        out.loc[target, "target_weight"] = "0"
        out.loc[target, "execution_flag"] = "BLOCK"
        out.loc[target, "entry_type"] = "未通過TEST早期Gate"
        out.loc[target, "system_note"] = _v302_append_note(
            out.loc[target, "system_note"],
            "v303.2：未通過TEST早期主力Gate，禁止進入 TEST/IGNITION/EVOLUTION"
        )

    passed = (~protected) & final_upper.isin(["BUY", "TEST", "WATCH", "IGNITION", "EVOLUTION"]) & main_force_gate
    if passed.any():
        out.loc[passed, "system_note"] = _v302_append_note(
            out.loc[passed, "system_note"],
            "v303.2：TEST早期主力Gate通過，交由狀態機判斷是否升級"
        )

    out = _v302_force_object_cols(out, [
        "final_action", "entry_type", "execution_flag", "system_note", "reason",
        "top_opportunity", "section_top_opportunity", "opportunity_rank", "section_opportunity_rank",
    ])
    out = _v302_force_all_object(out)

    return out

def apply_test_ignition_evolution_v303(out):
    """
    v303.3 實戰升級版：
    - TEST：保留 v303.2 的主力早期佈局池，不吸乾。
    - IGNITION：從 TEST 中挑出「剛要點火」標的，不再過度嚴格。
    - EVOLUTION：從 IGNITION 中挑出「主升前夕」標的，仍維持少量精選。
    - 不動 SELL / REDUCE / EXIT。
    """
    if out is None or out.empty:
        return out, pd.DataFrame(), pd.DataFrame()

    out = _v302_force_all_object(out.copy())

    for c in [
        "signal_stage_v303", "signal_stage_rank_v303",
        "ignition_score_v303", "evolution_score_v303", "stage_reason_v303",
    ]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype("object").where(out[c].notna(), "")

    final_upper = out["final_action"].astype(str).str.upper()
    source_upper = out["source"].astype(str).str.upper() if "source" in out.columns else pd.Series("", index=out.index)

    protected = source_upper.eq("EXIT") | final_upper.isin(["SELL", "REDUCE"])

    close = _v302_num(out, "close", 0)
    open_ = _v302_num(out, "open", close)
    high = _v302_num(out, "high", close)
    volume_ratio = _v302_num(out, "volume_ratio", 1)

    mom5 = _v302_num(out, "mom5", 0)
    mom10 = _v302_num(out, "mom10", 0)
    mom20 = _v302_num(out, "mom20", 0)

    ma5 = _v302_num(out, "ma5", close)
    ma10 = _v302_num(out, "ma10", close)
    ma20 = _v302_num(out, "ma20", close)
    ma60 = _v302_num(out, "ma60", close)
    high20 = _v302_num(out, "high_20", close)
    high60 = _v302_num(out, "high_60", close)

    main_gate = _v302_num(out, "main_force_gate_v302", 0) >= 1
    main_score = _v302_num(out, "main_force_score_v302", 0)
    chip_trace = _v302_num(out, "chip_trace_v302", 0) >= 1
    obv_trace = _v302_num(out, "obv_trace_v302", 0) >= 1
    attack_structure = _v302_num(out, "attack_structure_v302", 0) >= 1
    volume_start = _v302_num(out, "volume_start_v302", 0) >= 1
    not_overheat = _v302_num(out, "not_overheat_v302", 0) >= 1

    foreign = _v302_num(out, "foreign_net_buy", 0)
    trust = _v302_num(out, "trust_net_buy", 0)
    inst = _v302_num(out, "inst_net_buy", 0)
    inst_days = _v302_num(out, "inst_buy_days", 0)

    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high20_pos = (close / high20).replace([np.inf, -np.inf], 0).fillna(0)
    high60_pos = (close / high60).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    # TEST 最大機會池：通過主力早期 Gate 者，都先保留 TEST。
    test_pool = (
        (~protected)
        & final_upper.isin(["BUY", "TEST", "WATCH", "IGNITION", "EVOLUTION"])
        & main_gate
        & not_overheat
    )

    # 實戰版 IGNITION：從 TEST 中挑「剛轉強 + 量開始啟動 + 主力痕跡」。
    # 不強迫已突破，只要求開始靠近點火。
    ignition_gate = (
        test_pool
        & (chip_trace | obv_trace)
        & (
            attack_structure
            | (
                (close > ma20 * 0.975)
                & (ma5 >= ma10 * 0.965)
                & (mom10 > -0.002)
                & (mom20 > 0.010)
            )
        )
        & (
            volume_start
            | volume_ratio.between(0.95, 4.30)
        )
        & (mom5 > -0.018)
        & (mom10 > -0.006)
        & (mom20.between(0.010, 0.42))
        & (ma20_gap.between(-0.045, 0.255))
        & (high60_pos >= 0.80)
        & (upper_shadow <= 0.095)
    )

    # 實戰版 EVOLUTION：主升前夕，仍比 IGNITION 嚴格，但不要嚴到永遠 0。
    evolution_gate = (
        ignition_gate
        & (mom10 > 0.010)
        & (mom20.between(0.035, 0.45))
        & (close > ma10 * 0.985)
        & (ma5 >= ma10 * 0.975)
        & (ma10 >= ma20 * 0.955)
        & (volume_ratio.between(1.00, 4.00))
        & (ma20_gap.between(-0.010, 0.270))
        & (high20_pos >= 0.93)
        & (
            (inst > 0)
            | (trust > 0)
            | (foreign > 0)
            | (inst_days >= 1)
            | obv_trace
        )
        & (upper_shadow <= 0.085)
        & (intraday > -0.045)
    )

    # 分數：用來排序與 TOP，不作為硬性唯一 Gate。
    ignition_score = (
        main_score
        + chip_trace.astype(int) * 18
        + obv_trace.astype(int) * 16
        + attack_structure.astype(int) * 14
        + volume_start.astype(int) * 12
        + (trust > 0).astype(int) * 10
        + (foreign > 0).astype(int) * 6
        + (inst > 0).astype(int) * 8
        + (mom10 > 0.010).astype(int) * 8
        + (mom20 > 0.030).astype(int) * 8
        + (close > ma20).astype(int) * 6
        - (ma20_gap > 0.27).astype(int) * 35
        - (volume_ratio > 4.8).astype(int) * 25
        - (upper_shadow > 0.10).astype(int) * 25
    ).round(1)

    evolution_score = (
        ignition_score
        + (mom10 > 0.025).astype(int) * 12
        + (mom20 > 0.060).astype(int) * 12
        + (close > high20 * 0.970).astype(int) * 10
        + (ma5 >= ma10).astype(int) * 8
        + (ma10 >= ma20 * 0.975).astype(int) * 8
        + ((inst > 0) | (trust > 0) | (foreign > 0)).astype(int) * 10
        - (ma20_gap > 0.30).astype(int) * 35
        - (volume_ratio > 4.3).astype(int) * 20
    ).round(1)

    out["ignition_score_v303"] = ignition_score.astype("object")
    out["evolution_score_v303"] = evolution_score.astype("object")

    out.loc[test_pool, "signal_stage_v303"] = "TEST"
    out.loc[test_pool, "stage_reason_v303"] = "主力Gate通過｜最大機會TEST池"

    out.loc[ignition_gate, "signal_stage_v303"] = "IGNITION"
    out.loc[ignition_gate, "stage_reason_v303"] = "TEST升級：起漲點火｜主力痕跡＋轉強＋量能啟動"

    out.loc[evolution_gate, "signal_stage_v303"] = "EVOLUTION"
    out.loc[evolution_gate, "stage_reason_v303"] = "IGNITION升級：主升前夕｜籌碼續強＋量價延續"

    plain_test = test_pool & (~ignition_gate) & (~evolution_gate)

    out.loc[plain_test, "final_action"] = "TEST"
    out.loc[plain_test, "priority"] = "3"
    out.loc[plain_test, "entry_type"] = "最大機會試單"
    out.loc[plain_test, "execution_flag"] = "TEST"
    out.loc[plain_test, "system_note"] = _v302_append_note(
        out.loc[plain_test, "system_note"],
        "v303.3：主力Gate通過，保留於 TEST 最大機會池"
    )

    out.loc[ignition_gate, "final_action"] = "IGNITION"
    out.loc[ignition_gate, "priority"] = "2.5"
    out.loc[ignition_gate, "entry_type"] = "起漲點火"
    out.loc[ignition_gate, "execution_flag"] = "IGNITION"
    out.loc[ignition_gate, "system_note"] = _v302_append_note(
        out.loc[ignition_gate, "system_note"],
        "v303.3：由 TEST 升級 IGNITION，起漲訊號成立"
    )

    out.loc[evolution_gate, "final_action"] = "EVOLUTION"
    out.loc[evolution_gate, "priority"] = "2"
    out.loc[evolution_gate, "entry_type"] = "主升前夕"
    out.loc[evolution_gate, "execution_flag"] = "EVOLUTION"
    out.loc[evolution_gate, "system_note"] = _v302_append_note(
        out.loc[evolution_gate, "system_note"],
        "v303.3：由 IGNITION 升級 EVOLUTION，主升前夕訊號成立"
    )

    # 排名限制：TEST 可多，IGNITION 少量，EVOLUTION 精選。
    for stage, score_col, limit in [
        ("EVOLUTION", "evolution_score_v303", 5),
        ("IGNITION", "ignition_score_v303", 8),
        ("TEST", "main_force_score_v302", 20),
    ]:
        m = out["signal_stage_v303"].astype(str).str.upper().eq(stage)
        if not m.any():
            continue
        idx = (
            out.loc[m]
            .assign(_stage_score=pd.to_numeric(out.loc[m, score_col], errors="coerce").fillna(0))
            .sort_values(["_stage_score", "main_force_score_v302", "stock_id"], ascending=[False, False, True])
            .head(limit)
            .index
        )
        out.loc[idx, "signal_stage_rank_v303"] = [str(i) for i in range(1, len(idx) + 1)]

    ignition_df = out[out["signal_stage_v303"].astype(str).str.upper().eq("IGNITION")].copy()
    evolution_df = out[out["signal_stage_v303"].astype(str).str.upper().eq("EVOLUTION")].copy()

    ignition_df["_rank"] = pd.to_numeric(ignition_df["signal_stage_rank_v303"], errors="coerce").fillna(999)
    evolution_df["_rank"] = pd.to_numeric(evolution_df["signal_stage_rank_v303"], errors="coerce").fillna(999)
    ignition_df = ignition_df.sort_values(["_rank", "ignition_score_v303"], ascending=[True, False]).drop(columns=["_rank"], errors="ignore")
    evolution_df = evolution_df.sort_values(["_rank", "evolution_score_v303"], ascending=[True, False]).drop(columns=["_rank"], errors="ignore")

    out = _v302_force_all_object(out)
    return out, ignition_df, evolution_df

# ===== v304 TEST INTERNAL RANKING / TEST 內部排序強化 =====
def apply_test_internal_rank_v304(out):
    if out is None or out.empty:
        return out

    out = _v302_force_all_object(out.copy())

    for c in ["test_rank_score_v304", "test_rank_v304", "test_top_tag_v304", "test_rank_reason_v304"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype("object").where(out[c].notna(), "")

    final_upper = out["final_action"].astype(str).str.upper()
    stage_upper = out["signal_stage_v303"].astype(str).str.upper() if "signal_stage_v303" in out.columns else pd.Series("", index=out.index)
    test_mask = final_upper.eq("TEST") | stage_upper.eq("TEST")
    if not test_mask.any():
        return out

    close = _v302_num(out, "close", 0)
    high = _v302_num(out, "high", close)
    open_ = _v302_num(out, "open", close)
    volume_ratio = _v302_num(out, "volume_ratio", 1)

    mom5 = _v302_num(out, "mom5", 0)
    mom10 = _v302_num(out, "mom10", 0)
    mom20 = _v302_num(out, "mom20", 0)

    ma5 = _v302_num(out, "ma5", close)
    ma10 = _v302_num(out, "ma10", close)
    ma20 = _v302_num(out, "ma20", close)
    high20 = _v302_num(out, "high_20", close)
    high60 = _v302_num(out, "high_60", close)

    main_score = _v302_num(out, "main_force_score_v302", 0)
    chip_trace = _v302_num(out, "chip_trace_v302", 0) >= 1
    obv_trace = _v302_num(out, "obv_trace_v302", 0) >= 1
    attack_structure = _v302_num(out, "attack_structure_v302", 0) >= 1
    volume_start = _v302_num(out, "volume_start_v302", 0) >= 1
    not_overheat = _v302_num(out, "not_overheat_v302", 0) >= 1

    foreign = _v302_num(out, "foreign_net_buy", 0)
    trust = _v302_num(out, "trust_net_buy", 0)
    inst = _v302_num(out, "inst_net_buy", 0)
    inst_days = _v302_num(out, "inst_buy_days", 0)

    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high20_pos = (close / high20).replace([np.inf, -np.inf], 0).fillna(0)
    high60_pos = (close / high60).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    near_ignition = (
        (close > ma20 * 0.975)
        & (ma5 >= ma10 * 0.965)
        & (mom10 > -0.006)
        & (mom20 > 0.010)
        & (volume_ratio.between(0.95, 4.30))
        & (high60_pos >= 0.80)
        & (upper_shadow <= 0.095)
    )

    compression_to_break = (
        (high20_pos >= 0.90)
        & (high20_pos <= 1.03)
        & (ma20_gap.between(-0.045, 0.255))
        & (upper_shadow <= 0.095)
    )

    chip_strength = (
        (inst > 0).astype(int) * 16
        + (trust > 0).astype(int) * 14
        + (foreign > 0).astype(int) * 8
        + (inst_days >= 1).astype(int) * 6
        + chip_trace.astype(int) * 18
        + obv_trace.astype(int) * 16
    )

    structure_strength = (
        attack_structure.astype(int) * 15
        + near_ignition.astype(int) * 14
        + compression_to_break.astype(int) * 10
        + (close > ma20).astype(int) * 6
        + (ma5 >= ma10).astype(int) * 6
        + (mom5 > -0.01).astype(int) * 4
        + (mom10 > 0.00).astype(int) * 6
        + (mom20 > 0.03).astype(int) * 6
    )

    volume_strength = (
        volume_start.astype(int) * 12
        + volume_ratio.between(1.00, 2.20).astype(int) * 12
        + volume_ratio.between(2.20, 3.50).astype(int) * 7
        - (volume_ratio > 4.80).astype(int) * 25
    )

    risk_penalty = (
        (ma20_gap > 0.28).astype(int) * 30
        + (mom20 > 0.48).astype(int) * 30
        + (upper_shadow > 0.10).astype(int) * 25
        + ((intraday < -0.045) & (volume_ratio > 2.0)).astype(int) * 25
        + (~not_overheat).astype(int) * 35
    )

    test_rank_score = (main_score * 0.55 + chip_strength + structure_strength + volume_strength - risk_penalty).round(1)

    out.loc[test_mask, "test_rank_score_v304"] = test_rank_score.loc[test_mask].astype("object")
    out.loc[test_mask, "test_rank_reason_v304"] = "TEST排序：主力痕跡＋剛轉強＋接近點火＋未過熱"

    ranked = (
        out.loc[test_mask]
        .assign(_test_rank_score=pd.to_numeric(out.loc[test_mask, "test_rank_score_v304"], errors="coerce").fillna(0))
        .sort_values(["_test_rank_score", "main_force_score_v302", "stock_id"], ascending=[False, False, True])
    )

    if not ranked.empty:
        ordered_idx = ranked.index.tolist()
        out.loc[ordered_idx, "test_rank_v304"] = [str(i) for i in range(1, len(ordered_idx) + 1)]
        top5_idx = ordered_idx[:5]
        top10_idx = ordered_idx[5:10]

        if top5_idx:
            out.loc[top5_idx, "test_top_tag_v304"] = "🔥TOP5"
            out.loc[top5_idx, "top_opportunity"] = "🔥TEST TOP5"
            out.loc[top5_idx, "section_top_opportunity"] = "🔥TEST TOP5"
            out.loc[top5_idx, "system_note"] = _v302_append_note(
                out.loc[top5_idx, "system_note"],
                "v304：TEST TOP5，最接近 IGNITION 的最大機會"
            )

        if top10_idx:
            out.loc[top10_idx, "test_top_tag_v304"] = "TOP10"

    out = _v302_force_all_object(out)
    return out

def apply_top_opportunities_v26614(out):
    """
    v266.15.2：
    1. 全清單 TOP5：top_opportunity / opportunity_rank
    2. 分區 TOP5：section_top_opportunity / section_opportunity_rank
       - TEST 前5
       - WATCH 前5
       - BUY 前5
    """
    if out.empty:
        return out, pd.DataFrame()

    out = out.copy()
    out = _v302_force_all_object(out)

    out["opportunity_score"] = out.apply(calc_opportunity_score, axis=1)
    out = _v302_force_all_object(out)

    # v302.1：TOP 欄位用 object，不用 pandas string dtype。
    for _c in ["opportunity_rank", "top_opportunity", "section_opportunity_rank", "section_top_opportunity"]:
        out[_c] = pd.Series([""] * len(out), index=out.index, dtype="object")

    base_mask = (
        out["final_action"].astype(str).str.upper().isin(["TEST", "WATCH", "BUY"])
        & (pd.to_numeric(out["opportunity_score"], errors="coerce").fillna(0) > 0)
    )

    candidates = out[base_mask].copy()
    if candidates.empty:
        return out, candidates

    # 全清單 TOP5
    candidates["_op"] = pd.to_numeric(candidates["opportunity_score"], errors="coerce").fillna(0)
    overall = candidates.sort_values(["_op", "score"], ascending=[False, False]).head(5).copy()
    overall_ids = [str(x) for x in overall["stock_id"].tolist()]

    for rank, sid in enumerate(overall_ids, start=1):
        mask = out["stock_id"].astype(str).eq(str(sid))
        out.loc[mask, "opportunity_rank"] = str(rank)
        out.loc[mask, "top_opportunity"] = f"TOP{rank}"
        out.loc[mask, "system_note"] = _v302_append_note(
            out.loc[mask, "system_note"],
            f"全清單 TOP{rank}：優先觀察發動機會"
        )

    # 分區 TOP5：讓 WATCH / TEST 各自有 TOP
    for action_name, label in [("BUY", "買進"), ("TEST", "試單"), ("WATCH", "觀察")]:
        part = candidates[candidates["final_action"].astype(str).str.upper().eq(action_name)].copy()
        if part.empty:
            continue

        part = part.sort_values(["_op", "score"], ascending=[False, False]).head(5).copy()
        for rank, sid in enumerate([str(x) for x in part["stock_id"].tolist()], start=1):
            mask = out["stock_id"].astype(str).eq(str(sid))
            out.loc[mask, "section_opportunity_rank"] = str(rank)
            out.loc[mask, "section_top_opportunity"] = f"{label}TOP{rank}"
            out.loc[mask, "system_note"] = _v302_append_note(
                out.loc[mask, "system_note"],
                f"{label}清單 TOP{rank}：本區最可能發動"
            )

    top_df = out[
        (out["top_opportunity"].astype(str).str.strip() != "")
        | (out["section_top_opportunity"].astype(str).str.strip() != "")
    ].copy()

    top_df["_rank"] = pd.to_numeric(top_df["opportunity_rank"], errors="coerce").fillna(999)
    top_df["_section_rank"] = pd.to_numeric(top_df["section_opportunity_rank"], errors="coerce").fillna(999)
    top_df = top_df.sort_values(["_rank", "_section_rank"]).drop(columns=["_rank", "_section_rank"], errors="ignore")

    return out, top_df




# ===== v307.4 FINAL DECISION ATTACK SORT PATCH =====
# 最終決策層修正：
# 1. TEST / WATCH 排序改用「攻擊結構」而不是單純 liquidity / opportunity_score。
# 2. 橫盤、牛皮、金融、弱攻擊 TEST 降回 WATCH。
# 3. TOP5 重新用攻擊結構產生。
# 4. 不動 app.js、不動 UI、不動持倉、不動 workflow。
def apply_final_attack_sort_v3074(out):
    import numpy as np
    import pandas as pd

    if out is None or out.empty:
        return out, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    d = out.copy()

    def _num(col, default=0.0):
        if col in d.columns:
            return pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=d.index, dtype="float64")

    def _text(col, default=""):
        if col in d.columns:
            return d[col].astype(str).fillna(default)
        return pd.Series(default, index=d.index, dtype="object")

    sid = _text("stock_id")
    finance = sid.str.startswith(("28", "58"))

    base = _num("opportunity_score")
    if float(base.abs().sum()) == 0:
        base = _num("score")

    liq = _num("liquidity_score")
    chip = _num("chip_score")
    main_force = _num("main_force_score_v302")
    ignition_old = _num("ignition_score_v303")
    evolution_old = _num("evolution_score_v303")
    test_rank_old = _num("test_rank_score_v304")
    volume = _num("volume")
    turnover = _num("turnover")
    close = _num("close")

    # 從現有 final_decision 可用欄位重建攻擊分數。
    # 注意：這裡不能依賴 ma5_slope/mom20，因為 final_decision 輸出欄位原本沒有帶那些細欄位。
    attack = pd.Series(0.0, index=d.index)

    # 主要攻擊訊號來源：v302/v303/v304 已經存在的結構欄位
    attack += main_force * 0.38
    attack += ignition_old * 0.32
    attack += evolution_old * 0.42
    attack += test_rank_old * 0.28
    attack += base * 0.22

    # 輔助：籌碼與流動性只能輔助，不可主導
    attack += (chip >= 60).astype(int) * 6
    attack += (chip >= 80).astype(int) * 8
    attack += (liq >= 70).astype(int) * 3

    # 成交量/成交金額基本門檻：太冷不能上前排
    attack += ((volume >= 1000) | (turnover >= 30_000_000)).astype(int) * 5

    # 文字結構判斷：利用已有 reason / system_note / entry_type
    txt = (
        _text("reason") + " " +
        _text("system_note") + " " +
        _text("entry_type") + " " +
        _text("bucket") + " " +
        _text("strategy_type")
    )

    strong_words = txt.str.contains("發動|起漲|點火|突破|強勢|主力|攻擊|轉強|放量|吸籌|卡位", regex=True, na=False)
    flat_words = txt.str.contains("橫盤|牛皮|整理過久|無方向|量縮無攻擊|攻擊不足|弱攻擊", regex=True, na=False)
    risky_words = txt.str.contains("過熱|追高|假突破|上影|誘多|轉弱", regex=True, na=False)

    attack += strong_words.astype(int) * 14
    attack -= flat_words.astype(int) * 26
    attack -= risky_words.astype(int) * 16

    # 金融直接重扣，避免金融牛皮靠穩定/流動性上前排
    attack -= finance.astype(int) * 70

    # 高流動但沒有主力/起漲/進化分數，不應該在 TEST 前排
    liquidity_only = (
        (liq >= 70) &
        (main_force < 45) &
        (ignition_old < 35) &
        (evolution_old < 35) &
        (test_rank_old < 35) &
        (~strong_words)
    )
    attack -= liquidity_only.astype(int) * 35

    d["final_attack_score_v3074"] = attack.round(2)
    d["final_sort_score_v3074"] = (attack * 0.70 + base * 0.30).round(2)
    d["liquidity_only_penalty_v3074"] = liquidity_only.astype(int)

    action = _text("final_action").str.upper()

    # TEST 若只是高流動、金融、或攻擊分數不足，降回 WATCH
    downgrade = action.eq("TEST") & (
        finance |
        liquidity_only |
        (d["final_attack_score_v3074"] < 42)
    )

    d.loc[downgrade, "final_action"] = "WATCH"
    d.loc[downgrade, "entry_type"] = "攻擊結構不足，降回觀察"
    d.loc[downgrade, "system_note"] = d.loc[downgrade, "system_note"].astype(str) + "｜v307.4：非攻擊型 TEST，降回 WATCH"

    # 清掉舊 TOP，避免舊 opportunity_score / liquidity TOP 污染
    for c in ["opportunity_rank", "top_opportunity", "section_opportunity_rank", "section_top_opportunity", "execution_flag"]:
        if c not in d.columns:
            d[c] = ""
        d[c] = ""

    # 重新給 TOP5：同區內用 final_attack_score 排
    action = d["final_action"].astype(str).str.upper()
    for action_name, label in [("BUY", "買進"), ("TEST", "試單"), ("WATCH", "觀察")]:
        part_mask = action.eq(action_name) & (~finance)
        # TEST/BUY 要求攻擊分數更高；WATCH 可較低但仍不能是金融
        min_score = 42 if action_name in ["BUY", "TEST"] else 35
        idx = (
            d.loc[part_mask & (d["final_attack_score_v3074"] >= min_score)]
            .sort_values(
                ["final_attack_score_v3074", "final_sort_score_v3074", "score", "stock_id"],
                ascending=[False, False, False, True]
            )
            .head(5)
            .index
        )

        for rank, ridx in enumerate(idx, start=1):
            d.loc[ridx, "execution_flag"] = "TOP"
            d.loc[ridx, "opportunity_rank"] = str(rank)
            d.loc[ridx, "top_opportunity"] = f"TOP{rank}"
            d.loc[ridx, "section_opportunity_rank"] = str(rank)
            d.loc[ridx, "section_top_opportunity"] = f"{label}TOP{rank}"
            d.loc[ridx, "system_note"] = str(d.loc[ridx, "system_note"]) + f"｜v307.4：攻擊結構{label}TOP{rank}"

    # IGNITION / EVOLUTION 升級清單：不改 final_action，只產生 signal_stage / signal df
    if "signal_stage_v303" not in d.columns:
        d["signal_stage_v303"] = ""

    ignition_mask = (
        d["final_action"].astype(str).str.upper().isin(["TEST", "WATCH", "BUY"]) &
        (d["final_attack_score_v3074"] >= 58) &
        (~finance) &
        (~liquidity_only)
    )
    evolution_mask = (
        d["final_action"].astype(str).str.upper().isin(["TEST", "BUY"]) &
        (d["final_attack_score_v3074"] >= 72) &
        (~finance) &
        (~liquidity_only)
    )

    d.loc[ignition_mask, "signal_stage_v303"] = "IGNITION"
    d.loc[evolution_mask, "signal_stage_v303"] = "EVOLUTION"

    if "stage_reason_v303" in d.columns:
        d.loc[ignition_mask, "stage_reason_v303"] = "v307.4：攻擊結構達 IGNITION"
        d.loc[evolution_mask, "stage_reason_v303"] = "v307.4：攻擊結構達 EVOLUTION"

    # 最終排序：priority → TOP rank → attack
    priority = pd.to_numeric(d.get("priority", 9), errors="coerce").fillna(9)
    top_rank = pd.to_numeric(d["section_opportunity_rank"], errors="coerce").fillna(999)
    d["_priority_v3074"] = priority
    d["_top_rank_v3074"] = top_rank

    d = d.sort_values(
        ["_priority_v3074", "_top_rank_v3074", "final_attack_score_v3074", "final_sort_score_v3074", "stock_id"],
        ascending=[True, True, False, False, True]
    ).drop(columns=["_priority_v3074", "_top_rank_v3074"], errors="ignore")

    top_df = d[
        (d["top_opportunity"].astype(str).str.strip() != "") |
        (d["section_top_opportunity"].astype(str).str.strip() != "")
    ].copy()

    ignition_df = d[d["signal_stage_v303"].astype(str).str.upper().eq("IGNITION")].copy()
    evolution_df = d[d["signal_stage_v303"].astype(str).str.upper().eq("EVOLUTION")].copy()

    return d, top_df, ignition_df, evolution_df




# ===== v309 FINAL DECISION ATTACK-FIRST PATCH / 最終決策攻擊排序 =====
# 這段是最後一道門：不管前面哪裡給了舊 TOP，這裡都會重算、清掉舊 TOP、重新排序。
def apply_final_attack_sort_v309(out):
    import numpy as np
    import pandas as pd

    if out is None or out.empty:
        return out, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    d = out.copy()

    def n(col, default=0.0):
        if col in d.columns:
            return pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)
        return pd.Series(default, index=d.index, dtype="float64")

    def t(col, default=""):
        if col in d.columns:
            return d[col].astype(str).fillna(default)
        return pd.Series(default, index=d.index, dtype="object")

    sid = t("stock_id")
    finance = sid.str.startswith(("28", "58"))

    base = n("score")
    opportunity = n("opportunity_score")
    liq = n("liquidity_score")
    chip = n("chip_score")
    main_force = n("main_force_score_v302")
    ignition = n("ignition_score_v303")
    evolution = n("evolution_score_v303")
    test_rank = n("test_rank_score_v304")
    upstream_attack = n("final_attack_score_v309")
    upstream_sort = n("final_sort_score_v309")
    upstream_reject = n("hard_reject_v309")
    volume = n("volume")
    turnover = n("turnover")

    text_all = t("reason") + " " + t("system_note") + " " + t("entry_type") + " " + t("bucket") + " " + t("strategy_type")
    strong = text_all.str.contains("攻擊|突破|發動|起漲|點火|轉強|放量|多頭|主力|強勢", regex=True, na=False)
    weak = text_all.str.contains("轉弱|跌破|攻擊結構不足|弱攻擊|橫盤|牛皮|箱型|高流動性觀察|低信心", regex=True, na=False)
    fake = text_all.str.contains("假突破|誘多|過熱|上影|追高|整理過久", regex=True, na=False)

    attack = pd.Series(0.0, index=d.index)
    # upstream strategy attack score has priority when present
    attack += upstream_attack * 0.85
    attack += upstream_sort * 0.10
    # final-decision internal scores are only secondary
    attack += test_rank * 0.30
    attack += ignition * 0.28
    attack += evolution * 0.32
    attack += main_force * 0.20
    attack += opportunity * 0.15
    attack += chip.clip(lower=0, upper=100) * 0.05
    attack += ((volume >= 1000) | (turnover >= 30_000_000)).astype(int) * 4
    attack += strong.astype(int) * 12
    attack -= weak.astype(int) * 36
    attack -= fake.astype(int) * 26
    attack -= finance.astype(int) * 120
    attack -= upstream_reject.astype(int) * 120

    liquidity_only = (liq >= 65) & (upstream_attack < 58) & (main_force < 50) & (ignition < 45) & (test_rank < 45) & (~strong)
    attack -= liquidity_only.astype(int) * 50

    hard_reject = finance | (upstream_reject.astype(int).eq(1)) | liquidity_only | weak | fake

    d["final_attack_score_final_v309"] = attack.round(2)
    d["final_sort_score_final_v309"] = (attack * 0.78 + base * 0.12 + opportunity * 0.10).round(2)
    d["hard_reject_final_v309"] = hard_reject.astype(int)

    for c in ["top_opportunity", "opportunity_rank", "section_top_opportunity", "section_opportunity_rank", "execution_flag"]:
        if c not in d.columns:
            d[c] = ""
        d[c] = ""

    action = t("final_action").str.upper()
    # Hard reject cannot remain BUY/TEST. This is the key fix.
    to_watch = action.isin(["BUY", "TEST"]) & (hard_reject | (d["final_attack_score_final_v309"] < 58))
    d.loc[to_watch, "final_action"] = "WATCH"
    d.loc[to_watch, "priority"] = 8
    d.loc[to_watch, "entry_type"] = "攻擊結構不足，降回觀察"
    d.loc[to_watch, "system_note"] = d.loc[to_watch, "system_note"].astype(str) + "｜v309：最終攻擊排序未達門檻，取消舊TOP/降回WATCH"

    # Rebuild TOP only from valid attack pool.
    action = d["final_action"].astype(str).str.upper()
    for action_name, label, min_score in [("BUY", "買進", 70), ("TEST", "試單", 66), ("WATCH", "觀察", 62)]:
        mask = action.eq(action_name) & (~hard_reject) & (d["final_attack_score_final_v309"] >= min_score)
        idx = (
            d.loc[mask]
            .sort_values(["final_attack_score_final_v309", "final_sort_score_final_v309", "stock_id"], ascending=[False, False, True])
            .head(5)
            .index
        )
        for rank, ridx in enumerate(idx, start=1):
            d.loc[ridx, "execution_flag"] = "TOP"
            d.loc[ridx, "opportunity_rank"] = str(rank)
            d.loc[ridx, "top_opportunity"] = f"TOP{rank}"
            d.loc[ridx, "section_opportunity_rank"] = str(rank)
            d.loc[ridx, "section_top_opportunity"] = f"{label}TOP{rank}"
            d.loc[ridx, "system_note"] = str(d.loc[ridx, "system_note"]) + f"｜v309：攻擊結構{label}TOP{rank}"

    # Rebuild signal dfs after final top/attack calculation.
    if "signal_stage_v303" not in d.columns:
        d["signal_stage_v303"] = ""
    ignition_mask = (d["final_attack_score_final_v309"] >= 70) & (~hard_reject) & action.isin(["WATCH", "TEST", "BUY"])
    evolution_mask = (d["final_attack_score_final_v309"] >= 82) & (~hard_reject) & action.isin(["TEST", "BUY"])
    d.loc[ignition_mask, "signal_stage_v303"] = "IGNITION"
    d.loc[evolution_mask, "signal_stage_v303"] = "EVOLUTION"

    priority = pd.to_numeric(d.get("priority", 9), errors="coerce").fillna(9)
    top_rank = pd.to_numeric(d["section_opportunity_rank"], errors="coerce").fillna(999)
    d["_p_v309"] = priority
    d["_top_v309"] = top_rank
    d = d.sort_values(
        ["_p_v309", "_top_v309", "final_attack_score_final_v309", "final_sort_score_final_v309", "stock_id"],
        ascending=[True, True, False, False, True]
    ).drop(columns=["_p_v309", "_top_v309"], errors="ignore")

    top_df = d[(d["top_opportunity"].astype(str).str.strip() != "") | (d["section_top_opportunity"].astype(str).str.strip() != "")].copy()
    ignition_df = d[d["signal_stage_v303"].astype(str).str.upper().eq("IGNITION")].copy()
    evolution_df = d[d["signal_stage_v303"].astype(str).str.upper().eq("EVOLUTION")].copy()
    return d, top_df, ignition_df, evolution_df

def main():
    generated_at = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    lookup = make_lookup()

    trading = read_csv_any([
        ROOT / "trading_system_plan.csv",
        DATA_DIR / "trading_system_plan.csv",
        ROOT / "trade_plan.csv",
        DATA_DIR / "trade_plan.csv",
    ])

    # v266.32D：日期權威來源固定看 trade_plan.csv。
    # trade_plan.csv 已由策略引擎正確輸出 signal_date / trade_date，
    # final_action_plan.csv 若來源列沒有日期，統一用這裡回填。
    trade_plan_date_src = read_csv_any([
        ROOT / "trade_plan.csv",
        DATA_DIR / "trade_plan.csv",
        ROOT / "mobile_dashboard_v1" / "data" / "trade_plan.csv",
    ])

    exitp = read_csv_any([ROOT / "exit_risk_plan.csv", DATA_DIR / "exit_risk_plan.csv"])

    # v266.32D：統一抓訊號日/交易日來源，優先順序：
    # 1. trade_plan.csv
    # 2. trading_system_plan.csv / trade_plan fallback
    # 3. exit_risk_plan.csv
    fallback_signal_date = ""
    fallback_trade_date = ""
    for src_df in [trade_plan_date_src, trading, exitp]:
        if src_df is not None and not src_df.empty:
            if "trade_date" in src_df.columns:
                tvals = src_df["trade_date"].dropna().astype(str)
                if len(tvals) > 0:
                    fallback_trade_date = _date_text(tvals.iloc[0])

            for c in ["signal_date", "date", "asof_date", "run_date", "generated_date"]:
                if c in src_df.columns:
                    vals = src_df[c].dropna().astype(str)
                    if len(vals) > 0:
                        fallback_signal_date = _date_text(vals.iloc[0])
                        if fallback_signal_date:
                            break
            if fallback_signal_date:
                break

    if fallback_signal_date and not fallback_trade_date:
        fallback_trade_date = next_tw_trading_day(fallback_signal_date)

    rows = []
    holding_ids = set()

    # 持倉優先
    if not exitp.empty and "stock_id" in exitp.columns:
        exitp["stock_id"] = exitp["stock_id"].apply(normalize_stock_id)
        holding_ids = set(exitp["stock_id"])

        for _, r in exitp.iterrows():
            raw_action = norm_action(r.get("exit_action", ""))

            if raw_action == "SELL":
                final_action, priority, allowed, note = "SELL", 0, True, "持倉風控：必須優先處理出場"
            elif raw_action == "REDUCE":
                final_action, priority, allowed, note = "REDUCE", 1, True, "持倉風控：建議降倉控風險"
            elif raw_action in ["HOLD", "WATCH"]:
                final_action, priority, allowed, note = "WATCH", 7, False, "持倉觀察：目前不新增、不出場"
            else:
                continue

            sid = normalize_stock_id(r.get("stock_id", ""))
            reason_parts = []

            er = clean_text(r.get("exit_reason", ""))
            if er:
                reason_parts.append(er)

            u = pct_text(r.get("unrealized_pct", ""))
            if u:
                reason_parts.append(f"損益 {u}")

            avg = clean_text(r.get("avg_cost", ""))
            if avg:
                reason_parts.append(f"均價 {avg}")

            lots = clean_text(r.get("lots", ""))
            if lots:
                reason_parts.append(f"張數 {lots}")

            signal_date = pick_signal_date(r, fallback_signal_date)
            trade_date = next_tw_trading_day(signal_date)

            rows.append({
                "final_action": final_action,
                "signal_date": signal_date,
                "trade_date": trade_date,
                "stock_id": sid,
                "stock_name": pick({"stock_id": sid}, lookup, "stock_name", ""),
                "source": "EXIT",
                "bucket": "POSITION",
                "strategy_type": "POSITION",
                "score": clean_text(r.get("exit_priority", 0)),
                "entry_type": raw_action,
                "execution_flag": raw_action,
                "allowed": allowed,
                "close": clean_text(r.get("close", pick({"stock_id": sid}, lookup, "close", ""))),
                "suggested_amount": clean_text(r.get("position_value", "")),
                "target_weight": "",
                "priority": priority,
                "reason": " | ".join(reason_parts),
                "system_note": f"{note}｜風險 {clean_text(r.get('risk_level', ''))}",
                "liquidity_level": pick({"stock_id": sid}, lookup, "liquidity_level", ""),
                "liquidity_tag": pick({"stock_id": sid}, lookup, "liquidity_tag", ""),
                "liquidity_score": pick({"stock_id": sid}, lookup, "liquidity_score", ""),
                "volume": pick({"stock_id": sid}, lookup, "volume", ""),
                "turnover": pick({"stock_id": sid}, lookup, "turnover", ""),
            })

    # 進場/觀察/禁止
    if not trading.empty and "stock_id" in trading.columns:
        trading["stock_id"] = trading["stock_id"].apply(normalize_stock_id)

        for _, r in trading.iterrows():
            sid = normalize_stock_id(r.get("stock_id", ""))
            if not sid or sid in holding_ids:
                continue

            raw_action = norm_action(r.get("action", r.get("final_action", "")))
            allowed = is_true(r.get("allowed", True))
            strategy_type = pick(r, lookup, "strategy_type", pick(r, lookup, "bucket", ""))
            bucket = pick(r, lookup, "bucket", strategy_type)
            liq = pick(r, lookup, "liquidity_level", "").upper()

            if raw_action in ["BUY", "TEST", "WATCH", "BLOCK"]:
                final_action = raw_action
            else:
                flag = norm_action(r.get("execution_flag", ""))
                if allowed and flag == "TOP":
                    final_action = "BUY" if str(strategy_type).upper() == "ALPHA" else "TEST"
                elif flag == "WATCH":
                    final_action = "WATCH"
                else:
                    final_action = "BLOCK"

            # 實戰保護：流動性不足不可 BUY
            if final_action == "BUY" and liq in ["LOW", "BLOCK", ""]:
                final_action = "TEST" if liq == "LOW" else "BLOCK"

            priority = {"SELL": 0, "REDUCE": 1, "BUY": 2, "TEST": 3, "WATCH": 8, "BLOCK": 9}.get(final_action, 9)

            signal_date = pick_signal_date(r, fallback_signal_date)
            trade_date = next_tw_trading_day(signal_date)

            rows.append({
                "final_action": final_action,
                "signal_date": signal_date,
                "trade_date": trade_date,
                "stock_id": sid,
                "stock_name": pick(r, lookup, "stock_name", ""),
                "source": pick(r, lookup, "source", "ENTRY"),
                "bucket": bucket,
                "strategy_type": strategy_type,
                "score": pick(r, lookup, "score", pick(r, lookup, "entry_score", "")),
                "entry_type": pick(r, lookup, "action_sub", r.get("entry_type", "")),
                "execution_flag": pick(r, lookup, "execution_flag", raw_action),
                "allowed": allowed,
                "close": pick(r, lookup, "close", pick(r, lookup, "ref_price", "")),
                "suggested_amount": pick(r, lookup, "suggested_amount", ""),
                "target_weight": pick(r, lookup, "target_weight", ""),
                "priority": priority,
                "reason": pick(r, lookup, "reason", pick(r, lookup, "note", "")),
                "system_note": pick(r, lookup, "system_note", pick(r, lookup, "note", "")),
                "liquidity_level": pick(r, lookup, "liquidity_level", ""),
                "liquidity_tag": pick(r, lookup, "liquidity_tag", ""),
                "liquidity_score": pick(r, lookup, "liquidity_score", ""),
                "volume": pick(r, lookup, "volume", ""),
                "turnover": pick(r, lookup, "turnover", ""),
                "attack_score_v309": pick(r, lookup, "attack_score_v309", ""),
                "final_attack_score_v309": pick(r, lookup, "final_attack_score_v309", ""),
                "final_sort_score_v309": pick(r, lookup, "final_sort_score_v309", ""),
                "hard_reject_v309": pick(r, lookup, "hard_reject_v309", ""),
                "short_turn_weak_v309": pick(r, lookup, "short_turn_weak_v309", ""),
                "ma_sticky_no_attack_v309": pick(r, lookup, "ma_sticky_no_attack_v309", ""),
                "box_middle_v309": pick(r, lookup, "box_middle_v309", ""),
                "liquidity_only_v309": pick(r, lookup, "liquidity_only_v309", ""),
            })

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    out = _v302_force_all_object(out)

    if not out.empty:
        out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

        # v302.2：在 market_guard / macro / top / hard_gate 之前先鎖定所有會被改的欄位型別。
        # 這是本次修正的核心，避免 pandas string dtype 在後面 loc 指派 0/False/BLOCK 時爆炸。
        out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

        # v266.32D：最後保險，日期永遠優先由 trade_plan.csv 回填。
        if "signal_date" not in out.columns:
            out["signal_date"] = fallback_signal_date
        if "trade_date" not in out.columns:
            out["trade_date"] = fallback_trade_date

        out["signal_date"] = out["signal_date"].apply(lambda x: _date_text(x) or fallback_signal_date)
        out["trade_date"] = out["trade_date"].apply(lambda x: _date_text(x) or fallback_trade_date)
        out["trade_date"] = out.apply(
            lambda r: _date_text(r.get("trade_date", "")) or next_tw_trading_day(r.get("signal_date", "")),
            axis=1
        )

        # v266.10.1：最後保險補股票名稱
        # 來源順序：market_snapshot.csv → stock_basic_tw_full.csv → stock_basic.csv
        name_maps = []
        for name_file in ["market_snapshot.csv", "stock_basic_tw_full.csv", "stock_basic.csv"]:
            df_name = read_csv_any([ROOT / name_file, DATA_DIR / name_file])
            if df_name.empty or "stock_id" not in df_name.columns:
                continue

            df_name = df_name.copy()

            if "stock_name" not in df_name.columns:
                for alt in ["name", "證券名稱", "股票名稱", "公司名稱"]:
                    if alt in df_name.columns:
                        df_name["stock_name"] = df_name[alt]
                        break

            if "stock_name" not in df_name.columns:
                continue

            df_name["stock_id"] = df_name["stock_id"].apply(normalize_stock_id)
            df_name["stock_name"] = df_name["stock_name"].astype(str).replace(["nan", "None", "null"], "")
            df_name = df_name[df_name["stock_name"].astype(str).str.strip() != ""]
            if not df_name.empty:
                name_maps.append(
                    df_name[["stock_id", "stock_name"]]
                    .drop_duplicates("stock_id", keep="first")
                    .set_index("stock_id")["stock_name"]
                    .to_dict()
                )

        def fill_stock_name(row):
            cur = clean_text(row.get("stock_name", ""), "")
            if cur not in ["", "--"]:
                return cur
            sid = normalize_stock_id(row.get("stock_id", ""))
            for mp in name_maps:
                v = clean_text(mp.get(sid, ""), "")
                if v not in ["", "--"]:
                    return v
            return ""

        out["stock_name"] = out.apply(fill_stock_name, axis=1)

        # v302.2：名稱補完後再次鎖欄位，因為前面的 apply/merge 可能讓 dtype 回到 string。
        out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

        out, market_guard = apply_market_guard(out)
        out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

        # v266.15：總經攻擊強度
        out, macro_guard = apply_macro_strength_v26614(out)
        out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

        # v302：最後決策主力硬門檻。這一步在 TOP5 前執行，避免舊 opportunity_score 把垃圾重新標回 TEST/WATCH。
        out = apply_final_main_force_gate_v302(out)
        out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

        # v303：TEST → IGNITION → EVOLUTION 狀態升級。
        # 最大機會先在 TEST；起漲確認進 IGNITION；主升前夕進 EVOLUTION。
        out, ignition_signal_df, evolution_signal_df = apply_test_ignition_evolution_v303(out)
        out = _v302_force_all_object(out)

        # v304：TEST 內部排序強化，只排序 TEST，不動 Gate / IGNITION / EVOLUTION。
        out = apply_test_internal_rank_v304(out)
        out = _v302_force_all_object(out)

        # TOP5 機會評測只允許在通過 v302/v303/v304 後的有效池內產生。
        out, top_opportunity_df = apply_top_opportunities_v26614(out)
        out = _v302_force_all_object(out)

        out["_score_num"] = pd.to_numeric(out["score"], errors="coerce").fillna(0)
        out["_priority_num"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9)
        out["_op_num"] = pd.to_numeric(out["opportunity_score"], errors="coerce").fillna(0)
        out = out.sort_values(["_priority_num", "_op_num", "_score_num", "stock_id"], ascending=[True, False, False, True])
        out = out.drop(columns=["_score_num", "_priority_num", "_op_num"])

    if "market_guard" not in locals():
        market_guard = load_market_guard()
    if "macro_guard" not in locals():
        macro_guard = load_macro_regime_for_v26614()

    if "ignition_signal_df" not in locals():
        ignition_signal_df = pd.DataFrame()
    if "evolution_signal_df" not in locals():
        evolution_signal_df = pd.DataFrame()

    if "top_opportunity_df" not in locals():
        out, ignition_signal_df, evolution_signal_df = apply_test_ignition_evolution_v303(out)
        out = apply_test_internal_rank_v304(out)
        out, top_opportunity_df = apply_top_opportunities_v26614(out)

    out = add_chip_columns(out)

    # v302.2：籌碼欄位合併後再次鎖定文字欄位型別，避免後續輸出/摘要前 dtype 衝突。
    out = _v302_force_object_cols(out, V302_MUTABLE_COLS)

    # v266.32D：籌碼欄位合併後再次保險，避免日期欄位遺失或被覆蓋。
    if not out.empty:
        if "signal_date" not in out.columns:
            out["signal_date"] = fallback_signal_date
        if "trade_date" not in out.columns:
            out["trade_date"] = fallback_trade_date

        out["signal_date"] = out["signal_date"].apply(lambda x: _date_text(x) or fallback_signal_date)
        out["trade_date"] = out["trade_date"].apply(lambda x: _date_text(x) or fallback_trade_date)
        out["trade_date"] = out.apply(
            lambda r: _date_text(r.get("trade_date", "")) or next_tw_trading_day(r.get("signal_date", "")),
            axis=1
        )

    write_csv_both(out, "final_action_plan.csv")
    write_csv_both(top_opportunity_df, "top_opportunities.csv")
    write_csv_both(ignition_signal_df, "ignition_signals.csv")
    write_csv_both(evolution_signal_df, "evolution_signals.csv")

    summary = {
        "generated_at": generated_at,
        "source": "final_decision_engine_v309_attack_first_final",
        "signal_date": str(out["signal_date"].iloc[0]) if not out.empty and "signal_date" in out.columns else "",
        "trade_date": str(out["trade_date"].iloc[0]) if not out.empty and "trade_date" in out.columns else "",
        "rows": int(len(out)),
        "sell_count": int((out["final_action"] == "SELL").sum()) if not out.empty else 0,
        "reduce_count": int((out["final_action"] == "REDUCE").sum()) if not out.empty else 0,
        "buy_count": int((out["final_action"] == "BUY").sum()) if not out.empty else 0,
        "test_count": int((out["final_action"] == "TEST").sum()) if not out.empty else 0,
        "watch_count": int((out["final_action"] == "WATCH").sum()) if not out.empty else 0,
        "block_count": int((out["final_action"] == "BLOCK").sum()) if not out.empty else 0,
        "ignition_count": int((out["final_action"] == "IGNITION").sum()) if not out.empty else 0,
        "evolution_count": int((out["final_action"] == "EVOLUTION").sum()) if not out.empty else 0,
        "stage_test_count": int((out["signal_stage_v303"].astype(str).str.upper() == "TEST").sum()) if not out.empty and "signal_stage_v303" in out.columns else 0,
        "stage_ignition_count": int((out["signal_stage_v303"].astype(str).str.upper() == "IGNITION").sum()) if not out.empty and "signal_stage_v303" in out.columns else 0,
        "stage_evolution_count": int((out["signal_stage_v303"].astype(str).str.upper() == "EVOLUTION").sum()) if not out.empty and "signal_stage_v303" in out.columns else 0,
        "test_top5_count_v304": int((out["test_top_tag_v304"].astype(str).str.upper() == "🔥TOP5").sum()) if not out.empty and "test_top_tag_v304" in out.columns else 0,
        "alpha_count": int((out["strategy_type"].astype(str).str.upper() == "ALPHA").sum()) if not out.empty else 0,
        "core_count": int((out["strategy_type"].astype(str).str.upper() == "CORE").sum()) if not out.empty else 0,
        "high_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "HIGH").sum()) if not out.empty else 0,
        "medium_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "MEDIUM").sum()) if not out.empty else 0,
        "low_liquidity_count": int((out["liquidity_level"].astype(str).str.upper() == "LOW").sum()) if not out.empty else 0,
        "backfill_source": "feature_panel_daily.csv",
        "extra_lookup_sources": [
            "trade_plan.csv",
            "trading_system_plan.csv",
            "candidates.csv",
            "alpha_candidates.csv",
            "core_candidates.csv",
            "pre_move_candidates.csv",
            "timing_candidates.csv"
        ],
        "with_name_count": int((out["stock_name"].astype(str).str.strip() != "").sum()) if not out.empty else 0,
        "top_opportunity_count": int((out["top_opportunity"].astype(str).str.strip() != "").sum()) if "top_opportunity" in out.columns and not out.empty else 0,
        "chip_high_count": int((pd.to_numeric(out.get("chip_score", pd.Series(dtype=float)), errors="coerce").fillna(0) >= 80).sum()) if not out.empty and "chip_score" in out.columns else 0,
        "macro_regime": macro_guard.get("macro_regime", ""),
        "macro_label": macro_guard.get("macro_label", ""),
        "macro_score": macro_guard.get("macro_score", 0),
        "macro_score_ratio": macro_guard.get("macro_score_ratio", 0),
        "macro_policy": macro_guard.get("macro_policy", ""),
        "macro_raw_regime": macro_guard.get("macro_raw_regime", ""),
        "macro_raw_label": macro_guard.get("macro_raw_label", ""),
        "macro_adjusted_score": macro_guard.get("macro_adjusted_score", 0),
        "macro_confidence": macro_guard.get("macro_confidence", ""),
        "macro_confidence_label": macro_guard.get("macro_confidence_label", ""),
        "macro_confidence_ratio": macro_guard.get("macro_confidence_ratio", 0),
        "valid_indicator_count": macro_guard.get("valid_indicator_count", 0),
        "total_indicator_count": macro_guard.get("total_indicator_count", 0),
        "unknown_count": macro_guard.get("unknown_count", 0),
        "encoding": "utf-8-sig",
    }

    for p in [ROOT / "final_action_summary.json", DATA_DIR / "final_action_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
