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
    if out is None or out.empty:
        return out

    out = out.copy()
    out = _v302_force_all_object(out)
    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

    # v302.3：先鎖整張表 object，再鎖定 final decision 會被 loc 指派的欄位型別。
    out = _v302_force_object_cols(out, [
        "final_action", "entry_type", "execution_flag", "system_note", "reason",
        "source", "bucket", "strategy_type", "top_opportunity",
        "section_top_opportunity", "opportunity_rank", "section_opportunity_rank",
    ])

    feature = _v302_load_feature_latest()
    chip = _v302_load_chip_source()

    # 合併 feature：只補 final decision 需要的技術/量價欄位。
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
        # v302.3：merge 後 pandas 可能把欄位推回 string dtype，先整張表轉 object。
        out = _v302_force_all_object(out)

        # 若原欄位空值，用 feature 補。
        for c in ["close", "volume", "turnover", "liquidity_score", "liquidity_level", "liquidity_tag"]:
            fx = c + "_fx"
            if fx in out.columns:
                if c not in out.columns:
                    out[c] = out[fx].astype("object")
                else:
                    out[c] = out[c].astype("object")
                    out[fx] = out[fx].astype("object")
                    empty = out[c].astype(str).str.strip().isin(["", "nan", "None", "null", "0", "0.0"])
                    # v302.3：右側也轉 object，避免 string dtype 指派數字爆炸。
                    out.loc[empty, c] = out.loc[empty, fx].astype("object")
                out = out.drop(columns=[fx], errors="ignore")
        out = _v302_force_all_object(out)

    # 合併 chip：每日籌碼驗證。
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

    mom5 = _v302_num(out, "mom5", 0)
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

    # 這裡是硬門檻：低量/低成交金額不做；流動性只是門票。
    liquidity_gate = (
        (volume >= 1500) |
        (turnover >= 50_000_000) |
        (liq_score >= 55) |
        liq_level.isin(["MEDIUM", "HIGH"])
    )

    # 真正主力痕跡：法人開始買，或 OBV/低點墊高明顯。
    chip_trace = (
        (inst_valid >= 1) &
        (
            (inst > 0) |
            (foreign > 0) |
            (trust > 0) |
            (inst_days >= 2)
        )
    )

    obv_trace = (
        (obv_mom5 > 0) &
        (obv_up5 >= 2) &
        (low_hold5 >= 3)
    )

    main_force_trace = chip_trace | obv_trace

    ma20_gap = ((close / ma20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    # 攻擊結構：擋水泥/食品/防禦/牛皮股。
    attack_structure = (
        (close >= 15) &
        (close > ma20 * 0.99) &
        (ma5 >= ma10 * 0.985) &
        (ma10 >= ma20 * 0.965) &
        (ma20 >= ma60 * 0.94) &
        (mom10 > 0.005) &
        (mom20 > 0.035) &
        (close >= high60 * 0.88)
    )

    # 量能剛啟動，不追過熱爆量。
    volume_start = volume_ratio.between(1.08, 4.20)

    not_overheat = (
        (mom20 <= 0.42) &
        (ma20_gap <= 0.24) &
        (volume_ratio <= 5.2) &
        ~((upper_shadow > 0.06) & (volume_ratio > 1.6)) &
        ~((intraday < -0.03) & (volume_ratio > 1.8))
    )

    main_force_gate = liquidity_gate & main_force_trace & attack_structure & volume_start & not_overheat

    score = (
        chip_trace.astype(int) * 36 +
        obv_trace.astype(int) * 28 +
        (trust > 0).astype(int) * 12 +
        (foreign > 0).astype(int) * 8 +
        volume_start.astype(int) * 10 +
        attack_structure.astype(int) * 10 -
        (~not_overheat).astype(int) * 30
    )

    out["main_force_gate_v302"] = main_force_gate.astype(int)
    out["main_force_score_v302"] = pd.Series(score, index=out.index).round(1)
    out["chip_trace_v302"] = chip_trace.astype(int)
    out["obv_trace_v302"] = obv_trace.astype(int)
    out["attack_structure_v302"] = attack_structure.astype(int)
    out["volume_start_v302"] = volume_start.astype(int)
    out["not_overheat_v302"] = not_overheat.astype(int)

    # 核心：新進場/試單/觀察若沒有主力 Gate，一律 BLOCK，避免垃圾顯示在 TEST/WATCH。
    target = (~protected) & final_upper.isin(["BUY", "TEST", "WATCH"]) & (~main_force_gate)

    if target.any():
        # v302.1：所有文字欄位都已強制 object，這裡只塞純字串，避免 pandas string dtype TypeError。
        out.loc[target, "final_action"] = "BLOCK"
        out.loc[target, "priority"] = "9"
        out.loc[target, "allowed"] = "False"
        out.loc[target, "suggested_amount"] = "0"
        out.loc[target, "target_weight"] = "0"
        out.loc[target, "execution_flag"] = "BLOCK"
        out.loc[target, "entry_type"] = "未通過主力Gate"
        out.loc[target, "system_note"] = _v302_append_note(
            out.loc[target, "system_note"],
            "v302主力硬門檻：無籌碼/OBV主力痕跡或攻擊結構不足，禁止進 TEST/WATCH"
        )

    # 通過 Gate 的標的，補註記，但不亂改原本 action。
    passed = (~protected) & final_upper.isin(["BUY", "TEST", "WATCH"]) & main_force_gate
    if passed.any():
        out.loc[passed, "system_note"] = _v302_append_note(
            out.loc[passed, "system_note"],
            "v302主力Gate通過：籌碼/OBV痕跡＋剛轉強＋量能啟動＋未過熱"
        )

    # v302.1：函式出口再次鎖定輸出文字欄位，避免後續 apply_top_opportunities_v26614 再遇到 string dtype。
    out = _v302_force_object_cols(out, [
        "final_action", "entry_type", "execution_flag", "system_note", "reason",
        "top_opportunity", "section_top_opportunity", "opportunity_rank", "section_opportunity_rank",
    ])

    return out


# ===== v303 TEST → IGNITION → EVOLUTION STATE MACHINE =====
# 核心目的：
# - TEST：最大機會候選池，不是最後答案。
# - IGNITION：從 TEST 裡升級，代表「起漲點火」。
# - EVOLUTION：從 IGNITION 條件再升級，代表「主升前夕」。
# - 不動持倉 SELL / REDUCE，不動 UI，不重寫 pipeline。
# - 只在 final_decision_engine 的最後決策層建立狀態升級。

def apply_test_ignition_evolution_v303(out):
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
    low20 = _v302_num(out, "low_20", close)

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
    high20_gap = ((close / high20) - 1).replace([np.inf, -np.inf], 0).fillna(0)
    high60_pos = (close / high60).replace([np.inf, -np.inf], 0).fillna(0)
    base_range = ((high20 - low20) / close).replace([np.inf, -np.inf], 0).fillna(999)
    upper_shadow = ((high - close) / high).replace([np.inf, -np.inf], 0).fillna(0)
    intraday = ((close - open_) / open_).replace([np.inf, -np.inf], 0).fillna(0)

    # TEST 來源：必須先通過主力 Gate，且原本屬於 BUY/TEST/WATCH。
    base = (
        (~protected)
        & final_upper.isin(["BUY", "TEST", "WATCH", "IGNITION", "EVOLUTION"])
        & main_gate
        & not_overheat
    )

    # IGNITION：從 TEST 裡升級到「起漲點火」
    # 必須：主力痕跡 + 攻擊結構 + 量剛啟動 + 剛突破/站上均線 + 不過熱。
    ignition_gate = (
        base
        & (chip_trace | obv_trace)
        & attack_structure
        & volume_start
        & (close > ma5 * 0.995)
        & (ma5 >= ma10 * 0.985)
        & (mom5 > 0.005)
        & (mom10 > 0.015)
        & (mom20.between(0.035, 0.32))
        & (volume_ratio.between(1.12, 3.60))
        & (ma20_gap.between(-0.015, 0.18))
        & (high60_pos >= 0.88)
        & (upper_shadow <= 0.065)
    )

    # EVOLUTION：主升前夕。比 IGNITION 更嚴格：
    # 主力 Gate 已通過，量價延續，均線擴散但未過熱，法人/OBV 持續。
    evolution_gate = (
        ignition_gate
        & (mom10 > 0.035)
        & (mom20.between(0.07, 0.36))
        & (close > ma10)
        & (ma5 >= ma10)
        & (ma10 >= ma20 * 0.99)
        & (ma20 >= ma60 * 0.96)
        & (volume_ratio.between(1.15, 3.20))
        & (ma20_gap.between(0.015, 0.20))
        & (high60_pos >= 0.92)
        & (
            (inst > 0)
            | (trust > 0)
            | (foreign > 0)
            | (inst_days >= 2)
            | obv_trace
        )
        & (upper_shadow <= 0.055)
        & (intraday > -0.025)
    )

    ignition_score = (
        main_score
        + chip_trace.astype(int) * 18
        + obv_trace.astype(int) * 15
        + attack_structure.astype(int) * 12
        + volume_start.astype(int) * 12
        + (trust > 0).astype(int) * 10
        + (foreign > 0).astype(int) * 6
        + (mom10 > 0.025).astype(int) * 8
        + (mom20 > 0.06).astype(int) * 8
        - (ma20_gap > 0.20).astype(int) * 35
        - (volume_ratio > 4.0).astype(int) * 25
        - (upper_shadow > 0.065).astype(int) * 25
    ).round(1)

    evolution_score = (
        ignition_score
        + (mom10 > 0.04).astype(int) * 12
        + (mom20 > 0.10).astype(int) * 12
        + (close > high20 * 0.995).astype(int) * 10
        + (ma5 >= ma10).astype(int) * 8
        + (ma10 >= ma20).astype(int) * 8
        + ((inst > 0) | (trust > 0) | (foreign > 0)).astype(int) * 10
        - (ma20_gap > 0.22).astype(int) * 35
        - (volume_ratio > 3.8).astype(int) * 20
    ).round(1)

    out["ignition_score_v303"] = ignition_score.astype("object")
    out["evolution_score_v303"] = evolution_score.astype("object")

    # 先清空，再標狀態。
    active = base
    out.loc[active, "signal_stage_v303"] = "TEST"
    out.loc[active, "stage_reason_v303"] = "主力Gate通過｜列入最大機會TEST池"

    out.loc[ignition_gate, "signal_stage_v303"] = "IGNITION"
    out.loc[ignition_gate, "stage_reason_v303"] = "TEST升級：點火起漲｜主力痕跡＋剛轉強＋量能啟動＋未過熱"

    out.loc[evolution_gate, "signal_stage_v303"] = "EVOLUTION"
    out.loc[evolution_gate, "stage_reason_v303"] = "IGNITION升級：主升前夕｜籌碼續強＋量價延續＋均線轉強＋未過熱"

    # 讓 UI 的 IGNITION / EVOLUTION 區塊能吃到資料：直接把 final_action 升級成對應狀態。
    # TEST 仍保留原 TEST；只有最強的狀態才升級。
    out.loc[ignition_gate, "final_action"] = "IGNITION"
    out.loc[ignition_gate, "priority"] = "2.5"
    out.loc[ignition_gate, "entry_type"] = "起漲點火"
    out.loc[ignition_gate, "execution_flag"] = "IGNITION"
    out.loc[ignition_gate, "system_note"] = _v302_append_note(
        out.loc[ignition_gate, "system_note"],
        "v303：由 TEST 升級 IGNITION，起漲訊號成立"
    )

    out.loc[evolution_gate, "final_action"] = "EVOLUTION"
    out.loc[evolution_gate, "priority"] = "2"
    out.loc[evolution_gate, "entry_type"] = "主升前夕"
    out.loc[evolution_gate, "execution_flag"] = "EVOLUTION"
    out.loc[evolution_gate, "system_note"] = _v302_append_note(
        out.loc[evolution_gate, "system_note"],
        "v303：由 IGNITION 升級 EVOLUTION，主升前夕訊號成立"
    )

    # 排名：EVOLUTION 用 evolution_score，IGNITION 用 ignition_score。
    for stage, score_col in [("EVOLUTION", "evolution_score_v303"), ("IGNITION", "ignition_score_v303")]:
        m = out["signal_stage_v303"].astype(str).str.upper().eq(stage)
        if not m.any():
            continue
        idx = (
            out.loc[m]
            .assign(_stage_score=pd.to_numeric(out.loc[m, score_col], errors="coerce").fillna(0))
            .sort_values(["_stage_score", "main_force_score_v302", "stock_id"], ascending=[False, False, True])
            .head(8 if stage == "IGNITION" else 5)
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

        # TOP5 機會評測只允許在通過 v302/v303 後的有效池內產生。
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
        "source": "final_decision_engine_v303_test_to_ignition_to_evolution",
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
