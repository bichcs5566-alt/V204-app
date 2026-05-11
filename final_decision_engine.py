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

OUTPUT_COLUMNS = [
    "final_action", "signal_date", "trade_date", "stock_id", "stock_name", "source", "bucket", "strategy_type", "score", "entry_type",
    "execution_flag", "allowed", "close", "suggested_amount", "target_weight",
    "priority", "reason", "system_note",
    "opportunity_score", "opportunity_rank", "top_opportunity",
    "liquidity_level", "liquidity_tag", "liquidity_score", "volume", "turnover",
    "chip_score", "chip_label", "chip_display", "chip_reason", "chip_hint", "chip_valid_count", "chip_missing", "chip_confidence",
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
        out.loc[buy_mask, "priority"] = 3
        out.loc[test_mask, "final_action"] = "WATCH"
        out.loc[test_mask, "priority"] = 8
        out.loc[test_mask, "suggested_amount"] = 0
        out.loc[test_mask, "target_weight"] = 0

        macro_note = f"{macro_label}：{macro_policy}"
        affected = buy_mask | test_mask
        out.loc[affected, "system_note"] = (
            out.loc[affected, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + macro_note)
        )

    elif macro_regime == "NEUTRAL":
        # 總經中性：ALPHA BUY 降 TEST；CORE 小倉可以保留
        alpha_buy = (~protected) & final_upper.eq("BUY") & strategy_upper.str.contains("ALPHA", na=False)
        out.loc[alpha_buy, "final_action"] = "TEST"
        out.loc[alpha_buy, "priority"] = 3
        out.loc[alpha_buy, "system_note"] = (
            out.loc[alpha_buy, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + f"{macro_label}：ALPHA 降級 TEST")
        )

    # 重新抓一次 final_action，避免前面已改動
    final_upper = out["final_action"].astype(str).str.upper()

    # === 市場層：再控制當天節奏 ===
    if market_mode == "MID":
        mask = (~protected) & final_upper.eq("BUY")
        out.loc[mask, "final_action"] = "TEST"
        out.loc[mask, "priority"] = 3
        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + market_label)
        )

    elif market_mode == "WEAK":
        mask = (~protected) & final_upper.isin(["BUY", "TEST"])
        out.loc[mask, "final_action"] = "WATCH"
        out.loc[mask, "priority"] = 8
        out.loc[mask, "suggested_amount"] = 0
        out.loc[mask, "target_weight"] = 0
        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + market_label)
        )

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
        out.loc[mask, "priority"] = 8
        out.loc[mask, "suggested_amount"] = 0
        out.loc[mask, "target_weight"] = 0
        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + f"{label}：總經偏保守，降級觀察")
        )

    elif regime in ["NEUTRAL", "MID"]:
        mask = (~protected) & out["final_action"].astype(str).str.upper().eq("BUY")
        out.loc[mask, "final_action"] = "TEST"
        out.loc[mask, "priority"] = 3
        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + f"{label}：BUY 降級 TEST，控制追高")
        )

    else:
        # RISK_ON：保留進攻
        pass

    return out, macro


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

    out["opportunity_score"] = out.apply(calc_opportunity_score, axis=1)
    out["opportunity_rank"] = ""
    out["top_opportunity"] = ""
    out["section_opportunity_rank"] = ""
    out["section_top_opportunity"] = ""

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
        out.loc[mask, "system_note"] = (
            out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
            .apply(lambda x: (x + "｜" if x else "") + f"全清單 TOP{rank}：優先觀察發動機會")
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
            out.loc[mask, "system_note"] = (
                out.loc[mask, "system_note"].astype(str).replace(["nan", "None", "null"], "")
                .apply(lambda x: (x + "｜" if x else "") + f"{label}清單 TOP{rank}：本區最可能發動")
            )

    top_df = out[
        (out["top_opportunity"].astype(str).str.strip() != "")
        | (out["section_top_opportunity"].astype(str).str.strip() != "")
    ].copy()

    top_df["_rank"] = pd.to_numeric(top_df["opportunity_rank"], errors="coerce").fillna(999)
    top_df["_section_rank"] = pd.to_numeric(top_df["section_opportunity_rank"], errors="coerce").fillna(999)
    top_df = top_df.sort_values(["_rank", "_section_rank"]).drop(columns=["_rank", "_section_rank"], errors="ignore")

    return out, top_df




# ===== v274 FINAL TRUE EXPORT PATCH / 最後輸出分數正規化 =====
# 目的：
# 1. 不動策略核心
# 2. 不動 UI / pipeline / 持倉 / macro
# 3. 只在 final_decision_engine 最後輸出前，把 raw score 正規化為 60~99
# 4. TEST / WATCH / BUY 各自區間內依真實分數排序，避免 139 / 138 這種未正規化分數直接顯示
def v274_pick_raw_score_col(df):
    if df is None or df.empty:
        return None

    candidates = [
        "v273_continuous_score",
        "continuous_score",
        "raw_score",
        "score",
        "total_score",
        "entry_score",
        "opportunity_score",
    ]

    for c in candidates:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().sum() > 0:
                return c

    return None


def v274_percentile_normalize_series(s, low=60.0, high=99.0):
    s = pd.to_numeric(s, errors="coerce")
    valid = s.notna()

    out = pd.Series(index=s.index, dtype="float64")
    out.loc[~valid] = low

    if valid.sum() <= 1:
        out.loc[valid] = high
        return out.round(1)

    pct = s.loc[valid].rank(method="average", pct=True)
    out.loc[valid] = low + pct * (high - low)
    return out.round(1)


def apply_v274_final_true_export_patch(out):
    if out is None or out.empty:
        return out

    out = out.copy()

    score_col = v274_pick_raw_score_col(out)
    if score_col is None:
        return out

    action_upper = out["final_action"].astype(str).str.upper() if "final_action" in out.columns else pd.Series("", index=out.index)
    protected = action_upper.isin(["SELL", "REDUCE", "BLOCK"])

    out["v274_raw_score"] = pd.to_numeric(out[score_col], errors="coerce").fillna(0)

    # 只正規化可比較的進場/觀察類；出場/禁止保留原本風控排序，不影響持倉。
    tradable_mask = ~protected

    out["v274_normalized_score"] = pd.to_numeric(out.get("score", 0), errors="coerce").fillna(0)

    if tradable_mask.any():
        # 各 action 分區正規化，避免 TEST / WATCH 混在一起互相扭曲。
        for action_name in ["BUY", "TEST", "WATCH"]:
            m = tradable_mask & action_upper.eq(action_name)
            if m.any():
                out.loc[m, "v274_normalized_score"] = v274_percentile_normalize_series(
                    out.loc[m, "v274_raw_score"],
                    low=60.0,
                    high=99.0
                )

        # 若有其他非出場 action，也補一次全體正規化。
        other = tradable_mask & ~action_upper.isin(["BUY", "TEST", "WATCH"])
        if other.any():
            out.loc[other, "v274_normalized_score"] = v274_percentile_normalize_series(
                out.loc[other, "v274_raw_score"],
                low=60.0,
                high=99.0
            )

    # UI 主要吃 score，因此最後輸出前強制覆蓋。
    out["score"] = pd.to_numeric(out["v274_normalized_score"], errors="coerce").fillna(0).round(1)

    # 相容其他舊欄位：有就同步，不新增太多策略欄。
    for c in ["entry_score", "total_score", "rank_score"]:
        if c in out.columns:
            out[c] = out["score"]

    # opportunity_score 保留欄位，但重新對齊最後 UI 排序的可讀分數。
    if "opportunity_score" in out.columns:
        out["opportunity_score"] = out["score"]

    if "priority" in out.columns:
        out["_priority_num_v274"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9)
    else:
        out["_priority_num_v274"] = 9

    out["_score_num_v274"] = pd.to_numeric(out["score"], errors="coerce").fillna(0)
    out = out.sort_values(
        ["_priority_num_v274", "_score_num_v274", "stock_id"],
        ascending=[True, False, True]
    ).drop(columns=["_priority_num_v274", "_score_num_v274"], errors="ignore")

    return out.reset_index(drop=True)



# ===== v275 EDGE EXPANSION PATCH / 分數差距拉開補丁 =====
# 目的：
# 1. 不動策略核心、不動 UI、不動 pipeline、不動持倉、不動 macro。
# 2. 只在 v274 最後正規化之後，把過度集中在 98/97 的分數拉開。
# 3. 用 raw score + 成交/流動性 + 原始排名做穩定排序，避免大量同分。
# 4. 每個 action 分區內獨立處理，TEST 不會跟 WATCH 互相干擾。
def v275_num_series(df, col, default=0.0):
    if df is None or col not in df.columns:
        return pd.Series(default, index=df.index, dtype='float64')
    return pd.to_numeric(df[col], errors='coerce').fillna(default)


def v275_pick_edge_base(out):
    candidates = [
        'v274_raw_score',
        'v273_continuous_score',
        'continuous_score',
        'raw_score',
        'total_score',
        'entry_score',
        'opportunity_score',
        'score',
    ]
    for c in candidates:
        if c in out.columns:
            s = pd.to_numeric(out[c], errors='coerce')
            if s.notna().sum() > 0:
                return c
    return None


def apply_v275_edge_expansion_patch(out):
    if out is None or out.empty:
        return out

    out = out.copy()
    base_col = v275_pick_edge_base(out)
    if base_col is None:
        return out

    action_upper = out['final_action'].astype(str).str.upper() if 'final_action' in out.columns else pd.Series('', index=out.index)
    protected = action_upper.isin(['SELL', 'REDUCE', 'BLOCK'])

    base = pd.to_numeric(out[base_col], errors='coerce').fillna(0)
    liq = v275_num_series(out, 'liquidity_score', 0)
    volume = v275_num_series(out, 'volume', 0)
    turnover = v275_num_series(out, 'turnover', 0)
    opp_rank = v275_num_series(out, 'opportunity_rank', 9999)

    # secondary 只用來打散同分，不改策略方向。
    liq_pct = liq.rank(method='average', pct=True).fillna(0)
    vol_pct = volume.rank(method='average', pct=True).fillna(0)
    turn_pct = turnover.rank(method='average', pct=True).fillna(0)
    rank_bonus = (1 / (opp_rank.replace(0, np.nan))).replace([np.inf, -np.inf], np.nan).fillna(0)
    rank_pct = rank_bonus.rank(method='average', pct=True).fillna(0)

    edge_base = (
        base.astype(float) * 1000000
        + liq_pct * 1000
        + vol_pct * 500
        + turn_pct * 300
        + rank_pct * 200
    )

    out['v275_edge_base'] = edge_base
    out['v275_edge_score'] = pd.to_numeric(out.get('score', 0), errors='coerce').fillna(0)

    # 分區處理：每個 action 區內拉成 70~99，並用 ordinal rank 避免同分。
    for action_name in ['BUY', 'TEST', 'WATCH']:
        m = (~protected) & action_upper.eq(action_name)
        n = int(m.sum())
        if n <= 0:
            continue

        vals = edge_base.loc[m]
        order = vals.rank(method='first', ascending=True, pct=True)

        # 非線性：前段拉開，後段保留距離。
        expanded = np.power(order, 0.72)

        # 不同 action 給不同可讀區間，但不改 action 本身。
        if action_name == 'BUY':
            low, high = 72.0, 99.5
        elif action_name == 'TEST':
            low, high = 68.0, 99.0
        else:  # WATCH
            low, high = 62.0, 98.5

        scores = low + expanded * (high - low)
        out.loc[m, 'v275_edge_score'] = pd.Series(scores, index=out.loc[m].index).round(1)

    # 其他非出場 action，保守套用全體排序。
    other = (~protected) & (~action_upper.isin(['BUY', 'TEST', 'WATCH']))
    if other.any():
        vals = edge_base.loc[other]
        order = vals.rank(method='first', ascending=True, pct=True)
        scores = 60.0 + np.power(order, 0.72) * 38.0
        out.loc[other, 'v275_edge_score'] = pd.Series(scores, index=out.loc[other].index).round(1)

    # 最後輸出 score 給 UI。
    out['score'] = pd.to_numeric(out['v275_edge_score'], errors='coerce').fillna(0).round(1)

    for c in ['entry_score', 'total_score', 'rank_score', 'opportunity_score']:
        if c in out.columns:
            out[c] = out['score']

    # action priority 保持原本邏輯，只在同 action 內依 v275 score 排序。
    if 'priority' in out.columns:
        out['_priority_num_v275'] = pd.to_numeric(out['priority'], errors='coerce').fillna(9)
    else:
        out['_priority_num_v275'] = 9

    out['_score_num_v275'] = pd.to_numeric(out['score'], errors='coerce').fillna(0)
    out['_edge_num_v275'] = pd.to_numeric(out['v275_edge_base'], errors='coerce').fillna(0)

    out = out.sort_values(
        ['_priority_num_v275', '_score_num_v275', '_edge_num_v275', 'stock_id'],
        ascending=[True, False, False, True]
    ).drop(columns=['_priority_num_v275', '_score_num_v275', '_edge_num_v275'], errors='ignore')

    return out.reset_index(drop=True)



# ===== v277 MAX OPPORTUNITY DIRECT LANE PATCH =====
# 目的：
# 1. 不改原本 TEST / WATCH / BUY 主清單
# 2. 不動 pipeline / UI / 持倉 / macro / export schema
# 3. 只從已產出的最終清單中，挑出「最大機會」寫入 IGNITION / EVOLUTION 區塊
# 4. IGNITION = 今日最大機會直通區；EVOLUTION = 正在升級中的候選區

def _v277_num(out, col, default=0.0):
    if out is None or col not in out.columns:
        return pd.Series(default, index=out.index if out is not None else [])
    return pd.to_numeric(out[col], errors="coerce").fillna(default)


def _v277_pct(s):
    s = pd.to_numeric(s, errors="coerce").fillna(0)
    if len(s) <= 1:
        return pd.Series(1.0, index=s.index)
    return s.rank(method="average", pct=True).fillna(0)


def _v277_clean_action_series(out):
    if out is None or out.empty:
        return pd.Series(dtype=str)
    if "final_action" in out.columns:
        return out["final_action"].astype(str).str.upper()
    if "action" in out.columns:
        return out["action"].astype(str).str.upper()
    return pd.Series("", index=out.index)


def build_v277_max_opportunity_lanes(out):
    """
    從 final_action_plan 的已確認候選裡，抽出兩個直通區：
    - ignition_candidates.csv：Top 1~3，最大機會，直接進場評估
    - strategy_evolution.csv：次強 4~8，準備升級，等確認/分批試單

    注意：這裡不改 out 主表，只另外產生兩個區塊資料，避免破壞原流程。
    """
    if out is None or out.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = out.copy()
    action = _v277_clean_action_series(df)

    # 只從可交易/可觀察池抽最大機會；不碰出場、減碼、禁止。
    tradable = action.isin(["BUY", "TEST", "WATCH"])
    df = df.loc[tradable].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    action = _v277_clean_action_series(df)

    score = _v277_num(df, "score", 0)
    opportunity = _v277_num(df, "opportunity_score", 0)
    liquidity = _v277_num(df, "liquidity_score", 0)
    volume = _v277_num(df, "volume", 0)
    turnover = _v277_num(df, "turnover", 0)
    chip = _v277_num(df, "chip_score", 0)

    score_pct = _v277_pct(score)
    opp_pct = _v277_pct(opportunity)
    liq_pct = _v277_pct(liquidity)
    vol_pct = _v277_pct(volume)
    turn_pct = _v277_pct(turnover)
    chip_pct = _v277_pct(chip)

    # TEST 比 WATCH 更接近可執行；BUY 如果存在則最高。
    action_bonus = pd.Series(0.0, index=df.index)
    action_bonus.loc[action.eq("BUY")] = 0.10
    action_bonus.loc[action.eq("TEST")] = 0.06
    action_bonus.loc[action.eq("WATCH")] = 0.02

    text = (
        df.get("reason", pd.Series("", index=df.index)).astype(str) + " " +
        df.get("system_note", pd.Series("", index=df.index)).astype(str) + " " +
        df.get("entry_type", pd.Series("", index=df.index)).astype(str) + " " +
        df.get("top_opportunity", pd.Series("", index=df.index)).astype(str) + " " +
        df.get("section_top_opportunity", pd.Series("", index=df.index)).astype(str)
    ).str.upper()

    ignition_hint = text.str.contains("TOP|突破|BREAK|IGNITION|起漲|點火|轉強|強勢|主力", regex=True)
    risk_hint = text.str.contains("過熱|追高|長上影|出貨|誘多|AVOID|RISK|禁止|BLOCK", regex=True)

    ignition_bonus = pd.Series(0.0, index=df.index)
    ignition_bonus.loc[ignition_hint] = 0.07

    risk_penalty = pd.Series(0.0, index=df.index)
    risk_penalty.loc[risk_hint] = 0.18

    # v277 最大機會分數：不是拿來重寫主策略，只用於兩個直通區排序。
    direct_score = (
        score_pct * 0.38 +
        opp_pct * 0.24 +
        liq_pct * 0.12 +
        vol_pct * 0.10 +
        turn_pct * 0.08 +
        chip_pct * 0.08 +
        action_bonus +
        ignition_bonus -
        risk_penalty
    )

    df["v277_direct_entry_score"] = (60 + direct_score.clip(0, 1) * 39).round(1)
    df["v277_direct_entry_rank"] = df["v277_direct_entry_score"].rank(method="first", ascending=False).astype(int)
    df["v277_direct_entry_tag"] = "B_NORMAL"
    df.loc[df["v277_direct_entry_rank"] <= 3, "v277_direct_entry_tag"] = "S_DIRECT_ENTRY"
    df.loc[(df["v277_direct_entry_rank"] > 3) & (df["v277_direct_entry_rank"] <= 8), "v277_direct_entry_tag"] = "A_EVOLUTION_UPGRADE"
    df.loc[risk_hint, "v277_direct_entry_tag"] = "AVOID_RISK"

    df = df.sort_values(["v277_direct_entry_score", "score", "stock_id"], ascending=[False, False, True]).reset_index(drop=True)

    # 避免過熱/風險直接進 IGNITION。
    safe_df = df[df["v277_direct_entry_tag"].ne("AVOID_RISK")].copy()
    if safe_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    ignition = safe_df.head(3).copy()
    evolution = safe_df.iloc[3:8].copy()

    if not ignition.empty:
        ignition["source"] = "IGNITION"
        ignition["bucket"] = "IGNITION"
        ignition["strategy_type"] = "IGNITION"
        ignition["strategy_name"] = "v277 最大機會直通"
        ignition["final_action"] = "BUY"
        ignition["action"] = "BUY"
        ignition["entry_type"] = "最大機會直通"
        ignition["action_sub"] = "DIRECT_ENTRY"
        ignition["score"] = ignition["v277_direct_entry_score"]
        ignition["entry_score"] = ignition["v277_direct_entry_score"]
        ignition["execution_flag"] = "TOP"
        ignition["reason"] = ignition.apply(
            lambda r: f"v277最大機會直通｜排名 {int(r.get('v277_direct_entry_rank', 0))}｜原訊號：{clean_text(r.get('reason', ''), '依策略判斷')}",
            axis=1
        )
        ignition["system_note"] = "最大機會直通：可直接進場評估；建議仍分批、小倉、避免開高追價。"
        ignition["operation_advice_zh"] = "可直接進場評估，但不要一次重倉；若開高過熱，等回測不破再進。"
        ignition["ignition_hint_zh"] = "由 TEST / WATCH / BUY 候選池升級：具備當日最大機會特徵。"
        ignition["fake_risk_tag"] = "PASS_V277"
        ignition["fake_reason_zh"] = "已排除明顯過熱、出貨、誘多文字風險。"

    if not evolution.empty:
        evolution["source"] = "EVOLUTION"
        evolution["bucket"] = "EVOLUTION"
        evolution["strategy_type"] = "EVOLUTION"
        evolution["strategy_name"] = "v277 策略進化候選"
        evolution["final_action"] = "TEST"
        evolution["action"] = "TEST"
        evolution["entry_type"] = "準備升級"
        evolution["action_sub"] = "EVOLUTION_UPGRADE"
        evolution["score"] = evolution["v277_direct_entry_score"]
        evolution["entry_score"] = evolution["v277_direct_entry_score"]
        evolution["execution_flag"] = "TOP"
        evolution["evolution_phase"] = "WATCH/TEST → DIRECT候選"
        evolution["reason"] = evolution.apply(
            lambda r: f"v277策略進化候選｜排名 {int(r.get('v277_direct_entry_rank', 0))}｜原訊號：{clean_text(r.get('reason', ''), '依策略判斷')}",
            axis=1
        )
        evolution["system_note"] = "策略進化候選：接近最大機會，但仍需確認量價延續，不建議一次重倉。"

    return ignition.reset_index(drop=True), evolution.reset_index(drop=True)



# ===== v278 TRUE TRIGGER DIRECT LANE PATCH =====
# 目的：
# 1. 不改原本 final_action_plan 主表
# 2. 不動 pipeline / UI / 持倉 / macro / export schema
# 3. 只把 TEST / WATCH / BUY 中「強 + 正在發動」的標的，寫入 IGNITION / EVOLUTION
# 4. IGNITION = 最大機會且有觸發確認，可直接進場評估
# 5. EVOLUTION = 接近觸發、準備升級

def _v278_num_series(s, default=0.0):
    try:
        return pd.to_numeric(s, errors="coerce").fillna(default)
    except Exception:
        return pd.Series(default)


def _v278_pct(s):
    s = pd.to_numeric(s, errors="coerce").fillna(0)
    if len(s) <= 1:
        return pd.Series(1.0, index=s.index)
    return s.rank(method="average", pct=True).fillna(0)


def _v278_read_latest_feature_map():
    """
    只讀資料，不改主流程。
    用 feature_panel_daily 補 MA / 均量 / K棒欄位，若沒有欄位則自動略過。
    """
    df = read_csv_any([ROOT / "feature_panel_daily.csv", DATA_DIR / "feature_panel_daily.csv"])
    if df.empty or "stock_id" not in df.columns:
        return {}

    df = df.copy()
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(["stock_id", "date"])
        df = df.groupby("stock_id", as_index=False).tail(1).copy()
    else:
        df = df.drop_duplicates("stock_id", keep="last").copy()

    return {str(r["stock_id"]): r.to_dict() for _, r in df.iterrows()}


def _v278_pick_series(df, feature_map, names, default=0.0):
    for c in names:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default)

    # 從 feature_map 依 stock_id 補。
    sid = df["stock_id"].astype(str).apply(normalize_stock_id) if "stock_id" in df.columns else pd.Series("", index=df.index)
    vals = []
    for s in sid:
        item = feature_map.get(str(s), {})
        v = default
        for c in names:
            if c in item and clean_text(item.get(c), "") not in ["", "--"]:
                v = item.get(c)
                break
        vals.append(v)
    return pd.to_numeric(pd.Series(vals, index=df.index), errors="coerce").fillna(default)


def _v278_text_series(df, names):
    out = pd.Series("", index=df.index)
    for c in names:
        if c in df.columns:
            out = out + " " + df[c].astype(str).fillna("")
    return out.str.upper()


def build_v278_true_trigger_lanes(out):
    """
    v278 真觸發直通區：
    - 先沿用 v277 的最大機會概念
    - 再加入 Trigger：量能確認 / 均線結構 / 未過熱 / 文字突破訊號 / 假突破排除
    - 只輸出 ignition_candidates.csv / strategy_evolution.csv，不改 final_action_plan
    """
    if out is None or out.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = out.copy()
    action = _v277_clean_action_series(df)
    df = df.loc[action.isin(["BUY", "TEST", "WATCH"])].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    action = _v277_clean_action_series(df)
    feature_map = _v278_read_latest_feature_map()

    score = _v278_pick_series(df, feature_map, ["score", "entry_score", "total_score"], 0)
    opportunity = _v278_pick_series(df, feature_map, ["opportunity_score", "v277_direct_entry_score"], 0)
    liquidity = _v278_pick_series(df, feature_map, ["liquidity_score"], 0)
    volume = _v278_pick_series(df, feature_map, ["volume", "vol", "成交量"], 0)
    turnover = _v278_pick_series(df, feature_map, ["turnover", "amount", "成交金額"], 0)
    chip = _v278_pick_series(df, feature_map, ["chip_score", "chip_concentration_score"], 0)

    close = _v278_pick_series(df, feature_map, ["close", "ref_price", "price", "收盤價"], 0)
    openp = _v278_pick_series(df, feature_map, ["open", "Open", "開盤價"], 0)
    high = _v278_pick_series(df, feature_map, ["high", "High", "最高價"], 0)
    low = _v278_pick_series(df, feature_map, ["low", "Low", "最低價"], 0)
    ma5 = _v278_pick_series(df, feature_map, ["ma5", "MA5", "ma_5", "sma5"], 0)
    ma10 = _v278_pick_series(df, feature_map, ["ma10", "MA10", "ma_10", "sma10"], 0)
    ma20 = _v278_pick_series(df, feature_map, ["ma20", "MA20", "ma_20", "sma20"], 0)
    vol_ma5 = _v278_pick_series(df, feature_map, ["volume_ma5", "vol_ma5", "avg_volume_5", "volume_5ma"], 0)

    text = _v278_text_series(df, [
        "reason", "system_note", "entry_type", "action_sub", "top_opportunity",
        "section_top_opportunity", "v276_opportunity_tag", "v277_direct_entry_tag"
    ])

    # === Trigger 條件 ===
    ma_ready = (close > 0) & (ma5 > 0) & (ma10 > 0) & (ma20 > 0) & (close >= ma5) & (ma5 >= ma10) & (ma10 >= ma20)
    near_support = (ma5 > 0) & ((close / ma5 - 1).abs() <= 0.08)
    not_overheat_ma = (ma20 > 0) & ((close / ma20 - 1) <= 0.24)

    # 沒有均量欄位時，改用同批候選的成交量百分位。
    vol_confirm_by_ma = (vol_ma5 > 0) & (volume >= vol_ma5 * 1.10)
    vol_confirm_by_pct = _v278_pct(volume) >= 0.70
    volume_confirm = vol_confirm_by_ma | vol_confirm_by_pct

    body = (close - openp).fillna(0)
    candle_range = (high - low).replace(0, np.nan)
    upper_shadow_ratio = ((high - close) / candle_range).replace([np.inf, -np.inf], np.nan).fillna(0)
    strong_candle = (openp > 0) & (close > openp) & (upper_shadow_ratio <= 0.45)

    breakout_text = text.str.contains("突破|起漲|點火|轉強|主力|強勢|TOP|BREAK|BREAKOUT|IGNITION", regex=True)
    risk_text = text.str.contains("過熱|追高|長上影|出貨|誘多|假突破|AVOID|RISK|禁止|BLOCK", regex=True)
    fake_break_risk = risk_text | (upper_shadow_ratio >= 0.55) | ((ma20 > 0) & ((close / ma20 - 1) > 0.30))

    trigger_confirm = (
        (ma_ready & volume_confirm & not_overheat_ma) |
        (breakout_text & volume_confirm & near_support)
    ) & (~fake_break_risk)

    near_trigger = (
        ((ma_ready | breakout_text) & (~fake_break_risk)) |
        (volume_confirm & near_support & not_overheat_ma)
    )

    action_bonus = pd.Series(0.0, index=df.index)
    action_bonus.loc[action.eq("BUY")] = 0.10
    action_bonus.loc[action.eq("TEST")] = 0.06
    action_bonus.loc[action.eq("WATCH")] = 0.02

    trigger_bonus = pd.Series(0.0, index=df.index)
    trigger_bonus.loc[ma_ready] += 0.08
    trigger_bonus.loc[volume_confirm] += 0.07
    trigger_bonus.loc[breakout_text] += 0.06
    trigger_bonus.loc[strong_candle] += 0.04
    trigger_bonus.loc[trigger_confirm] += 0.13

    risk_penalty = pd.Series(0.0, index=df.index)
    risk_penalty.loc[fake_break_risk] = 0.35

    # v278 直通分數：強度 + 觸發，觸發權重高於純分數。
    trigger_score = (
        _v278_pct(score) * 0.24 +
        _v278_pct(opportunity) * 0.14 +
        _v278_pct(liquidity) * 0.08 +
        _v278_pct(volume) * 0.10 +
        _v278_pct(turnover) * 0.07 +
        _v278_pct(chip) * 0.07 +
        trigger_bonus +
        action_bonus -
        risk_penalty
    ).clip(0, 1)

    df["v278_trigger_score"] = (60 + trigger_score * 39).round(1)
    df["v278_trigger_confirm"] = np.where(trigger_confirm, "YES", np.where(near_trigger, "NEAR", "NO"))
    df["v278_trigger_tag"] = np.where(
        fake_break_risk,
        "AVOID_FAKE_BREAK",
        np.where(
            trigger_confirm,
            "S_TRUE_TRIGGER",
            np.where(near_trigger, "A_NEAR_TRIGGER", "B_RANK_ONLY")
        )
    )
    df["v278_trigger_reason"] = np.where(
        fake_break_risk,
        "排除：疑似過熱、假突破、長上影或出貨風險。",
        np.where(
            trigger_confirm,
            "真觸發：強度、量能、均線/突破結構同時成立。",
            np.where(near_trigger, "準觸發：接近發動，但仍需下一根確認。", "僅排名強，尚未出現明確觸發。")
        )
    )

    df = df.sort_values(["v278_trigger_score", "score", "stock_id"], ascending=[False, False, True]).reset_index(drop=True)

    ignition_pool = df[df["v278_trigger_tag"].eq("S_TRUE_TRIGGER")].copy()
    evolution_pool = df[df["v278_trigger_tag"].isin(["A_NEAR_TRIGGER", "S_TRUE_TRIGGER"])].copy()

    # 若當天沒有完美真觸發，不讓 IGNITION 空白到失去操作價值：取前 1~3 檔 NEAR 作為「待開盤確認」。
    if ignition_pool.empty:
        ignition_pool = df[df["v278_trigger_tag"].eq("A_NEAR_TRIGGER")].head(3).copy()

    ignition = ignition_pool.head(3).copy()
    used = set(ignition.get("stock_id", pd.Series(dtype=str)).astype(str).tolist()) if not ignition.empty else set()
    evolution = evolution_pool[~evolution_pool["stock_id"].astype(str).isin(used)].head(5).copy()

    if not ignition.empty:
        ignition["source"] = "IGNITION"
        ignition["bucket"] = "IGNITION"
        ignition["strategy_type"] = "IGNITION"
        ignition["strategy_name"] = "v278 真觸發最大機會"
        ignition["final_action"] = "BUY"
        ignition["action"] = "BUY"
        ignition["entry_type"] = np.where(
            ignition["v278_trigger_confirm"].eq("YES"),
            "真觸發直通",
            "準觸發直通"
        )
        ignition["action_sub"] = "TRUE_TRIGGER_ENTRY"
        ignition["score"] = ignition["v278_trigger_score"]
        ignition["entry_score"] = ignition["v278_trigger_score"]
        ignition["execution_flag"] = "TOP"
        ignition["reason"] = ignition.apply(
            lambda r: f"v278最大機會直通｜{clean_text(r.get('v278_trigger_reason', ''))}｜原訊號：{clean_text(r.get('reason', ''), '依策略判斷')}",
            axis=1
        )
        ignition["system_note"] = "真觸發最大機會：可直接進場評估；若開高過熱，等待回測不破再進。"
        ignition["operation_advice_zh"] = "可直接進場評估，但仍建議分批；若開盤急拉或長上影，暫緩追價。"
        ignition["ignition_hint_zh"] = "由 TEST / WATCH / BUY 升級：強度 + 量能 + 觸發確認。"
        ignition["fake_risk_tag"] = "PASS_V278"
        ignition["fake_reason_zh"] = "已排除明顯過熱、長上影、出貨與假突破風險。"

    if not evolution.empty:
        evolution["source"] = "EVOLUTION"
        evolution["bucket"] = "EVOLUTION"
        evolution["strategy_type"] = "EVOLUTION"
        evolution["strategy_name"] = "v278 準觸發進化候選"
        evolution["final_action"] = "TEST"
        evolution["action"] = "TEST"
        evolution["entry_type"] = "準觸發升級"
        evolution["action_sub"] = "TRIGGER_UPGRADE"
        evolution["score"] = evolution["v278_trigger_score"]
        evolution["entry_score"] = evolution["v278_trigger_score"]
        evolution["execution_flag"] = "TOP"
        evolution["evolution_phase"] = "WATCH/TEST → TRUE_TRIGGER候選"
        evolution["reason"] = evolution.apply(
            lambda r: f"v278準觸發進化｜{clean_text(r.get('v278_trigger_reason', ''))}｜原訊號：{clean_text(r.get('reason', ''), '依策略判斷')}",
            axis=1
        )
        evolution["system_note"] = "準觸發進化候選：接近最大機會，需確認量價延續，不建議一次重倉。"

    return ignition.reset_index(drop=True), evolution.reset_index(drop=True)


# ===== v279 EVENT BOOST PATCH / 事件觸發加權補丁 =====
# 目的：
# 1. 不重寫原策略
# 2. 不改 pipeline / UI / 持倉 / macro / watchlist
# 3. 只在 v274/v275 後、IGNITION/EVOLUTION 輸出前，對「真正觸發事件」加權
# 4. 讓真突破 / 放量突破 / 起漲事件可以跳脫 98~99 排行榜，成為最大機會

def _v279_num_col(df, names, default=0.0):
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    for c in names:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index, dtype="float64")


def _v279_text_cols(df, names):
    if df is None or df.empty:
        return pd.Series(dtype=str)
    out = pd.Series("", index=df.index, dtype=str)
    for c in names:
        if c in df.columns:
            out = out + " " + df[c].astype(str).fillna("")
    return out.str.upper()


def _v279_bool_from_text(text, pattern):
    try:
        return text.str.contains(pattern, regex=True, na=False)
    except Exception:
        return pd.Series(False, index=text.index)


def apply_v279_event_boost_patch(out):
    if out is None or out.empty:
        return out

    out = out.copy()

    action = out["final_action"].astype(str).str.upper() if "final_action" in out.columns else pd.Series("", index=out.index)
    tradable = action.isin(["BUY", "TEST", "WATCH"])

    if not tradable.any():
        return out

    base_score = _v279_num_col(out, ["score", "opportunity_score", "entry_score", "total_score"], 0.0)

    close = _v279_num_col(out, ["close", "ref_price", "price"], 0.0)
    ma5 = _v279_num_col(out, ["ma5", "MA5", "ma_5", "sma5"], 0.0)
    ma10 = _v279_num_col(out, ["ma10", "MA10", "ma_10", "sma10"], 0.0)
    ma20 = _v279_num_col(out, ["ma20", "MA20", "ma_20", "sma20"], 0.0)

    volume = _v279_num_col(out, ["volume", "vol", "成交量"], 0.0)
    turnover = _v279_num_col(out, ["turnover", "amount", "成交金額"], 0.0)
    liquidity_score = _v279_num_col(out, ["liquidity_score"], 0.0)
    chip_score = _v279_num_col(out, ["chip_score", "chip_concentration_score"], 0.0)

    text = _v279_text_cols(out, [
        "reason", "system_note", "entry_type", "execution_flag",
        "top_opportunity", "section_top_opportunity",
        "v276_opportunity_tag", "v277_direct_entry_tag", "v278_trigger_tag"
    ])

    # ===== 事件偵測：有欄位用欄位，沒欄位用文字/相對分位 =====
    ma_structure = (close > ma5) & (ma5 >= ma10) & (ma10 >= ma20) & (ma20 > 0)
    above_ma20_not_far = (ma20 > 0) & ((close / ma20 - 1) <= 0.22)
    overheat_distance = (ma20 > 0) & ((close / ma20 - 1) > 0.30)

    volume_q70 = volume.quantile(0.70) if volume.notna().any() else 0
    volume_q88 = volume.quantile(0.88) if volume.notna().any() else 0
    turnover_q70 = turnover.quantile(0.70) if turnover.notna().any() else 0
    liquidity_q70 = liquidity_score.quantile(0.70) if liquidity_score.notna().any() else 0

    volume_confirm = (volume > 0) & (volume >= volume_q70)
    volume_surge = (volume > 0) & (volume >= volume_q88)
    money_confirm = ((turnover > 0) & (turnover >= turnover_q70)) | ((liquidity_score > 0) & (liquidity_score >= liquidity_q70))

    breakout_text = _v279_bool_from_text(text, r"BREAK|BREAKOUT|突破|轉強|點火|起漲|IGNITION|TURN_FIRST|EARLY_TURN|主升")
    fake_or_risk_text = _v279_bool_from_text(text, r"假突破|長上影|出貨|誘多|過熱|追高|AVOID|RISK|轉弱|跌破")

    clean_breakout = tradable & ma_structure & above_ma20_not_far & volume_confirm & money_confirm
    explosive_breakout = clean_breakout & volume_surge
    ignition_event = tradable & above_ma20_not_far & (breakout_text | clean_breakout) & ~fake_or_risk_text
    leader_event = tradable & ma_structure & volume_surge & (chip_score >= chip_score.quantile(0.65)) & ~overheat_distance
    fake_breakout = tradable & (fake_or_risk_text | overheat_distance)

    # ===== 事件加分：讓真正事件跳脫 98~99 排行榜 =====
    bonus = pd.Series(0.0, index=out.index)

    bonus = bonus + np.where(clean_breakout, 16.0, 0.0)
    bonus = bonus + np.where(explosive_breakout, 18.0, 0.0)
    bonus = bonus + np.where(ignition_event, 22.0, 0.0)
    bonus = bonus + np.where(leader_event, 14.0, 0.0)
    bonus = bonus + np.where(fake_breakout, -35.0, 0.0)

    # 出場/減碼/禁止完全不碰。
    bonus = pd.Series(bonus, index=out.index)
    bonus.loc[~tradable] = 0.0

    # 避免單次補丁過度失控，但允許最大機會跳到 100+。
    bonus = bonus.clip(lower=-40.0, upper=55.0)

    event_score = base_score + bonus

    out["v279_event_bonus"] = bonus.round(1)
    out["v279_event_score"] = event_score.round(1)

    out["v279_event_tag"] = np.where(
        fake_breakout,
        "AVOID_FAKE_BREAKOUT",
        np.where(
            explosive_breakout | ignition_event,
            "S_TRUE_TRIGGER",
            np.where(
                clean_breakout,
                "A_CLEAN_BREAKOUT",
                np.where(
                    leader_event,
                    "A_LEADER_CONTINUATION",
                    "B_NO_EVENT"
                )
            )
        )
    )

    # 只有可交易/觀察類覆蓋分數，讓事件真正反映在 TEST/WATCH/IGNITION 排名。
    out.loc[tradable, "score"] = event_score.loc[tradable].round(1)

    for c in ["entry_score", "total_score", "rank_score", "opportunity_score"]:
        if c in out.columns:
            out.loc[tradable, c] = out.loc[tradable, "score"]

    # 排序維持原 action priority，再看事件分數與事件 bonus。
    priority_map = {"SELL": 1, "REDUCE": 2, "BUY": 3, "TEST": 4, "WATCH": 5, "BLOCK": 6}
    out["_v279_priority"] = action.map(priority_map).fillna(9)
    out["_v279_score"] = pd.to_numeric(out["score"], errors="coerce").fillna(0)
    out["_v279_bonus"] = pd.to_numeric(out["v279_event_bonus"], errors="coerce").fillna(0)

    out = out.sort_values(
        ["_v279_priority", "_v279_score", "_v279_bonus", "stock_id"],
        ascending=[True, False, False, True]
    ).drop(columns=["_v279_priority", "_v279_score", "_v279_bonus"], errors="ignore")

    return out.reset_index(drop=True)


# ===== v280 EVENT PROMOTION ENGINE / 事件直接升級補丁 =====
# 目的：
# 1. 不重寫原策略
# 2. 不動 pipeline / UI / 持倉 / macro / watchlist
# 3. 不破壞 final_action_plan 主表欄位
# 4. 只在最後輸出前，讓真正事件股直接跳層到 IGNITION / EVOLUTION 專區
# 5. 避免最大機會被一般 TEST / WATCH 排行榜埋掉

def _v280_num_col(df, names, default=0.0):
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    for c in names:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index, dtype="float64")


def _v280_text_cols(df, names):
    if df is None or df.empty:
        return pd.Series(dtype=str)
    out = pd.Series("", index=df.index, dtype=str)
    for c in names:
        if c in df.columns:
            out = out + " " + df[c].astype(str).fillna("")
    return out.str.upper()


def _v280_pick_cols(df, cols):
    keep = [c for c in cols if c in df.columns]
    return df[keep].copy() if keep else df.copy()


def build_v280_event_promotion_lanes(out):
    """
    事件直接升級：
    - IGNITION：真突破 / 放量突破 / 起漲事件，Top 1~3
    - EVOLUTION：接近觸發 / 正在升級，Top 4~8
    - 不改原 TEST / WATCH / BUY 主清單，只另外輸出兩個專區 CSV
    """
    if out is None or out.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = out.copy()

    action = df["final_action"].astype(str).str.upper() if "final_action" in df.columns else pd.Series("", index=df.index)
    tradable = action.isin(["BUY", "TEST", "WATCH"])

    df = df.loc[tradable].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    action = df["final_action"].astype(str).str.upper() if "final_action" in df.columns else pd.Series("", index=df.index)

    score = _v280_num_col(df, ["score", "v279_event_score", "opportunity_score", "entry_score", "total_score"], 0.0)
    event_bonus = _v280_num_col(df, ["v279_event_bonus"], 0.0)
    liquidity = _v280_num_col(df, ["liquidity_score"], 0.0)
    volume = _v280_num_col(df, ["volume", "vol"], 0.0)
    turnover = _v280_num_col(df, ["turnover", "amount"], 0.0)
    chip = _v280_num_col(df, ["chip_score", "chip_concentration_score"], 0.0)

    close = _v280_num_col(df, ["close", "ref_price", "price"], 0.0)
    ma5 = _v280_num_col(df, ["ma5", "MA5", "ma_5", "sma5"], 0.0)
    ma10 = _v280_num_col(df, ["ma10", "MA10", "ma_10", "sma10"], 0.0)
    ma20 = _v280_num_col(df, ["ma20", "MA20", "ma_20", "sma20"], 0.0)

    text = _v280_text_cols(df, [
        "reason", "system_note", "entry_type", "execution_flag",
        "top_opportunity", "section_top_opportunity",
        "v276_opportunity_tag", "v277_direct_entry_tag",
        "v278_trigger_tag", "v279_event_tag"
    ])

    ma_structure = (close > ma5) & (ma5 >= ma10) & (ma10 >= ma20) & (ma20 > 0)
    not_overheat = ~((ma20 > 0) & ((close / ma20 - 1) > 0.30))

    volume_q70 = volume.quantile(0.70) if volume.notna().any() else 0
    volume_q85 = volume.quantile(0.85) if volume.notna().any() else 0
    turnover_q65 = turnover.quantile(0.65) if turnover.notna().any() else 0
    liquidity_q60 = liquidity.quantile(0.60) if liquidity.notna().any() else 0
    chip_q60 = chip.quantile(0.60) if chip.notna().any() else 0

    volume_confirm = (volume > 0) & (volume >= volume_q70)
    volume_surge = (volume > 0) & (volume >= volume_q85)
    money_confirm = ((turnover > 0) & (turnover >= turnover_q65)) | ((liquidity > 0) & (liquidity >= liquidity_q60))
    chip_confirm = (chip > 0) & (chip >= chip_q60)

    breakout_text = text.str.contains(r"BREAK|BREAKOUT|突破|轉強|點火|起漲|IGNITION|S_TRUE_TRIGGER|CLEAN_BREAKOUT|主升", regex=True, na=False)
    risk_text = text.str.contains(r"假突破|長上影|出貨|誘多|過熱|追高|AVOID|RISK|轉弱|跌破", regex=True, na=False)

    # 事件定義：不是只看分數，而是看「強 + 發動」。
    true_trigger = (
        not_overheat &
        ~risk_text &
        (
            (event_bonus >= 20) |
            (breakout_text & volume_confirm) |
            (ma_structure & volume_surge & money_confirm)
        )
    )

    evolution_trigger = (
        not_overheat &
        ~risk_text &
        ~true_trigger &
        (
            (score >= score.quantile(0.80)) |
            (ma_structure & volume_confirm) |
            (volume_confirm & money_confirm) |
            chip_confirm
        )
    )

    # 事件排序分：事件優先，不再被一般 98~99 分排行榜壓住。
    promote_score = (
        score.fillna(0) +
        event_bonus.fillna(0) * 1.8 +
        np.where(true_trigger, 80, 0) +
        np.where(evolution_trigger, 35, 0) +
        np.where(volume_surge, 12, 0) +
        np.where(money_confirm, 8, 0) +
        np.where(chip_confirm, 6, 0) -
        np.where(risk_text, 80, 0)
    )

    df["v280_event_promote_score"] = pd.Series(promote_score, index=df.index).round(1)
    df["v280_event_lane"] = np.where(true_trigger, "IGNITION", np.where(evolution_trigger, "EVOLUTION", "NORMAL"))

    promoted = df[df["v280_event_lane"].isin(["IGNITION", "EVOLUTION"])].copy()

    # 如果今天完全沒有明確事件，保留舊邏輯 fallback，避免 UI 區塊空白。
    if promoted.empty:
        try:
            return build_v278_true_trigger_lanes(out)
        except Exception:
            try:
                return build_v277_max_opportunity_lanes(out)
            except Exception:
                return pd.DataFrame(), pd.DataFrame()

    promoted = promoted.sort_values(
        ["v280_event_lane", "v280_event_promote_score", "score", "stock_id"],
        ascending=[True, False, False, True]
    )

    ignition = promoted[promoted["v280_event_lane"] == "IGNITION"].copy()
    evolution = promoted[promoted["v280_event_lane"] == "EVOLUTION"].copy()

    # 若 IGNITION 不足 3 檔，用 EVOLUTION 前排補足，但標註為準直通。
    if len(ignition) < 3 and not evolution.empty:
        need = 3 - len(ignition)
        fill = evolution.head(need).copy()
        fill["v280_event_lane"] = "IGNITION_CANDIDATE"
        ignition = pd.concat([ignition, fill], ignore_index=True)
        evolution = evolution.iloc[need:].copy()

    ignition = ignition.sort_values(["v280_event_promote_score", "score", "stock_id"], ascending=[False, False, True]).head(3).copy()
    evolution = evolution.sort_values(["v280_event_promote_score", "score", "stock_id"], ascending=[False, False, True]).head(5).copy()

    if not ignition.empty:
        ignition["final_action"] = "IGNITION"
        ignition["execution_flag"] = "DIRECT"
        ignition["entry_type"] = "MAX_EVENT"
        ignition["priority"] = 2
        ignition["allowed"] = True
        ignition["score"] = pd.to_numeric(ignition["v280_event_promote_score"], errors="coerce").fillna(
            pd.to_numeric(ignition.get("score", 0), errors="coerce").fillna(0)
        ).round(1)
        ignition["reason"] = ignition.apply(
            lambda r: f"v280事件直通｜{r.get('stock_id','')}｜事件分 {r.get('v280_event_promote_score','')}｜原訊號：{clean_text(r.get('reason',''), '最大機會候選')}",
            axis=1
        )
        ignition["system_note"] = "IGNITION：事件直接升級，不再只看一般 TEST/WATCH 排名；可直接進場評估，但仍需依資金控管分批。"

    if not evolution.empty:
        evolution["final_action"] = "EVOLUTION"
        evolution["execution_flag"] = "READY"
        evolution["entry_type"] = "PRE_EVENT"
        evolution["priority"] = 3
        evolution["allowed"] = True
        evolution["score"] = pd.to_numeric(evolution["v280_event_promote_score"], errors="coerce").fillna(
            pd.to_numeric(evolution.get("score", 0), errors="coerce").fillna(0)
        ).round(1)
        evolution["reason"] = evolution.apply(
            lambda r: f"v280準事件升級｜{r.get('stock_id','')}｜事件分 {r.get('v280_event_promote_score','')}｜原訊號：{clean_text(r.get('reason',''), '準最大機會候選')}",
            axis=1
        )
        evolution["system_note"] = "EVOLUTION：接近事件觸發，等待量價延續或突破確認，可列為優先觀察/小試單。"

    return ignition.reset_index(drop=True), evolution.reset_index(drop=True)

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

    if not out.empty:
        out["stock_id"] = out["stock_id"].apply(normalize_stock_id)

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

        out, market_guard = apply_market_guard(out)

        # v266.15：總經攻擊強度 + TOP5 機會評測
        out, macro_guard = apply_macro_strength_v26614(out)
        out, top_opportunity_df = apply_top_opportunities_v26614(out)

        out["_score_num"] = pd.to_numeric(out["score"], errors="coerce").fillna(0)
        out["_priority_num"] = pd.to_numeric(out["priority"], errors="coerce").fillna(9)
        out["_op_num"] = pd.to_numeric(out["opportunity_score"], errors="coerce").fillna(0)
        out = out.sort_values(["_priority_num", "_op_num", "_score_num", "stock_id"], ascending=[True, False, False, True])
        out = out.drop(columns=["_score_num", "_priority_num", "_op_num"])

    if "market_guard" not in locals():
        market_guard = load_market_guard()
    if "macro_guard" not in locals():
        macro_guard = load_macro_regime_for_v26614()

    if "top_opportunity_df" not in locals():
        out, top_opportunity_df = apply_top_opportunities_v26614(out)

    out = add_chip_columns(out)

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

    # v274 FINAL TRUE EXPORT PATCH：
    # 只在最後輸出前正規化分數，不改前面策略判斷。
    out = apply_v274_final_true_export_patch(out)
    # v275 EDGE EXPANSION PATCH：只拉開最後可讀分數差距，不改策略判斷。
    out = apply_v2
