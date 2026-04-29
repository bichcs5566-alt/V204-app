"""
feature_timing_engine.py

最終整合版輔助特徵與進場 Timing 引擎

用途：
為候選股票補上：
- strategy_lane：PRE / CORE / ALPHA / UNKNOWN
- entry_type：BREAK / PULLBACK / REVERSAL / WAIT
- entry_note：進場說明
- timing_score：進場時機分數

輸入：
- price_panel_daily.csv
- trade_plan.csv
- core_candidates.csv
- alpha_candidates.csv
- pre_move_candidates.csv

輸出：
- timing_candidates.csv
- mobile_dashboard_v1/data/timing_candidates.csv
- timing_summary.json
- mobile_dashboard_v1/data/timing_summary.json

設計原則：
1. 不取代原策略。
2. 不刪除候選名單。
3. 只增加「進場方式」與「是否等待」。
"""

from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRICE_CANDIDATES = [
    ROOT / "price_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
]

CANDIDATE_FILES = {
    "CORE": [ROOT / "trade_plan.csv", ROOT / "core_candidates.csv", DATA_DIR / "trade_plan.csv", DATA_DIR / "core_candidates.csv"],
    "ALPHA": [ROOT / "alpha_candidates.csv", DATA_DIR / "alpha_candidates.csv"],
    "PRE": [ROOT / "pre_move_candidates.csv", DATA_DIR / "pre_move_candidates.csv"],
}

OUTPUT_COLUMNS = [
    "stock_id",
    "strategy_lane",
    "entry_type",
    "entry_note",
    "timing_score",
    "close",
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "atr20_pct",
    "mom5",
    "mom20",
    "volume_ratio20",
    "ma_compression",
    "near_20d_high",
    "pullback_to_ma20",
    "reversal_signal",
    "breakout_signal",
]


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if p.exists() and p.stat().st_size > 0:
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return df
            except Exception:
                pass
    return pd.DataFrame()


def find_price_file():
    for p in PRICE_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError("missing price_panel_daily.csv")


def load_price_panel():
    p = find_price_file()
    df = pd.read_csv(p)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns and "trade_date" in df.columns:
        df["date"] = df["trade_date"]

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            if c in ["open", "high", "low"] and "close" in df.columns:
                df[c] = df["close"]
            elif c == "volume":
                df[c] = 0
            else:
                raise ValueError(f"missing {c}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)]
    df = df[df["close"] > 0].copy()
    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)


def build_candidate_universe():
    rows = []
    for lane, paths in CANDIDATE_FILES.items():
        df = read_csv_any(paths)
        if df.empty:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        if "stock_id" not in df.columns and "symbol" in df.columns:
            df["stock_id"] = df["symbol"]

        if "stock_id" not in df.columns:
            continue

        for _, r in df.iterrows():
            sid = normalize_stock_id(r.get("stock_id", ""))
            if not sid or not sid.isdigit() or len(sid) != 4:
                continue
            rows.append({"stock_id": sid, "strategy_lane": lane})

    if not rows:
        return pd.DataFrame(columns=["stock_id", "strategy_lane"])

    out = pd.DataFrame(rows)
    # 同股多策略保留最高優先：CORE > PRE > ALPHA
    priority = {"CORE": 1, "PRE": 2, "ALPHA": 3}
    out["priority"] = out["strategy_lane"].map(priority).fillna(9)
    out = out.sort_values(["stock_id", "priority"]).drop_duplicates("stock_id", keep="first")
    return out[["stock_id", "strategy_lane"]]


def add_features(g):
    g = g.sort_values("date").copy()

    g["ma5"] = g["close"].rolling(5).mean()
    g["ma10"] = g["close"].rolling(10).mean()
    g["ma20"] = g["close"].rolling(20).mean()
    g["ma60"] = g["close"].rolling(60).mean()

    prev_close = g["close"].shift(1)
    tr1 = g["high"] - g["low"]
    tr2 = (g["high"] - prev_close).abs()
    tr3 = (g["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    g["atr20"] = tr.rolling(20).mean()
    g["atr20_pct"] = g["atr20"] / g["close"]

    g["mom5"] = g["close"] / g["close"].shift(5) - 1
    g["mom20"] = g["close"] / g["close"].shift(20) - 1

    g["vol_ma20"] = g["volume"].rolling(20).mean()
    g["volume_ratio20"] = g["volume"] / g["vol_ma20"].replace(0, np.nan)

    g["high20"] = g["high"].rolling(20).max()
    g["low10_prev"] = g["low"].shift(1).rolling(10).min()

    g["ma_compression"] = (
        (g["ma5"] - g["ma20"]).abs() / g["close"] +
        (g["ma10"] - g["ma20"]).abs() / g["close"]
    ) / 2

    return g


def classify_timing(row, lane):
    close = row.get("close", np.nan)
    ma5 = row.get("ma5", np.nan)
    ma10 = row.get("ma10", np.nan)
    ma20 = row.get("ma20", np.nan)
    ma60 = row.get("ma60", np.nan)
    high20 = row.get("high20", np.nan)
    low10_prev = row.get("low10_prev", np.nan)
    mom5 = row.get("mom5", np.nan)
    mom20 = row.get("mom20", np.nan)
    vr = row.get("volume_ratio20", np.nan)
    atr = row.get("atr20_pct", np.nan)
    comp = row.get("ma_compression", np.nan)

    if not np.isfinite(close) or close <= 0:
        return "WAIT", "資料不足，等待", 0, False, False, False, False

    near_20d_high = bool(np.isfinite(high20) and high20 > 0 and close >= high20 * 0.97)
    pullback_to_ma20 = bool(np.isfinite(ma20) and abs(close / ma20 - 1) <= 0.035)
    reversal_signal = bool(
        np.isfinite(low10_prev) and
        row.get("low", close) < low10_prev and
        close > low10_prev
    )
    breakout_signal = bool(
        near_20d_high and
        np.isfinite(vr) and vr >= 1.15 and
        np.isfinite(mom20) and mom20 >= 0.03
    )

    timing_score = 0

    if lane == "CORE":
        if breakout_signal:
            timing_score += 45
        if pullback_to_ma20 and np.isfinite(mom20) and mom20 > 0:
            timing_score += 35
        if np.isfinite(ma5) and np.isfinite(ma10) and np.isfinite(ma20) and close > ma5 > ma10 > ma20:
            timing_score += 15
        if np.isfinite(vr) and vr >= 1.0:
            timing_score += 5

        if breakout_signal:
            return "BREAK", "突破型：可分批追，但避免開盤急拉直接追", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal
        if pullback_to_ma20:
            return "PULLBACK", "回檔型：接近 MA20，適合分批低接", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal
        return "WAIT", "CORE 尚未到理想進場點，等待突破或回檔", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal

    if lane == "ALPHA":
        if reversal_signal:
            timing_score += 45
        if pullback_to_ma20:
            timing_score += 25
        if np.isfinite(mom5) and mom5 > 0:
            timing_score += 15
        if np.isfinite(vr) and vr >= 1.0:
            timing_score += 10

        if reversal_signal:
            return "REVERSAL", "反轉型：只適合短線試單，失敗要快出", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal
        if pullback_to_ma20:
            return "PULLBACK", "回檔型：可觀察是否轉強，不追高", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal
        return "WAIT", "ALPHA 時機未明，先觀察", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal

    if lane == "PRE":
        if np.isfinite(comp) and comp <= 0.035:
            timing_score += 30
        if np.isfinite(atr) and atr <= 0.035:
            timing_score += 25
        if np.isfinite(vr) and 0.75 <= vr <= 1.5:
            timing_score += 20
        if not breakout_signal and np.isfinite(mom20) and mom20 < 0.15:
            timing_score += 15
        if pullback_to_ma20:
            timing_score += 10

        if timing_score >= 70:
            return "WAIT", "PRE 佈局型：等待突破確認，小倉觀察，不主動重倉", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal
        return "WAIT", "PRE 條件未完整，僅保留觀察", min(100, timing_score), near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal

    return "WAIT", "未知策略線，等待", 0, near_20d_high, pullback_to_ma20, reversal_signal, breakout_signal


def main():
    price = load_price_panel()
    universe = build_candidate_universe()

    if universe.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_csv(ROOT / "timing_candidates.csv", index=False, encoding="utf-8")
        out.to_csv(DATA_DIR / "timing_candidates.csv", index=False, encoding="utf-8")
        return

    candidate_ids = set(universe["stock_id"].astype(str))
    price = price[price["stock_id"].isin(candidate_ids)].copy()

    rows = []
    lane_map = dict(zip(universe["stock_id"], universe["strategy_lane"]))

    for sid, g in price.groupby("stock_id", sort=False):
        if len(g) < 80:
            continue

        feat = add_features(g)
        r = feat.iloc[-1].copy()
        lane = lane_map.get(sid, "UNKNOWN")
        entry_type, entry_note, timing_score, near_high, pullback, reversal, breakout = classify_timing(r, lane)

        rows.append({
            "stock_id": sid,
            "strategy_lane": lane,
            "entry_type": entry_type,
            "entry_note": entry_note,
            "timing_score": timing_score,
            "close": round(float(r.get("close", 0)), 2),
            "ma5": round(float(r.get("ma5", 0)), 2) if pd.notna(r.get("ma5")) else "",
            "ma10": round(float(r.get("ma10", 0)), 2) if pd.notna(r.get("ma10")) else "",
            "ma20": round(float(r.get("ma20", 0)), 2) if pd.notna(r.get("ma20")) else "",
            "ma60": round(float(r.get("ma60", 0)), 2) if pd.notna(r.get("ma60")) else "",
            "atr20_pct": round(float(r.get("atr20_pct", 0)), 4) if pd.notna(r.get("atr20_pct")) else "",
            "mom5": round(float(r.get("mom5", 0)), 4) if pd.notna(r.get("mom5")) else "",
            "mom20": round(float(r.get("mom20", 0)), 4) if pd.notna(r.get("mom20")) else "",
            "volume_ratio20": round(float(r.get("volume_ratio20", 0)), 4) if pd.notna(r.get("volume_ratio20")) else "",
            "ma_compression": round(float(r.get("ma_compression", 0)), 4) if pd.notna(r.get("ma_compression")) else "",
            "near_20d_high": near_high,
            "pullback_to_ma20": pullback,
            "reversal_signal": reversal,
            "breakout_signal": breakout,
        })

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    out = out.sort_values(["strategy_lane", "timing_score", "stock_id"], ascending=[True, False, True])

    out.to_csv(ROOT / "timing_candidates.csv", index=False, encoding="utf-8")
    out.to_csv(DATA_DIR / "timing_candidates.csv", index=False, encoding="utf-8")

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "feature_timing_engine",
        "rows": int(len(out)),
        "entry_counts": out["entry_type"].value_counts().to_dict() if not out.empty else {},
        "lane_counts": out["strategy_lane"].value_counts().to_dict() if not out.empty else {},
        "rule": "BREAK 突破；PULLBACK 回檔；REVERSAL 反轉；WAIT 等待。"
    }

    for p in [ROOT / "timing_summary.json", DATA_DIR / "timing_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
