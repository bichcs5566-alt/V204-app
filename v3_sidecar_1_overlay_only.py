"""
v3_sidecar_1_overlay_only.py

只旁路診斷，不接管主程式。

輸入：
- trade_plan.csv
- price_panel_daily.csv

輸出：
- trade_plan_enriched.csv
- sidecar_candidates.csv
- missed_opportunities.csv
- sidecar_debug.csv

原則：
不改 trade_plan.csv
不改 action
不改 target_weight
不改 v1_stable_pipeline.py
"""

from pathlib import Path
import json
import pandas as pd
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_first(paths):
    for p in paths:
        p = Path(p)
        if p.exists():
            return pd.read_csv(p), p
    return pd.DataFrame(), None


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def is_common_stock_id(x):
    s = normalize_stock_id(x)
    return s.isdigit() and len(s) == 4 and not s.startswith(("00", "03", "04", "05", "06", "07", "08", "09"))


def prepare_features(df):
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(c).lower().strip() for c in out.columns]

    if "stock_id" not in out.columns or "close" not in out.columns:
        return pd.DataFrame()

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    else:
        out["date"] = pd.Timestamp.today().normalize()

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in out.columns:
            out[col] = out["close"] if col in ["open", "high", "low"] else np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)
    out = out[out["stock_id"].apply(is_common_stock_id)].copy()
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)

    g = out.groupby("stock_id", group_keys=False)

    for n in [5, 10, 20, 60]:
        out[f"ma{n}"] = g["close"].rolling(n, min_periods=max(5, min(n, 20))).mean().reset_index(level=0, drop=True)

    out["mom5"] = g["close"].pct_change(5)
    out["mom20"] = g["close"].pct_change(20)

    out["vol_ma5"] = g["volume"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    out["vol_ma20"] = g["volume"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["vol_ma20"] + 1e-9)
    out["vol_dry_ratio"] = out["vol_ma5"] / (out["vol_ma20"] + 1e-9)

    out["high_20"] = g["high"].rolling(20, min_periods=10).max().reset_index(level=0, drop=True)
    out["low_20"] = g["low"].rolling(20, min_periods=10).min().reset_index(level=0, drop=True)
    out["high_60"] = g["high"].rolling(60, min_periods=20).max().reset_index(level=0, drop=True)
    out["low_60"] = g["low"].rolling(60, min_periods=20).min().reset_index(level=0, drop=True)
    out["range_20"] = (out["high_20"] - out["low_20"]) / (out["close"] + 1e-9)

    out["ma_max"] = out[["ma5", "ma10", "ma20"]].max(axis=1)
    out["ma_min"] = out[["ma5", "ma10", "ma20"]].min(axis=1)
    out["ma_converge_pct"] = (out["ma_max"] - out["ma_min"]) / (out["close"] + 1e-9)

    # KD
    low9 = g["low"].rolling(9, min_periods=5).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9, min_periods=5).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_cross"] = ((out["kd_k"] > out["kd_d"]) & (g["kd_k"].shift(1) <= g["kd_d"].shift(1))).astype(int)

    # MACD
    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_cross"] = ((out["macd_diff"] > out["macd_signal"]) & (g["macd_diff"].shift(1) <= g["macd_signal"].shift(1))).astype(int)

    # 主力感知 proxy
    diff = g["close"].diff()
    signed_volume = np.where(diff > 0, out["volume"], np.where(diff < 0, -out["volume"], 0))
    out["signed_volume"] = signed_volume
    out["obv_proxy"] = out.groupby("stock_id")["signed_volume"].cumsum()

    for w in [3, 5, 10]:
        out[f"obv_up_count_{w}"] = out.groupby("stock_id")["obv_proxy"].transform(
            lambda s, ww=w: (s.diff() > 0).astype(float).rolling(ww, min_periods=max(2, ww // 2)).sum()
        )
        out[f"low_non_down_count_{w}"] = out.groupby("stock_id")["low"].transform(
            lambda s, ww=w: (s.diff() >= 0).astype(float).rolling(ww, min_periods=max(2, ww // 2)).sum()
        )

    return out


def score_row(row):
    score = 0
    reasons = []

    close = row.get("close")
    ma20 = row.get("ma20")
    ma60 = row.get("ma60")
    high20 = row.get("high_20")
    high60 = row.get("high_60")
    ma_conv = row.get("ma_converge_pct")
    rng20 = row.get("range_20")
    dry = row.get("vol_dry_ratio")
    mom5 = row.get("mom5")
    mom20 = row.get("mom20")

    if pd.notna(ma_conv) and ma_conv <= 0.07:
        score += 22
        reasons.append("均線收斂")
    if pd.notna(rng20) and rng20 <= 0.23:
        score += 18
        reasons.append("區間壓縮")
    if pd.notna(dry) and 0.45 <= dry <= 1.10:
        score += 15
        reasons.append("量縮整理")
    if pd.notna(close) and pd.notna(ma20) and close >= ma20 * 0.96:
        score += 12
        reasons.append("貼近MA20")
    if pd.notna(close) and pd.notna(ma60) and close >= ma60 * 0.85:
        score += 8
        reasons.append("未遠離MA60")
    if pd.notna(close) and pd.notna(high20) and close >= high20 * 0.94:
        score += 12
        reasons.append("接近20日高")
    if pd.notna(close) and pd.notna(high60) and 0.70 <= close / high60 <= 0.96:
        score += 10
        reasons.append("低位有空間")

    sense = 0
    if row.get("obv_up_count_10", 0) >= 6 or row.get("low_non_down_count_10", 0) >= 6:
        sense = max(sense, 22)
        reasons.append("10日感知")
    if row.get("obv_up_count_5", 0) >= 3 or row.get("low_non_down_count_5", 0) >= 3:
        sense = max(sense, 16)
        reasons.append("5日感知")
    if row.get("obv_up_count_3", 0) >= 2 or row.get("low_non_down_count_3", 0) >= 2:
        sense = max(sense, 10)
        reasons.append("3日感知")
    score += sense

    if row.get("kd_cross", 0) == 1 or row.get("macd_cross", 0) == 1:
        score += 10
        reasons.append("KD/MACD轉強")
    if pd.notna(mom5) and mom5 > 0:
        score += 8
        reasons.append("5日動能正")
    if pd.notna(mom20) and mom20 > 0:
        score += 6
        reasons.append("20日動能正")

    if pd.notna(mom20) and mom20 > 0.28:
        score -= 15
        reasons.append("20日過熱扣分")
    if pd.notna(close) and close < 10:
        score -= 15
        reasons.append("低價風險扣分")

    if score >= 75:
        stage = "STRUCTURE_READY"
        label = "🔵 結構待發"
    elif score >= 60:
        stage = "LATENT_STRONG"
        label = "🟡 強潛伏"
    elif score >= 45:
        stage = "LATENT"
        label = "🟡 潛伏"
    elif score >= 35:
        stage = "WATCH"
        label = "⚪ 觀察"
    else:
        stage = "SKIP"
        label = "❌ 排除"

    return pd.Series({
        "sidecar_score": round(float(score), 2),
        "sidecar_stage": stage,
        "sidecar_label": label,
        "sidecar_reason": "、".join(reasons) if reasons else "無明顯旁路訊號"
    })


def main():
    price_panel, price_path = read_first([
        ROOT / "price_panel_daily.csv",
        ROOT / "data" / "price_panel_daily.csv",
        DATA_DIR / "price_panel_daily.csv",
    ])
    trade_plan, trade_path = read_first([
        ROOT / "trade_plan.csv",
        DATA_DIR / "trade_plan.csv",
    ])

    feat = prepare_features(price_panel)
    if feat.empty:
        latest = pd.DataFrame()
    else:
        latest = feat[feat["date"] == feat["date"].max()].copy()

    if latest.empty:
        scored = pd.DataFrame()
    else:
        scores = latest.apply(score_row, axis=1)
        scored = pd.concat([latest.reset_index(drop=True), scores.reset_index(drop=True)], axis=1)
        scored = scored[scored["sidecar_stage"] != "SKIP"].copy()
        scored = scored.sort_values(["sidecar_score"], ascending=False)

    sidecar_candidates = scored.head(40).copy()

    if trade_plan is not None and not trade_plan.empty and "stock_id" in trade_plan.columns:
        enriched = trade_plan.copy()
        enriched["stock_id"] = enriched["stock_id"].apply(normalize_stock_id)
        small = scored[["stock_id", "sidecar_score", "sidecar_stage", "sidecar_label", "sidecar_reason"]].copy()
        small["stock_id"] = small["stock_id"].apply(normalize_stock_id)
        enriched = enriched.merge(small, on="stock_id", how="left")
        if "note" in enriched.columns:
            enriched["note"] = enriched["note"].fillna("").astype(str) + "｜旁路：" + enriched["sidecar_label"].fillna("") + " " + enriched["sidecar_reason"].fillna("")
        else:
            enriched["note"] = "旁路：" + enriched["sidecar_label"].fillna("") + " " + enriched["sidecar_reason"].fillna("")
    else:
        enriched = pd.DataFrame()

    original_ids = set()
    if trade_plan is not None and not trade_plan.empty and "stock_id" in trade_plan.columns:
        original_ids = set(trade_plan["stock_id"].apply(normalize_stock_id).astype(str))

    if not sidecar_candidates.empty:
        missed = sidecar_candidates[~sidecar_candidates["stock_id"].astype(str).isin(original_ids)].head(30).copy()
    else:
        missed = pd.DataFrame()

    debug = pd.DataFrame([{
        "price_panel_path": str(price_path) if price_path else "",
        "trade_plan_path": str(trade_path) if trade_path else "",
        "price_panel_rows": int(len(price_panel)) if price_panel is not None else 0,
        "latest_rows": int(len(latest)) if latest is not None else 0,
        "original_trade_plan_rows": int(len(trade_plan)) if trade_plan is not None else 0,
        "sidecar_candidates_count": int(len(sidecar_candidates)),
        "missed_opportunities_count": int(len(missed)),
        "mode": "overlay_only_no_main_change"
    }])

    outputs = {
        "trade_plan_enriched.csv": enriched,
        "sidecar_candidates.csv": sidecar_candidates,
        "missed_opportunities.csv": missed,
        "sidecar_debug.csv": debug,
    }

    for name, df in outputs.items():
        df.to_csv(ROOT / name, index=False, encoding="utf-8-sig")
        df.to_csv(DATA_DIR / name, index=False, encoding="utf-8-sig")

    with open(DATA_DIR / "sidecar_meta.json", "w", encoding="utf-8") as f:
        json.dump(debug.iloc[0].to_dict(), f, ensure_ascii=False, indent=2)

    print("v3 sidecar overlay only completed")
    print(debug.to_string(index=False))


if __name__ == "__main__":
    main()
