"""
v3_0_core_engine.py
v3.0 主力行為核心引擎｜嚴格版

定位：
這不是傳統動能選股器。
這是「主力行為偵測核心」。

核心概念：
1. 不用單日 KD / MACD 猜主力
2. 用 20~60 日行為判斷：
   - 吸籌 accumulation
   - 控盤 control
   - 試單 test_move
   - 發動前 pre_breakout
3. 動能只做確認，不做第一層篩選

嚴格版特色：
- 股票會比較少
- 排除死股、假收斂、無資金容量標的
- 適合波段 / 主力同步
"""

import numpy as np
import pandas as pd


def _num(s):
    return pd.to_numeric(s, errors="coerce")


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) <= 4 else s


def is_common_stock_id(stock_id):
    s = normalize_stock_id(stock_id)
    if not s.isdigit():
        return False
    if len(s) != 4:
        return False
    # 排除 ETF / 權證 / 特殊商品常見區
    if s.startswith(("00", "03", "04", "05", "06", "07", "08", "09")):
        return False
    return True


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    輸入：歷史日資料
    必要欄位：
    - date
    - stock_id
    - open/high/low/close
    - volume

    輸出：
    - 加上 v3 主力行為需要的所有欄位
    """
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
            if col in ["open", "high", "low"]:
                out[col] = out["close"]
            else:
                out[col] = np.nan
        out[col] = _num(out[col])

    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)
    out = out[out["stock_id"].apply(is_common_stock_id)].copy()
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)

    g = out.groupby("stock_id", group_keys=False)

    out["turnover_value"] = out["close"] * out["volume"]

    for n in [5, 10, 20, 60]:
        out[f"ma{n}"] = g["close"].rolling(n, min_periods=max(5, min(n, 20))).mean().reset_index(level=0, drop=True)

    out["ret_1d"] = g["close"].pct_change(1)
    out["ret_5d"] = g["close"].pct_change(5)
    out["ret_20d"] = g["close"].pct_change(20)

    out["vol_ma20"] = g["volume"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["vol_ma60"] = g["volume"].rolling(60, min_periods=20).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["vol_ma20"] + 1e-9)

    out["high_20"] = g["high"].rolling(20, min_periods=10).max().reset_index(level=0, drop=True)
    out["low_20"] = g["low"].rolling(20, min_periods=10).min().reset_index(level=0, drop=True)
    out["high_60"] = g["high"].rolling(60, min_periods=20).max().reset_index(level=0, drop=True)
    out["low_60"] = g["low"].rolling(60, min_periods=20).min().reset_index(level=0, drop=True)

    out["range_20"] = (out["high_20"] - out["low_20"]) / (out["close"] + 1e-9)
    out["range_60"] = (out["high_60"] - out["low_60"]) / (out["close"] + 1e-9)

    out["ma_max_5_20"] = out[["ma5", "ma10", "ma20"]].max(axis=1)
    out["ma_min_5_20"] = out[["ma5", "ma10", "ma20"]].min(axis=1)
    out["ma_converge_pct"] = (out["ma_max_5_20"] - out["ma_min_5_20"]) / (out["close"] + 1e-9)

    out["ma20_slope_5d"] = g["ma20"].diff(5) / (g["ma20"].shift(5) + 1e-9)
    out["ma60_slope_10d"] = g["ma60"].diff(10) / (g["ma60"].shift(10) + 1e-9)

    # ATR-like 波動
    prev_close = g["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - prev_close).abs()
    tr3 = (out["low"] - prev_close).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["atr20"] = out.groupby("stock_id")["tr"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["atr_pct"] = out["atr20"] / (out["close"] + 1e-9)

    # K線實體與影線：控盤偵測
    out["body_pct"] = (out["close"] - out["open"]).abs() / (out["close"] + 1e-9)
    out["upper_shadow_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / (out["close"] + 1e-9)
    out["lower_shadow_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / (out["close"] + 1e-9)

    out["body20"] = out.groupby("stock_id")["body_pct"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["shadow20"] = out.groupby("stock_id")[["upper_shadow_pct", "lower_shadow_pct"]].sum(axis=1) if False else np.nan
    out["shadow_sum"] = out["upper_shadow_pct"].fillna(0) + out["lower_shadow_pct"].fillna(0)
    out["shadow20"] = out.groupby("stock_id")["shadow_sum"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)

    # KD / MACD 僅作確認
    low9 = g["low"].rolling(9, min_periods=5).min().reset_index(level=0, drop=True)
    high9 = g["high"].rolling(9, min_periods=5).max().reset_index(level=0, drop=True)
    rsv = (out["close"] - low9) / (high9 - low9 + 1e-9) * 100
    out["kd_k"] = rsv.groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_d"] = out["kd_k"].groupby(out["stock_id"]).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out["kd_cross"] = ((out["kd_k"] > out["kd_d"]) & (g["kd_k"].shift(1) <= g["kd_d"].shift(1))).astype(int)

    ema12 = g["close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out["macd_diff"] = ema12 - ema26
    out["macd_signal"] = out.groupby("stock_id")["macd_diff"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out["macd_hist"] = out["macd_diff"] - out["macd_signal"]
    out["macd_cross"] = ((out["macd_diff"] > out["macd_signal"]) & (g["macd_diff"].shift(1) <= g["macd_signal"].shift(1))).astype(int)

    return out


def strict_market_filter(latest: pd.DataFrame) -> pd.DataFrame:
    """
    嚴格版第一層：
    先確保這檔股票有主力能進出。
    """
    if latest.empty:
        return latest

    x = latest.copy()

    for col in ["close", "volume", "turnover_value"]:
        x[col] = _num(x[col])

    # 嚴格版硬條件
    x = x[
        (x["close"] >= 10) &
        (
            (x["volume"] >= 1000) |
            (x["turnover_value"] >= 30000)
        )
    ].copy()

    return x


def _score_clip(v, low=0, high=100):
    try:
        return float(np.clip(v, low, high))
    except Exception:
        return 0.0


def accumulation_score(row) -> tuple[float, str]:
    """
    吸籌分數：不是看一天，而是看 20~60 日是否有承接。
    """
    score = 0
    reasons = []

    low20 = row.get("low_20")
    low60 = row.get("low_60")
    ret20 = row.get("ret_20d")
    vr = row.get("volume_ratio")
    atr = row.get("atr_pct")
    ma20_slope = row.get("ma20_slope_5d")
    close = row.get("close")
    ma60 = row.get("ma60")

    # 低點不破 / 抬高
    if pd.notna(low20) and pd.notna(low60) and low20 >= low60 * 0.97:
        score += 25
        reasons.append("低點守住")

    # 下跌不再擴大
    if pd.notna(ret20) and ret20 >= -0.08:
        score += 15
        reasons.append("20日跌幅受控")

    # 量能穩定，不是死量也不是爆量
    if pd.notna(vr) and 0.75 <= vr <= 2.5:
        score += 20
        reasons.append("量能穩定")

    # 波動收縮
    if pd.notna(atr) and atr <= 0.045:
        score += 20
        reasons.append("波動收縮")

    # MA20 走平
    if pd.notna(ma20_slope) and ma20_slope >= -0.004:
        score += 10
        reasons.append("MA20走平")

    # 不離中期線太遠，避免破底股
    if pd.notna(close) and pd.notna(ma60) and close >= ma60 * 0.88:
        score += 10
        reasons.append("未遠離MA60")

    return _score_clip(score), "、".join(reasons) if reasons else "無吸籌"


def control_score(row) -> tuple[float, str]:
    """
    控盤分數：K線乾淨、波動小、均線收斂。
    """
    score = 0
    reasons = []

    ma_conv = row.get("ma_converge_pct")
    range20 = row.get("range_20")
    body20 = row.get("body20")
    shadow20 = row.get("shadow20")
    atr = row.get("atr_pct")

    if pd.notna(ma_conv) and ma_conv <= 0.07:
        score += 30
        reasons.append("均線收斂")

    if pd.notna(range20) and range20 <= 0.22:
        score += 25
        reasons.append("區間收斂")

    if pd.notna(body20) and body20 <= 0.025:
        score += 15
        reasons.append("K線實體小")

    if pd.notna(shadow20) and shadow20 <= 0.06:
        score += 15
        reasons.append("影線干擾低")

    if pd.notna(atr) and atr <= 0.045:
        score += 15
        reasons.append("ATR低波動")

    return _score_clip(score), "、".join(reasons) if reasons else "無控盤"


def test_move_score(row) -> tuple[float, str]:
    """
    試單分數：主力開始測市場反應，但不一定主升。
    """
    score = 0
    reasons = []

    vr = row.get("volume_ratio")
    ret1 = row.get("ret_1d")
    ret5 = row.get("ret_5d")
    close = row.get("close")
    ma20 = row.get("ma20")
    kd = row.get("kd_cross")
    macd = row.get("macd_cross")

    if pd.notna(vr) and 1.15 <= vr <= 4.0:
        score += 25
        reasons.append("量能試單")

    if pd.notna(ret1) and 0.005 <= ret1 <= 0.05:
        score += 20
        reasons.append("單日推升")

    if pd.notna(ret5) and ret5 > 0:
        score += 15
        reasons.append("短線轉正")

    if pd.notna(close) and pd.notna(ma20) and close >= ma20 * 0.98:
        score += 15
        reasons.append("靠近MA20")

    if kd == 1 or macd == 1:
        score += 25
        reasons.append("KD/MACD確認")

    return _score_clip(score), "、".join(reasons) if reasons else "無試單"


def pre_breakout_score(row) -> tuple[float, str]:
    """
    發動前兆：均線收斂後接近突破。
    """
    score = 0
    reasons = []

    close = row.get("close")
    ma20 = row.get("ma20")
    high20 = row.get("high_20")
    ma_conv = row.get("ma_converge_pct")
    ma20_slope = row.get("ma20_slope_5d")
    vr = row.get("volume_ratio")
    ret5 = row.get("ret_5d")

    if pd.notna(close) and pd.notna(ma20) and close >= ma20:
        score += 20
        reasons.append("站上MA20")

    if pd.notna(high20) and pd.notna(close) and close >= high20 * 0.96:
        score += 20
        reasons.append("接近20日高")

    if pd.notna(ma_conv) and ma_conv <= 0.08:
        score += 20
        reasons.append("均線糾結")

    if pd.notna(ma20_slope) and ma20_slope >= -0.002:
        score += 15
        reasons.append("MA20不弱")

    if pd.notna(vr) and vr >= 1.0:
        score += 10
        reasons.append("量能回溫")

    if pd.notna(ret5) and ret5 > 0:
        score += 15
        reasons.append("5日動能正")

    return _score_clip(score), "、".join(reasons) if reasons else "無發動前兆"


def classify_stage(row, scores):
    acc = scores["accumulation_score"]
    ctrl = scores["control_score"]
    test = scores["test_move_score"]
    pre = scores["pre_breakout_score"]
    final = scores["main_force_score"]

    if final >= 72 and pre >= 60 and test >= 45:
        return "BREAKOUT_READY", "🟢 發動前", "可小倉分批，等突破確認"
    if acc >= 65 and ctrl >= 65 and pre >= 45:
        return "LATENT_STRONG", "🟡 強潛伏", "主力潛伏末期，優先追蹤"
    if acc >= 55 and ctrl >= 55:
        return "LATENT", "🟡 潛伏", "有吸籌控盤，等待試單"
    if test >= 55 and pre >= 45:
        return "TEST", "🟠 試單", "主力試盤，隔日確認"
    if final >= 50:
        return "WATCH", "⚪ 觀察", "條件未完整，僅追蹤"
    return "SKIP", "❌ 排除", "無主力行為"


def score_latest(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty:
        return latest

    rows = []

    for _, row in latest.iterrows():
        acc, acc_reason = accumulation_score(row)
        ctrl, ctrl_reason = control_score(row)
        test, test_reason = test_move_score(row)
        pre, pre_reason = pre_breakout_score(row)

        final = (
            acc * 0.40 +
            ctrl * 0.30 +
            test * 0.20 +
            pre * 0.10
        )

        stage, action_label, action_note = classify_stage(row, {
            "accumulation_score": acc,
            "control_score": ctrl,
            "test_move_score": test,
            "pre_breakout_score": pre,
            "main_force_score": final,
        })

        r = row.to_dict()
        r.update({
            "accumulation_score": round(acc, 2),
            "control_score": round(ctrl, 2),
            "test_move_score": round(test, 2),
            "pre_breakout_score": round(pre, 2),
            "main_force_score": round(final, 2),
            "stage": stage,
            "action_label": action_label,
            "action_note": action_note,
            "accumulation_reason": acc_reason,
            "control_reason": ctrl_reason,
            "test_move_reason": test_reason,
            "pre_breakout_reason": pre_reason,
            "note": f"{action_note}｜吸籌{round(acc)} 控盤{round(ctrl)} 試單{round(test)} 前兆{round(pre)}",
        })
        rows.append(r)

    out = pd.DataFrame(rows)

    # 嚴格版：只留下有主力行為的標的
    out = out[out["stage"].isin(["BREAKOUT_READY", "LATENT_STRONG", "LATENT", "TEST", "WATCH"])].copy()

    # 排序：強潛伏 / 發動前優先
    stage_rank = {
        "BREAKOUT_READY": 0,
        "LATENT_STRONG": 1,
        "TEST": 2,
        "LATENT": 3,
        "WATCH": 4,
    }
    out["stage_rank"] = out["stage"].map(stage_rank).fillna(99)

    out = out.sort_values(
        ["stage_rank", "main_force_score", "accumulation_score", "control_score"],
        ascending=[True, False, False, False]
    )

    return out


def run_v3_core_engine(price_panel: pd.DataFrame, latest_date=None, top_n=30):
    """
    主入口：
    1. 準備特徵
    2. 取最新日
    3. 嚴格市場過濾
    4. 主力行為打分
    """
    feat = prepare_features(price_panel)
    if feat.empty:
        return pd.DataFrame(), pd.DataFrame([{
            "state": "empty_features",
            "input_count": len(price_panel) if price_panel is not None else 0,
            "filtered_count": 0,
            "selected_count": 0,
        }])

    if latest_date is None:
        latest_date = feat["date"].max()
    else:
        latest_date = pd.to_datetime(latest_date)

    latest = feat[feat["date"] == latest_date].copy()
    input_count = len(latest)

    filtered = strict_market_filter(latest)
    filtered_count = len(filtered)

    scored = score_latest(filtered).head(top_n).copy()

    debug = pd.DataFrame([{
        "state": "v3_0_core_engine_strict",
        "latest_date": str(latest_date.date()) if pd.notna(latest_date) else "",
        "input_count": int(input_count),
        "filtered_count": int(filtered_count),
        "selected_count": int(len(scored)),
        "breakout_ready_count": int((scored["stage"] == "BREAKOUT_READY").sum()) if len(scored) else 0,
        "latent_strong_count": int((scored["stage"] == "LATENT_STRONG").sum()) if len(scored) else 0,
        "latent_count": int((scored["stage"] == "LATENT").sum()) if len(scored) else 0,
        "test_count": int((scored["stage"] == "TEST").sum()) if len(scored) else 0,
    }])

    return scored, debug


if __name__ == "__main__":
    # standalone 測試：
    # python v3_0_core_engine.py
    import os

    candidates = [
        "price_panel_daily.csv",
        "data/price_panel_daily.csv",
        "mobile_dashboard_v1/data/price_panel_daily.csv",
    ]

    src = None
    for p in candidates:
        if os.path.exists(p):
            src = p
            break

    if src is None:
        print("找不到 price_panel_daily.csv")
    else:
        df = pd.read_csv(src)
        result, debug = run_v3_core_engine(df, top_n=30)
        result.to_csv("v3_core_candidates.csv", index=False, encoding="utf-8-sig")
        debug.to_csv("v3_core_debug.csv", index=False, encoding="utf-8-sig")
        print(debug.to_string(index=False))
        print(result[["stock_id", "close", "stage", "main_force_score", "note"]].head(30).to_string(index=False))
