"""
v3_0_core_engine.py
v3.3 行為 × 結構 引擎

核心目的：
v3.2 已經能偵測「主力有沒有動」。
v3.3 加入「主力在哪個型態位置動」。

新增 structure_score：
1. 波動壓縮：近10日波動 < 近30/60日波動
2. 均線糾結：MA5 / MA10 / MA20 貼近
3. 量縮整理：近5日量 < 近20日量，但不是死量
4. 低位準備：距離60日高點仍有空間，避免追高
5. 試突破/洗盤：接近高點但還沒正式噴

總分：
main_force_score =
  0.34 * accumulation_score
+ 0.24 * control_score
+ 0.22 * structure_score
+ 0.12 * test_move_score
+ 0.08 * pre_breakout_score

這版目標：
不是抓「已經漲的股票」，
而是抓「主力正在準備拉的股票」。
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
    if s.startswith(("00", "03", "04", "05", "06", "07", "08", "09")):
        return False
    return True


def _rolling_up_count(series: pd.Series, window: int) -> pd.Series:
    diff = series.diff()
    up = (diff > 0).astype(float)
    return up.rolling(window, min_periods=max(2, window // 2)).sum()


def _rolling_non_down_count(series: pd.Series, window: int) -> pd.Series:
    diff = series.diff()
    ok = (diff >= 0).astype(float)
    return ok.rolling(window, min_periods=max(2, window // 2)).sum()


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
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
        out[col] = _num(out[col])

    out["stock_id"] = out["stock_id"].apply(normalize_stock_id)
    out = out[out["stock_id"].apply(is_common_stock_id)].copy()
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)

    g = out.groupby("stock_id", group_keys=False)

    out["turnover_value"] = out["close"] * out["volume"]

    for n in [5, 10, 20, 30, 60]:
        out[f"ma{n}"] = g["close"].rolling(n, min_periods=max(5, min(n, 20))).mean().reset_index(level=0, drop=True)

    out["ret_1d"] = g["close"].pct_change(1)
    out["ret_3d"] = g["close"].pct_change(3)
    out["ret_5d"] = g["close"].pct_change(5)
    out["ret_10d"] = g["close"].pct_change(10)
    out["ret_20d"] = g["close"].pct_change(20)

    out["vol_ma5"] = g["volume"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    out["vol_ma10"] = g["volume"].rolling(10, min_periods=5).mean().reset_index(level=0, drop=True)
    out["vol_ma20"] = g["volume"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["vol_ma60"] = g["volume"].rolling(60, min_periods=20).mean().reset_index(level=0, drop=True)
    out["volume_ratio"] = out["volume"] / (out["vol_ma20"] + 1e-9)
    out["vol_dry_ratio"] = out["vol_ma5"] / (out["vol_ma20"] + 1e-9)

    for w in [10, 20, 30, 60]:
        out[f"high_{w}"] = g["high"].rolling(w, min_periods=max(5, w // 2)).max().reset_index(level=0, drop=True)
        out[f"low_{w}"] = g["low"].rolling(w, min_periods=max(5, w // 2)).min().reset_index(level=0, drop=True)
        out[f"range_{w}"] = (out[f"high_{w}"] - out[f"low_{w}"]) / (out["close"] + 1e-9)

    out["ma_max_5_20"] = out[["ma5", "ma10", "ma20"]].max(axis=1)
    out["ma_min_5_20"] = out[["ma5", "ma10", "ma20"]].min(axis=1)
    out["ma_converge_pct"] = (out["ma_max_5_20"] - out["ma_min_5_20"]) / (out["close"] + 1e-9)

    out["ma20_slope_5d"] = g["ma20"].diff(5) / (g["ma20"].shift(5) + 1e-9)
    out["ma60_slope_10d"] = g["ma60"].diff(10) / (g["ma60"].shift(10) + 1e-9)

    prev_close = g["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - prev_close).abs()
    tr3 = (out["low"] - prev_close).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    for w in [10, 20, 30]:
        out[f"atr{w}"] = out.groupby("stock_id")["tr"].rolling(w, min_periods=max(5, w // 2)).mean().reset_index(level=0, drop=True)
        out[f"atr{w}_pct"] = out[f"atr{w}"] / (out["close"] + 1e-9)

    out["body_pct"] = (out["close"] - out["open"]).abs() / (out["close"] + 1e-9)
    out["upper_shadow_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / (out["close"] + 1e-9)
    out["lower_shadow_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / (out["close"] + 1e-9)
    out["shadow_sum"] = out["upper_shadow_pct"].fillna(0) + out["lower_shadow_pct"].fillna(0)

    out["body20"] = out.groupby("stock_id")["body_pct"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)
    out["shadow20"] = out.groupby("stock_id")["shadow_sum"].rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)

    price_diff = g["close"].diff()
    signed_volume = np.where(price_diff > 0, out["volume"], np.where(price_diff < 0, -out["volume"], 0))
    out["signed_volume"] = signed_volume
    out["obv_proxy"] = out.groupby("stock_id")["signed_volume"].cumsum()

    for w in [3, 5, 10]:
        out[f"obv_up_count_{w}"] = out.groupby("stock_id")["obv_proxy"].transform(lambda s, ww=w: _rolling_up_count(s, ww))
        out[f"close_non_down_count_{w}"] = out.groupby("stock_id")["close"].transform(lambda s, ww=w: _rolling_non_down_count(s, ww))
        out[f"low_non_down_count_{w}"] = out.groupby("stock_id")["low"].transform(lambda s, ww=w: _rolling_non_down_count(s, ww))
        out[f"vol_ratio_mean_{w}"] = out.groupby("stock_id")["volume_ratio"].rolling(w, min_periods=max(2, w//2)).mean().reset_index(level=0, drop=True)

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
    if latest.empty:
        return latest
    x = latest.copy()
    for col in ["close", "volume", "turnover_value"]:
        x[col] = _num(x[col])

    x = x[
        (x["close"] >= 10) &
        (
            (x["volume"] >= 500) |
            (x["turnover_value"] >= 20000)
        )
    ].copy()
    return x


def _score_clip(v, low=0, high=100):
    try:
        return float(np.clip(v, low, high))
    except Exception:
        return 0.0


def accumulation_multi_scale_score(row) -> tuple[float, str]:
    scale_scores = []
    reasons = []

    s3 = 0
    if row.get("obv_up_count_3", 0) >= 2:
        s3 += 16
    if row.get("close_non_down_count_3", 0) >= 2:
        s3 += 10
    if row.get("low_non_down_count_3", 0) >= 2:
        s3 += 10
    vr3 = row.get("vol_ratio_mean_3")
    if pd.notna(vr3) and 0.75 <= vr3 <= 2.8:
        s3 += 8
    if s3 >= 30:
        scale_scores.append(("3日感知", min(s3, 40)))
        reasons.append("3日資金/價格轉穩")

    s5 = 0
    if row.get("obv_up_count_5", 0) >= 3:
        s5 += 22
    if row.get("close_non_down_count_5", 0) >= 3:
        s5 += 12
    if row.get("low_non_down_count_5", 0) >= 3:
        s5 += 12
    vr5 = row.get("vol_ratio_mean_5")
    if pd.notna(vr5) and 0.75 <= vr5 <= 2.5:
        s5 += 10
    if s5 >= 38:
        scale_scores.append(("5日感知", min(s5, 55)))
        reasons.append("5日穩定吸籌")

    s10 = 0
    if row.get("obv_up_count_10", 0) >= 6:
        s10 += 30
    if row.get("close_non_down_count_10", 0) >= 6:
        s10 += 15
    if row.get("low_non_down_count_10", 0) >= 6:
        s10 += 15
    vr10 = row.get("vol_ratio_mean_10")
    if pd.notna(vr10) and 0.65 <= vr10 <= 2.3:
        s10 += 12
    if s10 >= 48:
        scale_scores.append(("10日感知", min(s10, 70)))
        reasons.append("10日控盤吸籌")

    if not scale_scores:
        return 0.0, "無連續感知"

    best_name, best_score = max(scale_scores, key=lambda x: x[1])
    return _score_clip(best_score), f"{best_name}｜" + "、".join(reasons)


def accumulation_score(row) -> tuple[float, str]:
    base = 0
    reasons = []

    low20 = row.get("low_20")
    low60 = row.get("low_60")
    ret20 = row.get("ret_20d")
    vr = row.get("volume_ratio")
    atr = row.get("atr20_pct")
    ma20_slope = row.get("ma20_slope_5d")
    close = row.get("close")
    ma60 = row.get("ma60")

    if pd.notna(low20) and pd.notna(low60) and low20 >= low60 * 0.97:
        base += 18
        reasons.append("低點守住")
    if pd.notna(ret20) and ret20 >= -0.10:
        base += 12
        reasons.append("20日跌幅受控")
    if pd.notna(vr) and 0.65 <= vr <= 2.8:
        base += 14
        reasons.append("量能穩定")
    if pd.notna(atr) and atr <= 0.055:
        base += 14
        reasons.append("波動收縮")
    if pd.notna(ma20_slope) and ma20_slope >= -0.006:
        base += 8
        reasons.append("MA20走平")
    if pd.notna(close) and pd.notna(ma60) and close >= ma60 * 0.86:
        base += 8
        reasons.append("未遠離MA60")

    continuity, c_reason = accumulation_multi_scale_score(row)
    score = min(100, base + continuity)
    if continuity > 0:
        reasons.append(c_reason)

    return _score_clip(score), "、".join(reasons) if reasons else "無吸籌"


def control_score(row) -> tuple[float, str]:
    score = 0
    reasons = []

    ma_conv = row.get("ma_converge_pct")
    range20 = row.get("range_20")
    body20 = row.get("body20")
    shadow20 = row.get("shadow20")
    atr = row.get("atr20_pct")

    if pd.notna(ma_conv) and ma_conv <= 0.085:
        score += 30
        reasons.append("均線收斂")
    if pd.notna(range20) and range20 <= 0.26:
        score += 25
        reasons.append("區間收斂")
    if pd.notna(body20) and body20 <= 0.032:
        score += 15
        reasons.append("K線實體小")
    if pd.notna(shadow20) and shadow20 <= 0.075:
        score += 15
        reasons.append("影線干擾低")
    if pd.notna(atr) and atr <= 0.055:
        score += 15
        reasons.append("ATR低波動")

    return _score_clip(score), "、".join(reasons) if reasons else "無控盤"


def structure_score(row) -> tuple[float, str]:
    """
    v3.3 新增：型態位置層。
    核心：找「壓縮、量縮、低位、接近突破」。
    """
    score = 0
    reasons = []

    range10 = row.get("range_10")
    range30 = row.get("range_30")
    range60 = row.get("range_60")
    ma_conv = row.get("ma_converge_pct")
    vol_dry = row.get("vol_dry_ratio")
    close = row.get("close")
    high60 = row.get("high_60")
    high20 = row.get("high_20")
    ma20 = row.get("ma20")
    ma60 = row.get("ma60")
    atr10 = row.get("atr10_pct")
    atr30 = row.get("atr30_pct")
    ret20 = row.get("ret_20d")

    # 1. 波動壓縮：近10日比中期更窄
    if pd.notna(range10) and pd.notna(range30) and range10 < range30 * 0.82:
        score += 22
        reasons.append("10日波動壓縮")
    elif pd.notna(range10) and pd.notna(range60) and range10 < range60 * 0.70:
        score += 18
        reasons.append("相對60日壓縮")

    # 2. 均線糾結
    if pd.notna(ma_conv):
        if ma_conv <= 0.045:
            score += 25
            reasons.append("均線高度糾結")
        elif ma_conv <= 0.075:
            score += 18
            reasons.append("均線收斂")

    # 3. 量縮但不是死量
    if pd.notna(vol_dry):
        if 0.45 <= vol_dry <= 0.90:
            score += 18
            reasons.append("量縮整理")
        elif 0.90 < vol_dry <= 1.20:
            score += 8
            reasons.append("量能穩定")

    # 4. 還沒過熱：距離60日高點仍有空間
    if pd.notna(close) and pd.notna(high60) and high60 > 0:
        dist_from_high60 = close / high60
        if 0.70 <= dist_from_high60 <= 0.92:
            score += 16
            reasons.append("低位未過熱")
        elif 0.92 < dist_from_high60 <= 0.98:
            score += 10
            reasons.append("接近壓力區")

    # 5. 接近突破位，但尚未爆噴
    if pd.notna(close) and pd.notna(high20) and high20 > 0:
        if 0.93 <= close / high20 <= 1.01:
            score += 14
            reasons.append("接近20日突破位")

    # 6. 站回或貼近MA20
    if pd.notna(close) and pd.notna(ma20):
        if close >= ma20 * 0.98:
            score += 10
            reasons.append("貼近MA20")

    # 7. 不能是明顯破底弱勢
    if pd.notna(close) and pd.notna(ma60) and close < ma60 * 0.82:
        score -= 20
        reasons.append("遠低MA60扣分")

    # 8. 避免已經噴太多
    if pd.notna(ret20) and ret20 > 0.28:
        score -= 18
        reasons.append("20日過熱扣分")

    # 9. ATR 壓縮
    if pd.notna(atr10) and pd.notna(atr30) and atr10 < atr30 * 0.88:
        score += 12
        reasons.append("ATR壓縮")

    return _score_clip(score), "、".join(reasons) if reasons else "無結構優勢"


def test_move_score(row) -> tuple[float, str]:
    score = 0
    reasons = []

    vr = row.get("volume_ratio")
    ret1 = row.get("ret_1d")
    ret3 = row.get("ret_3d")
    ret5 = row.get("ret_5d")
    close = row.get("close")
    ma20 = row.get("ma20")
    kd = row.get("kd_cross")
    macd = row.get("macd_cross")

    if pd.notna(vr) and 1.03 <= vr <= 4.0:
        score += 22
        reasons.append("量能試單")
    if pd.notna(ret1) and 0.003 <= ret1 <= 0.06:
        score += 18
        reasons.append("單日推升")
    if pd.notna(ret3) and ret3 > 0:
        score += 12
        reasons.append("3日轉正")
    if pd.notna(ret5) and ret5 > -0.005:
        score += 12
        reasons.append("5日未弱")
    if pd.notna(close) and pd.notna(ma20) and close >= ma20 * 0.97:
        score += 14
        reasons.append("靠近MA20")
    if kd == 1 or macd == 1:
        score += 22
        reasons.append("KD/MACD確認")

    return _score_clip(score), "、".join(reasons) if reasons else "無試單"


def pre_breakout_score(row) -> tuple[float, str]:
    score = 0
    reasons = []

    close = row.get("close")
    ma20 = row.get("ma20")
    high20 = row.get("high_20")
    ma_conv = row.get("ma_converge_pct")
    ma20_slope = row.get("ma20_slope_5d")
    vr = row.get("volume_ratio")
    ret5 = row.get("ret_5d")

    if pd.notna(close) and pd.notna(ma20) and close >= ma20 * 0.985:
        score += 20
        reasons.append("貼近/站上MA20")
    if pd.notna(high20) and pd.notna(close) and close >= high20 * 0.94:
        score += 20
        reasons.append("接近20日高")
    if pd.notna(ma_conv) and ma_conv <= 0.09:
        score += 20
        reasons.append("均線糾結")
    if pd.notna(ma20_slope) and ma20_slope >= -0.004:
        score += 15
        reasons.append("MA20不弱")
    if pd.notna(vr) and vr >= 0.85:
        score += 10
        reasons.append("量能未死")
    if pd.notna(ret5) and ret5 > -0.005:
        score += 15
        reasons.append("5日不弱")

    return _score_clip(score), "、".join(reasons) if reasons else "無發動前兆"


def classify_stage(row, scores):
    acc = scores["accumulation_score"]
    ctrl = scores["control_score"]
    struct = scores["structure_score"]
    test = scores["test_move_score"]
    pre = scores["pre_breakout_score"]
    final = scores["main_force_score"]

    # v3.3：強調「行為 × 結構」
    if final >= 70 and struct >= 58 and pre >= 55 and test >= 38:
        return "BREAKOUT_READY", "🟢 發動前", "結構壓縮後接近突破，可小倉"
    if acc >= 68 and ctrl >= 55 and struct >= 60:
        return "LATENT_STRONG", "🟡 強潛伏", "吸籌控盤且結構壓縮，優先卡位"
    if struct >= 68 and acc >= 55:
        return "STRUCTURE_READY", "🔵 結構待發", "型態位置漂亮，等待試單"
    if acc >= 56 and ctrl >= 45 and struct >= 45:
        return "LATENT", "🟡 潛伏", "有吸籌控盤，等待確認"
    if test >= 48 and pre >= 40:
        return "TEST", "🟠 試單", "主力試盤，隔日確認"
    if final >= 48:
        return "WATCH", "⚪ 觀察", "條件未完整，僅追蹤"
    return "SKIP", "❌ 排除", "無主力行為"


def score_latest(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty:
        return latest

    rows = []
    for _, row in latest.iterrows():
        acc, acc_reason = accumulation_score(row)
        ctrl, ctrl_reason = control_score(row)
        struct, struct_reason = structure_score(row)
        test, test_reason = test_move_score(row)
        pre, pre_reason = pre_breakout_score(row)

        final = (
            acc * 0.34 +
            ctrl * 0.24 +
            struct * 0.22 +
            test * 0.12 +
            pre * 0.08
        )

        stage, action_label, action_note = classify_stage(row, {
            "accumulation_score": acc,
            "control_score": ctrl,
            "structure_score": struct,
            "test_move_score": test,
            "pre_breakout_score": pre,
            "main_force_score": final,
        })

        r = row.to_dict()
        r.update({
            "accumulation_score": round(acc, 2),
            "control_score": round(ctrl, 2),
            "structure_score": round(struct, 2),
            "test_move_score": round(test, 2),
            "pre_breakout_score": round(pre, 2),
            "main_force_score": round(final, 2),
            "stage": stage,
            "action_label": action_label,
            "action_note": action_note,
            "accumulation_reason": acc_reason,
            "control_reason": ctrl_reason,
            "structure_reason": struct_reason,
            "test_move_reason": test_reason,
            "pre_breakout_reason": pre_reason,
            "note": f"{action_note}｜吸籌{round(acc)} 控盤{round(ctrl)} 結構{round(struct)} 試單{round(test)} 前兆{round(pre)}",
        })
        rows.append(r)

    out = pd.DataFrame(rows)
    out = out[out["stage"].isin(["BREAKOUT_READY", "LATENT_STRONG", "STRUCTURE_READY", "LATENT", "TEST", "WATCH"])].copy()

    stage_rank = {
        "BREAKOUT_READY": 0,
        "LATENT_STRONG": 1,
        "STRUCTURE_READY": 2,
        "TEST": 3,
        "LATENT": 4,
        "WATCH": 5,
    }
    out["stage_rank"] = out["stage"].map(stage_rank).fillna(99)

    out = out.sort_values(
        ["stage_rank", "main_force_score", "structure_score", "accumulation_score", "control_score"],
        ascending=[True, False, False, False, False]
    )

    return out


def run_v3_core_engine(price_panel: pd.DataFrame, latest_date=None, top_n=40):
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
        "state": "v3_3_behavior_structure_engine",
        "latest_date": str(latest_date.date()) if pd.notna(latest_date) else "",
        "input_count": int(input_count),
        "filtered_count": int(filtered_count),
        "selected_count": int(len(scored)),
        "breakout_ready_count": int((scored["stage"] == "BREAKOUT_READY").sum()) if len(scored) else 0,
        "latent_strong_count": int((scored["stage"] == "LATENT_STRONG").sum()) if len(scored) else 0,
        "structure_ready_count": int((scored["stage"] == "STRUCTURE_READY").sum()) if len(scored) else 0,
        "latent_count": int((scored["stage"] == "LATENT").sum()) if len(scored) else 0,
        "test_count": int((scored["stage"] == "TEST").sum()) if len(scored) else 0,
        "watch_count": int((scored["stage"] == "WATCH").sum()) if len(scored) else 0,
    }]）
    return scored, debug


if __name__ == "__main__":
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
        result, debug = run_v3_core_engine(df, top_n=40)
        result.to_csv("v3_core_candidates.csv", index=False, encoding="utf-8-sig")
        debug.to_csv("v3_core_debug.csv", index=False, encoding="utf-8-sig")
        print(debug.to_string(index=False))
        if len(result):
            print(result[["stock_id", "close", "stage", "main_force_score", "note"]].head(40).to_string(index=False))
        else:
            print("no v3.3 candidates")
