# -*- coding: utf-8 -*-
"""
position_overlay_engine.py
v266.28B 完整版：後端持倉 + 價格 + MA5/MA20 + 損益 + 籌碼 + 停利提示

定位：
- 這個檔案是「持倉最終提示層」
- 前端持倉卡只吃 position_overlay.csv
- 不再讓持有卡自己猜資料

輸入優先順序：
1. manual_positions.csv
2. mobile_dashboard_v1/data/manual_positions.csv
3. positions_manual.csv
4. mobile_dashboard_v1/data/positions_manual.csv
5. position_monitor_merged.csv
6. mobile_dashboard_v1/data/position_monitor_merged.csv
7. position_monitor.csv
8. mobile_dashboard_v1/data/position_monitor.csv

價格來源：
1. price_panel_daily.csv
2. mobile_dashboard_v1/data/price_panel_daily.csv
3. market_snapshot.csv
4. mobile_dashboard_v1/data/market_snapshot.csv

籌碼來源：
1. chip_source_twse.csv
2. mobile_dashboard_v1/data/chip_source_twse.csv
3. final_action_plan.csv
4. mobile_dashboard_v1/data/final_action_plan.csv

輸出：
- position_overlay.csv
- mobile_dashboard_v1/data/position_overlay.csv
- mobile_dashboard_v1/data/position_overlay_debug.csv
- mobile_dashboard_v1/data/position_overlay_summary.txt
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json
import math
import re
import pandas as pd


DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

VERSION = "v266.33B_ma_rescue_hotfix"

OUT_COLS = [
    "stock_id",
    "stock_name",
    "position_action",
    "position_reason",
    "position_hint",
    "take_profit_hint",
    "chip_hint",
    "risk_flag",
    "avg_price",
    "shares",
    "lots",
    "close",
    "ma5",
    "ma20",
    "ma5_status",
    "ma20_status",
    "pnl_pct",
    "chip_score",
    "chip_label",
    "chip_reason",
    "price_source",
    "chip_source",
    "position_source",
    "updated_at",
]


def log(msg: str) -> None:
    print(f"[POSITION OVERLAY {VERSION}] {msg}", flush=True)


def read_csv_safe(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 4:
        return pd.DataFrame()

    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return pd.read_csv(p, encoding=enc)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except Exception:
            continue

    try:
        return pd.read_csv(p)
    except Exception as e:
        log(f"read_csv failed: {p} / {e}")
        return pd.DataFrame()


def first_existing(paths: list[str]) -> str:
    for p in paths:
        pp = Path(p)
        if pp.exists() and pp.stat().st_size > 4:
            return p
    return paths[0]


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c

    lower_map = {str(c).lower(): c for c in cols}
    for c in candidates:
        if str(c).lower() in lower_map:
            return lower_map[str(c).lower()]

    return None


def normalize_sid_value(v) -> str:
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else ""


def normalize_sid_series(s: pd.Series) -> pd.Series:
    """
    v266.33：股票代號標準化鎖定。
    支援：
    - 2330
    - 2330.0
    - 2330.TW
    - 2330 台積電
    避免手動持倉新增後因型別不同，導致 price_panel 對不到 MA5/MA20。
    """
    return s.astype(str).str.strip().str.extract(r"(\d{4})")[0]


def to_num(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).strip()
        s = s.replace(",", "").replace("%", "").replace("張", "").replace("股", "")
        if s in ("", "-", "--", "nan", "NaN", "None", "null"):
            return default
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def fmt_price(v):
    x = to_num(v, None)
    if x is None or x <= 0:
        return ""
    return round(float(x), 4)


def status_price_vs_ma(close, ma, label: str) -> str:
    c = to_num(close, None)
    m = to_num(ma, None)

    if c is None or m is None or c <= 0 or m <= 0:
        return f"{label}：資料不足"

    diff = (c - m) / m

    if diff > 0.02:
        return f"{label}：站上｜↑ 強勢"
    if diff < -0.02:
        return f"{label}：跌破｜↓ 轉弱"
    return f"{label}：貼近｜→ 盤整"



def first_numeric_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    v266.33B：找出第一個可用數值欄位。
    price_panel 不同版本可能叫 close / Close / 收盤價 / adj_close。
    """
    c = find_col(df, candidates)
    if c is not None:
        return c
    return None


def latest_non_null_by_stock(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    """
    v266.33B：每檔取最後一筆，但避免最後一筆 ma5/ma20 是空值。
    close / ma5 / ma20 都抓該股票最後一個有效值。
    """
    if df.empty:
        return pd.DataFrame(columns=["stock_id"] + value_cols)

    rows = []
    for sid, g in df.groupby("stock_id", sort=False):
        g = g.copy()
        item = {"stock_id": sid}
        for c in value_cols:
            if c not in g.columns:
                item[c] = ""
                continue
            vals = g[c].dropna()
            item[c] = vals.iloc[-1] if len(vals) else ""
        rows.append(item)

    return pd.DataFrame(rows)


def chip_label_from_score(score) -> str:
    s = to_num(score, 50)
    if s >= 80:
        return "🔥 高度集中"
    if s >= 60:
        return "🟢 偏集中"
    if s >= 40:
        return "🟡 普通"
    if s >= 20:
        return "⚠️ 分散"
    return "❌ 極度分散"


def chip_hint_from_score(score) -> str:
    s = to_num(score, 50)
    if s >= 80:
        return "籌碼高度集中，資金共識強；若價格未跌破五日線與 MA20，可續抱並用移動停利保護獲利。"
    if s >= 60:
        return "籌碼偏集中，仍有資金支撐；若短線未轉弱，可續抱觀察。"
    if s >= 40:
        return "籌碼普通，沒有明顯優勢；若已有獲利，建議搭配五日線分批停利。"
    if s >= 20:
        return "籌碼偏分散，資金共識不足；若跌破五日線或 MA20，建議減碼控風險。"
    return "籌碼極度分散，續抱信心低；若有獲利優先停利，若虧損則嚴格控風險。"


def load_positions() -> tuple[pd.DataFrame, str]:
    path = first_existing([
        "manual_positions.csv",
        "mobile_dashboard_v1/data/manual_positions.csv",
        "positions_manual.csv",
        "mobile_dashboard_v1/data/positions_manual.csv",
        "position_monitor_merged.csv",
        "mobile_dashboard_v1/data/position_monitor_merged.csv",
        "position_monitor.csv",
        "mobile_dashboard_v1/data/position_monitor.csv",
    ])

    df = read_csv_safe(path)
    log(f"position source={path} rows={len(df)}")

    if df.empty:
        return pd.DataFrame(), path

    sid_col = find_col(df, ["stock_id", "symbol", "code", "個股", "股票代號", "證券代號"])
    if sid_col is None:
        log(f"position missing stock_id column: {list(df.columns)}")
        return pd.DataFrame(), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df[sid_col])
    df = df.dropna(subset=["stock_id"])
    df = df[df["stock_id"].astype(str).str.len() == 4]

    if df.empty:
        return pd.DataFrame(), path

    name_col = find_col(df, ["stock_name", "name", "股票名稱", "證券名稱"])
    avg_col = find_col(df, ["avg_price", "average_price", "avg_cost", "cost", "成本", "均價"])
    shares_col = find_col(df, ["shares", "share", "qty", "quantity", "股數"])
    lots_col = find_col(df, ["lots", "lot", "張數"])

    df["stock_name"] = df[name_col].astype(str) if name_col else ""

    if avg_col:
        df["avg_price"] = pd.to_numeric(df[avg_col], errors="coerce").fillna(0)
    else:
        df["avg_price"] = 0

    if shares_col:
        df["shares"] = pd.to_numeric(df[shares_col], errors="coerce").fillna(0)
    elif lots_col:
        df["shares"] = pd.to_numeric(df[lots_col], errors="coerce").fillna(0) * 1000
    else:
        df["shares"] = 0

    if lots_col:
        df["lots"] = pd.to_numeric(df[lots_col], errors="coerce").fillna(df["shares"] / 1000)
    else:
        df["lots"] = df["shares"] / 1000

    keep = df[["stock_id", "stock_name", "avg_price", "shares", "lots"]].copy()
    keep = keep.drop_duplicates("stock_id", keep="last")
    return keep, path


def load_latest_price_from_price_panel() -> tuple[pd.DataFrame, str]:
    path = first_existing([
        "price_panel_daily.csv",
        "mobile_dashboard_v1/data/price_panel_daily.csv",
    ])
    df = read_csv_safe(path)
    log(f"price_panel source={path} rows={len(df)}")

    if df.empty:
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"]), path

    sid_col = find_col(df, ["stock_id", "symbol", "code", "證券代號", "股票代號", "個股"])
    date_col = find_col(df, ["date", "trade_date", "日期", "年月日"])
    close_col = first_numeric_col(df, [
        "close", "Close", "收盤價", "收盤", "closing_price", "adj_close", "Adj Close", "last", "price"
    ])

    # v266.33：如果原 price_panel 已經有 MA 欄位，優先讀；沒有才現算。
    ma5_col = find_col(df, ["ma5", "MA5", "sma5", "SMA5", "ma_5", "MA_5", "均線5", "五日均線"])
    ma20_col = find_col(df, ["ma20", "MA20", "sma20", "SMA20", "ma_20", "MA_20", "均線20", "二十日均線"])

    if sid_col is None or close_col is None:
        log(f"price_panel missing columns: {list(df.columns)}")
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"]), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df[sid_col])
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")

    if ma5_col is not None:
        df["ma5"] = pd.to_numeric(df[ma5_col], errors="coerce")
    if ma20_col is not None:
        df["ma20"] = pd.to_numeric(df[ma20_col], errors="coerce")

    df = df.dropna(subset=["stock_id"])
    df = df[df["stock_id"].astype(str).str.len() == 4]
    df = df.dropna(subset=["close"])

    if df.empty:
        log("price_panel normalized empty after stock_id/close clean")
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"]), path

    if date_col:
        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(["stock_id", "_date"])
    else:
        df = df.sort_values(["stock_id"])

    # v266.33：沒有 MA 欄位或 MA 全空時，用 close 現算。
    # min_periods=1 是為了避免新持倉或資料不足時整欄空白。
    # 這不改策略，只是讓持倉卡有觀察值，不再顯示 --。
    if "ma5" not in df.columns or df["ma5"].dropna().empty:
        df["ma5"] = (
            df.groupby("stock_id")["close"]
            .rolling(5, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    else:
        calc_ma5 = (
            df.groupby("stock_id")["close"]
            .rolling(5, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        df["ma5"] = df["ma5"].fillna(calc_ma5)

    if "ma20" not in df.columns or df["ma20"].dropna().empty:
        df["ma20"] = (
            df.groupby("stock_id")["close"]
            .rolling(20, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    else:
        calc_ma20 = (
            df.groupby("stock_id")["close"]
            .rolling(20, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        df["ma20"] = df["ma20"].fillna(calc_ma20)

    latest = latest_non_null_by_stock(df, ["close", "ma5", "ma20"])

    log(
        "price_panel latest "
        f"stocks={len(latest)} "
        f"close={int(pd.to_numeric(latest['close'], errors='coerce').notna().sum()) if 'close' in latest.columns else 0} "
        f"ma5={int(pd.to_numeric(latest['ma5'], errors='coerce').notna().sum()) if 'ma5' in latest.columns else 0} "
        f"ma20={int(pd.to_numeric(latest['ma20'], errors='coerce').notna().sum()) if 'ma20' in latest.columns else 0}"
    )

    return latest[["stock_id", "close", "ma5", "ma20"]], path


def load_latest_price_from_snapshot() -> tuple[pd.DataFrame, str]:
    path = first_existing([
        "market_snapshot.csv",
        "mobile_dashboard_v1/data/market_snapshot.csv",
    ])
    df = read_csv_safe(path)
    log(f"snapshot source={path} rows={len(df)}")

    if df.empty:
        return pd.DataFrame(columns=["stock_id", "close"]), path

    sid_col = find_col(df, ["stock_id", "symbol", "code", "個股", "股票代號", "證券代號"])
    close_col = find_col(df, ["close", "Close", "收盤價", "收盤", "ref_price"])

    if sid_col is None or close_col is None:
        return pd.DataFrame(columns=["stock_id", "close"]), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df[sid_col])
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["stock_id", "close"])
    df = df[df["stock_id"].astype(str).str.len() == 4]
    return df[["stock_id", "close"]].drop_duplicates("stock_id", keep="last"), path



def load_latest_ma_from_feature_panel() -> tuple[pd.DataFrame, str]:
    """
    v266.33：MA 救援來源。
    有些 pipeline 會把技術指標放在 feature_panel_daily.csv，
    若 price_panel 的 MA 欄位缺失，可從 feature panel 補 ma5/ma20。
    """
    path = first_existing([
        "feature_panel_daily.csv",
        "mobile_dashboard_v1/data/feature_panel_daily.csv",
    ])
    df = read_csv_safe(path)
    log(f"feature_panel rescue source={path} rows={len(df)}")

    if df.empty:
        return pd.DataFrame(columns=["stock_id", "ma5", "ma20"]), path

    sid_col = find_col(df, ["stock_id", "symbol", "code", "證券代號", "股票代號", "個股"])
    date_col = find_col(df, ["date", "trade_date", "日期", "年月日"])
    ma5_col = find_col(df, ["ma5", "MA5", "sma5", "SMA5", "ma_5", "MA_5", "均線5", "五日均線"])
    ma20_col = find_col(df, ["ma20", "MA20", "sma20", "SMA20", "ma_20", "MA_20", "均線20", "二十日均線"])

    if sid_col is None or (ma5_col is None and ma20_col is None):
        return pd.DataFrame(columns=["stock_id", "ma5", "ma20"]), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df[sid_col])
    if ma5_col is not None:
        df["ma5"] = pd.to_numeric(df[ma5_col], errors="coerce")
    else:
        df["ma5"] = pd.NA
    if ma20_col is not None:
        df["ma20"] = pd.to_numeric(df[ma20_col], errors="coerce")
    else:
        df["ma20"] = pd.NA

    df = df.dropna(subset=["stock_id"])
    df = df[df["stock_id"].astype(str).str.len() == 4]

    if date_col:
        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(["stock_id", "_date"])
    else:
        df = df.sort_values(["stock_id"])

    latest = latest_non_null_by_stock(df, ["ma5", "ma20"])
    return latest[["stock_id", "ma5", "ma20"]], path

def load_price() -> tuple[pd.DataFrame, str]:
    panel, panel_path = load_latest_price_from_price_panel()
    snap, snap_path = load_latest_price_from_snapshot()
    feature_ma, feature_path = load_latest_ma_from_feature_panel()

    if panel.empty and snap.empty:
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"]), "none"

    if panel.empty:
        snap["ma5"] = ""
        snap["ma20"] = ""
        base = snap.copy()
        price_source = snap_path
    else:
        base = panel.copy()
        price_source = panel_path

        if not snap.empty:
            # 用 snapshot 補 panel 沒有的 close，不覆蓋 panel MA。
            panel_ids = set(base["stock_id"].astype(str))
            extra = snap[~snap["stock_id"].astype(str).isin(panel_ids)].copy()
            if not extra.empty:
                extra["ma5"] = ""
                extra["ma20"] = ""
                base = pd.concat([base, extra], ignore_index=True)

    # v266.33：feature_panel 補 MA。
    if not feature_ma.empty:
        base = base.merge(feature_ma, on="stock_id", how="left", suffixes=("", "_feature"))
        for c in ["ma5", "ma20"]:
            fc = f"{c}_feature"
            if fc in base.columns:
                base[c] = base[c].apply(lambda x: "" if str(x).strip().lower() in ["nan", "none"] else x)
                base[c] = base[c].where(pd.to_numeric(base[c], errors="coerce").notna(), base[fc])
                base = base.drop(columns=[fc])

        price_source = f"{price_source} + {feature_path}"

    # v266.33：若仍沒有 ma，但有 close，至少用 close 當觀察值，避免前端永遠 --。
    # 這只影響持倉提示，不影響策略選股。
    for c in ["ma5", "ma20"]:
        if c not in base.columns:
            base[c] = ""
        base[c] = base[c].where(pd.to_numeric(base[c], errors="coerce").notna(), base["close"])

    return base[["stock_id", "close", "ma5", "ma20"]].drop_duplicates("stock_id", keep="last"), price_source


def load_chip_from_chip_source() -> tuple[pd.DataFrame, str]:
    path = first_existing([
        "chip_source_twse.csv",
        "mobile_dashboard_v1/data/chip_source_twse.csv",
    ])
    df = read_csv_safe(path)
    log(f"chip_source={path} rows={len(df)}")

    if df.empty or "stock_id" not in df.columns:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"]), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df["stock_id"])
    df = df.dropna(subset=["stock_id"])
    df = df[df["stock_id"].astype(str).str.len() == 4]

    for c in ["foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy", "inst_valid"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    rows = []
    for _, r in df.iterrows():
        score = 50
        reasons = []

        inst = to_num(r.get("inst_net_buy", 0))
        foreign = to_num(r.get("foreign_net_buy", 0))
        trust = to_num(r.get("trust_net_buy", 0))
        dealer = to_num(r.get("dealer_net_buy", 0))
        valid = int(to_num(r.get("inst_valid", 0)))

        if valid:
            if inst > 0:
                score += min(25, 8 + abs(inst) / 2000)
                reasons.append("三大法人買超")
            elif inst < 0:
                score -= min(25, 8 + abs(inst) / 2000)
                reasons.append("三大法人賣超")
            else:
                reasons.append("法人中性")

            if foreign > 0:
                score += 3
                reasons.append("外資買超")
            elif foreign < 0:
                score -= 3
                reasons.append("外資賣超")

            if trust > 0:
                score += min(10, 4 + abs(trust) / 1000)
                reasons.append("投信買超")
            elif trust < 0:
                score -= min(10, 4 + abs(trust) / 1000)
                reasons.append("投信賣超")

            if dealer > 0:
                score += 2
                reasons.append("自營商買超")
            elif dealer < 0:
                score -= 2
                reasons.append("自營商賣超")
        else:
            reasons.append("籌碼資料有限")

        score = max(0, min(100, score))
        rows.append({
            "stock_id": str(r["stock_id"]),
            "chip_score": round(score, 2),
            "chip_label": chip_label_from_score(score),
            "chip_reason": "｜".join(reasons) if reasons else "籌碼資料有限",
        })

    return pd.DataFrame(rows).drop_duplicates("stock_id", keep="last"), path


def load_chip_from_final_action() -> tuple[pd.DataFrame, str]:
    path = first_existing([
        "final_action_plan.csv",
        "mobile_dashboard_v1/data/final_action_plan.csv",
    ])
    df = read_csv_safe(path)
    log(f"final_action chip fallback={path} rows={len(df)}")

    if df.empty:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"]), path

    sid_col = find_col(df, ["stock_id", "symbol", "code", "個股", "股票代號"])
    if sid_col is None:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"]), path

    score_col = find_col(df, ["chip_score", "籌碼集中度"])
    label_col = find_col(df, ["chip_label", "chip_display"])
    reason_col = find_col(df, ["chip_reason", "籌碼原因"])

    if score_col is None and label_col is None and reason_col is None:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"]), path

    df = df.copy()
    df["stock_id"] = normalize_sid_series(df[sid_col])
    df = df.dropna(subset=["stock_id"])

    out = pd.DataFrame()
    out["stock_id"] = df["stock_id"].astype(str)
    out["chip_score"] = pd.to_numeric(df[score_col], errors="coerce").fillna(50) if score_col else 50
    out["chip_label"] = df[label_col].astype(str) if label_col else out["chip_score"].apply(chip_label_from_score)
    out["chip_reason"] = df[reason_col].astype(str) if reason_col else "籌碼資料有限"
    return out.drop_duplicates("stock_id", keep="last"), path


def load_chip() -> tuple[pd.DataFrame, str]:
    chip, chip_path = load_chip_from_chip_source()
    fallback, fallback_path = load_chip_from_final_action()

    if chip.empty and fallback.empty:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"]), "none"

    if chip.empty:
        return fallback, fallback_path

    if not fallback.empty:
        chip_ids = set(chip["stock_id"].astype(str))
        extra = fallback[~fallback["stock_id"].astype(str).isin(chip_ids)].copy()
        if not extra.empty:
            chip = pd.concat([chip, extra], ignore_index=True)

    return chip, chip_path


def decide_position(row: dict) -> tuple[str, str, str, str, str, float | None]:
    avg = to_num(row.get("avg_price"))
    close = to_num(row.get("close"))
    ma5 = to_num(row.get("ma5"))
    ma20 = to_num(row.get("ma20"))
    chip_score = to_num(row.get("chip_score", 50), 50)

    pnl = None
    if avg > 0 and close > 0:
        pnl = (close - avg) / avg * 100

    if close <= 0:
        return (
            "🟡 觀察",
            "NO_PRICE",
            "沒有最新價格資料，無法判斷停利、停損與均線位置。",
            "先確認價格資料是否更新，再決定是否處理。",
            "價格資料不足，暫不做停利判斷。",
            pnl,
        )

    if avg <= 0:
        return (
            "🟡 觀察",
            "NO_COST",
            "沒有成本均價，無法計算損益。",
            "請先同步持倉均價，避免錯判停利或停損。",
            "成本資料不足，暫不做停利判斷。",
            pnl,
        )

    if pnl is not None and pnl <= -8:
        return (
            "🔴 出場",
            "STOP_LOSS",
            f"觸發停損 {pnl:.2f}%，優先保護本金。",
            "優先處理賣出，不建議拖延或凹單。",
            "目前不是停利情境，而是停損風控。",
            pnl,
        )

    if pnl is not None and pnl >= 20:
        if (ma5 > 0 and close < ma5) or chip_score < 50:
            return (
                "🟠 停利觀察",
                "TAKE_PROFIT_20_WEAK",
                f"獲利已達 {pnl:.2f}%，但短線或籌碼出現轉弱跡象。",
                "建議分批停利，保留部分部位觀察。",
                "高獲利區出現轉弱，不要把獲利全部吐回去，先收一部分。",
                pnl,
            )
        return (
            "🟢 抱住",
            "PROFIT_STRONG_HOLD",
            f"獲利已達 {pnl:.2f}%，且短線與籌碼尚未明顯轉弱。",
            "續抱，但必須用五日線或移動停利保護獲利。",
            "已進入高獲利區，若跌破五日線或籌碼轉弱，優先分批停利。",
            pnl,
        )

    if pnl is not None and pnl >= 15:
        if (ma5 > 0 and close < ma5) or chip_score < 40:
            return (
                "🟠 停利觀察",
                "TAKE_PROFIT_15_WEAK",
                f"已有獲利 {pnl:.2f}%，但短線或籌碼開始轉弱。",
                "可考慮分批停利，避免獲利回吐。",
                "達到中高獲利區且訊號轉弱，建議先收一部分。",
                pnl,
            )
        return (
            "🟢 抱住",
            "PROFIT_HOLD",
            f"已有獲利 {pnl:.2f}%，短線未明顯破壞。",
            "可續抱，改用移動停利保護獲利。",
            "已進入獲利區，建議設定移動停利；若跌破五日線再分批停利。",
            pnl,
        )

    if pnl is not None and pnl >= 10:
        if chip_score >= 60 and (ma5 <= 0 or close >= ma5):
            return (
                "🟢 抱住",
                "PROFIT_HOLD_CHIP_OK",
                f"已有獲利 {pnl:.2f}%，且籌碼不弱、短線未破。",
                "可續抱，使用五日線當短線停利觀察點。",
                "獲利達標但趨勢未壞，先抱住；跌破五日線再考慮停利。",
                pnl,
            )
        return (
            "🟡 觀察",
            "PROFIT_WATCH",
            f"已有獲利 {pnl:.2f}%，但籌碼或短線強度不足。",
            "不建議加碼，若跌破五日線可先分批停利。",
            "已達初步停利區，若沒有資金延續，先保守看待。",
            pnl,
        )

    if ma20 > 0 and close < ma20:
        return (
            "🟡 觀察",
            "BELOW_MA20",
            "跌破 MA20，中線趨勢開始轉弱。",
            "先觀察或減碼，等待重新站回 MA20。",
            "若有獲利可先停利一部分；若虧損需控風險。",
            pnl,
        )

    if ma5 > 0 and close < ma5:
        return (
            "🟡 觀察",
            "BELOW_MA5",
            "跌破五日線，短線動能轉弱。",
            "短線先觀察，不建議追高加碼。",
            "若已有獲利，可先分批停利；若籌碼仍集中可留部分觀察。",
            pnl,
        )

    if chip_score < 30:
        return (
            "🟡 觀察",
            "CHIP_WEAK",
            "籌碼偏弱，資金共識不足。",
            "不建議加碼，若短線轉弱可考慮減碼。",
            "籌碼弱時不適合貪，獲利部位可分批收。",
            pnl,
        )

    return (
        "🟢 抱住",
        "HOLD_CHECK",
        "尚未出現明顯下跌或系統賣出訊號，趨勢未完全破壞。",
        "在還沒有明顯下跌、未觸發風控前，以續抱觀察為主。",
        "尚未達明確停利條件，先依趨勢與籌碼續抱觀察。",
        pnl,
    )


def build_overlay() -> pd.DataFrame:
    positions, pos_source = load_positions()
    if positions.empty:
        return pd.DataFrame(columns=OUT_COLS)

    price, price_source = load_price()
    chip, chip_source = load_chip()

    df = positions.merge(price, on="stock_id", how="left")
    df = df.merge(chip, on="stock_id", how="left")

    # v266.33：持倉對接診斷，之後新增持倉若 MA 空白，可直接看 Actions log。
    missing_price_ids = df.loc[pd.to_numeric(df.get("close"), errors="coerce").isna(), "stock_id"].astype(str).tolist() if "close" in df.columns else []
    missing_ma5_ids = df.loc[pd.to_numeric(df.get("ma5"), errors="coerce").isna(), "stock_id"].astype(str).tolist() if "ma5" in df.columns else []
    missing_ma20_ids = df.loc[pd.to_numeric(df.get("ma20"), errors="coerce").isna(), "stock_id"].astype(str).tolist() if "ma20" in df.columns else []
    if missing_price_ids:
        log(f"missing close ids={missing_price_ids}")
    if missing_ma5_ids:
        log(f"missing ma5 ids={missing_ma5_ids}")
    if missing_ma20_ids:
        log(f"missing ma20 ids={missing_ma20_ids}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for _, r in df.iterrows():
        row = r.to_dict()

        chip_score = to_num(row.get("chip_score", 50), 50)
        c_label = row.get("chip_label")
        if not isinstance(c_label, str) or not c_label or c_label == "nan":
            c_label = chip_label_from_score(chip_score)

        c_reason = row.get("chip_reason")
        if not isinstance(c_reason, str) or not c_reason or c_reason == "nan":
            c_reason = "籌碼資料有限"

        action, risk, reason, hint, take_profit, pnl = decide_position({**row, "chip_score": chip_score})

        rows.append({
            "stock_id": str(row.get("stock_id", "")),
            "stock_name": str(row.get("stock_name", "")),
            "position_action": action,
            "position_reason": reason,
            "position_hint": hint,
            "take_profit_hint": take_profit,
            "chip_hint": chip_hint_from_score(chip_score),
            "risk_flag": risk,
            "avg_price": round(to_num(row.get("avg_price")), 4),
            "shares": round(to_num(row.get("shares")), 4),
            "lots": round(to_num(row.get("lots")), 4),
            "close": fmt_price(row.get("close")),
            "ma5": fmt_price(row.get("ma5")),
            "ma20": fmt_price(row.get("ma20")),
            "ma5_status": status_price_vs_ma(row.get("close"), row.get("ma5"), "MA5"),
            "ma20_status": status_price_vs_ma(row.get("close"), row.get("ma20"), "MA20"),
            "pnl_pct": round(float(pnl), 2) if pnl is not None else "",
            "chip_score": round(chip_score, 2),
            "chip_label": c_label,
            "chip_reason": c_reason,
            "price_source": price_source,
            "chip_source": chip_source,
            "position_source": pos_source,
            "updated_at": now,
        })

    return pd.DataFrame(rows, columns=OUT_COLS)


def main() -> None:
    log("START")

    out = build_overlay()

    if out.empty:
        reason = "NO_POSITION: 找不到 manual_positions.csv 或持倉檔沒有有效個股。"
        empty = pd.DataFrame(columns=OUT_COLS)
        empty.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
        empty.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
        (DATA_DIR / "position_overlay_debug.csv").write_text("", encoding="utf-8")
        (DATA_DIR / "position_overlay_summary.txt").write_text(reason, encoding="utf-8")
        log(reason)
        return

    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay_debug.csv", index=False, encoding="utf-8-sig")

    summary = {
        "version": VERSION,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": int(len(out)),
        "stocks": out["stock_id"].astype(str).tolist(),
        "non_empty_close": int((out["close"].astype(str) != "").sum()),
        "non_empty_ma5": int((out["ma5"].astype(str) != "").sum()),
        "non_empty_ma20": int((out["ma20"].astype(str) != "").sum()),
        "empty_ma5_ids": out.loc[out["ma5"].astype(str).isin(["", "nan", "None", "--"]), "stock_id"].astype(str).tolist(),
        "empty_ma20_ids": out.loc[out["ma20"].astype(str).isin(["", "nan", "None", "--"]), "stock_id"].astype(str).tolist(),
    }
    (DATA_DIR / "position_overlay_summary.txt").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"DONE rows={len(out)}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
