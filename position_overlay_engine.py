# -*- coding: utf-8 -*-
"""
position_overlay_engine.py
v266.28A 後端持倉檔接入版

重點：
- 優先讀 manual_positions.csv / mobile_dashboard_v1/data/manual_positions.csv
- 沒有才讀 positions_manual / position_monitor
- 輸出 position_overlay.csv 不能再只有 header
"""

from pathlib import Path
from datetime import datetime
import math
import pandas as pd

DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_COLS = [
    "stock_id","stock_name","position_action","position_reason","position_hint",
    "take_profit_hint","chip_hint","risk_flag","avg_price","shares","lots",
    "close","ma5","ma20","ma5_status","ma20_status","pnl_pct",
    "chip_score","chip_label","chip_reason","updated_at"
]

def log(msg):
    print(f"[POSITION OVERLAY v266.28A] {msg}", flush=True)

def read_csv_safe(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 4:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception:
        try:
            return pd.read_csv(p)
        except Exception as e:
            log(f"read failed {path}: {e}")
            return pd.DataFrame()

def first_existing(paths):
    for p in paths:
        pp = Path(p)
        if pp.exists() and pp.stat().st_size > 4:
            return p
    return paths[0]

def find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None

def sid_series(s):
    return s.astype(str).str.extract(r"(\d{4})")[0]

def num(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).replace(",", "").replace("%", "").replace("張", "").replace("股", "").strip()
        if s in ("", "-", "--", "nan", "NaN", "None"):
            return default
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default

def status_price_vs_ma(close, ma, label):
    c = num(close, None)
    m = num(ma, None)
    if c is None or m is None or c <= 0 or m <= 0:
        return f"{label}：資料不足"
    diff = (c - m) / m
    if diff > 0.02:
        return f"{label}：站上｜↑ 強勢"
    if diff < -0.02:
        return f"{label}：跌破｜↓ 轉弱"
    return f"{label}：貼近｜→ 盤整"

def chip_label(score):
    s = num(score, 50)
    if s >= 80: return "🔥 高度集中"
    if s >= 60: return "🟢 偏集中"
    if s >= 40: return "🟡 普通"
    if s >= 20: return "⚠️ 分散"
    return "❌ 極度分散"

def chip_hint(score):
    s = num(score, 50)
    if s >= 80: return "籌碼高度集中，資金共識強，若趨勢未破可續抱或用移動停利。"
    if s >= 60: return "籌碼偏集中，有資金支撐，若未跌破 MA5/MA20 可續抱觀察。"
    if s >= 40: return "籌碼普通，沒有明顯優勢，獲利時可分批停利。"
    if s >= 20: return "籌碼偏分散，資金共識不足，若短線轉弱要考慮減碼。"
    return "籌碼極度分散，若已獲利建議優先停利，若虧損需控風險。"

def write_empty(reason):
    out = pd.DataFrame(columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    (DATA_DIR / "position_overlay_summary.txt").write_text(reason, encoding="utf-8")
    log(reason)

def load_positions():
    path = first_existing([
        "manual_positions.csv",
        "mobile_dashboard_v1/data/manual_positions.csv",
        "positions_manual.csv",
        "mobile_dashboard_v1/data/positions_manual.csv",
        "position_monitor.csv",
        "mobile_dashboard_v1/data/position_monitor.csv",
        "position_monitor_merged.csv",
        "mobile_dashboard_v1/data/position_monitor_merged.csv",
    ])
    df = read_csv_safe(path)
    log(f"position source={path} rows={len(df)}")
    if df.empty:
        return df

    sid_col = find_col(df, ["stock_id","symbol","code","個股","股票代號"])
    if sid_col is None:
        log(f"no stock_id column in positions: {list(df.columns)}")
        return pd.DataFrame()

    df = df.copy()
    df["stock_id"] = sid_series(df[sid_col])
    df = df.dropna(subset=["stock_id"])
    df = df[df["stock_id"].astype(str).str.len() == 4]

    name_col = find_col(df, ["stock_name","name","股票名稱","證券名稱"])
    avg_col = find_col(df, ["avg_price","average_price","cost","成本","均價","avg_cost"])
    shares_col = find_col(df, ["shares","share","qty","quantity","股數"])
    lots_col = find_col(df, ["lots","張數"])

    df["stock_name"] = df[name_col].astype(str) if name_col else ""
    df["avg_price"] = pd.to_numeric(df[avg_col], errors="coerce").fillna(0) if avg_col else 0

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

    return df[["stock_id","stock_name","avg_price","shares","lots"]].drop_duplicates("stock_id", keep="last")

def latest_price():
    path = first_existing([
        "mobile_dashboard_v1/data/price_panel_daily.csv",
        "price_panel_daily.csv",
    ])
    df = read_csv_safe(path)
    log(f"price source={path} rows={len(df)}")
    if df.empty:
        return pd.DataFrame(columns=["stock_id","close","ma5","ma20"])

    sid_col = find_col(df, ["stock_id","symbol","code","個股","股票代號"])
    date_col = find_col(df, ["date","trade_date","日期"])
    close_col = find_col(df, ["close","Close","收盤價","收盤"])
    if sid_col is None or close_col is None:
        log(f"price missing columns: {list(df.columns)}")
        return pd.DataFrame(columns=["stock_id","close","ma5","ma20"])

    df = df.copy()
    df["stock_id"] = sid_series(df[sid_col])
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["stock_id","close"])
    if date_col:
        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(["stock_id","_date"])
    else:
        df = df.sort_values(["stock_id"])

    df["ma5"] = df.groupby("stock_id")["close"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    df["ma20"] = df.groupby("stock_id")["close"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    return df.groupby("stock_id").tail(1)[["stock_id","close","ma5","ma20"]]

def chip_data():
    path = first_existing([
        "mobile_dashboard_v1/data/chip_source_twse.csv",
        "chip_source_twse.csv",
    ])
    df = read_csv_safe(path)
    log(f"chip source={path} rows={len(df)}")
    if df.empty or "stock_id" not in df.columns:
        return pd.DataFrame(columns=["stock_id","chip_score","chip_label","chip_reason"])

    df = df.copy()
    df["stock_id"] = sid_series(df["stock_id"])
    df = df.dropna(subset=["stock_id"])

    for c in ["foreign_net_buy","trust_net_buy","dealer_net_buy","inst_net_buy","inst_valid"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    rows = []
    for _, r in df.iterrows():
        inst = num(r.get("inst_net_buy", 0))
        trust = num(r.get("trust_net_buy", 0))
        foreign = num(r.get("foreign_net_buy", 0))
        valid = int(num(r.get("inst_valid", 0)))
        score = 50
        reasons = []

        if valid:
            if inst > 0:
                score += min(25, 8 + abs(inst) / 2000)
                reasons.append("三大法人買超")
            elif inst < 0:
                score -= min(25, 8 + abs(inst) / 2000)
                reasons.append("三大法人賣超")
            else:
                reasons.append("法人中性")

            if trust > 0:
                score += min(10, 4 + abs(trust) / 1000)
                reasons.append("投信買超")
            elif trust < 0:
                score -= min(10, 4 + abs(trust) / 1000)
                reasons.append("投信賣超")

            if foreign > 0:
                reasons.append("外資買超")
            elif foreign < 0:
                reasons.append("外資賣超")
        else:
            reasons.append("籌碼資料有限")

        score = max(0, min(100, score))
        rows.append({
            "stock_id": r["stock_id"],
            "chip_score": round(score, 2),
            "chip_label": chip_label(score),
            "chip_reason": "｜".join(reasons),
        })

    return pd.DataFrame(rows).drop_duplicates("stock_id", keep="last")

def decide(row):
    avg = num(row.get("avg_price"))
    close = num(row.get("close"))
    ma5 = num(row.get("ma5"))
    ma20 = num(row.get("ma20"))
    chip_score = num(row.get("chip_score", 50), 50)

    pnl = None
    if avg > 0 and close > 0:
        pnl = (close - avg) / avg * 100

    if close <= 0:
        return "🟡 觀察","NO_PRICE","沒有最新價格資料，無法判斷停利/停損。","先確認價格資料是否更新。","價格資料不足，暫不做停利判斷。",pnl

    if avg <= 0:
        return "🟡 觀察","NO_COST","沒有成本均價，無法計算損益。","請先同步持倉均價。","成本資料不足，暫不做停利判斷。",pnl

    if pnl <= -8:
        return "🔴 出場","STOP_LOSS",f"觸發停損 {pnl:.2f}%，優先保護本金。","優先處理賣出，不建議拖延或凹單。","目前不是停利情境，而是停損風控。",pnl

    if pnl >= 15 and ((ma5 > 0 and close < ma5) or chip_score < 40):
        return "🟠 停利觀察","TAKE_PROFIT_WATCH",f"已有獲利 {pnl:.2f}%，但短線或籌碼開始轉弱。","可考慮分批停利，保留部分部位觀察。","達到獲利區且訊號轉弱，建議分批停利。",pnl

    if pnl >= 10 and chip_score >= 60 and (ma5 <= 0 or close >= ma5):
        return "🟢 抱住","PROFIT_HOLD",f"已有獲利 {pnl:.2f}%，且籌碼不弱、短線未破。","可續抱，改用移動停利保護獲利。","已進入獲利區，建議設定移動停利。",pnl

    if ma20 > 0 and close < ma20:
        return "🟡 觀察","BELOW_MA20","跌破 MA20，中線趨勢開始轉弱。","先觀察或減碼，等待重新站回 MA20。","若有獲利可先停利一部分；若虧損需控風險。",pnl

    if ma5 > 0 and close < ma5:
        return "🟡 觀察","BELOW_MA5","跌破五日線，短線動能轉弱。","短線先觀察，不建議追高加碼。","若已有獲利，可先分批停利；若籌碼仍集中可留部分觀察。",pnl

    if chip_score < 30:
        return "🟡 觀察","CHIP_WEAK","籌碼偏弱，資金共識不足。","不建議加碼，若短線轉弱可考慮減碼。","籌碼弱時不適合貪，獲利部位可分批收。",pnl

    return "🟢 抱住","HOLD_CHECK","尚未出現明顯下跌或系統賣出訊號，趨勢未完全破壞。","在還沒有明顯下跌、未觸發風控前，以續抱觀察為主。","尚未達明確停利條件，先依趨勢與籌碼續抱觀察。",pnl

def main():
    log("START")
    pos = load_positions()
    if pos.empty:
        write_empty("NO_POSITION: no manual_positions / positions data")
        return

    px = latest_price()
    chip = chip_data()

    df = pos.merge(px, on="stock_id", how="left")
    df = df.merge(chip, on="stock_id", how="left")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, r in df.iterrows():
        chip_score = num(r.get("chip_score", 50), 50)
        c_label = r.get("chip_label")
        if not isinstance(c_label, str) or not c_label:
            c_label = chip_label(chip_score)
        c_reason = r.get("chip_reason")
        if not isinstance(c_reason, str) or not c_reason:
            c_reason = "籌碼資料有限"

        action,risk,reason,hint,tp,pnl = decide({**r.to_dict(), "chip_score": chip_score})

        rows.append({
            "stock_id": str(r["stock_id"]),
            "stock_name": str(r.get("stock_name", "")),
            "position_action": action,
            "position_reason": reason,
            "position_hint": hint,
            "take_profit_hint": tp,
            "chip_hint": chip_hint(chip_score),
            "risk_flag": risk,
            "avg_price": round(num(r.get("avg_price")), 4),
            "shares": round(num(r.get("shares")), 4),
            "lots": round(num(r.get("lots")), 4),
            "close": round(num(r.get("close")), 4) if num(r.get("close")) else "",
            "ma5": round(num(r.get("ma5")), 4) if num(r.get("ma5")) else "",
            "ma20": round(num(r.get("ma20")), 4) if num(r.get("ma20")) else "",
            "ma5_status": status_price_vs_ma(r.get("close"), r.get("ma5"), "MA5"),
            "ma20_status": status_price_vs_ma(r.get("close"), r.get("ma20"), "MA20"),
            "pnl_pct": round(pnl, 2) if pnl is not None else "",
            "chip_score": round(chip_score, 2),
            "chip_label": c_label,
            "chip_reason": c_reason,
            "updated_at": now,
        })

    out = pd.DataFrame(rows, columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay_debug.csv", index=False, encoding="utf-8-sig")
    (DATA_DIR / "position_overlay_summary.txt").write_text(f"v266.28A rows={len(out)} updated={now}", encoding="utf-8")

    log(f"DONE rows={len(out)}")
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
