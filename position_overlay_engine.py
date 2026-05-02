# -*- coding: utf-8 -*-
"""
position_overlay_engine.py
v266.27 持倉停利 + MA5/MA20 + 籌碼提示版

功能：
1. position_monitor / positions_manual 讀持倉。
2. price_panel_daily 計算最新 close、MA5、MA20、損益%。
3. chip_source_twse 讀籌碼分數/法人資料，產出籌碼提示。
4. 判斷：
   - 停損：損益 <= -8%
   - 觀察：跌破 MA20 或跌破 MA5
   - 停利觀察：獲利 >= 10% 且 MA5 轉弱 / 籌碼轉弱
   - 抱住：趨勢未壞，籌碼不弱
5. 輸出：
   - position_overlay.csv
   - mobile_dashboard_v1/data/position_overlay.csv
"""

from pathlib import Path
import pandas as pd
import math
from datetime import datetime

DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

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
    "close",
    "ma5",
    "ma20",
    "ma5_status",
    "ma20_status",
    "pnl_pct",
    "chip_score",
    "chip_label",
    "chip_reason",
    "updated_at",
]


def log(msg):
    print(f"[POSITION OVERLAY v266.27] {msg}", flush=True)


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
        except Exception:
            return pd.DataFrame()


def pick_file(paths):
    for p in paths:
        if Path(p).exists() and Path(p).stat().st_size > 4:
            return p
    return paths[0]


def sid_series(s):
    return s.astype(str).str.extract(r"(\d{4})")[0]


def sid_value(v):
    import re
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else ""


def find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None


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


def chip_label_from_score(score):
    s = num(score, 50)
    if s >= 80:
        return "🔥 高度集中"
    if s >= 60:
        return "🟢 偏集中"
    if s >= 40:
        return "🟡 普通"
    if s >= 20:
        return "⚠️ 分散"
    return "❌ 極度分散"


def chip_hint(score):
    s = num(score, 50)
    if s >= 80:
        return "籌碼高度集中，資金共識強，若趨勢未破可續抱或用移動停利。"
    if s >= 60:
        return "籌碼偏集中，有資金支撐，若未跌破 MA5/MA20 可續抱觀察。"
    if s >= 40:
        return "籌碼普通，沒有明顯優勢，獲利時可分批停利。"
    if s >= 20:
        return "籌碼偏分散，資金共識不足，若短線轉弱要考慮減碼。"
    return "籌碼極度分散，若已獲利建議優先停利，若虧損需控風險。"


def write_empty(reason):
    out = pd.DataFrame(columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    (DATA_DIR / "position_overlay_summary.txt").write_text(reason, encoding="utf-8")
    log(reason)


def build_latest_price():
    price_path = pick_file([
        "mobile_dashboard_v1/data/price_panel_daily.csv",
        "price_panel_daily.csv",
    ])
    price = read_csv_safe(price_path)
    if price.empty:
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"])

    sid_col = find_col(price, ["stock_id", "symbol", "code", "個股", "股票代號"])
    date_col = find_col(price, ["date", "trade_date", "日期"])
    close_col = find_col(price, ["close", "Close", "收盤價", "收盤"])
    if sid_col is None or close_col is None:
        return pd.DataFrame(columns=["stock_id", "close", "ma5", "ma20"])

    price = price.copy()
    price["stock_id"] = sid_series(price[sid_col])
    price["close"] = pd.to_numeric(price[close_col], errors="coerce")
    price = price.dropna(subset=["stock_id", "close"])
    if date_col:
        price["_date"] = pd.to_datetime(price[date_col], errors="coerce")
        price = price.sort_values(["stock_id", "_date"])
    else:
        price = price.sort_values(["stock_id"])

    price["ma5"] = price.groupby("stock_id")["close"].rolling(5, min_periods=3).mean().reset_index(level=0, drop=True)
    price["ma20"] = price.groupby("stock_id")["close"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)

    return price.groupby("stock_id").tail(1)[["stock_id", "close", "ma5", "ma20"]]


def build_chip():
    chip_path = pick_file([
        "mobile_dashboard_v1/data/chip_source_twse.csv",
        "chip_source_twse.csv",
    ])
    chip = read_csv_safe(chip_path)
    if chip.empty or "stock_id" not in chip.columns:
        return pd.DataFrame(columns=["stock_id", "chip_score", "chip_label", "chip_reason"])

    chip = chip.copy()
    chip["stock_id"] = sid_series(chip["stock_id"])
    chip = chip.dropna(subset=["stock_id"])

    for c in ["foreign_net_buy", "trust_net_buy", "dealer_net_buy", "inst_net_buy", "inst_valid", "margin_valid"]:
        if c not in chip.columns:
            chip[c] = 0
        chip[c] = pd.to_numeric(chip[c], errors="coerce").fillna(0)

    rows = []
    for _, r in chip.iterrows():
        inst = num(r.get("inst_net_buy", 0))
        foreign = num(r.get("foreign_net_buy", 0))
        trust = num(r.get("trust_net_buy", 0))
        inst_valid = int(num(r.get("inst_valid", 0)))

        score = 50
        reasons = []

        if inst_valid:
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
            reasons.append("法人資料有限")

        score = max(0, min(100, score))
        rows.append({
            "stock_id": r["stock_id"],
            "chip_score": round(score, 2),
            "chip_label": chip_label_from_score(score),
            "chip_reason": "｜".join(reasons),
        })

    return pd.DataFrame(rows).drop_duplicates("stock_id", keep="last")


def main():
    log("START")

    pos_path = pick_file([
        "mobile_dashboard_v1/data/position_monitor.csv",
        "position_monitor.csv",
        "mobile_dashboard_v1/data/position_monitor_merged.csv",
        "position_monitor_merged.csv",
        "mobile_dashboard_v1/data/positions_manual.csv",
        "positions_manual.csv",
    ])
    pos = read_csv_safe(pos_path)

    if pos.empty:
        write_empty("NO_POSITION: position file empty")
        return

    sid_col = find_col(pos, ["stock_id", "symbol", "code", "個股", "股票代號"])
    if sid_col is None:
        write_empty(f"NO_STOCK_ID_COLUMN: {list(pos.columns)}")
        return

    pos = pos.copy()
    pos["stock_id"] = sid_series(pos[sid_col])
    pos = pos.dropna(subset=["stock_id"])
    pos = pos[pos["stock_id"].astype(str).str.len() == 4]

    if pos.empty:
        write_empty("NO_VALID_POSITION")
        return

    name_col = find_col(pos, ["stock_name", "name", "股票名稱", "證券名稱"])
    avg_col = find_col(pos, ["avg_price", "average_price", "cost", "成本", "均價", "avg_cost"])
    shares_col = find_col(pos, ["shares", "share", "qty", "quantity", "股數", "張數", "lots"])

    pos["stock_name"] = pos[name_col].astype(str) if name_col else ""
    pos["avg_price"] = pd.to_numeric(pos[avg_col], errors="coerce") if avg_col else 0
    pos["shares"] = pd.to_numeric(pos[shares_col], errors="coerce") if shares_col else 0

    latest = build_latest_price()
    chip = build_chip()

    df = pos.merge(latest, on="stock_id", how="left")
    df = df.merge(chip, on="stock_id", how="left")

    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, r in df.iterrows():
        sid = str(r.get("stock_id", ""))
        name = str(r.get("stock_name", ""))
        avg = num(r.get("avg_price", 0))
        shares = num(r.get("shares", 0))
        close = num(r.get("close", 0))
        ma5 = num(r.get("ma5", 0))
        ma20 = num(r.get("ma20", 0))
        chip_score = num(r.get("chip_score", 50), 50)
        chip_label = str(r.get("chip_label", chip_label_from_score(chip_score)))
        chip_reason = str(r.get("chip_reason", "籌碼資料有限"))

        pnl = None
        if avg > 0 and close > 0:
            pnl = (close - avg) / avg * 100

        ma5_status = status_price_vs_ma(close, ma5, "MA5")
        ma20_status = status_price_vs_ma(close, ma20, "MA20")

        action = "🟢 抱住"
        risk_flag = "HOLD_CHECK"
        reason = "尚未出現明顯下跌或系統賣出訊號，趨勢未完全破壞。"
        hint = "在還沒有明顯下跌、未觸發風控前，以續抱觀察為主。"
        take_profit = "尚未達明確停利條件，先依趨勢與籌碼續抱觀察。"

        if close <= 0:
            action = "🟡 觀察"
            risk_flag = "NO_PRICE"
            reason = "沒有最新價格資料，無法判斷停利/停損。"
            hint = "先確認價格資料是否更新，再決定是否處理。"
            take_profit = "價格資料不足，暫不做停利判斷。"
        elif avg <= 0:
            action = "🟡 觀察"
            risk_flag = "NO_COST"
            reason = "沒有成本均價，無法計算損益。"
            hint = "請先同步持倉均價。"
            take_profit = "成本資料不足，暫不做停利判斷。"
        elif pnl is not None and pnl <= -8:
            action = "🔴 出場"
            risk_flag = "STOP_LOSS"
            reason = f"觸發停損 {pnl:.2f}%，優先保護本金。"
            hint = "優先處理賣出，不建議拖延或凹單。"
            take_profit = "目前不是停利情境，而是停損風控。"
        elif pnl is not None and pnl >= 15 and (close < ma5 or chip_score < 40):
            action = "🟠 停利觀察"
            risk_flag = "TAKE_PROFIT_WATCH"
            reason = f"已有獲利 {pnl:.2f}%，但短線或籌碼開始轉弱。"
            hint = "可考慮分批停利，保留部分部位觀察。"
            take_profit = "達到獲利區且訊號轉弱，建議分批停利，不要一次賭回去。"
        elif pnl is not None and pnl >= 10 and chip_score >= 60 and close >= ma5:
            action = "🟢 抱住"
            risk_flag = "PROFIT_HOLD"
            reason = f"已有獲利 {pnl:.2f}%，且籌碼偏集中、短線未破。"
            hint = "可續抱，改用移動停利保護獲利。"
            take_profit = "已進入獲利區，建議設定移動停利；若跌破 MA5 或籌碼轉弱再分批停利。"
        elif close < ma20:
            action = "🟡 觀察"
            risk_flag = "BELOW_MA20"
            reason = "跌破 MA20，中線趨勢開始轉弱。"
            hint = "先觀察或減碼，等待重新站回 MA20。"
            take_profit = "若有獲利可先停利一部分；若虧損需控風險。"
        elif close < ma5:
            action = "🟡 觀察"
            risk_flag = "BELOW_MA5"
            reason = "跌破五日線，短線動能轉弱。"
            hint = "短線先觀察，不建議追高加碼。"
            take_profit = "若已有獲利，可先分批停利；若籌碼仍集中可留部分觀察。"
        elif chip_score < 30:
            action = "🟡 觀察"
            risk_flag = "CHIP_WEAK"
            reason = "籌碼偏弱，資金共識不足。"
            hint = "不建議加碼，若短線轉弱可考慮減碼。"
            take_profit = "籌碼弱時不適合貪，獲利部位可分批收。"

        rows.append({
            "stock_id": sid,
            "stock_name": name,
            "position_action": action,
            "position_reason": reason,
            "position_hint": hint,
            "take_profit_hint": take_profit,
            "chip_hint": chip_hint(chip_score),
            "risk_flag": risk_flag,
            "avg_price": round(avg, 4) if avg else 0,
            "shares": round(shares, 4) if shares else 0,
            "close": round(close, 4) if close else "",
            "ma5": round(ma5, 4) if ma5 else "",
            "ma20": round(ma20, 4) if ma20 else "",
            "ma5_status": ma5_status,
            "ma20_status": ma20_status,
            "pnl_pct": round(pnl, 2) if pnl is not None else "",
            "chip_score": round(chip_score, 2),
            "chip_label": chip_label,
            "chip_reason": chip_reason,
            "updated_at": now,
        })

    out = pd.DataFrame(rows, columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    (DATA_DIR / "position_overlay_summary.txt").write_text(f"v266.27 rows={len(out)} updated={now}", encoding="utf-8")

    log(f"DONE rows={len(out)}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
