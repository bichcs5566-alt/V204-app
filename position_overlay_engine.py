# -*- coding: utf-8 -*-
"""
position_overlay_engine.py
v266.25.1 持倉提示安全版

修正：
- position_monitor.csv 空檔時不崩潰
- position_monitor.csv 欄位不完整時不崩潰
- price_panel_daily.csv 欄位格式不同時盡量自動對應
- 輸出 position_overlay.csv 與 mobile_dashboard_v1/data/position_overlay.csv
"""

from pathlib import Path
import pandas as pd


DATA_DIR = Path("mobile_dashboard_v1/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT_COLS = [
    "stock_id",
    "stock_name",
    "position_action",
    "position_reason",
    "position_hint",
    "risk_flag",
    "avg_price",
    "shares",
    "close",
    "ma20",
    "pnl_pct",
]


def log(msg):
    print(f"[POSITION OVERLAY v266.25.1] {msg}", flush=True)


def safe_read_csv(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 4:
        log(f"{path} empty or not found")
        return pd.DataFrame()

    try:
        return pd.read_csv(p, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        log(f"{path} EmptyDataError")
        return pd.DataFrame()
    except Exception:
        try:
            return pd.read_csv(p)
        except Exception as e:
            log(f"{path} read failed: {e}")
            return pd.DataFrame()


def pick_file(paths):
    for p in paths:
        if Path(p).exists() and Path(p).stat().st_size > 4:
            return p
    return paths[0]


def normalize_stock_id(s):
    return s.astype(str).str.extract(r"(\d{4})")[0]


def find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None


def write_empty(reason):
    out = pd.DataFrame(columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")
    summary = DATA_DIR / "position_overlay_summary.txt"
    summary.write_text(reason, encoding="utf-8")
    log(reason)


def main():
    log("START")

    pos_path = pick_file([
        "mobile_dashboard_v1/data/position_monitor.csv",
        "position_monitor.csv",
        "mobile_dashboard_v1/data/position_monitor_merged.csv",
        "position_monitor_merged.csv",
        "positions_manual.csv",
        "mobile_dashboard_v1/data/positions_manual.csv",
    ])

    price_path = pick_file([
        "mobile_dashboard_v1/data/price_panel_daily.csv",
        "price_panel_daily.csv",
    ])

    pos = safe_read_csv(pos_path)
    price = safe_read_csv(price_path)

    if pos.empty:
        write_empty("NO_POSITION: position_monitor is empty. Overlay generated as empty file.")
        return

    sid_col = find_col(pos, ["stock_id", "symbol", "code", "個股", "股票代號"])
    if sid_col is None:
        write_empty(f"NO_STOCK_ID_COLUMN: position file columns={list(pos.columns)}")
        return

    pos = pos.copy()
    pos["stock_id"] = normalize_stock_id(pos[sid_col])
    pos = pos.dropna(subset=["stock_id"])
    pos = pos[pos["stock_id"].astype(str).str.len() == 4]

    if pos.empty:
        write_empty("NO_VALID_POSITION: no valid 4-digit stock_id in position file.")
        return

    name_col = find_col(pos, ["stock_name", "name", "股票名稱", "證券名稱"])
    avg_col = find_col(pos, ["avg_price", "average_price", "cost", "成本", "均價", "avg_cost"])
    shares_col = find_col(pos, ["shares", "share", "qty", "quantity", "股數", "張數"])

    pos["stock_name"] = pos[name_col].astype(str) if name_col else ""
    pos["avg_price"] = pd.to_numeric(pos[avg_col], errors="coerce") if avg_col else 0
    pos["shares"] = pd.to_numeric(pos[shares_col], errors="coerce") if shares_col else 0

    if price.empty:
        latest = pd.DataFrame(columns=["stock_id", "close", "ma20"])
    else:
        price = price.copy()
        p_sid_col = find_col(price, ["stock_id", "symbol", "code", "個股", "股票代號"])
        date_col = find_col(price, ["date", "trade_date", "日期"])
        close_col = find_col(price, ["close", "收盤價", "收盤", "Close"])

        if p_sid_col is None or close_col is None:
            latest = pd.DataFrame(columns=["stock_id", "close", "ma20"])
        else:
            price["stock_id"] = normalize_stock_id(price[p_sid_col])
            price["close"] = pd.to_numeric(price[close_col], errors="coerce")
            if date_col:
                price["_date"] = pd.to_datetime(price[date_col], errors="coerce")
                price = price.sort_values(["stock_id", "_date"])
            else:
                price = price.sort_values(["stock_id"])

            price["ma20"] = price.groupby("stock_id")["close"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
            latest = price.groupby("stock_id").tail(1)[["stock_id", "close", "ma20"]]

    df = pos.merge(latest, on="stock_id", how="left")

    rows = []
    for _, r in df.iterrows():
        sid = str(r.get("stock_id", ""))
        name = str(r.get("stock_name", ""))
        avg_price = pd.to_numeric(r.get("avg_price", 0), errors="coerce")
        shares = pd.to_numeric(r.get("shares", 0), errors="coerce")
        close = pd.to_numeric(r.get("close", None), errors="coerce")
        ma20 = pd.to_numeric(r.get("ma20", None), errors="coerce")

        if pd.isna(avg_price):
            avg_price = 0
        if pd.isna(shares):
            shares = 0

        action = "🟡 觀察"
        reason = "資料不足，先觀察"
        hint = "持倉資料或價格資料不足，不建議用系統直接決定出場。"
        risk_flag = "DATA_LIMITED"
        pnl_pct = None

        if pd.isna(close):
            action = "🟡 觀察"
            reason = "無最新價格資料"
            hint = "價格資料未對上，先不要依此出場。"
            risk_flag = "NO_PRICE"
        elif avg_price <= 0:
            action = "🟡 觀察"
            reason = "無成本均價，無法計算損益"
            hint = "請先同步持倉成本，再做抱住/出場判斷。"
            risk_flag = "NO_COST"
        else:
            pnl_pct = (float(close) - float(avg_price)) / float(avg_price) * 100

            if pnl_pct <= -8:
                action = "🔴 出場"
                reason = f"觸發停損 {pnl_pct:.2f}%"
                hint = "優先處理出場，不建議拖延。"
                risk_flag = "STOP_LOSS"
            elif not pd.isna(ma20) and close < ma20:
                action = "🟡 觀察"
                reason = "跌破 MA20，趨勢轉弱"
                hint = "先觀察或減碼，等待重新站回均線。"
                risk_flag = "BELOW_MA20"
            else:
                action = "🟢 抱住"
                reason = "尚未明顯下跌，趨勢未壞"
                hint = "續抱觀察，不急著賣。"
                risk_flag = "HOLD_OK"

        rows.append({
            "stock_id": sid,
            "stock_name": name,
            "position_action": action,
            "position_reason": reason,
            "position_hint": hint,
            "risk_flag": risk_flag,
            "avg_price": round(float(avg_price), 4) if avg_price else 0,
            "shares": round(float(shares), 4) if shares else 0,
            "close": round(float(close), 4) if not pd.isna(close) else "",
            "ma20": round(float(ma20), 4) if not pd.isna(ma20) else "",
            "pnl_pct": round(float(pnl_pct), 2) if pnl_pct is not None else "",
        })

    out = pd.DataFrame(rows, columns=OUT_COLS)
    out.to_csv("position_overlay.csv", index=False, encoding="utf-8-sig")
    out.to_csv(DATA_DIR / "position_overlay.csv", index=False, encoding="utf-8-sig")

    log(f"DONE rows={len(out)}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
