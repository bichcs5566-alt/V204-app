"""
exit_risk_engine.py
v266.9.1 穩定版

修正：
1. 持倉風控原因一律重新產生乾淨中文，不沿用可能壞掉的舊文字。
2. CSV 以 utf-8-sig 輸出，避免 Safari / GitHub Pages 顯示亂碼。
3. 支援 positions_manual.csv 欄位：
   stock_id, avg_price, lots, shares, note, updated_at
"""

from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

POSITION_FILES = [
    ROOT / "positions_manual.csv",
    DATA_DIR / "positions_manual.csv",
    ROOT / "current_positions.csv",
    DATA_DIR / "current_positions.csv",
    ROOT / "position_monitor.csv",
    DATA_DIR / "position_monitor.csv",
]

PRICE_FILES = [
    ROOT / "price_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
]

OUTPUT_COLUMNS = [
    "stock_id", "shares", "lots", "avg_cost", "close", "position_value", "unrealized_pct",
    "exit_action", "exit_priority", "exit_reason", "stop_loss_pct",
    "ma20", "ma60", "mom5", "mom20", "risk_level", "market_regime", "generated_at",
]

def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) <= 4 else s

def read_csv_any(paths):
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc, dtype={"stock_id": str})
                if not df.empty:
                    return df, p
            except Exception:
                continue
    return pd.DataFrame(), None

def load_json_any(paths):
    for p in paths:
        p = Path(p)
        if p.exists() and p.stat().st_size > 0:
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
    return {}

def write_csv_both(df, name):
    df.to_csv(ROOT / name, index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / name, index=False, encoding="utf-8-sig")

def normalize_positions(df):
    if df.empty:
        return pd.DataFrame(columns=["stock_id", "shares", "lots", "avg_cost"])

    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "stock_id" not in df.columns:
        for alt in ["symbol", "code", "ticker", "股票", "個股"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break
        else:
            return pd.DataFrame(columns=["stock_id", "shares", "lots", "avg_cost"])

    if "shares" not in df.columns:
        if "lots" in df.columns:
            df["shares"] = pd.to_numeric(df["lots"], errors="coerce") * 1000
        elif "張數" in df.columns:
            df["shares"] = pd.to_numeric(df["張數"], errors="coerce") * 1000
        elif "qty" in df.columns:
            df["shares"] = df["qty"]
        elif "quantity" in df.columns:
            df["shares"] = df["quantity"]
        elif "股數" in df.columns:
            df["shares"] = df["股數"]
        else:
            df["shares"] = 0

    if "lots" not in df.columns:
        df["lots"] = pd.to_numeric(df["shares"], errors="coerce") / 1000

    if "avg_cost" not in df.columns:
        for alt in ["avg_price", "cost", "entry_price", "buy_price", "均價", "成本"]:
            if alt in df.columns:
                df["avg_cost"] = df[alt]
                break
        else:
            df["avg_cost"] = np.nan

    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
    df["lots"] = pd.to_numeric(df["lots"], errors="coerce").fillna(df["shares"] / 1000)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce")

    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)]
    df = df[df["shares"] > 0].copy()

    df["cost_value"] = df["shares"] * df["avg_cost"]
    grouped = df.groupby("stock_id", as_index=False).agg({"shares": "sum", "cost_value": "sum"})
    grouped["avg_cost"] = grouped["cost_value"] / grouped["shares"]
    grouped["lots"] = grouped["shares"] / 1000

    return grouped[["stock_id", "shares", "lots", "avg_cost"]]

def load_price():
    df, _ = read_csv_any(PRICE_FILES)
    if df.empty:
        raise FileNotFoundError("missing price_panel_daily.csv")

    df = df.copy()
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
            df[c] = df["close"] if c != "volume" and "close" in df.columns else 0
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)
    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)]
    df = df[df["close"] > 0].copy()
    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)

def add_features(g):
    g = g.sort_values("date").copy()
    g["ma20"] = g["close"].rolling(20).mean()
    g["ma60"] = g["close"].rolling(60).mean()
    g["mom5"] = g["close"] / g["close"].shift(5) - 1
    g["mom20"] = g["close"] / g["close"].shift(20) - 1
    return g

def decide_exit(row, regime):
    close = float(row["close"])
    avg_cost = float(row["avg_cost"])
    ma20 = row.get("ma20", np.nan)
    ma60 = row.get("ma60", np.nan)
    mom5 = row.get("mom5", np.nan)
    mom20 = row.get("mom20", np.nan)

    unreal = close / avg_cost - 1 if avg_cost > 0 else np.nan
    stop_loss_pct = -0.08
    if regime == "BEAR":
        stop_loss_pct = -0.05
    elif regime == "RANGE":
        stop_loss_pct = -0.06

    action = "HOLD"
    priority = 10
    risk = "LOW"
    reasons = []

    if pd.notna(unreal) and unreal <= stop_loss_pct:
        action, priority, risk = "SELL", 100, "HIGH"
        reasons.append(f"觸發停損 {unreal * 100:.2f}%")

    if pd.notna(ma20) and close < ma20:
        if action != "SELL":
            action = "REDUCE"
        priority = max(priority, 70)
        risk = "MEDIUM"
        reasons.append("跌破 MA20")

    if pd.notna(ma60) and close < ma60:
        action = "SELL"
        priority = max(priority, 90)
        risk = "HIGH"
        reasons.append("跌破 MA60")

    if pd.notna(mom5) and mom5 < -0.04:
        if action == "HOLD":
            action = "REDUCE"
        priority = max(priority, 60)
        risk = "MEDIUM"
        reasons.append("短線動能轉弱")

    if pd.notna(mom20) and mom20 < -0.08:
        action = "SELL"
        priority = max(priority, 85)
        risk = "HIGH"
        reasons.append("20日動能轉弱")

    if regime == "BEAR":
        if action == "HOLD":
            action = "WATCH"
            priority = max(priority, 30)
            risk = "MEDIUM"
            reasons.append("市場偏弱，持倉防守")
        elif action == "REDUCE":
            priority = max(priority, 80)
            reasons.append("市場偏弱，減碼優先")

    if not reasons:
        reasons.append("持倉狀態正常，續抱觀察")

    return action, priority, "｜".join(reasons), stop_loss_pct, risk, unreal

def write_empty_summary(regime, pos_src, generated_at):
    out = pd.DataFrame(columns=OUTPUT_COLUMNS)
    write_csv_both(out, "exit_risk_plan.csv")

    summary = {
        "generated_at": generated_at,
        "source": "exit_risk_engine_v266_9_1_stable",
        "position_source": str(pos_src) if pos_src else "",
        "market_regime": regime,
        "positions": 0,
        "sell_count": 0,
        "reduce_count": 0,
        "watch_count": 0,
        "hold_count": 0,
        "note": "沒有持倉資料，因此不產生出場動作。"
    }
    for p in [ROOT / "exit_risk_summary.json", DATA_DIR / "exit_risk_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

def main():
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    pos_raw, pos_src = read_csv_any(POSITION_FILES)
    pos = normalize_positions(pos_raw)

    regime_obj = load_json_any([ROOT / "market_regime.json", DATA_DIR / "market_regime.json"])
    regime = regime_obj.get("regime", regime_obj.get("label", "UNKNOWN"))

    if pos.empty:
        write_empty_summary(regime, pos_src, generated_at)
        return

    price = load_price()
    price = price[price["stock_id"].isin(set(pos["stock_id"]))].copy()

    latest_rows = []
    for _, g in price.groupby("stock_id", sort=False):
        if len(g) >= 20:
            latest_rows.append(add_features(g).iloc[-1].to_dict())

    latest = pd.DataFrame(latest_rows)
    if latest.empty:
        write_empty_summary(regime, pos_src, generated_at)
        return

    merged = pos.merge(latest, on="stock_id", how="left")
    rows = []

    for _, r in merged.iterrows():
        if pd.isna(r.get("close")):
            continue

        action, priority, reason, stop_loss_pct, risk, unreal = decide_exit(r, regime)
        close = float(r["close"])
        shares = float(r["shares"])

        rows.append({
            "stock_id": r["stock_id"],
            "shares": int(shares),
            "lots": round(float(r["lots"]), 3),
            "avg_cost": round(float(r["avg_cost"]), 2),
            "close": round(close, 2),
            "position_value": round(close * shares, 0),
            "unrealized_pct": round(float(unreal), 4) if pd.notna(unreal) else "",
            "exit_action": action,
            "exit_priority": priority,
            "exit_reason": reason,
            "stop_loss_pct": stop_loss_pct,
            "ma20": round(float(r["ma20"]), 2) if pd.notna(r.get("ma20")) else "",
            "ma60": round(float(r["ma60"]), 2) if pd.notna(r.get("ma60")) else "",
            "mom5": round(float(r["mom5"]), 4) if pd.notna(r.get("mom5")) else "",
            "mom20": round(float(r["mom20"]), 4) if pd.notna(r.get("mom20")) else "",
            "risk_level": risk,
            "market_regime": regime,
            "generated_at": generated_at,
        })

    out = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if not out.empty:
        out = out.sort_values(["exit_priority", "stock_id"], ascending=[False, True])

    write_csv_both(out, "exit_risk_plan.csv")

    summary = {
        "generated_at": generated_at,
        "source": "exit_risk_engine_v266_9_1_stable",
        "position_source": str(pos_src) if pos_src else "",
        "market_regime": regime,
        "positions": int(len(out)),
        "sell_count": int((out["exit_action"] == "SELL").sum()) if not out.empty else 0,
        "reduce_count": int((out["exit_action"] == "REDUCE").sum()) if not out.empty else 0,
        "watch_count": int((out["exit_action"] == "WATCH").sum()) if not out.empty else 0,
        "hold_count": int((out["exit_action"] == "HOLD").sum()) if not out.empty else 0,
        "encoding": "utf-8-sig"
    }
    for p in [ROOT / "exit_risk_summary.json", DATA_DIR / "exit_risk_summary.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
