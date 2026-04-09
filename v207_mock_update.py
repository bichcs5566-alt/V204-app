
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
SUM_PATH = ROOT / "v202_summary.csv"
UPDATE_LOG_PATH = ROOT / "update_log.csv"

TIER_MAP = {
    "all": ROOT / "tier_all_positions.csv",
    "lt10": ROOT / "tier_lt10_positions.csv",
    "10_30": ROOT / "tier_10_30_positions.csv",
    "30_50": ROOT / "tier_30_50_positions.csv",
    "50_100": ROOT / "tier_50_100_positions.csv",
    "100p": ROOT / "tier_100p_positions.csv",
    "unknown": ROOT / "tier_unknown_positions.csv",
}
OVERVIEW_PATH = ROOT / "tier_overview_summary.csv"

def safe_num(s):
    return pd.to_numeric(s, errors="coerce")

def pick_price_col(df):
    for c in ["price", "entry_price", "close", "close_used", "last_price", "px"]:
        if c in df.columns:
            return c
    return None

def classify_price(x):
    if pd.isna(x): return "unknown"
    if x < 10: return "lt10"
    if 10 <= x < 30: return "10_30"
    if 30 <= x < 50: return "30_50"
    if 50 <= x < 100: return "50_100"
    if x >= 100: return "100p"
    return "unknown"

def main():
    if not POS_PATH.exists():
        raise FileNotFoundError(f"找不到 {POS_PATH}")

    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    nav = pd.read_csv(NAV_PATH) if NAV_PATH.exists() else pd.DataFrame()
    summary = pd.read_csv(SUM_PATH) if SUM_PATH.exists() else pd.DataFrame()

    price_col = pick_price_col(pos)
    if price_col is None:
        pos["__price__"] = np.nan
    else:
        pos["__price__"] = safe_num(pos[price_col])

    for col in ["day_ret", "cum_ret", "weight"]:
        if col in pos.columns:
            pos[col] = safe_num(pos[col])

    pos["price_tier"] = pos["__price__"].apply(classify_price)
    pos.to_csv(TIER_MAP["all"], index=False)

    overview_rows = []
    for tier in ["lt10", "10_30", "30_50", "50_100", "100p", "unknown"]:
        sub = pos[pos["price_tier"] == tier].copy()
        sub.to_csv(TIER_MAP[tier], index=False)

        overview_rows.append({
            "price_tier": tier,
            "rows": len(sub),
            "symbol_count": sub["symbol"].nunique() if "symbol" in sub.columns else np.nan,
            "avg_weight": float(sub["weight"].mean()) if "weight" in sub.columns and len(sub) else np.nan,
            "avg_day_ret": float(sub["day_ret"].mean()) if "day_ret" in sub.columns and len(sub) else np.nan,
            "avg_cum_ret": float(sub["cum_ret"].mean()) if "cum_ret" in sub.columns and len(sub) else np.nan,
            "latest_trade_date": str(sub["trade_date"].max()) if "trade_date" in sub.columns and len(sub) else "",
            "latest_signal_date": str(sub["signal_date"].max()) if "signal_date" in sub.columns and len(sub) else "",
        })

    pd.DataFrame(overview_rows).to_csv(OVERVIEW_PATH, index=False)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    row = pd.DataFrame([{
        "updated_at_utc": now,
        "latest_nav_date": str(nav["date"].max()) if "date" in nav.columns and len(nav) else "",
        "latest_trade_date": str(pos["trade_date"].max()) if "trade_date" in pos.columns and len(pos) else "",
        "latest_signal_date": str(pos["signal_date"].max()) if "signal_date" in pos.columns and len(pos) else "",
        "final_nav": summary.iloc[0]["final_nav"] if len(summary) and "final_nav" in summary.columns else np.nan,
        "sharpe": summary.iloc[0]["sharpe"] if len(summary) and "sharpe" in summary.columns else np.nan,
        "total_return": summary.iloc[0]["total_return"] if len(summary) and "total_return" in summary.columns else np.nan,
        "positions_rows": len(pos),
    }])

    if UPDATE_LOG_PATH.exists():
        old = pd.read_csv(UPDATE_LOG_PATH)
        out = pd.concat([old, row], ignore_index=True)
    else:
        out = row

    out.to_csv(UPDATE_LOG_PATH, index=False)
    print("v207 daily update done")

if __name__ == "__main__":
    main()
