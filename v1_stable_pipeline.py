import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd

DASHBOARD_DATA_DIR = Path("mobile_dashboard_v1/data")
ROOT_EXPORT_DIR = Path("root_exports")
INITIAL_CAPITAL = 1_000_000

PRICE_PANEL_CANDIDATES = [
    Path("price_panel_daily.csv"),
    Path("mobile_dashboard_v1/data/price_panel_daily.csv"),
    Path("root_exports/price_panel_daily.csv"),
]


def now_taipei():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)


def read_csv_auto(path: Path):
    for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def next_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    d = pd.Timestamp(ts).normalize() + pd.Timedelta(days=1)
    while d.weekday() >= 5:
        d += pd.Timedelta(days=1)
    return d


def ensure_dirs():
    DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ROOT_EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def find_price_panel():
    for path in PRICE_PANEL_CANDIDATES:
        if path.exists():
            return path
    raise FileNotFoundError(
        "缺少 price_panel_daily.csv，可搜尋路徑：\n"
        + "\n".join(str(p) for p in PRICE_PANEL_CANDIDATES)
    )


def load_price():
    price_panel_path = find_price_panel()
    print(f"使用主資料：{price_panel_path}")

    df = read_csv_auto(price_panel_path)
    df.columns = [str(c).lower().strip() for c in df.columns]

    if "date" not in df.columns:
        for alt in ["trade_date", "datetime"]:
            if alt in df.columns:
                df["date"] = df[alt]
                break

    if "stock_id" not in df.columns:
        for alt in ["symbol", "code"]:
            if alt in df.columns:
                df["stock_id"] = df[alt]
                break

    if "close" not in df.columns:
        raise ValueError("price_panel_daily.csv 缺少 close 欄位")

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.strip()

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["close"] > 0].sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df


def build_trade_plan(df):
    latest_date = pd.Timestamp(df["date"].max()).normalize()
    trade_date = next_business_day(latest_date)
    latest = df[df["date"].dt.normalize() == latest_date].copy()

    latest = latest.sort_values("close", ascending=True).drop_duplicates("stock_id").head(20).copy()

    def tier(px):
        if px < 50:
            return "50以下"
        if px < 100:
            return "50-100"
        if px < 300:
            return "100-300"
        if px < 500:
            return "300-500"
        if px < 1000:
            return "500-1000"
        return "1000以上"

    latest["action"] = "買進"
    latest["price_tier"] = latest["close"].apply(tier)
    latest["target_weight"] = 0.03
    latest["ref_price"] = latest["close"].round(3)
    latest["suggested_amount"] = INITIAL_CAPITAL * latest["target_weight"]
    latest["note"] = "v1_stable"
    latest["signal_date"] = str(latest_date.date())
    latest["trade_date"] = str(trade_date.date())

    cols = [
        "signal_date",
        "trade_date",
        "action",
        "stock_id",
        "price_tier",
        "target_weight",
        "ref_price",
        "suggested_amount",
        "note",
    ]
    return latest[cols], latest_date, trade_date


def write_csv(df, name):
    df.to_csv(DASHBOARD_DATA_DIR / name, index=False, encoding="utf-8-sig")
    df.to_csv(ROOT_EXPORT_DIR / name, index=False, encoding="utf-8-sig")


def main():
    ensure_dirs()
    df = load_price()
    trade_plan, signal_date, trade_date = build_trade_plan(df)

    empty_pos = pd.DataFrame(columns=[
        "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
        "shares", "avg_cost", "pnl_pct", "target_weight", "current_weight_est",
        "action", "note"
    ])
    empty_watch = pd.DataFrame(columns=[
        "signal_date", "trade_date", "stock_id", "price_tier", "ref_price",
        "holding_status", "strategy_bucket", "action", "pnl_pct"
    ])
    summary = pd.DataFrame([{"return": 0, "mdd": 0, "sharpe_daily": 0}])

    now_str = now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "generated_at": now_str,
        "now_time": now_str,
        "signal_date": str(signal_date.date()),
        "trade_date": str(trade_date.date()),
        "trade_plan_batch": now_str,
        "data_state": "ok",
        "source": "v1_stable_pipeline"
    }

    write_csv(trade_plan, "trade_plan.csv")
    write_csv(empty_pos, "position_monitor.csv")
    write_csv(empty_watch, "watchlist_monitor.csv")
    write_csv(summary, "full_summary.csv")

    (DASHBOARD_DATA_DIR / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    (ROOT_EXPORT_DIR / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
