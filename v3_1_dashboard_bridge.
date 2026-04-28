import json
from pathlib import Path
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_csv_safe(path: Path, default_columns=None) -> pd.DataFrame:
    default_columns = default_columns or []
    if not path.exists():
        return pd.DataFrame(columns=default_columns)
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=default_columns)
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def ensure_watchlist_root() -> pd.DataFrame:
    path = ROOT / "watchlist.csv"
    df = read_csv_safe(path, ["stock_id"])
    df = normalize_columns(df)

    rename_map = {}
    if "stock" in df.columns and "stock_id" not in df.columns:
        rename_map["stock"] = "stock_id"
    if "symbol" in df.columns and "stock_id" not in df.columns:
        rename_map["symbol"] = "stock_id"
    if "code" in df.columns and "stock_id" not in df.columns:
        rename_map["code"] = "stock_id"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "stock_id" not in df.columns:
        df["stock_id"] = None

    df = df[["stock_id"]].copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df = df[df["stock_id"] != ""].drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


def ensure_positions_root() -> pd.DataFrame:
    path = ROOT / "current_positions.csv"
    df = read_csv_safe(path, ["stock_id", "shares", "avg_cost"])
    df = normalize_columns(df)

    rename_map = {}
    if "stock" in df.columns and "stock_id" not in df.columns:
        rename_map["stock"] = "stock_id"
    if "symbol" in df.columns and "stock_id" not in df.columns:
        rename_map["symbol"] = "stock_id"
    if "code" in df.columns and "stock_id" not in df.columns:
        rename_map["code"] = "stock_id"
    if "qty" in df.columns and "shares" not in df.columns:
        rename_map["qty"] = "shares"
    if "cost" in df.columns and "avg_cost" not in df.columns:
        rename_map["cost"] = "avg_cost"
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ["stock_id", "shares", "avg_cost"]:
        if col not in df.columns:
            df[col] = None

    df = df[["stock_id", "shares", "avg_cost"]].copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce")
    df = df[df["stock_id"] != ""].reset_index(drop=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


def build_watchlist_monitor(watchlist_df: pd.DataFrame) -> pd.DataFrame:
    existing = read_csv_safe(ROOT / "watchlist_monitor.csv")
    existing = normalize_columns(existing)

    required = [
        "stock_id", "price_tier", "ref_price", "holding_status",
        "strategy_bucket", "action", "pnl_pct"
    ]

    if not existing.empty and "stock_id" in existing.columns:
        for col in required:
            if col not in existing.columns:
                existing[col] = ""
        existing["stock_id"] = existing["stock_id"].astype(str).str.strip()
        keep = existing[required].copy()
        keep = keep[keep["stock_id"] != ""].drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
        if len(keep) >= len(watchlist_df):
            return keep

    out = pd.DataFrame({
        "stock_id": watchlist_df["stock_id"] if "stock_id" in watchlist_df.columns else [],
        "price_tier": "æªåé¡",
        "ref_price": "--",
        "holding_status": "æªææ",
        "strategy_bucket": "æªé²ç­ç¥",
        "action": "WATCH",
        "pnl_pct": ""
    })
    return out


def build_position_monitor(position_df: pd.DataFrame) -> pd.DataFrame:
    existing = read_csv_safe(ROOT / "position_monitor.csv")
    existing = normalize_columns(existing)

    required = [
        "stock_id", "price_tier", "ref_price", "shares", "avg_cost",
        "pnl_pct", "target_weight", "action", "note"
    ]

    if not existing.empty and "stock_id" in existing.columns:
        for col in required:
            if col not in existing.columns:
                existing[col] = ""
        existing["stock_id"] = existing["stock_id"].astype(str).str.strip()
        keep = existing[required].copy()
        keep = keep[keep["stock_id"] != ""].drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
        if len(keep) >= len(position_df):
            return keep

    out = pd.DataFrame({
        "stock_id": position_df["stock_id"] if "stock_id" in position_df.columns else [],
        "price_tier": "æªåé¡",
        "ref_price": "--",
        "shares": position_df["shares"] if "shares" in position_df.columns else [],
        "avg_cost": position_df["avg_cost"] if "avg_cost" in position_df.columns else [],
        "pnl_pct": "",
        "target_weight": 0,
        "action": "HOLD",
        "note": "ç­å¾æ°è³æ"
    })
    return out


def ensure_trade_plan() -> pd.DataFrame:
    df = read_csv_safe(ROOT / "trade_plan.csv")
    df = normalize_columns(df)
    required = ["action", "stock_id", "price_tier", "ref_price", "target_weight", "suggested_amount", "note"]
    if df.empty:
        return pd.DataFrame(columns=required)
    for col in required:
        if col not in df.columns:
            df[col] = ""
    return df[required].copy()


def ensure_summary() -> pd.DataFrame:
    df = read_csv_safe(ROOT / "full_summary.csv")
    df = normalize_columns(df)
    required = ["return", "mdd", "sharpe_daily"]
    if df.empty:
        return pd.DataFrame([{"return": 0, "mdd": 0, "sharpe_daily": 0}])
    for col in required:
        if col not in df.columns:
            df[col] = 0
    return df[required].head(1).copy()


def ensure_debug() -> pd.DataFrame:
    df = read_csv_safe(ROOT / "selection_debug.csv")
    df = normalize_columns(df)
    required = [
        "total_input", "valid_after_na", "core_primary_count",
        "alpha_primary_count", "core_final_count", "alpha_final_count"
    ]
    if df.empty:
        return pd.DataFrame([{
            "total_input": 0,
            "valid_after_na": 0,
            "core_primary_count": 0,
            "alpha_primary_count": 0,
            "core_final_count": 0,
            "alpha_final_count": 0
        }])
    for col in required:
        if col not in df.columns:
            df[col] = 0
    return df[required].head(1).copy()


def sync_meta():
    meta_path = DATA_DIR / "meta.json"
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

    meta.setdefault("generated_at", "")
    meta.setdefault("now_time", meta.get("generated_at", ""))
    meta.setdefault("signal_date", "")
    meta.setdefault("trade_date", "")
    meta.setdefault("price_panel_latest_date", "")
    meta["data_state"] = meta.get("data_state", "ok") or "ok"
    meta["source"] = meta.get("source", "v3.1_dashboard_bridge")
    meta["position_writeback_state"] = meta.get("position_writeback_state", "idle")

    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    watchlist_df = ensure_watchlist_root()
    positions_df = ensure_positions_root()

    watchlist_monitor_df = build_watchlist_monitor(watchlist_df)
    position_monitor_df = build_position_monitor(positions_df)
    trade_plan_df = ensure_trade_plan()
    summary_df = ensure_summary()
    debug_df = ensure_debug()

    watchlist_df.to_csv(DATA_DIR / "watchlist.csv", index=False, encoding="utf-8-sig")
    positions_df.to_csv(DATA_DIR / "current_positions.csv", index=False, encoding="utf-8-sig")
    watchlist_monitor_df.to_csv(DATA_DIR / "watchlist_monitor.csv", index=False, encoding="utf-8-sig")
    position_monitor_df.to_csv(DATA_DIR / "position_monitor.csv", index=False, encoding="utf-8-sig")
    trade_plan_df.to_csv(DATA_DIR / "trade_plan.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(DATA_DIR / "full_summary.csv", index=False, encoding="utf-8-sig")
    debug_df.to_csv(DATA_DIR / "selection_debug.csv", index=False, encoding="utf-8-sig")

    sync_meta()

    print("â v3.1 dashboard bridge å®æ")
    print(f"watchlist rows: {len(watchlist_df)}")
    print(f"watchlist_monitor rows: {len(watchlist_monitor_df)}")
    print(f"positions rows: {len(positions_df)}")
    print(f"position_monitor rows: {len(position_monitor_df)}")


if __name__ == "__main__":
    main()
