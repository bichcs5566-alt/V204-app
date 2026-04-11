# =========================================================
# 檔案 2：v211_price_driven_nav.py
# =========================================================
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
SUM_PATH = ROOT / "v202_summary.csv"

PRICE_PANEL_CANDIDATES = [
    ROOT / "price_panel_daily.csv",
    ROOT / "price_panel.csv",
    ROOT / "daily_price_panel.csv",
    ROOT / "prices_daily.csv",
]

RETURN_LOG_PATH = ROOT / "v211_daily_return_log.csv"
SAMPLE_PATH = ROOT / "v211_symbol_return_sample.csv"

WINDOW_DAYS = 60


def to_num(s):
    return pd.to_numeric(s, errors="coerce")


def detect_col(df, candidates):
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            return cols[key]
    return None


def load_nav():
    nav = pd.read_csv(NAV_PATH)
    nav.columns = [str(c).strip() for c in nav.columns]

    if "date" not in nav.columns or "nav" not in nav.columns:
        raise ValueError("v202_nav.csv 必須包含 date 與 nav 欄位")

    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = to_num(nav["nav"])

    if "ret" in nav.columns:
        nav["ret"] = to_num(nav["ret"])
    else:
        nav["ret"] = np.nan

    if "holdings" in nav.columns:
        nav["holdings"] = to_num(nav["holdings"])
    else:
        nav["holdings"] = np.nan

    if "avg_exposure" in nav.columns:
        nav["avg_exposure"] = to_num(nav["avg_exposure"])
    else:
        nav["avg_exposure"] = np.nan

    nav = nav.dropna(subset=["date", "nav"]).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    if nav.empty:
        raise ValueError("v202_nav.csv 沒有有效資料")

    return nav


def load_snapshot_positions():
    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    symbol_col = detect_col(pos, ["symbol", "stock_id", "ticker", "code"])
    if symbol_col is None:
        raise ValueError("v202_positions.csv 找不到 symbol/ticker/code 欄")

    if symbol_col != "symbol":
        pos["symbol"] = pos[symbol_col].astype(str)
    else:
        pos["symbol"] = pos["symbol"].astype(str)

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / max(len(pos), 1)

    pos["weight"] = to_num(pos["weight"]).fillna(0.0)

    if "trade_date" in pos.columns:
        pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")
        if pos["trade_date"].notna().any():
            latest_trade_date = pos["trade_date"].max()
            pos = pos[pos["trade_date"] == latest_trade_date].copy()

    pos = pos.sort_values("symbol").drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)

    abs_sum = pos["weight"].abs().sum()
    if abs_sum > 1.5 and abs_sum > 0:
        pos["weight"] = pos["weight"] / abs_sum

    pos["weight"] = pos["weight"].clip(-1.0, 1.0)

    return pos


def find_price_panel():
    for p in PRICE_PANEL_CANDIDATES:
        if p.exists():
            return p
    return None


def load_price_panel(symbols, start_date, end_date):
    panel_path = find_price_panel()
    if panel_path is None:
        return None, None

    px = pd.read_csv(panel_path)
    px.columns = [str(c).strip() for c in px.columns]

    symbol_col = detect_col(px, ["symbol", "stock_id", "ticker", "code"])
    date_col = detect_col(px, ["date", "trade_date"])
    close_col = detect_col(px, ["close", "adj_close", "close_price", "收盤價"])

    if symbol_col is None or date_col is None or close_col is None:
        return panel_path.name, None

    if symbol_col != "symbol":
        px["symbol"] = px[symbol_col].astype(str)
    else:
        px["symbol"] = px["symbol"].astype(str)

    px["date"] = pd.to_datetime(px[date_col], errors="coerce")
    px["close"] = to_num(px[close_col])

    px = px.dropna(subset=["symbol", "date", "close"])
    px = px[px["symbol"].isin(set(symbols))]
    px = px[(px["date"] >= start_date) & (px["date"] <= end_date)]
    px = px.sort_values(["symbol", "date"]).reset_index(drop=True)

    if px.empty:
        return panel_path.name, px

    px["sym_ret"] = px.groupby("symbol")["close"].pct_change().fillna(0.0)
    px["sym_ret"] = px["sym_ret"].clip(-0.10, 0.10)

    return panel_path.name, px


def fallback_daily_returns(snapshot, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq="D")
    daily = pd.DataFrame({"date": dates})
    daily["ret"] = 0.0
    daily["holdings"] = int(snapshot["symbol"].nunique())
    daily["avg_exposure"] = float(snapshot["weight"].abs().sum())

    sample = pd.DataFrame(columns=["date", "symbol", "close", "sym_ret", "weight", "wret"])
    return daily, sample


def build_daily_returns_from_prices(px, snapshot):
    weights = snapshot[["symbol", "weight"]].copy()
    merged = px.merge(weights, on="symbol", how="inner")

    merged["sym_ret"] = to_num(merged["sym_ret"]).fillna(0.0)
    merged["weight"] = to_num(merged["weight"]).fillna(0.0)
    merged["wret"] = merged["sym_ret"] * merged["weight"]

    daily = (
        merged.groupby("date", as_index=False)
        .agg(
            ret=("wret", "sum"),
            holdings=("symbol", "nunique"),
            avg_exposure=("weight", lambda x: float(pd.Series(x).abs().sum()))
        )
    )

    daily["ret"] = daily["ret"].fillna(0.0).clip(-0.10, 0.10)

    sample = merged[["date", "symbol", "close", "sym_ret", "weight", "wret"]].copy()
    return daily, sample


def rebuild_recent_window(nav, daily, start_date):
    nav2 = nav.copy()

    before = nav2[nav2["date"] < start_date].copy()
    after_original = nav2[nav2["date"] >= start_date].copy()

    full_dates = pd.DataFrame({
        "date": pd.date_range(start_date, nav2["date"].max(), freq="D")
    })

    after = full_dates.merge(after_original, on="date", how="left", suffixes=("", "_orig"))
    after = after.merge(daily, on="date", how="left", suffixes=("", "_new"))

    after["ret"] = pd.to_numeric(after["ret"], errors="coerce").fillna(0.0)
    after["holdings"] = pd.to_numeric(after["holdings"], errors="coerce")
    after["avg_exposure"] = pd.to_numeric(after["avg_exposure"], errors="coerce")

    if before.empty:
        base_nav = float(nav2.iloc[0]["nav"])
    else:
        base_nav = float(before.iloc[-1]["nav"])

    after = after.sort_values("date").reset_index(drop=True)

    for i in range(len(after)):
        if i == 0:
            after.loc[i, "nav"] = base_nav * (1.0 + float(after.loc[i, "ret"]))
        else:
            after.loc[i, "nav"] = float(after.loc[i - 1, "nav"]) * (1.0 + float(after.loc[i, "ret"]))

    out = pd.concat([before, after[["date", "nav", "ret", "holdings", "avg_exposure"]]], ignore_index=True)
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out


def build_summary(nav):
    initial_capital = float(nav["nav"].iloc[0])
    final_nav = float(nav["nav"].iloc[-1])

    total_return = (final_nav / initial_capital - 1.0) if initial_capital != 0 else 0.0

    rets = nav["nav"].pct_change().dropna()
    if len(rets) > 1 and float(rets.std()) != 0:
        sharpe = float((rets.mean() / rets.std()) * np.sqrt(252))
    else:
        sharpe = 0.0

    mdd = float((nav["nav"] / nav["nav"].cummax() - 1.0).min()) if len(nav) else 0.0

    return pd.DataFrame([{
        "start_date": str(pd.to_datetime(nav["date"]).min().date()),
        "end_date": str(pd.to_datetime(nav["date"]).max().date()),
        "initial_capital": initial_capital,
        "final_nav": final_nav,
        "total_return": total_return,
        "sharpe": sharpe,
        "mdd": mdd,
        "trading_days": int(len(nav)),
        "avg_holdings": float(pd.to_numeric(nav["holdings"], errors="coerce").mean()) if "holdings" in nav.columns else np.nan,
        "avg_exposure": float(pd.to_numeric(nav["avg_exposure"], errors="coerce").mean()) if "avg_exposure" in nav.columns else np.nan
    }])


def write_logs(panel_name, snapshot, daily, sample, start_date, end_date):
    log = pd.DataFrame([{
        "window_start": str(start_date.date()),
        "window_end": str(end_date.date()),
        "price_panel_used": panel_name if panel_name is not None else "fallback_none",
        "snapshot_rows": int(len(snapshot)),
        "snapshot_symbols": int(snapshot["symbol"].nunique()),
        "daily_rows": int(len(daily)),
        "daily_nonzero_ret_days": int((daily["ret"].abs() > 0).sum()) if "ret" in daily.columns else 0
    }])
    log.to_csv(RETURN_LOG_PATH, index=False)

    sample2 = sample.copy()
    if not sample2.empty:
        sample2["date"] = pd.to_datetime(sample2["date"]).dt.strftime("%Y-%m-%d")
    sample2.head(500).to_csv(SAMPLE_PATH, index=False)


def main():
    nav = load_nav()
    snapshot = load_snapshot_positions()

    end_date = nav["date"].max()
    start_date = end_date - pd.Timedelta(days=WINDOW_DAYS)

    panel_name, px = load_price_panel(snapshot["symbol"].tolist(), start_date, end_date)

    if px is None or (isinstance(px, pd.DataFrame) and px.empty):
        daily, sample = fallback_daily_returns(snapshot, start_date, end_date)
    else:
        daily, sample = build_daily_returns_from_prices(px, snapshot)

    nav2 = rebuild_recent_window(nav, daily, start_date)
    summary = build_summary(nav2)

    nav_save = nav2.copy()
    nav_save["date"] = pd.to_datetime(nav_save["date"]).dt.strftime("%Y-%m-%d")
    nav_save.to_csv(NAV_PATH, index=False)

    pos_save = snapshot.copy()
    if "trade_date" in pos_save.columns:
        pos_save["trade_date"] = pd.to_datetime(pos_save["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    pos_save.to_csv(POS_PATH, index=False)

    summary.to_csv(SUM_PATH, index=False)
    write_logs(panel_name, snapshot, daily, sample, start_date, end_date)

    print("v211 DONE")
    print("price panel used:", panel_name)
    print("snapshot rows:", len(snapshot))
    print("daily rows:", len(daily))
    print("nonzero ret days:", int((daily["ret"].abs() > 0).sum()) if "ret" in daily.columns else 0)


if __name__ == "__main__":
    main()
