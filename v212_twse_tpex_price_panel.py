import pandas as pd
import numpy as np
import requests
import time
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parent

NAV_PATH = ROOT / "v202_nav.csv"
POS_PATH = ROOT / "v202_positions.csv"
SUM_PATH = ROOT / "v202_summary.csv"
PANEL_PATH = ROOT / "price_panel_daily.csv"

FETCH_LOG_PATH = ROOT / "v212_fetch_log.csv"
SAMPLE_PATH = ROOT / "v212_symbol_return_sample.csv"

WINDOW_DAYS = 90
TIMEOUT = 20
UA = {"User-Agent": "Mozilla/5.0"}


def to_num(s):
    return pd.to_numeric(s, errors="coerce")


def detect_col(df, candidates):
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def safe_get(url, params=None):
    r = requests.get(url, params=params, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r


def roc_date(dt):
    return f"{dt.year - 1911:03d}/{dt.month:02d}/{dt.day:02d}"


def load_nav():
    nav = pd.read_csv(NAV_PATH)
    nav.columns = [str(c).strip() for c in nav.columns]

    if "date" not in nav.columns or "nav" not in nav.columns:
        raise ValueError("v202_nav.csv 必須包含 date 與 nav")

    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    nav["nav"] = to_num(nav["nav"])

    if "ret" not in nav.columns:
        nav["ret"] = np.nan
    else:
        nav["ret"] = to_num(nav["ret"])

    if "holdings" not in nav.columns:
        nav["holdings"] = np.nan
    else:
        nav["holdings"] = to_num(nav["holdings"])

    if "avg_exposure" not in nav.columns:
        nav["avg_exposure"] = np.nan
    else:
        nav["avg_exposure"] = to_num(nav["avg_exposure"])

    nav = (
        nav.dropna(subset=["date", "nav"])
           .sort_values("date")
           .drop_duplicates("date", keep="last")
           .reset_index(drop=True)
    )

    if nav.empty:
        raise ValueError("v202_nav.csv 沒有有效資料")

    return nav


def load_snapshot_positions():
    pos = pd.read_csv(POS_PATH)
    pos.columns = [str(c).strip() for c in pos.columns]

    symbol_col = detect_col(pos, ["symbol", "stock_id", "ticker", "code"])
    if symbol_col is None:
        raise ValueError("v202_positions.csv 找不到 symbol/ticker/code 欄位")

    pos["symbol"] = pos[symbol_col].astype(str)

    if "weight" not in pos.columns:
        pos["weight"] = 1.0 / max(len(pos), 1)

    pos["weight"] = to_num(pos["weight"]).fillna(0.0)

    if "trade_date" in pos.columns:
        pos["trade_date"] = pd.to_datetime(pos["trade_date"], errors="coerce")
        if pos["trade_date"].notna().any():
            latest_trade_date = pos["trade_date"].max()
            pos = pos[pos["trade_date"] == latest_trade_date].copy()

    pos = (
        pos.sort_values("symbol")
           .drop_duplicates("symbol", keep="last")
           .reset_index(drop=True)
    )

    abs_sum = pos["weight"].abs().sum()
    if abs_sum > 1.5 and abs_sum > 0:
        pos["weight"] = pos["weight"] / abs_sum

    pos["weight"] = pos["weight"].clip(-1.0, 1.0)
    return pos


def fetch_twse_day(dt):
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {
        "response": "csv",
        "date": dt.strftime("%Y%m%d"),
        "type": "ALLBUT0999",
    }
    text = safe_get(url, params=params).text

    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("="):
            continue

        if line.startswith('"證券代號"') or line.startswith("證券代號"):
            lines.append(line)
            continue

        if lines:
            if line.startswith('"') or (line and line[0].isdigit()):
                lines.append(line)
            else:
                break

    if len(lines) < 2:
        return pd.DataFrame(columns=["symbol", "date", "close", "market"])

    df = pd.read_csv(StringIO("\n".join(lines)))
    df.columns = [str(c).strip().replace('"', "") for c in df.columns]

    symbol_col = detect_col(df, ["證券代號", "symbol"])
    close_col = detect_col(df, ["收盤價", "close"])

    if symbol_col is None or close_col is None:
        return pd.DataFrame(columns=["symbol", "date", "close", "market"])

    df["symbol"] = df[symbol_col].astype(str).str.strip()
    df["close"] = (
        df[close_col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"--": np.nan, "---": np.nan, "": np.nan})
    )
    df["close"] = to_num(df["close"])
    df["date"] = pd.Timestamp(dt.date())
    df["market"] = "TWSE"

    df = df.dropna(subset=["symbol", "close"])
    return df[["symbol", "date", "close", "market"]].copy()


def fetch_tpex_day(dt):
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php"
    params = {
        "l": "zh-tw",
        "d": roc_date(dt),
        "se": "AL",
        "s": "0,asc,0",
        "o": "csv",
    }

    try:
        text = safe_get(url, params=params).text
    except Exception:
        params.pop("o", None)
        text = safe_get(url, params=params).text

    lines = []
    header_found = False

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        if ("代號" in line and "名稱" in line and "收盤" in line) or ("Securities Company Code" in line):
            header_found = True
            lines.append(line)
            continue

        if header_found:
            if line.startswith('"') or (line and (line[0].isdigit() or line[0].isalpha())):
                lines.append(line)

    if len(lines) < 2:
        return pd.DataFrame(columns=["symbol", "date", "close", "market"])

    try:
        df = pd.read_csv(StringIO("\n".join(lines)))
    except Exception:
        return pd.DataFrame(columns=["symbol", "date", "close", "market"])

    df.columns = [str(c).strip().replace('"', "") for c in df.columns]

    symbol_col = detect_col(df, ["代號", "股票代號", "證券代號", "Securities Company Code"])
    close_col = detect_col(df, ["收盤", "收盤價", "Close", "Closing Price"])

    if symbol_col is None or close_col is None:
        return pd.DataFrame(columns=["symbol", "date", "close", "market"])

    df["symbol"] = df[symbol_col].astype(str).str.strip()
    df["close"] = (
        df[close_col]
        .astype(str)
        .str.replace(",", "", regex=False)
        .replace({"--": np.nan, "---": np.nan, "": np.nan})
    )
    df["close"] = to_num(df["close"])
    df["date"] = pd.Timestamp(dt.date())
    df["market"] = "TPEx"

    df = df.dropna(subset=["symbol", "close"])
    return df[["symbol", "date", "close", "market"]].copy()


def build_price_panel(symbols, start_date, end_date):
    days = pd.date_range(start_date, end_date, freq="D")
    all_rows = []
    stats = []

    for dt in days:
        twse_ok = False
        tpex_ok = False
        twse_rows = 0
        tpex_rows = 0

        try:
            twse = fetch_twse_day(dt)
            twse = twse[twse["symbol"].isin(symbols)]
            twse_ok = True
            twse_rows = len(twse)
        except Exception:
            twse = pd.DataFrame(columns=["symbol", "date", "close", "market"])

        try:
            tpex = fetch_tpex_day(dt)
            tpex = tpex[tpex["symbol"].isin(symbols)]
            tpex_ok = True
            tpex_rows = len(tpex)
        except Exception:
            tpex = pd.DataFrame(columns=["symbol", "date", "close", "market"])

        day_df = pd.concat([twse, tpex], ignore_index=True)
        if not day_df.empty:
            all_rows.append(day_df)

        stats.append({
            "date": dt.strftime("%Y-%m-%d"),
            "twse_ok": twse_ok,
            "tpex_ok": tpex_ok,
            "twse_rows": twse_rows,
            "tpex_rows": tpex_rows,
            "total_rows": len(day_df),
        })

        time.sleep(0.35)

    fetch_log = pd.DataFrame(stats)

    if all_rows:
        px = (
            pd.concat(all_rows, ignore_index=True)
            .sort_values(["symbol", "date"])
            .drop_duplicates(["symbol", "date"], keep="last")
            .reset_index(drop=True)
        )
    else:
        px = pd.DataFrame(columns=["symbol", "date", "close", "market"])

    return px, fetch_log


def build_daily_returns(px, snapshot):
    if px.empty:
        daily = pd.DataFrame(columns=["date", "ret", "holdings", "avg_exposure"])
        sample = pd.DataFrame(columns=["date", "symbol", "close", "sym_ret", "weight", "wret"])
        return daily, sample

    weights = snapshot[["symbol", "weight"]].copy()
    px2 = px.merge(weights, on="symbol", how="inner")

    px2["sym_ret"] = px2.groupby("symbol")["close"].pct_change().fillna(0.0)
    px2["sym_ret"] = px2["sym_ret"].clip(-0.10, 0.10)
    px2["wret"] = px2["sym_ret"] * px2["weight"]

    daily = (
        px2.groupby("date", as_index=False)
        .agg(
            ret=("wret", "sum"),
            holdings=("symbol", "nunique"),
            avg_exposure=("weight", lambda x: float(pd.Series(x).abs().sum()))
        )
    )
    daily["ret"] = daily["ret"].fillna(0.0).clip(-0.10, 0.10)

    sample = px2[["date", "symbol", "close", "sym_ret", "weight", "wret"]].copy()
    return daily, sample


def rebuild_recent_window(nav, daily, start_date):
    nav2 = nav.copy()

    before = nav2[nav2["date"] < start_date].copy()
    after_original = nav2[nav2["date"] >= start_date].copy()

    full_dates = pd.DataFrame({"date": pd.date_range(start_date, nav2["date"].max(), freq="D")})
    after = full_dates.merge(after_original, on="date", how="left")
    after = after.merge(daily, on="date", how="left", suffixes=("", "_new"))

    after["ret"] = to_num(after["ret"]).fillna(0.0)
    after["holdings"] = to_num(after["holdings"])
    after["avg_exposure"] = to_num(after["avg_exposure"])

    base_nav = float(nav2.iloc[0]["nav"]) if before.empty else float(before.iloc[-1]["nav"])

    after = after.sort_values("date").reset_index(drop=True)

    for i in range(len(after)):
        if i == 0:
            after.loc[i, "nav"] = base_nav * (1.0 + float(after.loc[i, "ret"]))
        else:
            after.loc[i, "nav"] = float(after.loc[i - 1, "nav"]) * (1.0 + float(after.loc[i, "ret"]))

    out = pd.concat([before, after[["date", "nav", "ret", "holdings", "avg_exposure"]]], ignore_index=True)
    out = out.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    return out


def build_summary(nav):
    initial_capital = float(nav["nav"].iloc[0])
    final_nav = float(nav["nav"].iloc[-1])
    total_return = (final_nav / initial_capital - 1.0) if initial_capital != 0 else 0.0

    rets = nav["nav"].pct_change().dropna()
    sharpe = float((rets.mean() / rets.std()) * np.sqrt(252)) if len(rets) > 1 and float(rets.std()) != 0 else 0.0
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
        "avg_holdings": float(to_num(nav["holdings"]).mean()) if "holdings" in nav.columns else np.nan,
        "avg_exposure": float(to_num(nav["avg_exposure"]).mean()) if "avg_exposure" in nav.columns else np.nan
    }])


def main():
    nav = load_nav()
    snapshot = load_snapshot_positions()

    end_date = nav["date"].max()
    start_date = end_date - pd.Timedelta(days=WINDOW_DAYS)

    px, fetch_log = build_price_panel(set(snapshot["symbol"].tolist()), start_date, end_date)

    if not px.empty:
        px_save = px.copy()
        px_save["date"] = pd.to_datetime(px_save["date"]).dt.strftime("%Y-%m-%d")
        px_save.to_csv(PANEL_PATH, index=False)

    daily, sample = build_daily_returns(px, snapshot)
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
    fetch_log.to_csv(FETCH_LOG_PATH, index=False)

    sample2 = sample.copy()
    if not sample2.empty:
        sample2["date"] = pd.to_datetime(sample2["date"]).dt.strftime("%Y-%m-%d")
    sample2.head(500).to_csv(SAMPLE_PATH, index=False)

    print("v212 DONE")
    print("snapshot rows:", len(snapshot))
    print("price rows:", len(px))
    print("daily rows:", len(daily))
    print("nonzero ret days:", int((daily["ret"].abs() > 0).sum()) if not daily.empty else 0))


if __name__ == "__main__":
    main()
