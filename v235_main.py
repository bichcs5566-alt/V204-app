import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"
INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv(INPUT_FILE)
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = None
    for c in ["trade_date", "date", "datetime", "signal_date"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise Exception(f"找不到日期欄位，目前欄位: {df.columns.tolist()}")

    if "symbol" not in df.columns:
        raise Exception(f"缺少 symbol 欄位，目前欄位: {df.columns.tolist()}")

    if "close" not in df.columns:
        raise Exception(f"缺少 close 欄位，目前欄位: {df.columns.tolist()}")

    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    # 正確的跨天報酬
    df["ret"] = df.groupby("symbol")["close"].pct_change()

    return df


def load_macro(df):
    macro = pd.read_csv(MACRO_FILE)
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    if "trade_date" not in macro.columns:
        raise Exception(f"macro_signal.csv 缺少 trade_date，目前欄位: {macro.columns.tolist()}")

    if "macro_score" not in macro.columns:
        raise Exception(f"macro_signal.csv 缺少 macro_score，目前欄位: {macro.columns.tolist()}")

    macro["trade_date"] = pd.to_datetime(macro["trade_date"], errors="coerce")
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    macro = macro.dropna(subset=["trade_date", "macro_score"]).copy()
    macro = macro.sort_values("trade_date")

    # 保留 macro 資料做對齊
    df = df.merge(macro[["trade_date", "macro_score"]], on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0.0)

    return df


def daily_signal_ret(day: pd.DataFrame) -> float:
    # 先排除沒有 ret 的資料
    day = day.dropna(subset=["ret"]).copy()
    if day.empty:
        return 0.0

    # 簡單穩定版：用動能排名挑前段股票，不再用全市場平均
    if "mom5" not in day.columns:
        day["mom5"] = 0.0
    if "mom10" not in day.columns:
        day["mom10"] = 0.0

    # 若當前檔案沒有事先建 feature，這裡補一次
    if day["mom5"].isna().all() or day["mom10"].isna().all():
        # 這裡不在單日內重算；交給外部 feature。若缺失就退回用 ret 平均
        return float(day["ret"].mean())

    d = day.dropna(subset=["mom5", "mom10"]).copy()
    if d.empty:
        return float(day["ret"].mean())

    d["score"] = d["mom10"] * 0.6 + d["mom5"] * 0.4
    d = d.sort_values("score", ascending=False).head(20).copy()

    return float(d["ret"].mean())


def build_features(df):
    g = df.groupby("symbol", group_keys=False)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    rows = []

    df = build_features(df)

    for date in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == date].copy()

        base_ret = daily_signal_ret(day)

        # 讓 macro 真正進系統：只控制倍率，不重寫策略
        macro_score = float(day["macro_score"].iloc[0]) if not day.empty else 0.0

        if macro_score > 0.5:
            exposure = 1.15
        elif macro_score > -0.2:
            exposure = 1.00
        else:
            exposure = 0.75

        ret = base_ret * exposure

        if pd.isna(ret):
            ret = 0.0

        # 基本風控：避免異常值污染
        ret = max(min(ret, 0.12), -0.12)

        nav *= (1 + ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        rows.append([
            pd.to_datetime(date),
            nav,
            ret,
            macro_score,
            exposure,
            dd
        ])

    out = pd.DataFrame(rows, columns=[
        "date", "nav", "ret", "macro_score", "exposure", "dd"
    ])
    return out


def main():
    df = load_data()
    df = load_macro(df)

    out = backtest(df)

    if out.empty:
        raise Exception("v235 沒有產出結果，請檢查資料內容。")

    summary = pd.DataFrame([{
        "return": float(out["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "mdd": float(out["dd"].min()),
        "avg_exposure": float(out["exposure"].mean()),
        "avg_macro_score": float(out["macro_score"].mean())
    }])

    out.to_csv("v235_nav.csv", index=False)
    summary.to_csv("v235_summary.csv", index=False)

    print("DONE v235")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
