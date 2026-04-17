# v242_main.py（準正式版｜降低交易頻率 + 分段加碼 + 穩定持倉）

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5

STOP_LOSS_1 = -0.07   # 第一段減碼
STOP_LOSS_2 = -0.12   # 第二段全出

ADD_LV1 = 0.05        # 小加碼
ADD_LV2 = 0.10        # 再加碼

REBALANCE_INTERVAL = 3  # 每 3 個交易日才允許補股 / 調倉


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date", "date", "datetime", "signal_date"] if c in df.columns), None)
    if date_col is None:
        raise Exception(f"找不到日期欄位: {df.columns.tolist()}")

    if "symbol" not in df.columns or "close" not in df.columns:
        raise Exception(f"缺少 symbol / close 欄位: {df.columns.tolist()}")

    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date", "symbol", "close"]).copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    # 跨天報酬
    df["ret"] = df.groupby("symbol")["close"].pct_change()
    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    if "trade_date" not in macro.columns or "macro_score" not in macro.columns:
        raise Exception(f"macro_signal.csv 欄位錯誤: {macro.columns.tolist()}")

    macro["trade_date"] = pd.to_datetime(macro["trade_date"], errors="coerce")
    macro["macro_score"] = pd.to_numeric(macro["macro_score"], errors="coerce")
    macro = macro.dropna(subset=["trade_date", "macro_score"]).copy()

    df = df.merge(macro[["trade_date", "macro_score"]], on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0.0)
    return df


def build_features(df):
    g = df.groupby("symbol", group_keys=False)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def get_exposure(macro):
    if macro > 0.5:
        return 1.30
    elif macro > 0:
        return 1.10
    elif macro > -0.6:
        return 0.80
    else:
        return 0.0


def rank(day):
    d = day.dropna(subset=["mom5", "mom10"]).copy()
    if d.empty:
        return d

    d["score"] = d["mom10"] * 0.6 + d["mom5"] * 0.4
    return d.sort_values("score", ascending=False)


def simulate(df):
    df = build_features(df)

    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    holdings = {}
    nav_rows = []
    trade_rows = []

    day_count = 0

    for date in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)
        ranked = rank(day)

        ret = 0.0
        to_remove = []

        # 更新現有持倉
        for sym, pos in list(holdings.items()):
            rows = day[day["symbol"] == sym]
            if rows.empty:
                continue

            row = rows.iloc[-1]
            cur = float(row["close"])
            r = float(row["ret"]) if pd.notna(row["ret"]) else 0.0
            pnl = cur / pos["entry"] - 1.0

            # 分級停損
            if pnl <= STOP_LOSS_2:
                trade_rows.append([date, "STOP_FULL", sym, cur, pos["weight"]])
                to_remove.append(sym)

            elif pnl <= STOP_LOSS_1:
                holdings[sym]["weight"] *= 0.5
                trade_rows.append([date, "STOP_PART", sym, cur, holdings[sym]["weight"]])

            # 分段加碼
            elif pnl >= ADD_LV2:
                holdings[sym]["weight"] *= 1.15
                trade_rows.append([date, "ADD_LV2", sym, cur, holdings[sym]["weight"]])

            elif pnl >= ADD_LV1:
                holdings[sym]["weight"] *= 1.05
                trade_rows.append([date, "ADD_LV1", sym, cur, holdings[sym]["weight"]])

            ret += r * pos["weight"]
            holdings[sym]["last"] = cur

        for sym in to_remove:
            holdings.pop(sym, None)

        # macro 關閉時全部出清
        if exposure == 0.0:
            if len(holdings) > 0:
                for sym, pos in list(holdings.items()):
                    trade_rows.append([date, "SELL_MACRO_OFF", sym, pos["last"], pos["weight"]])
                holdings = {}
            ret = 0.0

        # 只在固定節奏調倉
        elif day_count % REBALANCE_INTERVAL == 0 and not ranked.empty:
            for _, row in ranked.iterrows():
                if len(holdings) >= TOP_N:
                    break

                sym = str(row["symbol"])
                if sym not in holdings:
                    holdings[sym] = {
                        "entry": float(row["close"]),
                        "last": float(row["close"]),
                        "weight": 1.0 / TOP_N
                    }
                    trade_rows.append([date, "BUY", sym, float(row["close"]), holdings[sym]["weight"]])

        # 權重正規化
        if len(holdings) > 0:
            total_w = sum(float(holdings[s]["weight"]) for s in holdings)

            if total_w <= 0:
                equal_w = exposure / len(holdings)
                for s in holdings:
                    holdings[s]["weight"] = equal_w
            else:
                for s in holdings:
                    holdings[s]["weight"] = (float(holdings[s]["weight"]) / total_w) * exposure

        ret = max(min(ret, 0.12), -0.08)

        nav *= (1.0 + ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        nav_rows.append([date, nav, ret, macro, exposure, dd, len(holdings)])
        day_count += 1

    nav_df = pd.DataFrame(nav_rows, columns=[
        "date", "nav", "ret", "macro", "exposure", "dd", "count"
    ])

    trades_df = pd.DataFrame(trade_rows, columns=[
        "date", "action", "symbol", "price", "weight"
    ])

    return nav_df, trades_df


def main():
    df = load_data()
    df = load_macro(df)

    nav, trades = simulate(df)

    if nav.empty:
        raise Exception("v242 沒有產出結果，請檢查資料。")

    summary = pd.DataFrame([{
        "return": float(nav["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "mdd": float(nav["dd"].min()),
        "avg_exposure": float(nav["exposure"].mean()),
        "avg_count": float(nav["count"].mean()),
        "trade_count": int(len(trades))
    }])

    nav.to_csv("v242_nav.csv", index=False)
    trades.to_csv("v242_trades.csv", index=False)
    summary.to_csv("v242_summary.csv", index=False)

    print("DONE v242")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
