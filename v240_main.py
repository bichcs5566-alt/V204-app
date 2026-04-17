import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5
STOP_LOSS = -0.07
SWAP_THRESHOLD = 0.04


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
    macro = macro.sort_values("trade_date")

    df = df.merge(macro[["trade_date", "macro_score"]], on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0.0)
    return df


def build_features(df):
    g = df.groupby("symbol", group_keys=False)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["vol5"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)
    return df


def get_exposure(macro):
    if macro > 0.5:
        return 1.30
    elif macro > 0.0:
        return 1.10
    elif macro > -0.6:
        return 0.80
    else:
        return 0.0


def rank(day):
    d = day.dropna(subset=["mom5", "mom10", "mom20", "close"]).copy()
    if d.empty:
        return d

    # v240：溫和加分，不做硬過濾砍掉候選池
    d["score"] = (
        d["mom10"].fillna(0) * 0.45 +
        d["mom5"].fillna(0) * 0.25 +
        d["mom20"].fillna(0) * 0.20 -
        d["vol5"].fillna(0) * 0.10
    )

    # 只排除極端過熱，不排除普通弱勢，避免像 v239 一樣持股池太淺
    d = d[d["mom5"].fillna(0) < 0.30].copy()

    return d.sort_values("score", ascending=False)


def simulate(df):
    df = build_features(df)

    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    holdings = {}
    nav_rows = []
    trade_rows = []
    hold_rows = []

    for date in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)
        ranked = rank(day)

        ret = 0.0
        to_remove = []
        day_map = day.set_index("symbol")

        # 更新持倉 / 停損
        for sym, pos in holdings.items():
            if sym in day_map.index:
                row = day_map.loc[sym]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[-1]

                cur_price = float(row["close"])
                r = float(row["ret"]) if pd.notna(row["ret"]) else 0.0

                if cur_price / pos["entry"] - 1.0 <= STOP_LOSS:
                    trade_rows.append([date, "STOP", sym, cur_price, pos["weight"], pos["score"]])
                    to_remove.append(sym)
                else:
                    holdings[sym]["last"] = cur_price
                    if sym in ranked["symbol"].values:
                        holdings[sym]["score"] = float(ranked[ranked["symbol"] == sym]["score"].iloc[0])
                    ret += r * pos["weight"]

        for sym in to_remove:
            holdings.pop(sym, None)

        # macro 關閉則空手
        if exposure == 0.0:
            if len(holdings) > 0:
                for sym, pos in list(holdings.items()):
                    trade_rows.append([date, "SELL_MACRO_OFF", sym, pos["last"], pos["weight"], pos["score"]])
                holdings = {}
            ret = 0.0
        else:
            # 候選池維持寬一點，避免持股數過低
            top = ranked.head(15).copy()

            # 補股到 TOP_N
            if not top.empty:
                for _, row in top.iterrows():
                    if len(holdings) >= TOP_N:
                        break
                    sym = str(row["symbol"])
                    if sym not in holdings:
                        holdings[sym] = {
                            "entry": float(row["close"]),
                            "last": float(row["close"]),
                            "weight": 0.0,
                            "score": float(row["score"]),
                        }
                        trade_rows.append([date, "BUY", sym, float(row["close"]), 0.0, float(row["score"])])

            # 溫和換股：只在明顯更強時換，不要像 v239 那樣過度篩選
            if len(holdings) >= TOP_N and not top.empty:
                weakest_sym = min(holdings.keys(), key=lambda s: holdings[s]["score"])
                weakest_score = holdings[weakest_sym]["score"]

                for _, row in top.head(8).iterrows():
                    sym = str(row["symbol"])
                    score = float(row["score"])

                    if sym not in holdings and score > weakest_score * (1 + SWAP_THRESHOLD):
                        trade_rows.append([date, "SWAP_OUT", weakest_sym, holdings[weakest_sym]["last"], holdings[weakest_sym]["weight"], holdings[weakest_sym]["score"]])
                        holdings.pop(weakest_sym, None)

                        holdings[sym] = {
                            "entry": float(row["close"]),
                            "last": float(row["close"]),
                            "weight": 0.0,
                            "score": score,
                        }
                        trade_rows.append([date, "SWAP_IN", sym, float(row["close"]), 0.0, score])
                        break

            # 半等權配置：保留 edge，但避免太極端
            if len(holdings) > 0:
                syms = list(holdings.keys())
                scores = np.array([max(0.001, holdings[s]["score"]) for s in syms], dtype=float)
                score_weights = scores / scores.sum()
                equal_weights = np.repeat(1 / len(syms), len(syms))

                # 混合權重：50% 等權 + 50% 分數權重
                weights = 0.5 * equal_weights + 0.5 * score_weights
                weights = weights / weights.sum()

                # 限制單一持股上限，避免像 v239 那樣過度集中
                weights = np.minimum(weights, 0.30)
                weights = weights / weights.sum()

                for i, sym in enumerate(syms):
                    holdings[sym]["weight"] = float(weights[i] * exposure)

        ret = max(min(ret, 0.12), -0.08)

        nav *= (1 + ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        nav_rows.append([date, nav, ret, macro, exposure, dd, len(holdings)])

        for sym, pos in holdings.items():
            hold_rows.append([date, sym, pos["entry"], pos["last"], pos["weight"], pos["score"]])

    nav_df = pd.DataFrame(nav_rows, columns=[
        "date", "nav", "ret", "macro", "exposure", "dd", "count"
    ])

    trades_df = pd.DataFrame(trade_rows, columns=[
        "date", "action", "symbol", "price", "weight", "score"
    ])

    holds_df = pd.DataFrame(hold_rows, columns=[
        "date", "symbol", "entry_price", "last_price", "weight", "score"
    ])

    return nav_df, trades_df, holds_df


def main():
    df = load_data()
    df = load_macro(df)

    nav_df, trades_df, holds_df = simulate(df)

    if nav_df.empty:
        raise Exception("v240 沒有產出結果，請檢查資料。")

    summary = pd.DataFrame([{
        "return": float(nav_df["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "mdd": float(nav_df["dd"].min()),
        "avg_exposure": float(nav_df["exposure"].mean()),
        "avg_count": float(nav_df["count"].mean()),
        "trade_count": int(len(trades_df))
    }])

    nav_df.to_csv("v240_nav.csv", index=False)
    trades_df.to_csv("v240_trades.csv", index=False)
    holds_df.to_csv("v240_holdings.csv", index=False)
    summary.to_csv("v240_summary.csv", index=False)

    print("DONE v240")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
