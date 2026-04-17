# v243_main.py
# v243：融合版
# 核心整合：
# 1) 保留 v241 的賺錢骨架：持倉延續 + 分級停損 + 分段加碼
# 2) 修正 v242 的錯誤：補股每天都能做，只有換股/重平衡採低頻
# 3) 保留 macro 控制：弱市場降曝險，極弱市場關閉新倉
# 4) 保留 v240 的溫和打分：不做過度硬過濾
# 5) 增加 cooldown：全停損後 2 天內不重買同檔，避免來回被洗

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5

STOP_LOSS_1 = -0.07      # 第一段：減碼
STOP_LOSS_2 = -0.12      # 第二段：全出
ADD_LV1 = 0.05           # 小加碼
ADD_LV2 = 0.10           # 再加碼

REBALANCE_INTERVAL = 3   # 只限制換股/重平衡頻率，不限制補股
COOLDOWN_DAYS = 2        # STOP_FULL 後暫停回補天數
MIN_HOLDINGS = 3         # 至少維持 3 檔
MAX_SINGLE_WEIGHT = 0.35 # 單一持股上限


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
    elif macro > 0:
        return 1.10
    elif macro > -0.6:
        return 0.80
    else:
        return 0.0


def rank(day):
    d = day.dropna(subset=["mom5", "mom10", "mom20", "close"]).copy()
    if d.empty:
        return d

    # 溫和打分，不做 v239 那種硬砍候選池
    d["score"] = (
        d["mom10"].fillna(0) * 0.45 +
        d["mom5"].fillna(0) * 0.25 +
        d["mom20"].fillna(0) * 0.20 -
        d["vol5"].fillna(0) * 0.10
    )

    # 只排除極端過熱，保留池深
    d = d[d["mom5"].fillna(0) < 0.30].copy()
    return d.sort_values("score", ascending=False)


def normalize_weights(holdings, exposure):
    if len(holdings) == 0:
        return holdings

    scores = np.array([max(0.001, float(holdings[s].get("score", 0.001))) for s in holdings], dtype=float)
    score_w = scores / scores.sum() if scores.sum() > 0 else np.repeat(1 / len(holdings), len(holdings))
    equal_w = np.repeat(1 / len(holdings), len(holdings))

    # 兼顧 edge 與分散：60% 分數權重 + 40% 等權
    weights = 0.6 * score_w + 0.4 * equal_w
    weights = np.minimum(weights, MAX_SINGLE_WEIGHT)
    weights = weights / weights.sum()

    syms = list(holdings.keys())
    for i, sym in enumerate(syms):
        holdings[sym]["weight"] = float(weights[i] * exposure)

    return holdings


def simulate(df):
    df = build_features(df)

    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    holdings = {}
    cooldown = {}  # symbol -> remaining days
    nav_rows = []
    trade_rows = []
    hold_rows = []

    day_count = 0

    for date in sorted(df["trade_date"].dropna().unique()):
        day = df[df["trade_date"] == date].copy()
        if day.empty:
            continue

        # cooldown 倒數
        for sym in list(cooldown.keys()):
            cooldown[sym] -= 1
            if cooldown[sym] <= 0:
                cooldown.pop(sym, None)

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)
        ranked = rank(day)

        ret = 0.0
        to_remove = []
        day_map = day.set_index("symbol")

        # 1) 先更新現有持倉：分級停損 + 分段加碼 + 當日收益
        for sym, pos in list(holdings.items()):
            if sym not in day_map.index:
                continue

            row = day_map.loc[sym]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]

            cur = float(row["close"])
            r = float(row["ret"]) if pd.notna(row["ret"]) else 0.0
            pnl = cur / float(pos["entry"]) - 1.0

            # 第二段停損：全出並進入 cooldown
            if pnl <= STOP_LOSS_2:
                trade_rows.append([date, "STOP_FULL", sym, cur, pos["weight"], pos.get("score", 0.0)])
                to_remove.append(sym)
                cooldown[sym] = COOLDOWN_DAYS
                continue

            # 第一段停損：減碼
            if pnl <= STOP_LOSS_1:
                holdings[sym]["weight"] *= 0.5
                trade_rows.append([date, "STOP_PART", sym, cur, holdings[sym]["weight"], pos.get("score", 0.0)])

            # 分段加碼：只對贏家，且不讓單一持股無限膨脹
            elif pnl >= ADD_LV2:
                holdings[sym]["weight"] *= 1.10
                trade_rows.append([date, "ADD_LV2", sym, cur, holdings[sym]["weight"], pos.get("score", 0.0)])

            elif pnl >= ADD_LV1:
                holdings[sym]["weight"] *= 1.04
                trade_rows.append([date, "ADD_LV1", sym, cur, holdings[sym]["weight"], pos.get("score", 0.0)])

            ret += r * float(pos["weight"])
            holdings[sym]["last"] = cur

            # 如果今天仍在排名池，更新 score，讓贏家能續抱
            if not ranked.empty and sym in ranked["symbol"].values:
                holdings[sym]["score"] = float(ranked[ranked["symbol"] == sym]["score"].iloc[0])

        for sym in to_remove:
            holdings.pop(sym, None)

        # 2) macro 關閉：全部出清，不開新倉
        if exposure == 0.0:
            if len(holdings) > 0:
                for sym, pos in list(holdings.items()):
                    trade_rows.append([date, "SELL_MACRO_OFF", sym, pos["last"], pos["weight"], pos.get("score", 0.0)])
                holdings = {}
            ret = 0.0

        else:
            # 3) 每天都允許補股（修正 v242 問題）
            if not ranked.empty:
                ranked_daily = ranked[~ranked["symbol"].isin(list(cooldown.keys()))].copy()

                for _, row in ranked_daily.iterrows():
                    if len(holdings) >= TOP_N:
                        break
                    sym = str(row["symbol"])
                    if sym not in holdings:
                        holdings[sym] = {
                            "entry": float(row["close"]),
                            "last": float(row["close"]),
                            "weight": 1.0 / TOP_N,
                            "score": float(row["score"])
                        }
                        trade_rows.append([date, "BUY", sym, float(row["close"]), holdings[sym]["weight"], holdings[sym]["score"]])

            # 4) 只有固定節奏才允許換股 / 主動重平衡
            if day_count % REBALANCE_INTERVAL == 0 and not ranked.empty and len(holdings) > 0:
                ranked_reb = ranked[~ranked["symbol"].isin(list(cooldown.keys()))].copy()

                # 維持至少 MIN_HOLDINGS，不讓系統掉成 1~2 檔
                if len(holdings) < MIN_HOLDINGS:
                    for _, row in ranked_reb.iterrows():
                        if len(holdings) >= MIN_HOLDINGS:
                            break
                        sym = str(row["symbol"])
                        if sym not in holdings:
                            holdings[sym] = {
                                "entry": float(row["close"]),
                                "last": float(row["close"]),
                                "weight": 1.0 / TOP_N,
                                "score": float(row["score"])
                            }
                            trade_rows.append([date, "BUY_RESTORE", sym, float(row["close"]), holdings[sym]["weight"], holdings[sym]["score"]])

                # 溫和換股：只有明顯更強才換，避免過度交易
                if len(holdings) >= TOP_N:
                    weakest_sym = min(holdings.keys(), key=lambda s: holdings[s].get("score", 0.0))
                    weakest_score = float(holdings[weakest_sym].get("score", 0.0))

                    for _, row in ranked_reb.head(8).iterrows():
                        sym = str(row["symbol"])
                        score = float(row["score"])
                        if sym not in holdings and score > weakest_score * 1.04:
                            trade_rows.append([date, "SWAP_OUT", weakest_sym, holdings[weakest_sym]["last"], holdings[weakest_sym]["weight"], holdings[weakest_sym].get("score", 0.0)])
                            holdings.pop(weakest_sym, None)

                            holdings[sym] = {
                                "entry": float(row["close"]),
                                "last": float(row["close"]),
                                "weight": 1.0 / TOP_N,
                                "score": score
                            }
                            trade_rows.append([date, "SWAP_IN", sym, float(row["close"]), holdings[sym]["weight"], score])
                            break

            holdings = normalize_weights(holdings, exposure)

        # 極端日保護
        ret = max(min(ret, 0.12), -0.08)

        nav *= (1.0 + ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        nav_rows.append([date, nav, ret, macro, exposure, dd, len(holdings)])

        for sym, pos in holdings.items():
            hold_rows.append([date, sym, pos["entry"], pos["last"], pos["weight"], pos.get("score", 0.0)])

        day_count += 1

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
        raise Exception("v243 沒有產出結果，請檢查資料。")

    summary = pd.DataFrame([{
        "return": float(nav_df["nav"].iloc[-1] / INITIAL_CAPITAL - 1.0),
        "mdd": float(nav_df["dd"].min()),
        "avg_exposure": float(nav_df["exposure"].mean()),
        "avg_count": float(nav_df["count"].mean()),
        "trade_count": int(len(trades_df))
    }])

    nav_df.to_csv("v243_nav.csv", index=False)
    trades_df.to_csv("v243_trades.csv", index=False)
    holds_df.to_csv("v243_holdings.csv", index=False)
    summary.to_csv("v243_summary.csv", index=False)

    print("DONE v243")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
