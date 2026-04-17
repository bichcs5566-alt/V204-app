import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000.0
TOP_N = 5
STOP_LOSS = -0.05
SWAP_THRESHOLD = 0.03


def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df.columns = [str(c).lower().strip() for c in df.columns]

    date_col = next((c for c in ["trade_date","date","datetime","signal_date"] if c in df.columns), None)

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date","symbol","close"])
    df = df.sort_values(["symbol","trade_date"])

    df["ret"] = df.groupby("symbol")["close"].pct_change()

    return df


def load_macro(df):
    macro = pd.read_csv("macro_signal.csv")
    macro.columns = [str(c).lower().strip() for c in macro.columns]

    macro["trade_date"] = pd.to_datetime(macro["trade_date"])

    df = df.merge(macro, on="trade_date", how="left")
    df["macro_score"] = df["macro_score"].ffill().fillna(0)

    return df


def build_features(df):
    g = df.groupby("symbol")
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    return df


def get_exposure(macro):
    if macro > 0.5:
        return 1.3
    elif macro > 0:
        return 1.1
    elif macro > -0.6:
        return 0.8
    else:
        return 0.0


def rank(day):
    d = day.dropna(subset=["mom5","mom10"]).copy()
    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    return d.sort_values("score", ascending=False)


def simulate(df):
    df = build_features(df)

    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL

    holdings = {}
    nav_rows = []
    trade_rows = []

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        macro = float(day["macro_score"].iloc[0])
        exposure = get_exposure(macro)

        ranked = rank(day)

        # === 更新持倉 ===
        to_remove = []
        ret = 0.0

        for sym, pos in holdings.items():
            if sym in day["symbol"].values:
                row = day[day["symbol"] == sym].iloc[-1]
                r = row["ret"] if pd.notna(row["ret"]) else 0

                # 停損
                if row["close"] / pos["entry"] - 1 <= STOP_LOSS:
                    trade_rows.append([date,"STOP",sym,row["close"]])
                    to_remove.append(sym)
                else:
                    ret += r * pos["weight"]
                    holdings[sym]["last"] = row["close"]

        for sym in to_remove:
            holdings.pop(sym)

        # === 補/換股 ===
        if exposure > 0:
            top = ranked.head(10)

            # 補股
            while len(holdings) < TOP_N:
                for _, r in top.iterrows():
                    if r["symbol"] not in holdings:
                        holdings[r["symbol"]] = {
                            "entry": r["close"],
                            "last": r["close"],
                            "weight": 0
                        }
                        trade_rows.append([date,"BUY",r["symbol"],r["close"]])
                        break
                else:
                    break

            # 換股（只有更強才換）
            if len(holdings) >= TOP_N and len(top) > 0:
                weakest = min(holdings.keys(), key=lambda s: ranked[ranked["symbol"]==s]["score"].values[0] if s in ranked["symbol"].values else -999)
                best = top.iloc[0]

                if best["symbol"] not in holdings:
                    weakest_score = ranked[ranked["symbol"]==weakest]["score"].values[0]
                    if best["score"] > weakest_score * (1+SWAP_THRESHOLD):
                        trade_rows.append([date,"SWAP_OUT",weakest,holdings[weakest]["last"]])
                        holdings.pop(weakest)

                        holdings[best["symbol"]] = {
                            "entry": best["close"],
                            "last": best["close"],
                            "weight": 0
                        }
                        trade_rows.append([date,"SWAP_IN",best["symbol"],best["close"]])

        # === 重新配置權重 ===
        if len(holdings) > 0:
            w = exposure / len(holdings)
            for sym in holdings:
                holdings[sym]["weight"] = w

        # === 風控 ===
        ret = max(min(ret,0.12),-0.08)

        nav *= (1+ret)
        peak = max(peak, nav)
        dd = nav/peak - 1

        nav_rows.append([date,nav,ret,macro,exposure,dd,len(holdings)])

    return pd.DataFrame(nav_rows,columns=["date","nav","ret","macro","exposure","dd","count"]), pd.DataFrame(trade_rows,columns=["date","action","symbol","price"])


def main():
    df = load_data()
    df = load_macro(df)

    nav, trades = simulate(df)

    summary = pd.DataFrame([{
        "return": nav["nav"].iloc[-1]/INITIAL_CAPITAL - 1,
        "mdd": nav["dd"].min(),
        "avg_exposure": nav["exposure"].mean()
    }])

    nav.to_csv("v237_1_nav.csv",index=False)
    trades.to_csv("v237_1_trades.csv",index=False)
    summary.to_csv("v237_1_summary.csv",index=False)

    print(summary)


if __name__ == "__main__":
    main()
