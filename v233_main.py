import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"
INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv(INPUT_FILE)
    df.columns = [c.lower().strip() for c in df.columns]

    date_col = next(c for c in ["trade_date","date","datetime"] if c in df.columns)
    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date","symbol","close"])
    df = df.sort_values(["symbol","trade_date"])
    return df


def build_features(df):
    g = df.groupby("symbol")

    df["ret"] = g["close"].pct_change().shift(-1)
    df["mom3"] = g["close"].pct_change(3)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)

    df["vol"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)

    market = df.groupby("trade_date")["close"].mean().reset_index()
    market["ma5"] = market["close"].rolling(5).mean()
    market["ma10"] = market["close"].rolling(10).mean()

    def reg(r):
        if r["ma5"] > r["ma10"]:
            return "strong"
        return "weak"

    market["price_regime"] = market.apply(reg, axis=1)

    return df.merge(market[["trade_date","price_regime"]], on="trade_date")


def load_macro(df):
    macro = pd.read_csv(MACRO_FILE)
    macro["trade_date"] = pd.to_datetime(macro["trade_date"])

    def mreg(x):
        if x > 0.5:
            return "strong"
        elif x > -0.2:
            return "normal"
        return "weak"

    macro["macro_regime"] = macro["macro_score"].apply(mreg)
    return df.merge(macro, on="trade_date", how="left")


# ===== v233 A 引擎升級 =====

def engine_A(d):
    d = d.dropna(subset=["mom3","mom5","mom10","ret","vol"])

    # 🔥 動能一致性 + 過熱過濾 + 穩定性
    d = d[
        (d["mom3"] > 0.01) &
        (d["mom5"] > 0.015) &
        (d["mom10"] > 0.0) &
        (d["mom3"] < 0.25) &
        (d["vol"] < 0.08)
    ]

    if len(d) == 0:
        return 0,0

    d["score"] = (
        d["mom3"]*0.4 +
        d["mom5"]*0.3 +
        d["mom10"]*0.3
    )

    d = d.sort_values("score", ascending=False).head(5)

    return d["ret"].clip(-0.08).mean(), len(d)


def engine_B(d):
    d = d.dropna(subset=["mom5","mom10","ret"])

    if len(d) == 0:
        return 0,0

    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    d = d.sort_values("score", ascending=False).head(10)

    return d["ret"].clip(-0.05).mean(), len(d)


# ===== v232 結構（保留） =====

def get_structure(macro_regime):
    if macro_regime == "strong":
        return 0.60, 0.45
    elif macro_regime == "normal":
        return 0.40, 0.50
    else:
        return 0.12, 0.45


def get_exposure(price_regime):
    if price_regime == "strong":
        return 1.15
    else:
        return 0.80


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    nav_hist = []
    rows = []

    prev_wA, prev_wB = 0,0

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"] == date]

        pr = day["price_regime"].iloc[0]
        mr = day["macro_regime"].iloc[0]

        baseA, baseB = get_structure(mr)
        exposure = get_exposure(pr)

        wA = baseA * exposure
        wB = baseB * exposure

        # NAV
        if len(nav_hist) > 10:
            if np.mean(nav_hist[-5:]) < np.mean(nav_hist[-10:]) * 0.997:
                wA *= 0.85

        # DD
        dd = nav / peak - 1

        if dd < -0.07:
            wA *= 0.85
            wB *= 0.9
        if dd < -0.10:
            wA *= 0.6
            wB *= 0.75

        # 平滑
        wA = prev_wA*0.6 + wA*0.4
        wB = prev_wB*0.6 + wB*0.4

        rA,nA = engine_A(day)
        rB,nB = engine_B(day)

        if pd.isna(rA): rA=0
        if pd.isna(rB): rB=0

        ret = rA*wA + rB*wB
        nav *= (1+ret)
        peak = max(peak, nav)

        rows.append([
            date, nav, wA, wB, nA, nB, ret, nav/peak-1
        ])

        nav_hist.append(nav)
        prev_wA, prev_wB = wA, wB

    return pd.DataFrame(rows, columns=[
        "date","nav","wA","wB","A_count","B_count","ret","dd"
    ])


def main():
    df = load_data()
    df = build_features(df)
    df = load_macro(df)

    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1]/INITIAL_CAPITAL - 1,
        "mdd": out["dd"].min(),
        "avg_wA": out["wA"].mean(),
        "avg_wB": out["wB"].mean()
    }])

    out.to_csv("v233_nav.csv", index=False)
    summary.to_csv("v233_summary.csv", index=False)

    print(summary)


if __name__ == "__main__":
    main()
