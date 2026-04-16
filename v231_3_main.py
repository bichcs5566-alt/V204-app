import os
import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
MACRO_FILE = "macro_signal.csv"
INITIAL_CAPITAL = 100000.0


def load_base_panel(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).replace("\ufeff", "").strip().lower() for c in df.columns]

    date_col = next((c for c in ["trade_date","date","datetime","signal_date"] if c in df.columns), None)
    if date_col is None:
        raise Exception("缺少日期欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["trade_date","symbol","close"])
    df = df.sort_values(["symbol","trade_date"]).reset_index(drop=True)
    return df


def build_features(df):
    g = df.groupby("symbol")

    df["ret_1d"] = g["close"].pct_change().shift(-1)
    df["mom3"] = g["close"].pct_change(3)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)
    df["vol5"] = g["close"].pct_change().rolling(5).std().reset_index(level=0, drop=True)

    market = df.groupby("trade_date")["close"].mean().reset_index()
    market["ma5"] = market["close"].rolling(5).mean()
    market["ma10"] = market["close"].rolling(10).mean()
    market["ma20"] = market["close"].rolling(20).mean()

    def reg(r):
        if r["ma5"]>r["ma10"]>r["ma20"]: return "strong"
        elif r["ma10"]>r["ma20"]: return "normal"
        return "weak"

    market["price_regime"] = market.apply(reg,axis=1)
    return df.merge(market[["trade_date","price_regime"]],on="trade_date")


def load_macro(df):
    macro = pd.read_csv(MACRO_FILE)
    macro["trade_date"] = pd.to_datetime(macro["trade_date"])

    def mreg(x):
        if x>0.5: return "strong"
        elif x>-0.2: return "normal"
        return "weak"

    macro["macro_regime"] = macro["macro_score"].apply(mreg)
    return df.merge(macro,on="trade_date",how="left")


# --- 引擎（不動核心，只微調） ---
def engine_A(d):
    d = d.dropna(subset=["mom3","mom5","ret_1d"])
    d = d[(d["mom3"]>0.01)&(d["mom5"]>0.015)]

    if len(d)==0: return 0,0

    d["score"] = d["mom3"]*0.6 + d["mom5"]*0.4
    d = d.sort_values("score",ascending=False).head(4)
    return d["ret_1d"].clip(-0.08).mean(), len(d)


def engine_B(d):
    d = d.dropna(subset=["mom5","mom10","ret_1d"])
    if len(d)==0: return 0,0

    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4
    d = d.sort_values("score",ascending=False).head(8)
    return d["ret_1d"].clip(-0.05).mean(), len(d)


# --- 核心：權重邏輯 ---
def get_weights(price_regime, macro_regime):
    # 🔥 base from price
    if price_regime=="strong":
        wA,wB = 0.6,0.6
    elif price_regime=="normal":
        wA,wB = 0.3,0.5
    else:
        wA,wB = 0.1,0.3

    # 🔥 macro override（關鍵）
    if macro_regime=="strong":
        wA *= 1.2
        wB *= 1.1
    elif macro_regime=="weak":
        wA *= 0.3   # 🔥 幾乎關 A
        wB *= 0.7   # 🔥 B 保底

    return wA,wB


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    nav_hist=[]
    rows=[]

    prev_wA, prev_wB = 0,0

    for d in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"]==d]

        pr = day["price_regime"].iloc[0]
        mr = day["macro_regime"].iloc[0]

        wA,wB = get_weights(pr,mr)

        # 🔥 NAV 控制
        if len(nav_hist)>10:
            if np.mean(nav_hist[-5:]) < np.mean(nav_hist[-10:])*0.997:
                wA *= 0.7

        # 🔥 DD 控制（兩段）
        dd = nav/peak-1
        if dd<-0.05:
            wA *= 0.8
            wB *= 0.9
        if dd<-0.08:
            wA *= 0.5
            wB *= 0.7

        # 🔥 平滑
        wA = prev_wA*0.6 + wA*0.4
        wB = prev_wB*0.6 + wB*0.4

        rA,nA = engine_A(day) if wA>0 else (0,0)
        rB,nB = engine_B(day) if wB>0 else (0,0)

        if pd.isna(rA): rA=0
        if pd.isna(rB): rB=0

        ret = rA*wA + rB*wB
        nav *= (1+ret)
        peak = max(peak,nav)

        rows.append([d,nav,wA,wB,nA,nB,ret,nav/peak-1,mr])
        nav_hist.append(nav)

        prev_wA, prev_wB = wA,wB

    return pd.DataFrame(rows,columns=[
        "date","nav","wA","wB","A_count","B_count","ret","dd","macro_regime"
    ])


def main():
    df = load_base_panel(INPUT_FILE)
    df = build_features(df)
    df = load_macro(df)

    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1]/INITIAL_CAPITAL-1,
        "mdd": out["dd"].min(),
        "avg_wA": out["wA"].mean(),
        "avg_wB": out["wB"].mean(),
        "macro_weak_days": (out["macro_regime"]=="weak").sum()
    }])

    out.to_csv("v231_3_nav.csv",index=False)
    summary.to_csv("v231_3_summary.csv",index=False)

    print(summary)


if __name__ == "__main__":
    main()
