import pandas as pd
import numpy as np

INPUT_FILE = "price_panel_daily.csv"
INITIAL_CAPITAL = 100000.0


def load_data():
    df = pd.read_csv(INPUT_FILE)
    df.columns = [str(c).strip().lower() for c in df.columns]

    date_col = None
    for c in ["trade_date","date","datetime","signal_date"]:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise Exception("缺少日期欄位")

    df["trade_date"] = pd.to_datetime(df[date_col])
    df = df.sort_values(["symbol","trade_date"])

    return df


def build_features(df):
    g = df.groupby("symbol")

    df["ret"] = g["close"].pct_change().shift(-1)
    df["mom3"] = g["close"].pct_change(3)
    df["mom5"] = g["close"].pct_change(5)
    df["mom10"] = g["close"].pct_change(10)
    df["mom20"] = g["close"].pct_change(20)

    market = df.groupby("trade_date")["close"].mean().reset_index()
    market["ma5"] = market["close"].rolling(5).mean()
    market["ma10"] = market["close"].rolling(10).mean()
    market["ma20"] = market["close"].rolling(20).mean()

    def reg(r):
        if r["ma5"]>r["ma10"]>r["ma20"]:
            return "strong"
        elif r["ma10"]>r["ma20"]:
            return "normal"
        return "weak"

    market["regime"] = market.apply(reg,axis=1)
    df = df.merge(market[["trade_date","regime"]],on="trade_date")

    return df


# 🔥 A 引擎（放寬）
def engine_A(d):
    d = d.dropna(subset=["mom3","mom5","ret"])
    d = d[(d["mom3"]>0.01)&(d["mom5"]>0.015)]

    if len(d)==0: return 0,0

    d["score"] = d["mom3"]*0.6 + d["mom5"]*0.4
    d = d.sort_values("score",ascending=False).head(5)

    return d["ret"].clip(-0.08).mean(), len(d)


# 🔥 B 引擎（強制參與）
def engine_B(d):
    d = d.dropna(subset=["mom5","mom10","ret"])

    if len(d)==0: return 0,0

    d["score"] = d["mom10"]*0.6 + d["mom5"]*0.4

    # 🔥 保證選股（核心）
    d = d.sort_values("score",ascending=False).head(10)

    return d["ret"].clip(-0.05).mean(), len(d)


def backtest(df):
    nav = INITIAL_CAPITAL
    peak = INITIAL_CAPITAL
    nav_hist=[]
    rows=[]

    for date in sorted(df["trade_date"].unique()):
        day = df[df["trade_date"]==date]
        reg = day["regime"].iloc[0]

        # 🔥 更積極權重
        if reg=="strong":
            wA,wB = 0.9,0.7
        elif reg=="normal":
            wA,wB = 0.5,0.6
        else:
            wA,wB = 0.2,0.5

        # NAV 控制（保留）
        if len(nav_hist)>=10:
            if np.mean(nav_hist[-5:]) < np.mean(nav_hist[-10:])*0.995:
                wA *= 0.7

        # DD 控制（保留）
        dd = nav/peak-1
        if dd<-0.08:
            wA *= 0.6
            wB *= 0.8

        rA,nA = engine_A(day) if wA>0 else (0,0)
        rB,nB = engine_B(day) if wB>0 else (0,0)

        if pd.isna(rA): rA=0
        if pd.isna(rB): rB=0

        ret = rA*wA + rB*wB
        nav *= (1+ret)
        peak = max(peak,nav)

        rows.append([date,nav,wA,wB,nA,nB,ret,nav/peak-1])
        nav_hist.append(nav)

    return pd.DataFrame(rows,columns=[
        "date","nav","wA","wB","A_count","B_count","ret","dd"
    ])


def main():
    df = load_data()
    df = build_features(df)
    out = backtest(df)

    summary = pd.DataFrame([{
        "return": out["nav"].iloc[-1]/INITIAL_CAPITAL-1,
        "mdd": out["dd"].min(),
        "avg_A": out["A_count"].mean(),
        "avg_B": out["B_count"].mean()
    }])

    out.to_csv("v230_2_nav.csv",index=False)
    summary.to_csv("v230_2_summary.csv",index=False)

    print(summary)


if __name__ == "__main__":
    main()
