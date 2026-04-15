import pandas as pd
import numpy as np

FILE = "price_panel_daily.csv"
CAPITAL = 100000.0

df = pd.read_csv(FILE)
df.columns = [str(c).replace("\ufeff","").strip().lower() for c in df.columns]

date_candidates = ["trade_date","date","datetime","signal_date"]
found = None
for c in date_candidates:
    if c in df.columns:
        found = c
        break

if found is None:
    raise Exception("缺少日期欄位")

df["trade_date"] = pd.to_datetime(df[found])
df = df.sort_values(["symbol","trade_date"])

g = df.groupby("symbol",group_keys=False)

df["ret"] = g["close"].pct_change().shift(-1)
df["mom3"] = g["close"].pct_change(3)
df["mom5"] = g["close"].pct_change(5)
df["mom10"] = g["close"].pct_change(10)

market = df.groupby("trade_date")["close"].mean().reset_index()
market["ma5"] = market["close"].rolling(5).mean()
market["ma10"] = market["close"].rolling(10).mean()
market["ma20"] = market["close"].rolling(20).mean()

def regime(r):
    if r["ma5"]>r["ma10"]>r["ma20"]: return "strong"
    elif r["ma10"]>r["ma20"]: return "normal"
    return "weak"

market["regime"] = market.apply(regime,axis=1)
df = df.merge(market[["trade_date","regime"]],on="trade_date")

def A(d):
    d=d[(d["mom3"]>0.02)&(d["mom5"]>0.03)]
    if len(d)==0:return 0
    return d.sort_values("mom3",ascending=False).head(3)["ret"].clip(-0.08).mean()

def B(d):
    d=d[(d["mom5"]>0)&(d["mom10"]>0)]
    if len(d)==0:return 0
    return d.sort_values("mom10",ascending=False).head(8)["ret"].clip(-0.05).mean()

nav=CAPITAL
peak=CAPITAL
nav_hist=[]
rows=[]

for d in sorted(df["trade_date"].unique()):
    day=df[df["trade_date"]==d]
    reg=day["regime"].iloc[0]

    if reg=="strong": wA,wB=0.8,0.6
    elif reg=="normal": wA,wB=0.35,0.55
    else: wA,wB=0.0,0.35

    nav_weak=False
    if len(nav_hist)>=10:
        if np.mean(nav_hist[-5:])<np.mean(nav_hist[-10:]):
            nav_weak=True
            wA*=0.5

    dd=nav/peak-1
    if dd<=-0.08:
        wA*=0.5
        wB*=0.7

    rA=A(day) if wA>0 else 0
    rB=B(day) if wB>0 else 0

    ret=rA*wA+rB*wB
    nav*=1+ret
    peak=max(peak,nav)

    rows.append([d,nav,wA,wB,ret,nav/peak-1,nav_weak])
    nav_hist.append(nav)

out=pd.DataFrame(rows,columns=["date","nav","wA","wB","ret","dd","nav_weak"])

summary=pd.DataFrame([{
    "return":nav/CAPITAL-1,
    "mdd":out["dd"].min(),
    "avg_wA":out["wA"].mean(),
    "avg_wB":out["wB"].mean(),
    "nav_weak_days":out["nav_weak"].sum()
}])

out.to_csv("v229_1_nav.csv",index=False)
summary.to_csv("v229_1_summary.csv",index=False)

print("DONE v229.1")
