"""
backfill_missing_days.py

用途：
merge_price_panel_parts_runtime.py 先產生 price_panel_daily.csv，
本檔只補最後日期之後缺口，不重抓 10 年。
"""
from pathlib import Path
from datetime import datetime
import time, json, requests
import pandas as pd

ROOT=Path(".")
DATA_DIR=ROOT/"mobile_dashboard_v1"/"data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SLEEP_SECONDS=0.35
HEADERS={"User-Agent":"Mozilla/5.0","Accept":"application/json,text/plain,*/*","Referer":"https://www.twse.com.tw/"}

def clean_text(x):
    return str(x).strip().replace(",","").replace("--","").replace("—","").replace("X","")

def to_number(x):
    s=clean_text(x)
    if s=="": return None
    try: return float(s)
    except Exception: return None

def to_int(x):
    s=clean_text(x)
    if s=="": return None
    try: return int(float(s))
    except Exception: return None

def normalize_stock_id(x):
    s=clean_text(x)
    if s.endswith(".0"): s=s[:-2]
    if s.isdigit() and len(s)<=4: return s.zfill(4)
    return s

def is_common_stock_id(x):
    s=normalize_stock_id(x)
    return s.isdigit() and len(s)==4 and not s.startswith(("00","03","04","05","06","07","08","09"))

def candidate_tables(payload):
    out=[]
    for t in payload.get("tables",[]):
        fields=[str(c).strip() for c in t.get("fields",[])]
        data=t.get("data",[])
        if fields and data: out.append((fields,data,"tables"))
    for key in ["data9","data8","data7","data6","data5","data4","data3","data2","data1"]:
        data=payload.get(key); fields=payload.get(f"fields{key[-1]}")
        if isinstance(data,list) and data and isinstance(fields,list) and fields:
            out.append(([str(c).strip() for c in fields],data,key))
    return out

def field_index(fields, keywords):
    for i,f in enumerate(fields):
        for kw in keywords:
            if kw in f: return i
    return None

def parse_rows(payload, date_str, market):
    for fields, rows, source_key in candidate_tables(payload):
        joined=" | ".join(fields)
        if not ("收盤" in joined and ("證券代號" in joined or "股票代號" in joined or "代號" in joined)):
            continue
        idx_stock=field_index(fields,["證券代號","股票代號","代號"])
        idx_name=field_index(fields,["證券名稱","股票名稱","名稱"])
        idx_close=field_index(fields,["收盤價","收盤"])
        idx_volume=field_index(fields,["成交股數","成交量"])
        idx_open=field_index(fields,["開盤價","開盤"])
        idx_high=field_index(fields,["最高價","最高"])
        idx_low=field_index(fields,["最低價","最低"])
        if idx_stock is None or idx_close is None: continue

        records=[]
        for r in rows:
            try:
                stock_id=normalize_stock_id(r[idx_stock])
                close=to_number(r[idx_close])
                if not is_common_stock_id(stock_id) or close is None or close<=0: continue
                open_p=to_number(r[idx_open]) if idx_open is not None and idx_open<len(r) else close
                high_p=to_number(r[idx_high]) if idx_high is not None and idx_high<len(r) else close
                low_p=to_number(r[idx_low]) if idx_low is not None and idx_low<len(r) else close
                volume=to_int(r[idx_volume]) if idx_volume is not None and idx_volume<len(r) else 0
                records.append({"date":date_str,"stock_id":stock_id,"name":clean_text(r[idx_name]) if idx_name is not None and idx_name<len(r) else "","market":market,"open":open_p or close,"high":high_p or close,"low":low_p or close,"close":close,"volume":volume or 0})
            except Exception:
                continue
        if records:
            print(f"{date_str} {market} parsed {source_key} rows={len(records)}")
            return pd.DataFrame(records)
    return pd.DataFrame(columns=["date","stock_id","name","market","open","high","low","close","volume"])

def fetch_twse_day(dt):
    url="https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params={"response":"json","date":dt.strftime("%Y%m%d"),"type":"ALL"}
    r=requests.get(url,params=params,headers=HEADERS,timeout=30); r.raise_for_status()
    return parse_rows(r.json(), dt.strftime("%Y-%m-%d"), "TWSE")

def fetch_tpex_day(dt):
    date_str=dt.strftime("%Y-%m-%d")
    roc_year=dt.year-1911
    roc_date=f"{roc_year}/{dt.month:02d}/{dt.day:02d}"
    endpoints=[
        ("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",{"l":"zh-tw","d":roc_date,"s":"0,asc,0"}),
        ("https://www.tpex.org.tw/www/zh-tw/afterTrading/otc",{"date":dt.strftime("%Y/%m/%d"),"type":"EW","response":"json"}),
    ]
    for url,params in endpoints:
        try:
            r=requests.get(url,params=params,headers=HEADERS,timeout=30); r.raise_for_status()
            parsed=parse_rows(r.json(),date_str,"TPEX")
            if not parsed.empty: return parsed
        except Exception as e:
            print("TPEX failed:", date_str, str(e)[:120])
    return pd.DataFrame(columns=["date","stock_id","name","market","open","high","low","close","volume"])

def finalize(df):
    out=df.copy()
    out.columns=[str(c).strip().lower() for c in out.columns]
    if "date" not in out.columns and "trade_date" in out.columns: out["date"]=out["trade_date"]
    if "stock_id" not in out.columns and "symbol" in out.columns: out["stock_id"]=out["symbol"]
    for c in ["name","market"]:
        if c not in out.columns: out[c]=""
    for c in ["open","high","low","close","volume"]:
        out[c]=pd.to_numeric(out[c], errors="coerce")
    out["date"]=pd.to_datetime(out["date"], errors="coerce")
    out["stock_id"]=out["stock_id"].apply(normalize_stock_id)
    out=out.dropna(subset=["date","stock_id","close"])
    out=out[out["stock_id"].apply(is_common_stock_id)].copy()
    out=out[out["close"]>0].copy()
    for c in ["open","high","low"]: out[c]=out[c].fillna(out["close"])
    out["volume"]=out["volume"].fillna(0)
    out=out.drop_duplicates(["date","stock_id"],keep="last")
    out=out.sort_values(["stock_id","date"]).reset_index(drop=True)
    out["date"]=out["date"].dt.strftime("%Y-%m-%d")
    return out[["date","stock_id","name","market","open","high","low","close","volume"]]

def main():
    p=ROOT/"price_panel_daily.csv"
    if not p.exists() or p.stat().st_size==0:
        raise FileNotFoundError("price_panel_daily.csv missing. Run merge_price_panel_parts_runtime.py first.")
    old=finalize(pd.read_csv(p))
    last_date=pd.to_datetime(old["date"]).max()
    today=datetime.now()
    start=last_date+pd.Timedelta(days=1)
    parts=[old]; filled=[]; skipped=[]
    cur=start
    while cur<=today:
        if cur.weekday()>=5:
            skipped.append({"date":cur.strftime("%Y-%m-%d"),"reason":"weekend"})
            cur+=pd.Timedelta(days=1); continue
        print("fetch missing day:", cur.strftime("%Y-%m-%d"))
        day_parts=[]
        try:
            x=fetch_twse_day(cur)
            if not x.empty: day_parts.append(x)
        except Exception as e: print("TWSE failed:", cur.strftime("%Y-%m-%d"), str(e)[:160])
        try:
            x=fetch_tpex_day(cur)
            if not x.empty: day_parts.append(x)
        except Exception as e: print("TPEX hard failed:", cur.strftime("%Y-%m-%d"), str(e)[:160])
        if day_parts:
            ddf=pd.concat(day_parts,ignore_index=True)
            parts.append(ddf)
            filled.append({"date":cur.strftime("%Y-%m-%d"),"rows":int(len(ddf)),"symbols":int(ddf["stock_id"].nunique())})
        else:
            skipped.append({"date":cur.strftime("%Y-%m-%d"),"reason":"no data returned"})
        time.sleep(SLEEP_SECONDS)
        cur+=pd.Timedelta(days=1)
    merged=finalize(pd.concat(parts,ignore_index=True))
    merged.to_csv(ROOT/"price_panel_daily.csv",index=False,encoding="utf-8")
    merged.to_csv(DATA_DIR/"price_panel_daily.csv",index=False,encoding="utf-8")
    report={"generated_at":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"source":"backfill_missing_days","before_last_date":str(last_date.date()),"after_last_date":str(pd.to_datetime(merged["date"]).max().date()),"rows":int(len(merged)),"stock_count":int(merged["stock_id"].nunique()),"unique_dates":int(merged["date"].nunique()),"filled_days":filled,"skipped_days":skipped}
    for out in [ROOT/"backfill_report.json",DATA_DIR/"backfill_report.json"]:
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=="__main__":
    main()
