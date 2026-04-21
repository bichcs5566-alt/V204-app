# v266.10.3 postprocess
import csv, json, subprocess
from datetime import datetime, timedelta, date
from pathlib import Path
DATA_DIR=Path("mobile_dashboard_v1/data")
META_PATH=DATA_DIR/"meta.json"
CURRENT_POS=DATA_DIR/"current_positions.csv"
WATCHLIST=DATA_DIR/"watchlist.csv"
TRADE_PLAN=DATA_DIR/"trade_plan.csv"
POS_MON=DATA_DIR/"position_monitor.csv"
WATCH_MON=DATA_DIR/"watchlist_monitor.csv"
POS_MERGED=DATA_DIR/"position_monitor_merged.csv"
WATCH_MERGED=DATA_DIR/"watchlist_monitor_merged.csv"
PRICE_PANEL=DATA_DIR/"price_panel_daily.csv"
def read_csv(path):
    if not path.exists(): return []
    with path.open("r",encoding="utf-8-sig",newline="") as f: return list(csv.DictReader(f))
def write_csv(path,rows,headers):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=headers); w.writeheader()
        for row in rows: w.writerow({h:row.get(h,"") for h in headers})
def taipei_now(): return datetime.utcnow()+timedelta(hours=8)
def prev_business_day(d):
    x=d-timedelta(days=1)
    while x.weekday()>=5: x-=timedelta(days=1)
    return x
def next_business_day(d):
    x=d+timedelta(days=1)
    while x.weekday()>=5: x+=timedelta(days=1)
    return x
def expected_latest_signal_date(now_tp):
    today=now_tp.date()
    if today.weekday()>=5: return prev_business_day(today)
    if now_tp.hour<19: return prev_business_day(today)
    return today
def parse_price_tier(ref):
    try: x=float(ref)
    except: return "unknown"
    if x<50:return"lt_50"
    if x<100:return"p50_100"
    if x<300:return"p100_300"
    if x<500:return"p300_500"
    if x<1000:return"p500_1000"
    return"gt_1000"
def latest_price_map():
    rows=read_csv(PRICE_PANEL); latest={}; latest_date=None
    for r in rows:
        sid=str(r.get("stock_id") or r.get("code") or "").strip(); ds=str(r.get("date") or "").strip(); close=str(r.get("close") or r.get("adj_close") or r.get("æ¶ç¤å¹") or "").strip()
        if not sid or not ds: continue
        try: d=datetime.strptime(ds,"%Y-%m-%d").date()
        except: continue
        if latest_date is None or d>latest_date: latest_date=d
        old=latest.get(sid)
        if old is None or d>=old["date"]: latest[sid] = {"date":d,"close":close}
    return latest, latest_date
def build_merged():
    current_positions=read_csv(CURRENT_POS); watchlist=read_csv(WATCHLIST); trade_plan=read_csv(TRADE_PLAN); pos_mon=read_csv(POS_MON); watch_mon=read_csv(WATCH_MON); price_map, latest_date = latest_price_map()
    trade_map={str(r.get("stock_id"," ")).strip():r for r in trade_plan if str(r.get("stock_id"," ")).strip()}
    pos_mon_map={str(r.get("stock_id"," ")).strip():r for r in pos_mon if str(r.get("stock_id"," ")).strip()}
    watch_mon_map={str(r.get("stock_id"," ")).strip():r for r in watch_mon if str(r.get("stock_id"," ")).strip()}
    pos_rows=[]
    for r in current_positions:
        sid=str(r.get("stock_id","")).strip(); pr=pos_mon_map.get(sid,{}); tr=trade_map.get(sid,{}); px=price_map.get(sid,{})
        ref=pr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        pos_rows.append({"stock_id":sid,"price_tier":pr.get("price_tier") or tr.get("price_tier") or parse_price_tier(ref),"ref_price":ref,"shares":str(r.get("shares"," ")).strip(),"avg_cost":str(r.get("avg_cost"," ")).strip(),"pnl_pct":pr.get("pnl_pct",""),"target_weight":pr.get("target_weight") or tr.get("target_weight",""),"current_weight_est":pr.get("current_weight_est",""),"action":pr.get("action") or tr.get("action") or "HOLD","note":pr.get("note") or r.get("note") or ""})
    watch_rows=[]
    for r in watchlist:
        sid=str(r.get("stock_id","")).strip(); wr=watch_mon_map.get(sid,{}); tr=trade_map.get(sid,{}); px=price_map.get(sid,{})
        ref=wr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        watch_rows.append({"stock_id":sid,"price_tier":wr.get("price_tier") or tr.get("price_tier") or parse_price_tier(ref),"ref_price":ref,"holding_status":wr.get("holding_status","æªææ"),"strategy_bucket":wr.get("strategy_bucket") or tr.get("strategy_bucket") or "NONE","action":wr.get("action") or tr.get("action") or "WATCH","pnl_pct":wr.get("pnl_pct","")})
    return pos_rows, watch_rows, latest_date
def git_commit_push(files,message):
    subprocess.run(["git","config","user.name","github-actions[bot]"],check=True)
    subprocess.run(["git","config","user.email","41898282+github-actions[bot]@users.noreply.github.com"],check=True)
    subprocess.run(["git","add",*files],check=True)
    diff=subprocess.run(["git","diff","--cached","--quiet"])
    if diff.returncode!=0:
        subprocess.run(["git","commit","-m",message],check=True)
        subprocess.run(["git","pull","--rebase","origin","main"],check=True)
        subprocess.run(["git","push","origin","main"],check=True)
def main():
    DATA_DIR.mkdir(parents=True,exist_ok=True)
    pos_rows, watch_rows, latest_date = build_merged()
    write_csv(POS_MERGED,pos_rows,["stock_id","price_tier","ref_price","shares","avg_cost","pnl_pct","target_weight","current_weight_est","action","note"])
    write_csv(WATCH_MERGED,watch_rows,["stock_id","price_tier","ref_price","holding_status","strategy_bucket","action","pnl_pct"])
    now_tp=taipei_now(); expected=expected_latest_signal_date(now_tp); meta={}
    if META_PATH.exists():
        try: meta=json.loads(META_PATH.read_text(encoding="utf-8"))
        except: meta={}
    if latest_date:
        meta["signal_date"]=latest_date.isoformat(); meta["trade_date"]=next_business_day(latest_date).isoformat(); meta["price_panel_latest_date"]=latest_date.isoformat()
    meta["generated_at"]=now_tp.strftime("%Y-%m-%d %H:%M:%S")
    meta["trade_plan_batch"]=now_tp.strftime("%Y-%m-%d %H:%M:%S")
    meta["source"]="v266.10.3_pipeline"
    meta["data_state"]="ok" if latest_date and latest_date>=expected else "stale"
    META_PATH.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    git_commit_push([str(META_PATH),str(POS_MERGED),str(WATCH_MERGED)],"v266.10.3 postprocess meta and merged outputs")
if __name__=="__main__": main()
