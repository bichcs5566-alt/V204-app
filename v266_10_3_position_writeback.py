# v266.10.3 position writeback
import os, csv, subprocess, time, json, urllib.request
from pathlib import Path
ROOT_POS=Path("current_positions.csv")
DASH_POS=Path("mobile_dashboard_v1/data/current_positions.csv")
MAIN_PIPELINE="v266_10_3_pipeline.yml"
def read_rows(path):
    if not path.exists(): return []
    with path.open("r",encoding="utf-8-sig",newline="") as f: return list(csv.DictReader(f))
def write_rows(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=["stock_id","shares","avg_cost","last_action_date","note"]); w.writeheader(); w.writerows(rows)
def push_retry(max_retry=5,sleep_sec=3):
    for _ in range(max_retry):
        subprocess.run(["git","pull","--rebase","origin","main"],check=True)
        res=subprocess.run(["git","push","origin","main"])
        if res.returncode==0: return
        time.sleep(sleep_sec)
    raise RuntimeError("git push failed after retries")
def dispatch_pipeline(token,repo):
    req=urllib.request.Request(f"https://api.github.com/repos/{repo}/actions/workflows/{MAIN_PIPELINE}/dispatches",data=json.dumps({"ref":"main"}).encode("utf-8"),headers={"Accept":"application/vnd.github+json","Authorization":f"Bearer {token}","X-GitHub-Api-Version":"2022-11-28","Content-Type":"application/json"},method="POST")
    with urllib.request.urlopen(req) as resp: print(resp.status)
def main():
    action_type=os.environ.get("ACTION_TYPE","").strip().lower(); stock_id=os.environ.get("STOCK_ID","").strip(); shares=os.environ.get("SHARES","").strip(); avg_cost=os.environ.get("AVG_COST","").strip()
    if not action_type or not stock_id: raise ValueError("ACTION_TYPE æ STOCK_ID ç¼ºå¤±")
    rows=read_rows(ROOT_POS if ROOT_POS.exists() else DASH_POS)
    out=[]; found=False
    for r in rows:
        sid=str(r.get("stock_id","")).strip()
        if sid==stock_id:
            found=True
            if action_type=="delete": continue
            r["shares"]=shares; r["avg_cost"]=avg_cost; r["note"]="v266.10.3 çåå¯«æ´æ°"
        out.append({"stock_id":sid,"shares":str(r.get("shares","")).strip(),"avg_cost":str(r.get("avg_cost","")).strip(),"last_action_date":str(r.get("last_action_date","")).strip(),"note":str(r.get("note","")).strip()})
    if action_type=="upsert" and not found:
        out.append({"stock_id":stock_id,"shares":shares,"avg_cost":avg_cost,"last_action_date":"","note":"v266.10.3 çåå¯«æ°å¢"})
    out=sorted(out,key=lambda x:x["stock_id"])
    write_rows(ROOT_POS,out); write_rows(DASH_POS,out)
    subprocess.run(["git","config","user.name","github-actions[bot]"],check=True)
    subprocess.run(["git","config","user.email","41898282+github-actions[bot]@users.noreply.github.com"],check=True)
    subprocess.run(["git","add",str(ROOT_POS),str(DASH_POS)],check=True)
    diff=subprocess.run(["git","diff","--cached","--quiet"])
    if diff.returncode!=0:
        subprocess.run(["git","commit","-m",f"v266.10.3 position {action_type} {stock_id}"],check=True); push_retry()
    token=os.environ.get("GH_TOKEN","").strip(); repo=os.environ.get("GH_REPO","").strip()
    if token and repo: dispatch_pipeline(token,repo)
if __name__=="__main__": main()
