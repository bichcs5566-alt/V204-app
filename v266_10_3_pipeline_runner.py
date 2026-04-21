# v266.10.3 pipeline runner
import json, os, subprocess, time, urllib.request
OWNER_REPO=os.environ["GH_REPO"]
TOKEN=os.environ["GH_TOKEN"]
BASE_WORKFLOW=os.environ.get("BASE_WORKFLOW","v266_8_2_complete_fix.yml")
def api(method,url,body=None):
    data=None
    headers={"Accept":"application/vnd.github+json","Authorization":f"Bearer {TOKEN}","X-GitHub-Api-Version":"2022-11-28","Content-Type":"application/json"}
    if body is not None: data=json.dumps(body).encode("utf-8")
    req=urllib.request.Request(url,data=data,headers=headers,method=method)
    with urllib.request.urlopen(req) as resp: return resp.read().decode("utf-8")
def dispatch_base(): api("POST",f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/dispatches",{"ref":"main"})
def find_latest_run(timeout=120):
    start=time.time()
    while time.time()-start<timeout:
        txt=api("GET",f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/runs?branch=main&event=workflow_dispatch&per_page=10")
        runs=json.loads(txt).get("workflow_runs",[])
        if runs: return runs[0]["id"]
        time.sleep(5)
    raise RuntimeError("æ¾ä¸å° base workflow run")
def wait_run(run_id,timeout=3600):
    start=time.time()
    while time.time()-start<timeout:
        txt=api("GET",f"https://api.github.com/repos/{OWNER_REPO}/actions/runs/{run_id}")
        data=json.loads(txt); status=data.get("status"); conclusion=data.get("conclusion")
        print("child workflow:",status,conclusion)
        if status=="completed":
            if conclusion=="success": return
            raise RuntimeError(f"base workflow failed: {conclusion}")
        time.sleep(10)
    raise RuntimeError("base workflow timeout")
def main():
    dispatch_base(); run_id=find_latest_run(); wait_run(run_id)
    subprocess.run(["git","pull","--rebase","origin","main"],check=True)
    subprocess.run(["python","v266_10_3_postprocess.py"],check=True)
if __name__=="__main__": main()
