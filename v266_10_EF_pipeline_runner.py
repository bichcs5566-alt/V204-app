import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

OWNER_REPO = os.environ["GH_REPO"]
TOKEN = os.environ["GH_TOKEN"]
BASE_WORKFLOW = "v266_8_2_complete_fix.yml"

def api(method, url, body=None):
    data = None
    headers = {
        "Accept":"application/vnd.github+json",
        "Authorization":f"Bearer {TOKEN}",
        "X-GitHub-Api-Version":"2022-11-28",
        "Content-Type":"application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        txt = resp.read().decode("utf-8")
        return txt

def dispatch_base():
    api("POST", f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/dispatches", {"ref":"main"})

def find_latest_run(start_ts):
    txt = api("GET", f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/runs?branch=main&event=workflow_dispatch&per_page=5")
    data = json.loads(txt)
    runs = data.get("workflow_runs", [])
    for r in runs:
        created = datetime.fromisoformat(r["created_at"].replace("Z","+00:00")).timestamp()
        if created >= start_ts - 30:
            return r["id"]
    return None

def wait_run(run_id, timeout=900):
    start = time.time()
    while time.time() - start < timeout:
        txt = api("GET", f"https://api.github.com/repos/{OWNER_REPO}/actions/runs/{run_id}")
        data = json.loads(txt)
        status = data.get("status")
        conclusion = data.get("conclusion")
        print("child workflow:", status, conclusion)
        if status == "completed":
            if conclusion == "success":
                return True
            raise RuntimeError(f"base workflow failed: {conclusion}")
        time.sleep(10)
    raise RuntimeError("base workflow timeout")

def main():
    start_ts = time.time()
    dispatch_base()
    run_id = None
    for _ in range(18):
        run_id = find_latest_run(start_ts)
        if run_id:
            break
        time.sleep(5)
    if not run_id:
        raise RuntimeError("æ¾ä¸å°å dispatch ç base workflow run")
    wait_run(run_id)
    subprocess.run(["git","pull","--rebase","origin","main"], check=True)
    subprocess.run(["python","v266_10_EF_postprocess.py"], check=True)

if __name__ == "__main__":
    main()
