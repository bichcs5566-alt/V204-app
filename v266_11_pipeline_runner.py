import os, shutil, subprocess, json, time, urllib.request
from pathlib import Path

OWNER_REPO = os.environ["GH_REPO"]
TOKEN = os.environ["GH_TOKEN"]
BASE_WORKFLOW = os.environ.get("BASE_WORKFLOW", "v266_8_2_complete_fix.yml")

ROOT_PRICE = Path("price_panel_daily.csv")
DASH_PRICE = Path("mobile_dashboard_v1/data/price_panel_daily.csv")

def api(method, url, body=None):
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")

def dispatch_base():
    api(
        "POST",
        f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/dispatches",
        {"ref": "main"},
    )

def latest_run(timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        txt = api(
            "GET",
            f"https://api.github.com/repos/{OWNER_REPO}/actions/workflows/{BASE_WORKFLOW}/runs?branch=main&event=workflow_dispatch&per_page=10",
        )
        runs = json.loads(txt).get("workflow_runs", [])
        if runs:
            return runs[0]["id"]
        time.sleep(5)
    raise RuntimeError("找不到 base workflow run")

def wait_run(run_id, timeout=3600):
    start = time.time()
    while time.time() - start < timeout:
        txt = api("GET", f"https://api.github.com/repos/{OWNER_REPO}/actions/runs/{run_id}")
        data = json.loads(txt)
        status = data.get("status")
        conclusion = data.get("conclusion")
        print("base workflow:", status, conclusion)
        if status == "completed":
            if conclusion == "success":
                return
            raise RuntimeError(f"base workflow failed: {conclusion}")
        time.sleep(10)
    raise RuntimeError("base workflow timeout")

def ensure_price_panel():
    subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=True)
    if not ROOT_PRICE.exists():
        raise FileNotFoundError("base workflow 未產出 root/price_panel_daily.csv")
    DASH_PRICE.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT_PRICE, DASH_PRICE)
    print(f"copied {ROOT_PRICE} -> {DASH_PRICE}")

def main():
    dispatch_base()
    run_id = latest_run()
    wait_run(run_id)
    ensure_price_panel()
    subprocess.run(["python", "v266_11_postprocess.py"], check=True)

if __name__ == "__main__":
    main()
