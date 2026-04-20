import os
import csv
import subprocess
import time
from pathlib import Path

ROOT_WATCH = Path("watchlist.csv")
DASH_WATCH = Path("mobile_dashboard_v1/data/watchlist.csv")
MAIN_PIPELINE = "v266_10_EF_pipeline.yml"

def read_watch(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_watch(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["stock_id","note"])
        writer.writeheader()
        writer.writerows(rows)

def git_push_retry(max_retry=3, sleep_sec=2):
    for attempt in range(1, max_retry+1):
        subprocess.run(["git","pull","--rebase","origin","main"])
        res = subprocess.run(["git","push","origin","main"])
        if res.returncode == 0:
            return
        if attempt == max_retry:
            raise RuntimeError("git push failed after retries")
        time.sleep(sleep_sec)

def dispatch_pipeline(token, repo):
    import urllib.request, json
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/workflows/{MAIN_PIPELINE}/dispatches",
        data=json.dumps({"ref":"main"}).encode("utf-8"),
        headers={
            "Accept":"application/vnd.github+json",
            "Authorization":f"Bearer {token}",
            "X-GitHub-Api-Version":"2022-11-28",
            "Content-Type":"application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print("dispatch unified pipeline status", resp.status)

def main():
    action_type = os.environ.get("ACTION_TYPE","").strip().lower()
    stock_id = os.environ.get("STOCK_ID","").strip()
    if not action_type or not stock_id:
        raise ValueError("ACTION_TYPE æ STOCK_ID ç¼ºå¤±")

    rows = read_watch(ROOT_WATCH if ROOT_WATCH.exists() else DASH_WATCH)
    rows2 = []
    exists = False
    for r in rows:
        sid = str(r.get("stock_id","")).strip()
        if sid == stock_id:
            exists = True
            if action_type == "delete":
                continue
        rows2.append({"stock_id": sid, "note": str(r.get("note","")).strip()})
    if action_type == "upsert" and not exists:
        rows2.append({"stock_id": stock_id, "note": "v266.10-EF èªé¸è¡æ°å¢"})
    rows2 = sorted(rows2, key=lambda x: x["stock_id"])

    write_watch(rows2, ROOT_WATCH)
    write_watch(rows2, DASH_WATCH)

    subprocess.run(["git","config","user.name","github-actions[bot]"], check=True)
    subprocess.run(["git","config","user.email","41898282+github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git","add","watchlist.csv","mobile_dashboard_v1/data/watchlist.csv"], check=True)

    diff = subprocess.run(["git","diff","--cached","--quiet"])
    if diff.returncode != 0:
        subprocess.run(["git","commit","-m",f"watchlist writeback {action_type} {stock_id}"], check=True)
        git_push_retry()

    token = os.environ.get("GH_TOKEN","").strip()
    repo = os.environ.get("GH_REPO","").strip()
    if token and repo:
        dispatch_pipeline(token, repo)

if __name__ == "__main__":
    main()
