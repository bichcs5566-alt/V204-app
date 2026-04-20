import os
import csv
import subprocess
import time
from pathlib import Path

ROOT_POS = Path("current_positions.csv")
DASH_POS = Path("mobile_dashboard_v1/data/current_positions.csv")
MAIN_PIPELINE = "v266_8_2_complete_fix.yml"

def read_positions(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        out.append({
            "stock_id": str(r.get("stock_id", "")).strip(),
            "shares": str(r.get("shares", "")).strip(),
            "avg_cost": str(r.get("avg_cost", "")).strip(),
            "last_action_date": str(r.get("last_action_date", "")).strip(),
            "note": str(r.get("note", "")).strip(),
        })
    return out

def write_positions(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["stock_id", "shares", "avg_cost", "last_action_date", "note"]
        )
        writer.writeheader()
        writer.writerows(rows)

def run_cmd(cmd, check=True):
    print("RUN:", " ".join(cmd))
    return subprocess.run(cmd, check=check)

def git_sync_and_push(max_retry=3, sleep_sec=2):
    for attempt in range(1, max_retry + 1):
        print(f"git push attempt {attempt}/{max_retry}")
        pull_res = subprocess.run(["git", "pull", "--rebase", "origin", "main"])
        if pull_res.returncode != 0:
            print("git pull --rebase å¤±æ")
            if attempt == max_retry:
                raise RuntimeError("git pull --rebase failed")
            time.sleep(sleep_sec)
            continue

        push_res = subprocess.run(["git", "push", "origin", "main"])
        if push_res.returncode == 0:
            print("git push æå")
            return

        print("git push å¤±æï¼æºåéè©¦")
        if attempt == max_retry:
            raise RuntimeError("git push failed after retries")
        time.sleep(sleep_sec)

    raise RuntimeError("git push failed after retries")

def dispatch_main_pipeline(token: str, repo: str):
    import urllib.request
    import json

    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/workflows/{MAIN_PIPELINE}/dispatches",
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print("dispatch main pipeline status", resp.status)

def main():
    action_type = os.environ.get("ACTION_TYPE", "").strip().lower()
    stock_id = os.environ.get("STOCK_ID", "").strip()
    shares = os.environ.get("SHARES", "").strip()
    avg_cost = os.environ.get("AVG_COST", "").strip()

    if not action_type or not stock_id:
        raise ValueError("ACTION_TYPE æ STOCK_ID ç¼ºå¤±")

    rows = read_positions(ROOT_POS if ROOT_POS.exists() else DASH_POS)

    if action_type == "upsert":
        if not shares or not avg_cost:
            raise ValueError("upsert éè¦ SHARES è AVG_COST")
        found = False
        for r in rows:
            if r["stock_id"] == stock_id:
                r["shares"] = shares
                r["avg_cost"] = avg_cost
                r["note"] = "v266.9-E çåå¯«æ´æ°"
                found = True
                break
        if not found:
            rows.append({
                "stock_id": stock_id,
                "shares": shares,
                "avg_cost": avg_cost,
                "last_action_date": "",
                "note": "v266.9-E çåå¯«æ°å¢"
            })
    elif action_type == "delete":
        rows = [r for r in rows if r["stock_id"] != stock_id]
    else:
        raise ValueError("ä¸æ¯æ´ç ACTION_TYPE")

    rows = sorted(rows, key=lambda x: x["stock_id"])
    write_positions(rows, ROOT_POS)
    write_positions(rows, DASH_POS)

    run_cmd(["git", "config", "user.name", "github-actions[bot]"])
    run_cmd(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"])
    run_cmd(["git", "add", "current_positions.csv", "mobile_dashboard_v1/data/current_positions.csv"])

    diff_result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff_result.returncode != 0:
        run_cmd(["git", "commit", "-m", f"position writeback {action_type} {stock_id}"])
        git_sync_and_push(max_retry=3, sleep_sec=2)
    else:
        print("æ²ææªæ¡å·®ç°ï¼ä¸éè¦ commit / push")

    token = os.environ.get("GH_TOKEN", "").strip()
    repo = os.environ.get("GH_REPO", "").strip()
    if token and repo:
        dispatch_main_pipeline(token, repo)
    else:
        print("ç¼ºå° GH_TOKEN æ GH_REPOï¼ç¥éä¸» pipeline dispatch")

if __name__ == "__main__":
    main()
