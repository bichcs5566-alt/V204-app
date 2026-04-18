# commit_generated_price_panel.py
# 將 GitHub Actions 產生的完整資料自動 commit 回 repo
# 會檢查新檔是否存在、非空，並只 commit 有變更的檔案

import os
import subprocess
from pathlib import Path

TARGET_FILES = [
    "price_panel_daily.csv",
    "build_price_panel_summary.csv",
    "build_price_panel_log.txt",
    "symbol_universe.csv",
]

def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    print(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, check=check, text=True)

def file_ok(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0

def main():
    missing = [f for f in TARGET_FILES if not file_ok(Path(f))]
    if missing:
        raise FileNotFoundError(f"缺少要提交回 repo 的檔案: {missing}")

    actor = os.getenv("GITHUB_ACTOR", "github-actions[bot]")
    email = f"{actor}@users.noreply.github.com"

    run(f'git config user.name "{actor}"')
    run(f'git config user.email "{email}"')

    files_str = " ".join(TARGET_FILES)
    run(f"git add {files_str}")

    diff = subprocess.run("git diff --cached --quiet", shell=True)
    if diff.returncode == 0:
        print("沒有檔案變更，略過 commit / push。")
        return

    run('git commit -m "auto update full price panel 10y"')
    run("git push")
    print("已將新資料 commit 回 repo。")

if __name__ == "__main__":
    main()
