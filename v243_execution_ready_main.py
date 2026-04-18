# v243_execution_ready_main.py
import pandas as pd
from pathlib import Path
import json

def merge_parts():
    parts_dir = Path("data/price_panel_parts")
    manifest = parts_dir / "manifest.json"
    if not manifest.exists():
        raise Exception("缺少 manifest.json")

    meta = json.loads(manifest.read_text(encoding="utf-8"))
    dfs = []
    for p in meta["parts"]:
        dfs.append(pd.read_csv(p["file"]))
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv("price_panel_daily.csv", index=False)
    print("merged price_panel_daily.csv")

def main():
    merge_parts()
    print("v243 execution ready")

if __name__ == "__main__":
    main()
