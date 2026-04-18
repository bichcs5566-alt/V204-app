# merge_chunked_price_panel.py
# 將 repo 內的 data/price_panel_parts/*.csv 合併成 price_panel_daily.csv
# 用於回測前的 workflow 步驟

import json
from pathlib import Path
import pandas as pd

PARTS_DIR = Path("data/price_panel_parts")
OUT_FILE = Path("price_panel_daily.csv")
MANIFEST = PARTS_DIR / "manifest.json"

def main():
    if not MANIFEST.exists():
        raise FileNotFoundError(f"找不到 manifest: {MANIFEST}")

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    frames = []
    for item in manifest["parts"]:
        p = Path(item["file"])
        if not p.exists():
            raise FileNotFoundError(f"缺少分片: {p}")
        frames.append(pd.read_csv(p))

    df = pd.concat(frames, ignore_index=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"merged -> {OUT_FILE} | rows={len(df)} | symbols={df['symbol'].nunique()}")

if __name__ == "__main__":
    main()
