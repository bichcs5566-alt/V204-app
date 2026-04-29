"""
pre_move_dashboard_patch.py

用途：
把 pre_move_candidates.csv 整理成手機 UI 容易讀的小檔。
如果你的 app.js 已經會讀 pre_move_candidates.csv，可不跑這支也沒關係。

輸入：
- mobile_dashboard_v1/data/pre_move_candidates.csv
或
- pre_move_candidates.csv

輸出：
- mobile_dashboard_v1/data/pre_move_cards.json
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SRC_CANDIDATES = [
    DATA_DIR / "pre_move_candidates.csv",
    ROOT / "pre_move_candidates.csv",
]


def find_src():
    for p in SRC_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def main():
    src = find_src()
    cards = []

    if src is not None:
        df = pd.read_csv(src)
        df.columns = [str(c).strip() for c in df.columns]

        for _, r in df.head(30).iterrows():
            action = str(r.get("action", "WATCH"))
            if action == "BUY":
                label = "小倉試單"
                color = "green"
            elif action == "TEST":
                label = "佈局試單"
                color = "yellow"
            else:
                label = "主力觀察"
                color = "gray"

            cards.append({
                "stock_id": str(r.get("stock_id", "")),
                "action": action,
                "label": label,
                "score": float(r.get("pre_score", 0)),
                "close": float(r.get("close", 0)),
                "price_tier": str(r.get("price_tier", "")),
                "target_weight": float(r.get("target_weight", 0)),
                "suggested_amount": int(float(r.get("suggested_amount", 0))),
                "setup_type": str(r.get("setup_type", "")),
                "signal_tags": str(r.get("signal_tags", "")),
                "color": color,
            })

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(cards),
        "cards": cards,
    }

    (DATA_DIR / "pre_move_cards.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
