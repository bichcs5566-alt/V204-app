from pathlib import Path
from datetime import datetime, timezone, timedelta
import json

DATA_DIR = Path("mobile_dashboard_v1/data")
META_PATH = DATA_DIR / "meta.json"

def utc_to_taipei_str():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    meta = {}
    if META_PATH.exists():
        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

    meta["generated_at"] = utc_to_taipei_str()
    meta.setdefault("signal_date", "")
    meta.setdefault("trade_date", "")
    meta.setdefault("data_state", "fresh")
    meta["source"] = "v266.7.3_supplement"

    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print("meta.json updated to Asia/Taipei")

if __name__ == "__main__":
    main()
