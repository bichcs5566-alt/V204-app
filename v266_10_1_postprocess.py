# v266.10.1 postprocess
import json, datetime

meta_path = "mobile_dashboard_v1/data/meta.json"

now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)

meta = {
    "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    "signal_date": now.strftime("%Y-%m-%d"),
    "trade_date": (now + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
    "trade_plan_batch": now.strftime("%Y-%m-%d %H:%M:%S"),
    "data_state": "ok"
}

with open(meta_path,"w",encoding="utf-8") as f:
    json.dump(meta,f,ensure_ascii=False,indent=2)
