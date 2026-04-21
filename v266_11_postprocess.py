import csv, json, subprocess
from datetime import datetime, timedelta, date
from pathlib import Path

DATA_DIR = Path("mobile_dashboard_v1/data")
META_PATH = DATA_DIR / "meta.json"
PRICE_PANEL = DATA_DIR / "price_panel_daily.csv"
TRADE_PLAN = DATA_DIR / "trade_plan.csv"
CURRENT_POS = DATA_DIR / "current_positions.csv"
WATCHLIST = DATA_DIR / "watchlist.csv"
POS_MON = DATA_DIR / "position_monitor.csv"
WATCH_MON = DATA_DIR / "watchlist_monitor.csv"
POS_MERGED = DATA_DIR / "position_monitor_merged.csv"
WATCH_MERGED = DATA_DIR / "watchlist_monitor_merged.csv"

def read_csv(path):
    if not path.exists(): return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, headers):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({h: row.get(h, "") for h in headers})

def tp_now():
    return datetime.utcnow() + timedelta(hours=8)

def prev_bday(d: date):
    x = d - timedelta(days=1)
    while x.weekday() >= 5: x -= timedelta(days=1)
    return x

def next_bday(d: date):
    x = d + timedelta(days=1)
    while x.weekday() >= 5: x += timedelta(days=1)
    return x

def expected_signal_date(now_tp):
    today = now_tp.date()
    if today.weekday() >= 5:
        return prev_bday(today)
    if now_tp.hour < 19:
        return prev_bday(today)
    return today

def price_tier(ref):
    try: n = float(ref)
    except: return "unknown"
    if n < 50: return "lt_50"
    if n < 100: return "p50_100"
    if n < 300: return "p100_300"
    if n < 500: return "p300_500"
    if n < 1000: return "p500_1000"
    return "gt_1000"

def latest_price_map():
    rows = read_csv(PRICE_PANEL)
    out, latest = {}, None
    for r in rows:
        sid = str(r.get("stock_id") or r.get("code") or "").strip()
        ds = str(r.get("date") or "").strip()
        close = str(r.get("close") or r.get("adj_close") or r.get("收盤價") or "").strip()
        if not sid or not ds: continue
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except:
            continue
        if latest is None or d > latest: latest = d
        old = out.get(sid)
        if old is None or d >= old["date"]:
            out[sid] = {"date": d, "close": close}
    return out, latest

def git_commit_push(files, message):
    subprocess.run(["git","config","user.name","github-actions[bot]"], check=True)
    subprocess.run(["git","config","user.email","41898282+github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git","add", *files], check=True)
    diff = subprocess.run(["git","diff","--cached","--quiet"])
    if diff.returncode != 0:
        subprocess.run(["git","commit","-m", message], check=True)
        subprocess.run(["git","pull","--rebase","origin","main"], check=True)
        subprocess.run(["git","push","origin","main"], check=True)

def main():
    trade_map = {str(r.get("stock_id","")).strip(): r for r in read_csv(TRADE_PLAN) if str(r.get("stock_id","")).strip()}
    pos_map = {str(r.get("stock_id","")).strip(): r for r in read_csv(POS_MON) if str(r.get("stock_id","")).strip()}
    watch_map = {str(r.get("stock_id","")).strip(): r for r in read_csv(WATCH_MON) if str(r.get("stock_id","")).strip()}
    price_map, latest_date = latest_price_map()

    pos_rows = []
    for r in read_csv(CURRENT_POS):
        sid = str(r.get("stock_id","")).strip()
        pr = pos_map.get(sid, {})
        tr = trade_map.get(sid, {})
        px = price_map.get(sid, {})
        ref = pr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        pos_rows.append({
            "stock_id": sid,
            "price_tier": pr.get("price_tier") or tr.get("price_tier") or price_tier(ref),
            "ref_price": ref,
            "shares": str(r.get("shares","")).strip(),
            "avg_cost": str(r.get("avg_cost","")).strip(),
            "pnl_pct": pr.get("pnl_pct",""),
            "target_weight": pr.get("target_weight") or tr.get("target_weight",""),
            "current_weight_est": pr.get("current_weight_est",""),
            "action": pr.get("action") or tr.get("action") or "HOLD",
            "note": pr.get("note") or r.get("note","")
        })

    watch_rows = []
    for r in read_csv(WATCHLIST):
        sid = str(r.get("stock_id","")).strip()
        wr = watch_map.get(sid, {})
        tr = trade_map.get(sid, {})
        px = price_map.get(sid, {})
        ref = wr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        watch_rows.append({
            "stock_id": sid,
            "price_tier": wr.get("price_tier") or tr.get("price_tier") or price_tier(ref),
            "ref_price": ref,
            "holding_status": wr.get("holding_status","未持有"),
            "strategy_bucket": wr.get("strategy_bucket") or tr.get("strategy_bucket") or "NONE",
            "action": wr.get("action") or tr.get("action") or "WATCH",
            "pnl_pct": wr.get("pnl_pct","")
        })

    write_csv(POS_MERGED, pos_rows, ["stock_id","price_tier","ref_price","shares","avg_cost","pnl_pct","target_weight","current_weight_est","action","note"])
    write_csv(WATCH_MERGED, watch_rows, ["stock_id","price_tier","ref_price","holding_status","strategy_bucket","action","pnl_pct"])

    now_tp = tp_now()
    meta = {}
    if META_PATH.exists():
        try: meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except: meta = {}
    if latest_date:
        meta["signal_date"] = latest_date.isoformat()
        meta["trade_date"] = next_bday(latest_date).isoformat()
        meta["price_panel_latest_date"] = latest_date.isoformat()
    meta["generated_at"] = now_tp.strftime("%Y-%m-%d %H:%M:%S")
    meta["trade_plan_batch"] = now_tp.strftime("%Y-%m-%d %H:%M:%S")
    meta["source"] = "v266.11_pipeline"
    meta["data_state"] = "ok" if latest_date and latest_date >= expected_signal_date(now_tp) else "stale"
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    git_commit_push(
        [str(META_PATH), str(POS_MERGED), str(WATCH_MERGED)],
        "v266.11 postprocess meta and merged outputs"
    )

if __name__ == "__main__":
    main()
