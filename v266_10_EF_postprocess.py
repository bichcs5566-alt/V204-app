import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

DATA_DIR = Path("mobile_dashboard_v1/data")
META_PATH = DATA_DIR / "meta.json"
CURRENT_POS = DATA_DIR / "current_positions.csv"
WATCHLIST = DATA_DIR / "watchlist.csv"
TRADE_PLAN = DATA_DIR / "trade_plan.csv"
POS_MON = DATA_DIR / "position_monitor.csv"
WATCH_MON = DATA_DIR / "watchlist_monitor.csv"
POS_MERGED = DATA_DIR / "position_monitor_merged.csv"
WATCH_MERGED = DATA_DIR / "watchlist_monitor_merged.csv"

PRICE_PANEL_CANDIDATES = [
    Path("price_panel_daily.csv"),
    DATA_DIR / "price_panel_daily.csv",
    Path("mobile_dashboard_v1/price_panel_daily.csv"),
]

def taipei_now():
    return datetime.utcnow() + timedelta(hours=8)

def read_csv(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows, headers):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})

def find_price_panel():
    for p in PRICE_PANEL_CANDIDATES:
        if p.exists():
            return p
    return None

def next_trading_day(d):
    x = d + timedelta(days=1)
    while x.weekday() >= 5:
        x += timedelta(days=1)
    return x

def read_latest_price_map():
    path = find_price_panel()
    if not path:
        return {}, None
    rows = read_csv(path)
    latest_by_stock = {}
    latest_date = None
    for r in rows:
        sid = str(r.get("stock_id") or r.get("code") or "").strip()
        ds = str(r.get("date") or "").strip()
        if not sid or not ds:
            continue
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        close = r.get("close") or r.get("adj_close") or r.get("æ¶ç¤å¹") or ""
        latest_date = max(latest_date, d) if latest_date else d
        old = latest_by_stock.get(sid)
        if old is None or d >= old["date"]:
            latest_by_stock[sid] = {"date": d, "close": str(close).strip()}
    return latest_by_stock, latest_date

def price_tier(v):
    try:
        x = float(v)
    except Exception:
        return "unknown"
    if x < 50: return "lt_50"
    if x < 100: return "p50_100"
    if x < 300: return "p100_300"
    if x < 500: return "p300_500"
    if x < 1000: return "p500_1000"
    return "gt_1000"

def build_monitor_rows():
    pos_rows = read_csv(CURRENT_POS)
    watch_rows = read_csv(WATCHLIST)
    trade_rows = read_csv(TRADE_PLAN)
    pos_mon_rows = read_csv(POS_MON)
    watch_mon_rows = read_csv(WATCH_MON)
    price_map, latest_date = read_latest_price_map()

    trade_map = {str(r.get("stock_id","")).strip(): r for r in trade_rows if str(r.get("stock_id","")).strip()}
    pos_mon_map = {str(r.get("stock_id","")).strip(): r for r in pos_mon_rows if str(r.get("stock_id","")).strip()}
    watch_mon_map = {str(r.get("stock_id","")).strip(): r for r in watch_mon_rows if str(r.get("stock_id","")).strip()}

    pos_out = []
    for r in pos_rows:
        sid = str(r.get("stock_id","")).strip()
        pr = pos_mon_map.get(sid, {})
        tr = trade_map.get(sid, {})
        px = price_map.get(sid, {})
        ref = pr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        row = {
            "stock_id": sid,
            "price_tier": pr.get("price_tier") or tr.get("price_tier") or price_tier(ref),
            "ref_price": ref,
            "shares": r.get("shares",""),
            "avg_cost": r.get("avg_cost",""),
            "pnl_pct": pr.get("pnl_pct",""),
            "target_weight": pr.get("target_weight") or tr.get("target_weight",""),
            "current_weight_est": pr.get("current_weight_est",""),
            "action": pr.get("action") or tr.get("action") or "HOLD",
            "note": pr.get("note") or r.get("note") or "",
        }
        pos_out.append(row)

    watch_out = []
    for r in watch_rows:
        sid = str(r.get("stock_id","")).strip()
        wr = watch_mon_map.get(sid, {})
        tr = trade_map.get(sid, {})
        px = price_map.get(sid, {})
        ref = wr.get("ref_price") or tr.get("ref_price") or px.get("close") or ""
        row = {
            "stock_id": sid,
            "price_tier": wr.get("price_tier") or tr.get("price_tier") or price_tier(ref),
            "ref_price": ref,
            "holding_status": wr.get("holding_status","æªææ"),
            "strategy_bucket": wr.get("strategy_bucket") or tr.get("strategy_bucket") or "NONE",
            "action": wr.get("action") or tr.get("action") or "WATCH",
            "pnl_pct": wr.get("pnl_pct",""),
        }
        watch_out.append(row)

    return pos_out, watch_out, latest_date

def main():
    pos_out, watch_out, latest_date = build_monitor_rows()

    write_csv(POS_MERGED, pos_out, ["stock_id","price_tier","ref_price","shares","avg_cost","pnl_pct","target_weight","current_weight_est","action","note"])
    write_csv(WATCH_MERGED, watch_out, ["stock_id","price_tier","ref_price","holding_status","strategy_bucket","action","pnl_pct"])

    meta = {}
    if META_PATH.exists():
        with META_PATH.open("r", encoding="utf-8") as f:
            meta = json.load(f)

    now = taipei_now()
    if latest_date:
        signal_date = latest_date
        trade_date = next_trading_day(latest_date)
        meta["signal_date"] = signal_date.isoformat()
        meta["trade_date"] = trade_date.isoformat()
    meta["generated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    meta["data_state"] = "ok"

    with META_PATH.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    subprocess.run(["git","config","user.name","github-actions[bot]"], check=True)
    subprocess.run(["git","config","user.email","41898282+github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git","add", str(META_PATH), str(POS_MERGED), str(WATCH_MERGED)], check=True)
    diff = subprocess.run(["git","diff","--cached","--quiet"])
    if diff.returncode != 0:
        subprocess.run(["git","commit","-m","postprocess EF metadata and merged monitors"], check=True)
        subprocess.run(["git","pull","--rebase","origin","main"])
        subprocess.run(["git","push","origin","main"], check=True)

if __name__ == "__main__":
    main()
