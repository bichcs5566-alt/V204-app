# v3.7.1 chip light radar builder
# 只新增 chip_light.csv，不動主策略、不改 trade_plan。
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(".")
DASH = Path("mobile_dashboard_v1/data")
PRICE_PANEL = ROOT / "price_panel_daily.csv"
TRADE_PLAN_CANDIDATES = [DASH / "trade_plan.csv", ROOT / "trade_plan.csv"]
OUT = DASH / "chip_light.csv"
OUT_ROOT = ROOT / "chip_light.csv"

def read_csv_auto(path: Path) -> pd.DataFrame:
    for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def normalize_price_panel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "trade_date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"trade_date": "date"})
    if "symbol" in df.columns and "stock_id" not in df.columns:
        df = df.rename(columns={"symbol": "stock_id"})
    for c in ["date", "stock_id", "close"]:
        if c not in df.columns:
            raise ValueError(f"price_panel_daily.csv missing column: {c}")
    if "volume" not in df.columns:
        df["volume"] = np.nan
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["date", "stock_id", "close"]).copy()
    df = df[df["close"] > 0].sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df

def load_trade_plan() -> pd.DataFrame:
    for p in TRADE_PLAN_CANDIDATES:
        if p.exists():
            df = read_csv_auto(p)
            df.columns = [str(c).strip().lower() for c in df.columns]
            if "stock_id" not in df.columns:
                raise ValueError(f"{p} missing stock_id")
            df["stock_id"] = df["stock_id"].astype(str).str.strip()
            return df
    raise FileNotFoundError("trade_plan.csv not found")

def score_one(stock_df: pd.DataFrame) -> dict:
    d = stock_df.sort_values("date").copy()
    latest = d.iloc[-1]
    stock_id = str(latest["stock_id"])
    latest_date = pd.Timestamp(latest["date"]).strftime("%Y-%m-%d")
    close = float(latest["close"])

    d["ret1"] = d["close"].pct_change()
    d["vol_ma5"] = d["volume"].rolling(5).mean()
    d["high20"] = d["close"].rolling(20).max()

    mom5 = close / float(d["close"].iloc[-6]) - 1 if len(d) >= 6 else np.nan
    mom20 = close / float(d["close"].iloc[-21]) - 1 if len(d) >= 21 else np.nan

    vol = latest.get("volume", np.nan)
    vol_ma5 = d["vol_ma5"].iloc[-1]
    high20 = d["high20"].iloc[-1]

    vol_ratio_5 = float(vol) / float(vol_ma5) if pd.notna(vol) and pd.notna(vol_ma5) and vol_ma5 > 0 else np.nan
    near_high_20 = close / float(high20) if pd.notna(high20) and high20 > 0 else np.nan

    score = 0
    labels = []
    notes = []

    if pd.notna(vol_ratio_5):
        if vol_ratio_5 >= 2.0:
            score += 3
            labels.append("📈量能強放大")
            notes.append(f"量比5日={vol_ratio_5:.2f}")
        elif vol_ratio_5 >= 1.5:
            score += 2
            labels.append("📈量能放大")
            notes.append(f"量比5日={vol_ratio_5:.2f}")

    if pd.notna(mom5):
        if mom5 >= 0.08:
            score += 2
            labels.append("🔥短線加速")
            notes.append(f"mom5={mom5:.2%}")
        elif mom5 >= 0.04:
            score += 1
            labels.append("🟢短線轉強")
            notes.append(f"mom5={mom5:.2%}")

    if pd.notna(mom20):
        if mom20 >= 0.15:
            score += 2
            labels.append("🚀中期強勢")
            notes.append(f"mom20={mom20:.2%}")
        elif mom20 >= 0.08:
            score += 1
            labels.append("🟢中期偏強")
            notes.append(f"mom20={mom20:.2%}")

    if pd.notna(near_high_20):
        if near_high_20 >= 0.995:
            score += 2
            labels.append("🔺近20日高")
            notes.append(f"近高比={near_high_20:.2%}")
        elif near_high_20 >= 0.97:
            score += 1
            labels.append("🟡接近區間高")
            notes.append(f"近高比={near_high_20:.2%}")

    if pd.notna(vol_ratio_5) and vol_ratio_5 >= 1.8 and pd.notna(mom5) and mom5 < 0:
        score -= 2
        labels.append("⚠️量增價弱")
        notes.append("量放大但5日動能為負")

    if score >= 6:
        chip_label = "🔥強勢加分"
    elif score >= 3:
        chip_label = "🟢偏強"
    elif score <= -1:
        chip_label = "⚠️注意"
    else:
        chip_label = "普通"

    return {
        "date": latest_date,
        "stock_id": stock_id,
        "chip_score": round(float(score), 4),
        "chip_label": chip_label,
        "chip_tags": " + ".join(labels) if labels else "普通",
        "chip_note": "；".join(notes) if notes else "",
        "vol_ratio_5": round(float(vol_ratio_5), 4) if pd.notna(vol_ratio_5) else "",
        "mom5": round(float(mom5), 4) if pd.notna(mom5) else "",
        "mom20": round(float(mom20), 4) if pd.notna(mom20) else "",
        "near_high_20": round(float(near_high_20), 4) if pd.notna(near_high_20) else "",
    }

def main():
    DASH.mkdir(parents=True, exist_ok=True)
    if not PRICE_PANEL.exists():
        raise FileNotFoundError("price_panel_daily.csv not found. Run merge_chunked_price_panel.py first.")
    panel = normalize_price_panel(read_csv_auto(PRICE_PANEL))
    trade = load_trade_plan()
    symbols = sorted(set(trade["stock_id"].astype(str).str.strip()))
    grouped = {sid: g for sid, g in panel.groupby("stock_id")}
    rows = []
    for sid in symbols:
        g = grouped.get(str(sid))
        if g is None or g.empty:
            rows.append({
                "date": "",
                "stock_id": sid,
                "chip_score": 0,
                "chip_label": "無資料",
                "chip_tags": "無資料",
                "chip_note": "price_panel 無此股票",
                "vol_ratio_5": "",
                "mom5": "",
                "mom20": "",
                "near_high_20": "",
            })
        else:
            rows.append(score_one(g))
    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    out.to_csv(OUT_ROOT, index=False, encoding="utf-8-sig")
    print("v3.7.1 chip light radar done")
    print("rows:", len(out))
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
