# v3.7.3 chip light builder - include trade plan + current positions
# 目的：
# 1. 不動 v2_8 主策略
# 2. 不動 trade_plan.csv
# 3. 不動 current_positions.csv
# 4. 只補強 chip_light.csv 的覆蓋範圍
#
# 修正重點：
# 原本 chip_light.csv 只算「今日操作名單」
# 這版會同時計算：
# - mobile_dashboard_v1/data/trade_plan.csv
# - mobile_dashboard_v1/data/current_positions.csv
#
# 這樣持倉監控裡的股票也會有籌碼狀態，不會一直顯示「—」。

from pathlib import Path
import numpy as np
import pandas as pd


ROOT = Path(".")
DASH = Path("mobile_dashboard_v1/data")

PRICE_PANEL = ROOT / "price_panel_daily.csv"

TRADE_PLAN_CANDIDATES = [
    DASH / "trade_plan.csv",
    ROOT / "trade_plan.csv",
]

CURRENT_POSITIONS_CANDIDATES = [
    DASH / "current_positions.csv",
    ROOT / "current_positions.csv",
]

OUT = DASH / "chip_light.csv"
OUT_ROOT = ROOT / "chip_light.csv"


def read_csv_auto(path: Path) -> pd.DataFrame:
    for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)


def clean_stock_id(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().replace("\ufeff", "")
    s = "".join(ch for ch in s if ch.isalnum())
    return s


def normalize_price_panel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]

    if "trade_date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"trade_date": "date"})
    if "symbol" in df.columns and "stock_id" not in df.columns:
        df = df.rename(columns={"symbol": "stock_id"})
    if "stock" in df.columns and "stock_id" not in df.columns:
        df = df.rename(columns={"stock": "stock_id"})
    if "code" in df.columns and "stock_id" not in df.columns:
        df = df.rename(columns={"code": "stock_id"})

    need = ["date", "stock_id", "close"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"price_panel_daily.csv missing columns: {missing}")

    if "volume" not in df.columns:
        df["volume"] = np.nan

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(clean_stock_id)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"]).copy()
    df = df[(df["stock_id"] != "") & (df["close"] > 0)]
    df = df.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return df


def extract_stock_ids_from_csv(path: Path, source_name: str) -> list[str]:
    if not path.exists():
        return []

    df = read_csv_auto(path)
    df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]

    stock_col = None
    for c in ["stock_id", "stock", "symbol", "code"]:
        if c in df.columns:
            stock_col = c
            break

    if stock_col is None:
        print(f"[WARN] {source_name}: cannot find stock id column in {path}")
        return []

    ids = (
        df[stock_col]
        .apply(clean_stock_id)
        .replace("", np.nan)
        .dropna()
        .astype(str)
        .tolist()
    )

    print(f"[INFO] loaded {len(ids)} symbols from {source_name}: {path}")
    return ids


def collect_target_symbols() -> list[str]:
    symbols = []

    for p in TRADE_PLAN_CANDIDATES:
        ids = extract_stock_ids_from_csv(p, "trade_plan")
        if ids:
            symbols.extend(ids)
            break

    for p in CURRENT_POSITIONS_CANDIDATES:
        ids = extract_stock_ids_from_csv(p, "current_positions")
        if ids:
            symbols.extend(ids)
            break

    seen = set()
    out = []
    for sid in symbols:
        sid = clean_stock_id(sid)
        if sid and sid not in seen:
            out.append(sid)
            seen.add(sid)

    print(f"[INFO] total target symbols for chip_light: {len(out)}")
    return out


def score_one(stock_df: pd.DataFrame) -> dict:
    d = stock_df.sort_values("date").copy()

    latest = d.iloc[-1]
    stock_id = clean_stock_id(latest["stock_id"])
    latest_date = pd.Timestamp(latest["date"]).strftime("%Y-%m-%d")
    close = float(latest["close"])

    d["ret1"] = d["close"].pct_change()
    d["vol_ma5"] = d["volume"].rolling(5).mean()
    d["vol_ma20"] = d["volume"].rolling(20).mean()
    d["high20"] = d["close"].rolling(20).max()

    mom5 = np.nan
    mom20 = np.nan
    vol_ratio_5 = np.nan
    near_high_20 = np.nan

    if len(d) >= 6:
        base5 = float(d["close"].iloc[-6])
        if base5 > 0:
            mom5 = close / base5 - 1

    if len(d) >= 21:
        base20 = float(d["close"].iloc[-21])
        if base20 > 0:
            mom20 = close / base20 - 1

    vol = latest.get("volume", np.nan)
    vol_ma5 = d["vol_ma5"].iloc[-1] if "vol_ma5" in d.columns else np.nan
    high20 = d["high20"].iloc[-1] if "high20" in d.columns else np.nan

    if pd.notna(vol) and pd.notna(vol_ma5) and vol_ma5 > 0:
        vol_ratio_5 = float(vol) / float(vol_ma5)

    if pd.notna(high20) and high20 > 0:
        near_high_20 = close / float(high20)

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
    symbols = collect_target_symbols()

    if not symbols:
        raise RuntimeError("No target symbols from trade_plan.csv or current_positions.csv")

    grouped = {sid: g for sid, g in panel.groupby("stock_id")}

    rows = []
    for sid in symbols:
        sid = clean_stock_id(sid)
        g = grouped.get(sid)

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
    out = out.sort_values(["stock_id"]).reset_index(drop=True)

    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    out.to_csv(OUT_ROOT, index=False, encoding="utf-8-sig")

    print("v3.7.3 chip light builder done")
    print("output:", OUT)
    print("rows:", len(out))
    print(out.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
