"""
market_regime_engine.py

市場狀態判斷引擎

目的：
把每天市場分成：
- BEAR：熊市 / 防守
- RANGE：盤整 / 觀察
- BULL：多頭 / 正常進攻
- EXPLOSIVE：爆發期 / 加速進攻

輸入：
- feature_panel_daily.csv
或
- price_panel_daily.csv

輸出：
- market_regime.json
- mobile_dashboard_v1/data/market_regime.json

核心邏輯：
不用大盤指數，直接用全市場股票狀態推估。
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CANDIDATES = [
    ROOT / "feature_panel_daily.csv",
    ROOT / "price_panel_daily.csv",
    DATA_DIR / "feature_panel_daily.csv",
    DATA_DIR / "price_panel_daily.csv",
]


def normalize_stock_id(x):
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s


def find_input():
    for p in INPUT_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError("missing feature_panel_daily.csv / price_panel_daily.csv")


def load_panel(path):
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns and "trade_date" in df.columns:
        df["date"] = df["trade_date"]

    if "stock_id" not in df.columns:
        if "symbol" in df.columns:
            df["stock_id"] = df["symbol"]
        elif "code" in df.columns:
            df["stock_id"] = df["code"]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["stock_id"] = df["stock_id"].apply(normalize_stock_id)

    for c in ["close", "volume"]:
        if c not in df.columns:
            raise ValueError(f"missing column: {c}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "stock_id", "close"])
    df = df[df["stock_id"].astype(str).str.match(r"^\d{4}$", na=False)].copy()
    df = df[df["close"] > 0].copy()
    return df.sort_values(["stock_id", "date"]).reset_index(drop=True)


def add_features(g):
    g = g.sort_values("date").copy()

    if "ma20" not in g.columns:
        g["ma20"] = g["close"].rolling(20).mean()
    if "ma60" not in g.columns:
        g["ma60"] = g["close"].rolling(60).mean()

    g["ret1"] = g["close"].pct_change()
    g["mom20_regime"] = g["close"] / g["close"].shift(20) - 1
    g["mom60_regime"] = g["close"] / g["close"].shift(60) - 1
    g["vol_ma20_regime"] = g["volume"].rolling(20).mean()

    return g


def main():
    src = find_input()
    df = load_panel(src)

    parts = []
    for _, g in df.groupby("stock_id", sort=False):
        if len(g) >= 80:
            parts.append(add_features(g))

    if not parts:
        raise RuntimeError("not enough data for regime")

    panel = pd.concat(parts, ignore_index=True)
    latest_date = panel["date"].max()
    today = panel[panel["date"] == latest_date].copy()

    today = today.dropna(subset=["ma20", "ma60", "mom20_regime", "mom60_regime"])

    if today.empty:
        raise RuntimeError("latest regime sample empty")

    above_ma20 = float((today["close"] > today["ma20"]).mean())
    above_ma60 = float((today["close"] > today["ma60"]).mean())
    mom20_pos = float((today["mom20_regime"] > 0).mean())
    mom60_pos = float((today["mom60_regime"] > 0).mean())

    # 爆發期判斷：量能與動能一起偏強
    volume_active = 0.0
    if "vol_ma20_regime" in today.columns:
        valid_vol = today.dropna(subset=["vol_ma20_regime"])
        valid_vol = valid_vol[valid_vol["vol_ma20_regime"] > 0]
        if not valid_vol.empty:
            volume_active = float((valid_vol["volume"] > valid_vol["vol_ma20_regime"] * 1.25).mean())

    breadth_score = (
        above_ma20 * 30 +
        above_ma60 * 25 +
        mom20_pos * 25 +
        mom60_pos * 15 +
        volume_active * 5
    )

    if breadth_score >= 70 and above_ma20 >= 0.62 and volume_active >= 0.28:
        regime = "EXPLOSIVE"
        label = "爆發期"
        risk_mode = "攻擊"
        gross_exposure = 0.75
        pre_budget = 0.12
        core_budget = 0.48
        alpha_budget = 0.15
    elif breadth_score >= 58 and above_ma60 >= 0.52:
        regime = "BULL"
        label = "多頭"
        risk_mode = "正常進攻"
        gross_exposure = 0.60
        pre_budget = 0.10
        core_budget = 0.38
        alpha_budget = 0.12
    elif breadth_score >= 42:
        regime = "RANGE"
        label = "盤整"
        risk_mode = "控風險"
        gross_exposure = 0.35
        pre_budget = 0.08
        core_budget = 0.22
        alpha_budget = 0.05
    else:
        regime = "BEAR"
        label = "熊市"
        risk_mode = "防守"
        gross_exposure = 0.15
        pre_budget = 0.00
        core_budget = 0.12
        alpha_budget = 0.03

    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "market_regime_engine",
        "input_file": str(src),
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "regime": regime,
        "label": label,
        "risk_mode": risk_mode,
        "breadth_score": round(float(breadth_score), 2),
        "above_ma20": round(above_ma20, 4),
        "above_ma60": round(above_ma60, 4),
        "mom20_positive": round(mom20_pos, 4),
        "mom60_positive": round(mom60_pos, 4),
        "volume_active": round(volume_active, 4),
        "gross_exposure": gross_exposure,
        "budget": {
            "PRE": pre_budget,
            "CORE": core_budget,
            "ALPHA": alpha_budget
        },
        "rule": "BEAR 停 PRE；RANGE 小倉 PRE；BULL 正常；EXPLOSIVE 加強 CORE，PRE 仍小倉。"
    }

    for p in [ROOT / "market_regime.json", DATA_DIR / "market_regime.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
