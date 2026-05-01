"""
macro_regime.py
v266.12 總經自動化版

定位：
- 舊 PDF 的「十項總經指標」只當規則模板。
- 本程式會自動抓最新資料，轉成 macro_score / macro_regime。
- 輸出給 app.js 與 final_decision_engine.py 使用。

輸出：
- macro_regime.json
- macro_regime.csv
- mobile_dashboard_v1/data/macro_regime.json
- mobile_dashboard_v1/data/macro_regime.csv

注意：
1. FRED 資料不需要 API key，直接用 CSV。
2. 台灣 M1B / 市值貨幣比 / 中國 PMI 若抓不到，會用空值跳過，不讓 pipeline 爆掉。
3. final_decision 會用可用指標數做評分，不會因單一來源失敗停止。
"""

from pathlib import Path
from datetime import datetime
import json
import math
import time
import urllib.parse

import pandas as pd
import requests

ROOT = Path(".")
DATA_DIR = ROOT / "mobile_dashboard_v1" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"

FRED_SERIES = {
    "yield_curve_10y3m": "T10Y3M",
    "us_unemployment": "UNRATE",
    "us_consumer_sentiment": "UMCSENT",
    "us_durable_orders": "DGORDER",
    "us_leading_index": "USSLIND",
}

MANUAL_INPUT_FILES = [ROOT / "macro_inputs.csv", DATA_DIR / "macro_inputs.csv"]


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, float) and math.isnan(x):
            return default
        s = str(x).strip().replace("%", "")
        if s in ["", "--", "nan", "None", "null"]:
            return default
        return float(s)
    except Exception:
        return default


def get_fred_series(series, timeout=20):
    url = FRED_BASE.format(series=series)
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))
        df.columns = [str(c).strip() for c in df.columns]
        df = df.rename(columns={"observation_date": "date", series: "value"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"])
        return df[["date", "value"]].sort_values("date")
    except Exception as e:
        print(f"⚠️ FRED {series} fetch failed: {e}")
        return pd.DataFrame(columns=["date", "value"])


def yahoo_chart(symbol, rng="1y", interval="1d", timeout=20):
    encoded = urllib.parse.quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={rng}&interval={interval}"
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        js = r.json()
        result = js.get("chart", {}).get("result", [])
        if not result:
            return pd.DataFrame(columns=["date", "close"])
        item = result[0]
        ts = item.get("timestamp", [])
        quote = item.get("indicators", {}).get("quote", [{}])[0]
        close = quote.get("close", [])
        df = pd.DataFrame({"date": pd.to_datetime(ts, unit="s", errors="coerce"), "close": close})
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df.dropna(subset=["date", "close"]).sort_values("date")
    except Exception as e:
        print(f"⚠️ Yahoo {symbol} fetch failed: {e}")
        return pd.DataFrame(columns=["date", "close"])


def read_manual_inputs():
    for p in MANUAL_INPUT_FILES:
        if not p.exists() or p.stat().st_size == 0:
            continue
        for enc in ["utf-8-sig", "utf-8", "big5", "cp950"]:
            try:
                df = pd.read_csv(p, encoding=enc)
                df.columns = [str(c).strip() for c in df.columns]
                return df
            except Exception:
                continue
    return pd.DataFrame()


def score_indicator(name, value, extra=None):
    extra = extra or {}
    if value is None:
        return 0, "UNKNOWN", "資料不足，暫不計分"

    if name == "yield_curve_10y3m":
        if value < 0:
            return -1, "BAD", "殖利率曲線倒掛，景氣風險升高"
        if value < 0.5:
            return 0, "MID", "殖利率曲線偏低，保守看待"
        return 1, "GOOD", "殖利率曲線正常"

    if name == "us_unemployment":
        prev = extra.get("prev")
        delta = value - prev if prev is not None else 0
        if value >= 5.0 or delta >= 0.4:
            return -1, "BAD", "失業率偏高或快速轉升"
        if value <= 4.2 and delta <= 0.2:
            return 1, "GOOD", "就業仍穩定"
        return 0, "MID", "就業中性"

    if name == "us_consumer_sentiment":
        if value >= 80:
            return 1, "GOOD", "美國消費信心偏強"
        if value < 65:
            return -1, "BAD", "美國消費信心偏弱"
        return 0, "MID", "美國消費信心中性"

    if name == "us_durable_orders":
        if value > 2:
            return 1, "GOOD", "耐久財訂單擴張"
        if value < -2:
            return -1, "BAD", "耐久財訂單收縮"
        return 0, "MID", "耐久財訂單中性"

    if name == "us_leading_index":
        prev = extra.get("prev")
        delta = value - prev if prev is not None else 0
        if delta > 0:
            return 1, "GOOD", "美國領先指標改善"
        if delta < 0:
            return -1, "BAD", "美國領先指標轉弱"
        return 0, "MID", "美國領先指標持平"

    if name == "taiex_trend":
        ret20 = extra.get("ret20")
        above_ma60 = extra.get("above_ma60")
        if ret20 is not None and ret20 > 0.03 and above_ma60:
            return 1, "GOOD", "台股中期趨勢偏多"
        if ret20 is not None and ret20 < -0.03 and not above_ma60:
            return -1, "BAD", "台股中期趨勢偏弱"
        return 0, "MID", "台股中期趨勢中性"

    if name == "crb_proxy":
        ret60 = extra.get("ret60")
        if ret60 is not None and ret60 > 0.05:
            return 1, "GOOD", "商品趨勢偏多，景氣/通膨動能較強"
        if ret60 is not None and ret60 < -0.05:
            return -1, "BAD", "商品趨勢偏弱"
        return 0, "MID", "商品趨勢中性"

    if name == "tw_m1b_yoy":
        if value > 0:
            return 1, "GOOD", "台灣 M1B 年增率為正，資金偏多"
        if value < 0:
            return -1, "BAD", "台灣 M1B 年增率為負，資金偏弱"
        return 0, "MID", "台灣 M1B 中性"

    if name == "tw_market_cap_money_ratio":
        if value > 2:
            return -1, "BAD", "市值貨幣比偏高，市場可能過熱"
        if value < 1.2:
            return 1, "GOOD", "市值貨幣比不高，評價壓力較低"
        return 0, "MID", "市值貨幣比中性"

    if name == "china_pmi":
        if value >= 50:
            return 1, "GOOD", "中國 PMI 高於榮枯線"
        return -1, "BAD", "中國 PMI 低於榮枯線"

    return 0, "UNKNOWN", "未定義指標"


def build_macro_indicators():
    rows = []

    for name, series in FRED_SERIES.items():
        df = get_fred_series(series)
        if df.empty:
            rows.append({"indicator": name, "source": f"FRED:{series}", "date": "", "value": "", "score": 0, "status": "UNKNOWN", "label": "抓取失敗或無資料"})
            continue
        val = float(df["value"].iloc[-1])
        date = pd.to_datetime(df["date"].iloc[-1]).strftime("%Y-%m-%d")
        prev = float(df["value"].iloc[-4]) if len(df) >= 4 else None
        score, status, label = score_indicator(name, val, {"prev": prev})
        rows.append({"indicator": name, "source": f"FRED:{series}", "date": date, "value": round(val, 4), "score": score, "status": status, "label": label})
        time.sleep(0.2)

    tw = yahoo_chart("^TWII", "1y", "1d")
    if not tw.empty and len(tw) >= 60:
        close = float(tw["close"].iloc[-1])
        ma60 = float(tw["close"].rolling(60).mean().iloc[-1])
        ret20 = close / float(tw["close"].iloc[-21]) - 1 if len(tw) >= 21 else None
        score, status, label = score_indicator("taiex_trend", close, {"ret20": ret20, "above_ma60": close >= ma60})
        rows.append({"indicator": "taiex_trend", "source": "Yahoo:^TWII", "date": tw["date"].iloc[-1].strftime("%Y-%m-%d"), "value": round(close, 2), "score": score, "status": status, "label": label, "ret20": round(ret20, 4), "ma60": round(ma60, 2)})
    else:
        rows.append({"indicator": "taiex_trend", "source": "Yahoo:^TWII", "date": "", "value": "", "score": 0, "status": "UNKNOWN", "label": "抓取失敗或資料不足"})

    crb = yahoo_chart("DBC", "1y", "1d")
    if not crb.empty and len(crb) >= 60:
        close = float(crb["close"].iloc[-1])
        ret60 = close / float(crb["close"].iloc[-61]) - 1 if len(crb) >= 61 else None
        score, status, label = score_indicator("crb_proxy", close, {"ret60": ret60})
        rows.append({"indicator": "crb_proxy", "source": "Yahoo:DBC", "date": crb["date"].iloc[-1].strftime("%Y-%m-%d"), "value": round(close, 2), "score": score, "status": status, "label": label, "ret60": round(ret60, 4)})
    else:
        rows.append({"indicator": "crb_proxy", "source": "Yahoo:DBC", "date": "", "value": "", "score": 0, "status": "UNKNOWN", "label": "抓取失敗或資料不足"})

    manual = read_manual_inputs()
    if not manual.empty:
        for _, r in manual.iterrows():
            name = str(r.get("indicator", "")).strip()
            val = safe_float(r.get("value", None))
            if not name:
                continue
            score, status, label = score_indicator(name, val)
            rows.append({"indicator": name, "source": str(r.get("source", "manual")), "date": str(r.get("date", "")), "value": val if val is not None else "", "score": score, "status": status, "label": label})

    return pd.DataFrame(rows)


def classify_macro(score, max_possible, valid_count):
    if valid_count <= 0 or max_possible <= 0:
        return "NEUTRAL", "總經中性", "總經資料不足，暫用中性模式"
    ratio = score / max_possible
    if ratio >= 0.35:
        return "RISK_ON", "總經偏多", "總經環境支持進攻"
    if ratio <= -0.25:
        return "RISK_OFF", "總經偏空", "總經環境偏保守，降低進攻"
    return "NEUTRAL", "總經中性", "總經環境中性，控制追高"


def main():
    df = build_macro_indicators()
    valid = df[df["status"].astype(str).str.upper() != "UNKNOWN"].copy()
    valid_count = int(len(valid))
    score = float(pd.to_numeric(valid["score"], errors="coerce").fillna(0).sum()) if valid_count else 0
    max_possible = float(valid_count)
    regime, label, policy = classify_macro(score, max_possible, valid_count)

    summary = {
        "generated_at": now_text(),
        "system": "macro_regime_v266_12",
        "macro_regime": regime,
        "macro_label": label,
        "macro_policy": policy,
        "macro_score": round(score, 2),
        "macro_score_ratio": round(score / max_possible, 4) if max_possible else 0,
        "valid_indicator_count": valid_count,
        "total_indicator_count": int(len(df)),
        "good_count": int((df["status"] == "GOOD").sum()),
        "mid_count": int((df["status"] == "MID").sum()),
        "bad_count": int((df["status"] == "BAD").sum()),
        "unknown_count": int((df["status"] == "UNKNOWN").sum()),
        "top_notes": df["label"].dropna().astype(str).head(8).tolist(),
        "encoding": "utf-8-sig",
    }

    df.to_csv(ROOT / "macro_regime.csv", index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / "macro_regime.csv", index=False, encoding="utf-8-sig")
    for p in [ROOT / "macro_regime.json", DATA_DIR / "macro_regime.json"]:
        p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
