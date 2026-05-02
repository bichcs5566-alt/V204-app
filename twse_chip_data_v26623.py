# v266.23 TWSE 籌碼資料層
import requests
import pandas as pd
from datetime import datetime

def fetch_institutional():
    url = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json"
    r = requests.get(url)
    data = r.json()["data"]
    cols = ["stock_id","name","foreign","trust","dealer"]
    rows = []
    for d in data:
        rows.append({
            "stock_id": d[0],
            "foreign_net_buy": float(d[4].replace(",","")),
            "trust_net_buy": float(d[10].replace(",","")),
            "dealer_net_buy": float(d[16].replace(",",""))
        })
    df = pd.DataFrame(rows)
    df["inst_net_buy"] = df["foreign_net_buy"] + df["trust_net_buy"] + df["dealer_net_buy"]
    return df

def fetch_margin():
    url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json"
    r = requests.get(url)
    data = r.json()["data"]
    rows = []
    for d in data:
        rows.append({
            "stock_id": d[0],
            "margin_balance": float(d[5].replace(",","")),
            "short_balance": float(d[9].replace(",",""))
        })
    return pd.DataFrame(rows)

def main():
    inst = fetch_institutional()
    margin = fetch_margin()

    df = inst.merge(margin, on="stock_id", how="left")

    df.to_csv("chip_source_twse.csv", index=False, encoding="utf-8-sig")
    print("TWSE chip data saved")

if __name__ == "__main__":
    main()
