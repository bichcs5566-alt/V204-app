"""
v266_build_market_data.py
資料層重建：建立 raw_market_daily.csv / price_panel_daily.csv

用途：
- 優先從既有 price_panel_daily.csv 延伸整理
- 如果 repo 已有 update_market_data.py，workflow 會先跑它
- 本檔負責把價格資料標準化，不做策略
"""
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np

ROOT = Path('.')
DATA_DIR = ROOT / 'mobile_dashboard_v1' / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

REQ = ['date','stock_id','open','high','low','close','volume']


def norm_id(x):
    s = str(x).strip()
    if s.endswith('.0'):
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) <= 4 else s


def is_common(s):
    s = norm_id(s)
    return s.isdigit() and len(s) == 4 and not s.startswith(('00','03','04','05','06','07','08','09'))


def find_source():
    candidates = [
        ROOT/'raw_market_daily.csv',
        ROOT/'price_panel_daily.csv',
        ROOT/'data'/'price_panel_daily.csv',
        DATA_DIR/'price_panel_daily.csv',
    ]
    for p in candidates:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError('找不到任何可用價格資料：raw_market_daily.csv / price_panel_daily.csv')


def standardize(df):
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    if 'trade_date' in df.columns and 'date' not in df.columns:
        df['date'] = df['trade_date']
    if 'symbol' in df.columns and 'stock_id' not in df.columns:
        df['stock_id'] = df['symbol']
    if 'code' in df.columns and 'stock_id' not in df.columns:
        df['stock_id'] = df['code']
    if 'close' not in df.columns:
        raise ValueError('缺 close 欄位')
    if 'date' not in df.columns:
        raise ValueError('缺 date 欄位')
    if 'stock_id' not in df.columns:
        raise ValueError('缺 stock_id 欄位')
    for c in ['open','high','low']:
        if c not in df.columns:
            df[c] = df['close']
    if 'volume' not in df.columns:
        df['volume'] = 0
    if 'name' not in df.columns:
        df['name'] = ''
    if 'market' not in df.columns:
        df['market'] = ''
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['stock_id'] = df['stock_id'].apply(norm_id)
    for c in ['open','high','low','close','volume']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['date','stock_id','close'])
    df = df[df['stock_id'].apply(is_common)]
    df = df[df['close'] > 0]
    for c in ['open','high','low']:
        df[c] = df[c].fillna(df['close'])
    df['volume'] = df['volume'].fillna(0)
    df = df.drop_duplicates(['date','stock_id'], keep='last')
    df = df.sort_values(['stock_id','date']).reset_index(drop=True)
    return df[['date','stock_id','name','market','open','high','low','close','volume']]


def main():
    src = find_source()
    raw = pd.read_csv(src)
    panel = standardize(raw)
    if len(panel) < 500:
        raise RuntimeError(f'價格資料太少：{len(panel)} rows')
    raw.to_csv(ROOT/'raw_market_daily.csv', index=False, encoding='utf-8')
    raw.to_csv(DATA_DIR/'raw_market_daily.csv', index=False, encoding='utf-8')
    panel.to_csv(ROOT/'price_panel_daily.csv', index=False, encoding='utf-8')
    panel.to_csv(DATA_DIR/'price_panel_daily.csv', index=False, encoding='utf-8')
    meta = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source': 'v266_build_market_data',
        'input_file': str(src),
        'panel_rows': int(len(panel)),
        'stock_count': int(panel['stock_id'].nunique()),
        'start_date': str(panel['date'].min().date()),
        'end_date': str(panel['date'].max().date()),
    }
    for p in [ROOT/'data_meta.json', DATA_DIR/'data_meta.json']:
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    print(json.dumps(meta, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
