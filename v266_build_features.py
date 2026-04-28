"""
v266_build_features.py
price_panel_daily.csv → feature_panel_daily.csv
策略只讀 feature_panel_daily.csv，不再自己亂補 momentum / MA。
"""
from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path('.')
DATA_DIR = ROOT / 'mobile_dashboard_v1' / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_price():
    p = ROOT/'price_panel_daily.csv'
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR/'price_panel_daily.csv'
    if not p.exists() or p.stat().st_size == 0:
        raise FileNotFoundError('price_panel_daily.csv not found')
    df = pd.read_csv(p)
    df.columns = [str(c).lower().strip() for c in df.columns]
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['stock_id'] = df['stock_id'].astype(str).str.zfill(4)
    for c in ['open','high','low','close','volume']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['date','stock_id','close'])
    df = df[df['close'] > 0].sort_values(['stock_id','date']).reset_index(drop=True)
    return df


def build_features(df):
    out = df.copy()
    g = out.groupby('stock_id', group_keys=False)
    out['ret1'] = g['close'].pct_change()
    for n in [3,5,10,20,60]:
        out[f'mom{n}'] = g['close'].pct_change(n)
    for n in [5,10,20,60]:
        out[f'ma{n}'] = g['close'].rolling(n, min_periods=n).mean().reset_index(level=0, drop=True)
    out['vol20'] = g['ret1'].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    out['vol_ma5'] = g['volume'].rolling(5, min_periods=5).mean().reset_index(level=0, drop=True)
    out['vol_ma20'] = g['volume'].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    out['volume_ratio'] = out['volume'] / (out['vol_ma20'] + 1e-9)
    out['vol_dry_ratio'] = out['vol_ma5'] / (out['vol_ma20'] + 1e-9)
    out['high_20'] = g['high'].rolling(20, min_periods=20).max().reset_index(level=0, drop=True)
    out['low_20'] = g['low'].rolling(20, min_periods=20).min().reset_index(level=0, drop=True)
    out['high_60'] = g['high'].rolling(60, min_periods=60).max().reset_index(level=0, drop=True)
    out['low_60'] = g['low'].rolling(60, min_periods=60).min().reset_index(level=0, drop=True)
    out['range_20'] = (out['high_20'] - out['low_20']) / (out['close'] + 1e-9)
    out['ma_max'] = out[['ma5','ma10','ma20']].max(axis=1)
    out['ma_min'] = out[['ma5','ma10','ma20']].min(axis=1)
    out['ma_converge_pct'] = (out['ma_max'] - out['ma_min']) / (out['close'] + 1e-9)
    out['ma20_slope'] = g['ma20'].diff(5) / (g['ma20'].shift(5) + 1e-9)
    low9 = g['low'].rolling(9, min_periods=9).min().reset_index(level=0, drop=True)
    high9 = g['high'].rolling(9, min_periods=9).max().reset_index(level=0, drop=True)
    rsv = (out['close'] - low9) / (high9 - low9 + 1e-9) * 100
    out['kd_k'] = rsv.groupby(out['stock_id']).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out['kd_d'] = out['kd_k'].groupby(out['stock_id']).ewm(com=2, adjust=False).mean().reset_index(level=0, drop=True)
    out['kd_cross'] = ((out['kd_k'] > out['kd_d']) & (g['kd_k'].shift(1) <= g['kd_d'].shift(1))).astype(int)
    ema12 = g['close'].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    ema26 = g['close'].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    out['macd_diff'] = ema12 - ema26
    out['macd_signal'] = out.groupby('stock_id')['macd_diff'].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    out['macd_hist'] = out['macd_diff'] - out['macd_signal']
    out['macd_cross'] = ((out['macd_diff'] > out['macd_signal']) & (g['macd_diff'].shift(1) <= g['macd_signal'].shift(1))).astype(int)
    close_diff = g['close'].diff()
    signed_volume = np.where(close_diff > 0, out['volume'], np.where(close_diff < 0, -out['volume'], 0))
    out['obv_proxy'] = pd.Series(signed_volume, index=out.index).groupby(out['stock_id']).cumsum()
    out['obv_mom5'] = g['obv_proxy'].pct_change(5).replace([np.inf, -np.inf], np.nan)
    for w in [3,5,10]:
        out[f'obv_up_count_{w}'] = out.groupby('stock_id')['obv_proxy'].transform(lambda s, ww=w: (s.diff() > 0).astype(float).rolling(ww, min_periods=ww).sum())
        out[f'low_non_down_count_{w}'] = out.groupby('stock_id')['low'].transform(lambda s, ww=w: (s.diff() >= 0).astype(float).rolling(ww, min_periods=ww).sum())
    out['history_count'] = g.cumcount() + 1
    out['has_60d_history'] = out['history_count'] >= 60
    return out


def main():
    price = load_price()
    feat = build_features(price)
    feat.to_csv(ROOT/'feature_panel_daily.csv', index=False, encoding='utf-8')
    feat.to_csv(DATA_DIR/'feature_panel_daily.csv', index=False, encoding='utf-8')
    meta = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source': 'v266_build_features',
        'rows': int(len(feat)),
        'stock_count': int(feat['stock_id'].nunique()),
        'start_date': str(feat['date'].min().date()),
        'end_date': str(feat['date'].max().date()),
        'latest_rows': int((feat['date'] == feat['date'].max()).sum()),
        'latest_has_60d_count': int(((feat['date'] == feat['date'].max()) & (feat['has_60d_history'])).sum()),
    }
    for p in [ROOT/'feature_meta.json', DATA_DIR/'feature_meta.json']:
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    print(json.dumps(meta, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
