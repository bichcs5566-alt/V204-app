"""
v266_data_guard.py
資料防呆：mom/MA/歷史長度異常就直接停止，不准產生假名單。
"""
from pathlib import Path
from datetime import datetime
import json
import pandas as pd

ROOT = Path('.')
DATA_DIR = ROOT / 'mobile_dashboard_v1' / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

MIN_LATEST_STOCKS = 500
MIN_HAS_60D_STOCKS = 300
MAX_ZERO_MOM20_RATIO = 0.75
MAX_ABOVE_MA20_RATIO = 0.98
MIN_UNIQUE_DATES = 60


def write_report(report):
    for p in [ROOT/'data_guard_report.json', DATA_DIR/'data_guard_report.json']:
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)


def fail(report, reason):
    report['status'] = 'FAIL'
    report['reason'] = reason
    write_report(report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise RuntimeError(reason)


def main():
    p = ROOT/'feature_panel_daily.csv'
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR/'feature_panel_daily.csv'
    report = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'source': 'v266_data_guard'}
    if not p.exists() or p.stat().st_size == 0:
        fail(report, 'feature_panel_daily.csv missing')
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    latest_date = df['date'].max()
    latest = df[df['date'] == latest_date].copy()
    for c in ['mom20','ma20','ma60','close']:
        latest[c] = pd.to_numeric(latest.get(c), errors='coerce')
    unique_dates = int(df['date'].nunique())
    latest_count = int(len(latest))
    has_60d_count = int(pd.Series(latest.get('has_60d_history', False)).astype(str).str.lower().isin(['true','1']).sum())
    valid_mom20 = latest['mom20'].dropna()
    zero_mom20_ratio = float((valid_mom20.abs() < 1e-12).mean()) if len(valid_mom20) else 1.0
    above_ma20_ratio = float((latest['close'] >= latest['ma20']).dropna().mean()) if latest['ma20'].notna().any() else 1.0
    report.update({
        'status': 'INIT',
        'feature_file': str(p),
        'unique_dates': unique_dates,
        'latest_date': str(latest_date.date()),
        'latest_count': latest_count,
        'has_60d_count': has_60d_count,
        'zero_mom20_ratio': round(zero_mom20_ratio, 4),
        'above_ma20_ratio': round(above_ma20_ratio, 4),
        'valid_mom20_count': int(len(valid_mom20)),
    })
    if unique_dates < MIN_UNIQUE_DATES:
        fail(report, f'unique_dates too small: {unique_dates}')
    if latest_count < MIN_LATEST_STOCKS:
        fail(report, f'latest stock count too small: {latest_count}')
    if has_60d_count < MIN_HAS_60D_STOCKS:
        fail(report, f'has_60d_count too small: {has_60d_count}')
    if zero_mom20_ratio > MAX_ZERO_MOM20_RATIO:
        fail(report, f'mom20 zero ratio too high: {zero_mom20_ratio}')
    if above_ma20_ratio > MAX_ABOVE_MA20_RATIO:
        fail(report, f'above_ma20 ratio impossible: {above_ma20_ratio}')
    report['status'] = 'PASS'
    report['reason'] = 'data guard passed'
    write_report(report)
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
