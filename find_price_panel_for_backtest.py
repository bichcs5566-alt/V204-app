import os
import shutil
from pathlib import Path
import pandas as pd

ROOT = Path('.')
OUTPUT = ROOT / 'price_panel_daily.csv'
CANDIDATE_KEYWORDS = [
    'price_panel_daily_10y',
    'price_panel_10y',
    'panel_10y',
    'daily_10y',
    'price_panel_daily',
    'price_panel',
    'panel_daily',
    'stock_panel',
]
DATE_COLS = ['trade_date', 'date', 'datetime', 'signal_date']
REQUIRED_COLS = {'symbol', 'close'}
TARGET_START = pd.Timestamp('2022-01-01')
TARGET_END = pd.Timestamp('2025-12-31')
MAX_PREVIEW_ROWS = 3000
SKIP_DIRS = {'.git', '__pycache__', '.venv', 'venv', 'node_modules'}


def norm_cols(cols):
    return [str(c).lower().strip() for c in cols]


def score_name(path: Path) -> int:
    name = path.name.lower()
    score = 0
    for i, kw in enumerate(CANDIDATE_KEYWORDS[::-1], start=1):
        if kw in name:
            score += i * 10
    if '10y' in name or '10yr' in name or '10year' in name:
        score += 50
    if 'daily' in name:
        score += 20
    if name == 'price_panel_daily.csv':
        score += 5
    return score


def find_csv_files(root: Path):
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in filenames:
            if fn.lower().endswith('.csv'):
                found.append(Path(dirpath) / fn)
    return found


def inspect_csv(path: Path):
    try:
        sample = pd.read_csv(path, nrows=MAX_PREVIEW_ROWS)
    except Exception as e:
        return None, f'read_error: {e}'

    sample.columns = norm_cols(sample.columns)
    date_col = next((c for c in DATE_COLS if c in sample.columns), None)
    if date_col is None:
        return None, 'missing_date_col'
    if not REQUIRED_COLS.issubset(set(sample.columns)):
        return None, 'missing_required_cols'

    try:
        dates = pd.to_datetime(sample[date_col], errors='coerce')
    except Exception as e:
        return None, f'date_parse_error: {e}'

    valid_dates = dates.dropna()
    if valid_dates.empty:
        return None, 'no_valid_dates'

    min_date = valid_dates.min()
    max_date = valid_dates.max()
    covers_target = (min_date <= TARGET_START) and (max_date >= pd.Timestamp('2025-01-01'))
    uniq_symbols = sample['symbol'].astype(str).nunique(dropna=True)

    score = score_name(path)
    score += min(uniq_symbols, 500) // 10
    if covers_target:
        score += 200
    if min_date <= pd.Timestamp('2018-01-01'):
        score += 50
    if max_date >= pd.Timestamp('2025-01-01'):
        score += 50

    info = {
        'path': str(path),
        'date_col': date_col,
        'min_date': str(min_date.date()),
        'max_date': str(max_date.date()),
        'uniq_symbols_preview': int(uniq_symbols),
        'covers_target': bool(covers_target),
        'score': int(score),
        'rows_preview': int(len(sample)),
    }
    return info, None


def main():
    csv_files = find_csv_files(ROOT)
    print(f'Found {len(csv_files)} csv files')

    inspected = []
    for path in csv_files:
        info, err = inspect_csv(path)
        if info:
            inspected.append(info)
            print(f"OK  | {path} | {info['min_date']} -> {info['max_date']} | score={info['score']}")
        else:
            print(f'SKIP| {path} | {err}')

    if not inspected:
        raise FileNotFoundError('æ¾ä¸å°å¯ç¨çå¹æ ¼è³æ CSVï¼è«ç¢ºèª repo å§æ¯å¦æ 10 å¹´æå®æ´ç price panel æªæ¡')

    inspected = sorted(inspected, key=lambda x: (x['score'], x['covers_target'], x['max_date']), reverse=True)
    best = inspected[0]
    best_path = Path(best['path'])

    print('\nSelected best candidate:')
    for k, v in best.items():
        print(f'{k}: {v}')

    if best_path.resolve() != OUTPUT.resolve():
        shutil.copy2(best_path, OUTPUT)
        print(f'Copied {best_path} -> {OUTPUT}')
    else:
        print('Best file is already price_panel_daily.csv; no copy needed')

    report = pd.DataFrame(inspected)
    report.to_csv('price_panel_search_report.csv', index=False)
    print('Saved price_panel_search_report.csv')

    if not best['covers_target']:
        raise ValueError(
            f"æ¾å°çæä½³æªæ¡ä»æªå®æ´è¦è 2022-2025ï¼{best['min_date']} -> {best['max_date']}ï¼è«è£å®æ´æ­·å²è³æ"
        )

    final = pd.read_csv(OUTPUT, nrows=5)
    print('\nprice_panel_daily.csv preview columns:')
    print(list(final.columns))
    print('\nSuccess: price_panel_daily.csv å·²æºåå®æï¼å¯ä¾åæ¸¬ä½¿ç¨')


if __name__ == '__main__':
    main()
