# v221_strategy.py

import pandas as pd
import numpy as np

def compute_exposure(row):
    r10 = row['rolling_ret_10']
    r20 = row['rolling_ret_20']

    if r10 > 0 and r20 > 0:
        regime = 'strong_bull'
    elif r10 > 0 or r20 > 0:
        regime = 'weak_bull'
    elif r10 < 0 and r20 < 0:
        regime = 'bear'
    else:
        regime = 'neutral'

    strength = (r10 if not np.isnan(r10) else 0) + (r20 if not np.isnan(r20) else 0)

    if regime == 'strong_bull':
        return 0.8 if strength > 0.02 else 0.6
    elif regime == 'weak_bull':
        return 0.5
    elif regime == 'neutral':
        return 0.3
    else:
        return 0.1

def apply_strategy(df):
    df = df.copy()
    df['exposure'] = df.apply(compute_exposure, axis=1)
    df['adj_ret'] = df['raw_ret'] * df['exposure']

    df['nav'] = (1 + df['adj_ret']).cumprod() * 100000
    df['cum_return'] = df['nav'] / 100000 - 1

    df['peak'] = df['nav'].cummax()
    df['drawdown'] = (df['nav'] - df['peak']) / df['peak']

    df.loc[df['drawdown'] < -0.03, 'exposure'] = 0.2

    return df

def summary(df):
    total_return = df['cum_return'].iloc[-1]
    ann_return = (1 + total_return) ** (252 / len(df)) - 1
    sharpe = df['adj_ret'].mean() / df['adj_ret'].std() * np.sqrt(252)
    mdd = df['drawdown'].min()

    return {
        "total_return": total_return,
        "ann_return": ann_return,
        "sharpe": sharpe,
        "mdd": mdd
    }
