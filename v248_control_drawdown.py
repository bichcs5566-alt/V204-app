import pandas as pd
import numpy as np

TOP_N = 6
MAX_GROSS_EXPOSURE = 0.9
MIN_ACTIVE_EXPOSURE = 0.35

STOP_LOSS_BASE = -0.06
STOP_LOSS_STRONG = -0.09

ADD_THRESHOLD = 0.04
ADD_SIZE = 0.3

def load_data():
    df = pd.read_csv("price_panel_daily.csv")
    df['date'] = pd.to_datetime(df['date'])
    return df

def compute_features(df):
    df = df.sort_values(['symbol','date'])
    df['ret'] = df.groupby('symbol')['close'].pct_change()
    df['mom5'] = df.groupby('symbol')['close'].pct_change(5)
    df['mom10'] = df.groupby('symbol')['close'].pct_change(10)
    df['mom20'] = df.groupby('symbol')['close'].pct_change(20)
    df['score'] = df['mom20']*0.45 + df['mom10']*0.35 + df['mom5']*0.20
    return df

def run_backtest(df):
    dates = sorted(df['date'].unique())
    capital = 1.0
    nav = []
    positions = {}

    for d in dates:
        day = df[df['date']==d].copy()
        day = day.dropna(subset=['score'])

        # exit
        for s in list(positions.keys()):
            row = day[day['symbol']==s]
            if row.empty:
                continue
            r = row['ret'].values[0]
            pnl = r * positions[s]['weight']
            positions[s]['nav'] *= (1 + pnl)

            stop = STOP_LOSS_STRONG if positions[s]['score'] > 0 else STOP_LOSS_BASE
            if pnl < stop:
                del positions[s]

        # entry
        ranked = day.sort_values('score', ascending=False).head(TOP_N)
        total_weight = sum([p['weight'] for p in positions.values()])

        for _, row in ranked.iterrows():
            if row['symbol'] in positions:
                continue
            if total_weight < MAX_GROSS_EXPOSURE:
                w = MIN_ACTIVE_EXPOSURE / TOP_N
                positions[row['symbol']] = {
                    'weight': w,
                    'nav': 1.0,
                    'score': row['score']
                }

        # add-on
        for s in positions:
            row = day[day['symbol']==s]
            if row.empty:
                continue
            if row['ret'].values[0] > ADD_THRESHOLD:
                positions[s]['weight'] *= (1 + ADD_SIZE)

        # compute nav
        daily_ret = sum([p['nav']*p['weight'] for p in positions.values()])
        capital = capital * (1 + daily_ret)
        nav.append({'date': d, 'nav': capital})

    nav_df = pd.DataFrame(nav)
    return nav_df

def summary(nav_df):
    nav_df['ret'] = nav_df['nav'].pct_change().fillna(0)
    total_return = nav_df['nav'].iloc[-1] - 1
    mdd = (nav_df['nav']/nav_df['nav'].cummax() -1).min()
    sharpe = nav_df['ret'].mean()/nav_df['ret'].std()*np.sqrt(252)
    return pd.DataFrame([{
        'return': total_return,
        'mdd': mdd,
        'sharpe_daily': sharpe
    }])

if __name__ == "__main__":
    df = load_data()
    df = compute_features(df)
    nav = run_backtest(df)
    nav.to_csv("full_nav.csv", index=False)
    summary(nav).to_csv("full_summary.csv", index=False)
