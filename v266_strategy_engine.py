"""
v266_strategy_engine.py
只讀 feature_panel_daily.csv 的策略引擎。
資料未通過 v266_data_guard.py 時，workflow 會先停止，不會產生假名單。
"""
from pathlib import Path
from datetime import datetime
import json
import numpy as np
import pandas as pd

ROOT = Path('.')
DATA_DIR = ROOT / 'mobile_dashboard_v1' / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)
INITIAL_CAPITAL = 1_000_000


def price_tier(p):
    p = float(p)
    if p < 50: return '50以下'
    if p < 100: return '50-100'
    if p < 300: return '100-300'
    if p < 500: return '300-500'
    if p < 1000: return '500-1000'
    return '1000以上'


def next_trade_date(signal_date):
    d = pd.to_datetime(signal_date) + pd.Timedelta(days=1)
    if d.weekday() == 5: d += pd.Timedelta(days=2)
    elif d.weekday() == 6: d += pd.Timedelta(days=1)
    return d


def write_both(df, name):
    df.to_csv(ROOT/name, index=False, encoding='utf-8')
    df.to_csv(DATA_DIR/name, index=False, encoding='utf-8')


def load_feature():
    p = ROOT/'feature_panel_daily.csv'
    if not p.exists() or p.stat().st_size == 0:
        p = DATA_DIR/'feature_panel_daily.csv'
    if not p.exists() or p.stat().st_size == 0:
        raise FileNotFoundError('feature_panel_daily.csv not found')
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['stock_id'] = df['stock_id'].astype(str).str.zfill(4)
    return df


def latest_valid(df):
    latest_date = df['date'].max()
    x = df[(df['date'] == latest_date) & (df['has_60d_history'].astype(str).str.lower().isin(['true','1']))].copy()
    numeric = ['open','high','low','close','volume','mom3','mom5','mom10','mom20','mom60','ma5','ma10','ma20','ma60','vol20','volume_ratio','vol_dry_ratio','high_20','low_20','high_60','low_60','range_20','ma_converge_pct','ma20_slope','kd_cross','macd_cross','macd_diff','obv_mom5','obv_up_count_5','low_non_down_count_5']
    for c in numeric:
        x[c] = pd.to_numeric(x.get(c), errors='coerce')
    x = x.dropna(subset=['close','mom20','ma20','ma60'])
    return latest_date, x


def detect_regime(x):
    pct_ma20 = float((x['close'] >= x['ma20']).mean())
    pct_ma60 = float((x['close'] >= x['ma60']).mean())
    pct_mom20 = float((x['mom20'] > 0).mean())
    pct_strong = float(((x['mom20'] > 0.08) & (x['close'] >= x['high_60'] * 0.92)).mean())
    med_mom20 = float(x['mom20'].median())
    score = int(pct_ma20 >= .55) + int(pct_ma60 >= .50) + int(pct_mom20 >= .50) + int(pct_strong >= .08) + int(med_mom20 > .015)
    if pct_ma60 < .35 and pct_mom20 < .35:
        regime = 'BEAR'
    elif score >= 4:
        regime = 'TREND'
    else:
        regime = 'RANGE'
    return regime, {'pct_above_ma20': round(pct_ma20,4), 'pct_above_ma60': round(pct_ma60,4), 'pct_mom20_pos': round(pct_mom20,4), 'pct_strong': round(pct_strong,4), 'median_mom20': round(med_mom20,4), 'regime_score': score}


def set_action(df, buy, test, watch, buy_sub, test_sub, watch_sub):
    df['action'] = 'SKIP'
    df.loc[watch, 'action'] = 'WATCH'
    df.loc[test, 'action'] = 'TEST'
    df.loc[buy, 'action'] = 'BUY'
    df['action_label'] = df['action'].map({'BUY':'買進','TEST':'試單','WATCH':'觀察','SKIP':'排除'}).fillna('排除')
    df['action_sub'] = '條件不足'
    df.loc[df['action']=='BUY','action_sub'] = buy_sub
    df.loc[df['action']=='TEST','action_sub'] = test_sub
    df.loc[df['action']=='WATCH','action_sub'] = watch_sub


def core_engine(x):
    d = x.copy()
    d['strategy_type'] = 'CORE'
    d['entry_score'] = 0.0
    d['entry_score'] += (d['mom10'] > .04).astype(int) * 12
    d['entry_score'] += (d['mom20'] > .08).astype(int) * 18
    d['entry_score'] += (d['mom60'] > .10).astype(int) * 8
    d['entry_score'] += (d['close'] > d['ma20']).astype(int) * 10
    d['entry_score'] += (d['ma20'] > d['ma60']).astype(int) * 10
    d['entry_score'] += (d['close'] >= d['high_60'] * .90).astype(int) * 8
    d['entry_score'] += d['volume_ratio'].between(1.1, 5.0).astype(int) * 8
    d['entry_score'] += (d['close'] >= 30).astype(int) * 6
    d['entry_score'] += (d['close'] >= 50).astype(int) * 4
    d['entry_score'] -= (d['close'] < 20).astype(int) * 14
    d['entry_score'] -= (d['mom20'] > .45).astype(int) * 10
    buy = (d['entry_score'] >= 54) & (d['mom20'] > .06) & (d['close'] > d['ma20']) & (d['close'] >= 30)
    test = (d['entry_score'] >= 42) & ~buy & (d['mom10'] > .015) & (d['close'] > d['ma20']*.98)
    watch = (d['entry_score'] >= 32) & ~buy & ~test
    set_action(d, buy, test, watch, '強勢主攻', '強勢試單', '強勢觀察')
    d['note'] = np.where(d['strategy_type']=='CORE', '強勢引擎｜20日動能｜均線多頭｜量能確認', '')
    return d.sort_values(['entry_score','mom20','mom10'], ascending=False)


def alpha_engine(x):
    d = x.copy()
    d['strategy_type'] = 'ALPHA'
    d['entry_score'] = 0.0
    d['entry_score'] += (d['mom3'] > 0).astype(int) * 6
    d['entry_score'] += (d['mom5'] > 0).astype(int) * 8
    d['entry_score'] += d['mom20'].between(-.08,.08).astype(int) * 8
    d['entry_score'] += (d['close'] >= d['ma20']*.97).astype(int) * 8
    d['entry_score'] += (d['ma_converge_pct'] <= .10).astype(int) * 8
    d['entry_score'] += (d['range_20'] <= .28).astype(int) * 6
    d['entry_score'] += d['volume_ratio'].between(1.0,3.5).astype(int) * 8
    d['entry_score'] += (d['low_non_down_count_5'] >= 3).astype(int) * 6
    d['entry_score'] -= (d['close'] < 8).astype(int) * 12
    d['entry_score'] -= (d['mom20'] > .25).astype(int) * 8
    buy = (d['entry_score'] >= 56) & (d['close'] > d['ma20']) & (d['mom5'] > .025) & (d['volume_ratio'] >= 1.3)
    test = (d['entry_score'] >= 44) & ~buy & ((d['mom5'] > 0) | (d['kd_cross'] == 1) | (d['macd_cross'] == 1))
    watch = (d['entry_score'] >= 34) & ~buy & ~test
    set_action(d, buy, test, watch, '反轉確認', '小倉試單', '反轉觀察')
    d['note'] = '反轉引擎｜靠近MA20｜均線收斂｜量能回溫'
    return d.sort_values(['entry_score','mom5','volume_ratio'], ascending=False)


def build_trade_plan(core, alpha, regime, signal_date):
    if regime == 'TREND':
        parts = [core[core.action=='BUY'].head(8), core[core.action=='TEST'].head(6), alpha[alpha.action.isin(['BUY','TEST'])].head(3)]
    elif regime == 'BEAR':
        parts = [core[core.action=='TEST'].head(2), alpha[alpha.action.isin(['BUY','TEST'])].head(8), alpha[alpha.action=='WATCH'].head(8)]
    else:
        parts = [core[core.action=='BUY'].head(5), core[core.action=='TEST'].head(5), alpha[alpha.action.isin(['BUY','TEST'])].head(8), alpha[alpha.action=='WATCH'].head(6)]
    s = pd.concat(parts, ignore_index=True)
    if s.empty:
        s = alpha.head(8).copy()
        s['action'] = 'WATCH'; s['action_label'] = '觀察'; s['action_sub'] = '低分觀察，不進場'
    s['priority'] = np.where(s['strategy_type']=='CORE',1,2)
    s = s.sort_values(['priority','entry_score'], ascending=[True,False]).drop_duplicates('stock_id').head(32)
    trade_date = next_trade_date(signal_date)
    rows = []
    for _, r in s.iterrows():
        px = float(r['close']) * 1.001
        action = r['action']; st = r['strategy_type']; score = float(r['entry_score'])
        if action == 'BUY' and st == 'CORE': w = .02 if score >= 65 else .01
        elif action == 'BUY': w = .008
        elif action == 'TEST': w = .005
        else: w = 0
        amount = INITIAL_CAPITAL*w
        shares = amount/px if px > 0 else 0
        rows.append({'signal_date':str(signal_date.date()),'trade_date':str(trade_date.date()),'market_regime':regime,'strategy_type':st,'action':action,'action_label':r['action_label'],'action_sub':r['action_sub'],'stock_id':r['stock_id'],'price_tier':price_tier(px),'ref_price':round(px,2),'target_weight':round(w,4),'suggested_amount':round(amount,0),'suggested_shares':round(shares,2),'estimated_total_cost':round(shares*px*1.0015,2),'entry_score':round(score,2),'source':'V266','note':r['note']})
    return pd.DataFrame(rows)


def main():
    df = load_feature()
    signal_date, latest = latest_valid(df)
    regime, info = detect_regime(latest)
    core = core_engine(latest).head(30)
    alpha = alpha_engine(latest).head(30)
    plan = build_trade_plan(core, alpha, regime, signal_date)
    debug = pd.DataFrame([{'generated_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'market_regime':regime,**info,'latest_stock_count':len(latest),'core_count':len(core),'alpha_count':len(alpha),'trade_plan_count':len(plan),'trade_buy_count':int((plan.action=='BUY').sum()),'trade_test_count':int((plan.action=='TEST').sum()),'trade_watch_count':int((plan.action=='WATCH').sum()),'core_max_score':float(core.entry_score.max()) if len(core) else 0,'alpha_max_score':float(alpha.entry_score.max()) if len(alpha) else 0}])
    write_both(core,'core_candidates.csv'); write_both(alpha,'alpha_candidates.csv'); write_both(pd.concat([core.assign(engine='CORE'),alpha.assign(engine='ALPHA')]),'candidates.csv'); write_both(plan,'trade_plan.csv'); write_both(debug,'selection_debug.csv')
    meta = {'generated_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'source':'v266_strategy_engine','signal_date':str(signal_date.date()),'trade_date':str(next_trade_date(signal_date).date()),'data_state':'fresh','market_regime':regime,'regime_info':info,'trade_plan_count':len(plan),'buy_count':int((plan.action=='BUY').sum()),'test_count':int((plan.action=='TEST').sum()),'watch_count':int((plan.action=='WATCH').sum())}
    for p in [ROOT/'meta.json', DATA_DIR/'meta.json']:
        with open(p,'w',encoding='utf-8') as f: json.dump(meta,f,ensure_ascii=False,indent=2)
    print(json.dumps(meta, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
