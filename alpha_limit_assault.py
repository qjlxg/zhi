import pandas as pd
import glob
from datetime import datetime

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','涨跌幅':'pct_chg'})
            code = str(df['code'].iloc[-1]).zfill(6)
            name = name_map.get(code, "未知")
            price = df['close'].iloc[-1]
            
            if not (5.0 <= price <= 20.0): continue
            if not (code.startswith('00') or code.startswith('60')): continue
            if 'ST' in name.upper() or '退' in name: continue
            
            df['ma20'] = df['close'].rolling(20).mean()
            is_limit_pb = (df['pct_chg'].iloc[-10:-1] > 9.5).any() and (df['close'].iloc[-1] >= df['ma20'].iloc[-1]) and (df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1])
            is_double = (df['pct_chg'].iloc[-3] > 9.5) and (df['close'].iloc[-2] < df['open'].iloc[-2]) and (df['pct_chg'].iloc[-1] > 5)
            
            if is_limit_pb or is_double:
                score = (1 if is_limit_pb else 0) + (1 if is_double else 0)
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name, '价格': price,
                    '形态汇总': ("回调+反包" if score==2 else ("涨停回调" if is_limit_pb else "阴阳双板")),
                    '共振得分': score
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results).sort_values(by='共振得分', ascending=False)
        res_df.to_csv(f"alpha_limit_assault_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
