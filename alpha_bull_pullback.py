import pandas as pd
import glob
from datetime import datetime

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','最高':'high','最低':'low','涨跌幅':'pct_chg'})
            if len(df) < 60: continue
            
            code = str(df['code'].iloc[-1]).zfill(6)
            name = name_map.get(code, "未知")
            price = df['close'].iloc[-1]
            
            # --- 硬性过滤 ---
            if not (5.0 <= price <= 20.0): continue
            if not (code.startswith('00') or code.startswith('60')): continue
            if 'ST' in name.upper() or '退' in name: continue
            
            # --- 指标计算 ---
            for m in [5, 10, 20, 60]: df[f'ma{m}'] = df['close'].rolling(m).mean()
            
            # --- 两个形态逻辑 ---
            is_duck = (df['ma5'].iloc[-1] > df['ma10'].iloc[-1] > df['ma60'].iloc[-1]) and \
                      (df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]) and \
                      (abs(df['close'].iloc[-1] - df['ma20'].iloc[-1]) / df['ma20'].iloc[-1] < 0.02)
            
            is_horse = (df['pct_chg'].iloc[-5:-1].max() > 7) and \
                       (abs(df['low'].iloc[-1] - df['ma10'].iloc[-1]) / df['ma10'].iloc[-1] < 0.01)
            
            if is_duck or is_horse:
                score = (1 if is_duck else 0) + (1 if is_horse else 0)
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name, '价格': price,
                    '形态汇总': ("老鸭头+回马枪" if score==2 else ("老鸭头" if is_duck else "回马枪")),
                    '共振得分': score
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results).sort_values(by='共振得分', ascending=False)
        res_df.to_csv(f"alpha_bull_pullback_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
