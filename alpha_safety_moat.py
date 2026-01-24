import pandas as pd
import glob
from datetime import datetime

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','最低':'low'})
            if len(df) < 250: continue
            
            code = str(df['code'].iloc[-1]).zfill(6)
            name = name_map.get(code, "未知")
            price = df['close'].iloc[-1]
            
            if not (5.0 <= price <= 20.0): continue
            if not (code.startswith('00') or code.startswith('60')): continue
            if 'ST' in name.upper() or '退' in name: continue
            
            df['ma60'] = df['close'].rolling(60).mean()
            df['ma250'] = df['close'].rolling(250).mean()
            
            # 草上飞逻辑
            is_grass = (df['volume'].iloc[-10:].std() / df['volume'].iloc[-10:].mean() < 0.2) and \
                       (abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.01)
            # 年线支撑逻辑
            is_no_loss = (abs(df['low'].iloc[-1] - df['ma250'].iloc[-1]) / df['ma250'].iloc[-1] < 0.01) and \
                         (df['close'].iloc[-1] > df['open'].iloc[-1])
            
            if is_grass or is_no_loss:
                score = (1 if is_grass else 0) + (1 if is_no_loss else 0)
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name, '价格': price,
                    '形态汇总': ("草上飞+年线支撑" if score==2 else ("草上飞" if is_grass else "年线支撑")),
                    '共振得分': score
                })
        except: continue

    if results:
        # 按得分降序排列，得分2的（双重命中）会排在最上面
        res_df = pd.DataFrame(results).sort_values(by='共振得分', ascending=False)
        res_df.to_csv(f"alpha_safety_moat_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
