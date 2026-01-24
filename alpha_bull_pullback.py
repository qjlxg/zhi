import pandas as pd
import numpy as np
import glob
from datetime import datetime

def logic_bull_pullback(df):
    # 计算均线
    for m in [5, 10, 20, 60]:
        df[f'ma{m}'] = df['close'].rolling(m).mean()
    
    # 形态A：老鸭头（均线多头+缩量回踩20线）
    ma_up = df['ma5'].iloc[-1] > df['ma10'].iloc[-1] > df['ma60'].iloc[-1]
    vol_shrink = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
    price_near_20 = abs(df['close'].iloc[-1] - df['ma20'].iloc[-1]) / df['ma20'].iloc[-1] < 0.02
    is_duck = ma_up and vol_shrink and price_near_20
    
    # 形态B：回马枪（近期大涨+精准踩10线）
    has_big_yang = df['pct_chg'].iloc[-5:-1].max() > 7
    near_ma10 = abs(df['low'].iloc[-1] - df['ma10'].iloc[-1]) / df['ma10'].iloc[-1] < 0.01
    is_horse = has_big_yang and near_ma10
    
    return is_duck, is_horse

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','收盘':'close','成交量':'volume','最高':'high','最低':'low','涨跌幅':'pct_chg'})
            if len(df) < 60: continue
            
            # --- 严格过滤条件 ---
            code = str(df['code'].iloc[-1]).zfill(6)
            name = name_map.get(code, "未知")
            price = df['close'].iloc[-1]
            
            if not (5.0 <= price <= 20.0): continue  # 价格过滤
            if not (code.startswith('00') or code.startswith('60')): continue # 仅限主板
            if 'ST' in name.upper() or '退' in name: continue # 排除ST/退市
            
            is_duck, is_horse = logic_bull_pullback(df)
            if is_duck or is_horse:
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name,
                    '价格': price, '涨幅': df['pct_chg'].iloc[-1],
                    '形态': "老鸭头" if is_duck else "回马枪"
                })
        except: continue

    if results:
        pd.DataFrame(results).to_csv(f"alpha_bull_pullback_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
        print(f"牛回头脚本完成！筛选后剩余: {len(results)} 只。")

if __name__ == "__main__":
    run()
