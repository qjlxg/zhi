import pandas as pd
import glob
from datetime import datetime

def logic_safety(df):
    df['ma60'] = df['close'].rolling(60).mean()
    df['ma250'] = df['close'].rolling(250).mean()
    # 形态A：草上飞（60日线极其平稳横盘）
    vol_std = df['volume'].iloc[-10:].std() / df['volume'].iloc[-10:].mean() < 0.2
    price_flat = abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.01
    is_grass = vol_std and price_flat
    
    # 形态B：年线支撑阳线
    is_no_loss = abs(df['low'].iloc[-1] - df['ma250'].iloc[-1]) / df['ma250'].iloc[-1] < 0.01 and df['close'].iloc[-1] > df['open'].iloc[-1]
    
    return is_grass, is_no_loss

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
            
            is_grass, is_no_loss = logic_safety(df)
            if is_grass or is_no_loss:
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name,
                    '价格': price, '形态': "草上飞" if is_grass else "年线支撑"
                })
        except: continue

    if results:
        pd.DataFrame(results).to_csv(f"alpha_safety_moat_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
        print(f"安全护城河完成！筛选后剩余: {len(results)} 只。")

if __name__ == "__main__":
    run()
