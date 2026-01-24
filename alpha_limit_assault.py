import pandas as pd
import glob
from datetime import datetime

def logic_assault(df):
    df['ma20'] = df['close'].rolling(20).mean()
    # 形态A：涨停回调（10天内有涨停，回踩20线不破）
    has_limit = (df['pct_chg'].iloc[-10:-1] > 9.5).any()
    is_support = df['close'].iloc[-1] >= df['ma20'].iloc[-1]
    vol_down = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
    is_limit_pb = has_limit and is_support and vol_down
    
    # 形态B：阴阳双板（反包）
    is_double = df['pct_chg'].iloc[-3] > 9.5 and df['close'].iloc[-2] < df['open'].iloc[-2] and df['pct_chg'].iloc[-1] > 5
    
    return is_limit_pb, is_double

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
            
            is_limit_pb, is_double = logic_assault(df)
            if is_limit_pb or is_double:
                results.append({
                    '日期': df['date'].iloc[-1], '代码': code, '名称': name,
                    '价格': price, '形态': "涨停回调" if is_limit_pb else "阴阳双板"
                })
        except: continue

    if results:
        pd.DataFrame(results).to_csv(f"alpha_limit_assault_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False, encoding='utf-8-sig')
        print(f"涨停突击脚本完成！筛选后剩余: {len(results)} 只。")

if __name__ == "__main__":
    run()
