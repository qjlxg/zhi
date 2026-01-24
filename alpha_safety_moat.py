import pandas as pd
import glob
from datetime import datetime

class AlphaLogics:
    @staticmethod
    def logic_grass_fly(df):
        df['ma60'] = df['close'].rolling(60).mean()
        vol_std = df['volume'].iloc[-10:].std() / df['volume'].iloc[-10:].mean() < 0.2
        price_flat = abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.01
        return vol_std and price_flat

    @staticmethod
    def logic_no_loss(df):
        df['ma250'] = df['close'].rolling(250).mean()
        return abs(df['low'].iloc[-1] - df['ma250'].iloc[-1]) / df['ma250'].iloc[-1] < 0.01 and df['close'].iloc[-1] > df['open'].iloc[-1]

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','最低':'low'})
            if len(df) < 250: continue
            
            is_grass = AlphaLogics.logic_grass_fly(df)
            is_no_loss = AlphaLogics.logic_no_loss(df)
            
            if is_grass or is_no_loss:
                code = str(df['code'].iloc[-1]).zfill(6)
                results.append({
                    '日期': df['date'].iloc[-1],
                    '股票代码': code,
                    '股票名称': name_map.get(code, "未知"),
                    '收盘价': df['close'].iloc[-1],
                    '形态': "草上飞" if is_grass else "年线支撑"
                })
        except: continue

    if results:
        file_name = f"alpha_safety_moat_report_{datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(results).to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"完成！安全边际脚本发现 {len(results)} 只标的。")

if __name__ == "__main__":
    run()
