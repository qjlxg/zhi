import pandas as pd
import glob
from datetime import datetime

class AlphaLogics:
    @staticmethod
    def logic_limit_pullback(df):
        df['ma20'] = df['close'].rolling(20).mean()
        has_limit = (df['pct_chg'].iloc[-10:-1] > 9.5).any()
        is_support = df['close'].iloc[-1] >= df['ma20'].iloc[-1]
        vol_down = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
        return has_limit and is_support and vol_down

    @staticmethod
    def logic_double_plate(df):
        # 阴阳双板：前前日涨停 + 昨日回调 + 今日反包
        return df['pct_chg'].iloc[-3] > 9.5 and df['close'].iloc[-2] < df['open'].iloc[-2] and df['pct_chg'].iloc[-1] > 5

def run():
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    results = []
    
    for f in glob.glob('stock_data/*.csv'):
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','涨跌幅':'pct_chg'})
            if len(df) < 20: continue
            
            is_limit_pb = AlphaLogics.logic_limit_pullback(df)
            is_double = AlphaLogics.logic_double_plate(df)
            
            if is_limit_pb or is_double:
                code = str(df['code'].iloc[-1]).zfill(6)
                results.append({
                    '日期': df['date'].iloc[-1],
                    '股票代码': code,
                    '股票名称': name_map.get(code, "未知"),
                    '收盘价': df['close'].iloc[-1],
                    '形态': "涨停回调" if is_limit_pb else "阴阳双板"
                })
        except: continue

    if results:
        file_name = f"alpha_limit_assault_report_{datetime.now().strftime('%Y%m%d')}.csv"
        pd.DataFrame(results).to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"完成！涨停突击发现 {len(results)} 只标的。")

if __name__ == "__main__":
    run()
