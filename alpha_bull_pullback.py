import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime

# --- 核心逻辑库 ---
class AlphaLogics:
    @staticmethod
    def get_indicators(df):
        df = df.copy()
        for m in [5, 10, 20, 60]:
            df[f'ma{m}'] = df['close'].rolling(m).mean()
        return df

    @staticmethod
    def logic_duck_head(df):
        # 老鸭头：MA5/10金叉，且股价在MA20附近缩量
        ma_up = df['ma5'].iloc[-1] > df['ma10'].iloc[-1] > df['ma60'].iloc[-1]
        vol_shrink = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
        price_near = df['ma20'].iloc[-1] * 0.98 <= df['close'].iloc[-1] <= df['ma20'].iloc[-1] * 1.02
        return ma_up and vol_shrink and price_near

    @staticmethod
    def logic_horse_back(df):
        # 回马枪：近期有大涨，今日10日线精准支撑，缩量
        has_big_yang = df['pct_chg'].iloc[-5:-1].max() > 7
        near_ma10 = abs(df['low'].iloc[-1] - df['ma10'].iloc[-1]) / df['ma10'].iloc[-1] < 0.01
        return has_big_yang and near_ma10

def run():
    # 读取名称映射
    name_df = pd.read_csv('stock_names.csv', dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    
    results = []
    files = glob.glob('stock_data/*.csv')
    
    for f in files:
        try:
            df = pd.read_csv(f).rename(columns={'日期':'date','股票代码':'code','收盘':'close','成交量':'volume','最高':'high','最低':'low','涨跌幅':'pct_chg'})
            if len(df) < 60: continue
            
            df = AlphaLogics.get_indicators(df)
            
            is_duck = AlphaLogics.logic_duck_head(df)
            is_horse = AlphaLogics.logic_horse_back(df)
            
            if is_duck or is_horse:
                code = str(df['code'].iloc[-1]).zfill(6)
                results.append({
                    '日期': df['date'].iloc[-1],
                    '股票代码': code,
                    '股票名称': name_map.get(code, "未知"),
                    '收盘价': df['close'].iloc[-1],
                    '涨跌幅': df['pct_chg'].iloc[-1],
                    '形态': "老鸭头" if is_duck else "回马枪"
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results)
        file_name = f"alpha_bull_pullback_report_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"成功！牛回头脚本发现 {len(results)} 只标的。")

if __name__ == "__main__":
    run()
