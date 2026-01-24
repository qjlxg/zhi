import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def backtest_logic(file_path, name_map):
    try:
        df = pd.read_csv(file_path).rename(columns={'日期':'date','股票代码':'code','收盘':'close','开盘':'open','最高':'high','成交量':'volume','涨跌幅':'pct_chg','换手率':'turnover'})
        code = str(df['code'].iloc[-1]).zfill(6)
        name = name_map.get(code, "未知")
        if not (code.startswith('60') or code.startswith('00')) or 'ST' in name: return []

        # 指标预算
        df['ma20'] = df['close'].rolling(20).mean()
        df['diff'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
        df['h10'] = df['close'].rolling(10).max().shift(1)
        df['v5'] = df['volume'].rolling(5).mean().shift(1)

        trades = []
        for i in range(20, len(df)):
            # 战法判断 (逻辑同选股脚本)
            if (df['close'].iloc[i] >= df['h10'].iloc[i] and 5.0 <= df['pct_chg'].iloc[i] <= 9.6 and 
                df['volume'].iloc[i] > df['v5'].iloc[i] * 2.5 and 4.0 <= df['turnover'].iloc[i] <= 10.0 and
                df['diff'].iloc[i] > 0 and df['ma20'].iloc[i] > df['ma20'].iloc[i-1]):
                
                res = {'date': df['date'].iloc[i], 'code': code, 'name': name, 'buy_price': df['close'].iloc[i]}
                # 记录 1,3,5,10,20,30,60 天收益
                for d in [1, 3, 5, 10, 20, 30, 60]:
                    if i + d < len(df):
                        res[f'day_{d}_ret'] = round((df['close'].iloc[i+d] - res['buy_price']) / res['buy_price'] * 100, 2)
                trades.append(res)
        return trades
    except: return []

def run():
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    all_trades = []
    with ProcessPoolExecutor() as executor:
        for r in executor.map(backtest_logic, files, [name_map]*len(files)): all_trades.extend(r)
    
    if all_trades:
        df_ledger = pd.DataFrame(all_trades)
        os.makedirs('backtest_results', exist_ok=True)
        df_ledger.to_csv("backtest_results/strategy_3in1_ledger.csv", index=False, encoding='utf-8-sig')
        
        # 打印多维胜率表
        print("\n--- 精英战法：多维度回测胜率统计 ---")
        stats = []
        for d in [1, 3, 5, 10, 20, 30, 60]:
            col = f'day_{d}_ret'
            v = df_ledger[df_ledger[col].notnull()]
            stats.append({'周期': f'{d}天', '胜率': f'{(v[col]>0).mean():.2%}', '均收': f'{v[col].mean():.2f}%'})
        print(pd.DataFrame(stats))

if __name__ == "__main__":
    run()
