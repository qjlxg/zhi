import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

# --- 配置区 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_AMOUNT_M = 300   # 门槛1：日成交额必须大于3亿
MAX_BIAS_5 = 8.0     # 门槛2：5日乖离率不能超过8% (太热不进)

def backtest_logic_pro(file_path, name_map):
    try:
        df = pd.read_csv(file_path).rename(columns={
            '日期':'date','股票代码':'code','收盘':'close','开盘':'open',
            '最高':'high','最低':'low','成交量':'volume','成交额':'amount',
            '涨跌幅':'pct_chg','换手率':'turnover'
        })
        if len(df) < 120: return []
        
        code = str(df['code'].iloc[-1]).zfill(6)
        name = name_map.get(code, "未知")
        if not (code.startswith('60') or code.startswith('00')) or 'ST' in name: return []
        
        # 指标计算
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd_val'] = (df['diff'] - df['dea']) * 2
        df['ma5'] = df['close'].rolling(5).mean()

        results = []
        for i in range(30, len(df)):
            # 基础逻辑：0轴下二次金叉 + 巨阳倍量
            c_macd = (df['diff'].iloc[i] < 0) and (df['diff'].iloc[i-1] <= df['dea'].iloc[i-1]) and (df['diff'].iloc[i] > df['dea'].iloc[i])
            c_3in1 = (df['pct_chg'].iloc[i] > 5) and (df['volume'].iloc[i] > df['volume'].iloc[i-1] * 1.8)
            
            # --- 新增：量能与乖离率硬过滤 ---
            amt_m = df['amount'].iloc[i] / 1000000
            bias_5 = ((df['close'].iloc[i] - df['ma5'].iloc[i]) / df['ma5'].iloc[i]) * 100
            
            if c_macd and c_3in1 and (amt_m >= MIN_AMOUNT_M) and (bias_5 <= MAX_BIAS_5):
                price = df['close'].iloc[i]
                res = {'date': df['date'].iloc[i], 'code': code, 'name': name, 'price': price}
                for d in [1, 3, 5, 10]:
                    if i + d < len(df):
                        res[f'day_{d}_ret'] = round((df['close'].iloc[i+d] - price) / price * 100, 2)
                results.append(res)
        return results
    except: return []

def run():
    print(f"开始执行【PRO版·底部反转】回测 (过滤条件：成交额>{MIN_AMOUNT_M}M, 乖离<{MAX_BIAS_5}%)")
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    full_h = []
    with ProcessPoolExecutor() as executor:
        for h in executor.map(backtest_logic_pro, files, [name_map]*len(files)):
            full_h.extend(h)
    
    if full_h:
        df_h = pd.DataFrame(full_h)
        stats = []
        for d in [1, 3, 5, 10]:
            col = f'day_{d}_ret'
            v = df_h[df_h[col].notnull()][col]
            if not v.empty:
                stats.append({
                    '周期': f'{d}天', '样本': len(v), '胜率': f'{(v>0).mean():.1%}',
                    '盈亏比': f'{(v[v>0].mean()/abs(v[v<=0].mean())):.2f}',
                    '平均盈%': f'{v.mean():.2f}', '极损%': f'{v.min():.2f}'
                })
        print("\n" + "="*80 + "\n【PRO版：底部反转多维回测结果】\n" + "="*80)
        print(pd.DataFrame(stats).to_string(index=False))

if __name__ == "__main__":
    run()
