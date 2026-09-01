import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# --- 核心配置 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
STRATEGY_NAME = 'alpha_3in1_bottom_macd'

def is_main_board_sh_sz(code):
    code = str(code).zfill(6)
    if code.startswith('30') or code.startswith('68'): return False
    return code.startswith('60') or code.startswith('00')

def analyze_single_stock(file_path, name_map):
    try:
        df = pd.read_csv(file_path).rename(columns={
            '日期': 'date', '股票代码': 'code', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount',
            '涨跌幅': 'pct_chg', '换手率': 'turnover'
        })
        if len(df) < 120: return None
        
        # --- 1. 计算 Alpha 指标 ---
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd_val'] = (df['diff'] - df['dea']) * 2
        
        # 复盘辅助指标
        for m in [5, 20, 60]: df[f'ma{m}'] = df['close'].rolling(m).mean()
        
        # --- 2. 逻辑判定 ---
        
        # A. logic_three_in_one: 巨阳 + 倍量 + MACD柱增长
        cond_3in1 = (df['pct_chg'].iloc[-1] > 5) and \
                    (df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.8) and \
                    (df['macd_val'].iloc[-1] > df['macd_val'].iloc[-2])
        
        # B. logic_macd_bottom: 0轴下二次金叉 (昨日死叉状态/未金叉，今日金叉)
        cond_bottom = (df['diff'].iloc[-1] < 0) and \
                      (df['diff'].iloc[-2] <= df['dea'].iloc[-2]) and \
                      (df['diff'].iloc[-1] > df['dea'].iloc[-1])
        
        # 基础过滤
        code = str(df['code'].iloc[-1]).zfill(6)
        if not is_main_board_sh_sz(code): return None
        stock_name = name_map.get(code, "未知")
        if any(x in stock_name for x in ['ST', '*', '退']): return None

        # --- 3. 合并触发 ---
        if cond_3in1 and cond_bottom:
            curr_price = df['close'].iloc[-1]
            entity = curr_price - df['open'].iloc[-1]
            
            # 复盘数据补充
            ma5 = df['ma5'].iloc[-1]
            bias_5 = round((curr_price - ma5) / ma5 * 100, 2)
            
            return {
                'date': df['date'].iloc[-1],
                'code': code,
                'name': stock_name,
                'price': curr_price,
                'stop_loss': round(df['open'].iloc[-1] + (entity * 0.5), 2),
                'pct_chg': df['pct_chg'].iloc[-1],
                'amount_M': round(df['amount'].iloc[-1] / 1000000, 2),
                'turnover': df['turnover'].iloc[-1],
                'bias_ma5': bias_5,
                'diff': round(df['diff'].iloc[-1], 3)
            }
    except: return None

def run():
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = {str(c).zfill(6): n for c, n in zip(name_df['code'], name_df['name'])}

    files = glob.glob(f"{DATA_DIR}/*.csv")
    results = []
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_single_stock, files, [name_map]*len(files)):
            if res: results.append(res)

    if results:
        res_df = pd.DataFrame(results).sort_values('amount_M', ascending=False)
        now = datetime.now()
        dir_path = f"results/{now.strftime('%Y-%m')}"
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        res_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"成功！发现 {len(res_df)} 个‘底部起爆’信号，已存入 {file_path}")
    else:
        print("今日未发现符合‘底部二次金叉+倍量巨阳’的标的")

if __name__ == "__main__": run()
