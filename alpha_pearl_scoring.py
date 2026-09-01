import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# --- 核心配置 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

def calculate_pearl_score(row):
    """
    基于东方明珠基因的评分逻辑
    """
    score = 0
    
    # 1. 成交额得分 (40分): 越接近或超过10亿分越高
    amt = row['amount_M']
    if amt >= 1000: score += 40
    elif amt >= 500: score += 30
    elif amt >= 300: score += 20
    else: score += 5
    
    # 2. RSI得分 (30分): 45-55是完美起爆区
    rsi = row['rsi_14']
    if 45 <= rsi <= 55: score += 30
    elif 40 <= rsi <= 65: score += 20
    elif rsi > 80: score += 5 # 太热扣分
    else: score += 10
    
    # 3. 乖离率得分 (20分): 3%-6%最安全
    bias = row['bias_ma5']
    if 2 <= bias <= 6: score += 20
    elif bias <= 8: score += 10
    elif bias > 10: score -= 10 # 追高风险扣分
    
    # 4. MACD位置得分 (10分): 0轴下方附近最强
    diff = row['diff']
    if -0.2 <= diff <= 0: score += 10
    elif diff < -0.5: score += 5
    
    return score

def analyze_stock(file_path, name_map):
    try:
        df = pd.read_csv(file_path).rename(columns={
            '日期': 'date', '股票代码': 'code', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount',
            '涨跌幅': 'pct_chg', '换手率': 'turnover'
        })
        if len(df) < 60: return None
        
        # 1. 核心指标计算
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd_val'] = (df['diff'] - df['dea']) * 2
        df['ma5'] = df['close'].rolling(5).mean()
        
        # RSI 计算
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['rsi_14'] = 100 - (100 / (1 + gain/loss))
        
        # 2. 触发逻辑：巨阳倍量 + 二次金叉
        idx = -1
        cond_3in1 = (df['pct_chg'].iloc[idx] > 5) and (df['volume'].iloc[idx] > df['volume'].iloc[idx-1] * 1.8)
        cond_bottom = (df['diff'].iloc[idx] < 0) and (df['diff'].iloc[idx-1] <= df['dea'].iloc[idx-1]) and (df['diff'].iloc[idx] > df['dea'].iloc[idx])
        
        if cond_3in1 and cond_bottom:
            code = str(df['code'].iloc[idx]).zfill(6)
            curr_price = df['close'].iloc[idx]
            
            row_data = {
                'date': df['date'].iloc[idx],
                'code': code,
                'name': name_map.get(code, "未知"),
                'price': curr_price,
                'amount_M': df['amount'].iloc[idx] / 1000000,
                'rsi_14': df['rsi_14'].iloc[idx],
                'bias_ma5': ((curr_price - df['ma5'].iloc[idx]) / df['ma5'].iloc[idx]) * 100,
                'diff': df['diff'].iloc[idx],
                'pct_chg': df['pct_chg'].iloc[idx]
            }
            
            # 计算得分
            row_data['pearl_score'] = calculate_pearl_score(row_data)
            row_data['stop_loss'] = round(df['open'].iloc[idx] + (curr_price - df['open'].iloc[idx]) * 0.5, 2)
            
            return row_data
    except: return None

def run():
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    results = []
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_stock, files, [name_map]*len(files)):
            if res: results.append(res)
            
    if results:
        res_df = pd.DataFrame(results)
        # 核心：按评分降序排列
        res_df = res_df.sort_values('pearl_score', ascending=False)
        
        # 格式化输出
        res_df['amount_M'] = res_df['amount_M'].round(2)
        res_df['rsi_14'] = res_df['rsi_14'].round(2)
        res_df['bias_ma5'] = res_df['bias_ma5'].round(2)
        
        file_name = f"alpha_pearl_scoring_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"评分完成！今日最高分标的：{res_df.iloc[0]['name']} ({res_df.iloc[0]['pearl_score']}分)")
        print(f"已生成评分榜单: {file_name}")
    else:
        print("今日未发现符合反转逻辑的标的。")

if __name__ == "__main__":
    run()
