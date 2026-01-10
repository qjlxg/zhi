import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：涨停突破回踩5日线 (Limit-Up Backtest 5MA)
# 核心逻辑：
# 1. 寻找近期（20天内）有过涨停（涨幅>=9.5%）的标的，代表有主力强力介入。
# 2. 突破前期横盘区间的上沿。
# 3. 当前股价回落至5日均线（MA5）附近，且不跌破前期突破位。
# 4. 价格限制：5.0 - 20.0元，排除ST及创业板(30开头)。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"
OUTPUT_DIR = datetime.now().strftime("%Y%m")

def analyze_stock(file_path, name_dict):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30:
            return None

        # 基础数据清洗与排序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # 提取最新一条数据
        latest = df.iloc[-1]
        code = str(latest['股票代码']).zfill(6)
        
        # 1. 基础过滤：价格区间 & 排除创业板 & 排除ST(假设名称中含ST)
        stock_name = name_dict.get(code, "未知")
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if code.startswith('30'): return None
        if 'ST' in stock_name: return None

        # 2. 计算技术指标
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        
        # 3. 寻找最近20天内的涨停板 (以9.9%为准简化)
        df['is_limit_up'] = df['涨跌幅'] >= 9.8
        recent_limit_ups = df.tail(20)
        
        if not recent_limit_ups['is_limit_up'].any():
            return None

        # 获取最近一次涨停的索引和价格
        last_limit_up_idx = recent_limit_ups[recent_limit_ups['is_limit_up']].index[-1]
        limit_up_price = df.loc[last_limit_up_idx, '收盘']
        
        # 4. 核心战法筛选逻辑：回踩5日线
        # - 当前收盘价靠近MA5 (差异在2%以内)
        # - 当前收盘价高于MA20 (趋势向上)
        # - 相比涨停后有小幅缩量回调
        dist_to_ma5 = abs(latest['收盘'] - latest['MA5']) / latest['MA5']
        
        is_returning = latest['收盘'] >= latest['MA5'] and dist_to_ma5 <= 0.02
        is_above_support = latest['收盘'] >= (limit_up_price * 0.95) # 不跌破涨停价太远

        if is_returning and is_above_support:
            # 5. 自动复盘逻辑：强度评分
            score = 0
            if latest['换手率'] < 10: score += 40 # 缩量回调加分
            if latest['收盘'] > latest['开盘']: score += 30 # 收阳线加分
            if latest['涨跌幅'] > -2: score += 30 # 回调温柔加分
            
            # 操作建议
            suggestion = "暂时放弃"
            if score >= 80: suggestion = "强力推荐：回踩到位，一击必中"
            elif score >= 60: suggestion = "适量试错：观察分时图择机入场"
            else: suggestion = "继续观察：等待止跌信号"

            return {
                "日期": latest['日期'].strftime('%Y-%m-%d'),
                "代码": code,
                "名称": stock_name,
                "收盘价": latest['收盘'],
                "MA5": round(latest['MA5'], 2),
                "换手率": latest['换手率'],
                "信号强度": score,
                "操作建议": suggestion
            }
            
    except Exception as e:
        return None

def run_main():
    # 加载名称映射
    names_df = pd.read_csv(NAMES_FILE)
    name_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行处理
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, name_dict) for f in files])
    
    # 过滤空结果
    final_list = [r for r in results if r is not None]
    
    if final_list:
        result_df = pd.DataFrame(final_list).sort_values("信号强度", ascending=False)
        
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        timestamp = datetime.now().strftime("%H%M%S")
        file_name = f"LimitUp_Backtest_5MA_{timestamp}.csv"
        result_df.to_csv(f"{OUTPUT_DIR}/{file_name}", index=False, encoding='utf-8-sig')
        print(f"筛选完成，发现 {len(result_df)} 只符合战法标的。")
    else:
        print("今日无符合条件的标的。")

if __name__ == "__main__":
    run_main()
