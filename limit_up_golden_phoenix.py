import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：涨停金凤凰 (Limit Up Golden Phoenix)
# 核心逻辑：
# 1. 启动：近期必须有一个放量涨停板（涨幅 > 9.5%）。
# 2. 支撑：后续调整过程中，收盘价始终不跌破该涨停板的最高价（或误差2%内）。
# 3. 缩量：调整期间成交量显著萎缩（缩倍量），证明主力未出货。
# 4. 价格控制：5元 < 最新价 < 20元，排除ST和创业板。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'

def screen_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 10: return None
        
        # 基础数据清洗
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 1. 范围筛选：排除ST(假设名称中含ST，此处仅依代码过滤)及创业板(30开头)
        if code.startswith('30') or code.startswith('68'): return None
        
        # 获取最新数据
        latest = df.iloc[-1]
        close_price = latest['收盘']
        
        if not (5.0 <= close_price <= 20.0): return None
        
        # 2. 战法逻辑计算
        # 寻找最近10个交易日内的涨停板
        df['is_limit_up'] = df['涨跌幅'] >= 9.5
        limit_up_days = df[df['is_limit_up']]
        
        if limit_up_days.empty: return None
        
        # 取最近的一个涨停板
        last_limit_idx = limit_up_days.index[-1]
        limit_high = df.loc[last_limit_idx, '最高']
        limit_vol = df.loc[last_limit_idx, '成交量']
        
        # 涨停板后的交易日
        after_limit_df = df.loc[last_limit_idx + 1:]
        if after_limit_df.empty: return None # 刚涨停，还在观察期
        
        # 条件 A: 涨停后收盘价不破涨停最高价 (允许0.5%的震荡误差)
        support_hold = after_limit_df['收盘'].min() >= (limit_high * 0.995)
        
        # 条件 B: 缩量调整 (当前量小于涨停量的50%)
        volume_shrink = latest['成交量'] <= (limit_vol * 0.6)
        
        # 条件 C: 距离涨停日不宜过久 (3-7天内最佳)
        days_count = len(after_limit_df)
        
        if support_hold and volume_shrink and (1 <= days_count <= 8):
            # 评分系统
            strength = "极高" if days_count <= 3 else "中等"
            # 胜率优化：如果今日是缩量十字星，得分更高
            signal = "五星买入-金凤凰起飞" if latest['涨跌幅'] < 2 else "四星观察-蓄势待发"
            
            return {
                "代码": code,
                "日期": latest['日期'],
                "现价": close_price,
                "支撑位": limit_high,
                "调整天数": days_count,
                "信号强度": strength,
                "操作建议": f"{signal} (建议：回踩支撑位分批买入，跌破{limit_high*0.97:.2f}止损)"
            }
            
    except Exception:
        return None

def main():
    # 1. 并行扫描
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(screen_logic, files)
    
    results = [r for r in results if r is not None]
    
    # 2. 匹配名称
    if results:
        final_df = pd.DataFrame(results)
        names_df = pd.read_csv(NAMES_FILE)
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
        
        # 优化输出列
        output_cols = ['代码', 'name', '现价', '支撑位', '调整天数', '信号强度', '操作建议']
        final_df = final_df[output_cols].rename(columns={'name': '股票名称'})
        
        # 3. 创建目录并保存
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            
        file_name = f"limit_up_golden_phoenix_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf-8-sig')
        print(f"扫描完成，筛选出 {len(final_df)} 只潜力股。")
    else:
        print("今日未筛选出符合战法的股票。")

if __name__ == "__main__":
    main()
