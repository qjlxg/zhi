import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.0
# 操作要领：
# 1. 支撑确认：股价回踩“专属均线”不破，且成交量显著萎缩（洗盘彻底）。
# 2. 趋势共振：均线斜率必须向上，拒绝阴跌票。
# 3. 优中选优：历史回测胜率低于 60% 的信号直接舍弃。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 基础筛选：价格与板块
        code = os.path.basename(file_path).split('.')[0]
        last_price = df['收盘'].iloc[-1]
        
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None
        if code.startswith('30') or 'ST' in file_path.upper(): return None

        # --- 专属生命线拟合逻辑 ---
        # 寻找过去60天内最能贴合低点的均线周期 (N在10-40之间搜索)
        best_n = 20
        min_error = float('inf')
        
        for n in range(10, 41):
            ma = df['收盘'].rolling(window=n).mean()
            # 计算回踩误差：收盘价在MA上方且接近MA的程度
            diff = (df['最低'] - ma) / ma
            error = diff[(diff > 0) & (diff < 0.02)].abs().sum()
            if error > 0 and error < min_error:
                min_error = error
                best_n = n

        # 计算最优均线
        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        
        # 当前状态特征
        curr_close = df['收盘'].iloc[-1]
        curr_low = df['最低'].iloc[-1]
        curr_ma = df['MA_LIFE'].iloc[-1]
        prev_ma = df['MA_LIFE'].iloc[-2]
        curr_vol = df['成交量'].iloc[-1]
        avg_vol = df['MA_VOL'].iloc[-1]
        
        # --- 信号判定 ---
        # 1. 回踩生命线：最低价接近MA，收盘价在MA之上
        on_support = (curr_low <= curr_ma * 1.01) and (curr_close >= curr_ma * 0.99)
        # 2. 缩量：当前成交量小于5日均量
        vol_shrink = curr_vol < avg_vol * 0.8
        # 3. 趋势向上：MA斜率为正
        trend_up = curr_ma > prev_ma
        
        if on_support and trend_up:
            # --- 历史回测功能 ---
            hits = 0
            wins = 0
            for i in range(best_n, len(df) - 5):
                hist_low = df['最低'].iloc[i]
                hist_ma = df['MA_LIFE'].iloc[i]
                if hist_low <= hist_ma * 1.01 and df['收盘'].iloc[i] >= hist_ma:
                    hits += 1
                    # 5日后涨幅是否超过 3%
                    if (df['收盘'].iloc[i+5] / df['收盘'].iloc[i]) > 1.03:
                        wins += 1
            
            win_rate = (wins / hits) if hits > 0 else 0
            
            # --- 综合评分与建议 ---
            strength = "弱"
            action = "暂时观察"
            score = 0
            
            if win_rate > 0.6: score += 40
            if vol_shrink: score += 30
            if curr_close > curr_ma: score += 30
            
            if score >= 90:
                strength = "极强 (一击必中)"
                action = "重仓买入/加仓"
            elif score >= 70:
                strength = "强"
                action = "适量试错"
            elif score >= 50:
                strength = "中"
                action = "小仓位观察"

            if score >= 0: # 只输出高质量结果
                return {
                    "代码": code,
                    "最优周期": best_n,
                    "收盘价": curr_close,
                    "回测胜率": f"{win_rate:.2%}",
                    "信号强度": strength,
                    "操作建议": action,
                    "逻辑简述": f"缩量回踩{best_n}日生命线，趋势向上"
                }
    except Exception as e:
        return None
    return None

if __name__ == '__main__':
    # 并行处理
    stock_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, stock_files)
    
    filtered_results = [r for r in results if r is not None]
    
    # 匹配名称
    if os.path.exists(NAMES_FILE):
        names_df = pd.read_csv(NAMES_FILE)
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        final_df = pd.DataFrame(filtered_results)
        if not final_df.empty:
            final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
            final_df = final_df[['代码', 'name', '收盘价', '信号强度', '回测胜率', '操作建议', '逻辑简述']]
            
            # 保存结果
            now = datetime.now()
            dir_path = f"./{now.strftime('%Y%m')}"
            os.makedirs(dir_path, exist_ok=True)
            file_name = f"LifeLine_Strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
            
            final_df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf-8-sig')
            print(f"扫描完毕，发现 {len(final_df)} 只潜力股。")
