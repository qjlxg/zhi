import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.1
# 更新说明：修复了语法冒号错误，增加了文件读取日志，优化了并行逻辑
# 操作要领：
# 1. 支撑确认：股价回踩“专属均线”不破，且成交量显著萎缩（地量见地价）。
# 2. 趋势共振：均线斜率必须向上。
# 3. 历史回测：只做大概率赢钱的票，通过过去 60 天的模拟回踩筛选。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path):
    try:
        # 读取数据
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 提取代码并过滤板块
        code = os.path.basename(file_path).split('.')[0]
        # 排除 30 (创业板) 和 68 (科创板)
        if code.startswith('30') or code.startswith('68'): return None
        
        # 基础价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # --- 专属生命线拟合逻辑 ---
        best_n = 20
        min_error = float('inf')
        
        # 动态寻找过去 60 天内最吻合的支撑均线 (10-40日)
        lookback_df = df.tail(60).copy()
        for n in range(10, 41):
            ma = df['收盘'].rolling(window=n).mean()
            ma_subset = ma.tail(60)
            # 计算回踩点误差（最低价触碰均线但不大幅跌破）
            diff = (lookback_df['最低'] - ma_subset) / ma_subset
            # 统计在均线附近 2% 范围内的点位
            support_points = diff[(diff > -0.01) & (diff < 0.02)]
            if len(support_points) > 0:
                error = support_points.abs().sum() / len(support_points)
                if error < min_error:
                    min_error = error
                    best_n = n

        # 计算最优均线和量能
        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        
        # 当前特征提取
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 信号判定逻辑 ---
        # 1. 回踩：最低价触及或接近生命线，收盘站在线上
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.99)
        # 2. 缩量：成交量小于5日均量的 85%
        vol_shrink = curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.85
        # 3. 趋势：生命线方向向上
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # --- 历史回测判定 ---
            hits, wins = 0, 0
            # 扫描过去半年内的回踩表现
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low = test_range['最低'].iloc[i]
                h_ma = test_range['MA_LIFE'].iloc[i]
                h_close = test_range['收盘'].iloc[i]
                
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    # 5个交易日内最高涨幅是否达 3%
                    future_max = test_range['最高'].iloc[i+1 : i+6].max()
                    if (future_max / h_close) > 1.03:
                        wins += 1
            
            win_rate = (wins / hits) if hits > 0 else 0
            
            # --- 综合评分方案 ---
            score = 0
            if win_rate >= 0.6: score += 40
            if vol_shrink: score += 30
            if curr['涨跌幅'] < 2: score += 30  # 排除暴涨当天的票，回踩更稳

            if score >= 70:  # 只有高质量信号才输出
                strength = "极强 (一击必中)" if score >= 90 else "强"
                action = "分批建仓" if score >= 90 else "轻仓试错"
                
                return {
                    "代码": str(code).zfill(6),
                    "最优周期": f"{best_n}日线",
                    "当前收盘": curr['收盘'],
                    "回测胜率": f"{win_rate:.2%}",
                    "信号强度": strength,
                    "操作建议": action,
                    "战法分析": f"缩量回踩{best_n}日线,趋势多头"
                }
    except Exception as e:
        return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 开始执行战法扫描...")
    
    if not os.path.exists(DATA_DIR):
        print(f"错误：目录 {DATA_DIR} 不存在！")
        exit(1)

    stock_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"检测到 {len(stock_files)} 个股票数据文件。")

    # 多进程加速
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, stock_files)
    
    valid_results = [r for r in results if r is not None]

    if valid_results:
        final_df = pd.DataFrame(valid_results)
        # 匹配股票名称
        if os.path.exists(NAMES_FILE):
            names_df = pd.read_csv(NAMES_FILE)
            names_df['code'] = names_df['code'].astype(str).str.zfill(6)
            final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
            final_df = final_df[['代码', 'name', '当前收盘', '信号强度', '回测胜率', '操作建议', '战法分析']]
        
        # 写入文件
        now = datetime.now()
        out_dir = f"./{now.strftime('%Y%m')}"
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(out_dir, f"LifeLine_Strategy_{now.strftime('%d_%H%M%S')}.csv")
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"成功！筛选出 {len(final_df)} 只符合【专属生命线】战法的股票。结果已保存至 {file_path}")
    else:
        print("扫描完成，今日未发现符合严格准入条件的股票。")

    print(f"总耗时: {datetime.now() - start_time}")

def get_china_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
