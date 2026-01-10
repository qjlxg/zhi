import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.2
# 更新说明：修复函数定义顺序错误，增强代码健壮性
# 核心逻辑：拟合 10-40 日动态生命线，寻找“回踩+缩量+趋势向上”的共振点
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    """获取格式化的时间字符串"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def analyze_stock(file_path):
    """
    单个股票分析逻辑
    """
    try:
        # 读取数据
        df = pd.read_csv(file_path)
        if len(df) < 60: 
            return None
        
        # 提取并过滤代码
        code = os.path.basename(file_path).split('.')[0]
        # 排除 30 (创业板), 68 (科创板) 和 43/83/87 (北交所)
        if code.startswith(('30', '68', '43', '83', '87')): 
            return None
        
        # 基础价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): 
            return None

        # --- 专属生命线拟合 ---
        best_n = 20
        min_error = float('inf')
        lookback_df = df.tail(60).copy()
        
        for n in range(10, 41):
            ma = df['收盘'].rolling(window=n).mean()
            ma_subset = ma.tail(60)
            # 计算回踩点误差
            diff = (lookback_df['最低'] - ma_subset) / ma_subset
            support_points = diff[(diff > -0.01) & (diff < 0.02)]
            if len(support_points) > 0:
                error = support_points.abs().sum() / len(support_points)
                if error < min_error:
                    min_error = error
                    best_n = n

        # 计算指标
        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 判定条件 ---
        # 1. 回踩：最低价触及生命线 1.5% 范围内，收盘不破线
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.99)
        # 2. 缩量：成交量低于5日均量的 85%（代表抛压衰竭）
        vol_shrink = curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.85
        # 3. 趋势：生命线角度向上
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # --- 简易回测 ---
            hits, wins = 0, 0
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low = test_range['最低'].iloc[i]
                h_ma = test_range.iloc[i].get('MA_LIFE', 0) 
                h_close = test_range['收盘'].iloc[i]
                
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    future_max = test_range['最高'].iloc[i+1 : i+6].max()
                    if (future_max / h_close) > 1.03:
                        wins += 1
            
            win_rate = (wins / hits) if hits > 0 else 0
            
            # --- 评分系统 ---
            score = 0
            if win_rate >= 0.6: score += 40
            if vol_shrink: score += 30
            if curr['涨跌幅'] < 2.5: score += 30 

            if score >= 70:
                return {
                    "代码": str(code).zfill(6),
                    "生命线": f"{best_n}日",
                    "收盘": curr['收盘'],
                    "胜率": f"{win_rate:.2%}",
                    "强度": "极强" if score >= 90 else "强",
                    "建议": "重仓信号" if score >= 90 else "试错观察",
                    "逻辑": f"回踩{best_n}日线+地量"
                }
    except Exception:
        return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 启动扫描程序...")
    
    if not os.path.exists(DATA_DIR):
        print(f"FATAL: {DATA_DIR} 目录未找到，请检查数据路径。")
        exit(1)

    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"读取到 {len(all_files)} 个数据文件。")

    # 使用多进程提高 CPU 利用率
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)
    
    final_list = [r for r in results if r is not None]

    if final_list:
        res_df = pd.DataFrame(final_list)
        # 合并名称
        if os.path.exists(NAMES_FILE):
            names = pd.read_csv(NAMES_FILE)
            names['code'] = names['code'].astype(str).str.zfill(6)
            res_df = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
            res_df = res_df[['代码', 'name', '收盘', '强度', '胜率', '建议', '逻辑']]
        
        # 结果存入年月文件夹
        folder = datetime.now().strftime('%Y%m')
        os.makedirs(folder, exist_ok=True)
        ts = datetime.now().strftime('%d_%H%M%S')
        save_path = f"{folder}/LifeLine_Strategy_{ts}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 扫描完成！发现 {len(res_df)} 个高价值信号，已存至: {save_path}")
    else:
        print("💡 扫描完成，今日未发现完全符合条件的个股。")

    print(f"任务结束，总计耗时: {datetime.now() - start_time}")
