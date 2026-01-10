import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.3
# 更新说明：
# 1. 适配用户 CSV 列名（日期, 股票代码, 开盘, 收盘, 最高, 最低, 成交量, 涨跌幅）
# 2. 修复 NameError 错误，调整函数声明顺序
# 3. 优化多进程数据处理，增加列名清洗功能
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    """获取当前时间字符串"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def analyze_stock(file_path):
    try:
        # 1. 读取数据并清洗列名
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        
        if len(df) < 120: return None # 确保有足够数据进行半年回测
        
        # 2. 提取代码并过滤板块
        # 优先从文件名提取，若文件名不是代码，可改用 df['股票代码'].iloc[-1]
        code_raw = os.path.basename(file_path).split('.')[0]
        # 兼容处理：如果文件名带前缀(如SH600519)，只取数字
        code = ''.join(filter(str.isdigit, code_raw)).zfill(6)
        
        # 排除 30 (创业板) 和 68 (科创板)
        if code.startswith('30') or code.startswith('68'): return None
        
        # 3. 基础价格过滤 (取最后一天收盘价)
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
            
            # 计算回踩点误差（最低价接近均线但未大幅跌破）
            # 误差 = (最低价 - 均线) / 均线
            diff = (lookback_df['最低'] - ma_subset) / ma_subset
            # 统计在均线附近 -1% 到 +2% 范围内的点位
            support_points = diff[(diff > -0.01) & (diff < 0.02)]
            
            if len(support_points) > 0:
                error = support_points.abs().sum() / len(support_points)
                if error < min_error:
                    min_error = error
                    best_n = n

        # 计算最优均线和5日均量
        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        
        # 当前特征
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 信号判定逻辑 ---
        # 1. 支撑：最低价触及或接近生命线（1.5%范围内），收盘站在线上
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.99)
        # 2. 缩量：成交量小于5日均量的 85%（地量确认）
        vol_shrink = curr['成交量'] < curr['MA_VOL'] * 0.85
        # 3. 趋势：生命线斜率向上
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # --- 历史回测判定 ---
            hits, wins = 0, 0
            test_range = df.tail(120).copy() # 扫描过去120个交易日
            
            # 重新计算该均线在历史上的表现
            test_range['MA_TEST'] = test_range['收盘'].rolling(window=best_n).mean()
            
            for i in range(best_n, len(test_range) - 6):
                h_low = test_range['最低'].iloc[i]
                h_ma = test_range['MA_TEST'].iloc[i]
                h_close = test_range['收盘'].iloc[i]
                
                # 历史回踩点定义
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
            # 涨跌幅过滤：排除当天大涨的，回踩更稳
            if curr['涨跌幅'] < 2.0: score += 30

            if score >= 70:
                strength = "极强 (一击必中)" if score >= 90 else "强"
                action = "分批建仓" if score >= 90 else "轻仓试错"
                
                return {
                    "代码": code,
                    "最优周期": f"{best_n}日线",
                    "当前收盘": curr['收盘'],
                    "涨跌幅": f"{curr['涨跌幅']}%",
                    "回测胜率": f"{win_rate:.2%}",
                    "信号强度": strength,
                    "操作建议": action,
                    "战法分析": f"缩量回踩{best_n}日支撑,历史触发{hits}次"
                }
    except Exception as e:
        return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 开始执行【乾坤一击】战法扫描...")
    
    if not os.path.exists(DATA_DIR):
        print(f"错误：目录 {DATA_DIR} 不存在！")
        exit(1)

    stock_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"共检测到 {len(stock_files)} 个股票数据文件。")

    # 并行处理
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, stock_files)
    
    valid_results = [r for r in results if r is not None]

    if valid_results:
        final_df = pd.DataFrame(valid_results)
        
        # 关联股票名称
        if os.path.exists(NAMES_FILE):
            try:
                names_df = pd.read_csv(NAMES_FILE)
                # 确保 code 是 6 位字符串
                names_df['code'] = names_df['code'].astype(str).str.zfill(6)
                final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
                # 整理顺序
                cols = ['代码', 'name', '当前收盘', '涨跌幅', '信号强度', '回测胜率', '操作建议', '战法分析']
                final_df = final_df[[c for c in cols if c in final_df.columns]]
            except Exception as e:
                print(f"名称表匹配出错: {e}")
        
        # 保存结果
        now = datetime.now()
        out_dir = f"./策略输出_{now.strftime('%Y%m')}"
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(out_dir, f"生命线选股_{now.strftime('%d_%H%M%S')}.csv")
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*50)
        print(final_df.to_string(index=False))
        print("="*50)
        print(f"扫描完成！筛选出 {len(final_df)} 只符合条件的股票。")
        print(f"详细报告已生成：{file_path}")
    else:
        print("\n扫描完成，今日未发现符合战法准入条件的股票。")

    print(f"总耗时: {datetime.now() - start_time}")
