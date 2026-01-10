import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.5 (大盘联动版)
# 核心逻辑：
# 1. 大盘定性：上证指数站稳5日线才开工，避开系统性风险。
# 2. 个股拟合：动态寻找10-40日生命线。
# 3. 三维共振：回踩支撑 + 地量洗盘 + 趋势向上。
# 4. 严格过滤：排除ST、创业板、科创板、高价股。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
MARKET_INDEX_FILE = './stock_data/000001.csv' # 默认上证指数数据路径
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    """获取格式化的时间字符串"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def check_market_environment():
    """
    大盘环境联动过滤逻辑
    返回 True (环境安全) 或 False (环境危险)
    """
    if not os.path.exists(MARKET_INDEX_FILE):
        print(f"⚠️ 未找到大盘数据({MARKET_INDEX_FILE})，默认不启动联动过滤。")
        return True
    
    try:
        m_df = pd.read_csv(MARKET_INDEX_FILE)
        if len(m_df) < 10: return True
        
        m_curr = m_df.iloc[-1]
        m_ma5 = m_df['收盘'].rolling(window=5).mean().iloc[-1]
        m_pct = m_curr['涨跌幅']
        
        # 判定标准：
        # 1. 上证指数收盘价在5日线之上 (短期强势)
        # 2. 今日大盘跌幅未超过 -1.5% (非暴跌日)
        if m_curr['收盘'] >= m_ma5 and m_pct > -1.5:
            print(f"✅ 大盘环境安全：上证指数处于5日线上方，涨跌幅 {m_pct}%。开始扫描个股。")
            return True
        else:
            print(f"❌ 大盘环境风险：上证指数收于5日线下或跌幅过大({m_pct}%)。为保住本金，今日不建议操作。")
            return False
    except Exception as e:
        print(f"⚠️ 大盘分析异常: {e}，跳过过滤。")
        return True

def analyze_stock(file_path):
    """
    单个股票分析逻辑
    """
    try:
        # 1. 严格排除ST股票
        file_name = os.path.basename(file_path).upper()
        if 'ST' in file_name: return None
        
        # 2. 读取数据
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 3. 提取并过滤代码 (排除创业板、科创板、北交所)
        code = file_name.split('.')[0]
        if code.startswith(('30', '68', '43', '83', '87')): return None
        
        # 4. 基础价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # --- 专属生命线拟合 ---
        best_n = 20
        min_error = float('inf')
        lookback_df = df.tail(60).copy()
        
        for n in range(10, 41):
            ma = df['收盘'].rolling(window=n).mean()
            ma_subset = ma.tail(60)
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
        
        # --- 战法条件判定 ---
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.99)
        vol_shrink = curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.85
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # --- 历史回测 ---
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

            if score >= 90: # 仅输出极强信号
                return {
                    "代码": str(code).zfill(6),
                    "生命线": f"{best_n}日",
                    "收盘": curr['收盘'],
                    "胜率": f"{win_rate:.2%}",
                    "强度": "极强",
                    "建议": "重仓信号",
                    "逻辑": f"回踩{best_n}日线+地量"
                }
    except Exception:
        return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 启动扫描程序...")
    
    # 第一步：大盘联动检查
    if not check_market_environment():
        print("🛑 监测到市场风险，脚本终止运行。")
        exit(0)
    
    # 第二步：个股扫描
    if not os.path.exists(DATA_DIR):
        print(f"FATAL: {DATA_DIR} 目录未找到。")
        exit(1)

    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"读取到 {len(all_files)} 个股票数据文件。")

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)
    
    final_list = [r for r in results if r is not None]

    if final_list:
        res_df = pd.DataFrame(final_list)
        if os.path.exists(NAMES_FILE):
            names = pd.read_csv(NAMES_FILE)
            names['code'] = names['code'].astype(str).str.zfill(6)
            res_df = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
            res_df = res_df[['代码', 'name', '收盘', '强度', '胜率', '建议', '逻辑']]
        
        folder = datetime.now().strftime('%Y%m')
        os.makedirs(folder, exist_ok=True)
        ts = datetime.now().strftime('%d_%H%M%S')
        save_path = f"{folder}/LifeLine_Strategy_{ts}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 扫描完成！发现 {len(res_df)} 个高质量信号。")
    else:
        print("💡 扫描完成，今日未发现符合条件的个股。")

    print(f"任务结束，耗时: {datetime.now() - start_time}")
