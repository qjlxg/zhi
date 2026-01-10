import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·底部乾坤版】V3.7
# 核心逻辑：
# 1. 动态拟合：自动寻找个股特有的 10-40 日运行节奏线。
# 2. 联动过滤：大盘趋势未破（MA20支撑或MA5上行）且跌幅 < 2.5% 时准入。
# 3. 买入条件：回踩生命线 + 显著缩量 + 均线斜率向上。
# 4. 严格过滤：深沪A股，排除ST、创业板、科创板、高价股。
# 5. 底部增强：新增250日价格区间定位，确保处于筑底阶段。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
MARKET_INDEX_FILE = './stock_data/000001.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    """获取格式化的时间字符串"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def calculate_rsi(series, period=14):
    """计算RSI指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9) # 防止除零
    return 100 - (100 / (1 + rs))

def check_market_environment():
    """大盘联动过滤"""
    if not os.path.exists(MARKET_INDEX_FILE):
        return True
    try:
        m_df = pd.read_csv(MARKET_INDEX_FILE)
        if len(m_df) < 20: return True
        m_df['MA5'] = m_df['收盘'].rolling(window=5).mean()
        m_df['MA20'] = m_df['收盘'].rolling(window=20).mean()
        curr = m_df.iloc[-1]
        prev = m_df.iloc[-2]
        # 允许回踩：MA20支撑位之上 OR MA5方向向上
        trend_ok = (curr['收盘'] >= curr['MA20']) or (curr['MA5'] > prev['MA5'])
        panic_free = curr['涨跌幅'] > -2.5 # 避开暴跌
        return trend_ok and panic_free
    except:
        return True

def analyze_stock(file_path):
    """个股分析逻辑"""
    try:
        file_name = os.path.basename(file_path).upper()
        if 'ST' in file_name or '指数' in file_name: return None
        
        df = pd.read_csv(file_path)
        if len(df) < 250: return None # 满足筑底检测的一年数据要求
        
        code = file_name.split('.')[0]
        if code.startswith(('30', '68', '43', '83', '87')): return None
        
        # 1. 基础价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # 2. 筑底过程检测 (新增)
        # 计算一年内的价格位置，relative_pos越小说明越靠近底部
        high_250 = df['最高'].tail(250).max()
        low_250 = df['最低'].tail(250).min()
        relative_pos = (last_price - low_250) / (high_250 - low_250)
        
        # 过滤掉高位股：只选处于一年内价格波动区间前 40% 的个股
        if relative_pos > 0.4: return None

        # 3. RSI 强度辅助 (过滤过热)
        df['RSI'] = calculate_rsi(df['收盘'])
        curr_rsi = df['RSI'].iloc[-1]
        if not (30 <= curr_rsi <= 65): return None

        # 4. 专属生命线动态拟合 (核心功能)
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

        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 5. 买入三维判定 (回踩 + 缩量 + 向上)
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.985)
        vol_shrink = curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.85
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # 6. 历史胜率回测
            hits, wins = 0, 0
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low = test_range['最低'].iloc[i]
                h_ma = test_range.iloc[i].get('MA_LIFE', 0)
                h_close = test_range['收盘'].iloc[i]
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    future_max = test_range['最高'].iloc[i+1 : i+6].max()
                    if (future_max / h_close) > 1.03: wins += 1
            
            win_rate = (wins / hits) if hits > 0 else 0
            
            # 7. 评分系统 (权重优化)
            score = 0
            if win_rate >= 0.65: score += 40      # 高胜率权重
            if vol_shrink: score += 30           # 缩量权重
            if relative_pos < 0.2: score += 30   # 底部绝对低位权重

            if score >= 90:
                return {
                    "代码": str(code).zfill(6),
                    "收盘": curr['收盘'],
                    "筑底位": f"{relative_pos:.1%}",
                    "RSI": f"{curr_rsi:.1f}",
                    "生命线": f"{best_n}日",
                    "胜率": f"{win_rate:.2%}",
                    "强度": "极强",
                    "建议": "底部启动信号" if relative_pos < 0.2 else "中线回踩点",
                    "逻辑": f"{best_n}线支撑+底部共振"
                }
    except Exception:
        return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 启动 V3.7 筑底增强扫描...")
    
    if not check_market_environment():
        print("🛑 监测到市场大环境风险，脚本根据策略自动终止运行。")
        exit(0)
    
    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"正在分析 {len(all_files)} 只深沪A股...")

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)
    
    final_list = [r for r in results if r is not None]

    if final_list:
        res_df = pd.DataFrame(final_list)
        if os.path.exists(NAMES_FILE):
            names = pd.read_csv(NAMES_FILE)
            names['code'] = names['code'].astype(str).str.zfill(6)
            res_df = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
            res_df = res_df[['代码', 'name', '收盘', '筑底位', 'RSI', '胜率', '强度', '建议', '逻辑']]
        
        folder = datetime.now().strftime('%Y%m')
        os.makedirs(folder, exist_ok=True)
        save_path = f"{folder}/LifeLine_BottomV3.7_{datetime.now().strftime('%d_%H%M%S')}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 扫描完成！在底部区域发现 {len(res_df)} 个高质量信号，存至: {save_path}")
    else:
        print("💡 扫描完成，今日未发现处于底部共振区的标的。")

    print(f"任务结束，总计耗时: {datetime.now() - start_time}")
