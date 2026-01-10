import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】V3.9 全功能实战版
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
MARKET_INDEX_FILE = './stock_data/000001.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def check_market_environment():
    """逻辑2：大盘环境联动过滤 (MA20/MA5/跌幅控制)"""
    if not os.path.exists(MARKET_INDEX_FILE): return True
    try:
        m_df = pd.read_csv(MARKET_INDEX_FILE)
        m_df['MA5'] = m_df['收盘'].rolling(window=5).mean()
        m_df['MA20'] = m_df['收盘'].rolling(window=20).mean()
        curr, prev = m_df.iloc[-1], m_df.iloc[-2]
        # 条件：收盘在MA20上 OR MA5向上 且 跌幅 > -2.5%
        trend_ok = (curr['收盘'] >= curr['MA20']) or (curr['MA5'] > prev['MA5'])
        panic_free = curr['涨跌幅'] > -2.5
        return trend_ok and panic_free
    except: return True

def analyze_stock(file_path):
    """个股全维度分析"""
    try:
        # 逻辑4：严格过滤 (排除ST、指数、创业板、科创板等)
        file_name = os.path.basename(file_path).upper()
        if 'ST' in file_name or '指数' in file_name: return None
        code = file_name.split('.')[0]
        if code.startswith(('30', '68', '43', '83', '87')): return None

        df = pd.read_csv(file_path)
        if len(df) < 250: return None # 逻辑5需要一年数据
        
        # 逻辑4：价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # 逻辑1：动态拟合生命线 (10-40日)
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
                    min_error, best_n = error, n

        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        curr, prev = df.iloc[-1], df.iloc[-2]
        
        # 逻辑3：买入核心条件 (回踩 + 斜率向上)
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.02) and (curr['收盘'] >= curr['MA_LIFE'] * 0.98)
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        # 逻辑3补充：显著缩量 (设为弹性评分项，但保留基础门槛)
        is_vol_ok = curr['成交量'] < df['MA_VOL'].iloc[-1] * 1.0 # 基础门槛：不放量即可
        
        if on_support and trend_up and is_vol_ok:
            # 逻辑5：底部增强评分
            high_250 = df['最高'].tail(250).max()
            low_250 = df['最低'].tail(250).min()
            relative_pos = (last_price - low_250) / (high_250 - low_250 + 1e-9)
            
            # 计算历史胜率回测
            hits, wins = 0, 0
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low, h_ma, h_close = test_range['最低'].iloc[i], test_range.iloc[i].get('MA_LIFE', 0), test_range['收盘'].iloc[i]
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    if (test_range['最高'].iloc[i+1 : i+6].max() / h_close) > 1.03: wins += 1
            win_rate = (wins / hits) if hits > 0 else 0

            # 综合评分逻辑
            score = 0
            if relative_pos < 0.3: score += 35   # 筑底加分
            if curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.8: score += 35 # 缩量加分
            if win_rate >= 0.6: score += 30      # 胜率加分

            # 只要满足核心逻辑且分值不为0就输出
            return {
                "代码": str(code).zfill(6),
                "筑底位": f"{relative_pos:.1%}",
                "生命线": f"{best_n}日",
                "胜率": f"{win_rate:.2%}",
                "评分": score,
                "强度": "极强" if score >= 70 else ("强" if score >= 35 else "标准"),
                "逻辑": f"回踩{best_n}线+{'底部' if relative_pos < 0.4 else '中继'}"
            }
    except: return None
    return None

if __name__ == '__main__':
    start_time = datetime.now()
    print(f"[{get_china_time()}] 启动全功能 V3.9 实战版...")
    if not check_market_environment():
        print("🛑 监测到大盘风险。")
        exit(0)
    
    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)
    
    final_list = [r for r in results if r is not None]
    if final_list:
        res_df = pd.DataFrame(final_list).sort_values(by='评分', ascending=False)
        if os.path.exists(NAMES_FILE):
            names = pd.read_csv(NAMES_FILE)
            names['code'] = names['code'].astype(str).str.zfill(6)
            res_df = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
            res_df = res_df[['代码', 'name', '筑底位', '生命线', '胜率', '评分', '强度', '逻辑']]
        
        save_path = f"LifeLine_V3.9_{datetime.now().strftime('%d_%H%M%S')}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 完成！选出 {len(res_df)} 只，最高评分标的为 {res_df.iloc[0]['name']}")
    else:
        print("💡 未发现完全符合“回踩+趋势向上”核心逻辑的标的。")
