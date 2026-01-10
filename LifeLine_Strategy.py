import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：【专属生命线·乾坤一击】优化版 V3.6 (大盘联动弹性版)
# 核心逻辑：
# 1. 动态拟合：自动寻找个股特有的 10-40 日运行节奏线。
# 2. 联动过滤：大盘趋势未破（MA20支撑或MA5上行）且跌幅 < 2.5% 时准入。
# 3. 买入条件：回踩生命线 + 显著缩量 + 均线斜率向上。
# 4. 严格过滤：深沪A股，排除ST、创业板、科创板、高价股。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
MARKET_INDEX_FILE = './stock_data/000001.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def get_china_time():
    """获取格式化的时间字符串"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def check_market_environment():
    """
    大盘环境联动过滤 (V3.6 缓冲带版)
    """
    if not os.path.exists(MARKET_INDEX_FILE):
        print(f"⚠️ 未找到大盘数据({MARKET_INDEX_FILE})，跳过联动过滤。")
        return True
    
    try:
        m_df = pd.read_csv(MARKET_INDEX_FILE)
        if len(m_df) < 20: return True
        
        # 计算 5日 和 20日 均线
        m_df['MA5'] = m_df['收盘'].rolling(window=5).mean()
        m_df['MA20'] = m_df['收盘'].rolling(window=20).mean()
        
        curr = m_df.iloc[-1]
        prev = m_df.iloc[-2]
        
        # 判定逻辑：
        # A. 收盘在20日趋势线上方 (中期安全)
        # B. 或者 5日线方向依然向上 (短期良性回踩)
        # C. 且单日跌幅不大于 2.5%
        trend_ok = (curr['收盘'] >= curr['MA20']) or (curr['MA5'] > prev['MA5'])
        panic_free = curr['涨跌幅'] > -2.5
        
        if trend_ok and panic_free:
            print(f"✅ 大盘环境OK：趋势未破且无恐慌大跌。今日涨跌幅: {curr['涨跌幅']}%")
            return True
        else:
            print(f"🛑 大盘环境不佳：趋势走弱或跌幅过大({curr['涨跌幅']}%)。建议空仓避险。")
            return False
    except Exception as e:
        print(f"⚠️ 大盘分析异常: {e}，跳过过滤机制。")
        return True

def analyze_stock(file_path):
    """
    个股核心筛选逻辑
    """
    try:
        # 1. 排除ST和路径异常
        file_name = os.path.basename(file_path).upper()
        if 'ST' in file_name or '指数' in file_name: return None
        
        # 2. 读取数据
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 3. 排除创业板、科创板、北交所 (只选深沪A股)
        code = file_name.split('.')[0]
        if code.startswith(('30', '68', '43', '83', '87')): return None
        
        # 4. 价格区间过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # --- 专属生命线动态拟合 ---
        best_n = 20
        min_error = float('inf')
        lookback_df = df.tail(60).copy()
        
        for n in range(10, 41):
            ma = df['收盘'].rolling(window=n).mean()
            ma_subset = ma.tail(60)
            diff = (lookback_df['最低'] - ma_subset) / ma_subset
            # 统计回踩点（误差1%到2%之间）
            support_points = diff[(diff > -0.01) & (diff < 0.02)]
            if len(support_points) > 0:
                error = support_points.abs().sum() / len(support_points)
                if error < min_error:
                    min_error = error
                    best_n = n

        # 指标计算
        df['MA_LIFE'] = df['收盘'].rolling(window=best_n).mean()
        df['MA_VOL'] = df['成交量'].rolling(window=5).mean()
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 战法三维共振判定 ---
        # 1. 回踩确认
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.015) and (curr['收盘'] >= curr['MA_LIFE'] * 0.985)
        # 2. 缩量判定
        vol_shrink = curr['成交量'] < df['MA_VOL'].iloc[-1] * 0.85
        # 3. 趋势判定
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # --- 历史胜率回测 ---
            hits, wins = 0, 0
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low = test_range['最低'].iloc[i]
                h_ma = test_range.iloc[i].get('MA_LIFE', 0)
                h_close = test_range['收盘'].iloc[i]
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    # 后5日最高涨幅达3%计为盈利
                    future_max = test_range['最高'].iloc[i+1 : i+6].max()
                    if (future_max / h_close) > 1.03:
                        wins += 1
            
            win_rate = (wins / hits) if hits > 0 else 0
            
            # --- 综合评分 ---
            score = 0
            if win_rate >= 0.6: score += 40
            if vol_shrink: score += 30
            if curr['涨跌幅'] < 2.5: score += 30 

            if score >= 90:
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
    
    # 大盘联动环境检查
    if not check_market_environment():
        print("🛑 大盘趋势不佳，为规避系统性风险，程序已提前结束。")
        exit(0)
    
    if not os.path.exists(DATA_DIR):
        print(f"FATAL: {DATA_DIR} 目录未找到，请检查 stock_data 文件夹。")
        exit(1)

    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"正在扫描 {len(all_files)} 个数据文件...")

    # 多进程并行执行
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
        
        # 按年月归档
        folder = datetime.now().strftime('%Y%m')
        os.makedirs(folder, exist_ok=True)
        ts = datetime.now().strftime('%d_%H%M%S')
        save_path = f"{folder}/LifeLine_Strategy_{ts}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 扫描完成！在适宜的市场环境中选出 {len(res_df)} 只高确定性标的。")
    else:
        print("💡 扫描完成，今日未发现符合【重仓级别】的个股。")

    print(f"总计耗时: {datetime.now() - start_time}")
