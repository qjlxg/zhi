import os
import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing as mp
import subprocess

# ==========================================
# 战法名称：【专属生命线·顶级精选版】V4.1
# 核心逻辑（完整保留）：
# 1. 动态拟合：寻找 10-40 日节奏线。
# 2. 联动过滤：大盘 MA20 支撑/MA5 上行 + 跌幅 < 2.5%。
# 3. 买入条件：回踩生命线 + 显著缩量 + 均线斜率向上。
# 4. 严格过滤：排除 ST、创业板、科创板、高价股 (>20元)。
# 5. 底部增强：250日价格定位，底部高分制。
# 6. 精选拦截：成交量连续萎缩 + RSI 强弱对冲。
# 7. 自动推送：年月分类并推送至远程 Git 仓库。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
MARKET_INDEX_FILE = './stock_data/000001.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0
STRATEGY_NAME = "LifeLine_Strategy"

def get_china_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def check_market_environment():
    """逻辑2：大盘联动过滤"""
    if not os.path.exists(MARKET_INDEX_FILE): return True
    try:
        m_df = pd.read_csv(MARKET_INDEX_FILE)
        m_df['MA5'] = m_df['收盘'].rolling(window=5).mean()
        m_df['MA20'] = m_df['收盘'].rolling(window=20).mean()
        curr, prev = m_df.iloc[-1], m_df.iloc[-2]
        trend_ok = (curr['收盘'] >= curr['MA20']) or (curr['MA5'] > prev['MA5'])
        panic_free = curr['涨跌幅'] > -2.5
        return trend_ok and panic_free
    except: return True

def analyze_stock(file_path):
    """个股全维度分析 + 精选拦截器"""
    try:
        # 逻辑4：严格过滤
        file_name = os.path.basename(file_path).upper()
        if 'ST' in file_name or '指数' in file_name: return None
        code = file_name.split('.')[0]
        if code.startswith(('30', '68', '43', '83', '87')): return None

        df = pd.read_csv(file_path)
        if len(df) < 250: return None 
        
        # 逻辑4：价格过滤
        last_price = df['收盘'].iloc[-1]
        if not (PRICE_MIN <= last_price <= PRICE_MAX): return None

        # 逻辑1：动态拟合生命线
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
        df['RSI'] = calculate_rsi(df['收盘'])
        curr, prev = df.iloc[-1], df.iloc[-2]
        
        # 逻辑3：核心条件
        on_support = (curr['最低'] <= curr['MA_LIFE'] * 1.02) and (curr['收盘'] >= curr['MA_LIFE'] * 0.98)
        trend_up = curr['MA_LIFE'] > prev['MA_LIFE']
        
        if on_support and trend_up:
            # 逻辑5：底部增强
            high_250 = df['最高'].tail(250).max()
            low_250 = df['最低'].tail(250).min()
            relative_pos = (last_price - low_250) / (high_250 - low_250 + 1e-9)
            
            # 逻辑6：精选拦截器核心（成交量连续缩减 + RSI过滤）
            vol_history = df['成交量'].tail(3).values
            is_vol_shrink_3 = (vol_history[2] < vol_history[1]) or (vol_history[2] < df['MA_VOL'].iloc[-1] * 0.8)
            is_rsi_healthy = 35 <= curr['RSI'] <= 60
            
            # 回测胜率计算
            hits, wins = 0, 0
            test_range = df.tail(120) 
            for i in range(len(test_range) - 6):
                h_low, h_ma, h_close = test_range['最低'].iloc[i], test_range.iloc[i].get('MA_LIFE', 0), test_range['收盘'].iloc[i]
                if h_low <= h_ma * 1.01 and h_close >= h_ma:
                    hits += 1
                    if (test_range['最高'].iloc[i+1 : i+6].max() / h_close) > 1.03: wins += 1
            win_rate = (wins / hits) if hits > 0 else 0

            # 评分
            score = 0
            if relative_pos < 0.3: score += 35   
            if is_vol_shrink_3: score += 35 
            if win_rate >= 0.6: score += 30      

            data = {
                "代码": str(code).zfill(6),
                "收盘": curr['收盘'],
                "筑底位": f"{relative_pos:.1%}",
                "生命线": f"{best_n}日",
                "胜率": f"{win_rate:.2%}",
                "评分": score,
                "强度": "顶级" if (score >= 80 and is_rsi_healthy) else ("强" if score >= 40 else "标准"),
                "精选": "★" if (is_vol_shrink_3 and is_rsi_healthy and relative_pos < 0.4) else "",
                "逻辑": f"回踩{best_n}线+{'缩量' if is_vol_shrink_3 else '地量'}"
            }
            return data
    except: return None

def push_to_github():
    try:
        subprocess.run(["git", "add", "."], check=True)
        msg = f"Strategy Update V4.1: {datetime.now().strftime('%Y-%m-%d')}"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "push"], check=True)
        print("🚀 仓库同步完成！")
    except Exception as e: print(f"❌ Git推送失败: {e}")

if __name__ == '__main__':
    print(f"[{get_china_time()}] 启动 V4.1 顶级精选版...")
    if not check_market_environment():
        print("🛑 市场环境不佳，跳过今日操作。")
        exit(0)
    
    all_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, all_files)
    
    final_list = [r for r in results if r is not None]
    
    if final_list:
        res_df = pd.DataFrame(final_list).sort_values(by=['精选', '评分'], ascending=False)
        if os.path.exists(NAMES_FILE):
            names = pd.read_csv(NAMES_FILE)
            names['code'] = names['code'].astype(str).str.zfill(6)
            res_df = pd.merge(res_df, names, left_on='代码', right_on='code', how='left')
            res_df = res_df[['代码', 'name', '收盘', '筑底位', '生命线', '胜率', '评分', '强度', '精选', '逻辑']]
        
        folder = datetime.now().strftime('%Y%m')
        os.makedirs(folder, exist_ok=True)
        ts = datetime.now().strftime('%d_%H%M%S')
        save_path = os.path.join(folder, f"{STRATEGY_NAME}_{ts}.csv")
        
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 完成！扫描出 {len(res_df)} 只回踩标的，精选标的已置顶。")
        
        # 仅打印精选出的星标个股
        pick_df = res_df[res_df['精选'] == "★"]
        if not pick_df.empty:
            print("\n🔥 今日顶级精选（拦截器通过）：")
            print(pick_df[['代码', 'name', '评分', '筑底位', '胜率']])
        
        push_to_github()
    else:
        print("💡 未发现符合逻辑的标的。")
