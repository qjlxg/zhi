import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：涨停金凤凰 (Limit Up Golden Phoenix)
# 
# 【战法逻辑说明】：
# 1. 强力启动：寻找近期出现过 9.5% 以上涨幅的大阳涨停板。
# 2. 核心支撑：涨停后的回踩/横盘期间，收盘价严禁有效跌破涨停当日最高价。
# 3. 极度缩量：洗盘期间成交量必须缩减至涨停日的 60% 以下，代表主力控盘且惜售。
# 4. 优加选优：价格在 5-20 元之间，排除 30/68 开头和 ST，确保流动性与稳定性。
# 5. 买入逻辑：缩量回踩支撑位不破，博取第二波主升浪。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_and_backtest(file_path):
    """
    单个股票的筛选与回测逻辑
    """
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        code = os.path.basename(file_path).replace('.csv', '')
        # 基础过滤：排除创业板(30)、科创板(68)、ST(假设代码或数据已处理)
        if code.startswith(('30', '68')): return None
        
        # 识别涨停（按9.5%计算）
        df['is_limit_up'] = df['涨跌幅'] >= 9.5
        
        # --- 历史回测部分 ---
        # 统计过去一年内符合该战法的次数及5日后胜率
        limit_indices = df[df['is_limit_up']].index
        win_count = 0
        total_signals = 0
        
        for idx in limit_indices:
            if idx + 6 >= len(df): continue # 离现在太近，不计入回测
            
            # 考察涨停后的3-5天是否符合缩量横盘
            obs_window = df.loc[idx+1 : idx+3]
            limit_high = df.loc[idx, '最高']
            limit_vol = df.loc[idx, '成交量']
            
            # 条件：横盘不破支撑且缩量
            if obs_window['收盘'].min() >= (limit_high * 0.99) and obs_window['成交量'].max() < limit_vol * 0.7:
                total_signals += 1
                # 计算买入后5天的最高涨幅是否超过5%
                future_max = df.loc[idx+4 : idx+8, '最高'].max()
                buy_price = df.loc[idx+3, '收盘']
                if (future_max - buy_price) / buy_price >= 0.05:
                    win_count += 1

        history_win_rate = (win_count / total_signals) if total_signals > 0 else 0

        # --- 今日实时筛选部分 ---
        latest = df.iloc[-1]
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        
        # 寻找最近10天内的涨停板
        recent_df = df.tail(10).copy()
        recent_limit_up = recent_df[recent_df['is_limit_up']]
        
        if not recent_limit_up.empty:
            last_limit_idx = recent_limit_up.index[-1]
            # 必须不是今天刚涨停，需要有回踩过程
            if last_limit_idx < df.index[-1]:
                limit_high = df.loc[last_limit_idx, '最高']
                limit_vol = df.loc[last_limit_idx, '成交量']
                
                after_df = df.loc[last_limit_idx + 1:]
                
                # 判定：收盘不破支撑位，且当前量能大幅萎缩
                support_ok = after_df['收盘'].min() >= (limit_high * 0.995)
                vol_ok = latest['成交量'] < (limit_vol * 0.6)
                
                if support_ok and vol_ok:
                    # 综合评分：结合历史表现和当前形态
                    score = "极高(一击必中)" if history_win_rate > 0.6 and len(after_df) <= 4 else "观察"
                    
                    return {
                        "代码": code,
                        "日期": latest['日期'],
                        "现价": latest['收盘'],
                        "涨停支撑位": limit_high,
                        "横盘天数": len(after_df),
                        "历史战法胜率": f"{history_win_rate:.2%}",
                        "买入信号强度": score,
                        "操作建议": f"建议：回踩{limit_high}附近进场，破{(limit_high*0.97):.2f}止损，预期空间10%+"
                    }
        return None
    except Exception:
        return None

def main():
    # 获取待扫描文件列表
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # 3. 并行运行提高速度
    print(f"开始并行分析 {len(files)} 只股票...")
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_and_backtest, files)
    
    valid_results = [r for r in results if r is not None]
    
    # 4. 匹配股票名称并输出
    if valid_results:
        final_df = pd.DataFrame(valid_results)
        if os.path.exists(NAMES_FILE):
            names_df = pd.read_csv(NAMES_FILE)
            names_df['code'] = names_df['code'].astype(str).str.zfill(6)
            final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
        
        # 格式化输出
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        
        file_path = os.path.join(dir_path, f"limit_up_golden_phoenix_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"全自动复盘完成。筛选出潜力品种 {len(final_df)} 只，结果已存至 {file_path}")
    else:
        print("今日无符合战法条件的股票。")

if __name__ == "__main__":
    main()
