import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==============================================================================
# 战法名称：涨停金凤凰 (Limit Up Golden Phoenix) - 五星精选版
# 
# 【战法逻辑说明】：
# 1. 核心定义：寻找近期出现过涨停（涨幅 >= 9.8%）的领涨标的。
# 2. 支撑逻辑：涨停后横盘期间，收盘价严禁跌破涨停当日最高价（误差 < 0.5%）。
# 3. 缩量核心：洗盘成交量必须萎缩至涨停日的 60% 以下，地量代表筹码锁定。
# 4. 择时优化：选取涨停后 2-7 天的标的，这是二次起爆的黄金窗口。
# 5. 优加选优：【核心变更】脚本仅输出历史回测胜率 >= 60% 的 5 星标的。
# ==============================================================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'

def analyze_logic(file_path):
    """
    单股核心分析逻辑：筛选 + 历史回测
    """
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 提取代码并过滤范围
        code = os.path.basename(file_path).replace('.csv', '')
        if code.startswith(('30', '68', 'ST', '*ST')): return None
        
        # 1. 基础硬性筛选
        latest = df.iloc[-1]
        cur_close = latest['收盘']
        if not (5.0 <= cur_close <= 20.0): return None

        # 2. 识别历史所有涨停信号用于回测
        df['is_limit_up'] = df['涨跌幅'] >= 9.8
        limit_indices = df[df['is_limit_up']].index
        if limit_indices.empty: return None
        
        # 3. 历史回测部分：计算该股的历史“股性”
        success_count = 0
        total_signals = 0
        for idx in limit_indices:
            # 确保涨停后有足够数据进行回测统计 (观察后续5-8天)
            if idx + 8 >= len(df): continue
            
            h = df.loc[idx, '最高']
            v = df.loc[idx, '成交量']
            # 模拟历史：涨停后3天内不破位且缩量
            obs = df.loc[idx+1 : idx+3]
            if obs['收盘'].min() >= h * 0.99 and obs['成交量'].max() < v * 0.7:
                total_signals += 1
                buy_price = df.loc[idx+3, '收盘']
                post_max = df.loc[idx+4 : idx+8, '最高'].max()
                if (post_max - buy_price) / buy_price >= 0.05: # 5%涨幅算成功
                    success_count += 1
        
        win_rate_val = success_count / total_signals if total_signals > 0 else 0.0

        # --- 【强制过滤逻辑】 ---
        # 只有历史胜率 >= 60% 且至少出现过一次成功案例的才进入 5 星池
        if win_rate_val < 0.6 or total_signals == 0:
            return None

        # 4. 今日实时形态检测
        last_idx = limit_indices[-1]
        days_count = len(df) - 1 - last_idx
        
        # 仅选择涨停后调整 2 到 7 天的股票
        if not (2 <= days_count <= 7): return None
        
        limit_high = df.loc[last_idx, '最高']
        limit_vol = df.loc[last_idx, '成交量']
        after_limit_df = df.loc[last_idx + 1:]
        
        # 形态校验：收盘价站稳支撑位 且 当前是缩量的
        is_supported = after_limit_df['收盘'].min() >= (limit_high * 0.995)
        is_vol_shrink = latest['成交量'] < (limit_vol * 0.6) 
        
        if is_supported and is_vol_shrink:
            strength = "⭐⭐⭐⭐⭐ [一击必中]"
            advice = f"该股历史表现极佳(胜率{win_rate_val:.1%})！目前缩量至{latest['成交量']/limit_vol:.1%}，建议分批介入。"

            return {
                "代码": code,
                "日期": latest['日期'],
                "现价": cur_close,
                "支撑位": limit_high,
                "缩量占比": f"{(latest['成交量']/limit_vol):.1%}",
                "横盘天数": days_count,
                "历史胜率": f"{win_rate_val:.1%}",
                "买入信号强度": strength,
                "全自动复盘建议": advice
            }
        return None
    except Exception:
        return None

def main():
    if not os.path.exists(DATA_DIR):
        print(f"错误: 找不到目录 {DATA_DIR}")
        return

    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"正在全量扫描 {len(files)} 只股票，仅筛选 5 星‘一击必中’标的...")

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_logic, files)
    
    results = [r for r in results if r is not None]
    
    if results:
        final_df = pd.DataFrame(results)
        
        if os.path.exists(NAMES_FILE):
            names_df = pd.read_csv(NAMES_FILE)
            names_df['code'] = names_df['code'].astype(str).str.zfill(6)
            final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
            cols = ['代码', 'name', '现价', '支撑位', '横盘天数', '缩量占比', '历史胜率', '买入信号强度', '全自动复盘建议']
            final_df = final_df[cols].rename(columns={'name': '股票名称'})
        
        # 5 星级内部按胜率和缩量程度再次排序
        final_df = final_df.sort_values(by=['历史胜率', '缩量占比'], ascending=[False, True])

        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        out_file = os.path.join(dir_path, f"limit_up_golden_phoenix_{timestamp}.csv")
        
        final_df.to_csv(out_file, index=False, encoding='utf-8-sig')
        print(f"🔥 复盘完成！今日发现 {len(final_df)} 只 5 星级标的。")
        print(final_df[['代码', '股票名称', '历史胜率', '缩量占比']].to_string(index=False))
    else:
        print("💡 今日未发现 5 星级‘一击必中’标的，建议空仓或观察 4 星以下品种。")

if __name__ == "__main__":
    main()
