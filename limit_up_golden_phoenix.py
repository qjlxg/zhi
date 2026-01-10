import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==============================================================================
# 战法名称：涨停金凤凰 (Limit Up Golden Phoenix) - 完整实战版
# 
# 【战法逻辑说明】：
# 1. 核心定义：寻找近期出现过涨停（涨幅 >= 9.8%）的领涨标的。
# 2. 支撑逻辑：涨停后的“炸板”或“横盘”期间，每日收盘价严禁有效跌破涨停当日最高价。
# 3. 缩量核心：洗盘期间成交量必须缩减（地量），证明主力筹码锁定，无出货意愿。
# 4. 择时优化：选取涨停后 2-7 天的标的，避开过早（未洗完）或过晚（动力衰竭）。
# 5. 回测驱动：通过历史数据计算该股“股性”，只有历史表现好的股才会被标记为“高强度”。
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

        # 2. 识别涨停信号
        df['is_limit_up'] = df['涨跌幅'] >= 9.8
        limit_indices = df[df['is_limit_up']].index
        if limit_indices.empty: return None
        
        # 3. 历史回测部分：计算该战法在历史上的表现
        success_count = 0
        total_signals = 0
        for idx in limit_indices:
            # 确保涨停后有足够数据进行回测统计 (至少看5天)
            if idx + 8 >= len(df): continue
            
            h = df.loc[idx, '最高']
            v = df.loc[idx, '成交量']
            # 模拟：涨停后3天内不破位且缩量
            obs = df.loc[idx+1 : idx+3]
            if obs['收盘'].min() >= h * 0.99 and obs['成交量'].max() < v * 0.7:
                total_signals += 1
                buy_price = df.loc[idx+3, '收盘']
                post_max = df.loc[idx+4 : idx+8, '最高'].max()
                if (post_max - buy_price) / buy_price >= 0.05: # 5%涨幅算成功
                    success_count += 1
        
        win_rate = success_count / total_signals if total_signals > 0 else 0.0

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
        is_vol_shrink = latest['成交量'] < (limit_vol * 0.6) # 当前量不到涨停量的60%
        
        if is_supported and is_vol_shrink:
            # 5. 自动复盘分级与操作建议
            if win_rate >= 0.6:
                strength = "⭐⭐⭐⭐⭐ [一击必中]"
                advice = "该股历史爆发力极强！缩量回踩完毕，建议现价或回踩支撑位分批建仓。"
            elif win_rate >= 0.4:
                strength = "⭐⭐⭐⭐ [积极观察]"
                advice = "形态非常标准，历史胜率尚可。可小仓位试错。"
            else:
                strength = "⭐⭐ [形态观察]"
                advice = "形态符合但该股历史胜率一般，建议等待放量起爆瞬间再介入。"

            return {
                "代码": code,
                "日期": latest['日期'],
                "现价": cur_close,
                "支撑位": limit_high,
                "缩量占比": f"{(latest['成交量']/limit_vol):.1%}",
                "横盘天数": days_count,
                "历史胜率": f"{win_rate:.1%}",
                "买入信号强度": strength,
                "全自动复盘建议": advice
            }
        return None
    except Exception:
        return None

def main():
    # 获取 stock_data 目录下所有 CSV 文件
    if not os.path.exists(DATA_DIR):
        print(f"错误: 找不到目录 {DATA_DIR}")
        return

    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    print(f"正在分析 {len(files)} 只股票，采用并行计算方案...")

    # 并行处理
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_logic, files)
    
    # 过滤空结果
    results = [r for r in results if r is not None]
    
    if results:
        final_df = pd.DataFrame(results)
        
        # 关联股票名称
        if os.path.exists(NAMES_FILE):
            names_df = pd.read_csv(NAMES_FILE)
            # 统一代码格式为6位字符串
            names_df['code'] = names_df['code'].astype(str).str.zfill(6)
            final_df = pd.merge(final_df, names_df, left_on='代码', right_on='code', how='left')
            # 整理列顺序
            cols = ['代码', 'name', '现价', '支撑位', '横盘天数', '缩量占比', '历史胜率', '买入信号强度', '全自动复盘建议']
            final_df = final_df[cols].rename(columns={'name': '股票名称'})
        
        # 排序：按历史胜率和缩量占比优选
        final_df = final_df.sort_values(by=['历史胜率', '缩量占比'], ascending=[False, True])

        # 保存到年月目录
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        out_file = os.path.join(dir_path, f"limit_up_golden_phoenix_{timestamp}.csv")
        
        final_df.to_csv(out_file, index=False, encoding='utf-8-sig')
        print(f"成功筛选出 {len(final_df)} 只符合‘金凤凰’战法的股票。结果已保存至 {out_file}")
    else:
        print("今日未发现符合战法逻辑的股票，建议空仓休息。")

if __name__ == "__main__":
    main()
