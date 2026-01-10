import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 战法名称：断板反包智能复盘系统 (顶级精选版) ---
# 核心逻辑：
# 1. 控盘：5日内有缩量涨停。
# 2. 洗盘：涨停后大阴线暴力洗盘，缩量回调，不破支撑位。
# 3. 护盘：地量企稳。
# 4. 评价：根据量价偏差度自动给出买入信号。

def intelligent_reviewer(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        df = df.sort_values('日期').reset_index(drop=True)
        
        last_row = df.iloc[-1]
        code = str(last_row['股票代码']).zfill(6)
        price = last_row['收盘']
        
        # 1. 基础条件：价格区间 + 排除创业板/ST/北交所
        if not (5.0 <= price <= 20.0): return None
        if code.startswith(('30', '68', '4', '8', '9')): return None
        
        recent = df.tail(10).copy()
        limit_up_idx = recent[recent['涨跌幅'] > 9.8].index.tolist()
        if not limit_up_idx: return None
        
        idx = limit_up_idx[-1]
        if idx >= len(recent) - 1: return None # 排除今日涨停
        
        limit_up_row = recent.loc[idx]
        post_limit = recent.loc[idx+1:]
        
        # 2. 核心战法数据计算
        vol_ratio = last_row['成交量'] / limit_up_row['成交量'] # 今日与涨停日比
        rebound_gap = (last_row['收盘'] - limit_up_row['开盘']) / limit_up_row['开盘'] # 离支撑位距离
        wash_depth = (post_limit['最低'].min() - limit_up_row['最高']) / limit_up_row['最高'] # 回调深度
        
        # 3. 严格准入标准
        # - 地量：成交量萎缩至 38% 以下
        # - 支撑：价格不低于涨停开盘价下 1%
        # - 洗盘：回调深度必须低于涨停最高价
        if vol_ratio > 0.38 or rebound_gap < -0.01 or wash_depth > -0.02:
            return None

        # 4. 评分系统 (0-100分)
        score = 0
        # 量能分 (越小越高)
        score += max(0, (0.4 - vol_ratio) * 100) 
        # 支撑分 (越贴近开盘价分数越高，说明主力护盘精准)
        score += max(0, (0.05 - abs(rebound_gap)) * 400)
        
        # 5. 生成决策建议
        strength = "极强" if score > 75 else "强" if score > 50 else "观察"
        if score > 75:
            advice = "【一击必中】主力护盘明显，次日放量翻红即可重仓介入。"
        elif score > 50:
            advice = "【分批试错】缩量到位，可轻仓底仓，等待反包。"
        else:
            advice = "【继续观察】形态尚可，但量能或位置不够极致。"

        return {
            '代码': code,
            '评分': round(score, 2),
            '信号强度': strength,
            '操作建议': advice,
            '今日收盘': price,
            '量能缩比': f"{round(vol_ratio*100, 2)}%"
        }
            
    except Exception:
        return None

def main():
    stock_data_path = 'stock_data/*.csv'
    stock_names_file = 'stock_names.csv'
    
    names_df = pd.read_csv(stock_names_file, dtype={'code': str})
    name_map = dict(zip(names_df['code'].str.zfill(6), names_df['name']))
    
    files = glob.glob(stock_data_path)
    with Pool(cpu_count()) as p:
        results = p.map(intelligent_reviewer, files)
    
    # 过滤并排序结果
    valid_results = [r for r in results if r is not None]
    for r in valid_results:
        name = name_map.get(r['代码'], "未知")
        r['名称'] = name
    
    # 排除ST并按评分降序
    final_list = [r for r in valid_results if "ST" not in r['名称'] and "st" not in r['名称']]
    final_list = sorted(final_list, key=lambda x: x['评分'], reverse=True)[:10] # 只取前10名精选

    if final_list:
        output_df = pd.DataFrame(final_list)
        # 调整列顺序
        cols = ['代码', '名称', '评分', '信号强度', '操作建议', '今日收盘', '量能缩比']
        output_df = output_df[cols]
        
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"顶级精选复盘_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_path, file_name)
        output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成！精选出 {len(final_list)} 只标的，已按评分排序。")
    else:
        print("今日暂无符合'一击必中'逻辑的极品标的。")

if __name__ == "__main__":
    main()
