import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：【断板反包 · 顶级一击必中】
# 核心逻辑：
# 1. 控盘(异动)：5日内出现过涨停板，确认为主力活跃股。
# 2. 洗盘(暴力)：涨停后必须有缩量回调，且回调深度要足够，洗掉获利盘。
# 3. 护盘(地量)：当前成交量必须萎缩至极点（地量见底），且价格在涨停起始位企稳。
# 4. 逻辑：主力利用断板大阴线恐吓散户，随后地量守住基准价，次日反包即是启动点。
# ==========================================

def evaluate_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 25: return None
        
        df = df.sort_values('日期').reset_index(drop=True)
        last_row = df.iloc[-1]
        code = str(last_row['股票代码']).zfill(6)
        price = last_row['收盘']
        
        # --- 条件1：基础过滤 (沪深A股，排除ST/30/68/北交) ---
        if not (5.0 <= price <= 20.0): return None
        if code.startswith(('30', '68', '4', '8', '9')): return None
        
        recent = df.tail(10).copy().reset_index(drop=True)
        # 寻找最近10天内的涨停板
        limit_up_idx = recent[recent['涨跌幅'] > 9.8].index.tolist()
        if not limit_up_idx: return None
        
        idx = limit_up_idx[-1]
        if idx >= len(recent) - 1: return None # 排除今日涨停
        
        limit_up_row = recent.loc[idx]
        post_limit = recent.loc[idx+1:]
        
        # --- 条件2：核心量化指标 ---
        # 1. 缩量比：今日量 / 涨停量 (越小代表洗盘越彻底)
        vol_shrink_ratio = last_row['成交量'] / limit_up_row['成交量']
        # 2. 价格回撤比：今日收盘离涨停开盘价的距离 (判断支撑强度)
        support_distance = (price - limit_up_row['开盘']) / limit_up_row['开盘']
        # 3. 洗盘幅度：涨停后最高价到今日收盘的回撤
        drop_depth = (price - recent['最高'].max()) / recent['最高'].max()

        # --- 条件3：严格入围准则 ---
        # 极致地量：量能缩减至 35% 以下； 关键位：跌幅不破涨停起始位2%
        if vol_shrink_ratio > 0.35 or support_distance < -0.02:
            return None

        # --- 条件4：自动复盘评分系统 (总分100) ---
        score = 0
        # A. 量能评分 (最高50分)：量越小分越高，地量是反转的前提
        score += max(0, (0.4 - vol_shrink_ratio) * 125)
        # B. 支撑评分 (最高30分)：越接近涨停起始位说明主力控盘越稳
        score += max(0, (0.05 - abs(support_distance)) * 600)
        # C. 趋势评分 (最高20分)：近期未大幅破位
        score += 20 if last_row['涨跌幅'] > -2.0 else 0

        # --- 条件5：操作决策生成 ---
        if score >= 80:
            signal, advice = "★★★★★ 极强", "【一击必中】主力地量护盘极精准，次日放量翻红即买入，止损位设为涨停开盘价。"
        elif score >= 60:
            signal, advice = "★★★☆ 强", "【分批试错】缩量企稳信号明显，可轻仓建立底仓，等待反包大阳确认。"
        elif score >= 40:
            signal, advice = "★★☆☆ 观察", "【继续观察】形态尚可但量能未达极致地量，等待关键位缩量十字星。"
        else:
            signal, advice = "★☆☆☆ 放弃", "【暂不介入】支撑力度一般，主力意图不明。"

        return {
            '代码': code,
            '评分': round(score, 1),
            '信号': signal,
            '操作建议': advice,
            '今日价格': price,
            '量能缩比': f"{round(vol_shrink_ratio*100, 1)}%",
            '支撑距离': f"{round(support_distance*100, 2)}%"
        }
            
    except Exception:
        return None

def main():
    stock_data_path = 'stock_data/*.csv'
    stock_names_file = 'stock_names.csv'
    
    # 加载股票名称
    names_df = pd.read_csv(stock_names_file, dtype={'code': str})
    name_map = dict(zip(names_df['code'].str.zfill(6), names_df['name']))
    
    files = glob.glob(stock_data_path)
    with Pool(cpu_count()) as p:
        results = p.map(evaluate_stock, files)
    
    valid_results = [r for r in results if r is not None]
    for r in valid_results:
        r['名称'] = name_map.get(r['代码'], "未知")
    
    # 过滤ST并按评分降序，只取前5名实现“优中选优”
    final_list = [r for r in valid_results if "ST" not in r['名称'] and "st" not in r['名称']]
    final_list = sorted(final_list, key=lambda x: x['评分'], reverse=True)[:5]

    if final_list:
        output_df = pd.DataFrame(final_list)
        # 优化输出列顺序
        cols = ['代码', '名称', '评分', '信号', '操作建议', '今日价格', '量能缩比', '支撑距离']
        output_df = output_df[cols]
        
        now = datetime.now()
        dir_path = now.strftime('%Y-%m')
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"战法自动复盘_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_path, file_name)
        output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"复盘完毕。已筛选出 {len(final_list)} 只精选标的。")
    else:
        print("今日行情未能触发'一击必中'极致筛选条件。")

if __name__ == "__main__":
    main()
