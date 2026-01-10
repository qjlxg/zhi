import pandas as pd
import os
import glob
from datetime import datetime
from joblib import Parallel, delayed

# ==========================================
# 战法名称：N字涨停回踩杀入战法 (极致优选完整版)
# 核心逻辑 (坚决不改)：
# 1. 启动阶段 (A柱)：涨幅 > 9.5%，放量确立。
# 2. 洗盘阶段 (B区)：连续 5-7 天缩量回调，K线实体小。
# 3. 支撑逻辑 (C点)：回调最低价不破启动板开盘价。
# 
# 优化增强 (优中选优)：
# - 换手阶梯：回调期日均换手 < 启动日换手率的 1/3。
# - 抛压剔除：回调期单日上影线 > 3% 且次数 >= 2 次的直接排除。
# - 板块共振：同类号段股票集体出现时，标注【共振】。
# ==========================================

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 20: return None
        
        # 基础数据清洗与排序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 1. 基础硬性筛选 (5-20元, 排除ST/创业板)
        last_bar = df.iloc[-1]
        last_price = last_bar['收盘']
        if not (5.0 <= last_price <= 20.0): return None
        if code.startswith(('30', '68', '8', '4')): return None 
        name = stock_names.get(code, "未知")
        if 'ST' in name: return None

        # 2. 定位“启动板” (核心逻辑 A)
        potential_start = df.iloc[-12:-4] 
        start_points = potential_start[potential_start['涨跌幅'] >= 9.5]
        if start_points.empty: return None
        
        start_bar = start_points.iloc[-1]
        start_idx = start_bar.name
        
        # 3. 分析回调区间 (核心逻辑 B)
        callback_df = df.iloc[start_idx + 1:]
        cb_len = len(callback_df)
        if not (5 <= cb_len <= 8): return None 

        # --- 增强过滤：换手率阶梯 (洗盘干净) ---
        start_turnover = start_bar['换手率']
        avg_cb_turnover = callback_df['换手率'].mean()
        if avg_cb_turnover > (start_turnover / 3.0): return None

        # --- 增强过滤：剔除长上影 (上方无抛压) ---
        # 计算上影线长度占比
        upper_shadows = callback_df.apply(lambda x: (x['最高'] - max(x['开盘'], x['收盘'])) / x['开盘'] > 0.03, axis=1)
        if upper_shadows.sum() >= 2: return None

        # --- 核心支撑逻辑 (核心逻辑 C) ---
        min_low_after = callback_df['最低'].min()
        if min_low_after < start_bar['开盘']: return None

        # 4. 评估与输出
        avg_vol_recent = callback_df['成交量'].tail(3).mean()
        vol_ratio = avg_vol_recent / start_bar['成交量']
        dist_to_support = (last_price - start_bar['开盘']) / start_bar['开盘']
        
        # 信号分级
        strength_score = 0
        if vol_ratio < 0.35: strength_score += 50 
        if dist_to_support < 0.04: strength_score += 50 
        
        strength = "一击必中" if strength_score >= 80 else "优选观察"
        status = "缩量止跌" if last_bar['涨跌幅'] > -1 else "寻底洗盘"
        
        # 针对N字逻辑的操作建议
        if strength == "一击必中":
            advice = "重仓潜伏，止损点设在启动板底价，博反包涨停"
        else:
            advice = "小仓试错，待量能温和放大时加仓"

        return {
            "代码": code,
            "名称": name,
            "启动日期": start_bar['日期'].strftime('%Y-%m-%d'),
            "当前价格": last_price,
            "回调天数": cb_len,
            "缩量比": round(vol_ratio, 2),
            "启动换手": start_turnover,
            "回调均换手": round(avg_cb_turnover, 2),
            "信号强度": strength,
            "状态": status,
            "操作建议": advice
        }
    except Exception:
        return None

def main():
    # 环境加载
    names_df = pd.read_csv('stock_names.csv')
    stock_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    files = glob.glob('stock_data/*.csv')
    
    # 并行运算
    print(f"正在根据N字核心逻辑扫描 {len(files)} 只标的...")
    results = Parallel(n_jobs=-1)(delayed(analyze_stock)(f, stock_dict) for f in files)
    results = [r for r in results if r is not None]
    
    if results:
        final_df = pd.DataFrame(results)
        
        # 行业共振辅助逻辑
        final_df['号段'] = final_df['代码'].str[:3]
        sector_counts = final_df['号段'].value_counts().to_dict()
        final_df['信号强度'] = final_df.apply(
            lambda x: "【共振】" + x['信号强度'] if sector_counts[x['号段']] >= 3 else x['信号强度'], axis=1
        )
        final_df = final_df.drop(columns=['号段'])

        # 保存结果
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        os.makedirs(dir_path, exist_ok=True)
        
        file_name = f"n_strategy_analyzer_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.sort_values(['信号强度', '缩量比'], ascending=[False, True]).to_csv(
            f"{dir_path}/{file_name}", index=False, encoding='utf-8-sig'
        )
        print(f"扫描完毕，共发现 {len(final_df)} 只符合极致缩量N字回踩的标的。")
    else:
        print("今日无符合极致条件的标的。")

if __name__ == "__main__":
    main()
