import pandas as pd
import os
import glob
from datetime import datetime
from joblib import Parallel, delayed

# ==========================================
# 战法名称：N字涨停回踩杀入战法 (优选增强版)
# 逻辑核心：
# 1. 启动阶段：前期出现过大阳线或涨停（涨幅 > 9.5%），且成交量明显放大。
# 2. 洗盘阶段：连续 5-7 天缩量回调，K线实体小（小阴小阳）。
# 3. 支撑逻辑：回调最低价不跌破前期启动板的开盘价。
# 4. 价格区间：5.0 - 20.0 元，排除ST、创业板（30开头）。
# 5. 复盘建议：根据回踩深度和缩量程度给出买入信号强度。
# ==========================================

def analyze_stock(file_path, stock_names):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 15: return None
        
        # 基础数据清洗
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 1. 基础筛选条件
        last_price = df.iloc[-1]['收盘']
        if not (5.0 <= last_price <= 20.0): return None
        if code.startswith('30') or 'ST' in stock_names.get(code, ''): return None

        # 2. 寻找 N 字结构的“首板” (寻找最近 10 天内的启动点)
        # 定义：涨幅 > 9.5% 视为模拟涨停板
        potential_start = df.iloc[-10:-4] 
        start_points = potential_start[potential_start['涨跌幅'] >= 9.5]
        
        if start_points.empty: return None
        
        # 取最近的一个启动板
        start_bar = start_points.iloc[-1]
        start_idx = start_bar.name
        start_date = start_bar['日期']
        
        # 3. 分析启动后的回调阶段 (5-7天)
        callback_df = df.loc[start_idx + 1:]
        if not (5 <= len(callback_df) <= 8): return None # 限制调整天数
        
        # 逻辑：回调不能破位
        min_low_after = callback_df['最低'].min()
        if min_low_after < start_bar['开盘']: return None
        
        # 逻辑：缩量回调 (最近3天平均成交量 < 启动当天成交量 * 0.6)
        avg_vol_now = callback_df['成交量'].tail(3).mean()
        if avg_vol_now > start_bar['成交量'] * 0.7: return None

        # 4. 信号强度评估
        # 越缩量、回踩越接近启动板开盘价但没破位，强度越高
        vol_ratio = avg_vol_now / start_bar['成交量']
        retracement = (last_price - start_bar['开盘']) / start_bar['开盘']
        
        strength = "极强" if vol_ratio < 0.4 and retracement < 0.05 else "较强"
        advice = "现价轻仓试错" if retracement > 0.05 else "重仓潜伏，博弈次日反包"
        
        if last_price > df.iloc[-2]['收盘']: # 今天小幅翻红或企稳
             status = "企稳信号已现"
        else:
             status = "仍在探底观察"

        return {
            "代码": code,
            "名称": stock_names.get(code, "未知"),
            "启动日期": start_date.strftime('%Y-%m-%d'),
            "当前价格": last_price,
            "回调天数": len(callback_df),
            "缩量比": f"{vol_ratio:.2f}",
            "信号强度": strength,
            "状态": status,
            "操作建议": advice
        }
    except Exception as e:
        return None

def main():
    # 加载股票名称
    names_df = pd.read_csv('stock_names.csv')
    stock_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    # 扫描数据目录
    files = glob.glob('stock_data/*.csv')
    
    # 并行处理提高速度
    results = Parallel(n_jobs=-1)(delayed(analyze_stock)(f, stock_dict) for f in files)
    results = [r for r in results if r is not None]
    
    # 结果输出
    if results:
        final_df = pd.DataFrame(results)
        # 优中选优：按缩量比升序排列（越缩量越好）
        final_df = final_df.sort_values('缩量比')
        
        # 创建目录
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            
        file_name = f"n_strategy_analyzer_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_path, file_name)
        
        final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"分析完成，找到 {len(final_df)} 只符合N字潜伏条件的股票。结果已保存至 {save_path}")
    else:
        print("今日无符合条件的股票。")

if __name__ == "__main__":
    main()
