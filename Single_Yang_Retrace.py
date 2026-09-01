import pandas as pd
import glob
import os
from datetime import datetime, timedelta

def run_strategy_7day_limit():
    # --- 1. 环境准备 ---
    target_dir = os.path.join('results', 'Single_Yang_Retrace')
    os.makedirs(target_dir, exist_ok=True)
    
    name_map = {}
    if os.path.exists('stock_names.csv'):
        for enc in ['utf-8', 'gbk', 'utf-8-sig']:
            try:
                names_df = pd.read_csv('stock_names.csv', dtype={'code': str}, encoding=enc)
                names_df.columns = names_df.columns.str.strip()
                names_df['code'] = names_df['code'].str.strip().str.zfill(6)
                name_map = dict(zip(names_df['code'], names_df['name']))
                break
            except: continue

    files = glob.glob('stock_data/*.csv')
    results = []
    current_time = datetime.now()
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = df.columns.str.strip()
            df['日期'] = pd.to_datetime(df['日期']) # 转换为日期格式进行比较
            df = df.sort_values('日期').reset_index(drop=True)
            
            df['MA5'] = df['收盘'].rolling(window=5).mean()
            
            last_idx = len(df) - 1
            curr_row = df.iloc[last_idx]
            
            # 基础过滤：价格 5-20元 & 站稳5日线
            if curr_row['收盘'] < curr_row['MA5']: continue
            if not (5.0 <= curr_row['收盘'] <= 20.0): continue
                
            code = str(curr_row['股票代码']).split('.')[0].zfill(6)
            name = name_map.get(code, "未知")

            # 寻找 1-5 天内的单阳
            for gap in range(1, 6):
                yang_idx = last_idx - gap
                if yang_idx < 0: continue
                
                row_yang = df.iloc[yang_idx]
                
                # --- 核心新增：单阳日必须在 7 天内 ---
                # 考虑交易日，这里用简单的日期差过滤掉太久远的信号
                days_diff = (curr_row['日期'] - row_yang['日期']).days
                if days_diff > 7: 
                    continue # 超过 7 天的旧单阳直接不要了
                
                adjust_period = df.iloc[yang_idx + 1:]
                
                # 缩量 + 不破最低价
                cond_vol = (adjust_period['成交量'] < row_yang['成交量']).all()
                cond_hold = (adjust_period['收盘'] >= row_yang['最低']).all()
                
                if not (cond_vol and cond_hold): continue

                yang_mid = (row_yang['收盘'] + row_yang['开盘']) / 2
                
                match_data = {
                    '代码': code, '名称': name, '调整天数': gap, 
                    '单阳日': row_yang['日期'].strftime('%Y-%m-%d'), 
                    '现价': curr_row['收盘'], '筛选时间': current_time_str
                }

                if row_yang['涨跌幅'] >= 8.0 and 1 <= gap <= 3:
                    if (adjust_period['收盘'] >= yang_mid).all():
                        match_data['类型'] = '强势型'
                        results.append(match_data)
                        break
                elif row_yang['涨跌幅'] >= 5.0 and 4 <= gap <= 5:
                    match_data['类型'] = '中势型'
                    results.append(match_data)
                    break
        except:
            continue

    # --- 3. 排序与多重保存 ---
    if results:
        res_df = pd.DataFrame(results)
        # 类型升序（中势在前，强势在后），内部天数倒序（5->1）
        res_df = res_df.sort_values(by=['类型', '调整天数'], ascending=[True, False])
        
        res_df.to_csv("Latest_Result.csv", index=False, encoding='utf_8_sig')
        history_path = os.path.join(target_dir, f"Screen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        res_df.to_csv(history_path, index=False, encoding='utf_8_sig')
        
        print(f"\n✅ 筛选完成！已剔除 7 天前的陈旧信号。")
        print(res_df.to_string(index=False))
    else:
        print(f"[{current_time_str}] 未匹配到 7 天内符合条件的个股。")

if __name__ == '__main__':
    run_strategy_7day_limit()
