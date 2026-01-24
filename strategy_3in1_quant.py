import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# --- 核心配置 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
STRATEGY_NAME = 'strategy_3in1_quant'

def is_main_board_sh_sz(code):
    code = str(code).zfill(6)
    if code.startswith('30') or code.startswith('68'): return False
    return code.startswith('60') or code.startswith('00')

def analyze_single_stock(file_path, name_map):
    try:
        # 1. 加载并映射数据
        df = pd.read_csv(file_path).rename(columns={
            '日期': 'date', '股票代码': 'code', '开盘': 'open', '收盘': 'close',
            '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount',
            '振幅': 'amplitude', '涨跌幅': 'pct_chg', '换手率': 'turnover'
        })
        if len(df) < 60: return None 
        
        code = str(df['code'].iloc[-1]).zfill(6)
        if not is_main_board_sh_sz(code): return None
        
        curr_price = df['close'].iloc[-1]
        if not (5.0 <= curr_price <= 20.0): return None
        
        stock_name = name_map.get(code, "未知")
        if any(x in stock_name for x in ['ST', '*', '退']): return None

        # --- 计算核心筛选指标 ---
        df['ma20'] = df['close'].rolling(20).mean()
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd_val'] = (df['diff'] - df['dea']) * 2
        
        vol_5_avg = df['volume'].rolling(5).mean().iloc[-2]
        vol_ratio = round(df['volume'].iloc[-1] / vol_5_avg, 2)
        slope_20 = round((df['ma20'].iloc[-1] - df['ma20'].iloc[-2]) / df['ma20'].iloc[-2] * 100, 3)

        # --- 【严苛筛选：核心 5 大逻辑不变】 ---
        # 1. 空间突破：创 10 日新高
        cond1 = curr_price >= df['close'].iloc[-11:-1].max()
        # 2. 量能极致：量比 > 2.5 且 换手 4%-10%
        cond2 = (vol_ratio > 2.5) and (4.0 <= df['turnover'].iloc[-1] <= 10.0)
        # 3. 趋势斜率：MA20 向上且具备进攻角度
        cond3 = (curr_price > df['ma20'].iloc[-1]) and (slope_20 > 0.001)
        # 4. K线质量：阳线且上影线短
        entity = curr_price - df['open'].iloc[-1]
        upper_shadow = df['high'].iloc[-1] - curr_price
        cond4 = (entity > 0) and (upper_shadow < entity * 0.3)
        # 5. 动能加速：MACD DIFF 水上且加速
        cond5 = (df['diff'].iloc[-1] > 0) and (df['diff'].iloc[-1] > df['diff'].iloc[-2]) and (df['macd_val'].iloc[-1] > df['macd_val'].iloc[-2])

        # 核心筛选触发
        if all([cond1, cond2, cond3, cond4, cond5]):
            # --- 【补充内容：仅用于结果显示，不参与筛选】 ---
            # RSI(14)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rsi = round(100 - (100 / (1 + gain/loss)).iloc[-1], 2)
            
            # KDJ(9)
            low_9 = df['low'].rolling(9).min()
            high_9 = df['high'].rolling(9).max()
            rsv = (df['close'] - low_9) / (high_9 - low_9) * 100
            k = rsv.ewm(com=2).mean()
            d = k.ewm(com=2).mean()
            kdj_j = round(3 * k.iloc[-1] - 2 * d.iloc[-1], 2)
            
            # 乖离率与布林带
            ma5 = df['close'].rolling(5).mean().iloc[-1]
            bias_5 = round((curr_price - ma5) / ma5 * 100, 2)
            std_20 = df['close'].rolling(20).std().iloc[-1]
            boll_pct = round((curr_price - (df['ma20'].iloc[-1] - 2*std_20)) / (4*std_20) * 100, 2)

            return {
                'date': df['date'].iloc[-1],
                'code': code,
                'name': stock_name,
                'price': curr_price,
                'pct_chg': df['pct_chg'].iloc[-1],
                'amount_M': round(df['amount'].iloc[-1] / 1000000, 2),
                'turnover': df['turnover'].iloc[-1],
                'vol_ratio': vol_ratio,
                'ma20_slope': slope_20,
                # 以下为新增补充字段，供复盘对比
                'rsi_14': rsi,
                'kdj_j': kdj_j,
                'bias_ma5': bias_5,
                'boll_pct': boll_pct,
                'amplitude': df['amplitude'].iloc[-1]
            }
    except Exception: return None
    return None

def run():
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = {str(c).zfill(6): n for c, n in zip(name_df['code'], name_df['name'])}

    files = glob.glob(f"{DATA_DIR}/*.csv")
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_single_stock, f, name_map) for f in files]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    if results:
        res_df = pd.DataFrame(results).sort_values('amount_M', ascending=False)
        now = datetime.now()
        dir_path = f"results/{now.strftime('%Y-%m')}"
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        res_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成！发现 {len(res_df)} 只精英标的。已保存至 {full_path}")
    else:
        print("今日未发现符合【精英版·三合一】严选条件的标的")

if __name__ == "__main__":
    run()
