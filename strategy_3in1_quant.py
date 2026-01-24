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

"""
战法名称：【精选三合一：水上共振猎手】
战法逻辑说明：
1. 价格共振：当日涨幅在 5% 到 9.5% 之间。排除封死涨停（难买入）和弱势拉升。
2. 量能加速：成交量 > 过去5日平均成交量的 2 倍。确保是主力主动性买入而非对倒。
3. 趋势共振：MACD DIFF线在0轴上方（强势区），且 MACD 柱值增长。
4. 活跃度过滤：换手率在 3% - 12% 之间。确保资金活跃但不过热。
5. 均线过滤：股价站稳 20日均线，且 20日线斜率向上，确保处于上升通道。

复盘要领：
- 核心：只做上升趋势中的倍量起爆，不做下跌通道的超跌反弹。
- 入场：次日关注分时图，若回踩今日收盘价附近企稳可分批介入。
- 止损：今日大阳线实体的 1/2 处或最低价。
"""

def is_main_board_sh_sz(code):
    """
    过滤逻辑：
    - 排除 30开头（创业板）, 68开头（科创板）
    - 只保留 60, 00, 002 开头的沪深主板
    """
    code = str(code).zfill(6)
    if code.startswith('30') or code.startswith('68'):
        return False
    if code.startswith('60') or code.startswith('00'):
        return True
    return False

def analyze_single_stock(file_path, name_map):
    try:
        # 读取数据
        df = pd.read_csv(file_path)
        if len(df) < 60: return None # 确保数据量足够计算均线
        
        # 字段名适配 (基于用户CSV格式)
        # 格式: 日期, 股票代码, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
        df = df.rename(columns={
            '日期': 'date', '股票代码': 'code', '收盘': 'close', 
            '成交量': 'volume', '涨跌幅': 'pct_chg', '换手率': 'turnover'
        })
        
        code = str(df['code'].iloc[-1]).zfill(6)
        
        # --- 1. 基础硬性过滤 ---
        if not is_main_board_sh_sz(code): return None
        
        curr_price = df['close'].iloc[-1]
        if not (5.0 <= curr_price <= 20.0): return None
        
        stock_name = name_map.get(code, "未知")
        if 'ST' in stock_name or '*' in stock_name: return None

        # --- 2. 指标计算 ---
        # 均线
        df['ma20'] = df['close'].rolling(20).mean()
        # MACD
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        # 5日均量
        vol_5_avg = df['volume'].rolling(5).mean().iloc[-2]

        # --- 3. 优化后的战法逻辑判定 ---
        # A. 价格：涨幅强势但未封死 (5%~9.5%)
        cond_price = 5.0 <= df['pct_chg'].iloc[-1] <= 9.6
        
        # B. 量能：爆量（今日量 > 5日均量2倍）且换手活跃 (3%-12%)
        cond_vol = (df['volume'].iloc[-1] > vol_5_avg * 2.0) and (3.0 <= df['turnover'].iloc[-1] <= 12.0)
        
        # C. 趋势：MA20向上且股价在MA20上（主升通道）
        cond_trend = (df['close'].iloc[-1] > df['ma20'].iloc[-1]) and (df['ma20'].iloc[-1] > df['ma20'].iloc[-2])
        
        # D. 动能：MACD水上金叉或强发散 (DIFF > 0 且 红柱增长)
        cond_macd = (df['diff'].iloc[-1] > 0) and (df['macd'].iloc[-1] > df['macd'].iloc[-2])

        if cond_price and cond_vol and cond_trend and cond_macd:
            return {
                'date': df['date'].iloc[-1],
                'code': code,
                'name': stock_name,
                'price': curr_price,
                'pct_chg': df['pct_chg'].iloc[-1],
                'turnover': df['turnover'].iloc[-1],
                'vol_ratio': round(df['volume'].iloc[-1] / vol_5_avg, 2)
            }
    except Exception:
        return None
    return None

def run():
    # 加载名称映射
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = {str(c).zfill(6): n for c, n in zip(name_df['code'], name_df['name'])}

    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    # 并行扫描提升效率
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_single_stock, f, name_map) for f in files]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    if results:
        res_df = pd.DataFrame(results)
        now = datetime.now()
        dir_path = f"results/{now.strftime('%Y-%m')}"
        if not os.path.exists(dir_path): os.makedirs(dir_path)
        
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        
        res_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，发现 {len(res_df)} 个目标")
    else:
        print("今日未发现高胜率符合条件标的")

if __name__ == "__main__":
    run()
