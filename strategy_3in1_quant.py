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
战法名称：【三合一：共振猎手】
战法逻辑说明：
1. 价格共振：当日涨幅 > 5%，代表主力攻击欲望强烈，处于强势启动期。
2. 成交共振：当日成交量 > 昨日成交量 * 1.8，代表资金显著介入，属于倍量起爆形态。
3. 趋势共振：MACD柱值(MACD)连续两日增长，代表动能正在加速，过滤掉强弩之末的虚假拉升。

操作要领：
- 买入：筛选出当日符合条件的标的，结合板块热度，在次日寻找回踩支撑位择机入场。
- 卖出：破MA5减仓，破MA10清仓。
"""

def is_main_board_sh_sz(code):
    """
    过滤逻辑：
    - 排除 ST（通常在名称中，这里通过代码和基础过滤筛选）
    - 排除 30开头（创业板）
    - 排除 68开头（科创板）
    - 保留 60/00/002 开头的沪深主板/中小板
    """
    code = str(code).zfill(6)
    if code.startswith('30') or code.startswith('68'):
        return False
    if code.startswith('60') or code.startswith('00'):
        return True
    return False

def analyze_single_stock(file_path, name_map):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 映射字段名
        df = df.rename(columns={
            '日期': 'date', '股票代码': 'code', '收盘': 'close', 
            '成交量': 'volume', '涨跌幅': 'pct_chg'
        })
        
        code = str(df['code'].iloc[-1]).zfill(6)
        
        # --- 基础过滤条件 ---
        # 1. 必须是深沪A股主板
        if not is_main_board_sh_sz(code): return None
        
        # 2. 价格区间：5.0 - 20.0 元
        curr_price = df['close'].iloc[-1]
        if not (5.0 <= curr_price <= 20.0): return None
        
        # 3. 排除 ST (通过名称过滤)
        stock_name = name_map.get(code, "未知")
        if 'ST' in stock_name or '*' in stock_name: return None

        # --- 技术指标计算 ---
        # 计算MACD (快速12, 慢速26, 信号9)
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2

        # --- 战法逻辑判定 ---
        # A. 巨阳：涨幅 > 5%
        cond_price = df['pct_chg'].iloc[-1] > 5
        # B. 倍量：成交量 > 前一日 1.8倍
        cond_vol = df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.8
        # C. 动能：MACD红柱增长或绿柱缩短 (今日柱值 > 昨日柱值)
        cond_macd = df['macd'].iloc[-1] > df['macd'].iloc[-2]

        if cond_price and cond_vol and cond_macd:
            return {
                'date': df['date'].iloc[-1],
                'code': code,
                'name': stock_name,
                'price': curr_price,
                'pct_chg': df['pct_chg'].iloc[-1],
                'volume_ratio': round(df['volume'].iloc[-1] / df['volume'].iloc[-2], 2)
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
    
    # 使用并行处理加快扫描速度
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_single_stock, f, name_map) for f in files]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    if results:
        res_df = pd.DataFrame(results)
        # 结果输出路径：年月目录 (例如 results/2024-05/)
        now = datetime.now()
        dir_path = f"results/{now.strftime('%Y-%m')}"
        if not os.path.exists(dir_path): os.makedirs(dir_path)
        
        # 文件名：脚本名 + 时间戳
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        
        res_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，发现 {len(res_df)} 个目标，已保存至: {full_path}")
    else:
        print("今日未发现符合【三合一】战法标的")

if __name__ == "__main__":
    run()
