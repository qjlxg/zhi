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
战法名称：【三合一·精英起爆版】
================================================================
战法核心逻辑 (高胜率筛选准则)：
1. 空间突破：今日收盘价必须创下近 10 个交易日的新高，确保是“破局”而非“震荡”。
2. 量能极致：量比（今日量/5日均量）> 2.5，且换手率处于 4%-10% 的机构活跃区间。
3. 趋势斜率：MA20 均线不仅要向上，且必须具备一定的进攻角度（斜率过滤）。
4. K线质量：要求今日为实体阳线，且上影线极短（< 实体的30%），确保收盘处于高位，无主力反手派发迹象。
5. 动能加速：MACD DIFF线在0轴上方运行，且今日 DIFF > 昨日 DIFF，代表动能二次加速。

操作要领：
- 核心：只做主升浪的第一个“倍量突破点”。
- 止损：以起爆阳线实体的 1/2 位作为关键防守点，跌破即离场。
- 止盈：短线目标 10%-15%，或回落破 5日线止盈。
================================================================
"""

def is_main_board_sh_sz(code):
    """
    排除创业板 (30开头) 和 科创板 (68开头)
    仅保留沪深主板 (60/00开头)
    """
    code = str(code).zfill(6)
    if code.startswith('30') or code.startswith('68'):
        return False
    return code.startswith('60') or code.startswith('00')

def analyze_single_stock(file_path, name_map):
    try:
        # 读取 CSV 原始数据
        df = pd.read_csv(file_path)
        if len(df) < 60: return None 
        
        # 字段名映射 (对应你上传的 CSV 格式)
        df = df.rename(columns={
            '日期': 'date', '股票代码': 'code', '收盘': 'close', 
            '成交量': 'volume', '涨跌幅': 'pct_chg', '换手率': 'turnover',
            '最高': 'high', '最低': 'low', '开盘': 'open'
        })
        
        code = str(df['code'].iloc[-1]).zfill(6)
        
        # --- 1. 基础硬性过滤 ---
        if not is_main_board_sh_sz(code): return None
        
        # 价格区间收紧为 5.0 - 20.0 元
        curr_price = df['close'].iloc[-1]
        if not (5.0 <= curr_price <= 20.0): return None
        
        # 排除 ST, *ST, 退市标的
        stock_name = name_map.get(code, "未知")
        if any(x in stock_name for x in ['ST', '*', '退']): return None

        # --- 2. 核心指标计算 ---
        # 计算 20日均线及斜率
        df['ma20'] = df['close'].rolling(20).mean()
        
        # 计算 MACD (12, 26, 9)
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        
        # 计算 5日均量 (不含今日)
        vol_5_avg = df['volume'].rolling(5).mean().iloc[-2]

        # --- 3. 精英版过滤逻辑判定 ---
        
        # A. 平台突破：收盘价创 10 日新高
        cond_breakout = curr_price >= df['close'].iloc[-11:-1].max()
        
        # B. 涨幅力度：5% 到 9.6% (排除涨停封死无法买入)
        cond_price = 5.0 <= df['pct_chg'].iloc[-1] <= 9.6
        
        # C. 量能极致：量比 > 2.5 且 换手率 4%~10%
        cond_vol = (df['volume'].iloc[-1] > vol_5_avg * 2.5) and (4.0 <= df['turnover'].iloc[-1] <= 10.0)
        
        # D. 趋势斜率：MA20 向上且具备进攻角度
        cond_slope = (curr_price > df['ma20'].iloc[-1]) and (df['ma20'].iloc[-1] > df['ma20'].iloc[-2] * 1.001)
        
        # E. K线质量：上影线长度小于实体的 30% (拒绝冲高回落，主力意图坚决)
        entity = curr_price - df['open'].iloc[-1]
        upper_shadow = df['high'].iloc[-1] - curr_price
        cond_k_quality = (entity > 0) and (upper_shadow < entity * 0.3)
        
        # F. MACD 强共振：DIFF 在 0 轴上方且处于加速状态
        cond_macd = (df['diff'].iloc[-1] > 0) and (df['diff'].iloc[-1] > df['diff'].iloc[-2]) and (df['macd'].iloc[-1] > df['macd'].iloc[-2])

        # 全部条件满足则入选
        if cond_breakout and cond_price and cond_vol and cond_slope and cond_k_quality and cond_macd:
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
    # 建立股票名称映射
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = {str(c).zfill(6): n for c, n in zip(name_df['code'], name_df['name'])}

    # 扫描数据目录
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    # 使用 ProcessPoolExecutor 进行并行扫描，显著缩短运行时间
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_single_stock, f, name_map) for f in files]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    # 结果保存逻辑
    if results:
        res_df = pd.DataFrame(results)
        now = datetime.now()
        # 结果保存到年月目录，如 results/2026-01/
        dir_path = f"results/{now.strftime('%Y-%m')}"
        if not os.path.exists(dir_path): os.makedirs(dir_path)
        
        # 文件名带时间戳，防止重名
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        
        res_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，发现 {len(res_df)} 个精英标的，已存入 {full_path}")
    else:
        print("今日未发现符合【精英版·三合一】条件的标的")

if __name__ == "__main__":
    run()
