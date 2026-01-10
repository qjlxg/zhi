import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import concurrent.futures

# ==========================================
# 战法名称：【三阴破阳 - 黄金坑洗盘狙击版】
# 战法逻辑：
# 1. 强力基因：过去20个交易日内必须有涨停（涨幅>=9.8%），确认主力深度介入。
# 2. 深度洗盘：涨停后出现连续调整，收盘价曾跌破涨停日开盘价（假摔破位）。
# 3. 地量过滤：回调期间成交量必须极度萎缩（地量 < 涨停日成交量的50%），确认主力未出货。
# 4. 企稳反转：最新价格需重回20日均线之上，或重新站稳涨停日开盘价。
# 5. 严格过滤：价格5-20元，排除ST、创业板(30)、科创板(688)，仅限深沪主板。
# ==========================================

STRATEGY_NAME = "san_yin_po_yang_strategy"
DATA_DIR = "stock_data"
NAMES_FILE = "stock_names.csv"

def analyze_stock(file_path, name_map):
    """
    核心战法筛选逻辑
    """
    try:
        # 1. 读取数据并预处理
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        df = df.sort_values('日期').reset_index(drop=True)
        
        # 2. 基础信息提取
        code = df['股票代码'].iloc[-1].strip("'").zfill(6)
        name = name_map.get(code, "未知")
        last_price = df['收盘'].iloc[-1]
        
        # 3. 硬性条件过滤 (价格 + 板块)
        if not (5.0 <= last_price <= 20.0): return None
        if "ST" in name or code.startswith(('30', '688', '43', '83', '87')): return None

        # 4. 技术指标计算
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        
        # 5. 战法逻辑识别
        # 寻找最近20个交易日内的涨停板
        lookback_df = df.tail(20)
        limit_up_candidates = lookback_df[lookback_df['涨跌幅'] >= 9.8]
        if limit_up_candidates.empty: return None
        
        # 取最近的一次涨停作为基准阳线
        last_limit_idx = limit_up_candidates.index[-1]
        limit_day = df.loc[last_limit_idx]
        limit_open = limit_day['开盘']
        limit_vol = limit_day['成交量']
        
        # 涨停后的交易数据（洗盘区）
        after_limit = df.loc[last_limit_idx + 1:]
        if len(after_limit) < 3: return None # 必须有至少3天洗盘过程
        
        # A. 是否存在跌破涨停开盘价的“假摔”行为
        has_broken = (after_limit['收盘'] < limit_open).any()
        
        # B. 是否存在地量（洗盘成交量 < 涨停日成交量的50%）
        min_vol_after = after_limit['成交量'].min()
        is_shrank = min_vol_after < (limit_vol * 0.5)
        
        # C. 企稳状态判定
        current_ma20 = df['MA20'].iloc[-1]
        is_above_ma20 = last_price >= current_ma20
        is_recovering = last_price >= limit_open * 0.98 # 接近或收复涨停开盘价

        # 6. 综合评分与复盘建议
        if has_broken and is_shrank:
            if is_recovering and is_above_ma20:
                strength = "极高（黄金坑反弹确认）"
                advice = "重仓狙击：洗盘结束信号明确，建议现价介入，止损设在坑底最低点。"
                score = 3
            elif is_shrank:
                strength = "中（地量企稳中）"
                advice = "适量试错：地量已现但反攻力度尚需观察，建议分批小量建仓。"
                score = 2
            else:
                return None
            
            # 返回复盘明细数据（含历史回测核心特征）
            return {
                "代码": code,
                "名称": name,
                "现价": last_price,
                "今日涨跌": f"{df['涨跌幅'].iloc[-1]}%",
                "信号强度": strength,
                "操作建议": advice,
                "逻辑说明": "涨停+假摔+地量收复",
                "主力成本区": limit_open,
                "地量比": round(min_vol_after / limit_vol, 2)
            }
            
    except Exception:
        return None
    return None

def main():
    # A. 加载名称表
    if not os.path.exists(NAMES_FILE):
        print(f"错误: 找不到 {NAMES_FILE}")
        return
    name_df = pd.read_csv(NAMES_FILE)
    name_df['code'] = name_df['code'].astype(str).str.zfill(6)
    name_map = dict(zip(name_df['code'], name_df['name']))

    # B. 扫描数据目录
    files = glob.glob(f"{DATA_DIR}/*.csv")
    if not files:
        print(f"错误: {DATA_DIR} 目录下没有发现CSV文件")
        return

    print(f"[{datetime.now()}] 启动并行复盘系统，扫描量: {len(files)}...")

    # C. 并行执行历史回测与实时筛选
    results = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_stock, f, name_map) for f in files]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    # D. 结果持久化与目录管理
    if results:
        res_df = pd.DataFrame(results)
        # 按强度排序
        res_df = res_df.sort_values(by="信号强度", ascending=False)
        
        # 生成年月目录（上海时区由Actions环境处理或Python处理）
        now = datetime.now()
        dir_name = now.strftime("%Y%m")
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_name, file_name)
        
        res_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成！命中 {len(results)} 只个股。结果已保存至: {full_path}")
    else:
        print(f"[{datetime.now()}] 扫描完毕：当前市场未触发【三阴破阳】严格选股信号。")

if __name__ == "__main__":
    main()
