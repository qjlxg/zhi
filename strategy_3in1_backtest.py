import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

# --- 配置区 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
# 账本记录起始日期：设为昨天
LEDGER_START_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def backtest_stock(file_path, name_map):
    """
    全历史并行回测逻辑：
    1. 扫描过去 10 年（取决于你的 CSV 数据量）计算胜率
    2. 过滤出近期信号存入账本
    """
    try:
        df = pd.read_csv(file_path).rename(columns={
            '日期':'date','股票代码':'code','收盘':'close','开盘':'open',
            '最高':'high','最低':'low','成交量':'volume','涨跌幅':'pct_chg','换手率':'turnover'
        })
        if len(df) < 120: return [], []
        
        code = str(df['code'].iloc[-1]).zfill(6)
        name = name_map.get(code, "未知")
        
        # 基础过滤：仅限沪深主板，排除 ST
        if not (code.startswith('60') or code.startswith('00')): return [], []
        if 'ST' in name or '*' in name: return [], []
        
        # --- 指标预计算 (基于 10 年历史) ---
        df['ma20'] = df['close'].rolling(20).mean()
        df['diff'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
        df['dea'] = df['diff'].ewm(span=9).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        df['v5'] = df['volume'].rolling(5).mean().shift(1)
        df['h10'] = df['close'].rolling(10).max().shift(1)

        all_history_signals = []
        ledger_signals = []

        for i in range(25, len(df)):
            # --- 精英三合一核心逻辑 ---
            # 1. 突破 + 巨阳 + 倍量
            c_break = df['close'].iloc[i] >= df['h10'].iloc[i]
            c_price = 5.0 <= df['pct_chg'].iloc[i] <= 9.6
            c_vol = (df['volume'].iloc[i] > df['v5'].iloc[i] * 2.5) and (4.0 <= df['turnover'].iloc[i] <= 10.0)
            # 2. 均线 + MACD 水上加速
            c_trend = (df['ma20'].iloc[i] > df['ma20'].iloc[i-1] * 1.001) and (df['diff'].iloc[i] > 0)
            # 3. K线质量
            entity = df['close'].iloc[i] - df['open'].iloc[i]
            c_k = (entity > 0) and ((df['high'].iloc[i] - df['close'].iloc[i]) < entity * 0.3)

            if all([c_break, c_price, c_vol, c_trend, c_k]):
                entry_date = df['date'].iloc[i]
                res = {'date': entry_date, 'code': code, 'name': name, 'price': df['close'].iloc[i]}
                
                # 计算 1,3,5,10,20,30,60 天收益 (用于胜率统计)
                for d in [1, 3, 5, 10, 20, 30, 60]:
                    if i + d < len(df):
                        res[f'day_{d}_ret'] = round((df['close'].iloc[i+d] - res['price']) / res['price'] * 100, 2)
                
                all_history_signals.append(res)
                
                # 如果是昨天及之后的信号，加入虚拟账本
                if entry_date >= LEDGER_START_DATE:
                    ledger_signals.append(res)
                    
        return all_history_signals, ledger_signals
    except:
        return [], []

def run():
    print(f"开始并行分析... (历史数据长度: 全量 / 账本起点: {LEDGER_START_DATE})")
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    full_history = []
    recent_ledger = []
    
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(backtest_stock, files, [name_map]*len(files)))
        for hist, ledg in results:
            full_history.extend(hist)
            recent_ledger.extend(ledg)
    
    # 1. 处理 10 年大统计
    if full_history:
        df_h = pd.DataFrame(full_history)
        print("\n" + "="*40 + "\n【10年历史回测胜率汇总】\n" + "="*40)
        stats = []
        for d in [1, 3, 5, 10, 20, 30, 60]:
            col = f'day_{d}_ret'
            v = df_h[df_h[col].notnull()]
            stats.append({'周期': f'{d}天', '胜率': f'{(v[col]>0).mean():.2%}', '均收': f'{v[col].mean():.2f}%'})
        print(pd.DataFrame(stats).to_string(index=False))

    # 2. 处理“从昨天开始”的账本
    os.makedirs('backtest_results', exist_ok=True)
    df_l = pd.DataFrame(recent_ledger)
    ledger_path = "backtest_results/virtual_ledger_recent.csv"
    # 按日期降序排列，方便查看
    if not df_l.empty:
        df_l.sort_values('date', ascending=False).to_csv(ledger_path, index=False, encoding='utf-8-sig')
        print(f"\n[成功] 虚拟账本已更新 (包含 {len(df_l)} 条近期记录): {ledger_path}")
    else:
        # 如果昨天没信号，创建一个空表带表头，防止后续程序报错
        pd.DataFrame(columns=['date','code','name','price']).to_csv(ledger_path, index=False)
        print(f"\n[提示] 近期（{LEDGER_START_DATE}之后）暂无新信号。")

if __name__ == "__main__":
    run()
