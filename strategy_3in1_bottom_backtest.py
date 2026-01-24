import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

# --- 配置区 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
LEDGER_START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

def backtest_logic_bottom(file_path, name_map):
    try:
        # 匹配你的CSV格式
        df = pd.read_csv(file_path).rename(columns={
            '日期':'date','股票代码':'code','收盘':'close','开盘':'open',
            '最高':'high','最低':'low','成交量':'volume','成交额':'amount',
            '涨跌幅':'pct_chg','换手率':'turnover'
        })
        if len(df) < 120: return [], []
        
        code = str(df['code'].iloc[-1]).zfill(6)
        name = name_map.get(code, "未知")
        if not (code.startswith('60') or code.startswith('00')) or 'ST' in name: return [], []
        
        # --- 计算指标 ---
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd_val'] = (df['diff'] - df['dea']) * 2
        df['ma5'] = df['close'].rolling(5).mean()

        all_history, ledger_recent = [], []

        for i in range(30, len(df)):
            # 1. 逻辑 A: 巨阳 + 倍量 + MACD柱增长
            c_3in1 = (df['pct_chg'].iloc[i] > 5) and \
                     (df['volume'].iloc[i] > df['volume'].iloc[i-1] * 1.8) and \
                     (df['macd_val'].iloc[i] > df['macd_val'].iloc[i-1])
            
            # 2. 逻辑 B: 0轴下二次金叉 (0轴下 + 刚好金叉)
            c_bottom = (df['diff'].iloc[i] < 0) and \
                       (df['diff'].iloc[i-1] <= df['dea'].iloc[i-1]) and \
                       (df['diff'].iloc[i] > df['dea'].iloc[i])

            if c_3in1 and c_bottom:
                entry_date = df['date'].iloc[i]
                price = df['close'].iloc[i]
                entity = price - df['open'].iloc[i]
                
                res = {
                    'date': entry_date, 'code': code, 'name': name, 'price': price,
                    'amount_M': round(df['amount'].iloc[i]/1000000, 2),
                    'bias_ma5': round((price - df['ma5'].iloc[i])/df['ma5'].iloc[i]*100, 2)
                }
                
                # 计算收益
                for d in [1, 3, 5, 10, 20]:
                    if i + d < len(df):
                        res[f'day_{d}_ret'] = round((df['close'].iloc[i+d] - price) / price * 100, 2)
                
                all_history.append(res)
                if entry_date >= LEDGER_START_DATE: ledger_recent.append(res)
                    
        return all_history, ledger_recent
    except: return [], []

def run_backtest():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 底部反转战法：全量并行回测中...")
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    full_h, ledger_r = [], []
    with ProcessPoolExecutor() as executor:
        for h, l in executor.map(backtest_logic_bottom, files, [name_map]*len(files)):
            full_h.extend(h); ledger_r.extend(l)
    
    if full_h:
        df_h = pd.DataFrame(full_h)
        print("\n" + "="*95 + "\n【10年全量回测：底部反转二次金叉版】\n" + "="*95)
        stats = []
        for d in [1, 3, 5, 10, 20]:
            col = f'day_{d}_ret'
            v = df_h[df_h[col].notnull()][col]
            if not v.empty:
                wins = v[v > 0]
                losses = v[v <= 0]
                avg_win, avg_loss = wins.mean(), abs(losses.mean())
                sharpe = (v.mean() / v.std()) if v.std() != 0 else 0
                stats.append({
                    '周期': f'{d}天', '样本': len(v), '胜率': f'{(len(wins)/len(v)):.1%}',
                    '盈亏比': f'{(avg_win/avg_loss if avg_loss !=0 else 0):.2f}',
                    '夏普': f'{sharpe:.2f}', '单笔极亏%': f'{v.min():.2f}', '平均盈%': f'{v.mean():.2f}'
                })
        print(pd.DataFrame(stats).to_string(index=False))

    # 保存账本
    os.makedirs('backtest_results', exist_ok=True)
    pd.DataFrame(ledger_r).sort_values('date', ascending=False).to_csv(
        "backtest_results/bottom_ledger_recent.csv", index=False, encoding='utf-8-sig'
    )
    print(f"\n[成功] 底部回测账本已更新。")

if __name__ == "__main__":
    run_backtest()
