import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

# --- 配置 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
LEDGER_START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d') # 账本看最近30天，方便对比

def calculate_indicators(df):
    """计算复盘所需的全部技术维度"""
    # 基础均线与斜率
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['slope_ma20'] = (df['ma20'] - df['ma20'].shift(1)) / df['ma20'].shift(1) * 100
    
    # 乖离率 (距离均线的远近)
    df['bias_ma5'] = (df['close'] - df['ma5']) / df['ma5'] * 100
    
    # MACD
    df['diff'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
    df['dea'] = df['diff'].ewm(span=9).mean()
    df['macd_val'] = (df['diff'] - df['dea']) * 2
    
    # RSI (14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # KDJ (9,3,3)
    low_list = df['lowest'].rolling(9).min()
    high_list = df['highest'].rolling(9).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['k'] = rsv.ewm(com=2).mean()
    df['d'] = df['k'].ewm(com=2).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']
    
    # 布林带位置 (收盘价在带宽中的百分比)
    mid = df['close'].rolling(20).mean()
    std = df['close'].rolling(20).std()
    df['bboll_top'] = mid + 2 * std
    df['bboll_pct'] = (df['close'] - (mid - 2 * std)) / (4 * std) * 100

    return df

def backtest_stock(file_path, name_map):
    try:
        # 匹配你的CSV格式: 日期 股票代码 开盘 收盘 最高 最低 成交量 成交额 振幅 涨跌幅 涨跌额 换手率
        df = pd.read_csv(file_path).rename(columns={
            '日期':'date','股票代码':'code','开盘':'open','收盘':'close',
            '最高':'highest','最低':'lowest','成交量':'volume','成交额':'amount',
            '振幅':'amplitude','涨跌幅':'pct_chg','换手率':'turnover'
        })
        if len(df) < 120: return [], []
        
        df = calculate_indicators(df)
        code = str(df['code'].iloc[-1]).zfill(6)
        name = name_map.get(code, "未知")
        if not (code.startswith('60') or code.startswith('00')) or 'ST' in name: return [], []
        
        # 辅助量能指标
        df['v5_avg'] = df['volume'].rolling(5).mean().shift(1)
        df['h10_max'] = df['close'].rolling(10).max().shift(1)

        all_hist, ledger_rec = [], []

        for i in range(30, len(df)):
            # 核心策略逻辑
            c_price = (5.0 <= df['pct_chg'].iloc[i] <= 9.6) and (df['close'].iloc[i] >= df['h10_max'].iloc[i])
            c_vol = (df['volume'].iloc[i] > df['v5_avg'].iloc[i] * 2.5) and (4.0 <= df['turnover'].iloc[i] <= 12.0)
            c_trend = (df['diff'].iloc[i] > 0) and (df['slope_ma20'].iloc[i] > 0)
            
            if c_price and c_vol and c_trend:
                # 记录“捕获瞬间”的全维度数据，用于对比复盘
                res = {
                    'date': df['date'].iloc[i], 'code': code, 'name': name, 'price': df['close'].iloc[i],
                    'turnover': df['turnover'].iloc[i], 'amount_m': round(df['amount'].iloc[i]/1000000, 2), # 成交额(百万)
                    'amplitude': df['amplitude'].iloc[i], 'bias_ma5': round(df['bias_ma5'].iloc[i], 2),
                    'rsi': round(df['rsi'].iloc[i], 2), 'kdj_j': round(df['j'].iloc[i], 2),
                    'boll_pct': round(df['bboll_pct'].iloc[i], 2), 'ma20_slope': round(df['slope_ma20'].iloc[i], 3)
                }
                # 记录未来收益
                for d in [1, 3, 5, 10, 20]:
                    if i + d < len(df):
                        res[f'day_{d}_ret'] = round((df['close'].iloc[i+d] - res['price']) / res['price'] * 100, 2)
                
                all_hist.append(res)
                if df['date'].iloc[i] >= LEDGER_START_DATE: ledger_rec.append(res)
        return all_hist, ledger_rec
    except: return [], []

def run():
    name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    name_map = dict(zip(name_df['code'].str.zfill(6), name_df['name']))
    files = glob.glob(f"{DATA_DIR}/*.csv")
    
    full_h, ledger_r = [], []
    with ProcessPoolExecutor() as executor:
        for h, l in executor.map(backtest_stock, files, [name_map]*len(files)):
            full_h.extend(h); ledger_r.extend(l)
    
    if full_h:
        df_h = pd.DataFrame(full_h)
        # --- 增加夏普比率统计 ---
        print("\n" + "="*100 + "\n【10年回测质量报告】\n" + "="*100)
        stats = []
        for d in [1, 3, 5, 10, 20]:
            col = f'day_{d}_ret'
            v = df_h[df_h[col].notnull()][col]
            if not v.empty:
                avg_ret = v.mean()
                std_ret = v.std()
                # 夏普比率 (简化版：日均收益/波动率，此处周期不同，仅作横向对比参考)
                sharpe = (avg_ret / std_ret) if std_ret != 0 else 0
                stats.append({
                    '周期': f'{d}天', '胜率': f'{(v>0).mean():.1%}', '盈亏比': f'{(v[v>0].mean()/abs(v[v<=0].mean())):.2f}',
                    '夏普': f'{sharpe:.2f}', '最大亏%': f'{v.min():.2f}', '平均盈%': f'{avg_ret:.2f}'
                })
        print(pd.DataFrame(stats).to_string(index=False))

    # --- 记录带“多维参数”的虚拟账本 ---
    os.makedirs('backtest_results', exist_ok=True)
    pd.DataFrame(ledger_r).sort_values('date', ascending=False).to_csv(
        "backtest_results/enhanced_ledger.csv", index=False, encoding='utf-8-sig'
    )
    print(f"\n[复盘必备] 已生成增强型账本：backtest_results/enhanced_ledger.csv")

if __name__ == "__main__":
    run()
