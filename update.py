import pandas as pd
import os
import time
import sys
from pytdx.hq import TdxHq_API

# --- 常量配置 ---
DATA_DIR = 'stock_data'
PROGRESS_DIR = 'results_data_update'
PROGRESS_FILE = os.path.join(PROGRESS_DIR, 'progress.txt')
STOCK_LIST_FILE = '列表.txt'
BATCH_SIZE = 300  # Pytdx 速度快，但为了 Action 稳定性，建议设在 300-500
TDX_SERVER = '119.147.212.81' # 也可以换成其他通达信服务器 IP

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROGRESS_DIR, exist_ok=True)

def fetch_tdx_data(code, api):
    """使用 Pytdx 获取日线数据"""
    # 市场判定：6开头为沪市(1)，其他(0、3等)为深市(0)
    market = 1 if code.startswith('6') else 0
    
    try:
        # 获取最近 2 天的数据，确保能拿到昨收来计算涨跌
        # 9 为日线
        data = api.get_security_bars(9, market, code, 0, 2)
        
        if not data or len(data) < 1:
            return pd.DataFrame()
        
        # 转换为 DataFrame
        df_raw = pd.DataFrame(data)
        
        # 取最新的一行数据
        curr = df_raw.iloc[-1]
        
        # 计算逻辑
        # 如果只有一条数据，涨跌额设为 0；如果有两条，则用最新收盘减去上一日收盘
        prev_close = df_raw.iloc[0]['close'] if len(df_raw) > 1 else curr['close']
        
        row = {
            '日期': pd.to_datetime(curr['datetime']).strftime('%Y-%m-%d'),
            '股票代码': code,
            '开盘': float(curr['open']),
            '收盘': float(curr['close']),
            '最高': float(curr['high']),
            '最低': float(curr['low']),
            '成交量': int(curr['vol']),
            '成交额': float(curr['amount']),
            '涨跌额': round(curr['close'] - prev_close, 2),
            '涨跌幅': round((curr['close'] - prev_close) / prev_close * 100, 2) if prev_close != 0 else 0,
            '振幅': round((curr['high'] - curr['low']) / prev_close * 100, 2) if prev_close != 0 else 0,
            '换手率': 0.0 # 基础 K 线接口不带换手率，此处占位
        }
        
        res_df = pd.DataFrame([row])
        cols = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        return res_df[cols]
    except:
        return pd.DataFrame()

def main():
    # 1. 加载股票列表
    try:
        stock_df = pd.read_csv(STOCK_LIST_FILE, sep='\t')
        stock_df.columns = stock_df.columns.str.strip().str.lower()
        code_col = '代码' if '代码' in stock_df.columns else 'code'
        stock_df[code_col] = stock_df[code_col].astype(str).str.zfill(6)
        # 过滤掉创业板和科创板
        stock_list = stock_df[~stock_df[code_col].str.startswith(('300', '301', '688'))]
        codes = stock_list[code_col].tolist()
    except Exception as e:
        print(f"读取列表失败: {e}")
        return

    # 2. 读取进度
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                start_index = int(f.read().strip())
            except: start_index = 0

    if start_index >= len(codes):
        print("所有数据已是最新，重置进度。")
        with open(PROGRESS_FILE, 'w') as f: f.write('0')
        return

    # 3. 连接 Pytdx
    api = TdxHq_API()
    if not api.connect(TDX_SERVER, 7709):
        print("连接通达信服务器失败")
        return

    # 4. 执行更新
    end_index = min(start_index + BATCH_SIZE, len(codes))
    current_batch = codes[start_index:end_index]

    for code in current_batch:
        df_new = fetch_tdx_data(code, api)
        if not df_new.empty:
            file_path = os.path.join(DATA_DIR, f"{code}.csv")
            if os.path.exists(file_path):
                old_df = pd.read_csv(file_path)
                old_df['股票代码'] = old_df['股票代码'].astype(str).str.zfill(6)
                combined = pd.concat([old_df, df_new]).drop_duplicates(subset=['日期'], keep='last')
                combined.to_csv(file_path, index=False)
            else:
                df_new.to_csv(file_path, index=False)
            print(f"√ {code}", end=' ')
        
    api.disconnect()

    # 5. 保存进度与退出
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(end_index))

    if end_index < len(codes):
        print(f"\n进度: {end_index}/{len(codes)}，准备重启下一批次...")
        sys.exit(99)
    else:
        print("\n今日主板数据更新全部完成！")
        sys.exit(0)

if __name__ == "__main__":
    main()
