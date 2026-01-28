import pandas as pd
import os
import time
import sys
import socket
from pytdx.hq import TdxHq_API

# --- 常量配置 ---
DATA_DIR = 'stock_data'
PROGRESS_DIR = 'results_data_update'
PROGRESS_FILE = os.path.join(PROGRESS_DIR, 'progress.txt')
STOCK_LIST_FILE = '列表.txt'
BATCH_SIZE = 300 

# 备选服务器列表 (涵盖深圳、上海、武汉等核心节点)
TDX_SERVERS = [
    ('119.147.212.81', 7709),  # 深圳主站
    ('115.238.90.165', 7709),  # 浙江电信
    ('218.75.126.9', 7709),    # 杭州电信
    ('124.160.9.155', 7709),   # 浙江联通
    ('61.153.209.139', 7709),  # 宁波电信
    ('183.60.224.178', 7709),  # 广州电信
]

def get_best_server():
    """遍历服务器列表，寻找响应最快的节点"""
    best_ip = None
    min_latency = float('inf')
    
    print("开始测试通达信服务器延迟...")
    for ip, port in TDX_SERVERS:
        start_time = time.time()
        try:
            # 设置 2 秒超时，防止在 Action 环境中挂死
            conn = socket.create_connection((ip, port), timeout=2)
            latency = time.time() - start_time
            print(f"📡 {ip}:{port} - 延迟: {latency:.3f}s")
            if latency < min_latency:
                min_latency = latency
                best_ip = ip
            conn.close()
        except Exception:
            print(f"❌ {ip}:{port} - 连接超时/失败")
            
    return best_ip

def fetch_tdx_data(code, api):
    """(保持原有逻辑)"""
    market = 1 if code.startswith('6') else 0
    try:
        data = api.get_security_bars(9, market, code, 0, 2)
        if not data or len(data) < 1: return pd.DataFrame()
        
        df_raw = pd.DataFrame(data)
        curr = df_raw.iloc[-1]
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
            '换手率': 0.0 
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
        stock_list = stock_df[~stock_df[code_col].str.startswith(('300', '301', '688'))]
        codes = stock_list[code_col].tolist()
    except Exception as e:
        print(f"读取列表失败: {e}")
        sys.exit(1)

    # 2. 读取进度
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try: start_index = int(f.read().strip())
            except: start_index = 0

    if start_index >= len(codes):
        print("所有数据已完成，重置进度。")
        with open(PROGRESS_FILE, 'w') as f: f.write('0')
        sys.exit(0)

    # 3. 寻找最快服务器并连接
    best_server_ip = get_best_server()
    if not best_server_ip:
        print("❌ 无法连接任何通达信服务器，请检查 GitHub Action 网络环境。")
        sys.exit(1)

    api = TdxHq_API()
    if not api.connect(best_server_ip, 7709):
        print(f"❌ 尝试连接最快服务器 {best_server_ip} 失败")
        sys.exit(1)

    # 4. 执行更新 (逻辑同上)
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
    with open(PROGRESS_FILE, 'w') as f: f.write(str(end_index))
    if end_index < len(codes):
        print(f"\n进度: {end_index}/{len(codes)}，分批继续...")
        sys.exit(99)
    else:
        print("\n更新任务全部完成！")
        sys.exit(0)

if __name__ == "__main__":
    main()
