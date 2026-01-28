import pandas as pd
import os
import time
import sys
import socket
from pytdx.hq import TdxHq_API

# --- 核心配置 ---
DATA_DIR = 'stock_data'
PROGRESS_DIR = 'results_data_update'
PROGRESS_FILE = os.path.join(PROGRESS_DIR, 'progress.txt')
STOCK_LIST_FILE = '列表.txt'
BATCH_SIZE = 200  # 每批处理200只，平衡速度与稳定性

# 整合你提供的华泰节点及常用稳定节点
TDX_SERVERS = [
    ('101.227.73.20', 7709),   # 华泰证券(上海电信) - 推荐海外优先
    ('101.227.77.254', 7709),  # 华泰证券(上海电信二)
    ('122.192.35.44', 7709),   # 华泰证券(南京联通)
    ('221.231.141.60', 7709),  # 华泰证券(南京电信)
    ('59.173.18.140', 7709),   # 华泰证券(武汉电信)
    ('14.215.128.18', 7709),   # 华泰证券(深圳电信)
    ('183.60.224.178', 7709),  # 广州电信
    ('119.147.212.81', 7709),  # 招商证券
    ('218.75.126.9', 7709),    # 杭州电信
]

def get_best_server():
    """自动化寻找延迟最低的服务器"""
    best_ip = None
    min_latency = float('inf')
    print("🚀 正在扫描最快通达信服务器...")
    for ip, port in TDX_SERVERS:
        start_time = time.time()
        try:
            with socket.create_connection((ip, port), timeout=1.5) as conn:
                latency = time.time() - start_time
                print(f"📡 {ip} | 响应: {latency:.3f}s")
                if latency < min_latency:
                    min_latency = latency
                    best_ip = ip
        except:
            print(f"❌ {ip} | 连接超时")
    return best_ip

def fetch_tdx_data(code, api):
    """抓取K线并尝试自动计算换手率"""
    market = 1 if code.startswith('6') else 0
    try:
        # 1. 获取日K线 (含昨收用于计算)
        data = api.get_security_bars(9, market, code, 0, 2)
        if not data or len(data) < 1: return pd.DataFrame()
        
        # 2. 获取财务信息（用于计算换手率：成交量 / 流通股本）
        # 注：Pytdx的成交量单位是手(100股)，财务数据的liutongguben单位通常也是股
        finance = api.get_finance_info(market, code)
        liutong = finance.get('liutongguben', 0) if finance else 0
        
        df_raw = pd.DataFrame(data)
        curr = df_raw.iloc[-1]
        prev_close = df_raw.iloc[0]['close'] if len(df_raw) > 1 else curr['close']
        
        # 计算换手率 (成交量*100 / 流通股本 * 100%)
        turnover = 0.0
        if liutong > 0:
            turnover = round((curr['vol'] * 100 / liutong) * 100, 2)

        row = {
            '日期': pd.to_datetime(curr['datetime']).strftime('%Y-%m-%d'),
            '股票代码': code,
            '开盘': float(curr['open']),
            '收盘': float(curr['close']),
            '最高': float(curr['high']),
            '最低': float(curr['low']),
            '成交量': int(curr['vol']),
            '成交额': float(curr['amount']),
            '振幅': round((curr['high'] - curr['low']) / prev_close * 100, 2) if prev_close != 0 else 0,
            '涨跌幅': round((curr['close'] - prev_close) / prev_close * 100, 2) if prev_close != 0 else 0,
            '涨跌额': round(curr['close'] - prev_close, 2),
            '换手率': turnover
        }
        
        res_df = pd.DataFrame([row])
        # 严格匹配你的12列格式
        cols = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        return res_df[cols]
    except:
        return pd.DataFrame()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PROGRESS_DIR, exist_ok=True)

    # 1. 加载股票列表
    try:
        stock_df = pd.read_csv(STOCK_LIST_FILE, sep='\t')
        stock_df.columns = stock_df.columns.str.strip().str.lower()
        code_col = '代码' if '代码' in stock_df.columns else 'code'
        stock_df[code_col] = stock_df[code_col].astype(str).str.zfill(6)
        # 排除创业板和科创板
        stock_list = stock_df[~stock_df[code_col].str.startswith(('300', '301', '688'))]
        codes = stock_list[code_col].tolist()
    except Exception as e:
        print(f"❌ 读取列表失败: {e}")
        sys.exit(1)

    # 2. 断点续传逻辑
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try: start_index = int(f.read().strip())
            except: start_index = 0

    if start_index >= len(codes):
        print("✅ 本次所有任务已完成，重置进度索引。")
        with open(PROGRESS_FILE, 'w') as f: f.write('0')
        sys.exit(0)

    # 3. 连接最快服务器
    best_ip = get_best_server()
    if not best_ip:
        print("🔥 警报：没有可用的通达信服务器！")
        sys.exit(1)

    api = TdxHq_API()
    with api.connect(best_ip, 7709):
        end_index = min(start_index + BATCH_SIZE, len(codes))
        current_batch = codes[start_index:end_index]
        print(f"📈 正在处理: {start_index} -> {end_index} (总计: {len(codes)})")

        for code in current_batch:
            df_new = fetch_tdx_data(code, api)
            if not df_new.empty:
                file_path = os.path.join(DATA_DIR, f"{code}.csv")
                # 数据合并与去重
                if os.path.exists(file_path):
                    old_df = pd.read_csv(file_path)
                    old_df['股票代码'] = old_df['股票代码'].astype(str).str.zfill(6)
                    combined = pd.concat([old_df, df_new]).drop_duplicates(subset=['日期'], keep='last')
                    combined.to_csv(file_path, index=False)
                else:
                    df_new.to_csv(file_path, index=False)
                print(f"{code}", end=' ', flush=True)
            # 稍微降低请求频率，防止被封IP
            time.sleep(0.05)

    # 4. 更新进度
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(end_index))
    
    print(f"\n✨ 批次 {end_index} 处理完毕。")
    if end_index < len(codes):
        sys.exit(99) # 触发 GitHub Actions 的自我重启
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
