import pandas as pd
import os
import time
import sys
import socket
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams

# --- 核心配置 ---
DATA_DIR = 'stock_data'
PROGRESS_DIR = 'results_data_update'
PROGRESS_FILE = os.path.join(PROGRESS_DIR, 'progress.txt')
STOCK_LIST_FILE = '列表.txt'
BATCH_SIZE = 200 

# 你找齐的 12 个核心 IP 阵列 (严禁丢失)
TDX_SERVERS = [
    ('101.227.73.20', 7709),   # 华泰证券(上海电信)
    ('101.227.77.254', 7709),  # 华泰证券(上海电信二)
    ('122.192.35.44', 7709),   # 华泰证券(南京联通)
    ('221.231.141.60', 7709),  # 华泰证券(南京电信)
    ('59.173.18.140', 7709),   # 华泰证券(武汉电信)
    ('14.215.128.18', 7709),   # 华泰证券(深圳电信)
    ('60.28.23.80', 7709),     # 华泰证券(天津联通)
    ('218.60.29.136', 7709),   # 华泰证券(沈阳联通)
    ('119.147.212.81', 7709),  # 招商证券深圳行情
    ('183.60.224.178', 7709),  # 广州电信
    ('115.238.90.165', 7709),  # 浙江电信
    ('123.125.108.14', 7709),  # 中信证券北京
]

def get_best_server():
    """扫描所有 12 个 IP，寻找当前响应最快的节点"""
    best_ip = None
    min_latency = float('inf')
    print("🚀 正在探测 12 个核心节点行情服务器...")
    for ip, port in TDX_SERVERS:
        start_time = time.time()
        try:
            with socket.create_connection((ip, port), timeout=1.5) as conn:
                latency = time.time() - start_time
                print(f"📡 {ip} | 延迟: {latency:.3f}s")
                if latency < min_latency:
                    min_latency = latency
                    best_ip = ip
        except:
            continue
    return best_ip

def fetch_tdx_data(code, api):
    """
    核心抓取函数：
    1. 使用 get_security_bars 获取日线 (接口2)
    2. 使用 get_finance_info 获取流通股本计算换手率 (接口13)
    """
    # 市场判定：6开头为上海(MARKET_SH=1)，其他为深圳(MARKET_SZ=0)
    market = TDXParams.MARKET_SH if code.startswith('6') else TDXParams.MARKET_SZ
    
    try:
        # 获取最新2日K线
        bars = api.get_security_bars(9, market, code, 0, 2)
        if not bars or len(bars) < 1: return pd.DataFrame()
        
        df_bars = pd.DataFrame(bars)
        curr = df_bars.iloc[-1]
        prev_close = df_bars.iloc[0]['close'] if len(df_bars) > 1 else curr['close']
        
        # 获取财务信息计算换手率 (文档接口13)
        finance = api.get_finance_info(market, code)
        liutong = finance.get('liutongguben', 0) if finance else 0
        
        # 换手率 = (成交量 * 100) / 流通股本 * 100%
        # Pytdx vol单位是手，liutong单位是股
        turnover = 0.0
        if liutong > 0:
            turnover = round((curr['vol'] * 100 / liutong) * 100, 2)

        # 封装 12 列数据格式
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
        
        cols = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        return pd.DataFrame([row])[cols]
    except:
        return pd.DataFrame()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PROGRESS_DIR, exist_ok=True)

    # 1. 读入列表
    try:
        stock_df = pd.read_csv(STOCK_LIST_FILE, sep='\t')
        stock_df.columns = stock_df.columns.str.strip().str.lower()
        code_col = '代码' if '代码' in stock_df.columns else 'code'
        stock_df[code_col] = stock_df[code_col].astype(str).str.zfill(6)
        # 过滤主板
        codes = stock_df[~stock_df[code_col].str.startswith(('300', '301', '688'))][code_col].tolist()
    except Exception as e:
        print(f"列表加载失败: {e}"); sys.exit(1)

    # 2. 进度断点
    start_idx = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try: start_idx = int(f.read().strip())
            except: start_idx = 0

    if start_idx >= len(codes):
        print("🎉 全部完成，进度重置"); 
        with open(PROGRESS_FILE, 'w') as f: f.write('0')
        sys.exit(0)

    # 3. 连接服务器
    best_ip = get_best_server()
    if not best_ip: 
        print("❌ 无可用节点"); sys.exit(1)

    api = TdxHq_API(multithread=True) # 开启文档建议的多线程支持
    with api.connect(best_ip, 7709):
        end_idx = min(start_idx + BATCH_SIZE, len(codes))
        print(f"🎯 正在处理 {start_idx} 至 {end_idx} | 节点: {best_ip}")
        
        for code in codes[start_idx:end_idx]:
            df_new = fetch_tdx_data(code, api)
            if not df_new.empty:
                path = os.path.join(DATA_DIR, f"{code}.csv")
                # 优化写入：增量追加
                if os.path.exists(path):
                    # 判断日期防止重复写入
                    try:
                        last_line = pd.read_csv(path).tail(1)
                        if last_line['日期'].iloc[0] != df_new['日期'].iloc[0]:
                            df_new.to_csv(path, mode='a', index=False, header=False)
                    except:
                        df_new.to_csv(path, index=False)
                else:
                    df_new.to_csv(path, index=False)
                print(f"{code}", end=' ', flush=True)

    # 4. 存进度
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(end_idx))
    
    sys.exit(99 if end_idx < len(codes) else 0)

if __name__ == "__main__":
    main()
