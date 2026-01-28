import pandas as pd
import os
import time
import requests
import sys

# --- 常量配置 ---
DATA_DIR = 'stock_data'
STOCK_LIST_FILE = '列表.txt'
# 新浪接口必须带上这个 Referer，否则会被拒绝访问
HEADERS = {
    'Referer': 'http://finance.sina.com.cn',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

os.makedirs(DATA_DIR, exist_ok=True)

def fetch_sina_data(code):
    """从新浪财经获取免费实时行情数据"""
    # 转换代码格式：上海 6 开头加 sh，深圳其他加 sz
    full_code = f"sh{code}" if code.startswith('6') else f"sz{code}"
    url = f"http://hq.sinajs.cn/list={full_code}"
    
    try:
        # 新浪接口返回的是 gbk 编码的字符串
        response = requests.get(url, headers=HEADERS, timeout=10)
        content = response.text
        
        if '"' not in content or len(content.split(',')) < 30:
            return pd.DataFrame()
        
        # 解析 JavaScript 变量格式
        raw_data = content.split('"')[1].split(',')
        
        # 对应新浪字段：1开盘, 2昨收, 3现价(收盘), 4最高, 5最低, 8成交量, 9成交额, 30日期
        data = {
            '日期': raw_data[30],
            '股票代码': code,
            '开盘': float(raw_data[1]),
            '收盘': float(raw_data[3]),
            '最高': float(raw_data[4]),
            '最低': float(raw_data[5]),
            '成交量': int(raw_data[8]),
            '成交额': float(raw_data[9]),
        }
        
        # 补充计算字段 (匹配你的12列格式)
        prev_close = float(raw_data[2])
        data['涨跌额'] = round(data['收盘'] - prev_close, 2)
        data['涨跌幅'] = round((data['涨跌额'] / prev_close * 100), 2) if prev_close != 0 else 0
        data['振幅'] = round((data['最高'] - data['最低']) / prev_close * 100, 2) if prev_close != 0 else 0
        data['换手率'] = 0.0 # 免费接口不提供，设为默认值
        
        df = pd.DataFrame([data])
        # 调整列顺序
        cols = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        return df[cols]
    except Exception as e:
        return pd.DataFrame()

def main():
    # 1. 加载并过滤列表
    try:
        stock_df = pd.read_csv(STOCK_LIST_FILE, sep='\t')
        stock_df.columns = stock_df.columns.str.strip().str.lower()
        code_col = '代码' if '代码' in stock_df.columns else 'code'
        stock_df[code_col] = stock_df[code_col].astype(str).str.zfill(6)
        
        # 过滤逻辑：跳过 300, 301, 688
        stock_list = stock_df[~stock_df[code_col].str.startswith(('300', '301', '688'))]
        print(f"列表加载成功，共 {len(stock_list)} 只主板股票。")
    except Exception as e:
        print(f"读取列表失败: {e}")
        return

    # 2. 循环处理
    for _, row in stock_list.iterrows():
        code = row[code_col]
        df_new = fetch_sina_data(code)
        
        if not df_new.empty:
            file_path = os.path.join(DATA_DIR, f"{code}.csv")
            # 如果存在旧文件则追加并去重
            if os.path.exists(file_path):
                old_df = pd.read_csv(file_path)
                combined = pd.concat([old_df, df_new]).drop_duplicates(subset=['日期'], keep='last')
                combined.to_csv(file_path, index=False)
            else:
                df_new.to_csv(file_path, index=False)
            print(f"√ {code} 已更新", end=' | ')
        else:
            print(f"× {code} 获取失败", end=' | ')
        
        # 免费接口也建议稍微留一点延迟
        time.sleep(0.1)

if __name__ == "__main__":
    main()
