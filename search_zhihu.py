import requests
import feedparser
import os
from datetime import datetime
import time

def search_zhihu_rss(keyword):
    # 定义多个镜像站地址，增加稳定性
    endpoints = [
        "https://rsshub.app",
        "https://rsshub.rssforever.com",
        "https://rss.shujer.com",
        "https://rsshub.moeyy.cn"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for base_url in endpoints:
        rss_url = f"{base_url}/zhihu/search/{keyword}"
        print(f"正在尝试镜像站: {base_url} ...")
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            if response.status_code == 200:
                feed = feedparser.parse(response.text)
                if len(feed.entries) > 0:
                    results = []
                    for entry in feed.entries[:10]:
                        results.append({
                            "title": entry.title,
                            "url": entry.link,
                            "date": entry.get('published', "未知时间"),
                            "summary": entry.get('summary', "无摘要")[:200]
                        })
                    print(f"成功从 {base_url} 获取到 {len(results)} 条数据")
                    return results
                else:
                    print(f"镜像站 {base_url} 返回结果为空，尝试下一个...")
            else:
                print(f"镜像站 {base_url} 响应异常: {response.status_code}")
        except Exception as e:
            print(f"镜像站 {base_url} 连接失败: {e}")
        
        # 稍微等一下再请求下一个，避免请求过快
        time.sleep(1)
        
    return []

def save_results(results):
    os.makedirs("data", exist_ok=True)
    # 使用日期加时间戳，方便观察每次运行状态
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"data/zhihu_etf_{date_str}.md"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# 知乎 ETF 专题监控 ({date_str})\n\n")
        if not results:
            f.write("> **警告**: 此次运行未获取到内容。可能是所有 RSSHub 镜像站均被拦截，或知乎搜索接口变动。\n")
        else:
            for idx, item in enumerate(results, 1):
                f.write(f"### {idx}. {item['title']}\n")
                f.write(f"- **发布时间**: {item['date']}\n")
                f.write(f"- **原文链接**: [点击查看]({item['url']})\n")
                f.write(f"- **摘要**: {item['summary']}...\n\n")
                f.write("---\n")
    print(f"文件保存成功: {filename}")

if __name__ == "__main__":
    data = search_zhihu_rss("ETF")
    save_results(data)
