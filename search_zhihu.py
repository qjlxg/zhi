import requests
import feedparser
import os
from datetime import datetime

def search_zhihu_rss(keyword):
    # 使用公开的 RSSHub 实例。如果这个域名失效，可以更换其他镜像
    # 接口文档：https://docs.rsshub.app/routes/social-media#zhi-hu-sou-suo
    rss_url = f"https://rsshub.app/zhihu/search/{keyword}"
    
    print(f"正在通过 RSSHub 获取数据: {rss_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(rss_url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"RSSHub 响应异常: {response.status_code}")
            return []

        # 解析 RSS 内容
        feed = feedparser.parse(response.text)
        results = []

        for entry in feed.entries[:10]:  # 获取前10条
            results.append({
                "title": entry.title,
                "url": entry.link,
                "date": entry.published if 'published' in entry else "未知时间",
                "summary": entry.summary[:200] if 'summary' in entry else "无摘要"
            })
        return results

    except Exception as e:
        print(f"发生错误: {e}")
        return []

def save_results(results):
    os.makedirs("data", exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/zhihu_etf_{date_str}.md"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# 知乎 ETF 专题监控 ({date_str})\n\n")
        if not results:
            f.write("本次运行未发现新内容或服务暂时不可用。\n")
        else:
            for idx, item in enumerate(results, 1):
                f.write(f"### {idx}. {item['title']}\n")
                f.write(f"- **发布时间**: {item['date']}\n")
                f.write(f"- **原文链接**: [点击查看]({item['url']})\n")
                f.write(f"- **摘要**: {item['summary']}...\n\n")
                f.write("---\n")
    
    print(f"结果已成功写入: {filename}")

if __name__ == "__main__":
    data = search_zhihu_rss("ETF")
    save_results(data)
