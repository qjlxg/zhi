import requests
import json
import os
from datetime import datetime

def search_zhihu(keyword):
    # 知乎搜索接口（使用简洁版 API 或 模拟搜索）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": "https://www.zhihu.com/"
    }
    
    url = f"https://www.zhihu.com/api/v4/search_content?q={keyword}&offset=0&limit=10"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for item in data.get('data', []):
            if 'object' in item:
                obj = item['object']
                # 兼容文章和问答
                title = obj.get('title') or obj.get('question', {}).get('title', '无标题')
                link = f"https://www.zhihu.com/question/{obj.get('question', {}).get('id')}" if 'question' in obj else f"https://zhuanlan.zhihu.com/p/{obj.get('id')}"
                excerpt = obj.get('excerpt', '无摘要')
                
                results.append({
                    "title": title.replace("<em>", "").replace("</em>", ""),
                    "url": link,
                    "excerpt": excerpt.replace("<em>", "").replace("</em>", "")
                })
        return results
    except Exception as e:
        print(f"搜索出错: {e}")
        return []

def save_results(results):
    if not results:
        return
    
    # 创建保存目录
    os.makedirs("data", exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/zhihu_etf_{date_str}.md"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# 知乎 ETF 搜索结果 ({date_str})\n\n")
        for idx, item in enumerate(results, 1):
            f.write(f"### {idx}. {item['title']}\n")
            f.write(f"- **链接**: {item['url']}\n")
            f.write(f"- **摘要**: {item['excerpt']}\n\n")
    
    print(f"结果已保存至 {filename}")

if __name__ == "__main__":
    search_results = search_zhihu("ETF")
    save_results(search_results)
