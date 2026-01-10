import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

async def search_zhihu():
    keyword = "ETF"
    results = []
    
    async with async_playwright() as p:
        # 启动浏览器
        browser = await p.chromium.launch(headless=True)
        # 设置模拟手机或桌面端的 User-Agent
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            # 访问搜索页
            url = f"https://www.zhihu.com/search?q={keyword}&type=content"
            print(f"正在访问: {url}")
            await page.goto(url, wait_until="networkidle")
            
            # 等待搜索结果列表加载 (知乎结果卡片的类名通常包含 ContentItem)
            await page.wait_for_selector(".SearchResult-Card", timeout=10000)
            
            # 提取数据
            items = await page.query_selector_all(".SearchResult-Card")
            for item in items[:10]: # 取前10条
                title_el = await item.query_selector(".ContentItem-title")
                excerpt_el = await item.query_selector(".RichText")
                
                if title_el:
                    title = await title_el.inner_text()
                    # 获取链接：查找 a 标签
                    link_el = await title_el.query_selector("a")
                    link = await link_el.get_attribute("href") if link_el else ""
                    if link and link.startswith("//"): link = "https:" + link
                    
                    excerpt = await excerpt_el.inner_text() if excerpt_el else "无摘要"
                    
                    results.append({
                        "title": title,
                        "url": link,
                        "excerpt": excerpt[:200] # 截取部分摘要
                    })
            
        except Exception as e:
            print(f"Playwright 抓取过程中出错: {e}")
        finally:
            await browser.close()
            
    return results

def save_to_markdown(results):
    os.makedirs("data", exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"data/zhihu_etf_{date_str}.md"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# 知乎 ETF 搜索结果 ({date_str})\n\n")
        if not results:
            f.write("未能抓取到结果，可能是被验证码拦截。")
        else:
            for idx, item in enumerate(results, 1):
                f.write(f"### {idx}. {item['title']}\n")
                f.write(f"- [查看文章]({item['url']})\n")
                f.write(f"- **摘要**: {item['excerpt']}\n\n")
    print(f"成功保存: {filename}")

if __name__ == "__main__":
    data = asyncio.run(search_zhihu())
    save_to_markdown(data)
