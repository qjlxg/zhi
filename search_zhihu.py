import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

async def search_zhihu():
    keyword = "ETF"
    results = []
    
    async with async_playwright() as p:
        # 这里的参数非常关键：禁用自动化特征
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        
        page = await context.new_page()
        
        # 注入脚本，抹除 webdriver 特征
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            url = f"https://www.zhihu.com/search?q={keyword}&type=content"
            print(f"正在访问: {url}")
            
            # 延长超时并模拟真人等待
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # 模拟向下滚动一屏，触发懒加载
            await page.mouse.wheel(0, 500)
            await asyncio.sleep(2) 

            # 检查是否遇到了验证码
            if "安全验证" in await page.content():
                print("检测到验证码拦截！")
                await page.screenshot(path="data/error_captcha.png")
                return []

            # 等待结果，尝试使用更通用的选择器
            try:
                await page.wait_for_selector(".SearchResult-Card", timeout=15000)
            except:
                print("未找到结果卡片，尝试保存截图诊断...")
                await page.screenshot(path="data/debug_page.png")
                return []

            items = await page.query_selector_all(".SearchResult-Card")
            for item in items[:10]:
                title_el = await item.query_selector(".ContentItem-title")
                excerpt_el = await item.query_selector(".RichText")
                
                if title_el:
                    title = await title_el.inner_text()
                    link_el = await title_el.query_selector("a")
                    link = await link_el.get_attribute("href") if link_el else ""
                    if link and link.startswith("//"): link = "https:" + link
                    elif link and link.startswith("/"): link = "https://www.zhihu.com" + link
                    
                    excerpt = await excerpt_el.inner_text() if excerpt_el else ""
                    results.append({"title": title, "url": link, "excerpt": excerpt[:200]})
                    
        except Exception as e:
            print(f"抓取异常: {e}")
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
            f.write("未能抓取到结果。请检查 data 目录下的 debug_page.png 截图。")
        else:
            for idx, item in enumerate(results, 1):
                f.write(f"### {idx}. {item['title']}\n- [原文链接]({item['url']})\n- 摘要: {item['excerpt']}\n\n")
    print(f"保存成功: {filename}")

if __name__ == "__main__":
    res = asyncio.run(search_zhihu())
    save_to_markdown(res)
