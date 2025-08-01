from playwright.sync_api import sync_playwright
from datetime import datetime

def scrape():
    jobs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.argonnelumber.com/employment", timeout=60000)

        # Wait for Wix job content to load
        page.wait_for_selector('[data-hook="richTextElement"]', timeout=10000)

        # Find all job listings by targeting text blocks or sections
        elements = page.query_selector_all('[data-hook="richTextElement"]')

        for el in elements:
            text = el.inner_text().strip()
            if text and any(keyword in text.lower() for keyword in ['position', 'hiring', 'apply']):
                jobs.append({
                    "dealer": "Argonne Lumber",
                    "title": text.split("\n")[0],
                    "summary": text,
                    "url": page.url,
                    "date_scraped": datetime.now().strftime("%Y-%m-%d")
                })

        browser.close()
    return jobs
