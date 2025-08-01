from playwright.sync_api import sync_playwright
import pandas as pd
from datetime import datetime

def scrape_argonne():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.argonnelumber.com/employment")
        page.wait_for_timeout(5000)

        jobs = []
        content = page.content()
        if "Sales Position" in content:
            jobs.append({
                "Company": "Argonne Lumber & Supply",
                "Job Title": "Sales Position",
                "Location": "WI",
                "Posting URL": page.url,
                "Date Collected": datetime.today().strftime('%Y-%m-%d')
            })
        if "Boom Truck Driver" in content:
            jobs.append({
                "Company": "Argonne Lumber & Supply",
                "Job Title": "Boom Truck Driver (CDL)",
                "Location": "WI",
                "Posting URL": page.url,
                "Date Collected": datetime.today().strftime('%Y-%m-%d')
            })

        df = pd.DataFrame(jobs)
        df.to_csv("job_postings.csv", index=False)
        print(df)

scrape_argonne()
