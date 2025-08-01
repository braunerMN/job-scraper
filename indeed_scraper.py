from playwright.sync_api import sync_playwright
import pandas as pd
import time

def scrape_indeed(company_name="Curtis Lumber"):
    query = company_name.replace(" ", "+")
    url = f"https://www.indeed.com/jobs?q=company%3A%22{query}%22&l="

    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print(f"Navigating to: {url}")
        page.goto(url, timeout=60000)
        page.wait_for_timeout(5000)  # Let JS render

        job_cards = page.query_selector_all("div.job_seen_beacon")

        for card in job_cards:
            title_el = card.query_selector("h2.jobTitle")
            title = title_el.inner_text().strip() if title_el else "N/A"

            company_el = card.query_selector("span.companyName")
            company = company_el.inner_text().strip() if company_el else company_name

            loc_el = card.query_selector("div.companyLocation")
            location = loc_el.inner_text().strip() if loc_el else "N/A"

            summary_el = card.query_selector("div.job-snippet")
            summary = summary_el.inner_text().strip().replace("\n", " ") if summary_el else "N/A"

            link_el = card.query_selector("a")
            href = link_el.get_attribute("href") if link_el else ""
            link = f"https://www.indeed.com{href}" if href else "N/A"

            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "description": summary,
                "url": link,
                "source": "Indeed"
            })

        browser.close()

    # Save or return
    print(f"🛠 Scraped {len(jobs)} jobs.")

    if jobs:
        df = pd.DataFrame(jobs)
        df.to_csv("indeed_postings.csv", index=False)
        print(f"Saved {len(df)} jobs to indeed_postings.csv")
    else:
        print("⚠️ No jobs found on Indeed.")

if __name__ == "__main__":
    scrape_indeed("Curtis Lumber")
