import pandas as pd
from playwright.sync_api import sync_playwright

def scrape_indeed(company_name="Curtis Lumber", location=""):
    print("🚀 Starting Indeed scraper...")

    jobs = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Build search URL
            base_url = "https://www.indeed.com/jobs"
            query = f"?q={company_name.replace(' ', '+')}"
            if location:
                query += f"&l={location.replace(' ', '+')}"
            url = base_url + query
            print(f"🌐 Navigating to: {url}")
            page.goto(url, timeout=60000)

            page.wait_for_selector("a.tapItem", timeout=10000)
            job_cards = page.query_selector_all("a.tapItem")

            for job in job_cards:
                try:
                    title = job.query_selector("h2.jobTitle").inner_text().strip()
                    company = job.query_selector("span.companyName").inner_text().strip()
                    location = job.query_selector("div.companyLocation").inner_text().strip()
                    link = job.get_attribute("href")
                    job_url = f"https://www.indeed.com{link}" if link else ""

                    jobs.append({
                        "Job Title": title,
                        "Company": company,
                        "Location": location,
                        "Link": job_url
                    })
                except Exception as e:
                    print(f"⚠️ Error parsing job card: {e}")

            browser.close()

        print(f"🛠 Scraped {len(jobs)} jobs.")

        if jobs:
            df = pd.DataFrame(jobs)
            df.to_csv("indeed_postings.csv", index=False)
            print(f"✅ Saved {len(df)} jobs to indeed_postings.csv")
        else:
            print("⚠️ No jobs found on Indeed.")

    except Exception as e:
        print(f"❌ Error during scraping: {e}")

if __name__ == "__main__":
    scrape_indeed("Curtis Lumber")
