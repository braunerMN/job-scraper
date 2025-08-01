from playwright.sync_api import sync_playwright
import pandas as pd

def scrape_argonne():
    url = "https://www.argonnelumber.com/employment"
    job_list = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print(f"Navigating to {url}")
        page.goto(url, timeout=60000)
        
        # Wait for the page and job elements to load
        page.wait_for_timeout(5000)  # Wait 5 seconds for JS content
        print("Page loaded. Dumping content length:", len(page.content()))

        # Try selecting common Wix dynamic job elements
        job_elements = page.query_selector_all("h2:has-text('Now Hiring'), h3:has-text('Now Hiring')")
        print(f"Found {len(job_elements)} job headers")

        for el in job_elements:
            title = el.inner_text().strip()
            parent = el.evaluate_handle("node => node.closest('section')")
            text = parent.inner_text().strip() if parent else ""
            job_list.append({
                "title": title,
                "description": text,
                "source": url
            })

        browser.close()

    if job_list:
        df = pd.DataFrame(job_list)
        df.to_csv("job_postings.csv", index=False)
        print(f"Wrote {len(df)} jobs to job_postings.csv")
    else:
        print("⚠️ No jobs found — inspect the HTML or adjust the selector.")

if __name__ == "__main__":
    scrape_argonne()
