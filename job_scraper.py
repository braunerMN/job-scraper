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
        page.wait_for_timeout(6000)

        print("Page loaded. Extracting job-related content...")

        # Grab all visible text blocks (paragraphs, divs, spans)
        elements = page.query_selector_all("p, span, div")

        current_job = {}
        for el in elements:
            text = el.inner_text().strip()
            if not text or len(text) < 5:
                continue

            if any(keyword in text.lower() for keyword in ["now hiring", "position", "job", "career", "opportunity", "join our team"]):
                if current_job:
                    job_list.append(current_job)
                    current_job = {}

                current_job["title"] = text
                current_job["source"] = url
                current_job["description"] = ""
            elif current_job:
                current_job["description"] += text + "\n"

        if current_job:
            job_list.append(current_job)

        browser.close()

    if job_list:
        df = pd.DataFrame(job_list)
        df.to_csv("job_postings.csv", index=False)
        print(f"✅ Wrote {len(df)} job(s) to job_postings.csv")
    else:
        print("⚠️ No job content matched. Inspect raw page or broaden keyword search.")

if __name__ == "__main__":
    scrape_argonne()
