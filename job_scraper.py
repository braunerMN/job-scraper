import pandas as pd
from dealer_scrapers import argonne  # Add new dealers here

def run_scrapers():
    all_jobs = []

    # Call each dealer's scraper
    all_jobs.extend(argonne.scrape())

    # Future: add other dealers here
    # from dealer_scrapers import another_dealer
    # all_jobs.extend(another_dealer.scrape())

    # Save to CSV
    df = pd.DataFrame(all_jobs)
    df.to_csv("job_postings.csv", index=False)
    print(f"Scraped {len(df)} jobs.")

if __name__ == "__main__":
    run_scrapers()
