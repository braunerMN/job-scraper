import os, csv, time
import pandas as pd
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

INPUT = "dealers_input.csv"
LOADSHEET = "loadsheet.csv"

CAREERS_HINTS = ["career", "careers", "employment", "jobs", "work-with-us", "join-our-team", "opportunities"]
PLATFORM_MARKERS = {
    "wix_generic": ["wix.com website builder", "wixstatic.com", "thunderbolt", "wixcode"],
    "lever":       ["lever.co", 'data-lever', "lever-jobs"],
    "greenhouse":  ["greenhouse.io", "boards.greenhouse.io"],
    "bamboohr":    ["bamboohr.com", "bamboohr"],
    # add more markers here as you scale (icims, workday, paycom, ukg, etc)
}

def guess_careers_url(page, homepage_url):
    # Try obvious paths first
    for tail in ["careers", "employment", "jobs", "join-our-team", "opportunities"]:
        test = urljoin(homepage_url + ("" if homepage_url.endswith("/") else "/"), tail)
        try:
            page.goto(test, timeout=15000)
            time.sleep(2)
            if "404" not in (page.title() or "").lower() and page.content():
                return page.url
        except:
            pass

    # Otherwise, scan homepage links for careers-like anchors
    try:
        page.goto(homepage_url, timeout=20000)
        time.sleep(2)
        anchors = page.query_selector_all("a[href]")
        scored = []
        for a in anchors:
            href = a.get_attribute("href") or ""
            txt = (a.inner_text() or "").strip().lower()
            if any(h in (href or "").lower() for h in CAREERS_HINTS) or any(h in txt for h in CAREERS_HINTS):
                abs_url = urljoin(homepage_url, href)
                scored.append(abs_url)
        if scored:
            # Pick the first viable
            return scored[0]
    except:
        pass
    return homepage_url  # fallback to homepage if nothing else

def detect_platform(content: str, url: str):
    lc = (content or "").lower()
    host = url.lower()
    for platform, needles in PLATFORM_MARKERS.items():
        if any(n in lc or n in host for n in needles):
            return platform
    # If it looks like a job board page layout, add more heuristics as needed
    return "custom_static"

def main():
    if not os.path.exists(INPUT):
        raise SystemExit(f"Missing {INPUT}")

    df = pd.read_csv(INPUT).fillna("")
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()

        for _, r in df.iterrows():
            company = r["company"].strip()
            homepage = r["homepage_url"].strip()
            careers = r["careers_url"].strip()

            # Resolve a working careers URL if none provided
            target = careers or homepage
            try:
                target = careers or guess_careers_url(page, homepage)
            except:
                pass

            # Load target & detect platform
            try:
                page.goto(target, timeout=30000)
                time.sleep(3)
                content = page.content()
                platform = detect_platform(content, page.url)
                resolved_url = page.url
            except Exception as e:
                print(f"⚠️ Could not load {company} → {target}: {e}")
                platform, resolved_url = "custom_static", target

            # Emit a primary row based on platform
            if platform in ("wix_generic", "custom_static"):
                rows.append({
                    "company": company,
                    "source_type": platform,
                    "url": resolved_url,
                    "extra_company_query": "",
                    "selector_card": "", "selector_title": "", "selector_location": "", "selector_link": "",
                    "notes": f"auto-detected {platform}"
                })
            elif platform in ("lever", "greenhouse", "bamboohr"):
                # Direct platform support (scraper handles these)
                rows.append({
                    "company": company,
                    "source_type": platform,
                    "url": resolved_url,
                    "extra_company_query": "",
                    "selector_card": "", "selector_title": "", "selector_location": "", "selector_link": "",
                    "notes": f"auto-detected {platform}"
                })
            else:
                rows.append({
                    "company": company,
                    "source_type": "custom_static",
                    "url": resolved_url,
                    "extra_company_query": "",
                    "selector_card": "", "selector_title": "", "selector_location": "", "selector_link": "",
                    "notes": "fallback custom_static"
                })

            # Always add an Indeed row as a second source for that company
            rows.append({
                "company": company,
                "source_type": "indeed",
                "url": "",
                "extra_company_query": "",
                "selector_card": "", "selector_title": "", "selector_location": "", "selector_link": "",
                "notes": "indeed company search"
            })

        browser.close()

    # Write loadsheet
    fieldnames = ["company","source_type","url","extra_company_query",
                  "selector_card","selector_title","selector_location","selector_link","notes"]
    with open(LOADSHEET, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print(f"✅ Created {LOADSHEET} with {len(rows)} rows (including Indeed fallbacks).")

if __name__ == "__main__":
    main()
