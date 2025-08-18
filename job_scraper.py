import os
import time
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as dtparse
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

# ------------------ paths & constants ------------------
LOADSHEET = "loadsheet.csv"
OUT_DIR = "outputs"
POSTINGS_CSV = os.path.join(OUT_DIR, "job_postings.csv")
STATE_CSV = os.path.join(OUT_DIR, "state.csv")
AGED_CSV = os.path.join(OUT_DIR, "aged_jobs.csv")


# ------------------ utilities ------------------
def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)

def normalize_space(s: str | None) -> str:
    return " ".join(s.split()) if isinstance(s, str) else ""

def utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def make_job_key(j: dict) -> str:
    """Stable key to deduplicate and track jobs across runs."""
    return "|".join([
        (j.get("company","") or "").lower(),
        (j.get("source","") or "").lower(),
        (j.get("title","") or "").lower(),
        (j.get("location","") or "").lower(),
        (j.get("url","") or "").lower()
    ])


# ------------------ blocklist-only filter ------------------
def is_likely_job(title: str, description: str = "") -> bool:
    """
    Keep this simple and flexible:
      - reject very short or one-word titles
      - reject titles/descriptions that contain obvious non-job phrases
      - reject SHOUTY one-line headers (e.g., "HIRING")
    Everything else passes (no required job keywords).
    """
    title = (title or "").strip()
    desc = (description or "").strip()
    tl = title.lower()
    dl = desc.lower()

    # too short to be useful as a job title
    if len(tl) < 5 or len(title.split()) < 2:
        return False

    # configurable phrases that are NOT job postings
    blocklist = [
        "open positions", "we are hiring", "hiring", "join our mailing list",
        "join our newsletter", "click to apply", "apply now", "benefits",
        "about us", "mission statement", "equal opportunity", "contact us",
        "locations", "hours", "privacy policy", "terms of service",
        "subscribe", "sign up", "follow us", "news", "events"
    ]
    if any(p in tl for p in blocklist) or any(p in dl for p in blocklist):
        return False

    # headers like ALL CAPS one-liners are often section headings
    if tl.isupper() and len(title.split()) <= 3:
        return False

    return True


# ------------------ platform scrapers ------------------
def scrape_indeed(page, company: str) -> list[dict]:
    jobs: list[dict] = []
    url = f"https://www.indeed.com/jobs?q=company%3A%22{company.replace(' ','+')}%22&l="
    print(f"🔎 Indeed → {company}: {url}")
    page.goto(url, timeout=60000)
    try:
        page.wait_for_selector("a.tapItem", timeout=10000)
    except:
        pass
    time.sleep(2)

    for c in page.query_selector_all("a.tapItem"):
        try:
            t = c.query_selector("h2.jobTitle")
            comp = c.query_selector("span.companyName")
            loc = c.query_selector("div.companyLocation")
            href = c.get_attribute("href") or ""
            link = "https://www.indeed.com" + href if href.startswith("/") else href
            title = normalize_space(t.inner_text()) if t else ""
            company_final = normalize_space(comp.inner_text()) if comp else company
            location = normalize_space(loc.inner_text()) if loc else ""
            if title and is_likely_job(title):
                jobs.append({
                    "company": company,
                    "source": "Indeed",
                    "title": title,
                    "location": location,
                    "url": link
                })
        except Exception as e:
            print("  ⚠️ indeed card parse:", e)
    print(f"   indeed kept: {len(jobs)}")
    return jobs


def scrape_wix_generic(page, url: str, company: str) -> list[dict]:
    """Heuristic Wix scraper with strong skip rules; relies on blocklist-only filter."""
    print(f"🌐 Wix → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(5)  # allow JS to render

    containers = ['[data-hook="richTextElement"]', '[data-testid="richTextElement"]', 'section', 'div']
    skip_phrases = {
        "click to apply", "open positions", "we are hiring", "hiring",
        "join our mailing list", "newsletter", "benefits", "about us",
        "privacy policy", "terms of service", "subscribe", "sign up"
    }

    kept = 0
    for sel in containers:
        for el in page.query_selector_all(sel):
            try:
                block = (el.inner_text() or "").strip()
            except:
                block = ""
            if not block:
                continue

            title = block.split("\n")[0].strip()
            tl = title.lower()

            # skip obvious non-job headers quickly
            if any(p in tl for p in skip_phrases):
                continue

            if is_likely_job(title, block):
                jobs.append({
                    "company": company,
                    "source": "Website (Wix)",
                    "title": title,
                    "location": "",
                    "url": url
                })
                kept += 1

        if kept:
            break

    print(f"   wix blocks kept: {kept}")
    return jobs


def scrape_custom_static(page, url: str, company: str,
                         sel_card: str, sel_title: str, sel_loc: str, sel_link: str) -> list[dict]:
    print(f"📄 Static → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(2)

    cards = page.query_selector_all(sel_card) if sel_card else []
    print(f"   static cards: {len(cards)}")

    for card in cards:
        try:
            title = card.query_selector(sel_title).inner_text().strip() if sel_title and card.query_selector(sel_title) else ""
            location = card.query_selector(sel_loc).inner_text().strip() if sel_loc and card.query_selector(sel_loc) else ""
            href = card.query_selector(sel_link).get_attribute("href") if sel_link and card.query_selector(sel_link) else ""
            link = urljoin(url, href) if href else url

            if title and is_likely_job(title):
                jobs.append({
                    "company": company,
                    "source": "Website (Static)",
                    "title": normalize_space(title),
                    "location": normalize_space(location),
                    "url": link
                })
        except Exception as e:
            print("  ⚠️ static card parse:", e)

    print(f"   static kept: {len(jobs)}")
    return jobs


def scrape_lever(page, url: str, company: str) -> list[dict]:
    print(f"🟣 Lever → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(2)

    postings = page.query_selector_all(".posting a, .posting-title a, a.posting-title")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link = a.get_attribute("href") or url
            if title and is_likely_job(title):
                jobs.append({"company": company, "source": "Lever", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   lever kept: {len(jobs)}")
    return jobs


def scrape_greenhouse(page, url: str, company: str) -> list[dict]:
    print(f"🟢 Greenhouse → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(2)

    postings = page.query_selector_all("section.opening a, .opening a, .opening a[href]")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link = a.get_attribute("href") or url
            if title and is_likely_job(title):
                jobs.append({"company": company, "source": "Greenhouse", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   greenhouse kept: {len(jobs)}")
    return jobs


def scrape_bamboohr(page, url: str, company: str) -> list[dict]:
    print(f"🟡 BambooHR → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(2)

    postings = page.query_selector_all(".opening a, .jobTitle a, a[href*='bamboohr.com/jobs']")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link = a.get_attribute("href") or url
            if title and is_likely_job(title):
                jobs.append({"company": company, "source": "BambooHR", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   bamboohr kept: {len(jobs)}")
    return jobs


# ------------------ orchestrator + aging ------------------
def run_all():
    ensure_dirs()

    if not os.path.exists(LOADSHEET):
        raise SystemExit(f"Missing {LOADSHEET}. Run build_loadsheet.py first.")

    sources = pd.read_csv(LOADSHEET).fillna("")
    all_rows: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()

        for _, row in sources.iterrows():
            company = row["company"].strip()
            stype = row["source_type"].strip().lower()
            url = row["url"].strip()
            sc = row.get("selector_card", "").strip()
            st = row.get("selector_title", "").strip()
            sl = row.get("selector_location", "").strip()
            slink = row.get("selector_link", "").strip()

            try:
                if stype == "indeed":
                    all_rows += scrape_indeed(page, company)
                elif stype == "wix_generic":
                    all_rows += scrape_wix_generic(page, url, company)
                elif stype == "custom_static":
                    all_rows += scrape_custom_static(page, url, company, sc, st, sl, slink)
                elif stype == "lever":
                    all_rows += scrape_lever(page, url, company)
                elif stype == "greenhouse":
                    all_rows += scrape_greenhouse(page, url, company)
                elif stype == "bamboohr":
                    all_rows += scrape_bamboohr(page, url, company)
                else:
                    print(f"⚠️ Unknown source_type: {stype} for {company}")
            except Exception as e:
                print(f"❌ {company} ({stype}) error: {e}")

        browser.close()

    # de-dupe on stable key
    uniq, seen = [], set()
    for j in all_rows:
        k = make_job_key(j)
        if k not in seen:
            seen.add(k)
            uniq.append(j)

    # post-filter (blocklist-only)
    filtered = [j for j in uniq if is_likely_job(j.get("title", ""), j.get("description", ""))]

    pd.DataFrame(filtered).to_csv(POSTINGS_CSV, index=False)
    print(f"✅ Wrote {len(filtered)} filtered rows (from {len(uniq)} raw) → {POSTINGS_CSV}")

    # ----- aging state -----
    state_cols = ["job_key","company","source","title","location","url","first_seen_utc","last_seen_utc"]
    state = pd.read_csv(STATE_CSV) if os.path.exists(STATE_CSV) else pd.DataFrame(columns=state_cols)

    run_df = pd.DataFrame(filtered)
    if run_df.empty:
        print("ℹ️ No rows this run; skipping aging.")
        return
    run_df["job_key"] = run_df.apply(make_job_key, axis=1)
    now = utc_iso()

    if state.empty:
        state = run_df[["job_key","company","source","title","location","url"]].copy()
        state["first_seen_utc"] = now
        state["last_seen_utc"] = now
    else:
        keys = set(state["job_key"])
        rkeys = set(run_df["job_key"])
        # update last_seen for jobs present in this run
        state.loc[state["job_key"].isin(rkeys), "last_seen_utc"] = now
        # add new jobs
        new_rows = run_df[~run_df["job_key"].isin(keys)][["job_key","company","source","title","location","url"]].copy()
        if not new_rows.empty:
            new_rows["first_seen_utc"] = now
            new_rows["last_seen_utc"] = now
            state = pd.concat([state, new_rows], ignore_index=True)

    state.to_csv(STATE_CSV, index=False)

    # compute age by first_seen; export aged (≥28 days)
    def age_days(first_iso: str):
        try:
            first = dtparse(first_iso)
            return (datetime.utcnow() - first.replace(tzinfo=None)).days
        except:
            return None

    state["age_days"] = state["first_seen_utc"].apply(age_days)
    aged = state[state["age_days"].fillna(0) >= 28].copy().sort_values("age_days", ascending=False)
    aged.to_csv(AGED_CSV, index=False)
    print(f"📣 Aged (≥28 days): {len(aged)} → {AGED_CSV}")


if __name__ == "__main__":
    run_all()
