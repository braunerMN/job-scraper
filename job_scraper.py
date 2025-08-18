import os, sys, time
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse as dtparse
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

LOADSHEET = "loadsheet.csv"
OUT_DIR = "outputs"
POSTINGS_CSV = os.path.join(OUT_DIR, "job_postings.csv")
STATE_CSV = os.path.join(OUT_DIR, "state.csv")
AGED_CSV = os.path.join(OUT_DIR, "aged_jobs.csv")

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)

def normalize_space(s): return " ".join(s.split()) if isinstance(s, str) else s
def utc_iso(): return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
def make_job_key(j):
    return "|".join([
        (j.get("company","") or "").lower(),
        (j.get("source","") or "").lower(),
        (j.get("title","") or "").lower(),
        (j.get("location","") or "").lower(),
        (j.get("url","") or "").lower()
    ])

# ------------ Platform scrapers ------------
def scrape_indeed(page, company):
    jobs = []
    url = f"https://www.indeed.com/jobs?q=company%3A%22{company.replace(' ','+')}%22&l="
    print(f"🔎 Indeed → {company}: {url}")
    page.goto(url, timeout=60000)
    try: page.wait_for_selector("a.tapItem", timeout=10000)
    except: pass
    time.sleep(2)
    for c in page.query_selector_all("a.tapItem"):
        try:
            title = c.query_selector("h2.jobTitle")
            comp = c.query_selector("span.companyName")
            loc  = c.query_selector("div.companyLocation")
            href = c.get_attribute("href") or ""
            link = "https://www.indeed.com" + href if href.startswith("/") else href
            title = normalize_space(title.inner_text()) if title else ""
            comp  = normalize_space(comp.inner_text())  if comp  else company
            loc   = normalize_space(loc.inner_text())   if loc   else ""
            if title:
                jobs.append({"company": company, "source": "Indeed",
                             "title": title, "location": loc, "url": link})
        except Exception as e:
            print("  ⚠️ indeed card parse:", e)
    return jobs

def scrape_wix_generic(page, url, company):
    print(f"🌐 Wix → {company} → {url}")
    jobs = []
    page.goto(url, timeout=60000); time.sleep(5)
    candidates = ['[data-hook="richTextElement"]','[data-testid="richTextElement"]','section','div']
    seen = 0
    for sel in candidates:
        for el in page.query_selector_all(sel):
            try:
                txt = normalize_space(el.inner_text() or "")
            except: txt = ""
            if not txt: continue
            if any(k in txt.lower() for k in ["position","hiring","apply","job","driver","sales","yard","associate"]):
                title = txt.split("\n")[0][:120]
                if title:
                    jobs.append({"company": company, "source": "Website (Wix)",
                                 "title": title, "location": "", "url": url})
                    seen += 1
        if seen: break
    print(f"   wix blocks: {seen}")
    return jobs

def scrape_custom_static(page, url, company, sel_card, sel_title, sel_loc, sel_link):
    print(f"📄 Static → {company} → {url}")
    jobs = []
    page.goto(url, timeout=60000); time.sleep(2)
    cards = page.query_selector_all(sel_card) if sel_card else []
    print(f"   cards: {len(cards)}")
    for card in cards:
        try:
            title = card.query_selector(sel_title).inner_text().strip() if sel_title and card.query_selector(sel_title) else ""
            loc   = card.query_selector(sel_loc).inner_text().strip()   if sel_loc and card.query_selector(sel_loc) else ""
            hrefn = card.query_selector(sel_link).get_attribute("href") if sel_link and card.query_selector(sel_link) else ""
            link  = urljoin(url, hrefn) if hrefn else url
            if title:
                jobs.append({"company": company, "source": "Website (Static)",
                             "title": normalize_space(title), "location": normalize_space(loc), "url": link})
        except Exception as e:
            print("  ⚠️ static card parse:", e)
    return jobs

def scrape_lever(page, url, company):
    print(f"🟣 Lever → {company} → {url}")
    jobs = []
    page.goto(url, timeout=60000); time.sleep(2)
    postings = page.query_selector_all(".posting a, .posting-title a, a.posting-title")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link  = a.get_attribute("href") or url
            if title:
                jobs.append({"company": company, "source": "Lever", "title": title, "location": "", "url": link})
        except: pass
    print(f"   lever: {len(jobs)}")
    return jobs

def scrape_greenhouse(page, url, company):
    print(f"🟢 Greenhouse → {company} → {url}")
    jobs = []
    page.goto(url, timeout=60000); time.sleep(2)
    postings = page.query_selector_all("section.opening a, .opening a, .opening a[href]")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link  = a.get_attribute("href") or url
            if title:
                jobs.append({"company": company, "source": "Greenhouse", "title": title, "location": "", "url": link})
        except: pass
    print(f"   greenhouse: {len(jobs)}")
    return jobs

def scrape_bamboohr(page, url, company):
    print(f"🟡 BambooHR → {company} → {url}")
    jobs = []
    page.goto(url, timeout=60000); time.sleep(2)
    postings = page.query_selector_all(".opening a, .jobTitle a, a[href*='bamboohr.com/jobs']")
    for a in postings:
        try:
            title = normalize_space(a.inner_text() or "")
            link  = a.get_attribute("href") or url
            if title:
                jobs.append({"company": company, "source": "BambooHR", "title": title, "location": "", "url": link})
        except: pass
    print(f"   bamboohr: {len(jobs)}")
    return jobs

# ------------ Orchestrator & aging ------------
def run_all():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(LOADSHEET):
        raise SystemExit(f"Missing {LOADSHEET}. Run build_loadsheet.py first.")

    src = pd.read_csv(LOADSHEET).fillna("")
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        for _, row in src.iterrows():
            company = row["company"].strip()
            stype   = row["source_type"].strip().lower()
            url     = row["url"].strip()
            sc      = row.get("selector_card","").strip()
            st      = row.get("selector_title","").strip()
            sl      = row.get("selector_location","").strip()
            slink   = row.get("selector_link","").strip()

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

    # de-dupe
    uniq, seen = [], set()
    for j in all_rows:
        k = make_job_key(j)
        if k not in seen:
            seen.add(k); uniq.append(j)

    pd.DataFrame(uniq).to_csv(POSTINGS_CSV, index=False)
    print(f"✅ Wrote {len(uniq)} rows → {POSTINGS_CSV}")

    # simple aging state
    state = pd.read_csv(STATE_CSV) if os.path.exists(STATE_CSV) else pd.DataFrame(
        columns=["job_key","company","source","title","location","url","first_seen_utc","last_seen_utc"]
    )
    run_df = pd.DataFrame(uniq)
    if run_df.empty:
        print("ℹ️ No rows this run; skipping aging.")
        return
    run_df["job_key"] = run_df.apply(make_job_key, axis=1)
    now = utc_iso()

    if state.empty:
        state = run_df[["job_key","company","source","title","location","url"]].copy()
        state["first_seen_utc"] = now
        state["last_seen_utc"]  = now
    else:
        keys = set(state["job_key"])
        rkeys = set(run_df["job_key"])
        state.loc[state["job_key"].isin(rkeys), "last_seen_utc"] = now
        new_rows = run_df[~run_df["job_key"].isin(keys)][["job_key","company","source","title","location","url"]].copy()
        if not new_rows.empty:
            new_rows["first_seen_utc"] = now
            new_rows["last_seen_utc"]  = now
            state = pd.concat([state, new_rows], ignore_index=True)

    state.to_csv(STATE_CSV, index=False)

    def age_days(first_iso):
        try:
            first = dtparse(first_iso)
            return (datetime.utcnow() - first.replace(tzinfo=None)).days
        except: return None

    state["age_days"] = state["first_seen_utc"].apply(age_days)
    aged = state[state["age_days"].fillna(0) >= 28].copy().sort_values("age_days", ascending=False)
    aged.to_csv(AGED_CSV, index=False)
    print(f"📣 Aged (≥28 days): {len(aged)} → {AGED_CSV}")

if __name__ == "__main__":
    run_all()
