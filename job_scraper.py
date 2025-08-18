import os
import re
import time
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as dtparse
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

# ------------------ paths ------------------
LOADSHEET = "loadsheet.csv"
OUT_DIR = "outputs"
CONFIG_DIR = "config"
POSTINGS_CSV = os.path.join(OUT_DIR, "job_postings.csv")
STATE_CSV = os.path.join(OUT_DIR, "state.csv")
AGED_CSV = os.path.join(OUT_DIR, "aged_jobs.csv")
REJECTIONS_CSV = os.path.join(OUT_DIR, "rejections.csv")

# ------------------ utils ------------------
def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CONFIG_DIR, exist_ok=True)

def normalize_space(s: str | None) -> str:
    return " ".join(s.split()) if isinstance(s, str) else ""

def utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def make_job_key(j: dict) -> str:
    return "|".join([
        (j.get("company","") or "").lower(),
        (j.get("source","") or "").lower(),
        (j.get("title","") or "").lower(),
        (j.get("location","") or "").lower(),
        (j.get("url","") or "").lower()
    ])

# ------------------ blocklist loader ------------------
DEFAULT_BLOCKLIST = [
    # generic non-job text
    "open positions", "we are hiring", "hiring", "join our mailing list",
    "join our newsletter", "click to apply", "apply now", "benefits",
    "about us", "mission statement", "equal opportunity", "contact us",
    "locations", "hours", "privacy policy", "terms of service",
    "subscribe", "sign up", "follow us", "news", "events",
    "sales event", "promotion", "coupon", "contest", "sweepstakes",
    "blog", "press",

    # footer/legal/junk
    "accessibility statement", "terms of use", "all rights reserved",
    "powered by", "copyright", "©",

    # application prompts
    "employment application", "download", "application", "apply online"
]

def load_blocklist() -> list[str]:
    """Merge defaults with optional config/blocklist.txt (one phrase per line)."""
    merged = set(DEFAULT_BLOCKLIST)
    user_file = os.path.join(CONFIG_DIR, "blocklist.txt")
    if os.path.exists(user_file):
        with open(user_file, "r", encoding="utf-8") as f:
            for line in f:
                phrase = line.strip().lower()
                if phrase and not phrase.startswith("#"):
                    merged.add(phrase)
    return sorted(merged)

BLOCKLIST = load_blocklist()

# ------------------ filtering with reasons ------------------
PHONE_RE = re.compile(r'\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})\b')
EMAIL_RE = re.compile(r'\b\S+@\S+\.\S+\b')
URL_RE = re.compile(r'https?://|www\.')
UPPER_RE = re.compile(r'^[^a-z]*$')  # title has no lowercase letters

def is_titlecase_like(title: str) -> bool:
    """
    Accept if at least ~50% of words are Title Case or acronyms (e.g., CDL).
    Helps keep 'Sales Position', 'Boom Truck Driver' and reject long sentences.
    """
    words = [w for w in re.split(r'\s+', title) if w]
    if not words:
        return False

    def ok(w: str) -> bool:
        if re.fullmatch(r'[A-Z]{2,5}', w):  # acronyms like CDL, HVAC
            return True
        # "Title-Case" or Title-Case hyphenated
        return re.fullmatch(r'[A-Z][a-z]+(?:-[A-Z][a-z]+)?', w) is not None

    hits = sum(1 for w in words if ok(w))
    return hits >= max(1, len(words) // 2)

def filter_with_reason(title: str, description: str = "") -> tuple[bool, str]:
    """
    Return (keep, reason_if_rejected).
    Blocklist-first + structural checks (no required job keywords).
    """
    title = (title or "").strip()
    desc = (description or "").strip()
    tl = title.lower()
    dl = desc.lower()

    # short / one-word headers
    if len(tl) < 5:
        return (False, "too_short")
    if len(title.split()) < 2:
        return (False, "one_word_header")

    # contact/footer/junk
    if PHONE_RE.search(title) or EMAIL_RE.search(title) or URL_RE.search(title):
        return (False, "contact_info")
    if re.search(r'\binc\.?\b|\bllc\b|\bcorp\.?\b', tl):
        return (False, "company_name")

    # obvious headers
    if UPPER_RE.match(title) and len(title.split()) <= 3:
        return (False, "all_caps_header")

    # sentences (not titles): long + terminal punctuation
    if title.endswith(('.', '!', '?')) and len(title.split()) >= 6:
        return (False, "sentence_like")

    # blocklist phrases anywhere in title or description
    for phrase in BLOCKLIST:
        if phrase in tl or phrase in dl:
            return (False, f"block:{phrase}")

    # requirement-only lines like "CDL Required", "Experience Required"
    if "required" in tl and len(title.split()) <= 3:
        return (False, "requirement_only")

    # shape: if it's not title-like and it's long, reject
    if not is_titlecase_like(title) and len(title.split()) > 5:
        return (False, "not_title_like")

    return (True, "")

def keep_row(title: str, description: str = "", record_rejection=None):
    keep, reason = filter_with_reason(title, description)
    if not keep and record_rejection is not None:
        record_rejection(title, description, reason)
    return keep

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
            if title:
                jobs.append({
                    "company": company,
                    "source": "Indeed",
                    "title": title,
                    "location": location,
                    "url": link
                })
        except Exception as e:
            print("  ⚠️ indeed card parse:", e)
    print(f"   indeed raw: {len(jobs)}")
    return jobs

def _wix_find_relevant_section(page) -> list:
    """
    Heuristic: find the first container that mentions careers/employment/positions,
    then search only inside it to reduce noise like footers, nav, newsletter.
    """
    signals = ["career", "careers", "employment", "jobs", "now hiring", "open positions", "positions", "apply"]
    containers = page.query_selector_all("section, div, main") or []
    for el in containers:
        try:
            txt = (el.inner_text() or "").lower()
        except:
            txt = ""
        if not txt:
            continue
        if any(s in txt for s in signals):
            return [el]
    return []

def scrape_wix_generic(page, url: str, company: str) -> list[dict]:
    """
    Wix scraper: limit scope to a relevant section, then collect candidate titles from
    headings (h1..h6), list items, and short paragraphs/links; apply strict filters later.
    """
    print(f"🌐 Wix → {company} → {url}")
    jobs: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(5)  # allow JS to render

    # 1) Restrict to relevant section(s)
    scopes = _wix_find_relevant_section(page)
    if not scopes:
        scopes = [page]  # fallback: whole page (filters will clean)

    # 2) Within scope, gather candidate elements
    selectors = [
        "h1", "h2", "h3", "h4",
        "ul li", "ol li",
        "[data-hook='richTextElement'] p",
        "[data-testid='richTextElement'] p",
        "a[data-testid='linkElement'] span",
        "p"
    ]

    for scope in scopes:
        for sel in selectors:
            els = scope.query_selector_all(sel) or []
            for el in els:
                try:
                    txt = (el.inner_text() or "").strip()
                except:
                    txt = ""
                if not txt:
                    continue

                # Consider only the first line of multi-line blocks here
                line = txt.split("\n")[0].strip()
                if not line:
                    continue

                jobs.append({
                    "company": company,
                    "source": "Website (Wix)",
                    "title": line,
                    "location": "",
                    "url": url,
                    "description": txt[:400]
                })

    print(f"   wix raw candidates: {len(jobs)}")
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
            if title:
                jobs.append({
                    "company": company,
                    "source": "Website (Static)",
                    "title": normalize_space(title),
                    "location": normalize_space(location),
                    "url": link
                })
        except Exception as e:
            print("  ⚠️ static card parse:", e)

    print(f"   static raw: {len(jobs)}")
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
            if title:
                jobs.append({"company": company, "source": "Lever", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   lever raw: {len(jobs)}")
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
            if title:
                jobs.append({"company": company, "source": "Greenhouse", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   greenhouse raw: {len(jobs)}")
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
            if title:
                jobs.append({"company": company, "source": "BambooHR", "title": title, "location": "", "url": link})
        except:
            pass
    print(f"   bamboohr raw: {len(jobs)}")
    return jobs

# ------------------ orchestrator + aging ------------------
def run_all():
    ensure_dirs()

    if not os.path.exists(LOADSHEET):
        raise SystemExit(f"Missing {LOADSHEET}. Run build_loadsheet.py first.")

    sources = pd.read_csv(LOADSHEET).fillna("")
    raw_rows: list[dict] = []

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
                    raw_rows += scrape_indeed(page, company)
                elif stype == "wix_generic":
                    raw_rows += scrape_wix_generic(page, url, company)
                elif stype == "custom_static":
                    raw_rows += scrape_custom_static(page, url, company, sc, st, sl, slink)
                elif stype == "lever":
                    raw_rows += scrape_lever(page, url, company)
                elif stype == "greenhouse":
                    raw_rows += scrape_greenhouse(page, url, company)
                elif stype == "bamboohr":
                    raw_rows += scrape_bamboohr(page, url, company)
                else:
                    print(f"⚠️ Unknown source_type: {stype} for {company}")
            except Exception as e:
                print(f"❌ {company} ({stype}) error: {e}")

        browser.close()

    # De-dupe by stable key
    uniq, seen = [], set()
    for j in raw_rows:
        k = make_job_key(j)
        if k not in seen:
            seen.add(k)
            uniq.append(j)

    # Filter with reason tracking
    rejections = []
    def record_rejection(title, description, reason):
        rejections.append({
            "title": title,
            "reason": reason,
            "snippet": (description or "")[:200]
        })

    filtered = []
    for j in uniq:
        title = j.get("title", "")
        desc = j.get("description", "")
        if keep_row(title, desc, record_rejection):
            # drop the temporary description field from final CSV if present
            if "description" in j:
                j = {k: v for k, v in j.items() if k != "description"}
            filtered.append(j)

    # Save outputs
    pd.DataFrame(filtered).to_csv(POSTINGS_CSV, index=False)
    pd.DataFrame(rejections).to_csv(REJECTIONS_CSV, index=False)
    print(f"✅ Wrote {len(filtered)} filtered rows (from {len(uniq)} raw) → {POSTINGS_CSV}")
    print(f"🧹 Rejections saved → {REJECTIONS_CSV}")

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
        state.loc[state["job_key"].isin(rkeys), "last_seen_utc"] = now
        new_rows = run_df[~run_df["job_key"].isin(keys)][["job_key","company","source","title","location","url"]].copy()
        if not new_rows.empty:
            new_rows["first_seen_utc"] = now
            new_rows["last_seen_utc"] = now
            state = pd.concat([state, new_rows], ignore_index=True)

    state.to_csv(STATE_CSV, index=False)

    # compute age (by first_seen)
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
