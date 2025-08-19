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
DEBUG_WIX_CSV = os.path.join(OUT_DIR, "debug_wix_candidates.csv")

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
    "employment application", "download application", "apply online",
    "apply here", "print application"
]

def load_blocklist() -> list[str]:
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
UPPER_RE = re.compile(r'^[^a-z]*$')  # no lowercase letters

def is_titlecase_like(title: str) -> bool:
    """
    Accept if at least ~50% of words are Title Case or acronyms (CDL/HVAC).
    Helps keep 'Sales Position', 'Boom Truck Driver' and reject long sentences.
    """
    words = [w for w in re.split(r'\s+', title) if w]
    if not words:
        return False

    def ok(w: str) -> bool:
        if re.fullmatch(r'[A-Z]{2,5}', w):  # CDL, HVAC
            return True
        return re.fullmatch(r'[A-Z][a-z]+(?:-[A-Z][a-z]+)?', w) is not None

    hits = sum(1 for w in words if ok(w))
    return hits >= max(1, len(words) // 2)

def filter_with_reason(title: str, description: str = "") -> tuple[bool, str]:
    """
    Blocklist-first + structural checks.
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

    # requirement-only lines like "CDL Required" or "Experience Required"
    if "required" in tl and len(title.split()) <= 3:
        return (False, "requirement_only")

    # shape: if it's not title-like and it's long, reject
    if not is_titlecase_like(title) and len(title.split()) > 6:
        return (False, "not_title_like")

    return (True, "")

def keep_row(title: str, description: str = "", record_rejection=None) -> bool:
    keep, reason = filter_with_reason(title, description)
    if not keep and record_rejection is not None:
        record_rejection(title, description, reason)
    return keep

# ------------------ WIX helpers (tight) ------------------
VERB_STOPWORDS = {
    "apply", "click", "join", "subscribe", "sign", "learn", "contact",
    "visit", "shop", "buy", "call", "email", "download", "view", "read", "follow"
}

ROLE_HINTS = {"driver","sales","yard","counter","associate","technician","foreman",
              "laborer","warehouse","estimator","designer","manager","supervisor",
              "cdl","delivery","purchasing","millwork","inside sales","outside sales"}

def _wix_find_relevant_sections(page) -> list:
    """
    Find containers that mention careers/employment/positions to reduce noise
    (nav/footer/newsletter). Fallback to whole page if none found.
    """
    signals = ["career", "careers", "employment", "jobs", "now hiring", "open positions", "positions", "apply"]
    containers = page.query_selector_all("main, section, div") or []
    ranked = []
    for el in containers:
        try:
            txt = (el.inner_text() or "").lower()
        except:
            txt = ""
        if not txt:
            continue
        score = sum(1 for s in signals if s in txt)
        if score:
            ranked.append((score, el))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [el for _, el in ranked[:3]] or [page]  # top 1-3 relevant, else whole page

def _looks_like_job_title(line: str) -> bool:
    """
    Pre-filter for Wix candidates before full filtering:
    - 2..6 words
    - <= 48 chars
    - not all-caps short headers
    - no obvious CTA verbs at start
    - has lowercase or TitleCase pattern
    """
    if not line:
        return False
    line = line.strip()
    if len(line) > 48:
        return False
    words = [w for w in re.split(r'\s+', line) if w]
    if len(words) < 2 or len(words) > 6:
        return False
    if UPPER_RE.match(line) and len(words) <= 3:
        return False
    first = words[0].lower().strip(":,.-/")
    if first in VERB_STOPWORDS:
        return False
    if not any(ch.islower() for ch in line) and not re.search(r'\b[A-Z][a-z]+', line):
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

def scrape_wix_generic(page, url: str, company: str) -> list[dict]:
    """
    Wix scraper: restrict scope, extract headings/list items/link text only,
    pre-filter candidates, then apply central filter later.
    """
    print(f"🌐 Wix → {company} → {url}")
    raw: list[dict] = []
    page.goto(url, timeout=60000)
    time.sleep(5)

    scopes = _wix_find_relevant_sections(page)
    selectors = [
        "h1", "h2", "h3", "h4",
        "ul li", "ol li",
        "a[data-testid='linkElement'] span",
        "a[role='link'] span",
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
                line = txt.split("\n")[0].strip()
                if not _looks_like_job_title(line):
                    continue
                raw.append({
                    "company": company,
                    "source": "Website (Wix)",
                    "title": line,
                    "location": "",
                    "url": url,
                    "description": txt[:400]
                })

    # Fallback: if we found nothing, scan headings again and keep ones with role hints
    if not raw:
        headings = page.query_selector_all("h1, h2, h3, h4") or []
        for h in headings:
            try:
                line = (h.inner_text() or "").strip()
            except:
                line = ""
            if not line:
                continue
            lo = line.lower()
            if any(w in lo for w in ROLE_HINTS) and _looks_like_job_title(line):
                raw.append({
                    "company": company,
                    "source": "Website (Wix)",
                    "title": line,
                    "location": "",
                    "url": url,
                    "description": line
                })

    print(f"   wix candidates: {len(raw)}")
    return raw  # final filtering happens centrally

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
    wix_debug_rows = []

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
                    wix_candidates = scrape_wix_generic(page, url, company)
                    raw_rows += wix_candidates
                    # keep candidates for debug
                    for c in wix_candidates:
                        wix_debug_rows.append({
                            "company": company,
                            "url": url,
                            "candidate": c.get("title",""),
                            "snippet": c.get("description","")
                        })
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
            "snippet": (description or "")[:250]
        })

    filtered = []
    for j in uniq:
        title = j.get("title", "")
        desc = j.get("description", "")
        if keep_row(title, desc, record_rejection):
            # drop temporary description field from final CSV
            if "description" in j:
                j = {k: v for k, v in j.items() if k != "description"}
            filtered.append(j)

    # Save outputs (always create the files)
    pd.DataFrame(filtered).to_csv(POSTINGS_CSV, index=False)
    pd.DataFrame(rejections).to_csv(REJECTIONS_CSV, index=False)
    pd.DataFrame(wix_debug_rows).to_csv(DEBUG_WIX_CSV, index=False)

    print(f"✅ Wrote {len(filtered)} filtered rows (from {len(uniq)} raw) → {POSTINGS_CSV}")
    print(f"🧹 Rejections saved → {REJECTIONS_CSV} ({len(rejections)} rows)")
    print(f"🔍 Wix candidates saved → {DEBUG_WIX_CSV} ({len(wix_debug_rows)} rows)")

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
