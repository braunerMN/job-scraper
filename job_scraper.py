import os
import re
import json
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
DEBUG_WIX_NET_CSV = os.path.join(OUT_DIR, "debug_wix_network_items.csv")

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
    """Accept if ~50% of words are Title Case or acronyms (CDL/HVAC)."""
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
    """Blocklist-first + structural checks."""
    title = (title or "").strip()
    desc = (description or "").strip()
    tl = title.lower()
    dl = desc.lower()

    if len(tl) < 5:
        return (False, "too_short")
    if len(title.split()) < 2:
        return (False, "one_word_header")

    if PHONE_RE.search(title) or EMAIL_RE.search(title) or URL_RE.search(title):
        return (False, "contact_info")
    if re.search(r'\binc\.?\b|\bllc\b|\bcorp\.?\b', tl):
        return (False, "company_name")

    if UPPER_RE.match(title) and len(title.split()) <= 3:
        return (False, "all_caps_header")

    # long sentences ending with punctuation likely not titles
    if title.endswith(('.', '!', '?')) and len(title.split()) >= 6:
        return (False, "sentence_like")

    for phrase in BLOCKLIST:
        if phrase in tl or phrase in dl:
            return (False, f"block:{phrase}")

    if "required" in tl and len(title.split()) <= 3:
        return (False, "requirement_only")

    if not is_titlecase_like(title) and len(title.split()) > 6:
        return (False, "not_title_like")

    return (True, "")

def keep_row(title: str, description: str = "", record_rejection=None) -> bool:
    keep, reason = filter_with_reason(title, description)
    if not keep and record_rejection is not None:
        record_rejection(title, description, reason)
    return keep

# ------------------ Wix helpers ------------------
VERB_STOPWORDS = {
    "apply", "click", "join", "subscribe", "sign", "learn", "contact",
    "visit", "shop", "buy", "call", "email", "download", "view", "read", "follow"
}
ROLE_HINTS = {"driver","sales","yard","counter","associate","technician","foreman",
              "laborer","warehouse","estimator","designer","manager","supervisor",
              "cdl","delivery","purchasing","millwork","inside sales","outside sales"}

def _auto_scroll(page, max_steps=15, step_px=1200, wait_s=0.6):
    """Scroll to bottom progressively to trigger lazy loading."""
    for _ in range(max_steps):
        page.evaluate(f"window.scrollBy(0, {step_px});")
        time.sleep(wait_s)

def _try_click_load_more(page):
    """Click load more buttons that Wix datasets often wire up."""
    texts = ["load more", "more jobs", "see more", "show more"]
    buttons = page.query_selector_all("button, a[role='button']")
    clicked = 0
    for b in buttons:
        try:
            t = (b.inner_text() or "").strip().lower()
        except:
            t = ""
        if any(x in t for x in texts):
            try:
                b.click()
                time.sleep(1.0)
                clicked += 1
            except:
                pass
    return clicked

def _looks_like_job_title(line: str) -> bool:
    """Pre-filter for Wix candidates."""
    if not line:
        return False
    line = line.strip()
    if len(line) > 64:
        return False
    words = [w for w in re.split(r'\s+', line) if w]
    if len(words) < 2 or len(words) > 7:
        return False
    if UPPER_RE.match(line) and len(words) <= 3:
        return False
    first = words[0].lower().strip(":,.-/")
    if first in VERB_STOPWORDS:
        return False
    if not any(ch.islower() for ch in line) and not re.search(r'\b[A-Z][a-z]+', line):
        return False
    return True

def _mine_json_for_titles(obj):
    """Recursively walk JSON and pull probable titles."""
    titles = []
    def walk(x):
        if isinstance(x, dict):
            # Common field hints used by Wix CMS / Repeater bindings
            for k, v in x.items():
                if isinstance(v, (dict, list)):
                    walk(v)
                else:
                    if isinstance(v, str):
                        ks = k.lower()
                        if any(h in ks for h in ["title","job","position","role","name","heading"]):
                            val = v.strip()
                            if val and _looks_like_job_title(val):
                                titles.append(val)
        elif isinstance(x, list):
            for it in x:
                walk(it)
    walk(obj)
    return titles

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
    Wix scraper:
      1) attach network listeners to capture JSON from CMS/datasets
      2) goto page, click 'load more', auto-scroll
      3) collect candidate titles from headings/list/link text in careers-like sections
      4) merge with any titles mined from JSON responses
    """
    print(f"🌐 Wix → {company} → {url}")
    raw: list[dict] = []
    net_titles = []

    # 1) network capture
    def on_response(resp):
        try:
            u = resp.url
            # Likely data endpoints used by Wix CMS/repeaters
            if any(s in u for s in ["/_api/", "/_api/cms", "wix-data", "wixapps", "wixsite"]):
                ct = resp.headers.get("content-type","").lower()
                if "application/json" in ct:
                    data = resp.json()
                    mined = _mine_json_for_titles(data)
                    for t in mined:
                        net_titles.append(t)
        except Exception:
            pass

    page.on("response", on_response)

    page.goto(url, timeout=60000)
    # give JS a moment
    page.wait_for_load_state("networkidle", timeout=30000)
    time.sleep(2)

    # 2) expand content
    for _ in range(3):
        _try_click_load_more(page)
        _auto_scroll(page, max_steps=6, step_px=1400, wait_s=0.5)

    # 3) DOM candidates (only headings/list/link text)
    scopes = page.query_selector_all("main, section, div") or []
    selectors = [
        "h1", "h2", "h3", "h4",
        "ul li", "ol li",
        "a[data-testid='linkElement'] span",
        "a[role='link'] span",
        "a[data-testid='linkElement']",
        "a[role='link']"
    ]

    for scope in scopes[:10]:  # limit breadth
        txt_scope = (scope.inner_text() or "").lower()
        if not any(sig in txt_scope for sig in ["career","careers","employment","jobs","positions","apply","opportunities"]):
            continue
        for sel in selectors:
            for el in scope.query_selector_all(sel) or []:
                try:
                    txt = (el.inner_text() or "").strip()
                except:
                    txt = ""
                if not txt:
                    continue
                line = txt.split("\n")[0].strip()
                if _looks_like_job_title(line):
                    raw.append({
                        "company": company,
                        "source": "Website (Wix)",
                        "title": line,
                        "location": "",
                        "url": url,
                        "description": txt[:400]
                    })

    # 4) merge network-mined titles
    for t in net_titles:
        raw.append({
            "company": company,
            "source": "Website (Wix, net)",
            "title": t,
            "location": "",
            "url": url,
            "description": ""
        })

    print(f"   wix candidates (DOM+net): {len(raw)}  | net_titles: {len(net_titles)}")
    return raw  # central filter handles quality

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
    wix_net_rows = []

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
    # Placeholder for network mined rows if you later store them separately
    pd.DataFrame(wix_net_rows).to_csv(DEBUG_WIX_NET_CSV, index=False)

    print(f"✅ Wrote {len(filtered)} filtered rows (from {len(uniq)} raw) → {POSTINGS_CSV}")
    print(f"🧹 Rejections saved → {REJECTIONS_CSV} ({len(rejections)} rows)")
    print(f"🔍 Wix candidates saved → {DEBUG_WIX_CSV} ({len(wix_debug_rows)} rows)")
    print(f"🔎 Wix network items saved → {DEBUG_WIX_NET_CSV} ({len(wix_net_rows)} rows)")

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
