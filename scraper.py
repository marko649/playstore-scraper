"""
Play Store Scraper Engine
==========================
Adapted for the web app. The scraper is driven by run_scrape_job() which
accepts a DataFrame and a progress callback, and returns an enriched DataFrame.

All logic is the same as the CLI version:
  - AF_initDataCallback JSON extraction (server-side embedded blobs)
  - BeautifulSoup HTML fallback
  - User-agent rotation + retry logic + jitter delay
  - Token-based app/developer validation
"""

import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

PLAY_SEARCH_URL = "https://play.google.com/store/search"
PLAY_APP_URL    = "https://play.google.com/store/apps/details"
MAX_CANDIDATES  = 3
MAX_RETRIES     = 3
DEFAULT_DELAY   = (1.0, 2.0)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class AppCandidate:
    app_name: str = ""
    dev_name: str = ""
    app_url:  str = ""
    app_id:   str = ""


@dataclass
class AppDetails:
    app_name: str = "No App"
    installs: str = "No App"
    rating:   str = "No App"
    category: str = "No App"
    platform: str = "Android"


NO_APP = AppDetails(
    app_name="No App",
    installs="No App",
    rating="No App",
    category="No App",
    platform="No App",
)

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }


def _get(url: str, params: dict = None,
         delay: Tuple[float, float] = DEFAULT_DELAY) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(random.uniform(*delay))
            resp = requests.get(url, headers=_headers(), params=params, timeout=15)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429):
                backoff = random.uniform(*delay) * (2 ** attempt)
                log.warning("HTTP %s — backing off %.1fs (attempt %d/%d)",
                            resp.status_code, backoff, attempt, MAX_RETRIES)
                time.sleep(backoff)
            else:
                log.warning("HTTP %s on attempt %d/%d: %s",
                            resp.status_code, attempt, MAX_RETRIES, url)
        except requests.RequestException as exc:
            log.warning("Request error attempt %d/%d: %s — %s", attempt, MAX_RETRIES, url, exc)
    log.error("All retries exhausted: %s", url)
    return None

# ── JSON blob extraction ──────────────────────────────────────────────────────

def _extract_af_blobs(html: str) -> List[list]:
    """Pull AF_initDataCallback data payloads from raw HTML."""
    blobs = []
    pattern = re.compile(
        r"AF_initDataCallback\s*\(\s*\{[^}]*?data\s*:\s*(\[.*?\])\s*,\s*sideChannel",
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        try:
            blobs.append(json.loads(m.group(1)))
        except json.JSONDecodeError:
            pass
    return blobs


def _collect_strings(node, out: list, max_depth: int = 8, depth: int = 0):
    if depth > max_depth:
        return
    if isinstance(node, str) and node.strip():
        out.append(node.strip())
    elif isinstance(node, list):
        for child in node:
            _collect_strings(child, out, max_depth, depth + 1)


def _safe_get(obj, *keys):
    for k in keys:
        try:
            obj = obj[k]
        except (IndexError, KeyError, TypeError):
            return None
    return obj

# ── Search ────────────────────────────────────────────────────────────────────

def search_playstore(company: str) -> Optional[str]:
    resp = _get(PLAY_SEARCH_URL, params={"q": company, "c": "apps", "hl": "en", "gl": "us"})
    return resp.text if resp else None

# ── Parse search results ──────────────────────────────────────────────────────

def parse_search_results(html: str) -> List[AppCandidate]:
    candidates: List[AppCandidate] = []
    blobs = _extract_af_blobs(html)
    for blob in blobs:
        _walk_blob(blob, candidates)
        if len(candidates) >= MAX_CANDIDATES:
            break

    if not candidates:
        candidates = _html_fallback_search(html)

    seen = set()
    unique = []
    for c in candidates:
        if c.app_id and c.app_id not in seen:
            seen.add(c.app_id)
            unique.append(c)
    return unique[:MAX_CANDIDATES]


_PKG_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,}$")


def _walk_blob(node, out: List[AppCandidate], depth: int = 0):
    if depth > 12 or len(out) >= MAX_CANDIDATES:
        return
    if isinstance(node, list):
        cand = _try_extract_app(node)
        if cand:
            out.append(cand)
            return
        for child in node:
            _walk_blob(child, out, depth + 1)


def _try_extract_app(node: list) -> Optional[AppCandidate]:
    strings: List[str] = []
    _collect_strings(node, strings)
    pkgs = [s for s in strings if _PKG_RE.match(s) and len(s) > 5]
    if not pkgs:
        return None
    app_id = pkgs[0]
    names = [s for s in strings if not _PKG_RE.match(s) and len(s) > 2
             and not s.startswith("http") and re.search(r"[A-Z]", s)]
    app_name = names[0] if names else app_id
    dev_name = next((s for s in names[1:] if s != app_name and len(s) > 2), "")
    return AppCandidate(
        app_name=app_name,
        dev_name=dev_name,
        app_url=f"{PLAY_APP_URL}?id={app_id}&hl=en&gl=us",
        app_id=app_id,
    )


def _html_fallback_search(html: str) -> List[AppCandidate]:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    seen = set()
    pkg_re = re.compile(r"[?&]id=([a-z][a-z0-9_.]+)", re.IGNORECASE)
    for a in soup.find_all("a", href=True):
        m = pkg_re.search(a["href"])
        if not m:
            continue
        app_id = m.group(1)
        if app_id in seen:
            continue
        seen.add(app_id)
        app_name = a.get_text(strip=True) or app_id
        dev_name = ""
        p = a.find_parent()
        if p:
            txts = [t.strip() for t in p.find_all(string=True) if t.strip() and t.strip() != app_name]
            dev_name = txts[0] if txts else ""
        candidates.append(AppCandidate(
            app_name=app_name, dev_name=dev_name,
            app_url=f"{PLAY_APP_URL}?id={app_id}&hl=en&gl=us",
            app_id=app_id,
        ))
        if len(candidates) >= MAX_CANDIDATES:
            break
    return candidates

# ── Validation ────────────────────────────────────────────────────────────────

_NOISE_WORDS = {
    "inc", "ltd", "llc", "corp", "co", "limited", "group",
    "technologies", "technology", "tech", "solutions", "services",
    "software", "systems", "digital", "app", "apps", "mobile",
}


def _norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    for w in _NOISE_WORDS:
        s = re.sub(rf"\b{w}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _domain_tokens(website: str) -> set:
    if not website:
        return set()
    d = re.sub(r"https?://", "", website)
    d = re.sub(r"www\.", "", d)
    base = d.split("/")[0].split(".")[0]
    return {_norm(base)} if base else set()


def validate_app(c: AppCandidate, company: str, website: str) -> bool:
    co_tokens = set(_norm(company).split()) | _domain_tokens(website)
    co_tokens.discard("")
    if not co_tokens:
        return True

    norm_app = _norm(c.app_name)
    norm_dev = _norm(c.dev_name)
    app_tokens = set(norm_app.split())
    dev_tokens = set(norm_dev.split())

    if (co_tokens & app_tokens) or (co_tokens & dev_tokens):
        return True

    for tok in co_tokens:
        if len(tok) >= 4 and (tok in norm_app or tok in norm_dev):
            return True

    return False

# ── Scrape app details ────────────────────────────────────────────────────────

_INSTALL_RE  = re.compile(r"^[\d,]+\+?$|^\d+[MKB]\+?$|^\d[\d,]*\+$")
_INSTALL_RE2 = re.compile(r"(\d[\d,]*\+?\s*(M|K|B|\+)?)\s*downloads?", re.IGNORECASE)
_RATING_RE   = re.compile(r"^[1-4]\.\d$|^5\.0$|^[1-5]$")
_PLAY_CATS   = {
    "GAME", "TOOLS", "COMMUNICATION", "PRODUCTIVITY", "BUSINESS",
    "FINANCE", "EDUCATION", "ENTERTAINMENT", "HEALTH_AND_FITNESS",
    "LIFESTYLE", "MEDICAL", "MUSIC_AND_AUDIO", "NEWS_AND_MAGAZINES",
    "PHOTOGRAPHY", "SHOPPING", "SOCIAL", "SPORTS", "TRAVEL_AND_LOCAL",
    "VIDEO_PLAYERS", "WEATHER", "MAPS_AND_NAVIGATION", "FOOD_AND_DRINK",
    "BOOKS_AND_REFERENCE", "PERSONALIZATION", "HOUSE_AND_HOME",
    "AUTO_AND_VEHICLES", "DATING", "EVENTS", "ART_AND_DESIGN",
}


def scrape_app_details(c: AppCandidate) -> AppDetails:
    resp = _get(c.app_url)
    if not resp:
        return AppDetails(app_name=c.app_name)
    html = resp.text
    details = _details_from_json(html, c.app_name)
    return details if details else _details_from_html(html, c.app_name)


def _details_from_json(html: str, app_name: str) -> Optional[AppDetails]:
    blobs = _extract_af_blobs(html)
    all_strings: List[str] = []
    for blob in blobs:
        _collect_strings(blob, all_strings, max_depth=15)

    installs = rating = category = ""

    for s in all_strings:
        s = s.strip()
        if not installs and _INSTALL_RE.match(s) and len(s) <= 15:
            installs = s
        if not installs and _INSTALL_RE2.search(s):
            installs = _INSTALL_RE2.search(s).group(0).strip()
        if not rating and _RATING_RE.match(s):
            rating = s
        if not category:
            upper = s.upper().replace(" ", "_").replace("&", "AND")
            if any(upper == cat or upper.startswith(cat) for cat in _PLAY_CATS):
                category = s

    if not installs and not rating and not category:
        return None
    return AppDetails(app_name=app_name, installs=installs or "N/A",
                      rating=rating or "N/A", category=category or "N/A",
                      platform="Android")


def _details_from_html(html: str, app_name: str) -> AppDetails:
    soup = BeautifulSoup(html, "lxml")

    installs = "N/A"
    for tag in soup.find_all(string=re.compile(r"\d[\d,]*\+", re.IGNORECASE)):
        t = tag.strip()
        if re.match(r"^[\d,]+\+$", t) or re.match(r"^\d+[MKB]\+?$", t):
            installs = t
            break

    rating = "N/A"
    for tag in soup.find_all("div", attrs={"aria-label": re.compile(r"Rated")}):
        m = re.search(r"([\d.]+)\s+out of", tag.get("aria-label", ""))
        if m:
            rating = m.group(1)
            break
    if rating == "N/A":
        for tag in soup.find_all(string=re.compile(r"^[1-5]\.\d$")):
            rating = tag.strip()
            break

    category = "N/A"
    for a in soup.find_all("a", href=re.compile(r"/store/apps/category/")):
        cat = a.get_text(strip=True)
        if cat:
            category = cat
            break

    return AppDetails(app_name=app_name, installs=installs, rating=rating,
                      category=category, platform="Android")

# ── Select best app ───────────────────────────────────────────────────────────

_MULTIPLIERS = {"b": 1_000_000_000, "m": 1_000_000, "k": 1_000}


def _parse_installs(s: str) -> int:
    if not s or s in ("N/A", "No App"):
        return -1
    s = s.lower().replace(",", "").replace("+", "").strip()
    for suffix, mult in _MULTIPLIERS.items():
        if s.endswith(suffix):
            try:
                return int(float(s[: -len(suffix)]) * mult)
            except ValueError:
                return -1
    try:
        return int(float(s))
    except ValueError:
        return -1


def select_best_app(apps: List[AppDetails]) -> AppDetails:
    if not apps:
        return NO_APP
    return max(apps, key=lambda a: _parse_installs(a.installs))

# ── Main entry point ──────────────────────────────────────────────────────────

def run_scrape_job(df: pd.DataFrame, on_progress: Callable[[dict], None]) -> pd.DataFrame:
    """
    Process all rows in `df`, enrich with Play Store data.
    Calls on_progress(event) after each row.

    event schema (type="row"):
      {
        "type": "row",
        "index": int,          # 0-based
        "company": str,
        "status": "matched" | "no_app" | "failed",
        "app_name": str,
        "installs": str,
        "rating": str,
        "category": str,
        "message": str,
      }
    """
    total = len(df)
    output_cols = ["Platform", "Installs (Range)", "Rating", "Category", "App Name"]
    for col in output_cols:
        if col not in df.columns:
            df[col] = ""

    for idx in range(total):
        row = df.iloc[idx]
        company = str(row.get("Company Name", "")).strip()
        website = str(row.get("Website", "")).strip()

        if not company:
            _fill_row(df, idx, NO_APP)
            on_progress({
                "type": "row", "index": idx, "company": company,
                "status": "failed", "app_name": "No App",
                "installs": "No App", "rating": "No App",
                "category": "No App", "message": "Empty company name — skipped",
            })
            continue

        log.info("[%d/%d] %s", idx + 1, total, company)

        # 1. Search
        html = search_playstore(company)
        if html is None:
            _fill_row(df, idx, NO_APP)
            on_progress(_row_event(idx, company, NO_APP, "Search request failed"))
            continue

        # 2. Parse
        candidates = parse_search_results(html)
        if not candidates:
            _fill_row(df, idx, NO_APP)
            on_progress(_row_event(idx, company, NO_APP, "No candidates found"))
            continue

        # 3. Validate
        valid = [c for c in candidates if validate_app(c, company, website)]
        if not valid:
            _fill_row(df, idx, NO_APP)
            on_progress(_row_event(idx, company, NO_APP,
                                   f"All {len(candidates)} candidates rejected"))
            continue

        # 4. Get details
        details_list = [scrape_app_details(c) for c in valid]

        # 5. Select best
        best = select_best_app(details_list)
        _fill_row(df, idx, best)

        status = "matched" if best.app_name != "No App" else "no_app"
        on_progress(_row_event(idx, company, best,
                               f"Matched: {best.app_name}", status=status))

    log.info("Scrape complete — %d rows processed", total)
    return df


def _fill_row(df: pd.DataFrame, idx: int, d: AppDetails):
    df.iat[idx, df.columns.get_loc("Platform")]         = d.platform
    df.iat[idx, df.columns.get_loc("Installs (Range)")] = d.installs
    df.iat[idx, df.columns.get_loc("Rating")]            = d.rating
    df.iat[idx, df.columns.get_loc("Category")]          = d.category
    df.iat[idx, df.columns.get_loc("App Name")]          = d.app_name


def _row_event(idx: int, company: str, d: AppDetails,
               message: str, status: str = None) -> dict:
    if status is None:
        status = "matched" if d.app_name != "No App" else "no_app"
    return {
        "type": "row",
        "index": idx,
        "company": company,
        "status": status,
        "app_name": d.app_name,
        "installs": d.installs,
        "rating": d.rating,
        "category": d.category,
        "message": message,
    }
