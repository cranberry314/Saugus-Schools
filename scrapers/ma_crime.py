"""
Scrapes municipal crime statistics from the MA State Police Beyond 2020 portal.

Source: https://ma.beyond2020.com/ma_tops
Data covers all MA municipalities, years 2020–2024 (NIBRS era).

Metrics captured per municipality per year:
  - total_crimes, crime_rate_per_100k, clearance_rate_pct, population
  - violent_crimes, homicides, sexual_assaults, aggravated_assaults

Table populated: municipal_crime

Run:
    python scrapers/ma_crime.py                # all municipalities
    python scrapers/ma_crime.py --slug saugus  # single municipality
    python scrapers/ma_crime.py --dry-run      # list slugs, no DB writes
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import re
import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL  = "https://ma.beyond2020.com/ma_tops"
SUBMIT    = f"{BASE_URL}/selector/submit"
QUERY_URL = f"{BASE_URL}/report/query"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
SLEEP     = 0.2   # seconds between API requests

# Years available in NIBRS era (MA transitioned ~2020–2021)
YEARS = [2020, 2021, 2022, 2023, 2024]
LATEST_YEAR = 2024

# Substrings that identify non-municipal jurisdictions to skip
SKIP_PATTERNS = [
    "college", "university", "state-police", "railroad", "hospital",
    "med-ctr", "mbta", "mta", "transit", "airport", "massachusetts",
    "amtrak", "comm-", "dock", "pike", "-cc",
]

UPSERT = text("""
    INSERT INTO municipal_crime
        (jurisdiction_slug, ori_code, jurisdiction_name, year,
         total_crimes, violent_crimes, homicides, sexual_assaults,
         aggravated_assaults, clearance_rate_pct, crime_rate_per_100k, population)
    VALUES
        (:slug, :ori, :name, :year,
         :total_crimes, :violent_crimes, :homicides, :sexual_assaults,
         :aggravated_assaults, :clearance_rate_pct, :crime_rate_per_100k, :population)
    ON CONFLICT (ori_code, year) DO UPDATE SET
        jurisdiction_slug    = EXCLUDED.jurisdiction_slug,
        jurisdiction_name    = EXCLUDED.jurisdiction_name,
        total_crimes         = EXCLUDED.total_crimes,
        violent_crimes       = EXCLUDED.violent_crimes,
        homicides            = EXCLUDED.homicides,
        sexual_assaults      = EXCLUDED.sexual_assaults,
        aggravated_assaults  = EXCLUDED.aggravated_assaults,
        clearance_rate_pct   = EXCLUDED.clearance_rate_pct,
        crime_rate_per_100k  = EXCLUDED.crime_rate_per_100k,
        population           = EXCLUDED.population,
        loaded_at            = NOW()
""")


def _safe_int(v):
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return None


def _safe_float(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


def _parse_summary_text(html_text: str) -> dict:
    """Parse the HTML text block from crime-overview/chart/1 into a dict."""
    result = {}
    if not html_text:
        return result
    soup = BeautifulSoup(html_text, "html.parser")
    text_content = soup.get_text(" ")

    m = re.search(r"Number of Crimes:\s*([\d,]+)", text_content)
    if m:
        result["total_crimes"] = _safe_int(m.group(1))

    m = re.search(r"Clearance Rate:\s*([\d.]+)%", text_content)
    if m:
        result["clearance_rate_pct"] = _safe_float(m.group(1))

    m = re.search(r"Population:\s*([\d,]+)", text_content)
    if m:
        result["population"] = _safe_int(m.group(1))

    m = re.search(r"Crime Rate:\s*([\d,.]+)\s*per 100,000", text_content)
    if m:
        result["crime_rate_per_100k"] = _safe_float(m.group(1))

    return result


def _get_trend(session, theme: str, ori: str, year: int, chart: int, token: str) -> dict:
    """
    Fetch a chart endpoint and return a {year: value} dict from the trend data.
    Returns {} on any error.
    """
    url = f"{QUERY_URL}/{theme}/{ori}/{year}/chart/{chart}"
    hdrs = {**HEADERS, "RequestVerificationToken": token, "X-Requested-With": "XMLHttpRequest"}
    try:
        r = session.get(url, headers=hdrs, timeout=25)
        if r.status_code != 200:
            return {}
        data = r.json()
        labels = data.get("labels", [])
        datasets = data.get("datasets", [])
        if not datasets or not labels:
            return {}
        values = datasets[0].get("data", [])
        return {int(lbl): _safe_int(v) for lbl, v in zip(labels, values) if lbl.isdigit()}
    except Exception:
        return {}


def _get_summary(session, ori: str, year: int, token: str) -> dict:
    """Fetch crime-overview/chart/1 and parse the summary text."""
    url = f"{QUERY_URL}/crime-overview/{ori}/{year}/chart/1"
    hdrs = {**HEADERS, "RequestVerificationToken": token, "X-Requested-With": "XMLHttpRequest"}
    try:
        r = session.get(url, headers=hdrs, timeout=25)
        if r.status_code != 200:
            return {}
        return _parse_summary_text(r.json().get("text", ""))
    except Exception:
        return {}


def get_all_slugs(session) -> list[tuple[str, str]]:
    """Return list of (slug, display_name) for all municipal jurisdictions."""
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    sel = soup.find("select", {"id": "Jurisdiction"})
    if not sel:
        raise RuntimeError("Jurisdiction selector not found on page")
    seen = set()
    results = []
    for opt in sel.find_all("option"):
        slug = opt.get("value", "").strip()
        name = opt.text.strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        if any(p in slug.lower() for p in SKIP_PATTERNS):
            continue
        results.append((slug, name))
    return results


def _get_initial_token(session) -> str:
    r = session.get(BASE_URL, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    inp = soup.find("input", {"name": "__RequestVerificationToken"})
    return inp.get("value", "") if inp else ""


def fetch_jurisdiction(session, slug: str, display_name: str, token: str) -> tuple[str | None, str, dict]:
    """
    POST to get the ORI code for a slug, then collect crime data.
    Returns (ori_code, new_token, {year: row_dict}) or (None, token, {}) on failure.
    """
    post_data = {
        "Jurisdiction": slug,
        "Year": str(LATEST_YEAR),
        "Theme": "crime-overview",
        "__RequestVerificationToken": token,
    }
    try:
        r = session.post(SUBMIT, data=post_data, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None, token, {}
        soup = BeautifulSoup(r.text, "html.parser")
        report_div = soup.find("div", {"id": "report"})
        if not report_div:
            return None, token, {}
        ori = report_div.get("data-jurisdiction", "").strip()
        if not ori:
            return None, token, {}
        new_token_inp = soup.find("input", {"name": "__RequestVerificationToken"})
        new_token = new_token_inp.get("value", token) if new_token_inp else token
    except Exception as e:
        print(f"    POST error: {e}")
        return None, token, {}

    # Collect trend data from LATEST_YEAR (gives 2020–2024 in one request)
    time.sleep(SLEEP)
    total_trend     = _get_trend(session, "crime-overview",  ori, LATEST_YEAR, 2, new_token)
    time.sleep(SLEEP)
    violent_trend   = _get_trend(session, "violent-crimes",  ori, LATEST_YEAR, 1, new_token)
    time.sleep(SLEEP)
    homicide_trend  = _get_trend(session, "violent-crimes",  ori, LATEST_YEAR, 2, new_token)
    time.sleep(SLEEP)
    sexual_trend    = _get_trend(session, "violent-crimes",  ori, LATEST_YEAR, 3, new_token)
    time.sleep(SLEEP)
    assault_trend   = _get_trend(session, "violent-crimes",  ori, LATEST_YEAR, 4, new_token)

    # Per-year summary stats (clearance rate, crime rate, population)
    summaries = {}
    for yr in YEARS:
        time.sleep(SLEEP)
        summaries[yr] = _get_summary(session, ori, yr, new_token)

    # Build per-year rows
    rows = {}
    for yr in YEARS:
        s = summaries.get(yr, {})
        row = {
            "slug":                slug,
            "ori":                 ori,
            "name":                display_name,
            "year":                yr,
            "total_crimes":        total_trend.get(yr) or s.get("total_crimes"),
            "violent_crimes":      violent_trend.get(yr),
            "homicides":           homicide_trend.get(yr),
            "sexual_assaults":     sexual_trend.get(yr),
            "aggravated_assaults": assault_trend.get(yr),
            "clearance_rate_pct":  s.get("clearance_rate_pct"),
            "crime_rate_per_100k": s.get("crime_rate_per_100k"),
            "population":          s.get("population"),
        }
        rows[yr] = row

    return ori, new_token, rows


def run(engine, slugs: list[str] | None = None, dry_run: bool = False,
        resume: bool = False) -> int:
    session = requests.Session()
    token = _get_initial_token(session)

    all_slugs = get_all_slugs(session)
    print(f"[ma_crime] {len(all_slugs)} municipal jurisdictions found")

    if slugs:
        all_slugs = [(s, n) for s, n in all_slugs if s in slugs]
        print(f"[ma_crime] Filtered to {len(all_slugs)} slug(s)")

    if resume and engine is not None:
        from sqlalchemy import text as _text
        with engine.connect() as conn:
            done = set(conn.execute(_text(
                "SELECT DISTINCT jurisdiction_slug FROM municipal_crime"
            )).scalars().all())
        all_slugs = [(s, n) for s, n in all_slugs if s not in done]
        print(f"[ma_crime] Resuming — {len(all_slugs)} slugs remaining")

    if dry_run:
        for s, n in all_slugs:
            print(f"  {s!r:30} -> {n}")
        return 0

    total_rows = 0
    for i, (slug, display_name) in enumerate(all_slugs, 1):
        print(f"[{i}/{len(all_slugs)}] {display_name} ({slug})...", end="", flush=True)
        ori, token, rows = fetch_jurisdiction(session, slug, display_name, token)
        if not ori:
            print(" SKIPPED (no ORI)")
            continue

        if rows and engine is not None:
            with engine.begin() as conn:
                conn.execute(UPSERT, list(rows.values()))
            total_rows += len(rows)
        print(f" {ori} — {len(rows)} rows")

    if total_rows:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('ma_crime', NULL, :n, 'ok')"
            ), {"n": total_rows})

    return total_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug",    type=str, help="Single municipality slug (e.g. saugus)")
    parser.add_argument("--dry-run", action="store_true", help="List slugs without loading")
    parser.add_argument("--resume",  action="store_true", help="Skip slugs already in the database")
    args = parser.parse_args()

    slugs = [args.slug] if args.slug else None
    eng = None if args.dry_run else get_engine()

    n = run(eng, slugs=slugs, dry_run=args.dry_run, resume=args.resume)
    if not args.dry_run:
        print(f"[ma_crime] Done — {n} total rows upserted.")
