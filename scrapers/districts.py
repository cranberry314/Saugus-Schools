"""
Fetches all MA public school district org codes and names from:
  https://profiles.doe.mass.edu/search/search.aspx

Populates the `districts` table.
Run: python scrapers/districts.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine, PROFILES_BASE, DOE_BASE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# MA DOE publishes a full district/school list as a downloadable Excel from SIMS school codes page
# Fallback: scrape profiles search
SIMS_SCHOOL_CODES_URL = f"{DOE_BASE}/infoservices/data/sims/schoolcodes.html"


def fetch_district_list_from_profiles() -> list[dict]:
    """
    Scrapes the MA DOE profiles search to get all public school district org codes.
    Returns list of dicts with keys: org_code, name, town, district_type.
    """
    print("[districts] Fetching district list from profiles search...")
    # The profiles site has a directory/search endpoint
    # org type 5 = public school districts
    url = f"{PROFILES_BASE}/search/search.aspx"
    params = {
        "leftNavId": "11",
        "orgTypeCode": "5",   # Public school districts
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[districts] WARNING: profiles search failed: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    print(f"[districts] Profiles search response: HTTP {r.status_code}, {len(r.text)} chars")

    # Count all links and orgcode links for debugging
    all_links = soup.find_all("a", href=True)
    orgcode_links = [a for a in all_links if "orgcode=" in a.get("href", "").lower()]
    print(f"[districts] Page has {len(all_links)} total links, {len(orgcode_links)} with orgcode=")

    records = []

    # The search results are in a table or list; parse all org links
    # Links look like: /general/general.aspx?...&orgcode=00760000&...
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "orgcode=" in href.lower():
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(href).query)
            # case-insensitive key lookup
            code_key = next((k for k in qs if k.lower() == "orgcode"), None)
            if code_key:
                org_code = qs[code_key][0].strip()
                name = a.get_text(strip=True)
                if org_code and name:
                    records.append({
                        "org_code": org_code,
                        "name": name,
                        "town": None,
                        "district_type": "Public School District",
                        "is_district": org_code.endswith("0000"),
                        "district_code": org_code[:4] + "0000" if not org_code.endswith("0000") else org_code,
                    })

    # Deduplicate
    seen = set()
    unique = []
    for r in records:
        if r["org_code"] not in seen:
            seen.add(r["org_code"])
            unique.append(r)

    print(f"[districts] Found {len(unique)} entries from profiles search")
    return unique


def fetch_sims_school_codes() -> list[dict]:
    """
    Fetches school code Excel files from the MA DOE SIMS school codes page.
    These Excel files contain the full district/school directory with org codes.
    """
    print("[districts] Fetching SIMS school codes page...")
    try:
        r = requests.get(SIMS_SCHOOL_CODES_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[districts] WARNING: SIMS page failed: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records = []

    # Look for Excel/CSV download links on the page
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith((".xlsx", ".xls", ".csv")):
            full_url = href if href.startswith("http") else DOE_BASE + href
            label = a.get_text(strip=True)
            print(f"[districts]   Found file: {label} → {full_url}")
            try:
                df = _download_and_parse_school_codes(full_url, label)
                records.extend(df)
            except Exception as e:
                print(f"[districts]   WARNING: could not parse {full_url}: {e}")

    return records


def _download_and_parse_school_codes(url: str, label: str) -> list[dict]:
    """Downloads and parses a school code Excel/CSV file from MA DOE."""
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()

    if url.lower().endswith(".csv"):
        import io
        df = pd.read_csv(io.BytesIO(r.content))
    else:
        import io
        df = pd.read_excel(io.BytesIO(r.content), dtype=str)

    df.columns = [str(c).strip() for c in df.columns]

    records = []
    # Column names vary; try to find org code, name, town columns
    col_map = {c.lower(): c for c in df.columns}
    org_col  = next((col_map[k] for k in col_map if "org" in k or "code" in k), None)
    name_col = next((col_map[k] for k in col_map if "name" in k or "district" in k or "school" in k), None)
    town_col = next((col_map[k] for k in col_map if "town" in k or "city" in k or "municipality" in k), None)
    type_col = next((col_map[k] for k in col_map if "type" in k), None)

    if not org_col or not name_col:
        print(f"[districts]   Could not map columns: {list(df.columns)}")
        return []

    for _, row in df.iterrows():
        org_code = str(row.get(org_col, "") or "").strip().zfill(8)
        name     = str(row.get(name_col, "") or "").strip()
        town     = str(row.get(town_col, "") or "").strip() if town_col else None
        dtype    = str(row.get(type_col, "") or "").strip() if type_col else None
        if org_code and name and org_code != "00000000":
            records.append({
                "org_code":      org_code,
                "name":          name,
                "town":          town or None,
                "district_type": dtype or ("Public School District" if org_code.endswith("0000") else "Public School"),
                "is_district":   org_code.endswith("0000"),
                "district_code": org_code[:4] + "0000",
            })

    print(f"[districts]   Parsed {len(records)} rows from {label}")
    return records


def upsert_districts(engine, records: list[dict]):
    """Inserts or updates district records."""
    if not records:
        print("[districts] No records to upsert.")
        return
    with engine.begin() as conn:
        for rec in records:
            conn.execute(text("""
                INSERT INTO districts (org_code, name, town, district_type, is_district, district_code, updated_at)
                VALUES (:org_code, :name, :town, :district_type, :is_district, :district_code, NOW())
                ON CONFLICT (org_code) DO UPDATE SET
                    name          = EXCLUDED.name,
                    town          = COALESCE(EXCLUDED.town, districts.town),
                    district_type = COALESCE(EXCLUDED.district_type, districts.district_type),
                    is_district   = EXCLUDED.is_district,
                    district_code = EXCLUDED.district_code,
                    updated_at    = NOW()
            """), rec)
    print(f"[districts] Upserted {len(records)} district/school records.")


def seed_districts_from_mcas(engine) -> int:
    """
    Populates the districts table from data already in mcas_results.
    Used as a last-resort fallback when web scraping yields nothing.
    Returns count of rows inserted.
    """
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO districts (org_code, name, is_district, district_code)
            SELECT DISTINCT
                org_code,
                COALESCE(NULLIF(district_name, ''), org_code) AS name,
                TRUE,
                LEFT(org_code, 4) || '0000'
            FROM mcas_results
            WHERE org_code IS NOT NULL AND org_code <> ''
            ON CONFLICT (org_code) DO UPDATE SET
                name       = COALESCE(NULLIF(EXCLUDED.name, ''), districts.name),
                updated_at = NOW()
        """))
        count = result.rowcount
    print(f"[districts] Seeded {count} district entries from MCAS data.")
    return count


def run():
    engine = get_engine()
    scraped_count = 0

    records = fetch_sims_school_codes()
    if not records:
        print("[districts] SIMS fetch returned nothing — falling back to profiles search")
        records = fetch_district_list_from_profiles()

    scraped_count = len(records)
    upsert_districts(engine, records)

    # Seed from MCAS if scraping failed or returned suspiciously few results
    with engine.connect() as conn:
        mcas_count = conn.execute(text("SELECT COUNT(*) FROM mcas_results")).scalar()
        dist_count = conn.execute(text("SELECT COUNT(*) FROM districts")).scalar()

    print(f"[districts] DB state: {dist_count} districts, {mcas_count} MCAS rows, {scraped_count} scraped this run")

    if mcas_count > 0 and scraped_count < 10:
        print("[districts] Scraping returned too few results — seeding from MCAS data")
        seeded = seed_districts_from_mcas(engine)
        with engine.connect() as conn:
            dist_count = conn.execute(text("SELECT COUNT(*) FROM districts")).scalar()
        print(f"[districts] After MCAS seed: {dist_count} total districts in DB")
    else:
        print(f"[districts] {dist_count} total districts in DB")

    # Sample a few districts for verification
    with engine.connect() as conn:
        sample = conn.execute(text(
            "SELECT org_code, name, town FROM districts "
            "WHERE is_district = TRUE ORDER BY name LIMIT 5"
        )).fetchall()
    if sample:
        print("[districts] Sample districts (first 5 alphabetically):")
        for row in sample:
            print(f"  {row[0]}  {row[1]}  ({row[2]})")
    else:
        print("[districts] WARNING: No district entries found in DB")


if __name__ == "__main__":
    run()
