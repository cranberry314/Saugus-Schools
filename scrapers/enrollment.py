"""
Fetches enrollment data from MA DOE.

Sources:
  1. MA DOE Enrollment Excel downloads:
       https://www.doe.mass.gov/infoservices/reports/enroll/
  2. Fallback: profiles.doe.mass.edu enrollment data per district

Run: python scrapers/enrollment.py [--year 2024] [--all]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import re
import io
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine, DOE_BASE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

ENROLL_INDEX_URL = f"{DOE_BASE}/infoservices/reports/enroll/"


def discover_enrollment_files(target_year: int | None = None) -> list[dict]:
    """Scrapes the enrollment index page for Excel/CSV download links."""
    print(f"[enrollment] Fetching enrollment index: {ENROLL_INDEX_URL}")
    try:
        r = requests.get(ENROLL_INDEX_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[enrollment] WARNING: could not fetch index: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    files = []
    for a in soup.find_all("a", href=True):
        href  = a["href"]
        label = a.get_text(strip=True)
        if not href.lower().endswith((".xlsx", ".xls", ".csv")):
            continue
        # Extract year from label or URL
        year_match = re.search(r"(\d{4})", href + " " + label)
        if not year_match:
            continue
        year = int(year_match.group(1))
        if year < 2010 or year > 2030:
            continue
        if target_year and year != target_year:
            continue
        full_url = href if href.startswith("http") else DOE_BASE + href
        files.append({"year": year, "url": full_url, "label": label})

    files = sorted(files, key=lambda x: x["year"])
    print(f"[enrollment] Found {len(files)} file(s): {[f['year'] for f in files]}")
    return files


def parse_enrollment_excel(raw_bytes: bytes, year: int) -> list[dict]:
    """
    Parses a MA DOE enrollment Excel file.
    Returns list of dicts for enrollment table.

    Typical column layout:
      District Code | District Name | School Code | School Name | Grade | Total | Male | Female
    """
    records = []
    try:
        xl = pd.ExcelFile(io.BytesIO(raw_bytes))
    except Exception as e:
        print(f"[enrollment] ERROR reading Excel: {e}")
        return records

    for sheet_name in xl.sheet_names:
        try:
            df = xl.parse(sheet_name, dtype=str)
        except Exception:
            continue

        if df.empty or len(df.columns) < 3:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        col_lower = {c.lower(): c for c in df.columns}

        dist_code_col  = next((col_lower[k] for k in col_lower if "district" in k and "code" in k), None)
        dist_name_col  = next((col_lower[k] for k in col_lower if "district" in k and "name" in k), None)
        sch_code_col   = next((col_lower[k] for k in col_lower if "school" in k and "code" in k), None)
        sch_name_col   = next((col_lower[k] for k in col_lower if "school" in k and "name" in k), None)
        grade_col      = next((col_lower[k] for k in col_lower if "grade" in k), None)
        total_col      = next((col_lower[k] for k in col_lower if "total" in k), None)
        male_col       = next((col_lower[k] for k in col_lower if k in ("male", "m")), None)
        female_col     = next((col_lower[k] for k in col_lower if k in ("female", "f")), None)

        # If we can't find the essential columns, try positional
        if not dist_code_col:
            dist_code_col = df.columns[0]
        if not grade_col or not total_col:
            continue  # can't use this sheet

        for _, row in df.iterrows():
            raw_dist  = str(row.get(dist_code_col, "") or "").strip()
            if not raw_dist or not re.match(r"^\d+$", raw_dist):
                continue

            dist_code = raw_dist.zfill(4)
            sch_code  = str(row.get(sch_code_col, "0000") or "0000").strip().zfill(4) if sch_code_col else "0000"
            org_code  = dist_code + sch_code

            def to_int(val):
                s = str(val or "").strip().replace(",", "")
                try: return int(float(s))
                except: return None

            records.append({
                "school_year":   year,
                "org_code":      org_code,
                "district_name": str(row.get(dist_name_col, "") or "").strip() if dist_name_col else None,
                "school_name":   str(row.get(sch_name_col, "") or "").strip() if sch_name_col else None,
                "grade":         str(row.get(grade_col, "") or "").strip(),
                "total":         to_int(row.get(total_col)),
                "male":          to_int(row.get(male_col)) if male_col else None,
                "female":        to_int(row.get(female_col)) if female_col else None,
            })

    return records


UPSERT_ENROLL = text("""
    INSERT INTO enrollment (school_year, org_code, district_name, school_name, grade, total, male, female)
    VALUES (:school_year, :org_code, :district_name, :school_name, :grade, :total, :male, :female)
    ON CONFLICT (school_year, org_code, grade) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        school_name   = EXCLUDED.school_name,
        total         = EXCLUDED.total,
        male          = EXCLUDED.male,
        female        = EXCLUDED.female,
        loaded_at     = NOW()
""")


def load_enrollment_file(engine, file_info: dict):
    year = file_info["year"]
    url  = file_info["url"]
    print(f"[enrollment] Downloading enrollment {year}: {url}")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()

    records = parse_enrollment_excel(r.content, year)
    if not records:
        print(f"[enrollment] WARNING: no records parsed from {url}")
        return 0

    with engine.begin() as conn:
        conn.execute(UPSERT_ENROLL, records)
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
            VALUES ('enrollment', :yr, :n, 'ok', :url)
        """), {"yr": year, "n": len(records), "url": url})

    print(f"[enrollment] Year {year}: loaded {len(records):,} rows")
    return len(records)


def run(target_year: int | None = None, load_all: bool = False):
    engine = get_engine()
    files = discover_enrollment_files(target_year=None if load_all else target_year)
    if not files:
        print("[enrollment] No enrollment files found.")
        return
    if not load_all and target_year is None:
        files = [files[-1]]   # most recent only
    for f in files:
        try:
            load_enrollment_file(engine, f)
        except Exception as e:
            print(f"[enrollment] ERROR loading {f}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MA DOE enrollment data")
    parser.add_argument("--year", type=int, help="School year to load (ending year, e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all available years")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all)
