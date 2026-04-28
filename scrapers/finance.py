"""
Fetches Per-Pupil Expenditure (PPE) and district financial data from MA DOE.

Sources:
  1. Per-Pupil Expenditure Excel files from:
       https://www.doe.mass.gov/finance/statistics/ppx/
  2. Chapter 70 / DART Finance data (when available as Excel)

Run: python scrapers/finance.py [--year 2024] [--all]
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
from config import get_engine, DOE_GOV

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PPE_INDEX_URL = f"{DOE_GOV}/finance/statistics/ppx/"


def discover_ppe_files(target_year: int | None = None) -> list[dict]:
    """
    Scrapes the PPE index page for Excel download links.
    Returns list of {year: int, url: str, label: str}.
    """
    print(f"[finance] Fetching PPE index: {PPE_INDEX_URL}")
    r = requests.get(PPE_INDEX_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    files = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = a.get_text(strip=True)
        if not href.lower().endswith((".xlsx", ".xls")):
            continue
        # Extract year from link text or URL (e.g. "FY2024", "2023-24", "ppx2024")
        year_match = re.search(r"(?:fy|ppx)?(\d{4})", href + label, re.IGNORECASE)
        if not year_match:
            continue
        year = int(year_match.group(1))
        if year < 2010 or year > 2030:
            continue
        if target_year and year != target_year:
            continue
        full_url = href if href.startswith("http") else DOE_GOV + href
        files.append({"year": year, "url": full_url, "label": label})

    files = sorted(files, key=lambda x: x["year"])
    print(f"[finance] Found {len(files)} PPE file(s): {[f['year'] for f in files]}")
    return files


def parse_ppe_excel(raw_bytes: bytes, year: int) -> list[dict]:
    """
    Parses a MA DOE Per-Pupil Expenditure Excel file.
    Returns list of dicts for per_pupil_expenditure table.

    The Excel layout varies slightly by year but generally has:
      Column A: District Code (4-digit), Column B: District Name
      Remaining columns: expenditure categories (in-district, out-of-district, total, etc.)
    """
    records = []
    try:
        xl = pd.ExcelFile(io.BytesIO(raw_bytes))
    except Exception as e:
        print(f"[finance] ERROR reading Excel: {e}")
        return records

    for sheet_name in xl.sheet_names:
        try:
            df = xl.parse(sheet_name, dtype=str)
        except Exception:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        col_lower = {c.lower(): c for c in df.columns}

        # Find district code + name columns
        code_col = next((col_lower[k] for k in col_lower
                         if "code" in k or k in ("district", "org")), None)
        name_col = next((col_lower[k] for k in col_lower
                         if "name" in k or "district" in k), None)

        if not code_col:
            # Try first column as code
            code_col = df.columns[0]
        if not name_col and len(df.columns) > 1:
            name_col = df.columns[1]

        # Value columns: everything after code and name
        value_cols = [c for c in df.columns if c not in (code_col, name_col)]

        for _, row in df.iterrows():
            raw_code = str(row.get(code_col, "") or "").strip()
            if not raw_code or not raw_code.replace("0", ""):
                continue  # skip blank or all-zero rows
            # Normalise to 8-digit district org code
            if len(raw_code) <= 4:
                org_code = raw_code.zfill(4) + "0000"
            else:
                org_code = raw_code.zfill(8)

            district_name = str(row.get(name_col, "") or "").strip() if name_col else None

            for cat_col in value_cols:
                val_str = str(row.get(cat_col, "") or "").strip()
                val_str = val_str.replace("$", "").replace(",", "").replace(" ", "")
                if not val_str or val_str in ("-", "N/A", "n/a", ".", "–"):
                    continue
                try:
                    amount = float(val_str)
                except ValueError:
                    continue

                records.append({
                    "school_year":   year,
                    "org_code":      org_code,
                    "district_name": district_name,
                    "category":      str(cat_col).strip(),
                    "amount":        amount,
                })

    return records


UPSERT_PPE = text("""
    INSERT INTO per_pupil_expenditure (school_year, org_code, district_name, category, amount)
    VALUES (:school_year, :org_code, :district_name, :category, :amount)
    ON CONFLICT (school_year, org_code, category) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        amount        = EXCLUDED.amount,
        loaded_at     = NOW()
""")


def load_ppe_file(engine, file_info: dict):
    year = file_info["year"]
    url  = file_info["url"]
    print(f"[finance] Downloading PPE {year}: {url}")
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()

    records = parse_ppe_excel(r.content, year)
    if not records:
        print(f"[finance] WARNING: no records parsed from {url}")
        return 0

    with engine.begin() as conn:
        conn.execute(UPSERT_PPE, records)
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
            VALUES ('per_pupil_expenditure', :yr, :n, 'ok', :url)
        """), {"yr": year, "n": len(records), "url": url})

    print(f"[finance] PPE {year}: loaded {len(records):,} rows")
    return len(records)


def run(target_year: int | None = None, load_all: bool = False):
    engine = get_engine()
    try:
        files = discover_ppe_files(target_year=None if load_all else target_year)
    except Exception as e:
        print(f"[finance] ERROR fetching PPE index: {e}")
        return
    if not files:
        print("[finance] No PPE files found.")
        return
    if not load_all and target_year is None:
        files = [files[-1]]   # most recent only
    for f in files:
        try:
            load_ppe_file(engine, f)
        except Exception as e:
            print(f"[finance] ERROR loading {f}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MA DOE Per-Pupil Expenditure data")
    parser.add_argument("--year", type=int, help="Fiscal year to load (e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all available years")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all)
