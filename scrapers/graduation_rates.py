"""
Loads graduation rate data from DESE statewide report.

Source: profiles.doe.mass.edu/statereport/gradrates.aspx
Report type: District, All Students, 4-Year Cohort
Available: SY2017–present (fycode = ending year)

Columns loaded:
  four_year_grad_pct   % Graduated (4-year cohort)
  dropout_pct          % Dropped Out

Run: python scrapers/graduation_rates.py [--year 2024] [--all] [--start-year 2017]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL   = "https://profiles.doe.mass.edu/statereport/gradrates.aspx"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
FIRST_YEAR = 2017
LAST_YEAR  = 2025

UPSERT = text("""
    INSERT INTO graduation_rates
        (school_year, org_code, district_name, student_group,
         four_year_grad_pct, dropout_pct)
    VALUES
        (:school_year, :org_code, :district_name, :student_group,
         :four_year_grad_pct, :dropout_pct)
    ON CONFLICT (school_year, org_code, student_group) DO UPDATE SET
        district_name      = EXCLUDED.district_name,
        four_year_grad_pct = EXCLUDED.four_year_grad_pct,
        dropout_pct        = EXCLUDED.dropout_pct,
        loaded_at          = NOW()
""")


def _pct(s) -> float | None:
    s = str(s or "").strip()
    if not s or s in ("-", "–", "N/A", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _load_year(engine, year: int) -> int:
    print(f"[graduation] Fetching SY{year} ...")

    # Step 1: GET to collect VIEWSTATE tokens
    r_get = requests.get(BASE_URL, headers=HEADERS, timeout=20)
    r_get.raise_for_status()
    soup = BeautifulSoup(r_get.content, "html.parser")

    vs  = soup.find("input", {"id": "__VIEWSTATE"})
    ev  = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

    # Start with all existing form inputs (captures hidden fields), then override
    all_inputs = {i.get("name"): i.get("value", "")
                  for i in soup.find_all("input") if i.get("name")}
    payload = {
        **all_inputs,
        "ctl00$ContentPlaceHolder1$hfExport":    "ViewReport",
        "ctl00$ContentPlaceHolder1$ddReportType":"District",
        "ctl00$ContentPlaceHolder1$ddYear":      str(year),
        "ctl00$ContentPlaceHolder1$ddRateType":  "4-Year:REG",
        "ctl00$ContentPlaceHolder1$ddSubgroup":  "ALL",
    }

    # Step 2: POST to retrieve the table
    r_post = requests.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    r_post.raise_for_status()
    soup2 = BeautifulSoup(r_post.content, "html.parser")

    tbl = soup2.find("table", {"id": "tblStateReport"})
    if tbl is None:
        # Fall back to first sizeable table
        for t in soup2.find_all("table"):
            if len(t.find_all("tr")) > 10:
                tbl = t
                break

    if tbl is None:
        print(f"[graduation]   No data table found for SY{year}")
        return 0

    rows = tbl.find_all("tr")
    # First non-empty row is the header
    header = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    print(f"[graduation]   Columns: {header}")

    # Map column names (flexible in case DESE renames them)
    def _ci(keyword):
        for i, h in enumerate(header):
            if keyword.lower() in h.lower():
                return i
        return None

    idx_name    = _ci("district name")   or 0
    idx_code    = _ci("district code")   or 1
    idx_grad    = _ci("graduated")       or _ci("% graduated")
    idx_dropout = _ci("dropped out")     or _ci("dropout")

    if idx_grad is None or idx_dropout is None:
        print(f"[graduation]   Could not locate grad/dropout columns: {header}")
        return 0

    records = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < max(idx_name, idx_code, idx_grad, idx_dropout) + 1:
            continue
        org_code = str(cells[idx_code]).strip().zfill(8)
        if not org_code or org_code == "00000000":
            continue
        records.append({
            "school_year":       year,
            "org_code":          org_code,
            "district_name":     cells[idx_name].strip() or None,
            "student_group":     "All",
            "four_year_grad_pct":_pct(cells[idx_grad]),
            "dropout_pct":       _pct(cells[idx_dropout]),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('graduation_rates', :yr, :n, 'ok')
            """), {"yr": year, "n": len(records)})

    print(f"[graduation]   ✓ SY{year}: {len(records)} districts loaded")
    return len(records)


def run(target_year: int | None = None, load_all: bool = False,
        start_year: int = FIRST_YEAR):
    engine = get_engine()
    if target_year:
        years = [target_year]
    elif load_all:
        years = list(range(start_year, LAST_YEAR + 1))
    else:
        years = [LAST_YEAR]

    total = 0
    for i, y in enumerate(years):
        total += _load_year(engine, y)
        if i < len(years) - 1:
            time.sleep(1)   # be polite to DESE server

    print(f"[graduation] Done. Total rows: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load DESE graduation rate data")
    parser.add_argument("--year",       type=int, help="School year (e.g. 2024)")
    parser.add_argument("--all",        action="store_true", help="Load all available years")
    parser.add_argument("--start-year", type=int, default=FIRST_YEAR,
                        help=f"First year when using --all (default {FIRST_YEAR})")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all, start_year=args.start_year)
