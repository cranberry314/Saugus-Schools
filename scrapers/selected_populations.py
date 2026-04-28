"""
Loads DESE Selected Populations data for MA school districts.

Source: profiles.doe.mass.edu/statereport/selectedpopulations.aspx
Reports the UNDUPLICATED count/% of High Needs students (ELL + low-income + SPED
combined, each student counted once regardless of how many categories they fall into).

This is the correct figure for inter-district comparisons — it cannot be derived
by summing the three separate percentages because of student overlap.

Also loads: ELL %, First Language Not English %, Low Income %, SPED %
(these duplicate what's in the demographics table but are kept here for consistency
with the High Needs composite).

Available years: FY1992–FY2026 (fycode = ending year, e.g. 2025 = SY2024-25)
We load FY2009 onward to align with other DESE tables.

Run: python scrapers/selected_populations.py [--year 2025] [--all] [--start-year 2009]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL  = "https://profiles.doe.mass.edu/statereport/selectedpopulations.aspx"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

FIRST_YEAR = 2009   # align with enrollment / demographics tables
LAST_YEAR  = 2026   # most recent available


UPSERT = text("""
    INSERT INTO district_selected_populations
        (school_year, org_code, district_name,
         high_needs_count, high_needs_pct,
         ell_count, ell_pct,
         flne_count, flne_pct,
         low_income_count, low_income_pct,
         sped_count, sped_pct)
    VALUES
        (:school_year, :org_code, :district_name,
         :high_needs_count, :high_needs_pct,
         :ell_count, :ell_pct,
         :flne_count, :flne_pct,
         :low_income_count, :low_income_pct,
         :sped_count, :sped_pct)
    ON CONFLICT (school_year, org_code) DO UPDATE SET
        district_name      = EXCLUDED.district_name,
        high_needs_count   = EXCLUDED.high_needs_count,
        high_needs_pct     = EXCLUDED.high_needs_pct,
        ell_count          = EXCLUDED.ell_count,
        ell_pct            = EXCLUDED.ell_pct,
        flne_count         = EXCLUDED.flne_count,
        flne_pct           = EXCLUDED.flne_pct,
        low_income_count   = EXCLUDED.low_income_count,
        low_income_pct     = EXCLUDED.low_income_pct,
        sped_count         = EXCLUDED.sped_count,
        sped_pct           = EXCLUDED.sped_pct,
        loaded_at          = NOW()
""")


def _to_int(s) -> int | None:
    try:
        return int(str(s).replace(",", "").strip())
    except Exception:
        return None


def _to_float(s) -> float | None:
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return None


def _load_year(engine, fycode: int) -> int:
    """
    Fetches the Selected Populations district report for one year.
    The page requires:
      1. GET to obtain fresh ASP.NET ViewState tokens
      2. POST with ViewReport to render the HTML table
    """
    s = requests.Session()
    s.headers.update(HEADERS)

    # Step 1: GET fresh tokens
    try:
        r = s.get(BASE_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[selected_pop] ERROR getting page for FY{fycode}: {e}")
        return 0

    soup = BeautifulSoup(r.text, "html.parser")

    def _val(name):
        el = soup.find("input", {"name": name})
        return el["value"] if el else ""

    # Step 2: POST ViewReport
    post_data = {
        "__VIEWSTATE":            _val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR":   _val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION":      _val("__EVENTVALIDATION"),
        "ctl00$ContentPlaceHolder1$ddReportType": "District",
        "ctl00$ContentPlaceHolder1$ddYear":       str(fycode),
        "ctl00$ContentPlaceHolder1$hfExport":     "ViewReport",
        "ctl00$ContentPlaceHolder1$btnViewReport": "View Report",
    }
    try:
        r2 = s.post(BASE_URL, data=post_data, timeout=60)
        r2.raise_for_status()
    except Exception as e:
        print(f"[selected_pop] ERROR posting FY{fycode}: {e}")
        return 0

    soup2 = BeautifulSoup(r2.text, "html.parser")
    table = soup2.find("table", {"id": "tblStateReport"})
    if table is None:
        print(f"[selected_pop] WARNING: tblStateReport not found for FY{fycode}")
        return 0

    rows = table.find_all("tr")
    if len(rows) < 3:
        print(f"[selected_pop] WARNING: too few rows ({len(rows)}) for FY{fycode}")
        return 0

    # Row 0: column group headers  (District Name | District Code | High Needs | ELL | FLNE | Low Income | SPED)
    # Row 1: # / % subheaders
    # Row 2+: data
    #
    # Column layout (0-indexed):
    #   0  District Name
    #   1  District Code
    #   2  High Needs #      3  High Needs %
    #   4  ELL #             5  ELL %
    #   6  FLNE #            7  FLNE %
    #   8  Low Income #      9  Low Income %
    #  10  SPED #           11  SPED %

    records = []
    skipped = 0
    for row in rows[2:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 10:
            skipped += 1
            continue

        org_code = str(cells[1]).strip()
        if not org_code or not org_code[0].isdigit():
            skipped += 1
            continue

        # Pad org_code to 8 chars if needed (DESE uses 8-digit codes)
        if len(org_code) < 8:
            org_code = org_code.zfill(8)

        dist_name = str(cells[0]).strip()
        # Strip trailing "(District)" suffix that DESE appends
        if dist_name.endswith(" (District)"):
            dist_name = dist_name[:-len(" (District)")]

        def _c(i): return cells[i] if i < len(cells) else None

        records.append({
            "school_year":      fycode,
            "org_code":         org_code,
            "district_name":    dist_name,
            "high_needs_count": _to_int(_c(2)),
            "high_needs_pct":   _to_float(_c(3)),
            "ell_count":        _to_int(_c(4)),
            "ell_pct":          _to_float(_c(5)),
            "flne_count":       _to_int(_c(6)),
            "flne_pct":         _to_float(_c(7)),
            "low_income_count": _to_int(_c(8)),
            "low_income_pct":   _to_float(_c(9)),
            "sped_count":       _to_int(_c(10)),
            "sped_pct":         _to_float(_c(11)),
        })

    print(f"[selected_pop]   FY{fycode}: {len(records)} districts "
          f"({skipped} skipped rows)")

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('selected_populations', :yr, :n, 'ok')
            """), {"yr": fycode, "n": len(records)})
        print(f"[selected_pop]   ✓ FY{fycode}: {len(records)} rows upserted")

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

    print(f"[selected_pop] Loading years: {years}")
    total = 0
    for yr in years:
        n = _load_year(engine, yr)
        total += n
        if len(years) > 1:
            time.sleep(0.5)   # polite to DESE servers

    print(f"[selected_pop] Done. Total rows loaded: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load DESE Selected Populations (High Needs %) by district")
    parser.add_argument("--year",       type=int, help="Single fiscal year (ending year, e.g. 2025)")
    parser.add_argument("--all",        action="store_true", help="Load all years from start-year")
    parser.add_argument("--start-year", type=int, default=FIRST_YEAR,
                        help=f"Earliest year to load when using --all (default {FIRST_YEAR})")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all, start_year=args.start_year)
