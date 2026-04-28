"""
Loads attendance and chronic absenteeism data from DESE statewide report.

Source: profiles.doe.mass.edu/statereport/attendance.aspx
Report type: District, All Students, End-of-Year snapshot
Available: SY2018–present
  - SY2018–2020: single annual value (ddYear = '2018', '2019', '2020')
  - SY2021–present: End-of-Year snapshot (ddYear = '2021EOY', '2022EOY', ...)

Columns loaded:
  attendance_rate_pct      Attendance Rate (%)
  chronic_absenteeism_pct  Chronically Absent — 10% or more of days missed

Run: python scrapers/attendance.py [--year 2024] [--all] [--start-year 2018]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL   = "https://profiles.doe.mass.edu/statereport/attendance.aspx"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
FIRST_YEAR = 2018
LAST_YEAR  = 2025

# SY2021+ uses "EOY" suffix for end-of-year snapshot
def _year_code(year: int) -> str:
    return f"{year}EOY" if year >= 2021 else str(year)


UPSERT = text("""
    INSERT INTO attendance
        (school_year, org_code, district_name, student_group,
         attendance_rate_pct, chronic_absenteeism_pct)
    VALUES
        (:school_year, :org_code, :district_name, :student_group,
         :attendance_rate_pct, :chronic_absenteeism_pct)
    ON CONFLICT (school_year, org_code, student_group) DO UPDATE SET
        district_name           = EXCLUDED.district_name,
        attendance_rate_pct     = EXCLUDED.attendance_rate_pct,
        chronic_absenteeism_pct = EXCLUDED.chronic_absenteeism_pct,
        loaded_at               = NOW()
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
    year_code = _year_code(year)
    print(f"[attendance] Fetching SY{year} (code={year_code}) ...")

    # GET for VIEWSTATE tokens
    r_get = requests.get(BASE_URL, headers=HEADERS, timeout=20)
    r_get.raise_for_status()
    soup = BeautifulSoup(r_get.content, "html.parser")

    all_inputs = {i.get("name"): i.get("value", "")
                  for i in soup.find_all("input") if i.get("name")}

    payload = {
        **all_inputs,
        "ctl00$ContentPlaceHolder1$hfExport":     "ViewReport",
        "ctl00$ContentPlaceHolder1$ddReportType": "District",
        "ctl00$ContentPlaceHolder1$ddYear":        year_code,
        "ctl00$ContentPlaceHolder1$ddStudentGroup":"ALL",
    }

    r_post = requests.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    r_post.raise_for_status()
    soup2 = BeautifulSoup(r_post.content, "html.parser")

    # Attendance page doesn't use tblStateReport — find the first large table
    tbl = soup2.find("table", {"id": "tblStateReport"})
    if tbl is None:
        for t in soup2.find_all("table"):
            if len(t.find_all("tr")) > 10:
                tbl = t
                break

    if tbl is None:
        print(f"[attendance]   No data table found for SY{year}")
        return 0

    rows = tbl.find_all("tr")
    header = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    print(f"[attendance]   Columns: {header}")

    def _ci(keyword):
        kw = keyword.lower()
        for i, h in enumerate(header):
            if kw in h.lower():
                return i
        return None

    idx_name    = _ci("district name")      or 0
    idx_code    = _ci("district code")      or 1
    idx_att     = _ci("attendance rate")
    idx_chronic = _ci("10%")               or _ci("chronically absent")

    if idx_att is None or idx_chronic is None:
        print(f"[attendance]   Could not locate attendance/chronic columns in: {header}")
        return 0

    records = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < max(idx_name, idx_code, idx_att, idx_chronic) + 1:
            continue
        org_code = str(cells[idx_code]).strip().zfill(8)
        if not org_code or org_code == "00000000":
            continue
        records.append({
            "school_year":            year,
            "org_code":               org_code,
            "district_name":          cells[idx_name].strip() or None,
            "student_group":          "All",
            "attendance_rate_pct":    _pct(cells[idx_att]),
            "chronic_absenteeism_pct":_pct(cells[idx_chronic]),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('attendance', :yr, :n, 'ok')
            """), {"yr": year, "n": len(records)})

    print(f"[attendance]   ✓ SY{year}: {len(records)} districts loaded")
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
            time.sleep(1)

    print(f"[attendance] Done. Total rows: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load DESE attendance / chronic absenteeism data")
    parser.add_argument("--year",       type=int, help="School year (e.g. 2024)")
    parser.add_argument("--all",        action="store_true", help="Load all available years")
    parser.add_argument("--start-year", type=int, default=FIRST_YEAR,
                        help=f"First year when using --all (default {FIRST_YEAR})")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all, start_year=args.start_year)
