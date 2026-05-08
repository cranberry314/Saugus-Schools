"""
Scrapes county-level unemployment data from the Bureau of Labor Statistics
Local Area Unemployment Statistics (LAUS) program.

Source: https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}
No API key required for the v1 endpoint (up to 25 series, 10 years).

Series format: LAUCN + state_fips(2) + county_fips(3) + area_type(10) + measure(2)
  measure 03 = unemployment rate
  measure 04 = unemployment count
  measure 06 = labor force

MA counties covered: all 14.

Table populated: county_unemployment

Run:
    python scrapers/bls_laus.py              # all counties, last 5 years
    python scrapers/bls_laus.py --all        # all available years (~2000–present)
    python scrapers/bls_laus.py --county 25009  # Essex County only
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import argparse
import datetime
import requests
from sqlalchemy import text
from config import get_engine

BLS_BASE_V1 = "https://api.bls.gov/publicAPI/v1/timeseries/data"
BLS_BASE_V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

# All 14 Massachusetts counties: state_fips(25) + county_fips(3)
MA_COUNTIES = {
    "25001": "Barnstable",
    "25003": "Berkshire",
    "25005": "Bristol",
    "25007": "Dukes",
    "25009": "Essex",
    "25011": "Franklin",
    "25013": "Hampden",
    "25015": "Hampshire",
    "25017": "Middlesex",
    "25019": "Nantucket",
    "25021": "Norfolk",
    "25023": "Plymouth",
    "25025": "Suffolk",
    "25027": "Worcester",
}

# BLS area type code for counties: 15 chars of zeros
AREA_TYPE = "0000000"   # 7 zeros
MEASURE_RATE  = "003"
MEASURE_COUNT = "004"
MEASURE_LF    = "006"

def _series_id(fips: str, measure: str) -> str:
    # Format: LAUCN(5) + fips(5) + area_type(7) + measure(3) = 20 chars total
    return f"LAUCN{fips}{AREA_TYPE}{measure}"

UPSERT = text("""
    INSERT INTO county_unemployment
        (state_fips, county_fips, county_name, year, month,
         unemployment_rate, unemployed_count, labor_force)
    VALUES
        (:state_fips, :county_fips, :county_name, :year, :month,
         :unemployment_rate, :unemployed_count, :labor_force)
    ON CONFLICT (county_fips, year, month) DO UPDATE SET
        county_name       = EXCLUDED.county_name,
        unemployment_rate = EXCLUDED.unemployment_rate,
        unemployed_count  = EXCLUDED.unemployed_count,
        labor_force       = EXCLUDED.labor_force,
        loaded_at         = NOW()
""")


def _fetch_series(series_id: str, api_key: str = "", start_year: int = 2010) -> list[dict]:
    if api_key:
        # v2 API: POST with JSON body, supports API key and year range
        import json
        payload = {
            "seriesid": [series_id],
            "startyear": str(start_year),
            "endyear": str(datetime.datetime.now().year),
            "registrationkey": api_key,
        }
        r = requests.post(BLS_BASE_V2, json=payload, headers=HEADERS, timeout=30)
    else:
        # v1 API: GET, 25 req/day limit, returns last ~2 years only
        r = requests.get(f"{BLS_BASE_V1}/{series_id}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        return []
    return data["Results"]["series"][0]["data"]


def run(engine, county_fips: list[str] | None = None, start_year: int = 2010,
        api_key: str = "") -> int:
    counties = {k: v for k, v in MA_COUNTIES.items() if not county_fips or k in county_fips}
    total = 0

    for fips, name in counties.items():
        print(f"[bls_laus] {name} ({fips})...")
        try:
            rate_data   = _fetch_series(_series_id(fips, MEASURE_RATE),  api_key, start_year)
            count_data  = _fetch_series(_series_id(fips, MEASURE_COUNT), api_key, start_year)
            lf_data     = _fetch_series(_series_id(fips, MEASURE_LF),    api_key, start_year)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        # Index count and lf by (year, period)
        count_idx = {(d["year"], d["period"]): d["value"] for d in count_data}
        lf_idx    = {(d["year"], d["period"]): d["value"] for d in lf_data}

        records = []
        for pt in rate_data:
            yr = int(pt["year"])
            if yr < start_year:
                continue
            period = pt["period"]  # e.g. "M01" through "M12", "M13" = annual
            if period == "M13":
                continue   # skip annual averages; keep monthly
            month = int(period[1:])
            key = (pt["year"], period)

            def _safe(v):
                try: return float(v)
                except: return None

            records.append({
                "state_fips":       fips[:2],
                "county_fips":      fips,
                "county_name":      name,
                "year":             yr,
                "month":            month,
                "unemployment_rate":_safe(pt["value"]),
                "unemployed_count": _safe(count_idx.get(key)),
                "labor_force":      _safe(lf_idx.get(key)),
            })

        if records:
            with engine.begin() as conn:
                conn.execute(UPSERT, records)
            print(f"  {len(records)} months upserted")
            total += len(records)

        time.sleep(0.3)   # be polite to BLS

    if total:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) VALUES ('bls_laus', NULL, :n, 'ok')"
            ), {"n": total})
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all",        action="store_true", help="Load all years from 2000 (requires API key for v2)")
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--county",     type=str, help="Single county FIPS (e.g. 25009)")
    args = parser.parse_args()

    start = 2000 if args.all else args.start_year
    counties = [args.county] if args.county else None

    try:
        from config import BLS_API_KEY
    except ImportError:
        BLS_API_KEY = ""

    engine = get_engine()
    n = run(engine, county_fips=counties, start_year=start, api_key=BLS_API_KEY or "")
    print(f"[bls_laus] Done — {n} total rows upserted.")
