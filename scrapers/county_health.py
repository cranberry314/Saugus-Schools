"""
Scrapes county-level health outcomes from the County Health Rankings program
(Robert Wood Johnson Foundation / University of Wisconsin Population Health Institute).

Source: https://www.countyhealthrankings.org/ — annual Excel downloads

Key metrics captured:
  - % fair or poor health
  - Avg physically unhealthy days / month
  - Avg mentally unhealthy days / month
  - % low birthweight
  - % adults smoking
  - % adults obese
  - % physically inactive
  - % excessive drinking
  - % uninsured
  - % children in poverty
  - Income ratio (80th/20th percentile income)
  - % children in single-parent households
  - % completed high school
  - % some college
  - % unemployed
  - Life expectancy (from Additional Measures sheet)

Table populated: county_health_rankings

Run:
    python scrapers/county_health.py              # most recent year
    python scrapers/county_health.py --all        # all available years (2015–present)
    python scrapers/county_health.py --year 2024
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import time
import argparse
import requests
import pandas as pd
from sqlalchemy import text
from config import get_engine

HEADERS = {"User-Agent": "Mozilla/5.0"}

# URL pattern — CHR publishes annually; year refers to the ranking year
def _excel_url(year: int) -> str:
    base = "https://www.countyhealthrankings.org/sites/default/files/media/document"
    name = f"{year}%20County%20Health%20Rankings%20Massachusetts%20Data"
    return f"{base}/{name}%20-%20v2.xlsx"   # v1 fallback handled in _load_year

CURRENT_YEAR = 2024
FIRST_YEAR   = 2015   # earlier years have different formats

UPSERT = text("""
    INSERT INTO county_health_rankings
        (ranking_year, state_fips, county_fips, county_name,
         pct_fair_poor_health, avg_physically_unhealthy_days, avg_mentally_unhealthy_days,
         pct_low_birthweight, pct_smokers, pct_obese, pct_physically_inactive,
         pct_excessive_drinking, pct_uninsured,
         pct_children_poverty, income_ratio, pct_children_single_parent,
         pct_hs_completed, pct_some_college, pct_unemployed)
    VALUES
        (:ranking_year, :state_fips, :county_fips, :county_name,
         :pct_fair_poor_health, :avg_physically_unhealthy_days, :avg_mentally_unhealthy_days,
         :pct_low_birthweight, :pct_smokers, :pct_obese, :pct_physically_inactive,
         :pct_excessive_drinking, :pct_uninsured,
         :pct_children_poverty, :income_ratio, :pct_children_single_parent,
         :pct_hs_completed, :pct_some_college, :pct_unemployed)
    ON CONFLICT (ranking_year, county_fips) DO UPDATE SET
        county_name                   = EXCLUDED.county_name,
        pct_fair_poor_health          = EXCLUDED.pct_fair_poor_health,
        avg_physically_unhealthy_days = EXCLUDED.avg_physically_unhealthy_days,
        avg_mentally_unhealthy_days   = EXCLUDED.avg_mentally_unhealthy_days,
        pct_low_birthweight           = EXCLUDED.pct_low_birthweight,
        pct_smokers                   = EXCLUDED.pct_smokers,
        pct_obese                     = EXCLUDED.pct_obese,
        pct_physically_inactive       = EXCLUDED.pct_physically_inactive,
        pct_excessive_drinking        = EXCLUDED.pct_excessive_drinking,
        pct_uninsured                 = EXCLUDED.pct_uninsured,
        pct_children_poverty          = EXCLUDED.pct_children_poverty,
        income_ratio                  = EXCLUDED.income_ratio,
        pct_children_single_parent    = EXCLUDED.pct_children_single_parent,
        pct_hs_completed              = EXCLUDED.pct_hs_completed,
        pct_some_college              = EXCLUDED.pct_some_college,
        pct_unemployed                = EXCLUDED.pct_unemployed,
        loaded_at                     = NOW()
""")


def _safe(val):
    try:
        f = float(val)
        return None if pd.isna(f) else round(f, 4)
    except Exception:
        return None


def _load_year(engine, year: int) -> int:
    url = _excel_url(year)
    print(f"[county_health] {year}: downloading from CHR...")
    raw = None
    base = "https://www.countyhealthrankings.org/sites/default/files/media/document"
    name = f"{year}%20County%20Health%20Rankings%20Massachusetts%20Data"
    for suffix in ["%20-%20v2.xlsx", "%20-%20v1.xlsx", ".xlsx"]:
        try:
            r = requests.get(f"{base}/{name}{suffix}", headers=HEADERS, timeout=60)
            if r.status_code == 200 and r.content[:2] == b"PK":
                raw = r.content
                break
        except Exception:
            continue
    if raw is None:
        print(f"  No Excel file found for {year}")
        return 0
    r = type("R", (), {"content": raw})()

    xl = pd.ExcelFile(io.BytesIO(r.content))

    # Sheet name changed between years
    sheet = None
    for candidate in ["Select Measure Data", "Ranked Measure Data"]:
        if candidate in xl.sheet_names:
            sheet = candidate
            break
    if sheet is None:
        print(f"  No ranked data sheet found for {year} (sheets: {xl.sheet_names})")
        return 0

    df = xl.parse(sheet, header=1, dtype=str)

    # Column names vary slightly by year; match by keyword
    def _find_col(df, *keywords):
        for col in df.columns:
            col_lower = col.lower()
            if all(kw.lower() in col_lower for kw in keywords):
                return col
        return None

    records = []
    for _, row in df.iterrows():
        fips = str(row.get("FIPS", "")).strip().zfill(5)
        county = str(row.get("County", "")).strip()
        if not fips or fips == "00000" or not county or county == "nan":
            continue

        records.append({
            "ranking_year":                year,
            "state_fips":                  fips[:2],
            "county_fips":                 fips,
            "county_name":                 county,
            "pct_fair_poor_health":        _safe(row.get("% Fair or Poor Health")),
            "avg_physically_unhealthy_days":_safe(row.get("Average Number of Physically Unhealthy Days")),
            "avg_mentally_unhealthy_days": _safe(row.get("Average Number of Mentally Unhealthy Days")),
            "pct_low_birthweight":         _safe(row.get("% Low Birthweight")),
            "pct_smokers":                 _safe(row.get("% Adults Reporting Currently Smoking")),
            "pct_obese":                   _safe(row.get("% Adults with Obesity")),
            "pct_physically_inactive":     _safe(row.get("% Physically Inactive")),
            "pct_excessive_drinking":      _safe(row.get("% Excessive Drinking")),
            "pct_uninsured":               _safe(row.get("% Uninsured")),
            "pct_children_poverty":        _safe(row.get("% Children in Poverty")),
            "income_ratio":                _safe(row.get("Income Ratio")),
            "pct_children_single_parent":  _safe(row.get("% Children in Single-Parent Households")),
            "pct_hs_completed":            _safe(row.get("% Completed High School")),
            "pct_some_college":            _safe(row.get("% Some College")),
            "pct_unemployed":              _safe(row.get("% Unemployed")),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
        print(f"  {len(records)} counties upserted")

    return len(records)


def run(engine, years: list[int] | None = None) -> int:
    if years is None:
        years = [CURRENT_YEAR]
    total = 0
    for year in years:
        n = _load_year(engine, year)
        total += n
        time.sleep(0.5)

    if total:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) VALUES ('county_health_rankings', NULL, :n, 'ok')"
            ), {"n": total})
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",       type=int, help="Ranking year (default: most recent)")
    parser.add_argument("--all",        action="store_true", help=f"Load all years from {FIRST_YEAR}")
    parser.add_argument("--start-year", type=int, default=FIRST_YEAR)
    args = parser.parse_args()

    if args.year:
        years = [args.year]
    elif args.all:
        years = list(range(args.start_year, CURRENT_YEAR + 1))
    else:
        years = [CURRENT_YEAR]

    engine = get_engine()
    n = run(engine, years=years)
    print(f"[county_health] Done — {n} total rows upserted.")
