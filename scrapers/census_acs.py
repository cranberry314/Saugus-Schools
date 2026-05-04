"""
Loads Census ACS 5-year estimates for Massachusetts municipalities.

Source: api.census.gov — ACS 5-year, county subdivisions (cities/towns), state=25

Variables loaded:
  - Total population
  - Population 65+  (summed from age-group detail)
  - Median household income
  - Owner-occupied housing %
  - Bachelor's degree or higher %

Years supported: Census ACS 5-year from 2014 onward (each year uses ending-year label).

Run: python scrapers/census_acs.py [--year 2023] [--all] [--start-year 2014]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import re
import time
import argparse
import requests
from sqlalchemy import text
from config import get_engine

CENSUS_BASE = "https://api.census.gov/data/{year}/acs/acs5"

# ACS variables needed
# Population total + 65+ age groups (male + female)
VARS = [
    "NAME",
    "B01001_001E",   # Total population
    "B01002_001E",   # Median age
    # Male under 18 (under 5, 5-9, 10-14, 15-17)
    "B01001_003E", "B01001_004E", "B01001_005E", "B01001_006E",
    # Female under 18
    "B01001_027E", "B01001_028E", "B01001_029E", "B01001_030E",
    # Male 65+
    "B01001_020E", "B01001_021E", "B01001_022E",
    "B01001_023E", "B01001_024E", "B01001_025E",
    # Female 65+
    "B01001_044E", "B01001_045E", "B01001_046E",
    "B01001_047E", "B01001_048E", "B01001_049E",
    "B19013_001E",   # Median household income
    "B25003_001E",   # Total occupied housing units
    "B25003_002E",   # Owner-occupied
    "B15003_001E",   # Population 25+ (education denominator)
    "B15003_022E",   # Bachelor's degree
    "B15003_023E",   # Master's degree
    "B15003_024E",   # Professional school degree
    "B15003_025E",   # Doctorate degree
    # Unemployment (B23025)
    "B23025_003E",   # Civilian labor force
    "B23025_005E",   # Civilian unemployed
    # Poverty (B17001)
    "B17001_001E",   # Total for poverty universe
    "B17001_002E",   # Below poverty level
    # Foreign-born (B05002)
    "B05002_013E",   # Foreign-born population
    # Divorced (B12001)
    "B12001_001E",   # Total pop 15+
    "B12001_010E",   # Male: Divorced
    "B12001_019E",   # Female: Divorced
    # Single-parent families with children (B11003)
    "B11003_001E",   # Total families
    "B11003_010E",   # Male householder, no spouse, with own children under 18
    "B11003_016E",   # Female householder, no spouse, with own children under 18
]

MA_STATE_FIPS = "25"
FIRST_YEAR    = 2014
LAST_YEAR     = 2023   # most recent complete ACS 5-year as of 2026


def _safe_int(v, null_sentinel: int = -666666666) -> int | None:
    try:
        n = int(v)
        return None if n == null_sentinel else n
    except Exception:
        return None

def _safe_float(v, null_sentinel: int = -666666666) -> float | None:
    try:
        f = float(v)
        return None if int(f) == null_sentinel else f
    except Exception:
        return None


def _clean_town_name(raw: str) -> str:
    """
    'Abington town, Plymouth County, Massachusetts' → 'Abington'
    'Springfield city, Hampden County, Massachusetts' → 'Springfield'
    """
    # Take everything before the first comma
    name = raw.split(",")[0].strip()
    # Remove trailing 'town', 'city', 'Town', 'City', etc.
    name = re.sub(r"\s+(town|city|Town|City|CDP)$", "", name).strip()
    return name


UPSERT = text("""
    INSERT INTO municipal_census_acs
        (acs_year, state_fips, county_fips, cousub_fips, name, municipality,
         total_population, median_age, pop_under18, pct_under18,
         pop_65_plus, pct_65_plus,
         median_hh_income, total_housing_units, owner_occupied, pct_owner_occupied,
         pop_25_plus, bachelors_plus, pct_bachelors_plus,
         unemployment_rate, poverty_pct, pct_foreign_born, pct_divorced, pct_single_parent)
    VALUES
        (:acs_year, :state_fips, :county_fips, :cousub_fips, :name, :municipality,
         :total_population, :median_age, :pop_under18, :pct_under18,
         :pop_65_plus, :pct_65_plus,
         :median_hh_income, :total_housing_units, :owner_occupied, :pct_owner_occupied,
         :pop_25_plus, :bachelors_plus, :pct_bachelors_plus,
         :unemployment_rate, :poverty_pct, :pct_foreign_born, :pct_divorced, :pct_single_parent)
    ON CONFLICT (acs_year, state_fips, county_fips, cousub_fips) DO UPDATE SET
        name               = EXCLUDED.name,
        municipality       = EXCLUDED.municipality,
        total_population   = EXCLUDED.total_population,
        median_age         = EXCLUDED.median_age,
        pop_under18        = EXCLUDED.pop_under18,
        pct_under18        = EXCLUDED.pct_under18,
        pop_65_plus        = EXCLUDED.pop_65_plus,
        pct_65_plus        = EXCLUDED.pct_65_plus,
        median_hh_income   = EXCLUDED.median_hh_income,
        total_housing_units= EXCLUDED.total_housing_units,
        owner_occupied     = EXCLUDED.owner_occupied,
        pct_owner_occupied = EXCLUDED.pct_owner_occupied,
        pop_25_plus        = EXCLUDED.pop_25_plus,
        bachelors_plus     = EXCLUDED.bachelors_plus,
        pct_bachelors_plus = EXCLUDED.pct_bachelors_plus,
        unemployment_rate  = EXCLUDED.unemployment_rate,
        poverty_pct        = EXCLUDED.poverty_pct,
        pct_foreign_born   = EXCLUDED.pct_foreign_born,
        pct_divorced       = EXCLUDED.pct_divorced,
        pct_single_parent  = EXCLUDED.pct_single_parent,
        loaded_at          = NOW()
""")

MALE_U18_VARS  = [f"B01001_00{n}E" for n in range(3, 7)]         # 003–006
FEMALE_U18_VARS= [f"B01001_0{n}E" for n in range(27, 31)]       # 027–030
MALE_65_VARS   = [f"B01001_0{n:02d}E" for n in range(20, 26)]   # 020–025
FEMALE_65_VARS = [f"B01001_0{n:02d}E" for n in range(44, 50)]   # 044–049
BACH_VARS      = ["B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"]


def _load_year(engine, year: int) -> int:
    url = CENSUS_BASE.format(year=year)
    params = {
        "get":  ",".join(VARS),
        "for":  "county subdivision:*",
        "in":   f"state:{MA_STATE_FIPS}",
    }
    print(f"[census_acs] Fetching ACS {year}: {url}")
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[census_acs] ERROR fetching ACS {year}: {e}")
        return 0

    header = data[0]
    rows   = data[1:]
    print(f"[census_acs]   {len(rows)} county subdivisions returned")

    # Build column index map
    idx = {col: i for i, col in enumerate(header)}

    records = []
    skipped = 0
    for row in rows:
        name_raw = row[idx["NAME"]]
        cousub   = row[idx["county subdivision"]]
        county   = row[idx["county"]]

        # Skip "County subdivisions not defined" entries
        if cousub == "00000" or "not defined" in name_raw.lower():
            skipped += 1
            continue

        pop_total = _safe_int(row[idx["B01001_001E"]])
        if pop_total is None or pop_total == 0:
            skipped += 1
            continue

        median_age = _safe_float(row[idx["B01002_001E"]])

        pop_u18 = sum(
            (_safe_int(row[idx[v]]) or 0)
            for v in MALE_U18_VARS + FEMALE_U18_VARS
        )
        pct_u18 = round(pop_u18 / pop_total * 100, 2) if pop_total else None

        pop_65 = sum(
            (_safe_int(row[idx[v]]) or 0)
            for v in MALE_65_VARS + FEMALE_65_VARS
        )
        pct_65 = round(pop_65 / pop_total * 100, 2) if pop_total else None

        med_income = _safe_int(row[idx["B19013_001E"]])
        tot_housing= _safe_int(row[idx["B25003_001E"]])
        owner_occ  = _safe_int(row[idx["B25003_002E"]])
        pct_owner  = (round(owner_occ / tot_housing * 100, 2)
                      if tot_housing and owner_occ is not None else None)

        pop_25   = _safe_int(row[idx["B15003_001E"]])
        bach_plus= sum((_safe_int(row[idx[v]]) or 0) for v in BACH_VARS)
        pct_bach = round(bach_plus / pop_25 * 100, 2) if pop_25 and pop_25 > 0 else None

        labor_force  = _safe_int(row[idx["B23025_003E"]])
        unemployed   = _safe_int(row[idx["B23025_005E"]])
        unemp_rate   = round(unemployed / labor_force * 100, 2) if labor_force and unemployed is not None else None

        pov_total = _safe_int(row[idx["B17001_001E"]])
        pov_below = _safe_int(row[idx["B17001_002E"]])
        poverty_pct = round(pov_below / pov_total * 100, 2) if pov_total and pov_below is not None else None

        foreign_born    = _safe_int(row[idx["B05002_013E"]])
        pct_foreign_born= round(foreign_born / pop_total * 100, 2) if pop_total and foreign_born is not None else None

        pop_15plus  = _safe_int(row[idx["B12001_001E"]])
        divorced    = (_safe_int(row[idx["B12001_010E"]]) or 0) + (_safe_int(row[idx["B12001_019E"]]) or 0)
        pct_divorced= round(divorced / pop_15plus * 100, 2) if pop_15plus and pop_15plus > 0 else None

        families       = _safe_int(row[idx["B11003_001E"]])
        single_parent  = (_safe_int(row[idx["B11003_010E"]]) or 0) + (_safe_int(row[idx["B11003_016E"]]) or 0)
        pct_single_par = round(single_parent / families * 100, 2) if families and families > 0 else None

        records.append({
            "acs_year":          year,
            "state_fips":        MA_STATE_FIPS,
            "county_fips":       county,
            "cousub_fips":       cousub,
            "name":              name_raw,
            "municipality":      _clean_town_name(name_raw),
            "total_population":  pop_total,
            "median_age":        median_age,
            "pop_under18":       pop_u18,
            "pct_under18":       pct_u18,
            "pop_65_plus":       pop_65,
            "pct_65_plus":       pct_65,
            "median_hh_income":  med_income,
            "total_housing_units":tot_housing,
            "owner_occupied":    owner_occ,
            "pct_owner_occupied":pct_owner,
            "pop_25_plus":       pop_25,
            "bachelors_plus":    bach_plus,
            "pct_bachelors_plus":pct_bach,
            "unemployment_rate": unemp_rate,
            "poverty_pct":       poverty_pct,
            "pct_foreign_born":  pct_foreign_born,
            "pct_divorced":      pct_divorced,
            "pct_single_parent": pct_single_par,
        })

    print(f"[census_acs]   Parsed {len(records)} municipalities "
          f"({skipped} skipped/undefined)")

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('census_acs', :yr, :n, 'ok')
            """), {"yr": year, "n": len(records)})
        print(f"[census_acs]   ✓ ACS {year}: {len(records)} towns inserted/updated")

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
    for year in years:
        n = _load_year(engine, year)
        total += n
        if len(years) > 1:
            time.sleep(1)   # be polite to the Census API

    print(f"[census_acs] Done. Total rows loaded: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Census ACS data for MA municipalities")
    parser.add_argument("--year",       type=int, help="ACS ending year (e.g. 2023)")
    parser.add_argument("--all",        action="store_true", help="Load all years from start-year")
    parser.add_argument("--start-year", type=int, default=FIRST_YEAR,
                        help=f"Earliest year to load (default {FIRST_YEAR})")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all, start_year=args.start_year)
