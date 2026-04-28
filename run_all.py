"""
Master ingestion script — runs all scrapers in order.

Usage:
  python run_all.py              # Load most recent year of all sources
  python run_all.py --all        # Load all available years
  python run_all.py --year 2024  # Load a specific year

Steps:
  1. Init DB (idempotent — safe to re-run)
  2. Load district/school directory
  3. Load MCAS results              (disabled — data already loaded Mar 2026)
  4. Load district CSV data         (disabled — data already loaded Mar 2026)
  5. Load school expenditures       (disabled — data already loaded Mar 2026)
  6. Load municipal finance data    (disabled — data already loaded Mar 2026)
  6b. Chapter 70 state aid          (disabled — data already loaded Mar 2026)
  6c. Census ACS demographics       (disabled — data already loaded Mar 2026)
  6d. FRED CPI inflation            (disabled — data already loaded Mar 2026)
  6e. Zillow housing data           (NEW)
  7. Compute Saugus peer districts
"""
import argparse

from db.init_db import create_database_if_not_exists, apply_schema
from scrapers.zillow_housing import run as run_zillow
from scrapers.selected_populations import run as run_selected_pop
from analysis.peers import run as run_peers

SAUGUS_ORG_CODE = "02620000"   # Saugus district org code (from MCAS data)


def main():
    parser = argparse.ArgumentParser(description="Load all MA school data")
    parser.add_argument("--year", type=int, help="School year (ending year, e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all available years")
    parser.add_argument("--skip-peers", action="store_true", help="Skip Mahalanobis peer calculation")
    args = parser.parse_args()

    print("=" * 60)
    print("MA Schools — Full Data Ingestion")
    print("=" * 60)

    # 1. Init DB
    print("\n[1/7] Initialising database...")
    create_database_if_not_exists()
    apply_schema()

    # 2. Districts — quick, idempotent, keep active for future re-runs
    # (disabled — data already loaded Mar 2026)
    # print("\n[2/7] Loading district directory...")
    # run_districts()

    # 3. MCAS (disabled — data already loaded Mar 2026)
    # from scrapers.mcas import run as run_mcas, _resolve_endpoint
    # print("\n[3/7] Loading MCAS data...")
    # if args.all:
    #     _, years, _ = _resolve_endpoint()
    #     run_mcas(years_to_load=years)
    # elif args.year:
    #     run_mcas(years_to_load=[str(args.year)])
    # else:
    #     run_mcas()

    # 2b. Re-seed districts from MCAS
    # (disabled — data already loaded Mar 2026)
    # print("\n[2b/7] Seeding district directory from MCAS data (if needed)...")
    # run_districts()

    # 4. District CSV data — enrollment, demographics, PPE, district school financials
    #    (disabled — data already loaded Mar 2026 from Files/District_Expenditures_by_Spending_Category_*.csv)
    # from scrapers.district_csv import run as run_district_csv
    # print("\n[4/7] Loading district CSV data...")
    # try:
    #     run_district_csv(target_year=args.year, load_all=args.all)
    # except Exception as e:
    #     print(f"[district_csv] ERROR: {e}")

    # 5. School-level expenditures
    #    (disabled — data already loaded Mar 2026 from Files/School_Expenditures_by_Spending_Category_*.csv)
    # from scrapers.school_finance import run as run_school_finance
    # print("\n[5/7] Loading school-level expenditures...")
    # try:
    #     run_school_finance(target_year=args.year, load_all=args.all)
    # except Exception as e:
    #     print(f"[school_finance] ERROR: {e}")

    # 6. Municipal finance (MA DLS Gateway)
    #    (disabled — data already loaded Mar 2026; re-enable to refresh)
    # from scrapers.municipal_finance import run as run_municipal_finance
    # print("\n[6/7] Loading municipal finance data...")
    # try:
    #     run_municipal_finance(target_year=args.year, load_all=args.all)
    # except Exception as e:
    #     print(f"[municipal_finance] ERROR: {e}")

    # 6b. Chapter 70 state aid (DESE, FY2023–present)
    #     (disabled — data already loaded Mar 2026; re-enable when DESE publishes new year)
    # from scrapers.chapter70 import run as run_chapter70
    # print("\n[6b/7] Loading Chapter 70 aid...")
    # try:
    #     run_chapter70(load_all=True)
    # except Exception as e:
    #     print(f"[chapter70] ERROR: {e}")

    # 6c. Census ACS demographics (Census Bureau API)
    #     (disabled — 2014-2023 loaded Mar 2026; re-enable when 2024 ACS is published ~Dec 2025)
    # from scrapers.census_acs import run as run_census_acs
    # print("\n[6c/7] Loading Census ACS data...")
    # try:
    #     run_census_acs(load_all=args.all)
    # except Exception as e:
    #     print(f"[census_acs] ERROR: {e}")

    # 6d. FRED CPI inflation (local CSV — no network needed)
    #     (disabled — 2014-2024 loaded Mar 2026; re-enable after updating Files/FPCPITOTLZGUSA.csv)
    # from scrapers.inflation import run as run_inflation
    # print("\n[6d/7] Loading FRED CPI inflation...")
    # try:
    #     run_inflation()
    # except Exception as e:
    #     print(f"[inflation] ERROR: {e}")

    # 6e. Zillow housing data (MA municipalities, monthly)
    #     (disabled — loaded Mar 2026; re-enable to refresh)
    # print("\n[6e/7] Loading Zillow housing data...")
    # try:
    #     run_zillow()
    # except Exception as e:
    #     print(f"[zillow] ERROR: {e}")

    # 6f. DESE Selected Populations — unduplicated High Needs % by district
    print("\n[6f/7] Loading DESE Selected Populations (High Needs)...")
    try:
        run_selected_pop(load_all=args.all)
    except Exception as e:
        print(f"[selected_pop] ERROR: {e}")

    # 7. Peer districts (Mahalanobis)
    if not args.skip_peers:
        print("\n[7/7] Computing Saugus peer districts...")
        from sqlalchemy import text as _text
        from config import get_engine as _get_engine
        with _get_engine().connect() as _conn:
            year = _conn.execute(_text("SELECT MAX(school_year) FROM mcas_results")).scalar() or args.year or 2025
        print(f"[peers] Using year={year} (most recent MCAS data)")
        try:
            run_peers(district=SAUGUS_ORG_CODE, year=year)
        except Exception as e:
            print(f"[peers] Skipped (not enough data yet): {e}")
    else:
        print("\n[7/7] Skipping peer computation (--skip-peers)")

    print("\n" + "=" * 60)
    print("Ingestion complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
