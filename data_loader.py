"""
Saugus Schools Data Loader
==========================
Checks each data source for new data and loads it into the database.

Run annually (or whenever DESE/DLS publishes new data):
    python data_loader.py

For each source, the loader will:
  1. Check what year is currently in the database
  2. Check what the latest expected year is
  3. Prompt you to confirm before loading
  4. Call the appropriate scraper

Schedule:
  MA DLS Schedule A    — released ~Oct for the prior fiscal year (FY ending Jun)
  MA DESE school data  — released ~Oct for the prior school year (SY ending Jun)
  Census ACS 5-year    — released ~Dec for the year 2 years prior
  BLS CPI (FRED)       — annual average available by Feb of following year
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text
from config import get_engine

# ── Expected latest year for each source ─────────────────────────────────────
# Update these at the start of each load cycle.
# Fiscal year = year the budget year ends (FY2026 = Jul 2025 – Jun 2026).
# School year = ending calendar year (SY2025 = 2024-25 academic year).

EXPECTED = {
    "municipal_revenues":           ("fiscal_year", 2025),
    "municipal_expenditures":       ("fiscal_year", 2025),
    "mcas_results":                 ("school_year", 2025),
    "graduation_rates":             ("school_year", 2025),
    "attendance":                   ("school_year", 2025),
    "staffing":                     ("school_year", 2024),   # lags by 1yr
    "per_pupil_expenditure":        ("school_year", 2024),   # lags by 1yr
    "enrollment":                   ("school_year", 2024),   # lags by 1yr
    "municipal_census_acs":         ("acs_year",    2023),   # lags by 2yrs
    "inflation_cpi":                ("year",        2025),
    "municipal_tax_rates":          ("fiscal_year", 2026),
    "district_chapter70":           ("fiscal_year", 2026),
    "district_selected_populations":("school_year", 2025),
}

# ── Human-readable labels and which scraper handles each source ───────────────
SOURCES = [
    {
        "tables":  ["municipal_revenues", "municipal_expenditures"],
        "display": "MA DLS Schedule A — General Fund Revenues & Expenditures",
        "scraper": "scrapers.municipal_finance",
        "fn":      "run",
        "note":    "Pulls from DLS Gateway API. Loads all municipalities.",
    },
    {
        "tables":  ["mcas_results"],
        "display": "MA DESE — MCAS Results (Next Gen, Grades 3-8 + 10)",
        "scraper": "scrapers.mcas",
        "fn":      "run",
        "note":    "Pulls from MA Education-to-Career Socrata API.",
        "year_fmt": "mcas",   # special: expects list of year strings like ["2024-2025"]
    },
    {
        "tables":  ["graduation_rates"],
        "display": "MA DESE — Graduation Rates",
        "scraper": "scrapers.graduation_rates",
        "fn":      "run",
        "note":    "Pulls from profiles.doe.mass.edu.",
    },
    {
        "tables":  ["attendance"],
        "display": "MA DESE — Attendance / Chronic Absenteeism",
        "scraper": "scrapers.attendance",
        "fn":      "run",
        "note":    "Pulls from profiles.doe.mass.edu.",
    },
    {
        "tables":  ["staffing", "per_pupil_expenditure", "enrollment"],
        "display": "MA DESE — Staffing, Per-Pupil Expenditure, Enrollment",
        "scraper": "scrapers.district_csv",
        "fn":      "run",
        "note":    "Pulls from www.doe.mass.edu bulk CSV files.",
    },
    {
        "tables":  ["municipal_census_acs"],
        "display": "U.S. Census Bureau — ACS 5-Year Estimates (MA municipalities)",
        "scraper": "scrapers.census_acs",
        "fn":      "run",
        "note":    "Pulls from api.census.gov. No API key required.",
    },
    {
        "tables":  ["inflation_cpi"],
        "display": "BLS CPI-U Annual Average (via FRED CSV)",
        "scraper": "scrapers.inflation",
        "fn":      "run",
        "note":    "Reads Files/FPCPITOTLZGUSA.csv — download from fred.stlouisfed.org/series/FPCPITOTLZGUSA first.",
        "no_year": True,   # inflation scraper reads a CSV, no year argument
    },
    {
        "tables":  ["municipal_tax_rates"],
        "display": "MA DOR — Municipal Tax Rates by Class",
        "scraper": "scrapers.municipal_finance",
        "fn":      "run",
        "note":    "Same DLS Gateway call that loads tax rates alongside fiscal data.",
    },
    {
        "tables":  ["district_chapter70"],
        "display": "MA DESE — Chapter 70 Aid per Pupil",
        "scraper": "scrapers.chapter70",
        "fn":      "run",
        "note":    "Pulls from DESE Chapter 70 Excel files.",
    },
    {
        "tables":  ["district_selected_populations"],
        "display": "MA DESE — Selected Populations (High-Needs, SPED, ELL, Low-Income)",
        "scraper": "scrapers.selected_populations",
        "fn":      "run",
        "note":    "Pulls from profiles.doe.mass.edu.",
    },
]


# ── Status check ─────────────────────────────────────────────────────────────

def get_max_years(engine) -> dict[str, int | None]:
    """Return the current max year for each table in EXPECTED."""
    result = {}
    with engine.connect() as conn:
        for table, (year_col, _) in EXPECTED.items():
            try:
                row = conn.execute(
                    text(f"SELECT MAX({year_col}) FROM {table}")
                ).fetchone()
                result[table] = int(row[0]) if row and row[0] else None
            except Exception:
                result[table] = None
    return result


def print_status(max_years: dict) -> list[str]:
    """Print a status dashboard. Returns list of tables that are behind."""
    behind = []
    print()
    print("=" * 72)
    print("  DATA FRESHNESS CHECK")
    print("=" * 72)
    print(f"  {'Table':<40} {'In DB':>6}  {'Expected':>8}  Status")
    print(f"  {'-'*40} {'-'*6}  {'-'*8}  ------")

    for table, (year_col, expected) in EXPECTED.items():
        current = max_years.get(table)
        if current is None:
            status = "⚠ EMPTY"
            behind.append(table)
        elif current < expected:
            status = f"↑ NEEDS UPDATE (+{expected - current}yr)"
            behind.append(table)
        else:
            status = "✓ Current"
        current_str = str(current) if current else "—"
        print(f"  {table:<40} {current_str:>6}  {expected:>8}  {status}")

    print("=" * 72)
    print()
    return behind


# ── Load helpers ─────────────────────────────────────────────────────────────

def _prompt(question: str) -> bool:
    """Ask a yes/no question. Returns True if user says yes."""
    while True:
        ans = input(f"  {question} [y/n]: ").strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no", ""):
            return False
        print("  Please enter y or n.")


def _import_run(scraper_module: str, fn_name: str):
    """Dynamically import and return a scraper's run function."""
    import importlib
    mod = importlib.import_module(scraper_module)
    return getattr(mod, fn_name)


def _get_missing_years(tables: list[str], max_years: dict) -> list[int]:
    """Return sorted list of years that are missing for any of the given tables."""
    missing = set()
    for table in tables:
        current = max_years.get(table)
        _, expected = EXPECTED.get(table, (None, None))
        if expected is None:
            continue
        start = (current + 1) if current else expected
        for yr in range(start, expected + 1):
            missing.add(yr)
    return sorted(missing)


def load_source(source: dict, max_years: dict) -> None:
    """Attempt to load a single source after user confirmation."""
    tables  = source["tables"]
    display = source["display"]
    note    = source.get("note", "")

    missing_years = _get_missing_years(tables, max_years)
    if not missing_years:
        print(f"  ✓ {display} — already current, skipping.\n")
        return

    print(f"  {display}")
    print(f"  Note: {note}")
    print(f"  Missing years: {missing_years}")

    if not _prompt(f"Load {display}?"):
        print(f"  Skipped.\n")
        return

    run_fn = _import_run(source["scraper"], source["fn"])

    # Inflation scraper: no year argument, just reads a CSV
    if source.get("no_year"):
        print(f"  Loading...")
        run_fn()
        print(f"  Done.\n")
        return

    # MCAS scraper: expects list of year strings like ["2024-2025"]
    if source.get("year_fmt") == "mcas":
        year_strings = [f"{yr - 1}-{yr}" for yr in missing_years]
        print(f"  Loading years: {year_strings}")
        run_fn(years_to_load=year_strings)
        print(f"  Done.\n")
        return

    # Standard scrapers: run(target_year=YYYY) for each missing year
    for yr in missing_years:
        print(f"  Loading {yr}...")
        run_fn(target_year=yr)
    print(f"  Done.\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print()
    print("Saugus Schools — Data Loader")
    print("Checking database against expected latest vintages...\n")

    engine   = get_engine()
    max_yrs  = get_max_years(engine)
    behind   = print_status(max_yrs)

    if not behind:
        print("All data sources are current. Nothing to do.")
        return

    # Identify which source groups have at least one behind table
    sources_to_run = []
    for source in SOURCES:
        if any(t in behind for t in source["tables"]):
            sources_to_run.append(source)

    print(f"  {len(sources_to_run)} source(s) need updating.\n")

    if not _prompt("Review each source and choose what to load?"):
        print("Exiting.")
        return

    print()
    for source in sources_to_run:
        load_source(source, max_yrs)

    # Reprint final status
    print("\nFinal status after loading:")
    print_status(get_max_years(engine))


if __name__ == "__main__":
    main()
