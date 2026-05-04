"""
Scrapes MA municipal assessed values by property class from the DLS Gateway LA-4 report.

Source: https://dls-gw.dor.state.ma.us/gateway/DLSPublic/ComparisonReport/GetLa4ComparisonData

Each request returns one municipality + one fiscal year. Loops over all active
municipalities (pulled from existing municipal_tax_rates rows) for each year.

Table populated: municipal_assessed_values

Run:
    python scrapers/assessed_values.py              # current + prior year
    python scrapers/assessed_values.py --year 2024  # single year
    python scrapers/assessed_values.py --all        # all years 2010–present
    python scrapers/assessed_values.py --muni 262   # single municipality (Saugus)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import re
import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL = "https://dls-gw.dor.state.ma.us/gateway/DLSPublic/ComparisonReport"
DATA_URL = BASE_URL + "/GetLa4ComparisonData"
DEFAULT_START_YEAR = 2010

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": BASE_URL,
}

UPSERT = text("""
    INSERT INTO municipal_assessed_values
        (fiscal_year, dor_code, municipality,
         res_av, open_space_av, commercial_av, industrial_av,
         personal_property_av, total_av, exempt_av)
    VALUES
        (:fiscal_year, :dor_code, :municipality,
         :res_av, :open_space_av, :commercial_av, :industrial_av,
         :personal_property_av, :total_av, :exempt_av)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality        = EXCLUDED.municipality,
        res_av              = EXCLUDED.res_av,
        open_space_av       = EXCLUDED.open_space_av,
        commercial_av       = EXCLUDED.commercial_av,
        industrial_av       = EXCLUDED.industrial_av,
        personal_property_av= EXCLUDED.personal_property_av,
        total_av            = EXCLUDED.total_av,
        exempt_av           = EXCLUDED.exempt_av,
        loaded_at           = NOW()
""")


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(BASE_URL, timeout=15)   # warm session / cookies
    return s


def _fetch_one(session: requests.Session, dor_code: int, fiscal_year: int) -> dict | None:
    """Fetch LA-4 comparison data for one municipality + year. Returns extracted row or None."""
    code_str = f"{dor_code:03d}"
    try:
        r = session.get(
            DATA_URL,
            params={
                "JurisdictionCode":     code_str,
                "FiscalYear":           str(fiscal_year),
                "fiscalYear":           str(fiscal_year),
                "jurCode":              code_str,
                "SelectedJurisdictionCode": code_str,
            },
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"    WARNING: HTTP error for code={code_str} FY={fiscal_year}: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Data lives in hidden inputs: LA4ComparisonReportData.LA4DataList[N].FieldName
    # Extract all of them into a list of dicts indexed by list position
    rows: dict[int, dict] = {}
    for inp in soup.find_all("input", type="hidden"):
        name = inp.get("name", "")
        m = re.match(r"LA4ComparisonReportData\.LA4DataList\[(\d+)\]\.(\w+)", name)
        if m:
            idx, field = int(m.group(1)), m.group(2)
            rows.setdefault(idx, {})[field] = inp.get("value", "")

    if not rows:
        return None   # no data for this municipality/year

    # Extract municipality name from the pre-selected dropdown option
    muni_name = None
    sel = soup.find("select", {"id": "ddlJurisdiction"})
    if sel:
        # Try selected attribute first; fall back to the option matching code_str
        opt = sel.find("option", selected=True)
        if not opt:
            opt = sel.find("option", value=str(dor_code))
        if opt:
            muni_name = re.sub(r"\s*-\s*\d+$", "", opt.get_text(strip=True))

    # Map row descriptions to class totals by looking at visible td text
    tables = soup.find_all("table")
    if len(tables) < 2:
        return None
    trows = tables[1].find_all("tr")[1:]   # skip header

    # Find total rows by matching text cell in each row against known labels
    CLASS_LABELS = {
        "TOTAL RESIDENTIAL":      "res_av",
        "TOTAL OPEN SPACE":       "open_space_av",
        "TOTAL COMMERCIAL":       "commercial_av",
        "TOTAL INDUSTRIAL":       "industrial_av",
        "TOTAL PERSONAL PROPERTY":"personal_property_av",
        "TOTAL REAL & PERSONAL":  "total_av",
        "EXEMPT PROPERTY":        "exempt_av",
    }

    result: dict[str, int] = {v: 0 for v in CLASS_LABELS.values()}

    for i, row in enumerate(trows):
        cells = [td.get_text(strip=True).upper() for td in row.find_all("td")]
        desc = cells[1] if len(cells) > 1 else ""
        field = CLASS_LABELS.get(desc)
        if field and i in rows:
            val_str = rows[i].get("AssessedValueCurrentYear", "0")
            try:
                result[field] = int(val_str) if val_str else 0
            except ValueError:
                result[field] = 0

    # Require at least one non-zero class to consider data valid
    if all(v == 0 for v in result.values()):
        return None

    return {
        "fiscal_year":          fiscal_year,
        "dor_code":             dor_code,
        "municipality":         muni_name,
        **result,
    }


def _get_municipalities(engine) -> list[tuple[int, str]]:
    """Return list of (dor_code, municipality) from the LA-4 dropdown."""
    session = requests.Session()
    session.headers.update(HEADERS)
    r = session.get(BASE_URL, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    sel = soup.find("select", {"id": "ddlJurisdiction"})
    if not sel:
        # Fallback: use municipalities already in our database
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT dor_code, municipality FROM municipal_tax_rates ORDER BY dor_code"
            )).fetchall()
        return [(r.dor_code, r.municipality) for r in rows]

    result = []
    for opt in sel.find_all("option"):
        val = opt.get("value", "")
        if val and val != "-1":
            try:
                code = int(val)
                name = re.sub(r"\s*-\s*\d+$", "", opt.get_text(strip=True))
                result.append((code, name))
            except ValueError:
                pass
    return result


def run(engine, years: list[int], dor_codes: list[int] | None = None) -> int:
    munis = _get_municipalities(engine)
    if dor_codes:
        munis = [(c, n) for c, n in munis if c in set(dor_codes)]

    total = 0
    session = _get_session()

    for year in years:
        year_count = 0
        print(f"[assessed_values] FY{year}: fetching {len(munis)} municipalities...")
        for i, (code, name) in enumerate(munis):
            row = _fetch_one(session, code, year)
            if row:
                with engine.begin() as conn:
                    conn.execute(UPSERT, row)
                year_count += 1

            # Brief pause every 10 requests to be polite
            if (i + 1) % 10 == 0:
                time.sleep(0.5)

            # Refresh session periodically
            if (i + 1) % 50 == 0:
                session = _get_session()

        print(f"[assessed_values]   FY{year}: {year_count} rows upserted")
        total += year_count

    if total:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_assessed_values', NULL, :n, 'ok')
            """), {"n": total})

    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",       type=int, help="Single fiscal year to fetch")
    parser.add_argument("--all",        action="store_true", help=f"Fetch all years from {DEFAULT_START_YEAR}")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--muni",       type=int, help="Single DOR municipality code (e.g. 262 for Saugus)")
    args = parser.parse_args()

    import datetime
    current_year = datetime.date.today().year

    if args.year:
        years = [args.year]
    elif args.all:
        years = list(range(args.start_year, current_year + 1))
    else:
        years = [current_year, current_year - 1]

    dor_codes = [args.muni] if args.muni else None

    engine = get_engine()
    n = run(engine, years, dor_codes=dor_codes)
    print(f"[assessed_values] Done — {n} total rows upserted.")
