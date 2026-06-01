"""
Ingest MA agency-level crime data from FBI UCR "Table 8" Excel files (2010–2024).

The FBI Crime Data Explorer API offense endpoints are broken/deprecated (503 errors).
Instead, this scraper downloads the annual "Offenses Known to Law Enforcement by City"
Excel files that the FBI publishes with each Crime in the United States report.

Coverage per year
-----------------
2010      MA-specific HTML-as-XLS  (confirmed 200)
2011      all-states binary XLS    (MA section embedded)
2012      MA-specific HTML-as-XLS
2013      MA-specific HTML-as-XLS
2014      all-states binary XLS    (MA section embedded)
2015      MA-specific HTML-as-XLS
2016      SKIPPED (only MSA-level file available)
2017–2019 MA-specific HTML-as-XLS
2020–2024 Beyond 2020 portal (existing ma_crime.py scraper)

Columns added
-------------
robbery, burglary, larceny, motor_vehicle_theft, arson
(plus fills in pre-2020 rows that ma_crime.py never created)

Run
---
    python scrapers/fbi_crime.py                   # 2010-2019, all agencies
    python scrapers/fbi_crime.py --year 2015       # single year
    python scrapers/fbi_crime.py --dry-run         # preview without DB writes
    python scrapers/fbi_crime.py --show-unmatched  # list city names with no ORI match

Requires
--------
    pip install xlrd lxml
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import re
import io
import time
import argparse
import requests
import xlrd
import pandas as pd
from sqlalchemy import text
from config import get_engine

# ── URL map: year → (url, file_type) ─────────────────────────────────────────
# file_type: "html" = HTML masquerading as XLS (MA-specific, parse with pd.read_html)
#            "xls"  = real binary XLS with all states (find MA section)
#            None   = skip
UCR_URLS = {
    2010: ("https://ucr.fbi.gov/crime-in-the-u.s/2010/crime-in-the-u.s.-2010"
           "/tables/table-8/10tbl08ma.xls", "html"),
    2011: ("https://ucr.fbi.gov/crime-in-the-u.s/2011/crime-in-the-u.s.-2011"
           "/tables/table_8_offenses_known_to_law_enforcement_by_state_by_city_2011.xls", "xls"),
    2012: ("https://ucr.fbi.gov/crime-in-the-u.s/2012/crime-in-the-u.s.-2012"
           "/tables/8tabledatadecpdf/table-8-state-cuts"
           "/table_8_offenses_known_to_law_enforcement_by_massachuetts_by_city_2012.xls", "html"),
    2013: ("https://ucr.fbi.gov/crime-in-the-u.s/2013/crime-in-the-u.s.-2013"
           "/tables/table-8/table-8-state-cuts"
           "/table_8_offenses_known_to_law_enforcement_massachusetts_by_city_2013.xls", "html"),
    2014: ("https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014"
           "/tables/table-8"
           "/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2014.xls", "xls"),
    2015: ("https://ucr.fbi.gov/crime-in-the-u.s/2015/crime-in-the-u.s.-2015"
           "/tables/table-8/table-8-state-pieces"
           "/table_8_offenses_known_to_law_enforcement_massachusetts_by_city_2015.xls", "html"),
    2016: (None, None),  # only MSA-level file exists — skip
    2017: ("https://ucr.fbi.gov/crime-in-the-u.s/2017/crime-in-the-u.s.-2017"
           "/tables/table-8/table-8-state-cuts/massachusetts.xls", "html"),
    2018: ("https://ucr.fbi.gov/crime-in-the-u.s/2018/crime-in-the-u.s.-2018"
           "/tables/table-8/table-8-state-cuts/massachusetts.xls", "html"),
    2019: ("https://ucr.fbi.gov/crime-in-the-u.s/2019/crime-in-the-u.s.-2019"
           "/tables/table-8/table-8-state-cuts/massachusetts.xls", "html"),
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
SLEEP   = 0.5

# ── Column name normalisers ───────────────────────────────────────────────────
# The FBI uses slightly different column labels across years. Map all variants
# to our standard column names.
COL_MAP = {
    # city / population
    r"city":                           "city",
    r"population":                     "population",
    # violent
    r"violent":                        "violent_total",
    r"murder|nonnegligent":            "murder",
    r"forcible.?rape|rape\d*|rape":    "rape",
    r"robbery":                        "robbery",
    r"aggravated":                     "aggravated_assault",
    # property
    r"property":                       "property_total",
    r"burglary":                       "burglary",
    r"larceny":                        "larceny",
    r"motor.?vehicle":                 "motor_vehicle_theft",
    r"arson":                          "arson",
}


def _norm_col(raw: str) -> str | None:
    """Map a raw FBI column header to our standard name, or None to drop it."""
    s = raw.lower().replace("\n", " ").strip()
    for pattern, name in COL_MAP.items():
        if re.search(pattern, s):
            return name
    return None


# FBI UCR uses abbreviated "boro" spellings; our DB uses full "borough"
_SLUG_ALIASES: dict[str, str] = {
    "middleboro":      "middleborough",
    "north-attleboro": "north-attleborough",
    "tyngsboro":       "tyngsborough",
}


def _slug(name: str) -> str:
    """Normalise a city name to match our jurisdiction_slug format."""
    s = name.lower().strip()
    # Strip trailing footnote markers like "3,4" or "12"
    s = re.sub(r"[\d,]+$", "", s).strip()
    s = re.sub(r"\s*(township|twp)\.?$", "", s)
    # Convert hyphens to spaces so "Manchester-by-the-Sea" → "manchester-by-the-sea"
    s = s.replace("-", " ")
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    slug = s
    return _SLUG_ALIASES.get(slug, slug)


def _safe_int(v) -> int | None:
    try:
        f = float(v)
        return int(f) if not pd.isna(f) else None
    except (TypeError, ValueError):
        return None


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_html_file(content: bytes, year: int) -> pd.DataFrame:
    """Parse an HTML-as-XLS MA-specific file. Returns DataFrame with standard cols."""
    tables = pd.read_html(io.StringIO(content.decode("utf-8", errors="replace")),
                          flavor="lxml")
    if not tables:
        raise ValueError("No tables found in HTML file")
    df = tables[0]

    # Rename columns
    rename = {}
    first_col_remapped = False
    for col in df.columns:
        norm = _norm_col(str(col))
        if norm:
            rename[col] = norm
        elif not first_col_remapped:
            # Some years label the city column "State" (MA-specific file still
            # contains city names there, not state names).
            col_lower = str(col).lower().strip()
            if col_lower in ("state", "city"):
                rename[col] = "city"
                first_col_remapped = True

    df = df.rename(columns=rename)
    df = df[[c for c in df.columns if c in set(COL_MAP.values()) | {"city"}]]
    df["year"] = year
    return df


def _parse_xls_file(content: bytes, year: int) -> pd.DataFrame:
    """Parse a real binary XLS all-states file. Returns MA rows only."""
    wb  = xlrd.open_workbook(file_contents=content)
    ws  = wb.sheets()[0]

    # Find header row (has 'City' or 'city' in one cell)
    header_row = None
    for i in range(min(10, ws.nrows)):
        row_vals = [str(ws.cell_value(i, c)).lower() for c in range(ws.ncols)]
        if "city" in row_vals:
            header_row = i
            break
    if header_row is None:
        raise ValueError("Could not find header row")

    raw_headers = [str(ws.cell_value(header_row, c)) for c in range(ws.ncols)]
    norm_headers = [_norm_col(h) for h in raw_headers]

    # Find Massachusetts section
    ma_start = ma_end = None
    for i in range(header_row + 1, ws.nrows):
        state_val = str(ws.cell_value(i, 0)).strip().upper()
        if "MASSACHUSETTS" in state_val and ma_start is None:
            ma_start = i
        elif ma_start is not None and state_val and state_val != "" and i > ma_start:
            # First non-empty state col after MA = next state
            ma_end = i
            break

    if ma_start is None:
        raise ValueError("Massachusetts section not found")
    if ma_end is None:
        ma_end = ws.nrows

    rows = []
    for i in range(ma_start, ma_end):
        row = {}
        for c, name in enumerate(norm_headers):
            if name:
                row[name] = ws.cell_value(i, c)
        # city col 1 in all-states format
        city_val = str(ws.cell_value(i, 1)).strip()
        if city_val and city_val != "nan":
            row["city"] = city_val
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df[[c for c in df.columns if c in set(COL_MAP.values()) | {"city"}]]
    df["year"] = year
    return df


def download_and_parse(session: requests.Session, year: int) -> pd.DataFrame | None:
    url, ftype = UCR_URLS.get(year, (None, None))
    if url is None:
        print(f"  {year}: skipped (no city-level file available)")
        return None

    r = session.get(url, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        print(f"  {year}: HTTP {r.status_code} — skipped")
        return None

    try:
        if ftype == "html":
            df = _parse_html_file(r.content, year)
        else:
            df = _parse_xls_file(r.content, year)
    except Exception as e:
        print(f"  {year}: parse error — {e}")
        return None

    # Drop rows with no city name
    if "city" in df.columns:
        df = df[df["city"].notna() & (df["city"].astype(str).str.strip() != "")]
    return df


# ── ORI lookup ────────────────────────────────────────────────────────────────

def load_ori_map(engine) -> dict[str, tuple[str, str]]:
    """Return {slug: (ori_code, jurisdiction_name)} from existing municipal_crime rows."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT jurisdiction_slug, ori_code, jurisdiction_name "
            "FROM municipal_crime WHERE ori_code IS NOT NULL"
        )).fetchall()
    return {r.jurisdiction_slug: (r.ori_code, r.jurisdiction_name) for r in rows}


# ── Upsert ────────────────────────────────────────────────────────────────────

UPSERT = text("""
    INSERT INTO municipal_crime
        (jurisdiction_slug, ori_code, jurisdiction_name, year,
         homicides, sexual_assaults, robbery, aggravated_assaults,
         burglary, larceny, motor_vehicle_theft, arson,
         violent_crimes, total_crimes, population,
         data_source)
    VALUES
        (:slug, :ori, :name, :year,
         :homicides, :sexual_assaults, :robbery, :aggravated_assaults,
         :burglary, :larceny, :motor_vehicle_theft, :arson,
         :violent_crimes, :total_crimes, :population,
         'fbi_ucr')
    ON CONFLICT (ori_code, year) DO UPDATE SET
        homicides            = COALESCE(EXCLUDED.homicides,
                                        municipal_crime.homicides),
        sexual_assaults      = COALESCE(EXCLUDED.sexual_assaults,
                                        municipal_crime.sexual_assaults),
        robbery              = COALESCE(EXCLUDED.robbery,
                                        municipal_crime.robbery),
        aggravated_assaults  = COALESCE(EXCLUDED.aggravated_assaults,
                                        municipal_crime.aggravated_assaults),
        burglary             = COALESCE(EXCLUDED.burglary,
                                        municipal_crime.burglary),
        larceny              = COALESCE(EXCLUDED.larceny,
                                        municipal_crime.larceny),
        motor_vehicle_theft  = COALESCE(EXCLUDED.motor_vehicle_theft,
                                        municipal_crime.motor_vehicle_theft),
        arson                = COALESCE(EXCLUDED.arson,
                                        municipal_crime.arson),
        violent_crimes       = COALESCE(EXCLUDED.violent_crimes,
                                        municipal_crime.violent_crimes),
        total_crimes         = COALESCE(EXCLUDED.total_crimes,
                                        municipal_crime.total_crimes),
        population           = COALESCE(EXCLUDED.population,
                                        municipal_crime.population),
        data_source          = CASE
                                 WHEN municipal_crime.data_source = 'beyond2020'
                                 THEN 'both'
                                 ELSE 'fbi_ucr'
                               END,
        loaded_at            = NOW()
""")


def process_year(df: pd.DataFrame, year: int, ori_map: dict,
                 engine, dry_run: bool, show_unmatched: bool) -> tuple[int, int]:
    """Match rows to ORI codes and upsert. Returns (matched, unmatched) counts."""
    matched = unmatched = 0
    rows_to_insert = []

    for _, row in df.iterrows():
        city_raw = str(row.get("city", "")).strip()
        slug = _slug(city_raw)
        ori_info = ori_map.get(slug)

        if ori_info is None:
            unmatched += 1
            if show_unmatched:
                print(f"    NO MATCH: {city_raw!r} → slug={slug!r}")
            continue

        ori_code, jur_name = ori_info
        matched += 1

        # Build upsert row (map FBI names → DB column names)
        murder   = _safe_int(row.get("murder"))
        rape     = _safe_int(row.get("rape"))
        robbery  = _safe_int(row.get("robbery"))
        agg_ass  = _safe_int(row.get("aggravated_assault"))
        burglary = _safe_int(row.get("burglary"))
        larceny  = _safe_int(row.get("larceny"))
        mvt      = _safe_int(row.get("motor_vehicle_theft"))
        # DB uses homicides/sexual_assaults/aggravated_assaults
        arson    = _safe_int(row.get("arson"))
        pop      = _safe_int(row.get("population"))

        vt = row.get("violent_total")
        violent = _safe_int(vt) if vt else (
            (murder or 0) + (rape or 0) + (robbery or 0) + (agg_ass or 0)
            if any(x is not None for x in [murder, rape, robbery, agg_ass]) else None
        )

        pt = row.get("property_total")
        prop = _safe_int(pt) if pt else (
            (burglary or 0) + (larceny or 0) + (mvt or 0) + (arson or 0)
            if any(x is not None for x in [burglary, larceny, mvt, arson]) else None
        )

        total = (violent or 0) + (prop or 0) if (violent or prop) else None

        rows_to_insert.append({
            "slug":                 slug,
            "ori":                  ori_code,
            "name":                 jur_name,
            "year":                 year,
            "homicides":            murder,
            "sexual_assaults":      rape,
            "robbery":              robbery,
            "aggravated_assaults":  agg_ass,
            "burglary":             burglary,
            "larceny":              larceny,
            "motor_vehicle_theft":  mvt,
            "arson":                arson,
            "violent_crimes":       violent,
            "total_crimes":         total,
            "population":           pop,
        })

    if rows_to_insert and not dry_run:
        with engine.begin() as conn:
            conn.execute(UPSERT, rows_to_insert)

    return matched, unmatched


# ── Main ──────────────────────────────────────────────────────────────────────

def run(engine, years: list[int] | None = None,
        dry_run: bool = False, show_unmatched: bool = False) -> int:

    if years is None:
        years = sorted(UCR_URLS.keys())

    ori_map = load_ori_map(engine)
    print(f"[fbi_crime] ORI map loaded: {len(ori_map)} known agencies")

    session = requests.Session()
    total_inserted = 0

    for year in years:
        print(f"[fbi_crime] {year}...", end="", flush=True)
        time.sleep(SLEEP)

        df = download_and_parse(session, year)
        if df is None or df.empty:
            continue

        print(f" {len(df)} rows parsed", end="", flush=True)

        matched, unmatched = process_year(
            df, year, ori_map, engine, dry_run, show_unmatched)
        total_inserted += matched
        print(f" → {matched} matched, {unmatched} unmatched")

    if total_inserted and not dry_run:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('fbi_crime', NULL, :n, 'ok')"
            ), {"n": total_inserted})

    return total_inserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest FBI UCR Table 8 crime data for MA agencies (2010–2019)")
    parser.add_argument("--year", type=int,
                        help="Single year to load (default: all 2010–2019)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and match without writing to the database")
    parser.add_argument("--show-unmatched", action="store_true",
                        help="Print city names that couldn't be matched to an ORI")
    args = parser.parse_args()

    years = [args.year] if args.year else None
    eng   = get_engine()  # always needed for ORI map lookup

    n = run(eng, years=years, dry_run=args.dry_run,
            show_unmatched=args.show_unmatched)
    print(f"\n[fbi_crime] Done — {n} rows upserted.")
