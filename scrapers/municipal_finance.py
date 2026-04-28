"""
Scrapes MA town-by-town financial data from the MA DLS (Division of Local Services) Gateway.

Source: https://dls-gw.dor.state.ma.us/reports/rdPage.aspx

Reports pulled:
  1. DOR Income & EQV Per Capita     (rdReport=DOR_Income_EQV_Per_Capita)
       — looped per year; available FY2007–present (NOT available for 2016, 2019, 2020, 2021)
       → municipal_income_eqv

  1b. EQV Biennial                   (rdReport=PropertyTaxInformation.EQV.EQV)
       — wide-format; even years only (biennial); fills 2016 & 2020 missing from report 1
       → municipal_income_eqv (eqv/eqv_per_capita columns only; income columns left NULL)

  2. Tax Rates by Class               (rdReport=propertytaxinformation.taxratesbyclass.taxratesbyclass)
       — single download; returns all available years
       → municipal_tax_rates

  3. GF Expenditures Per Capita       (rdReport=351GenFunperCapita)
       — single download; returns all available years
       → municipal_gf_expenditures

  4. New Growth                        (rdReport=newgrowth.newgrowth_dash_v2_test)
       — single download; returns all available years
       → municipal_new_growth

Run: python scrapers/municipal_finance.py [--year 2024] [--all] [--start-year 2014]
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

BASE_URL = "https://dls-gw.dor.state.ma.us/reports/rdPage.aspx"
DEFAULT_START_YEAR = 2014

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://dls-gw.dor.state.ma.us/",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _download_excel(session: requests.Session, params: dict, post_data: dict,
                    label: str, retries: int = 3) -> bytes | None:
    """
    POSTs to the rdPage export endpoint and follows the redirect to download
    the generated XLSX file.  Retries on timeout.  Returns raw bytes or None.
    """
    url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    for attempt in range(1, retries + 1):
        try:
            r = session.post(url, data=post_data, timeout=120, allow_redirects=True)
            r.raise_for_status()
            break
        except requests.exceptions.Timeout:
            wait = attempt * 5
            print(f"[municipal_finance] Timeout on {label} (attempt {attempt}/{retries}), "
                  f"retrying in {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"[municipal_finance] ERROR downloading {label}: {e}")
            return None
    else:
        print(f"[municipal_finance] FAILED {label} after {retries} attempts")
        return None

    # Check we got binary Excel data (PK magic bytes) not an HTML error page
    if r.content[:2] != b"PK":
        print(f"[municipal_finance] WARNING: {label} response is not Excel "
              f"({len(r.content)} bytes, starts: {r.content[:40]})")
        return None

    return r.content


def _parse_excel(raw: bytes, label: str) -> pd.DataFrame:
    try:
        return pd.read_excel(io.BytesIO(raw), dtype=str)
    except Exception as e:
        print(f"[municipal_finance] ERROR parsing {label}: {e}")
        return pd.DataFrame()


def _to_int(s) -> int | None:
    try:
        return int(str(s).replace(",", "").strip())
    except Exception:
        return None


def _to_float(s) -> float | None:
    s = str(s or "").strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in ("-", "–", "N/A", "nan", ""):
        return None
    try:
        return float(s)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 1. DOR Income & EQV Per Capita  (one request per year)
# ---------------------------------------------------------------------------

EXPORT_PARAMS_EQV = {
    "rdReport":           "DOR_Income_EQV_Per_Capita",
    "rdReportFormat":     "NativeExcel",
    "rdExportTableID":    "xtblDOR_Income_EQV_Per_Capita",
    "rdExportFilename":   "DOR_Income_EQV_Per_Capita",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

UPSERT_EQV = text("""
    INSERT INTO municipal_income_eqv
        (fiscal_year, dor_code, lea_code, municipality, county,
         population, dor_income, income_per_capita, eqv, eqv_per_capita)
    VALUES
        (:fiscal_year, :dor_code, :lea_code, :municipality, :county,
         :population, :dor_income, :income_per_capita, :eqv, :eqv_per_capita)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        lea_code          = EXCLUDED.lea_code,
        municipality      = EXCLUDED.municipality,
        county            = EXCLUDED.county,
        population        = EXCLUDED.population,
        dor_income        = EXCLUDED.dor_income,
        income_per_capita = EXCLUDED.income_per_capita,
        eqv               = EXCLUDED.eqv,
        eqv_per_capita    = EXCLUDED.eqv_per_capita,
        loaded_at         = NOW()
""")


def _load_eqv_year(engine, year: int) -> int:
    """Uses a fresh session per year to avoid ASP.NET session expiry."""
    import time as _time
    t0 = _time.time()
    print(f"[municipal_finance]   Income/EQV FY{year} — fetching...")
    # Fresh session every call to avoid ASP.NET_SessionId expiry
    session = _get_session()
    raw = _download_excel(session, EXPORT_PARAMS_EQV, {"islYear": str(year)},
                          f"EQV FY{year}")
    if raw is None:
        return 0

    print(f"[municipal_finance]     Downloaded {len(raw):,} bytes "
          f"({_time.time()-t0:.1f}s)")

    df = _parse_excel(raw, f"EQV FY{year}")
    if df.empty:
        print(f"[municipal_finance]     WARNING: empty DataFrame for FY{year}")
        return 0

    df.columns = [c.strip() for c in df.columns]
    print(f"[municipal_finance]     Columns: {df.columns.tolist()}")

    records = []
    skipped = 0
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        if dor is None:
            skipped += 1
            continue
        records.append({
            "fiscal_year":      year,
            "dor_code":         dor,
            "lea_code":         _to_int(row.get("LEA Code")),
            "municipality":     str(row.get("Municipality") or "").strip() or None,
            "county":           str(row.get("County") or "").strip() or None,
            "population":       _to_int(row.get("Population")),
            "dor_income":       _to_float(row.get("DOR Income")),
            "income_per_capita":_to_float(row.get("DOR Income Per Capita")),
            "eqv":              _to_float(row.get("EQV")),
            "eqv_per_capita":   _to_float(row.get("EQV Per Capita")),
        })

    print(f"[municipal_finance]     Parsed {len(records)} towns "
          f"({skipped} skipped rows)")

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT_EQV, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_income_eqv', :yr, :n, 'ok')
            """), {"yr": year, "n": len(records)})
        print(f"[municipal_finance]     ✓ FY{year}: {len(records)} rows inserted/updated "
              f"({_time.time()-t0:.1f}s total)")

    return len(records)


# ---------------------------------------------------------------------------
# 1b. EQV Biennial report — even years only, wide format
#     Fills 2016 & 2020 that DOR_Income_EQV_Per_Capita cannot deliver.
# ---------------------------------------------------------------------------

EXPORT_PARAMS_EQV_BIENNIAL = {
    "rdReport":           "PropertyTaxInformation.EQV.EQV",
    "rdReportFormat":     "NativeExcel",
    # No rdExportTableID — omitting it returns the full dataset; a wrong ID returns empty
    "rdExportFilename":   "EQV",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

# Only even years are published (biennial); restrict to our analysis window
EQV_BIENNIAL_YEARS = [2014, 2016, 2018, 2020, 2022, 2024]

# Upsert that only fills NULL EQV columns — never overwrites income data with NULLs
UPSERT_EQV_BIENNIAL = text("""
    INSERT INTO municipal_income_eqv
        (fiscal_year, dor_code, municipality, county, eqv, eqv_per_capita)
    VALUES
        (:fiscal_year, :dor_code, :municipality, :county, :eqv, :eqv_per_capita)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality   = COALESCE(municipal_income_eqv.municipality, EXCLUDED.municipality),
        county         = COALESCE(municipal_income_eqv.county,       EXCLUDED.county),
        eqv            = COALESCE(municipal_income_eqv.eqv,          EXCLUDED.eqv),
        eqv_per_capita = COALESCE(municipal_income_eqv.eqv_per_capita, EXCLUDED.eqv_per_capita),
        loaded_at      = NOW()
""")


def _load_eqv_biennial(engine, min_year: int = 2014) -> int:
    """
    Download the biennial EQV report (wide format, one row per municipality,
    one column per year) and upsert EQV values into municipal_income_eqv.
    Uses COALESCE so income columns already loaded are never overwritten.
    """
    import time as _time
    years = [y for y in EQV_BIENNIAL_YEARS if y >= min_year]
    if not years:
        return 0

    t0 = _time.time()
    print(f"[municipal_finance]   EQV Biennial — fetching years {years}...")

    session = _get_session()
    # iclYear is a checkbox list — pass as list so requests repeats the param
    post_data = {"iclYear": [str(y) for y in years]}
    raw = _download_excel(session, EXPORT_PARAMS_EQV_BIENNIAL, post_data,
                          "EQV Biennial")
    if raw is None:
        return 0

    print(f"[municipal_finance]     Downloaded {len(raw):,} bytes "
          f"({_time.time()-t0:.1f}s)")

    # The export has ~19 rows of license/title junk before the real header.
    # Detect the header row by looking for "DOR Code" in column 0.
    raw_df = pd.read_excel(io.BytesIO(raw), header=None, dtype=str)
    header_row = None
    for i, row in raw_df.iterrows():
        if str(row.iloc[0]).strip() == "DOR Code":
            header_row = i
            break
    if header_row is None:
        print("[municipal_finance]     WARNING: could not find 'DOR Code' header row "
              "in EQV Biennial export")
        return 0

    df = pd.read_excel(io.BytesIO(raw), header=header_row, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"[municipal_finance]     EQV Biennial columns: {df.columns.tolist()}")

    # Identify year columns (4-digit integers >= min_year) and ID columns
    year_cols = [c for c in df.columns
                 if c.isdigit() and int(c) in EQV_BIENNIAL_YEARS and int(c) >= min_year]
    if not year_cols:
        print("[municipal_finance]     WARNING: no year columns found; "
              "check rdExportTableID or column names")
        return 0

    records = []
    skipped = 0
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        if dor is None:
            skipped += 1
            continue
        municipality = str(row.get("Municipality") or "").strip() or None
        county       = str(row.get("County") or "").strip() or None

        for yr_col in year_cols:
            yr  = int(yr_col)
            eqv = _to_float(row.get(yr_col))
            if eqv is None:
                continue
            records.append({
                "fiscal_year":   yr,
                "dor_code":      dor,
                "municipality":  municipality,
                "county":        county,
                "eqv":           eqv,
                "eqv_per_capita": None,   # not available in this report
            })

    print(f"[municipal_finance]     EQV Biennial: {len(records)} municipality-years "
          f"({skipped} skipped rows, {_time.time()-t0:.1f}s)")

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT_EQV_BIENNIAL, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_income_eqv_biennial', NULL, :n, 'ok')
            """), {"n": len(records)})
        print(f"[municipal_finance]     ✓ EQV Biennial: {len(records)} rows upserted")

    return len(records)


# ---------------------------------------------------------------------------
# 2. Tax Rates by Class  (single download, all available years)
# ---------------------------------------------------------------------------

EXPORT_PARAMS_TAX = {
    "rdReport":           "propertytaxinformation.taxratesbyclass.taxratesbyclass",
    "rdReportFormat":     "NativeExcel",
    "rdExportTableID":    "tbl_taxratesbyclass",
    "rdExportFilename":   "TaxRatesByClass",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

UPSERT_TAX = text("""
    INSERT INTO municipal_tax_rates
        (fiscal_year, dor_code, municipality,
         residential, open_space, commercial, industrial, personal_property)
    VALUES
        (:fiscal_year, :dor_code, :municipality,
         :residential, :open_space, :commercial, :industrial, :personal_property)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality      = EXCLUDED.municipality,
        residential       = EXCLUDED.residential,
        open_space        = EXCLUDED.open_space,
        commercial        = EXCLUDED.commercial,
        industrial        = EXCLUDED.industrial,
        personal_property = EXCLUDED.personal_property,
        loaded_at         = NOW()
""")


def _load_tax_rates(engine, session: requests.Session,
                    min_year: int, max_year: int | None) -> int:
    print("[municipal_finance]   Tax Rates by Class...")
    raw = _download_excel(session, EXPORT_PARAMS_TAX, {}, "Tax Rates")
    if raw is None:
        return 0

    df = _parse_excel(raw, "Tax Rates")
    if df.empty:
        return 0

    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        fy  = _to_int(row.get("Fiscal Year"))
        if dor is None or fy is None:
            continue
        if fy < min_year:
            continue
        if max_year and fy > max_year:
            continue
        records.append({
            "fiscal_year":      fy,
            "dor_code":         dor,
            "municipality":     str(row.get("Municipality") or "").strip() or None,
            "residential":      _to_float(row.get("Residential")),
            "open_space":       _to_float(row.get("Open Space")),
            "commercial":       _to_float(row.get("Commercial")),
            "industrial":       _to_float(row.get("Industrial")),
            "personal_property":_to_float(row.get("Personal Property")),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT_TAX, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_tax_rates', NULL, :n, 'ok')
            """), {"n": len(records)})

    print(f"[municipal_finance]   Tax Rates: {len(records)} rows "
          f"(years: {sorted(set(r['fiscal_year'] for r in records))})")
    return len(records)


# ---------------------------------------------------------------------------
# 3. General Fund Expenditures Per Capita
# ---------------------------------------------------------------------------

EXPORT_PARAMS_GF = {
    "rdReport":           "351GenFunperCapita",
    "rdReportFormat":     "NativeExcel",
    "rdExportTableID":    "tblGenFundPerCap",
    "rdExportFilename":   "GenFundPerCap",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

UPSERT_GF = text("""
    INSERT INTO municipal_gf_expenditures
        (fiscal_year, dor_code, municipality, population,
         total_gf_expenditure, gf_expenditure_per_capita)
    VALUES
        (:fiscal_year, :dor_code, :municipality, :population,
         :total_gf_expenditure, :gf_expenditure_per_capita)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality              = EXCLUDED.municipality,
        population                = EXCLUDED.population,
        total_gf_expenditure      = EXCLUDED.total_gf_expenditure,
        gf_expenditure_per_capita = EXCLUDED.gf_expenditure_per_capita,
        loaded_at                 = NOW()
""")


def _load_gf_expenditures(engine, session: requests.Session,
                           min_year: int, max_year: int | None) -> int:
    print("[municipal_finance]   GF Expenditures per Capita...")
    raw = _download_excel(session, EXPORT_PARAMS_GF, {}, "GF Expenditures")
    if raw is None:
        return 0

    df = _parse_excel(raw, "GF Expenditures")
    if df.empty:
        return 0

    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        fy  = _to_int(row.get("Fiscal Year"))
        if dor is None or fy is None:
            continue
        if fy < min_year:
            continue
        if max_year and fy > max_year:
            continue
        records.append({
            "fiscal_year":              fy,
            "dor_code":                 dor,
            "municipality":             str(row.get("Name") or "").strip() or None,
            "population":               _to_int(row.get("Population")),
            "total_gf_expenditure":     _to_float(row.get("Total General Fund Expediture")
                                                   or row.get("Total General Fund Expenditure")),
            "gf_expenditure_per_capita":_to_float(row.get("Total General Fund Expenditures per Capita")),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT_GF, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_gf_expenditures', NULL, :n, 'ok')
            """), {"n": len(records)})

    print(f"[municipal_finance]   GF Expenditures: {len(records)} rows "
          f"(years: {sorted(set(r['fiscal_year'] for r in records))})")
    return len(records)


# ---------------------------------------------------------------------------
# 4. New Growth
# ---------------------------------------------------------------------------

EXPORT_PARAMS_NG = {
    "rdReport":           "newgrowth.newgrowth_dash_v2_test",
    "rdReportFormat":     "NativeExcel",
    "rdExportTableID":    "tblNewGrowth",
    "rdExportFilename":   "NewGrowth",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

UPSERT_NG = text("""
    INSERT INTO municipal_new_growth
        (fiscal_year, dor_code, municipality,
         residential_new_growth_value, residential_new_growth_applied,
         total_new_growth_value, total_new_growth_applied,
         res_pct_of_total, prior_year_levy_limit, new_growth_pct_of_py_levy)
    VALUES
        (:fiscal_year, :dor_code, :municipality,
         :residential_new_growth_value, :residential_new_growth_applied,
         :total_new_growth_value, :total_new_growth_applied,
         :res_pct_of_total, :prior_year_levy_limit, :new_growth_pct_of_py_levy)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality                    = EXCLUDED.municipality,
        residential_new_growth_value    = EXCLUDED.residential_new_growth_value,
        residential_new_growth_applied  = EXCLUDED.residential_new_growth_applied,
        total_new_growth_value          = EXCLUDED.total_new_growth_value,
        total_new_growth_applied        = EXCLUDED.total_new_growth_applied,
        res_pct_of_total                = EXCLUDED.res_pct_of_total,
        prior_year_levy_limit           = EXCLUDED.prior_year_levy_limit,
        new_growth_pct_of_py_levy       = EXCLUDED.new_growth_pct_of_py_levy,
        loaded_at                       = NOW()
""")


def _load_new_growth(engine, session: requests.Session,
                     min_year: int, max_year: int | None) -> int:
    print("[municipal_finance]   New Growth...")
    raw = _download_excel(session, EXPORT_PARAMS_NG, {}, "New Growth")
    if raw is None:
        return 0

    df = _parse_excel(raw, "New Growth")
    if df.empty:
        return 0

    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        fy  = _to_int(row.get("Fiscal Year"))
        if dor is None or fy is None:
            continue
        if fy < min_year:
            continue
        if max_year and fy > max_year:
            continue
        records.append({
            "fiscal_year":                  fy,
            "dor_code":                     dor,
            "municipality":                 str(row.get("Municipality") or "").strip() or None,
            "residential_new_growth_value": _to_float(row.get("Residential New Growth Value")),
            "residential_new_growth_applied":_to_float(row.get("Residential New Growth Applied to the Levy Limit")),
            "total_new_growth_value":       _to_float(row.get("Total New Growth Value")),
            "total_new_growth_applied":     _to_float(row.get("Total New Growth Applied to Levy Limit")),
            "res_pct_of_total":             _to_float(row.get("Res New Growth as a % of Total New Growth")),
            "prior_year_levy_limit":        _to_float(row.get("Prior Year's Levy Limit")),
            "new_growth_pct_of_py_levy":    _to_float(row.get("Total New Growth Applied to Limit as a % of PY Levy Limit")),
        })

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT_NG, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_new_growth', NULL, :n, 'ok')
            """), {"n": len(records)})

    print(f"[municipal_finance]   New Growth: {len(records)} rows "
          f"(years: {sorted(set(r['fiscal_year'] for r in records))})")
    return len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Year options available for Income/EQV report (per DLS Gateway as of 2026)
EQV_YEARS_AVAILABLE = list(range(2007, 2028))


def run(target_year: int | None = None, load_all: bool = False,
        start_year: int = DEFAULT_START_YEAR):
    engine = get_engine()
    session = _get_session()

    # Determine year range for the EQV per-year loop
    if target_year:
        eqv_years = [target_year]
        min_year  = target_year
        max_year  = target_year
    elif load_all:
        eqv_years = [y for y in EQV_YEARS_AVAILABLE if y >= start_year]
        min_year  = start_year
        max_year  = None
    else:
        # Default: start_year through most recent available
        most_recent = max(EQV_YEARS_AVAILABLE)
        eqv_years = [y for y in EQV_YEARS_AVAILABLE if y >= start_year]
        min_year  = start_year
        max_year  = None

    print(f"[municipal_finance] Loading EQV/Income years: {eqv_years}")

    # 1. DOR Income & EQV — fresh session per year (avoids ASP.NET session expiry)
    total_eqv = 0
    for year in eqv_years:
        n = _load_eqv_year(engine, year)
        total_eqv += n
        time.sleep(1)   # be polite to the server

    # 1b. EQV Biennial — fills even years (2016, 2020) missing from report 1
    print("[municipal_finance]   EQV Biennial (supplemental for even-year gaps)...")
    biennial_min = min_year if not target_year else (target_year if target_year % 2 == 0 else target_year + 1)
    total_eqv += _load_eqv_biennial(engine, min_year=biennial_min)
    time.sleep(1)

    # 2–4. Bulk single-download reports — shared session is fine here
    session = _get_session()
    total_tax = _load_tax_rates(engine, session, min_year, max_year)
    time.sleep(0.5)
    total_gf  = _load_gf_expenditures(engine, session, min_year, max_year)
    time.sleep(0.5)
    total_ng  = _load_new_growth(engine, session, min_year, max_year)

    print(f"\n[municipal_finance] Done. "
          f"income_eqv={total_eqv:,} (incl. biennial)  tax_rates={total_tax:,}  "
          f"gf_expenditures={total_gf:,}  new_growth={total_ng:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MA municipal finance data from DLS Gateway")
    parser.add_argument("--year",       type=int, help="Single fiscal year to load (e.g. 2024)")
    parser.add_argument("--all",        action="store_true", help="Load all years from start-year onward")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR,
                        help=f"Earliest year to load (default {DEFAULT_START_YEAR})")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all, start_year=args.start_year)
