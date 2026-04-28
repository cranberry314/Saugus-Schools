"""
Loads pre-FY2023 Chapter 70 aid data from DESE keyfactors.xlsx.

Source: https://www.doe.mass.edu/finance/chapter70/keyfactors.xlsx
Sheet:  dataAid
Available: FY2007–FY2022 (FY2023+ loaded separately by scrapers/chapter70.py)

The keyfactors workbook is a district-facing interactive tool maintained by DESE
with historical Ch70 aid trend data for all MA districts. The 'dataAid' sheet
contains one row per district per fiscal year.

Key columns used:
  LEANumCode     → lea_code
  Org8Code       → org_code (8-digit DESE format, e.g. '02620000')
  DistName       → district_name
  fy             → fiscal_year
  distfoundenro  → foundation_enrollment
  distfoundbudget→ foundation_budget
  distrlc        → required_contribution
  c70aid         → chapter70_aid
  rqdnss         → required_nss

Loads into the same district_chapter70 table used by chapter70.py so all
queries work uniformly regardless of data vintage.

Run: python scrapers/chapter70_historical.py [--start-year 2007] [--end-year 2022]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import argparse
import requests
import pandas as pd
from sqlalchemy import text
from config import get_engine

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
KEYFACTORS_URL = "https://www.doe.mass.edu/finance/chapter70/keyfactors.xlsx"
SHEET_NAME     = "dataAid"

# FY2023+ is handled by chapter70.py (newer URL format)
DEFAULT_START = 2007
DEFAULT_END   = 2022

# Uses same table as chapter70.py — same UPSERT, same schema
UPSERT = text("""
    INSERT INTO district_chapter70
        (fiscal_year, lea_code, district_name, foundation_enrollment,
         foundation_budget, required_contribution, chapter70_aid,
         required_nss, chapter70_aid_per_pupil, required_nss_per_pupil)
    VALUES
        (:fiscal_year, :lea_code, :district_name, :foundation_enrollment,
         :foundation_budget, :required_contribution, :chapter70_aid,
         :required_nss, :chapter70_aid_per_pupil, :required_nss_per_pupil)
    ON CONFLICT (fiscal_year, lea_code) DO UPDATE SET
        district_name           = EXCLUDED.district_name,
        foundation_enrollment   = EXCLUDED.foundation_enrollment,
        foundation_budget       = EXCLUDED.foundation_budget,
        required_contribution   = EXCLUDED.required_contribution,
        chapter70_aid           = EXCLUDED.chapter70_aid,
        required_nss            = EXCLUDED.required_nss,
        chapter70_aid_per_pupil = EXCLUDED.chapter70_aid_per_pupil,
        required_nss_per_pupil  = EXCLUDED.required_nss_per_pupil,
        loaded_at               = NOW()
""")


def _to_int(v) -> int | None:
    try:
        f = float(str(v).strip().replace(",", ""))
        return int(round(f)) if not pd.isna(f) else None
    except Exception:
        return None


def _to_float(v) -> float | None:
    try:
        f = float(str(v).strip().replace(",", ""))
        return None if pd.isna(f) else f
    except Exception:
        return None


def _download_data() -> pd.DataFrame:
    """Download keyfactors.xlsx and return the dataAid sheet as a clean DataFrame."""
    print(f"[ch70_hist] Downloading {KEYFACTORS_URL} ...")
    r = requests.get(KEYFACTORS_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    print(f"[ch70_hist] Downloaded {len(r.content):,} bytes")

    xl = pd.ExcelFile(io.BytesIO(r.content))
    # Row 2 (index 2) is the actual header; rows 0-1 are title/index junk
    raw = xl.parse(SHEET_NAME, header=None)

    # Identify the header row: it contains 'Org8Codefy' in column 0
    header_row = None
    for i, row in raw.iterrows():
        if str(row.iloc[0]).strip() == "Org8Codefy":
            header_row = i
            break
    if header_row is None:
        raise ValueError("Could not locate header row in dataAid sheet")

    df = raw.iloc[header_row + 1:].copy()
    df.columns = raw.iloc[header_row].tolist()
    df = df.reset_index(drop=True)

    # Keep only the left-hand data block (columns before the second Org8Codefy block)
    # The sheet has a duplicate block on the right for SFSF/EdJobs data
    first_dup = None
    cols = list(df.columns)
    seen = set()
    for i, c in enumerate(cols):
        if c in seen:
            first_dup = i
            break
        seen.add(c)
    if first_dup:
        df = df.iloc[:, :first_dup]

    return df


def run(start_year: int = DEFAULT_START, end_year: int = DEFAULT_END):
    engine = get_engine()

    df = _download_data()
    print(f"[ch70_hist] Raw rows: {len(df):,}")

    # Normalise column names (lower, strip)
    col_map = {str(c).strip(): c for c in df.columns}
    def _col(name):
        return col_map.get(name)

    lea_col    = _col("LEANumCode")
    org_col    = _col("Org8Code")
    name_col   = _col("DistName")
    fy_col     = _col("fy")
    enroll_col = _col("distfoundenro")
    budget_col = _col("distfoundbudget")
    contrib_col= _col("distrlc")
    aid_col    = _col("c70aid")
    nss_col    = _col("rqdnss")

    missing = [n for n, c in [
        ("LEANumCode", lea_col), ("Org8Code", org_col), ("DistName", name_col),
        ("fy", fy_col), ("distfoundenro", enroll_col), ("c70aid", aid_col), ("rqdnss", nss_col)
    ] if c is None]
    if missing:
        raise ValueError(f"Missing columns in dataAid sheet: {missing}")

    total_loaded = 0

    for year in range(start_year, end_year + 1):
        year_df = df[df[fy_col].astype(str).str.strip() == str(year)]
        if year_df.empty:
            print(f"[ch70_hist] FY{year}: no rows found — skipping")
            continue

        records = []
        for _, row in year_df.iterrows():
            lea = _to_int(row[lea_col])
            if lea is None or lea == 999:   # 999 = State totals row
                continue

            enroll  = _to_int(row[enroll_col])
            aid     = _to_float(row[aid_col])
            nss     = _to_float(row[nss_col])
            aid_pp  = round(aid / enroll, 2) if aid and enroll and enroll > 0 else None
            nss_pp  = round(nss / enroll, 2) if nss and enroll and enroll > 0 else None

            # Org8Code is the 8-digit DESE org_code (e.g. '02620000')
            org = str(row[org_col]).strip().zfill(8) if row[org_col] else None

            records.append({
                "fiscal_year":            year,
                "lea_code":               lea,
                "district_name":          str(row[name_col] or "").strip() or None,
                "foundation_enrollment":  enroll,
                "foundation_budget":      _to_float(row[budget_col]) if budget_col else None,
                "required_contribution":  _to_float(row[contrib_col]) if contrib_col else None,
                "chapter70_aid":          aid,
                "required_nss":           nss,
                "chapter70_aid_per_pupil":aid_pp,
                "required_nss_per_pupil": nss_pp,
            })

        if records:
            with engine.begin() as conn:
                conn.execute(UPSERT, records)
                conn.execute(text("""
                    INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
                    VALUES ('chapter70_historical', :yr, :n, 'ok', 'keyfactors.xlsx dataAid')
                """), {"yr": year, "n": len(records)})

        print(f"[ch70_hist] FY{year}: {len(records)} districts loaded")
        total_loaded += len(records)

    print(f"[ch70_hist] Done. Total rows: {total_loaded:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load pre-FY2023 Chapter 70 aid data from DESE keyfactors.xlsx")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START,
                        help=f"First fiscal year to load (default {DEFAULT_START})")
    parser.add_argument("--end-year",   type=int, default=DEFAULT_END,
                        help=f"Last fiscal year to load (default {DEFAULT_END})")
    args = parser.parse_args()
    run(start_year=args.start_year, end_year=args.end_year)
