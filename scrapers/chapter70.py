"""
Loads DESE Chapter 70 state aid data for MA school districts.

Source: https://www.doe.mass.edu/finance/chapter70/fy{YYYY}/chapter-{YYYY}-local.xlsx
Available: FY2023–present via current DESE URL pattern.

Columns loaded:
  LEA code, district name, foundation enrollment, foundation budget,
  required contribution, Chapter 70 aid, required net school spending
  + derived: Chapter 70 aid per pupil, required NSS per pupil

Run: python scrapers/chapter70.py [--year 2024] [--all]
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

# FY → URL (pattern confirmed for FY2023–2026; extend as DESE publishes more)
URL_TEMPLATE = ("https://www.doe.mass.edu/finance/chapter70/"
                "fy{year}/chapter-{year}-local.xlsx")

# Earliest year available via current DESE URL pattern
FIRST_YEAR = 2023
# Most recent confirmed year (update when DESE publishes new files)
LAST_YEAR  = 2026


def _to_int(s) -> int | None:
    try:
        return int(str(s or "").strip().replace(",", "").split(".")[0])
    except Exception:
        return None


def _to_float(s) -> float | None:
    s = str(s or "").strip().replace(",", "").replace("$", "")
    if not s or s in ("-", "–", "N/A", "nan"):
        return None
    try:
        return float(s)
    except Exception:
        return None


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


def _load_year(engine, year: int) -> int:
    url = URL_TEMPLATE.format(year=year)
    print(f"[chapter70] Downloading FY{year}: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[chapter70] ERROR FY{year}: {e}")
        return 0

    print(f"[chapter70]   Downloaded {len(r.content):,} bytes")

    # Header is at row 5 (0-indexed), data starts at row 6
    try:
        df = pd.read_excel(io.BytesIO(r.content), header=5, dtype=str)
    except Exception as e:
        print(f"[chapter70] ERROR parsing FY{year}: {e}")
        return 0

    df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
    print(f"[chapter70]   Columns: {df.columns.tolist()}")
    print(f"[chapter70]   Raw rows: {len(df)}")

    # Map flexible column names
    col = {c.lower(): c for c in df.columns}
    lea_col    = next((col[k] for k in col if "lea" in k), df.columns[0])
    dist_col   = next((col[k] for k in col if "district" in k), df.columns[1])
    enroll_col = next((col[k] for k in col if "foundation enrollment" in k), None)
    budget_col = next((col[k] for k in col if "foundation budget" in k), None)
    contrib_col= next((col[k] for k in col if "required contribution" in k), None)
    aid_col    = next((col[k] for k in col if "chapter 70" in k and "aid" in k), None)
    nss_col    = next((col[k] for k in col if "net school spending" in k), None)

    records = []
    skipped = 0
    for _, row in df.iterrows():
        lea = _to_int(row.get(lea_col))
        if lea is None:
            skipped += 1
            continue

        enroll = _to_int(row.get(enroll_col)) if enroll_col else None
        aid    = _to_float(row.get(aid_col))  if aid_col    else None
        nss    = _to_float(row.get(nss_col))  if nss_col    else None

        aid_pp = round(aid / enroll, 2) if aid and enroll and enroll > 0 else None
        nss_pp = round(nss / enroll, 2) if nss and enroll and enroll > 0 else None

        records.append({
            "fiscal_year":            year,
            "lea_code":               lea,
            "district_name":          str(row.get(dist_col, "") or "").strip() or None,
            "foundation_enrollment":  enroll,
            "foundation_budget":      _to_float(row.get(budget_col)) if budget_col else None,
            "required_contribution":  _to_float(row.get(contrib_col)) if contrib_col else None,
            "chapter70_aid":          aid,
            "required_nss":           nss,
            "chapter70_aid_per_pupil":aid_pp,
            "required_nss_per_pupil": nss_pp,
        })

    print(f"[chapter70]   Parsed {len(records)} districts ({skipped} skipped/blank rows)")

    if records:
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
                VALUES ('chapter70', :yr, :n, 'ok', :url)
            """), {"yr": year, "n": len(records), "url": url})
        print(f"[chapter70]   ✓ FY{year}: {len(records)} districts inserted/updated")

    return len(records)


def run(target_year: int | None = None, load_all: bool = False):
    engine = get_engine()
    if target_year:
        years = [target_year]
    elif load_all:
        years = list(range(FIRST_YEAR, LAST_YEAR + 1))
    else:
        years = [LAST_YEAR]

    total = 0
    for y in years:
        total += _load_year(engine, y)
    print(f"[chapter70] Done. Total rows loaded: {total:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load DESE Chapter 70 aid data")
    parser.add_argument("--year", type=int, help="Fiscal year (e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all available years")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all)
