"""
Fetches MCAS Achievement Results from the MA Education-to-Career (E2C) Socrata dataset.
Dataset: i9w6-niyt  (Next Generation MCAS, 2017–present)
API:     https://educationtocareer.data.mass.gov/resource/i9w6-niyt.json

Run: python scrapers/mcas.py [--year 2024] [--all]

Without flags: loads the most recent available year.
--all: loads every year in the dataset.
--year YYYY: loads a specific school year.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import json
import requests
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm
from config import (
    get_engine, SOCRATA_BASE, SOCRATA_FALLBACK,
    MCAS_DATASET_ID, SOCRATA_PAGE_SIZE
)

HEADERS = {"Accept": "application/json"}

# Column mapping: Socrata field name → our DB column
# Confirmed from live dataset sample (dataset i9w6-niyt)
COL_MAP = {
    "sy":               "school_year_raw",
    "dist_code":        "dist_code_raw",
    "dist_name":        "district_name",
    "org_code":         "org_code",        # already full org code in dataset
    "org_name":         "school_name",
    "test_grade":       "grade",
    "subject_code":     "subject",
    "stu_grp":          "student_group",
    "stu_cnt":          "tested_count",
    "e_pct":            "e_pct",
    "m_pct":            "m_pct",
    "pm_pct":           "pm_pct",
    "nm_pct":           "nm_pct",
    "m_plus_e_pct":     "meeting_exceeding_pct",
    "avg_scaled_score": "mean_scaled_score",
}


def _api_url(base: str) -> str:
    return f"{base}/{MCAS_DATASET_ID}.json"


def _get(url: str, timeout: int = 60) -> requests.Response:
    """GET with literal $ in query string (requests encodes $ as %24 which breaks Socrata)."""
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r


def fetch_sample(base_url: str) -> list[dict]:
    """Fetches 2 rows to discover actual column names in the dataset."""
    url = f"{_api_url(base_url)}?$limit=2"
    r = _get(url, timeout=30)
    return r.json()


def fetch_years(base_url: str) -> list[str]:
    """Returns the distinct school year values available in the dataset."""
    sample = fetch_sample(base_url)
    if not sample:
        return []
    # Try common year column names including 'sy' (the actual name in this dataset)
    year_col = next(
        (k for k in sample[0] if k.lower() in ("sy", "schoolyear", "school_year", "year")),
        None
    )
    if not year_col:
        # Broader fallback: any column with 'year' in name
        year_col = next((k for k in sample[0] if "year" in k.lower()), None)
    if not year_col:
        print(f"[mcas] WARNING: could not find year column. Available: {list(sample[0].keys())}")
        return []

    url = f"{_api_url(base_url)}?$select={year_col}&$group={year_col}&$limit=100"
    r = _get(url, timeout=30)
    years = sorted({row[year_col] for row in r.json() if year_col in row})
    print(f"[mcas] Year column: '{year_col}', values: {years}")
    return years


def fetch_mcas_page(base_url: str, year_str: str, offset: int, year_col: str = "schoolyear") -> list[dict]:
    """Fetches one page of MCAS results for a given school year string."""
    # Build URL with literal $ to avoid %24 encoding
    url = (
        f"{_api_url(base_url)}"
        f"?$where={year_col}='{year_str}'"
        f"&$limit={SOCRATA_PAGE_SIZE}"
        f"&$offset={offset}"
        f"&$order={year_col}"
    )
    r = _get(url, timeout=60)
    return r.json()


def parse_year(year_str: str) -> int:
    """Converts '2023-2024' or '2024' to the ending integer year 2024."""
    if "-" in str(year_str):
        return int(str(year_str).split("-")[-1])
    return int(year_str)


def _to_float(val) -> float | None:
    if val is None or str(val).strip() in ("", "N/A", "n/a", ".", "–"):
        return None
    try:
        return float(str(val).replace("%", "").strip())
    except ValueError:
        return None


def _to_int(val) -> int | None:
    if val is None or str(val).strip() in ("", "N/A", ".", "–"):
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except ValueError:
        return None


def transform_row(raw: dict) -> dict:
    """Normalises a raw Socrata row into our DB schema."""
    mapped = {our_col: raw.get(socrata_col) for socrata_col, our_col in COL_MAP.items()}

    year_raw = mapped.pop("school_year_raw", None)
    mapped.pop("dist_code_raw", None)   # use org_code directly

    # org_code is already the full code in this dataset
    mapped["org_code"] = str(mapped.get("org_code") or "").strip()

    mapped["school_year"]            = parse_year(year_raw) if year_raw else None
    mapped["tested_count"]           = _to_int(mapped.get("tested_count"))
    mapped["e_pct"]                  = _to_float(mapped.get("e_pct"))
    mapped["m_pct"]                  = _to_float(mapped.get("m_pct"))
    mapped["pm_pct"]                 = _to_float(mapped.get("pm_pct"))
    mapped["nm_pct"]                 = _to_float(mapped.get("nm_pct"))
    mapped["meeting_exceeding_pct"]  = _to_float(mapped.get("meeting_exceeding_pct"))
    mapped["mean_scaled_score"]      = _to_float(mapped.get("mean_scaled_score"))
    mapped["avg_student_growth_pct"] = None   # not in this dataset
    mapped["raw_row"]                = json.dumps(raw)

    return mapped


UPSERT_SQL = text("""
    INSERT INTO mcas_results (
        school_year, org_code, district_name, school_name, grade, subject,
        student_group, tested_count, e_pct, m_pct, pm_pct, nm_pct,
        meeting_exceeding_pct, mean_scaled_score, avg_student_growth_pct, raw_row
    ) VALUES (
        :school_year, :org_code, :district_name, :school_name, :grade, :subject,
        :student_group, :tested_count, :e_pct, :m_pct, :pm_pct, :nm_pct,
        :meeting_exceeding_pct, :mean_scaled_score, :avg_student_growth_pct,
        CAST(:raw_row AS jsonb)
    )
    ON CONFLICT (school_year, org_code, grade, subject, student_group)
    DO UPDATE SET
        district_name           = EXCLUDED.district_name,
        school_name             = EXCLUDED.school_name,
        tested_count            = EXCLUDED.tested_count,
        e_pct                   = EXCLUDED.e_pct,
        m_pct                   = EXCLUDED.m_pct,
        pm_pct                  = EXCLUDED.pm_pct,
        nm_pct                  = EXCLUDED.nm_pct,
        meeting_exceeding_pct   = EXCLUDED.meeting_exceeding_pct,
        mean_scaled_score       = EXCLUDED.mean_scaled_score,
        avg_student_growth_pct  = EXCLUDED.avg_student_growth_pct,
        raw_row                 = EXCLUDED.raw_row,
        loaded_at               = NOW()
""")


def load_year(engine, base_url: str, year_str: str, year_col: str = "schoolyear"):
    """Loads all MCAS rows for one school year into the database."""
    school_year = parse_year(year_str)
    print(f"[mcas] Loading year {year_str} (→ {school_year})...")

    offset = 0
    total_loaded = 0
    with tqdm(desc=f"MCAS {year_str}", unit=" rows") as pbar:
        while True:
            rows = fetch_mcas_page(base_url, year_str, offset, year_col=year_col)
            if not rows:
                break
            records = [transform_row(r) for r in rows]
            with engine.begin() as conn:
                conn.execute(UPSERT_SQL, records)
            total_loaded += len(records)
            pbar.update(len(records))
            if len(rows) < SOCRATA_PAGE_SIZE:
                break
            offset += SOCRATA_PAGE_SIZE

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
            VALUES ('mcas', :yr, :n, 'ok', :url)
        """), {"yr": school_year, "n": total_loaded, "url": base_url})

    print(f"[mcas] Year {year_str}: loaded {total_loaded:,} rows.")
    return total_loaded


def _resolve_endpoint() -> tuple[str, list[str], str]:
    """Returns (base_url, available_years, year_col). Tries primary then fallback."""
    for base_url in (SOCRATA_BASE, SOCRATA_FALLBACK):
        try:
            sample = fetch_sample(base_url)
            if not sample:
                continue
            print(f"[mcas] Connected to {base_url}")
            print(f"[mcas] Dataset columns: {list(sample[0].keys())}")
            years = fetch_years(base_url)
            if years:
                # year_col detected inside fetch_years; re-detect here for return value
                year_col = next(
                    (k for k in sample[0] if k.lower() in ("sy", "schoolyear", "school_year", "year")),
                    next((k for k in sample[0] if "year" in k.lower()), "sy")
                )
                return base_url, years, year_col
        except Exception as e:
            print(f"[mcas] {base_url} failed: {e}")
    return SOCRATA_BASE, [], "sy"


def run(years_to_load: list[str] | None = None):
    engine = get_engine()

    base_url, available_years, year_col = _resolve_endpoint()
    print(f"[mcas] Available years: {available_years}")

    if not available_years:
        print("[mcas] ERROR: no years found in dataset")
        return

    if years_to_load is None:
        years_to_load = [available_years[-1]]

    for year_str in years_to_load:
        if year_str not in available_years:
            print(f"[mcas] WARNING: year '{year_str}' not in dataset (available: {available_years})")
            continue
        load_year(engine, base_url, year_str, year_col=year_col)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MCAS data from MA E2C Socrata API")
    parser.add_argument("--year",  help="Specific school year to load (e.g. 2023-2024)")
    parser.add_argument("--all",   action="store_true", help="Load all available years")
    args = parser.parse_args()

    if args.all:
        run(years_to_load=None)  # will be overridden below
        # Reload with all years
        engine = get_engine()
        base_url = SOCRATA_BASE
        try:
            all_years = fetch_years(base_url)
        except Exception:
            all_years = fetch_years(SOCRATA_FALLBACK)
        run(years_to_load=all_years)
    elif args.year:
        run(years_to_load=[args.year])
    else:
        run()  # most recent year
