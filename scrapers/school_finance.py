"""
Loads school-level per-pupil expenditure data from the MA DOE
Education to Career CSV export.

Expected file pattern: Files/School_Expenditures_by_Spending_Category_*.csv

Only rows with IND_VALUE_TYPE = 'Amount' are loaded (financial rows).
Non-financial rows (demographics, staffing, MCAS) are skipped.

Run: python scrapers/school_finance.py [--year 2024] [--all]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import glob
import argparse
import pandas as pd
from sqlalchemy import text
from config import get_engine

FILES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Files")
CSV_GLOB  = os.path.join(FILES_DIR, "School_Expenditures_by_Spending_Category_*.csv")


def discover_csv_files(target_year: int | None = None) -> list[dict]:
    """
    Finds downloaded school-expenditure CSV files in Files/.
    Returns list of {year: int, path: str}.

    Year is taken from the SY column inside the file, not the filename.
    """
    paths = sorted(glob.glob(CSV_GLOB))
    if not paths:
        print(f"[school_finance] No CSV files found matching: {CSV_GLOB}")
        return []

    result = []
    for path in paths:
        # Peek at the SY column to learn what years are in the file
        try:
            years_in_file = (
                pd.read_csv(path, usecols=["SY"], dtype=str)["SY"]
                .str.strip()
                .dropna()
                .unique()
                .tolist()
            )
        except Exception as e:
            print(f"[school_finance] WARNING: cannot read {path}: {e}")
            continue

        for sy in years_in_file:
            try:
                yr = int(sy)
            except ValueError:
                continue
            if target_year and yr != target_year:
                continue
            result.append({"year": yr, "path": path})

    # deduplicate by (year, path)
    seen = set()
    deduped = []
    for r in result:
        key = (r["year"], r["path"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    print(f"[school_finance] Found {len(deduped)} (year, file) combination(s)")
    return deduped


UPSERT_SQL = text("""
    INSERT INTO school_expenditures
        (school_year, dist_code, dist_name, org_code, school_name,
         grades_served, ind_cat, ind_subcat, ind_value)
    VALUES
        (:school_year, :dist_code, :dist_name, :org_code, :school_name,
         :grades_served, :ind_cat, :ind_subcat, :ind_value)
    ON CONFLICT (school_year, org_code, ind_cat, ind_subcat) DO UPDATE SET
        dist_code     = EXCLUDED.dist_code,
        dist_name     = EXCLUDED.dist_name,
        school_name   = EXCLUDED.school_name,
        grades_served = EXCLUDED.grades_served,
        ind_value     = EXCLUDED.ind_value,
        loaded_at     = NOW()
""")


def load_file(engine, file_info: dict):
    year = file_info["year"]
    path = file_info["path"]
    print(f"[school_finance] Loading SY={year} from {os.path.basename(path)}")

    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # Filter to this year and Amount-type rows only
    df = df[df["SY"].str.strip() == str(year)]
    df = df[df["IND_VALUE_TYPE"].str.strip().str.lower() == "amount"]

    if df.empty:
        print(f"[school_finance] WARNING: no Amount rows for SY={year}")
        return 0

    records = []
    for _, row in df.iterrows():
        val_str = str(row.get("IND_VALUE", "") or "").strip().replace(",", "")
        try:
            amount = float(val_str)
        except ValueError:
            continue

        records.append({
            "school_year":  year,
            "dist_code":    str(row.get("DIST_CODE", "") or "").strip() or None,
            "dist_name":    str(row.get("DIST_NAME", "") or "").strip() or None,
            "org_code":     str(row.get("ORG_CODE",  "") or "").strip() or None,
            "school_name":  str(row.get("ORG_NAME",  "") or "").strip() or None,
            "grades_served":str(row.get("GRADES_SERVED", "") or "").strip() or None,
            "ind_cat":      str(row.get("IND_CAT",   "") or "").strip() or None,
            "ind_subcat":   str(row.get("IND_SUBCAT","") or "").strip() or None,
            "ind_value":    amount,
        })

    if not records:
        print(f"[school_finance] WARNING: 0 parseable records for SY={year}")
        return 0

    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, records)
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
            VALUES ('school_expenditures', :yr, :n, 'ok', :path)
        """), {"yr": year, "n": len(records), "path": path})

    print(f"[school_finance] SY={year}: loaded {len(records):,} rows")
    return len(records)


def run(target_year: int | None = None, load_all: bool = False):
    engine = get_engine()
    file_infos = discover_csv_files(target_year=None if load_all else target_year)
    if not file_infos:
        return

    if not load_all and target_year is None:
        # Most recent year only
        most_recent = max(f["year"] for f in file_infos)
        file_infos = [f for f in file_infos if f["year"] == most_recent]

    for fi in file_infos:
        try:
            load_file(engine, fi)
        except Exception as e:
            print(f"[school_finance] ERROR loading {fi}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MA school-level expenditure data")
    parser.add_argument("--year", type=int, help="School year to load (e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all years in file")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all)
