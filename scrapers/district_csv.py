"""
Loads district-level data from the MA DOE Education to Career CSV exports.

Two source files (both in Files/):
  District_Expenditures_by_Spending_Category_*.csv  →  enrollment, demographics,
                                                        per_pupil_expenditure, staffing
  District_Expenditures_by_Function_Code_*.csv      →  district_financials

Run: python scrapers/district_csv.py [--year 2024] [--all]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import glob
import argparse
import pandas as pd
from sqlalchemy import text
from config import get_engine

FILES_DIR    = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Files")
SPENDING_GLOB = os.path.join(FILES_DIR, "District_Expenditures_by_Spending_Category_*.csv")
FUNCTION_GLOB = os.path.join(FILES_DIR, "District_Expenditures_by_Function_Code_*.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_dollar(s: str) -> float | None:
    """'$1,234,567' → 1234567.0,  blank / '-' → None"""
    s = str(s or "").strip().replace("$", "").replace(",", "").replace(" ", "")
    if not s or s in ("-", "–", "N/A", "n/a", "."):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_count(s: str) -> float | None:
    s = str(s or "").strip().replace(",", "")
    if not s or s in ("-", "–", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _years_in_file(path: str) -> list[int]:
    try:
        return sorted(
            int(y) for y in
            pd.read_csv(path, usecols=["SY"], dtype=str)["SY"].str.strip().dropna().unique()
            if y.isdigit()
        )
    except Exception as e:
        print(f"[district_csv] WARNING: cannot read years from {path}: {e}")
        return []


# ---------------------------------------------------------------------------
# 1. Enrollment  (IND_CAT = "Student Enrollment", Count rows)
# ---------------------------------------------------------------------------

UPSERT_ENROLL = text("""
    INSERT INTO enrollment (school_year, org_code, district_name, grade, total)
    VALUES (:school_year, :org_code, :district_name, :grade, :total)
    ON CONFLICT (school_year, org_code, grade) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        total         = EXCLUDED.total,
        loaded_at     = NOW()
""")

# Map IND_SUBCAT → grade label stored in DB
_ENROLL_GRADE = {
    "total fte pupils":           "Total",
    "in-district fte pupils":     "In-District",
    "out-of-district fte pupils": "Out-of-District",
}


def _load_enrollment(conn, df_year: pd.DataFrame, year: int) -> int:
    sub = df_year[df_year["IND_CAT"] == "Student Enrollment"].copy()
    records = []
    for _, row in sub.iterrows():
        subcat_key = str(row["IND_SUBCAT"] or "").strip().lower()
        grade = _ENROLL_GRADE.get(subcat_key)
        if grade is None:
            continue
        val = _clean_count(row["IND_VALUE"])
        if val is None:
            continue
        records.append({
            "school_year":   year,
            "org_code":      str(row["DIST_CODE"]).strip(),
            "district_name": str(row["DIST_NAME"] or "").strip() or None,
            "grade":         grade,
            "total":         int(round(val)),
        })
    if records:
        conn.execute(UPSERT_ENROLL, records)
    return len(records)


# ---------------------------------------------------------------------------
# 2. Demographics  (IND_CAT = "Student Demographics")
# ---------------------------------------------------------------------------

UPSERT_DEMO = text("""
    INSERT INTO demographics (school_year, org_code, district_name, category, pct, count)
    VALUES (:school_year, :org_code, :district_name, :category, :pct, :count)
    ON CONFLICT (school_year, org_code, category) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        pct           = EXCLUDED.pct,
        count         = EXCLUDED.count,
        loaded_at     = NOW()
""")

# Normalise IND_SUBCAT → a stable category name the peers query can find
_DEMO_CAT = {
    "english learner % headcount":           "ell_pct",
    "low-income % headcount":                "low_income_pct",
    "student headcount":                     "student_headcount",
    "students with disabilities % headcount":"sped_pct",
}


def _load_demographics(conn, df_year: pd.DataFrame, year: int) -> int:
    sub = df_year[df_year["IND_CAT"] == "Student Demographics"].copy()
    records = []
    for _, row in sub.iterrows():
        subcat_key = str(row["IND_SUBCAT"] or "").strip().lower()
        category = _DEMO_CAT.get(subcat_key)
        if category is None:
            continue
        val = _clean_count(row["IND_VALUE"])
        if val is None:
            continue
        is_pct = str(row.get("IND_VALUE_TYPE", "")).strip().lower() == "percent"
        records.append({
            "school_year":   year,
            "org_code":      str(row["DIST_CODE"]).strip(),
            "district_name": str(row["DIST_NAME"] or "").strip() or None,
            "category":      category,
            "pct":           val if is_pct else None,
            "count":         int(round(val)) if not is_pct else None,
        })
    if records:
        conn.execute(UPSERT_DEMO, records)
    return len(records)


# ---------------------------------------------------------------------------
# 3. Per-Pupil Expenditure  (IND_CAT = "Expenditures Per Pupil", Amount rows)
# ---------------------------------------------------------------------------

UPSERT_PPE = text("""
    INSERT INTO per_pupil_expenditure (school_year, org_code, district_name, category, amount)
    VALUES (:school_year, :org_code, :district_name, :category, :amount)
    ON CONFLICT (school_year, org_code, category) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        amount        = EXCLUDED.amount,
        loaded_at     = NOW()
""")


def _load_ppe(conn, df_year: pd.DataFrame, year: int) -> int:
    sub = df_year[
        (df_year["IND_CAT"] == "Expenditures Per Pupil") &
        (df_year["IND_VALUE_TYPE"].str.strip().str.lower() == "amount")
    ].copy()
    records = []
    for _, row in sub.iterrows():
        val = _clean_count(row["IND_VALUE"])
        if val is None:
            continue
        records.append({
            "school_year":   year,
            "org_code":      str(row["DIST_CODE"]).strip(),
            "district_name": str(row["DIST_NAME"] or "").strip() or None,
            "category":      str(row["IND_SUBCAT"] or "").strip(),
            "amount":        val,
        })
    if records:
        conn.execute(UPSERT_PPE, records)
    return len(records)


# ---------------------------------------------------------------------------
# 4. Staffing  (IND_CAT = "Other Staff" | "Teacher Salaries")
# ---------------------------------------------------------------------------

UPSERT_STAFFING = text("""
    INSERT INTO staffing (school_year, org_code, district_name, category, fte, avg_salary)
    VALUES (:school_year, :org_code, :district_name, :category, :fte, :avg_salary)
    ON CONFLICT (school_year, org_code, category) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        fte           = COALESCE(EXCLUDED.fte,        staffing.fte),
        avg_salary    = COALESCE(EXCLUDED.avg_salary, staffing.avg_salary),
        loaded_at     = NOW()
""")

# Map (IND_CAT, IND_SUBCAT) → (category, field)
# field: 'fte' | 'avg_salary'
_STAFFING_MAP = {
    ("Other Staff",      "Paraprofessional FTE"):                          ("para_fte",           "fte"),
    ("Other Staff",      "Instructional Coach FTE"):                       ("instructional_coach_fte", "fte"),
    ("Other Staff",      "Instructional Support FTE"):                     ("instructional_support_fte", "fte"),
    ("Other Staff",      "Special Education Instructional Support FTE"):   ("sped_support_fte",   "fte"),
    ("Teacher Salaries", "Teacher FTE"):                                   ("teacher_fte",        "fte"),
    ("Teacher Salaries", "Teachers per 100 FTE students"):                 ("teachers_per_100_fte", "fte"),
    ("Teacher Salaries", "Average Teacher Salary"):                        ("teacher_avg_salary", "avg_salary"),
}


def _load_staffing(conn, df_year: pd.DataFrame, year: int) -> int:
    sub = df_year[df_year["IND_CAT"].isin(["Other Staff", "Teacher Salaries"])].copy()
    records = []
    for _, row in sub.iterrows():
        ind_cat    = str(row["IND_CAT"] or "").strip()
        ind_subcat = str(row["IND_SUBCAT"] or "").strip()
        key = (ind_cat, ind_subcat)
        if key not in _STAFFING_MAP:
            continue
        category, field = _STAFFING_MAP[key]
        val = _clean_count(row["IND_VALUE"])
        if val is None:
            continue
        records.append({
            "school_year":   year,
            "org_code":      str(row["DIST_CODE"]).strip(),
            "district_name": str(row["DIST_NAME"] or "").strip() or None,
            "category":      category,
            "fte":           val if field == "fte" else None,
            "avg_salary":    val if field == "avg_salary" else None,
        })
    if records:
        conn.execute(UPSERT_STAFFING, records)
    return len(records)


# ---------------------------------------------------------------------------
# 5. District Financials  (function code file)
# ---------------------------------------------------------------------------

UPSERT_FIN = text("""
    INSERT INTO district_financials
        (school_year, org_code, district_name, category, subcategory, amount)
    VALUES
        (:school_year, :org_code, :district_name, :category, :subcategory, :amount)
    ON CONFLICT (school_year, org_code, category, subcategory) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        amount        = EXCLUDED.amount,
        loaded_at     = NOW()
""")


def _load_district_financials(conn, df_func: pd.DataFrame, year: int) -> int:
    sub = df_func[df_func["SY"].str.strip() == str(year)].copy()
    records = []
    for _, row in sub.iterrows():
        tot = _clean_dollar(row.get("TOT_EXP"))
        if tot is None:
            continue
        in_out = str(row.get("IN_OUT_DIST") or "").strip() or "Total"
        func_cat = str(row.get("FUNC_CAT_DESC") or "").strip()
        func_desc = str(row.get("FUNC_DESC") or "").strip()
        # subcategory encodes the function description + in/out-district flag
        subcategory = f"{func_desc} [{in_out}]"
        records.append({
            "school_year":   year,
            "org_code":      str(row["DIST_CODE"]).strip(),
            "district_name": str(row["DIST_NAME"] or "").strip() or None,
            "category":      func_cat,
            "subcategory":   subcategory,
            "amount":        tot,
        })
        # Also store per-pupil expenditure as a separate row when present
        ppe = _clean_dollar(row.get("PER_PUPIL_EXP"))
        if ppe is not None and ppe > 0:
            records.append({
                "school_year":   year,
                "org_code":      str(row["DIST_CODE"]).strip(),
                "district_name": str(row["DIST_NAME"] or "").strip() or None,
                "category":      func_cat + " (per pupil)",
                "subcategory":   subcategory,
                "amount":        ppe,
            })
    if records:
        conn.execute(UPSERT_FIN, records)
    return len(records)


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def _find_csv(pattern: str) -> str | None:
    matches = sorted(glob.glob(pattern))
    if not matches:
        print(f"[district_csv] No file found matching: {pattern}")
        return None
    if len(matches) > 1:
        print(f"[district_csv] Multiple files found, using most recent: {matches[-1]}")
    return matches[-1]


def run(target_year: int | None = None, load_all: bool = False):
    engine = get_engine()

    spending_path = _find_csv(SPENDING_GLOB)
    function_path = _find_csv(FUNCTION_GLOB)

    if not spending_path and not function_path:
        print("[district_csv] No source files found — nothing to load.")
        return

    # Determine which years to load
    years: list[int] = []
    if spending_path:
        years = _years_in_file(spending_path)
    elif function_path:
        years = _years_in_file(function_path)

    if not years:
        print("[district_csv] No years found in source files.")
        return

    if not load_all and target_year is None:
        years = [max(years)]
    elif target_year is not None and not load_all:
        years = [y for y in years if y == target_year]

    print(f"[district_csv] Loading years: {years}")

    # Load spending-category CSV once (it's ~500k rows but fits in RAM fine)
    df_spending = None
    if spending_path:
        print(f"[district_csv] Reading {os.path.basename(spending_path)}...")
        df_spending = pd.read_csv(spending_path, dtype=str)
        df_spending["SY"] = df_spending["SY"].str.strip()

    df_function = None
    if function_path:
        print(f"[district_csv] Reading {os.path.basename(function_path)}...")
        df_function = pd.read_csv(function_path, dtype=str)
        df_function["SY"] = df_function["SY"].str.strip()

    for year in years:
        print(f"\n[district_csv] Processing SY={year}...")
        n_enroll = n_demo = n_ppe = n_staff = n_fin = 0

        with engine.begin() as conn:
            if df_spending is not None:
                df_yr = df_spending[df_spending["SY"] == str(year)]
                if df_yr.empty:
                    print(f"[district_csv]   No spending rows for {year}")
                else:
                    n_enroll = _load_enrollment(conn, df_yr, year)
                    n_demo   = _load_demographics(conn, df_yr, year)
                    n_ppe    = _load_ppe(conn, df_yr, year)
                    n_staff  = _load_staffing(conn, df_yr, year)

            if df_function is not None:
                n_fin = _load_district_financials(conn, df_function, year)

            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status, notes)
                VALUES ('district_csv', :yr, :n, 'ok', :notes)
            """), {
                "yr":    year,
                "n":     n_enroll + n_demo + n_ppe + n_staff + n_fin,
                "notes": f"enroll={n_enroll} demo={n_demo} ppe={n_ppe} staff={n_staff} fin={n_fin}",
            })

        print(f"[district_csv]   enrollment={n_enroll:,}  demographics={n_demo:,}  "
              f"ppe={n_ppe:,}  staffing={n_staff:,}  financials={n_fin:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load district data from local CSV exports")
    parser.add_argument("--year", type=int, help="School year to load (e.g. 2024)")
    parser.add_argument("--all",  action="store_true", help="Load all years in CSV")
    args = parser.parse_args()
    run(target_year=args.year, load_all=args.all)
