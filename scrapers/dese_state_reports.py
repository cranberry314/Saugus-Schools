"""
Scrapes MA district-level state reports from DESE school profiles.

Source: https://profiles.doe.mass.edu/statereport/

Reports:
  sat          — SAT Performance (2005–present)  → district_sat_scores
  postsecondary— Graduates Attending Higher Ed (2004–present) → district_postsecondary
  dropout      — Dropout Report (2008–present)   → district_dropout

Run:
    python scrapers/dese_state_reports.py sat              # all years
    python scrapers/dese_state_reports.py postsecondary    # all years
    python scrapers/dese_state_reports.py dropout          # all years
    python scrapers/dese_state_reports.py all              # all three
    python scrapers/dese_state_reports.py sat --year 2024  # single year
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import argparse
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE = "https://profiles.doe.mass.gov"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# ---------------------------------------------------------------------------
# Report configurations
# ---------------------------------------------------------------------------

REPORTS = {
    "sat": {
        "url": "https://profiles.doe.mass.edu/statereport/sat.aspx",
        "year_select": "ctl00$ContentPlaceHolder1$ddYear",
        "type_select": "ctl00$ContentPlaceHolder1$ddReportType",
        "type_value": "DISTRICT",
        "submit": "ctl00$ContentPlaceHolder1$btnViewReport",
        "table": "district_sat_scores",
        "source_name": "dese_sat",
    },
    "postsecondary": {
        "url": "https://profiles.doe.mass.edu/statereport/gradsattendingcollege.aspx",
        "year_select": "ctl00$ContentPlaceHolder1$ddYear",
        "type_select": "ctl00$ContentPlaceHolder1$ddReportType",
        "type_value": "District",
        "submit": "ctl00$ContentPlaceHolder1$btnViewReport",
        "table": "district_postsecondary",
        "source_name": "dese_postsecondary",
    },
    "dropout": {
        "url": "https://profiles.doe.mass.edu/statereport/dropout.aspx",
        "year_select": "ctl00$ContentPlaceHolder1$ddYear",
        "type_select": "ctl00$ContentPlaceHolder1$ddReportType",
        "type_value": "District",   # note: capital D, different from other reports
        "extra_fields": {"ctl00$ContentPlaceHolder1$ddSubgroup": "ALL"},
        "submit": "ctl00$ContentPlaceHolder1$btnViewReport",
        "table": "district_dropout",
        "source_name": "dese_dropout",
    },
}

UPSERT_SAT = text("""
    INSERT INTO district_sat_scores
        (school_year, district_code, district_name, tests_taken, mean_ebrw, mean_math)
    VALUES
        (:school_year, :district_code, :district_name, :tests_taken, :mean_ebrw, :mean_math)
    ON CONFLICT (school_year, district_code) DO UPDATE SET
        district_name = EXCLUDED.district_name,
        tests_taken   = EXCLUDED.tests_taken,
        mean_ebrw     = EXCLUDED.mean_ebrw,
        mean_math     = EXCLUDED.mean_math,
        loaded_at     = NOW()
""")

UPSERT_POSTSEC = text("""
    INSERT INTO district_postsecondary
        (school_year, district_code, district_name,
         grads_n, attending_n, attending_pct,
         private_2yr_pct, private_4yr_pct,
         public_2yr_pct, public_4yr_pct,
         ma_comm_college_pct, ma_state_univ_pct, umass_pct)
    VALUES
        (:school_year, :district_code, :district_name,
         :grads_n, :attending_n, :attending_pct,
         :private_2yr_pct, :private_4yr_pct,
         :public_2yr_pct, :public_4yr_pct,
         :ma_comm_college_pct, :ma_state_univ_pct, :umass_pct)
    ON CONFLICT (school_year, district_code) DO UPDATE SET
        district_name       = EXCLUDED.district_name,
        grads_n             = EXCLUDED.grads_n,
        attending_n         = EXCLUDED.attending_n,
        attending_pct       = EXCLUDED.attending_pct,
        private_2yr_pct     = EXCLUDED.private_2yr_pct,
        private_4yr_pct     = EXCLUDED.private_4yr_pct,
        public_2yr_pct      = EXCLUDED.public_2yr_pct,
        public_4yr_pct      = EXCLUDED.public_4yr_pct,
        ma_comm_college_pct = EXCLUDED.ma_comm_college_pct,
        ma_state_univ_pct   = EXCLUDED.ma_state_univ_pct,
        umass_pct           = EXCLUDED.umass_pct,
        loaded_at           = NOW()
""")

UPSERT_DROPOUT = text("""
    INSERT INTO district_dropout
        (school_year, district_code, district_name,
         enrolled_9_12, dropout_n, dropout_pct,
         gr9_pct, gr10_pct, gr11_pct, gr12_pct)
    VALUES
        (:school_year, :district_code, :district_name,
         :enrolled_9_12, :dropout_n, :dropout_pct,
         :gr9_pct, :gr10_pct, :gr11_pct, :gr12_pct)
    ON CONFLICT (school_year, district_code) DO UPDATE SET
        district_name  = EXCLUDED.district_name,
        enrolled_9_12  = EXCLUDED.enrolled_9_12,
        dropout_n      = EXCLUDED.dropout_n,
        dropout_pct    = EXCLUDED.dropout_pct,
        gr9_pct        = EXCLUDED.gr9_pct,
        gr10_pct       = EXCLUDED.gr10_pct,
        gr11_pct       = EXCLUDED.gr11_pct,
        gr12_pct       = EXCLUDED.gr12_pct,
        loaded_at      = NOW()
""")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(v: str) -> float | None:
    try:
        return float(v.replace(",", "").strip()) if v.strip() else None
    except ValueError:
        return None

def _safe_int(v: str) -> int | None:
    try:
        return int(v.replace(",", "").strip()) if v.strip() else None
    except ValueError:
        return None

def _get_page(session: requests.Session, url: str) -> tuple[BeautifulSoup, dict]:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = {i["name"]: i.get("value", "") for i in soup.find_all("input", type="hidden") if i.get("name")}
    return soup, hidden

def _fetch_table(session: requests.Session, cfg: dict, year: str) -> list[list[str]]:
    """POST the form for a given year and return rows as list-of-lists.

    Finds the report form on the page (not the site-search form), collects all
    field defaults, then POSTs with the requested year.  This handles pages with
    multiple forms and extra hidden dropdowns that earlier versions missed.
    """
    url = cfg["url"]

    # GET the page
    r0 = session.get(url, timeout=30)
    r0.raise_for_status()
    soup0 = BeautifulSoup(r0.text, "html.parser")

    # Find the report form: the one whose action points back to this page
    # (not the site-search form whose action goes elsewhere)
    report_form = None
    for form in soup0.find_all("form"):
        action = form.get("action", "")
        if action and "search_link" not in action and "/search/" not in action:
            report_form = form
            break
    if report_form is None:
        # Fall back: use all hidden fields from page
        report_form = soup0

    # Collect all current field values from the report form
    fields: dict[str, str] = {}
    for inp in report_form.find_all("input"):
        name = inp.get("name")
        if name:
            fields[name] = inp.get("value", "")
    for sel in report_form.find_all("select"):
        name = sel.get("name")
        if name:
            selected_opt = sel.find("option", selected=True)
            fields[name] = selected_opt.get("value", "") if selected_opt else ""

    # Resolve config keys: ASP.NET names may be full ("ctl00$...$ddYear") or
    # short ("ddYear") — match whichever is actually in the form.
    def _resolve(key: str) -> str:
        if key in fields:
            return key
        short = key.split("$")[-1]
        return short if short in fields else key

    fields["__EVENTTARGET"]   = ""
    fields["__EVENTARGUMENT"] = ""
    fields[_resolve(cfg["year_select"])] = year
    if cfg.get("type_select") and cfg.get("type_value"):
        fields[_resolve(cfg["type_select"])] = cfg["type_value"]
    for k, v in cfg.get("extra_fields", {}).items():
        fields[_resolve(k)] = v
    # Include submit button only if it has a name attribute (some pages use
    # <button name=""> with no name, in which case the server ignores it)
    submit_short = cfg["submit"].split("$")[-1]
    submit_el = report_form.find(attrs={"id": submit_short})
    if submit_el and submit_el.get("name"):
        fields[submit_el["name"]] = "View Report"

    r = session.post(url, data=fields, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tbl = soup.find("table")
    if not tbl:
        return []
    rows = tbl.find_all("tr")
    result = []
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
        if cells:
            result.append(cells)
    return result


def _available_years(url: str) -> list[str]:
    session = requests.Session()
    session.headers.update(HEADERS)
    soup, _ = _get_page(session, url)
    sel = soup.find("select", id=lambda x: x and "Year" in (x or ""))
    if not sel:
        return []
    return [o.get("value") for o in sel.find_all("option") if o.get("value")]


# ---------------------------------------------------------------------------
# Per-report loaders
# ---------------------------------------------------------------------------

def load_sat(engine, years: list[str] | None = None) -> int:
    cfg = REPORTS["sat"]
    all_years = years or _available_years(cfg["url"])
    session = requests.Session()
    session.headers.update(HEADERS)
    total = 0

    for year in all_years:
        rows = _fetch_table(session, cfg, year)
        if len(rows) < 2:
            print(f"[dese_sat] FY{year}: no data")
            continue

        records = []
        for row in rows[1:]:   # skip header
            if len(row) < 5:
                continue
            name, code = row[0], row[1]
            if not code or code == "District Code":
                continue
            records.append({
                "school_year":   int(year),
                "district_code": code,
                "district_name": name,
                "tests_taken":   _safe_int(row[2]),
                "mean_ebrw":     _safe_int(row[3]),
                "mean_math":     _safe_int(row[4]),
            })

        if records:
            with engine.begin() as conn:
                conn.execute(UPSERT_SAT, records)
            print(f"[dese_sat] FY{year}: {len(records)} districts upserted")
            total += len(records)
        time.sleep(0.5)

    if total:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO ingest_log (source, school_year, rows_loaded, status) VALUES ('dese_sat', NULL, :n, 'ok')"), {"n": total})
    return total


def load_postsecondary(engine, years: list[str] | None = None) -> int:
    cfg = REPORTS["postsecondary"]
    all_years = years or _available_years(cfg["url"])
    session = requests.Session()
    session.headers.update(HEADERS)
    total = 0

    for year in all_years:
        rows = _fetch_table(session, cfg, year)
        if len(rows) < 2:
            print(f"[dese_postsec] FY{year}: no data")
            continue

        header = [h.lower().replace(" ", "_").replace(".", "").replace("/", "_") for h in rows[0]]

        records = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            name, code = row[0], row[1]
            if not code or code == "District Code":
                continue
            # Columns vary by year — map by position after code
            vals = row[2:]
            records.append({
                "school_year":        int(year),
                "district_code":      code,
                "district_name":      name,
                "grads_n":            _safe_int(vals[0])  if len(vals) > 0 else None,
                "attending_n":        _safe_int(vals[1])  if len(vals) > 1 else None,
                "attending_pct":      _safe_float(vals[2])if len(vals) > 2 else None,
                "private_2yr_pct":    _safe_float(vals[3])if len(vals) > 3 else None,
                "private_4yr_pct":    _safe_float(vals[4])if len(vals) > 4 else None,
                # public 2yr/4yr may be absent in newer years
                "public_2yr_pct":     _safe_float(vals[5])if len(vals) > 5 and len(vals) > 8 else None,
                "public_4yr_pct":     _safe_float(vals[6])if len(vals) > 6 and len(vals) > 8 else None,
                "ma_comm_college_pct":_safe_float(vals[-3])if len(vals) >= 3 else None,
                "ma_state_univ_pct":  _safe_float(vals[-2])if len(vals) >= 2 else None,
                "umass_pct":          _safe_float(vals[-1])if len(vals) >= 1 else None,
            })

        if records:
            with engine.begin() as conn:
                conn.execute(UPSERT_POSTSEC, records)
            print(f"[dese_postsec] FY{year}: {len(records)} districts upserted")
            total += len(records)
        time.sleep(0.5)

    if total:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO ingest_log (source, school_year, rows_loaded, status) VALUES ('dese_postsecondary', NULL, :n, 'ok')"), {"n": total})
    return total


def load_dropout(engine, years: list[str] | None = None) -> int:
    cfg = REPORTS["dropout"]
    all_years = years or _available_years(cfg["url"])
    session = requests.Session()
    session.headers.update(HEADERS)
    total = 0

    for year in all_years:
        rows = _fetch_table(session, cfg, year)
        if len(rows) < 2:
            print(f"[dese_dropout] FY{year}: no data")
            continue

        records = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            name, code = row[0], row[1]
            # Skip blank rows and the header row where DESE writes "School Code" in the second column.
            if not code or code == "School Code":
                continue
            vals = row[2:]
            records.append({
                "school_year":   int(year),
                "district_code": code,
                "district_name": name,
                "enrolled_9_12": _safe_int(vals[0])   if len(vals) > 0 else None,
                "dropout_n":     _safe_int(vals[1])   if len(vals) > 1 else None,
                "dropout_pct":   _safe_float(vals[2]) if len(vals) > 2 else None,
                "gr9_pct":       _safe_float(vals[3]) if len(vals) > 3 else None,
                "gr10_pct":      _safe_float(vals[4]) if len(vals) > 4 else None,
                "gr11_pct":      _safe_float(vals[5]) if len(vals) > 5 else None,
                "gr12_pct":      _safe_float(vals[6]) if len(vals) > 6 else None,
            })

        if records:
            with engine.begin() as conn:
                conn.execute(UPSERT_DROPOUT, records)
            print(f"[dese_dropout] FY{year}: {len(records)} districts upserted")
            total += len(records)
        time.sleep(0.5)

    if total:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO ingest_log (source, school_year, rows_loaded, status) VALUES ('dese_dropout', NULL, :n, 'ok')"), {"n": total})
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report", choices=["sat", "postsecondary", "dropout", "all"])
    parser.add_argument("--year", type=str, help="Single school year code (e.g. 2024 = 2023-24)")
    args = parser.parse_args()

    engine = get_engine()
    years = [args.year] if args.year else None
    reports = ["sat", "postsecondary", "dropout"] if args.report == "all" else [args.report]

    for report in reports:
        if report == "sat":
            n = load_sat(engine, years)
        elif report == "postsecondary":
            n = load_postsecondary(engine, years)
        elif report == "dropout":
            n = load_dropout(engine, years)
        print(f"[dese_{report}] Done — {n} total rows.")
