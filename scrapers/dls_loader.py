"""
Master loader for the MA DLS (Division of Local Services) Gateway report
catalog. Single entry point for pulling municipal financial data covering
all 351 MA municipalities into the database.

Source: https://dls-gw.dor.state.ma.us/reports/rdPage.aspx?rdReport=<NAME>

The DLS "Databank" collection (mass.gov/collections/DLS-databank-reports)
links to ~50 rdReport pages on dls-gw.dor.state.ma.us. The "Dashboard.Cat_1_
Reports.*" family share one export mechanism: a single POST with
rdReportFormat=NativeExcel&rdExportTableID=<TblXXX>&iclYear=<year> (repeated
per year) returns a long-format workbook (one row per municipality x fiscal
year) covering FY2002-present in one shot.

Each entry in REPORT_DEFS below is one such report: its rdReport name, export
table id, target DB table (auto-created via its ddl), and a column_map from
source Excel column -> (db column, converter).

Run:
    python scrapers/dls_loader.py --load free_cash
    python scrapers/dls_loader.py --load all
    python scrapers/dls_loader.py --load stabilization --start-year 2010 --end-year 2025
    python scrapers/dls_loader.py --survey   # explore other rdReport pages for structure
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import re
import json
import time
import argparse
import threading
import concurrent.futures
from collections import Counter

import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL = "https://dls-gw.dor.state.ma.us/reports/rdPage.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://dls-gw.dor.state.ma.us/",
}

DEFAULT_START_YEAR = 2002
DEFAULT_END_YEAR = 2026


# --------------------------------------------------------------------------
# Value converters
# --------------------------------------------------------------------------
def _to_int(s):
    try:
        return int(str(s).replace(",", "").strip())
    except Exception:
        return None


def _to_float(s):
    s = str(s or "").strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in ("-", "–", "N/A", "nan", ""):
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_str(s):
    s = str(s or "").strip()
    return s or None


def _to_date(s):
    d = pd.to_datetime(s, errors="coerce")
    return d.date() if pd.notna(d) else None


CONVERTERS = {"int": _to_int, "float": _to_float, "str": _to_str, "date": _to_date}


# --------------------------------------------------------------------------
# Report registry — "Dashboard.Cat_1_Reports.*" family
# (long-format, all-municipality, multi-year NativeExcel export)
# --------------------------------------------------------------------------
REPORT_DEFS = {
    "free_cash": {
        "title":      "Certified Free Cash",
        "rdReport":   "Dashboard.Cat_1_Reports.CertifiedFreeCashBudget351",
        "table_id":   "TblCFC_PerBudg",
        "filename":   "FreeCash",
        "db_table":   "municipal_free_cash",
        "conflict_cols": ["fiscal_year", "dor_code"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS municipal_free_cash (
                id                BIGSERIAL PRIMARY KEY,
                fiscal_year       INTEGER       NOT NULL,
                dor_code          INTEGER       NOT NULL,
                municipality      VARCHAR(100),
                date_certified    DATE,
                cert_free_cash    BIGINT,
                operating_budget  NUMERIC(16,2),
                free_cash_pct     NUMERIC(8,4),
                loaded_at         TIMESTAMP DEFAULT NOW(),
                UNIQUE (fiscal_year, dor_code)
            );
            CREATE INDEX IF NOT EXISTS idx_muni_free_cash_fy_code ON municipal_free_cash (fiscal_year, dor_code);
        """,
        "column_map": {
            "fiscal_year":      ("Fiscal Year", "int"),
            "dor_code":         ("DOR Code", "int"),
            "municipality":     ("Municipality", "str"),
            "date_certified":   ("Date Certified", "date"),
            "cert_free_cash":   ("Certified Free Cash as of 7/1", "int"),
            "operating_budget": ("Operating Budget Prior Year", "float"),
            "free_cash_pct":    ("Certified Free Cash as a % of the Budget", "float"),
        },
    },

    "stabilization": {
        "title":      "Stabilization Fund Balances",
        "rdReport":   "Dashboard.Cat_1_Reports.StablPerBudget351",
        "table_id":   "TblStabl_PerBudg",
        "filename":   "Stabilization",
        "db_table":   "municipal_stabilization",
        "conflict_cols": ["fiscal_year", "dor_code"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS municipal_stabilization (
                id                                  BIGSERIAL PRIMARY KEY,
                fiscal_year                         INTEGER       NOT NULL,
                dor_code                             INTEGER       NOT NULL,
                municipality                         VARCHAR(100),
                stabilization_fund_balance           BIGINT,
                special_stabilization_fund_balance   BIGINT,
                total_stabilization_fund_balance     BIGINT,
                operating_budget                     NUMERIC(16,2),
                stabilization_pct                    NUMERIC(8,4),
                special_stabilization_pct            NUMERIC(8,4),
                total_stabilization_pct              NUMERIC(8,4),
                loaded_at                            TIMESTAMP DEFAULT NOW(),
                UNIQUE (fiscal_year, dor_code)
            );
            CREATE INDEX IF NOT EXISTS idx_muni_stabilization_fy_code ON municipal_stabilization (fiscal_year, dor_code);
        """,
        "column_map": {
            "fiscal_year":                        ("Schedule A Fiscal Year", "int"),
            "dor_code":                           ("DOR Code", "int"),
            "municipality":                       ("Municipality", "str"),
            "stabilization_fund_balance":         ("Stabilization Fund Balance", "int"),
            "special_stabilization_fund_balance": ("Special Purpose Stabilization Fund Balance", "int"),
            "total_stabilization_fund_balance":   ("Total Stabilization Fund Balance", "int"),
            "operating_budget":                   ("Operating Budget", "float"),
            "stabilization_pct":                  ("Stabilization Fund as % of Budget", "float"),
            "special_stabilization_pct":          ("Special Purpose Stabilization  as % of Budget", "float"),
            "total_stabilization_pct":            ("Total Stabilization as % of Budget", "float"),
        },
    },

    "overlay_reserves": {
        "title":      "Overlay Reserves",
        "rdReport":   "Dashboard.Cat_1_Reports.OL1PerLevy351",
        "table_id":   "TblOverlayPerLevy",
        "filename":   "Overlay",
        "db_table":   "municipal_overlay_reserves",
        "conflict_cols": ["fiscal_year", "dor_code"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS municipal_overlay_reserves (
                id                     BIGSERIAL PRIMARY KEY,
                fiscal_year            INTEGER       NOT NULL,
                dor_code               INTEGER       NOT NULL,
                municipality           VARCHAR(100),
                overlay_appropriation  BIGINT,
                total_levy             BIGINT,
                overlay_pct            NUMERIC(8,4),
                loaded_at              TIMESTAMP DEFAULT NOW(),
                UNIQUE (fiscal_year, dor_code)
            );
            CREATE INDEX IF NOT EXISTS idx_muni_overlay_fy_code ON municipal_overlay_reserves (fiscal_year, dor_code);
        """,
        "column_map": {
            "fiscal_year":           ("Fiscal Year", "int"),
            "dor_code":              ("DOR Code", "int"),
            "municipality":          ("Municipality", "str"),
            "overlay_appropriation": ("Overlay Appropriation", "int"),
            "total_levy":            ("Total Levy", "int"),
            "overlay_pct":           ("Overlay as a % of Total Levy", "float"),
        },
    },
}


# --------------------------------------------------------------------------
# Generic export download + load
# --------------------------------------------------------------------------
def _download_excel(session: requests.Session, rdef: dict, years: list[int]) -> bytes | None:
    params = {
        "rdReport":            rdef["rdReport"],
        "rdReportFormat":      "NativeExcel",
        "rdExportTableID":     rdef["table_id"],
        "rdExportFilename":    rdef["filename"],
        "rdShowGridlines":     "True",
        "rdExcelOutputFormat": "Excel2007",
    }
    url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    post_data = {"iclYear": [str(y) for y in years]}
    r = session.post(url, data=post_data, timeout=120, allow_redirects=True)
    r.raise_for_status()
    if r.content[:2] != b"PK":
        print(f"[dls_loader] WARNING: response is not Excel "
              f"({len(r.content)} bytes, starts: {r.content[:40]})")
        return None
    return r.content


def _upsert_query(db_table: str, cols: list[str], conflict_cols: list[str]):
    update_cols = [c for c in cols if c not in conflict_cols]
    col_list = ", ".join(cols)
    val_list = ", ".join(f":{c}" for c in cols)
    update_list = ",\n        ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    conflict_list = ", ".join(conflict_cols)
    return text(f"""
        INSERT INTO {db_table} ({col_list})
        VALUES ({val_list})
        ON CONFLICT ({conflict_list}) DO UPDATE SET
            {update_list},
            loaded_at = NOW()
    """)


def _build_upsert(rdef: dict):
    return _upsert_query(rdef["db_table"], list(rdef["column_map"].keys()), rdef["conflict_cols"])


def load_report(slug: str, start_year: int = None, end_year: int = None) -> int:
    rdef = REPORT_DEFS[slug]
    start_year = start_year if start_year is not None else DEFAULT_START_YEAR
    end_year = end_year if end_year is not None else DEFAULT_END_YEAR
    years = list(range(start_year, end_year + 1))

    print(f"[dls_loader] {slug}: fetching {rdef['title']} for FY{start_year}-{end_year} "
          f"(all municipalities)...")

    session = requests.Session()
    session.headers.update(HEADERS)
    raw = _download_excel(session, rdef, years)
    if raw is None:
        print(f"[dls_loader] {slug}: FAILED to download report")
        return 0

    print(f"[dls_loader] {slug}: downloaded {len(raw):,} bytes")

    df = pd.read_excel(io.BytesIO(raw), dtype=str)
    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        rec = {}
        for db_col, (src_col, kind) in rdef["column_map"].items():
            rec[db_col] = CONVERTERS[kind](row.get(src_col))
        if rec.get("dor_code") is None or rec.get("fiscal_year") is None:
            continue
        records.append(rec)

    print(f"[dls_loader] {slug}: parsed {len(records)} municipality-years")

    if records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(rdef["ddl"]))
            conn.execute(_build_upsert(rdef), records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES (:source, NULL, :n, 'ok')
            """), {"source": rdef["db_table"], "n": len(records)})
        print(f"[dls_loader] {slug}: done. {len(records)} rows upserted "
              f"(years: {sorted(set(r['fiscal_year'] for r in records))})")

    return len(records)


# --------------------------------------------------------------------------
# General Fund Revenues & Expenditures (ScheduleA.GeneralFund)
#
# Unlike the Cat_1_Reports family, this report has no all-municipality export
# — each (municipality, fiscal year, Revenues|Expenditures) combination is a
# separate GET returning a one-row Excel file. Real data covers FY2000-2025
# (FY2026+ comes back as an all-zero placeholder, not yet certified).
# Fetched concurrently since 351 munis x 26 years x 2 = ~18k requests.
# --------------------------------------------------------------------------
GENERAL_FUND_BASE_PARAMS = {
    "rdReport":            "ScheduleA.GeneralFund",
    "rdSubReport":         "True",
    "rdReportFormat":      "NativeExcel",
    "rdExportTableID":     "xtGenFund",
    "rdShowGridlines":     "True",
    "rdExcelOutputFormat": "Excel2007",
}

GENERAL_FUND_START_YEAR = 2000
GENERAL_FUND_END_YEAR = 2025

REVENUE_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_revenues (
        id                  SERIAL PRIMARY KEY,
        dor_code            INTEGER      NOT NULL,
        municipality        TEXT         NOT NULL,
        fiscal_year         INTEGER      NOT NULL,
        taxes               BIGINT,
        service_charges     BIGINT,
        licenses_permits    BIGINT,
        federal_revenue     BIGINT,
        state_revenue       BIGINT,
        intergovernmental   BIGINT,
        special_assessments BIGINT,
        fines_forfeitures   BIGINT,
        miscellaneous       BIGINT,
        other_financing     BIGINT,
        transfers           BIGINT,
        total_revenues      BIGINT,
        loaded_at           TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year)
    );
"""

EXPENDITURE_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_expenditures (
        id                  SERIAL PRIMARY KEY,
        dor_code            INTEGER      NOT NULL,
        municipality        TEXT         NOT NULL,
        fiscal_year         INTEGER      NOT NULL,
        general_government  BIGINT,
        public_safety       BIGINT,
        education           BIGINT,
        public_works        BIGINT,
        human_services      BIGINT,
        culture_recreation  BIGINT,
        fixed_costs         BIGINT,
        intergovernmental   BIGINT,
        other_expenditures  BIGINT,
        debt_service        BIGINT,
        total_expenditures  BIGINT,
        loaded_at           TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year)
    );
"""

# db column -> source Excel column (export headers match these exactly)
REVENUE_COLUMN_MAP = {
    "taxes":               "Taxes",
    "service_charges":     "Service Charges",
    "licenses_permits":    "Licenses and Permits",
    "federal_revenue":     "Federal Revenue",
    "state_revenue":       "State Revenue",
    "intergovernmental":   "Revenue from Other Governments",
    "special_assessments": "Special Assessments",
    "fines_forfeitures":   "Fines and Forfeitures",
    "miscellaneous":       "Miscellaneous",
    "other_financing":     "Other Financing Sources",
    "transfers":           "Transfers",
    "total_revenues":      "Total Revenues",
}

EXPENDITURE_COLUMN_MAP = {
    "general_government": "General Government",
    "public_safety":      "Public Safety",
    "education":          "Education",
    "public_works":       "Public Works",
    "human_services":     "Human Services",
    "culture_recreation": "Culture and Recreation",
    "fixed_costs":        "Fixed Costs",
    "intergovernmental":  "Intergov Assessments",
    "other_expenditures": "Other Expenditures",
    "debt_service":       "Debt Service",
    "total_expenditures": "Total Expenditures",
}

_thread_local = threading.local()


def _thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return _thread_local.session


def _get_all_municipalities(session: requests.Session) -> list[tuple[int, str]]:
    """Fetch the GeneralFund subreport form and extract (dor_code, name) for all municipalities."""
    r = session.get(BASE_URL, params={
        "rdReport": "ScheduleA.GeneralFund", "rdSubReport": "True", "rdRequestForwarding": "Form",
    }, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    munis = []
    for inp in soup.find_all("input", {"name": "iclMuni"}):
        code = inp.get("value", "")
        if code.isdigit():
            span = inp.find_next_sibling("span")
            name = span.text.strip() if span else code
            munis.append((int(code), name))
    return munis


def _fetch_general_fund_row(dor_code: int, year: int, amount_type: str):
    session = _thread_session()
    params = {
        **GENERAL_FUND_BASE_PARAMS,
        "rdExportFilename": f"GF{amount_type}{dor_code}_{year}",
        "iclMuni":          str(dor_code),
        "islYear":          str(year),
        "islAmountType":    amount_type,
    }
    r = session.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    if r.content[:2] != b"PK":
        return None
    df = pd.read_excel(io.BytesIO(r.content), dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(how="all")
    if df.empty:
        return None
    return df.iloc[0]


def load_general_fund(start_year: int = None, end_year: int = None, max_workers: int = 10) -> int:
    start_year = start_year if start_year is not None else GENERAL_FUND_START_YEAR
    end_year = end_year if end_year is not None else GENERAL_FUND_END_YEAR
    years = list(range(start_year, end_year + 1))

    session = requests.Session()
    session.headers.update(HEADERS)
    munis = _get_all_municipalities(session)
    print(f"[dls_loader] general_fund: {len(munis)} municipalities x FY{start_year}-{end_year} "
          f"x 2 (Revenues/Expenditures) = {len(munis) * len(years) * 2} requests, {max_workers} workers...")

    tasks = [(dor, year, kind) for dor, _ in munis for year in years for kind in ("Revenues", "Expenditures")]

    rev_records, exp_records = [], []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_general_fund_row, dor, year, kind): (dor, year, kind) for dor, year, kind in tasks}
        for fut in concurrent.futures.as_completed(futures):
            dor, year, kind = futures[fut]
            row = fut.result()
            done += 1
            if row is None:
                continue
            municipality = _to_str(row.get("Municipality"))
            if kind == "Revenues":
                rec = {"dor_code": dor, "fiscal_year": year, "municipality": municipality}
                for db_col, src_col in REVENUE_COLUMN_MAP.items():
                    rec[db_col] = _to_int(row.get(src_col))
                if rec["total_revenues"]:
                    rev_records.append(rec)
            else:
                rec = {"dor_code": dor, "fiscal_year": year, "municipality": municipality}
                for db_col, src_col in EXPENDITURE_COLUMN_MAP.items():
                    rec[db_col] = _to_int(row.get(src_col))
                if rec["total_expenditures"]:
                    exp_records.append(rec)
            if done % 1000 == 0:
                print(f"[dls_loader] general_fund: {done}/{len(tasks)} fetched...")

    print(f"[dls_loader] general_fund: parsed {len(rev_records)} revenue rows, {len(exp_records)} expenditure rows")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(REVENUE_DDL))
        conn.execute(text(EXPENDITURE_DDL))
        if rev_records:
            conn.execute(_upsert_query("municipal_revenues", list(rev_records[0].keys()), ["dor_code", "fiscal_year"]), rev_records)
        if exp_records:
            conn.execute(_upsert_query("municipal_expenditures", list(exp_records[0].keys()), ["dor_code", "fiscal_year"]), exp_records)
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status)
            VALUES ('municipal_revenues', NULL, :n, 'ok')
        """), {"n": len(rev_records)})
        conn.execute(text("""
            INSERT INTO ingest_log (source, school_year, rows_loaded, status)
            VALUES ('municipal_expenditures', NULL, :n, 'ok')
        """), {"n": len(exp_records)})

    print(f"[dls_loader] general_fund: done. {len(rev_records)} revenue + {len(exp_records)} expenditure rows upserted")
    return len(rev_records) + len(exp_records)


# --------------------------------------------------------------------------
# Survey mode — explore other rdReport pages for export structure
# (read-only HTML reconnaissance, no DB writes)
# --------------------------------------------------------------------------
KNOWN_REPORTS = {
    "free_cash":              "Dashboard.Cat_1_Reports.CertifiedFreeCashBudget351",
    "stabilization":          "Dashboard.Cat_1_Reports.StablPerBudget351",
    "overlay_reserves":       "Dashboard.Cat_1_Reports.OL1PerLevy351",
    "general_fund":           "ScheduleA.GenFund_MAIN",
    "trust_funds":            "ScheduleA.TrustFunds.TrustFunds",
    "special_rev_funds":      "ScheduleA.Special_Rev_Funds.SpecialRevFunds",
    "enterprise_funds":       "ScheduleA.EnterpriseFunds.EnterpriseFunds",
    "capital_projects":       "ScheduleA.CapitalProjects.CapitalProjects",
    "health_insurance":       "ScheduleA.HealthInsurance.HealthInsExpenditures",
    "personnel_expenditures": "ScheduleA.PesonnelExpenditures.PersonnelExpenditures",
    "free_cash_proof":        "BalanceSheet.FreecashProofComp",
    "free_cash_other_tax":    "BalanceSheet.FreeCashOtherTaxDistricts",
    "snow_ice":               "BalanceSheet.SnowIce",
    "ent_fund_retained":      "BalanceSheet.EntFundRetainedEarnings",
    "excess_deficiency_rsd":  "BalanceSheet.ExcessAndDeficiencyRSD",
    "outstanding_receivables":"BalanceSheet.OutstandingReceivables",
    "tax_rates_by_class":          "PropertyTaxInformation.taxratesbyclass.taxratesbyclass_main",
    "tax_rates_by_class_district": "Districts.Tax_Rates_by_Class",
    "tax_levy_by_class":            "Dashboard.TrendAnalysisReports.TaxLevyByClass",
    "tax_levy_by_class_district":   "Districts.Levy_By_Class",
    "assessed_values":               "PropertyTaxInformation.AssessedValuesbyClass.assessedvaluesbyclass",
    "assessed_values_district":      "Districts.Assessed_Value_By_Class",
    "parcel_counts":                 "PropertyTaxInformation.LA4.Parcel_counts_vals",
    "parcel_counts_district":        "Districts.parcel_count_by_type",
    "la4_exempt_pct":                 "LA4.Totals",
    "eqv":                            "PropertyTaxInformation.EQV.EQV",
    "cip_tax_shift":                  "TaxRate.CIP_TaxShift",
    "avg_single_family_tax_bill":     "AverageSingleTaxBill.SingleFamTaxBill_wRange",
    "tax_rate_form_status":           "Tracking.TaxRateFormStatus",
    "excess_levy_capacity":       "Prop2.5.ExcessLevyCapandOverride_MAIN",
    "excess_levy_capacity_0309":  "Prop2.5.ExcessLevyCapandOverride_03_09",
    "override_underride_votes":   "Votes.Prop2_5.OverrideUnderride",
    "stabilization_override_votes": "Votes.Prop2_5.Stabilization",
    "debt_exclusion_votes":        "Votes.Prop2_5.DebtExclusionVotes",
    "debt_exclusion_levy_amt":     "Votes.Prop2_5.DebtExclusionLevyAmt",
    "capital_exclusion_votes":     "Votes.Prop2_5.Capital",
    "revenue_by_source":          "RevenueBySource.RBS.RevbySourceMAIN",
    "local_receipts_act_vs_est":  "TaxRateRecap.PAGE3.LocalReceiptsAct_vs_Est",
    "motor_vehicle_excise":       "TaxRateRecap.PAGE3.Subreports.MV_Act_Est",
    "meals_rooms_local_options":  "Local_Option_Meals_Rooms",
    "room_occupancy_local_options": "LocalOptions.Room_Tax_Impact_Fee",
    "local_options_meals_excise": "LocalOptions.Local_Options_Tax",
    "residential_exemption_calc": "Analysis.ResExemptionCalc",
    "state_house_notes": "Bonds.StateHouseNotes",
    "bond_ratings":      "DLS_bond_ratings",
    "population":            "Socioeconomic.Population.population_main",
    "labor_force":           "Dashboard.TrendAnalysisReports.LaborForce",
    "cpi":                   "Socioeconomic.consumer.consumerpriceindex_main",
    "income_eqv_per_capita": "DOR_Income_EQV_Per_Capita",
    "new_growth":            "NewGrowth.NewGrowth_dash_v2_test",
    "community_snapshot":   "CommunityPage",
}


def survey_report(session: requests.Session, rdreport: str) -> dict:
    """GET a report's HTML form and extract year range, table-id candidates,
    and column headers (best-effort, no Excel export performed)."""
    url = f"{BASE_URL}?rdReport={rdreport}&rdRequestForwarding=Form&iclYear=2024"
    try:
        r = session.get(url, timeout=30)
        html = r.text
    except Exception as e:
        return {"error": str(e)}

    info = {"status": r.status_code, "len": len(html)}

    years = sorted(set(re.findall(r'value="(\d{4})"', html, re.I)))
    info["years"] = [y for y in years if 1990 <= int(y) <= 2027]

    tbls = re.findall(r'(Tbl[A-Za-z0-9_]+)', html)
    info["table_ids"] = Counter(tbls).most_common(5) if tbls else []

    cols = re.findall(r'id="col([A-Za-z0-9_]+)-TH"[^>]*>(.*?)</TH>', html, re.S)
    info["columns"] = [(cid, re.sub(r'<[^>]+>', '', ctxt).strip()) for cid, ctxt in cols]

    info["looks_like_error"] = ("rdPage" not in html and len(html) < 5000)
    return info


def survey_all(out_path: str):
    session = requests.Session()
    session.headers.update(HEADERS)

    results = {}
    for slug, rdreport in KNOWN_REPORTS.items():
        print(f"[dls_loader] surveying {slug} ({rdreport})...")
        results[slug] = {"rdReport": rdreport, **survey_report(session, rdreport)}
        time.sleep(0.3)

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[dls_loader] done, {len(results)} reports surveyed -> {out_path}")


# --------------------------------------------------------------------------
# Schedule A session-cache sub-reports
#
# Trust Funds, Enterprise Funds, Capital Projects, Health Insurance,
# and Personnel Expenditures are hosted as individual per-municipality HTML
# reports behind an Akamized Reporting Services viewer.  The export URL only
# returns data for the server-side cache that was generated by the last form
# POST; the rdDataCache token in the HTML ties the export to that state.
#
# Pattern (trust_funds / enterprise_funds / capital_projects):
#   1. GET form page → extract iclMuni list
#   2. POST form with iclMuni + iclYear + islAmountType → response HTML
#   3. grep rdDataCache from response
#   4. GET NativeExcel export URL with rdDataCache → XLSX
#
# Health Insurance: single GET, wide format (year columns) → melt to long.
# Personnel: POST islYear (single-select) per year → 352-row long format.
# --------------------------------------------------------------------------

SCHED_A_START_YEAR = 2002
SCHED_A_END_YEAR   = 2025

HEALTH_INSURANCE_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_health_insurance (
        id                           SERIAL PRIMARY KEY,
        dor_code                     INTEGER NOT NULL,
        municipality                 TEXT,
        fiscal_year                  INTEGER NOT NULL,
        health_insurance_expenditure BIGINT,
        loaded_at                    TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year)
    );
    CREATE INDEX IF NOT EXISTS idx_mhi_fy_code ON municipal_health_insurance (fiscal_year, dor_code);
"""

TRUST_FUNDS_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_trust_funds (
        id                     SERIAL PRIMARY KEY,
        dor_code               INTEGER NOT NULL,
        municipality           TEXT,
        fiscal_year            INTEGER NOT NULL,
        amount_type            TEXT    NOT NULL DEFAULT 'Expenditures',
        non_expendable_trust   BIGINT,
        workers_compensation   BIGINT,
        pension_reserve        BIGINT,
        stabilization          BIGINT,
        health_claims_town     BIGINT,
        health_claims_employee BIGINT,
        conservation_trust     BIGINT,
        opeb_trust             BIGINT,
        other_trust            BIGINT,
        special_stabilization  BIGINT,
        total_revenues         BIGINT,
        loaded_at              TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year, amount_type)
    );
    CREATE INDEX IF NOT EXISTS idx_mtf_fy_code ON municipal_trust_funds (fiscal_year, dor_code);
"""

ENTERPRISE_FUNDS_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_enterprise_funds (
        id                SERIAL PRIMARY KEY,
        dor_code          INTEGER NOT NULL,
        municipality      TEXT,
        fiscal_year       INTEGER NOT NULL,
        amount_type       TEXT    NOT NULL DEFAULT 'Expenditures',
        water             BIGINT,
        sewer             BIGINT,
        electric          BIGINT,
        landfills         BIGINT,
        hospital          BIGINT,
        health_care       BIGINT,
        airport           BIGINT,
        harbor            BIGINT,
        golf_courses      BIGINT,
        public_recreation BIGINT,
        other             BIGINT,
        total_revenues    BIGINT,
        loaded_at         TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year, amount_type)
    );
    CREATE INDEX IF NOT EXISTS idx_mef_fy_code ON municipal_enterprise_funds (fiscal_year, dor_code);
"""

CAPITAL_PROJECTS_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_capital_projects (
        id                  SERIAL PRIMARY KEY,
        dor_code            INTEGER NOT NULL,
        municipality        TEXT,
        fiscal_year         INTEGER NOT NULL,
        amount_type         TEXT    NOT NULL DEFAULT 'Expenditures',
        water               BIGINT,
        sewer               BIGINT,
        schools             BIGINT,
        municipal_buildings BIGINT,
        landfill            BIGINT,
        highways_ch90       BIGINT,
        other               BIGINT,
        total_revenues      BIGINT,
        loaded_at           TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year, amount_type)
    );
    CREATE INDEX IF NOT EXISTS idx_mcp_fy_code ON municipal_capital_projects (fiscal_year, dor_code);
"""

PERSONNEL_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_personnel (
        id                   SERIAL PRIMARY KEY,
        dor_code             INTEGER NOT NULL,
        municipality         TEXT,
        fiscal_year          INTEGER NOT NULL,
        total_employees      INTEGER,
        total_salaries_wages BIGINT,
        loaded_at            TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year)
    );
    CREATE INDEX IF NOT EXISTS idx_mpers_fy_code ON municipal_personnel (fiscal_year, dor_code);
"""


def _get_form_munis(rdreport: str, muni_field_name: str = "iclMuni") -> list[str]:
    """GET the report form and return all iclMuni (or named) input values."""
    session = requests.Session()
    session.headers.update(HEADERS)
    r = session.get(f"{BASE_URL}?rdReport={rdreport}&rdRequestForwarding=Form", timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    return [inp.get("value", "") for inp in soup.find_all("input")
            if inp.get("name") == muni_field_name and inp.get("value")]


def _rdcache_export(
    rdreport: str, table_id: str, filename: str,
    munis: list[str], muni_param: str,
    year: int, amount_type: str | None = None,
    delay: float = 0.5,
) -> pd.DataFrame | None:
    """
    POST form to build server-side cache, extract rdDataCache, GET Excel export.
    Returns a DataFrame or None on failure.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    post_data: dict = {muni_param: munis, "iclYear": [str(year)]}
    if amount_type:
        post_data["islAmountType"] = amount_type

    r_html = session.post(
        f"{BASE_URL}?rdReport={rdreport}",
        data=post_data,
        timeout=120,
    )
    cache_ids = re.findall(r"rdDataCache=(\d+)", r_html.text)
    if not cache_ids:
        print(f"[dls_loader] {filename} FY{year}: no rdDataCache in response "
              f"({len(r_html.content)} bytes, status {r_html.status_code})")
        return None

    cache_id = cache_ids[0]
    export_url = (
        f"{BASE_URL}?rdReport={rdreport}"
        f"&rdReportFormat=NativeExcel&rdExportTableID={table_id}"
        f"&rdExportFilename={filename}&rdShowGridlines=True&rdExcelOutputFormat=Excel2007"
        f"&rdDataCache={cache_id}"
    )
    r_xls = session.get(export_url, timeout=120)
    if r_xls.content[:2] != b"PK":
        print(f"[dls_loader] {filename} FY{year}: export not Excel "
              f"({len(r_xls.content)} bytes)")
        return None

    time.sleep(delay)
    return pd.read_excel(io.BytesIO(r_xls.content), dtype=str)


# ---- Health Insurance -------------------------------------------------------

def load_health_insurance(start_year: int = SCHED_A_START_YEAR,
                          end_year: int = SCHED_A_END_YEAR) -> int:
    """
    Single GET returns all municipalities × all available years (wide format).
    Available years appear as column headers (e.g. '2012', '2013', …).
    Melt to long before upserting.
    """
    print("[dls_loader] health_insurance: downloading (all years, wide format)...")
    session = requests.Session()
    session.headers.update(HEADERS)
    url = (f"{BASE_URL}?rdReport=ScheduleA.HealthInsurance.HealthInsExpenditures"
           f"&rdReportFormat=NativeExcel&rdExportTableID=ctHealthExp"
           f"&rdExportFilename=HealthIns&rdShowGridlines=True&rdExcelOutputFormat=Excel2007")
    r = session.get(url, timeout=120)
    if r.content[:2] != b"PK":
        print("[dls_loader] health_insurance: FAILED — not Excel")
        return 0

    df = pd.read_excel(io.BytesIO(r.content), dtype=str)
    df.columns = [c.strip() for c in df.columns]

    year_cols = [c for c in df.columns if c.isdigit()
                 and start_year <= int(c) <= end_year]

    records = []
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        if dor is None:
            continue
        muni = _to_str(row.get("Municipality ") or row.get("Municipality"))
        for yr_col in year_cols:
            val = _to_int(row.get(yr_col))
            if val is None:
                continue
            records.append({
                "dor_code":                     dor,
                "municipality":                 muni,
                "fiscal_year":                  int(yr_col),
                "health_insurance_expenditure": val,
            })

    print(f"[dls_loader] health_insurance: parsed {len(records)} municipality-years")

    if records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(HEALTH_INSURANCE_DDL))
            conn.execute(
                _upsert_query("municipal_health_insurance",
                              ["dor_code", "municipality", "fiscal_year",
                               "health_insurance_expenditure"],
                              ["dor_code", "fiscal_year"]),
                records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_health_insurance', NULL, :n, 'ok')"
            ), {"n": len(records)})

    return len(records)


# ---- Trust Funds ------------------------------------------------------------

_TRUST_FUNDS_COL_MAP = {
    "non_expendable_trust":   "Non-Expendable Trust",
    "workers_compensation":   "Workers' Compensation",
    "pension_reserve":        "Pension Reserve",
    "stabilization":          "Stabilization",
    "health_claims_town":     "Health Claims (City/Town Share)",
    "health_claims_employee": "Health Claims (Employee Share)",
    "conservation_trust":     "Conservation Trust Fund",
    "opeb_trust":             "OPEB Trust Funds",
    "other_trust":            "Other Trust Funds",
    "special_stabilization":  "Special Purpose Stabilization",
    "total_revenues":         "Total Revenues",
}


def load_trust_funds(start_year: int = SCHED_A_START_YEAR,
                     end_year: int = SCHED_A_END_YEAR,
                     amount_type: str = "Expenditures") -> int:
    rdreport  = "ScheduleA.TrustFunds.TrustFunds"
    table_id  = "xtTrustFunds"

    print(f"[dls_loader] trust_funds ({amount_type}): loading FY{start_year}-{end_year}...")
    munis = _get_form_munis(rdreport, "iclMuni")
    print(f"[dls_loader] trust_funds: {len(munis)} municipalities in form")

    all_records: list[dict] = []
    for year in range(start_year, end_year + 1):
        df = _rdcache_export(rdreport, table_id, "TrustFunds", munis, "iclMuni",
                             year, amount_type)
        if df is None:
            continue
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            dor = _to_int(row.get("DOR Code"))
            if dor is None:
                continue
            rec: dict = {
                "dor_code":    dor,
                "municipality": _to_str(row.get("Municipality")),
                "fiscal_year": _to_int(row.get("Fiscal Year")) or year,
                "amount_type": amount_type,
            }
            for db_col, src_col in _TRUST_FUNDS_COL_MAP.items():
                rec[db_col] = _to_int(row.get(src_col))
            all_records.append(rec)

        print(f"[dls_loader] trust_funds FY{year}: {len(df)} rows")

    print(f"[dls_loader] trust_funds: total {len(all_records)} municipality-years")
    if all_records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(TRUST_FUNDS_DDL))
            conn.execute(
                _upsert_query(
                    "municipal_trust_funds",
                    ["dor_code", "municipality", "fiscal_year", "amount_type",
                     *list(_TRUST_FUNDS_COL_MAP.keys())],
                    ["dor_code", "fiscal_year", "amount_type"],
                ),
                all_records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_trust_funds', NULL, :n, 'ok')"
            ), {"n": len(all_records)})

    return len(all_records)


# ---- Enterprise Funds -------------------------------------------------------

_ENTERPRISE_FUNDS_COL_MAP = {
    "water":             "Water",
    "sewer":             "Sewer",
    "electric":          "Electric",
    "landfills":         "Landfills",
    "hospital":          "Hospital",
    "health_care":       "Health Care",
    "airport":           "Airport",
    "harbor":            "Harbor",
    "golf_courses":      "Golf Courses",
    "public_recreation": "Public Recreation",
    "other":             "Other ",        # trailing space in DLS export
    "total_revenues":    "Total Revenues",
}


def load_enterprise_funds(start_year: int = SCHED_A_START_YEAR,
                          end_year: int = SCHED_A_END_YEAR,
                          amount_type: str = "Expenditures") -> int:
    rdreport = "ScheduleA.EnterpriseFunds.EnterpriseFunds"
    table_id = "xtEntFunds"

    print(f"[dls_loader] enterprise_funds ({amount_type}): loading FY{start_year}-{end_year}...")
    munis = _get_form_munis(rdreport, "iclMuni")
    print(f"[dls_loader] enterprise_funds: {len(munis)} municipalities in form")

    all_records: list[dict] = []
    for year in range(start_year, end_year + 1):
        df = _rdcache_export(rdreport, table_id, "EntFunds", munis, "iclMuni",
                             year, amount_type)
        if df is None:
            continue
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            dor = _to_int(row.get("DOR Code"))
            if dor is None:
                continue
            rec: dict = {
                "dor_code":    dor,
                "municipality": _to_str(row.get("Municipality")),
                "fiscal_year": _to_int(row.get("Fiscal Year")) or year,
                "amount_type": amount_type,
            }
            for db_col, src_col in _ENTERPRISE_FUNDS_COL_MAP.items():
                rec[db_col] = _to_int(row.get(src_col) or row.get(src_col.strip()))
            all_records.append(rec)

        print(f"[dls_loader] enterprise_funds FY{year}: {len(df)} rows")

    print(f"[dls_loader] enterprise_funds: total {len(all_records)} municipality-years")
    if all_records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(ENTERPRISE_FUNDS_DDL))
            conn.execute(
                _upsert_query(
                    "municipal_enterprise_funds",
                    ["dor_code", "municipality", "fiscal_year", "amount_type",
                     *list(_ENTERPRISE_FUNDS_COL_MAP.keys())],
                    ["dor_code", "fiscal_year", "amount_type"],
                ),
                all_records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_enterprise_funds', NULL, :n, 'ok')"
            ), {"n": len(all_records)})

    return len(all_records)


# ---- Capital Projects -------------------------------------------------------

_CAPITAL_PROJECTS_COL_MAP = {
    "water":               "Water",
    "sewer":               "Sewer",
    "schools":             "Schools",
    "municipal_buildings": "Municipal Buildings",
    "landfill":            "Landfill",
    "highways_ch90":       "Highways (Chapter 90)",
    "other":               "Other",
    "total_revenues":      "Total Revenues",
}


def load_capital_projects(start_year: int = SCHED_A_START_YEAR,
                          end_year: int = SCHED_A_END_YEAR,
                          amount_type: str = "Expenditures") -> int:
    rdreport = "ScheduleA.CapitalProjects.CapitalProjects"
    table_id = "xtCapProjects"

    print(f"[dls_loader] capital_projects ({amount_type}): loading FY{start_year}-{end_year}...")
    munis = _get_form_munis(rdreport, "iclMuni")
    print(f"[dls_loader] capital_projects: {len(munis)} municipalities in form")

    all_records: list[dict] = []
    for year in range(start_year, end_year + 1):
        df = _rdcache_export(rdreport, table_id, "CapProjects", munis, "iclMuni",
                             year, amount_type)
        if df is None:
            continue
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            dor = _to_int(row.get("DOR Code"))
            if dor is None:
                continue
            rec: dict = {
                "dor_code":    dor,
                "municipality": _to_str(row.get("Municipality")),
                "fiscal_year": _to_int(row.get("Fiscal Year")) or year,
                "amount_type": amount_type,
            }
            for db_col, src_col in _CAPITAL_PROJECTS_COL_MAP.items():
                rec[db_col] = _to_int(row.get(src_col))
            all_records.append(rec)

        print(f"[dls_loader] capital_projects FY{year}: {len(df)} rows")

    print(f"[dls_loader] capital_projects: total {len(all_records)} municipality-years")
    if all_records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(CAPITAL_PROJECTS_DDL))
            conn.execute(
                _upsert_query(
                    "municipal_capital_projects",
                    ["dor_code", "municipality", "fiscal_year", "amount_type",
                     *list(_CAPITAL_PROJECTS_COL_MAP.keys())],
                    ["dor_code", "fiscal_year", "amount_type"],
                ),
                all_records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_capital_projects', NULL, :n, 'ok')"
            ), {"n": len(all_records)})

    return len(all_records)


# ---- Personnel Expenditures -------------------------------------------------

def load_personnel(start_year: int = SCHED_A_START_YEAR,
                   end_year: int = SCHED_A_END_YEAR) -> int:
    """
    Personnel uses a single-year SELECT (islYear).  POST per year.
    Returns: DOR Code, Municipality, Fiscal Year, Total Employees, Total Salaries & Wages.
    """
    rdreport = "ScheduleA.PesonnelExpenditures.PersonnelExpenditures"
    table_id = "ctPerExp"

    print(f"[dls_loader] personnel: loading FY{start_year}-{end_year}...")

    all_records: list[dict] = []
    for year in range(start_year, end_year + 1):
        session = requests.Session()
        session.headers.update(HEADERS)
        url = (f"{BASE_URL}?rdReport={rdreport}&rdReportFormat=NativeExcel"
               f"&rdExportTableID={table_id}&rdExportFilename=Personnel"
               f"&rdShowGridlines=True&rdExcelOutputFormat=Excel2007")
        r = session.post(url, data={"islYear": str(year)}, timeout=60)
        if r.content[:2] != b"PK":
            print(f"[dls_loader] personnel FY{year}: not Excel ({len(r.content)} bytes)")
            continue

        df = pd.read_excel(io.BytesIO(r.content), dtype=str)
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            dor = _to_int(row.get("DOR Code"))
            if dor is None:
                continue
            fy = _to_int(row.get("Fiscal Year")) or year
            all_records.append({
                "dor_code":            dor,
                "municipality":        _to_str(row.get("Municipality")),
                "fiscal_year":         fy,
                "total_employees":     _to_int(row.get("Total Employees")),
                "total_salaries_wages":_to_int(row.get("Total Salaries & Wages")),
            })

        print(f"[dls_loader] personnel FY{year}: {len(df)} rows")
        time.sleep(0.3)

    print(f"[dls_loader] personnel: total {len(all_records)} municipality-years")
    if all_records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(PERSONNEL_DDL))
            conn.execute(
                _upsert_query(
                    "municipal_personnel",
                    ["dor_code", "municipality", "fiscal_year",
                     "total_employees", "total_salaries_wages"],
                    ["dor_code", "fiscal_year"],
                ),
                all_records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_personnel', NULL, :n, 'ok')"
            ), {"n": len(all_records)})

    return len(all_records)


# --------------------------------------------------------------------------
# Schedule A: Special Revenue Funds  (federal + state grants by category)
# --------------------------------------------------------------------------

SPECIAL_REV_FUNDS_DDL = """
    CREATE TABLE IF NOT EXISTS municipal_special_rev_funds (
        id                          SERIAL PRIMARY KEY,
        dor_code                    INTEGER NOT NULL,
        municipality                TEXT,
        fiscal_year                 INTEGER NOT NULL,
        amount_type                 TEXT NOT NULL DEFAULT 'Expenditures',
        fed_general_govt            BIGINT,
        fed_public_safety           BIGINT,
        fed_public_works            BIGINT,
        fed_education               BIGINT,
        fed_fema                    BIGINT,
        fed_culture_recreation      BIGINT,
        fed_cdbg                    BIGINT,
        fed_hud_other               BIGINT,
        fed_other                   BIGINT,
        state_general_govt          BIGINT,
        state_public_safety         BIGINT,
        state_public_works          BIGINT,
        state_education             BIGINT,
        state_culture_recreation    BIGINT,
        state_other                 BIGINT,
        total_revenues              BIGINT,
        loaded_at                   TIMESTAMP DEFAULT NOW(),
        UNIQUE (dor_code, fiscal_year, amount_type)
    );
    CREATE INDEX IF NOT EXISTS idx_msrf_fy_code ON municipal_special_rev_funds (fiscal_year, dor_code);
"""

_SPECIAL_REV_FUNDS_COL_MAP = {
    "fed_general_govt":       "Federal General Government Grants",
    "fed_public_safety":      "Federal Public Safety Grants",
    "fed_public_works":       "Federal Public Works Grants",
    "fed_education":          "Federal Education Grants",
    "fed_fema":               "Federal Emergency Management Agency",
    "fed_culture_recreation": "Federal Culture and Recreation Grants",
    "fed_cdbg":               "Federal Community Development Block Grants",
    "fed_hud_other":          "Other Federal Housing and Urban Development Grants",
    "fed_other":              "Other Federal Grants",
}


def load_special_rev_funds(start_year: int = SCHED_A_START_YEAR,
                           end_year: int = SCHED_A_END_YEAR,
                           amount_type: str = "Expenditures") -> int:
    rdreport = "ScheduleA.Special_Rev_Funds.SpecialRevFunds"

    print(f"[dls_loader] special_rev_funds ({amount_type}): loading FY{start_year}-{end_year}...")

    # Special_Rev_Funds form uses DOR codes; survey showed xtFedGrants as export table
    # Try both known export table IDs
    for table_id in ["xtFedGrants", "tblTaxlevybyclass"]:
        munis = _get_form_munis(rdreport, "iclMuni")
        if munis:
            break
    if not munis:
        print("[dls_loader] special_rev_funds: no municipalities found in form")
        return 0
    print(f"[dls_loader] special_rev_funds: {len(munis)} municipalities")

    all_records: list[dict] = []
    for year in range(start_year, end_year + 1):
        df = _rdcache_export(rdreport, "xtFedGrants", "SpecialRevFunds", munis, "iclMuni",
                             year, amount_type)
        if df is None:
            continue
        df.columns = [c.strip() for c in df.columns]

        for _, row in df.iterrows():
            dor = _to_int(row.get("DOR Code"))
            if dor is None:
                continue
            rec: dict = {
                "dor_code":    dor,
                "municipality": _to_str(row.get("Municipality")),
                "fiscal_year": _to_int(row.get("Fiscal Year")) or year,
                "amount_type": amount_type,
            }
            for db_col, src_col in _SPECIAL_REV_FUNDS_COL_MAP.items():
                rec[db_col] = _to_int(row.get(src_col))
            # Remaining columns not in the map → ignore; compute total from row
            rec["total_revenues"] = _to_int(row.get("Total Revenues") or row.get("Total"))
            # Skip state columns for now (map only what survey confirmed)
            for col in ["state_general_govt", "state_public_safety", "state_public_works",
                        "state_education", "state_culture_recreation", "state_other"]:
                rec[col] = None
            all_records.append(rec)

        print(f"[dls_loader] special_rev_funds FY{year}: {len(df)} rows")

    print(f"[dls_loader] special_rev_funds: total {len(all_records)} municipality-years")
    if all_records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(SPECIAL_REV_FUNDS_DDL))
            conn.execute(
                _upsert_query(
                    "municipal_special_rev_funds",
                    ["dor_code", "municipality", "fiscal_year", "amount_type",
                     *list(_SPECIAL_REV_FUNDS_COL_MAP.keys()),
                     "state_general_govt", "state_public_safety", "state_public_works",
                     "state_education", "state_culture_recreation", "state_other",
                     "total_revenues"],
                    ["dor_code", "fiscal_year", "amount_type"],
                ),
                all_records,
            )
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('municipal_special_rev_funds', NULL, :n, 'ok')"
            ), {"n": len(all_records)})

    return len(all_records)


# ---- Schedule A loaders master list ----------------------------------------

SCHEDULE_A_LOADERS = {
    "health_insurance":   load_health_insurance,
    "trust_funds":        load_trust_funds,
    "enterprise_funds":   load_enterprise_funds,
    "capital_projects":   load_capital_projects,
    "personnel":          load_personnel,
    "special_rev_funds":  load_special_rev_funds,
}

ALL_LOADERS = (list(REPORT_DEFS) + ["general_fund"] + list(SCHEDULE_A_LOADERS))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MA DLS Gateway master loader")
    parser.add_argument("--load", choices=ALL_LOADERS + ["all", "schedule_a"],
                        help="Load one report into the database; 'all' loads everything; "
                             "'schedule_a' loads all Schedule A sub-reports")
    parser.add_argument("--start-year", type=int, default=None,
                        help=f"Earliest fiscal year to load (default varies by report)")
    parser.add_argument("--end-year", type=int, default=None,
                        help=f"Latest fiscal year to load (default varies by report)")
    parser.add_argument("--max-workers", type=int, default=10,
                        help="Concurrent requests for general_fund (default 10)")
    parser.add_argument("--amount-type", default="Expenditures",
                        choices=["Expenditures", "Revenues"],
                        help="Amount type for trust_funds / enterprise_funds / capital_projects")
    parser.add_argument("--survey", action="store_true",
                        help="Survey all known DLS report pages for year range/export structure (no DB writes)")
    parser.add_argument("--out", default="/tmp/dls_survey.json", help="Output path for --survey results")
    args = parser.parse_args()

    sy = args.start_year
    ey = args.end_year

    if args.survey:
        survey_all(args.out)
    elif args.load == "general_fund":
        load_general_fund(start_year=sy, end_year=ey, max_workers=args.max_workers)
    elif args.load in SCHEDULE_A_LOADERS:
        fn = SCHEDULE_A_LOADERS[args.load]
        import inspect
        sig = inspect.signature(fn)
        kw: dict = {}
        if "start_year" in sig.parameters:
            kw["start_year"] = sy or SCHED_A_START_YEAR
        if "end_year" in sig.parameters:
            kw["end_year"] = ey or SCHED_A_END_YEAR
        if "amount_type" in sig.parameters:
            kw["amount_type"] = args.amount_type
        fn(**kw)
    elif args.load == "schedule_a":
        for slug, fn in SCHEDULE_A_LOADERS.items():
            print(f"\n[dls_loader] === {slug} ===")
            import inspect
            sig = inspect.signature(fn)
            kw = {}
            if "start_year" in sig.parameters:
                kw["start_year"] = sy or SCHED_A_START_YEAR
            if "end_year" in sig.parameters:
                kw["end_year"] = ey or SCHED_A_END_YEAR
            fn(**kw)
    elif args.load == "all":
        for slug in REPORT_DEFS:
            load_report(slug, start_year=sy, end_year=ey)
        load_general_fund(start_year=sy, end_year=ey, max_workers=args.max_workers)
        for slug, fn in SCHEDULE_A_LOADERS.items():
            print(f"\n[dls_loader] === {slug} ===")
            import inspect
            sig = inspect.signature(fn)
            kw = {}
            if "start_year" in sig.parameters:
                kw["start_year"] = sy or SCHED_A_START_YEAR
            if "end_year" in sig.parameters:
                kw["end_year"] = ey or SCHED_A_END_YEAR
            fn(**kw)
    elif args.load:
        load_report(args.load, start_year=sy, end_year=ey)
    else:
        parser.print_help()
