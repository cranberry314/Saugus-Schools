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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MA DLS Gateway master loader")
    parser.add_argument("--load", choices=list(REPORT_DEFS) + ["general_fund", "all"],
                        help="Load one registered report (or 'all') into the database")
    parser.add_argument("--start-year", type=int, default=None,
                        help=f"Earliest fiscal year to load (default {DEFAULT_START_YEAR}, "
                             f"or {GENERAL_FUND_START_YEAR} for general_fund)")
    parser.add_argument("--end-year", type=int, default=None,
                        help=f"Latest fiscal year to load (default {DEFAULT_END_YEAR}, "
                             f"or {GENERAL_FUND_END_YEAR} for general_fund)")
    parser.add_argument("--max-workers", type=int, default=10,
                        help="Concurrent requests for general_fund (default 10)")
    parser.add_argument("--survey", action="store_true",
                        help="Survey all known DLS report pages for year range/export structure (no DB writes)")
    parser.add_argument("--out", default="/tmp/dls_survey.json", help="Output path for --survey results")
    args = parser.parse_args()

    if args.survey:
        survey_all(args.out)
    elif args.load == "general_fund":
        load_general_fund(start_year=args.start_year, end_year=args.end_year, max_workers=args.max_workers)
    elif args.load:
        targets = list(REPORT_DEFS) if args.load == "all" else [args.load]
        for slug in targets:
            load_report(slug, start_year=args.start_year, end_year=args.end_year)
    else:
        parser.print_help()
