"""
MA DLS Gateway — Schedule A General Fund Scraper
=================================================
Scrapes Schedule A revenues and expenditures for all specified MA municipalities
from the DLS Gateway (dls-gw.dor.state.ma.us), FY2010–2025.

Source: MA Division of Local Services, Data Analytics and Resources Bureau
        https://dlsgateway.dor.state.ma.us/reports/rdPage.aspx?rdReport=ScheduleA.GeneralFund

Data tables created:
  municipal_revenues      — general fund revenues by category
  municipal_expenditures  — general fund expenditures by category

Run:
  python scrapers/municipal_finances.py              # Saugus + similar-pop towns
  python scrapers/municipal_finances.py --all        # all 351 MA municipalities
  python scrapers/municipal_finances.py --muni 262   # single DOR code
"""
import sys, os, io, time, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text
from config import get_engine

BASE_URL   = "https://dls-gw.dor.state.ma.us/reports/rdPage.aspx"
FORM_URL   = "https://dlsgateway.dor.state.ma.us/reports/rdPage.aspx?rdReport=ScheduleA.GeneralFund&rdSubReport=True"
YEARS      = list(range(2010, 2026))   # FY2010–FY2025
DELAY      = 0.5                       # seconds between requests

# Saugus DOR code + similar-population towns (~20k-40k residents, 2023 ACS)
SAUGUS_AND_PEERS = [
    (262, "Saugus"),
    (1,   "Abington"),
    (5,   "Agawam"),
    (14,  "Amesbury"),
    (25,  "Attleboro"),
    (27,  "Barnstable"),
    (38,  "Billerica"),
    (47,  "Bourne"),
    (48,  "Braintree"),
    (52,  "Burlington"),
    (68,  "Canton"),
    (71,  "Chelmsford"),
    (82,  "Danvers"),
    (84,  "Dartmouth"),
    (87,  "Dedham"),
    (89,  "Dennis"),
    (96,  "Dracut"),
    (102, "Easton"),
    (104, "Everett"),  # added - comparable size
    (111, "Falmouth"),
    (124, "Gardner"),
    (132, "Grafton"),
    (136, "Hanover"),
    (138, "Harwich"),
    (150, "Holden"),
    (164, "Hudson"),
    (172, "Ipswich"),
    (181, "Leicester"),
    (184, "Leominster"),
    (186, "Longmeadow"),
    (188, "Ludlow"),
    (193, "Mansfield"),
    (195, "Marblehead"),
    (197, "Marshfield"),
    (200, "Mashpee"),
    (203, "Medford"),  # slightly larger but nearby
    (211, "Milford"),
    (215, "Milton"),
    (218, "Natick"),
    (226, "Northampton"),
    (238, "Pembroke"),
    (243, "Plymouth"),
    (251, "Randolph"),
    (257, "Rockland"),
    (266, "Scituate"),
    (273, "Shrewsbury"),
    (277, "Somerset"),
    (278, "Somerville"),  # larger but for context
    (282, "Stoughton"),
    (289, "Swansea"),
    (294, "Tewksbury"),
    (299, "Wakefield"),
    (302, "Walpole"),
    (307, "Watertown"),
    (311, "Westfield"),
    (313, "Westford"),
    (318, "Weymouth"),
    (322, "Winchester"),
    (324, "Winthrop"),
    (325, "Woburn"),
    (328, "Yarmouth"),
]

CREATE_REVENUES = """
CREATE TABLE IF NOT EXISTS municipal_revenues (
    id                      SERIAL PRIMARY KEY,
    dor_code                INTEGER NOT NULL,
    municipality            TEXT    NOT NULL,
    fiscal_year             INTEGER NOT NULL,
    taxes                   BIGINT,
    service_charges         BIGINT,
    licenses_permits        BIGINT,
    federal_revenue         BIGINT,
    state_revenue           BIGINT,
    intergovernmental       BIGINT,
    special_assessments     BIGINT,
    fines_forfeitures       BIGINT,
    miscellaneous           BIGINT,
    other_financing         BIGINT,
    transfers               BIGINT,
    total_revenues          BIGINT,
    loaded_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (dor_code, fiscal_year)
)
"""

CREATE_EXPENDITURES = """
CREATE TABLE IF NOT EXISTS municipal_expenditures (
    id                      SERIAL PRIMARY KEY,
    dor_code                INTEGER NOT NULL,
    municipality            TEXT    NOT NULL,
    fiscal_year             INTEGER NOT NULL,
    general_government      BIGINT,
    public_safety           BIGINT,
    education               BIGINT,
    public_works            BIGINT,
    human_services          BIGINT,
    culture_recreation      BIGINT,
    fixed_costs             BIGINT,
    intergovernmental       BIGINT,
    other_expenditures      BIGINT,
    debt_service            BIGINT,
    total_expenditures      BIGINT,
    loaded_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (dor_code, fiscal_year)
)
"""

UPSERT_REVENUES = """
INSERT INTO municipal_revenues
    (dor_code, municipality, fiscal_year, taxes, service_charges, licenses_permits,
     federal_revenue, state_revenue, intergovernmental, special_assessments,
     fines_forfeitures, miscellaneous, other_financing, transfers, total_revenues)
VALUES
    (:dor_code, :municipality, :fiscal_year, :taxes, :service_charges, :licenses_permits,
     :federal_revenue, :state_revenue, :intergovernmental, :special_assessments,
     :fines_forfeitures, :miscellaneous, :other_financing, :transfers, :total_revenues)
ON CONFLICT (dor_code, fiscal_year) DO UPDATE SET
    municipality       = EXCLUDED.municipality,
    taxes              = COALESCE(EXCLUDED.taxes,              municipal_revenues.taxes),
    service_charges    = COALESCE(EXCLUDED.service_charges,    municipal_revenues.service_charges),
    licenses_permits   = COALESCE(EXCLUDED.licenses_permits,   municipal_revenues.licenses_permits),
    federal_revenue    = COALESCE(EXCLUDED.federal_revenue,    municipal_revenues.federal_revenue),
    state_revenue      = COALESCE(EXCLUDED.state_revenue,      municipal_revenues.state_revenue),
    intergovernmental  = COALESCE(EXCLUDED.intergovernmental,  municipal_revenues.intergovernmental),
    special_assessments= COALESCE(EXCLUDED.special_assessments,municipal_revenues.special_assessments),
    fines_forfeitures  = COALESCE(EXCLUDED.fines_forfeitures,  municipal_revenues.fines_forfeitures),
    miscellaneous      = COALESCE(EXCLUDED.miscellaneous,      municipal_revenues.miscellaneous),
    other_financing    = COALESCE(EXCLUDED.other_financing,    municipal_revenues.other_financing),
    transfers          = COALESCE(EXCLUDED.transfers,          municipal_revenues.transfers),
    total_revenues     = COALESCE(EXCLUDED.total_revenues,     municipal_revenues.total_revenues),
    loaded_at          = NOW()
"""

UPSERT_EXPENDITURES = """
INSERT INTO municipal_expenditures
    (dor_code, municipality, fiscal_year, general_government, public_safety, education,
     public_works, human_services, culture_recreation, fixed_costs,
     intergovernmental, other_expenditures, debt_service, total_expenditures)
VALUES
    (:dor_code, :municipality, :fiscal_year, :general_government, :public_safety, :education,
     :public_works, :human_services, :culture_recreation, :fixed_costs,
     :intergovernmental, :other_expenditures, :debt_service, :total_expenditures)
ON CONFLICT (dor_code, fiscal_year) DO UPDATE SET
    municipality        = EXCLUDED.municipality,
    general_government  = COALESCE(EXCLUDED.general_government, municipal_expenditures.general_government),
    public_safety       = COALESCE(EXCLUDED.public_safety,      municipal_expenditures.public_safety),
    education           = COALESCE(EXCLUDED.education,          municipal_expenditures.education),
    public_works        = COALESCE(EXCLUDED.public_works,       municipal_expenditures.public_works),
    human_services      = COALESCE(EXCLUDED.human_services,     municipal_expenditures.human_services),
    culture_recreation  = COALESCE(EXCLUDED.culture_recreation, municipal_expenditures.culture_recreation),
    fixed_costs         = COALESCE(EXCLUDED.fixed_costs,        municipal_expenditures.fixed_costs),
    intergovernmental   = COALESCE(EXCLUDED.intergovernmental,  municipal_expenditures.intergovernmental),
    other_expenditures  = COALESCE(EXCLUDED.other_expenditures, municipal_expenditures.other_expenditures),
    debt_service        = COALESCE(EXCLUDED.debt_service,       municipal_expenditures.debt_service),
    total_expenditures  = COALESCE(EXCLUDED.total_expenditures, municipal_expenditures.total_expenditures),
    loaded_at           = NOW()
"""


def _to_int(val):
    try:
        v = float(val)
        return int(v) if not pd.isna(v) else None
    except (TypeError, ValueError):
        return None


def _col(df, *candidates):
    """Return first matching column name (case-insensitive substring)."""
    lower_cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        for lc, orig in lower_cols.items():
            if cand.lower() in lc:
                return orig
    return None


def fetch_excel(sess, muni_code: int, year: int, amount_type: str) -> pd.DataFrame | None:
    params = {
        "rdReport":            "ScheduleA.GeneralFund",
        "rdSubReport":         "True",
        "rdReportFormat":      "NativeExcel",
        "rdExportTableID":     "xtGenFund",
        "rdExportFilename":    f"GenFund{amount_type}{year}",
        "rdShowGridlines":     "True",
        "rdExcelOutputFormat": "Excel2007",
        "iclMuni":             str(muni_code).zfill(3),
        "islYear":             str(year),
        "islAmountType":       amount_type,
    }
    try:
        r = sess.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "spreadsheet" not in ct and "excel" not in ct and r.content[:2] != b"PK":
            return None
        return pd.read_excel(io.BytesIO(r.content))
    except Exception as e:
        print(f"    fetch error {muni_code} {year} {amount_type}: {e}")
        return None


def _pick_row(df: pd.DataFrame):
    """Return the data row: prefer non-Totals row; fall back to Totals row.
    Returns None if the frame is empty or all totals are zero (no data)."""
    df = df.dropna(how="all").copy()
    if df.empty:
        return None
    non_total = df[~df.iloc[:, 0].astype(str).str.lower().str.contains("total", na=False)]
    row = non_total.iloc[0] if not non_total.empty else df.iloc[0]
    # Skip rows where everything numeric is zero or NaN (no data published)
    num_vals = pd.to_numeric(row, errors="coerce").dropna()
    if num_vals.empty or (num_vals == 0).all():
        return None
    return row


def parse_revenues(df: pd.DataFrame, muni_code: int, year: int) -> dict | None:
    row = _pick_row(df)
    if row is None:
        return None
    df = df.dropna(how="all")
    muni_name = str(row.get(_col(df, "Municipality") or df.columns[1], "")).strip()
    return {
        "dor_code":          muni_code,
        "municipality":      muni_name,
        "fiscal_year":       year,
        "taxes":             _to_int(row.get(_col(df, "Tax"))),
        "service_charges":   _to_int(row.get(_col(df, "Service"))),
        "licenses_permits":  _to_int(row.get(_col(df, "License"))),
        "federal_revenue":   _to_int(row.get(_col(df, "Federal"))),
        "state_revenue":     _to_int(row.get(_col(df, "State"))),
        "intergovernmental": _to_int(row.get(_col(df, "Governments", "Intergovernmental"))),
        "special_assessments":_to_int(row.get(_col(df, "Special"))),
        "fines_forfeitures": _to_int(row.get(_col(df, "Fine"))),
        "miscellaneous":     _to_int(row.get(_col(df, "Misc"))),
        "other_financing":   _to_int(row.get(_col(df, "Other Financing", "Financing"))),
        "transfers":         _to_int(row.get(_col(df, "Transfer"))),
        "total_revenues":    _to_int(row.get(_col(df, "Total"))),
    }


def parse_expenditures(df: pd.DataFrame, muni_code: int, year: int) -> dict | None:
    row = _pick_row(df)
    if row is None:
        return None
    df = df.dropna(how="all")
    muni_name = str(row.get(_col(df, "Municipality") or df.columns[1], "")).strip()
    return {
        "dor_code":           muni_code,
        "municipality":       muni_name,
        "fiscal_year":        year,
        "general_government": _to_int(row.get(_col(df, "General Gov"))),
        "public_safety":      _to_int(row.get(_col(df, "Public Safety"))),
        "education":          _to_int(row.get(_col(df, "Education"))),
        "public_works":       _to_int(row.get(_col(df, "Public Works"))),
        "human_services":     _to_int(row.get(_col(df, "Human"))),
        "culture_recreation": _to_int(row.get(_col(df, "Culture"))),
        "fixed_costs":        _to_int(row.get(_col(df, "Fixed"))),
        "intergovernmental":  _to_int(row.get(_col(df, "Intergov"))),
        "other_expenditures": _to_int(row.get(_col(df, "Other Exp"))),
        "debt_service":       _to_int(row.get(_col(df, "Debt"))),
        "total_expenditures": _to_int(row.get(_col(df, "Total"))),
    }


def run(muni_list: list[tuple[int, str]] = SAUGUS_AND_PEERS, years: list[int] = YEARS):
    engine = get_engine()

    print("[municipal_finances] Creating tables ...")
    with engine.connect() as conn:
        conn.execute(text(CREATE_REVENUES))
        conn.execute(text(CREATE_EXPENDITURES))
        conn.commit()

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})

    # Load all muni codes from the form once (validates our list)
    r0 = sess.get(FORM_URL, timeout=15)
    soup = BeautifulSoup(r0.text, "html.parser")
    valid_codes = {int(i.get("value","0")) for i in soup.find_all("input", {"name":"iclMuni"}) if i.get("value","").isdigit()}
    print(f"[municipal_finances] DLS Gateway has {len(valid_codes)} municipalities")

    total_rev = total_exp = 0

    for dor_code, name in muni_list:
        if dor_code not in valid_codes:
            print(f"  SKIP {name} ({dor_code}) — not in DLS Gateway")
            continue

        print(f"\n  {name} ({dor_code}) ...")
        muni_rev = muni_exp = 0

        with engine.connect() as conn:
            for year in years:
                # Revenues
                df_rev = fetch_excel(sess, dor_code, year, "Revenues")
                time.sleep(DELAY)
                if df_rev is not None:
                    rec = parse_revenues(df_rev, dor_code, year)
                    if rec:
                        conn.execute(text(UPSERT_REVENUES), rec)
                        muni_rev += 1

                # Expenditures
                df_exp = fetch_excel(sess, dor_code, year, "Expenditures")
                time.sleep(DELAY)
                if df_exp is not None:
                    rec = parse_expenditures(df_exp, dor_code, year)
                    if rec:
                        conn.execute(text(UPSERT_EXPENDITURES), rec)
                        muni_exp += 1

            conn.commit()

        print(f"    revenues: {muni_rev}  expenditures: {muni_exp} rows")
        total_rev += muni_rev
        total_exp += muni_exp

    print(f"\n[municipal_finances] Done — {total_rev} revenue rows, {total_exp} expenditure rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape MA DLS Schedule A municipal finances")
    parser.add_argument("--all",  action="store_true", help="Scrape all 351 MA municipalities")
    parser.add_argument("--muni", type=int, help="Single DOR municipality code")
    args = parser.parse_args()

    if args.all:
        # Fetch full list from form
        sess_tmp = requests.Session()
        sess_tmp.headers.update({"User-Agent": "Mozilla/5.0"})
        r_tmp = sess_tmp.get(FORM_URL, timeout=15)
        soup_tmp = BeautifulSoup(r_tmp.text, "html.parser")
        all_munis = []
        for inp in soup_tmp.find_all("input", {"name": "iclMuni"}):
            code = int(inp.get("value", 0))
            span = inp.find_next_sibling("span")
            name = span.text.strip() if span else str(code)
            all_munis.append((code, name))
        run(muni_list=all_munis)
    elif args.muni:
        run(muni_list=[(args.muni, f"Code_{args.muni}")])
    else:
        run()
