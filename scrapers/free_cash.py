"""
Scrapes Certified Free Cash data for all MA municipalities from the MA DLS
(Division of Local Services) Gateway.

Source: https://dls-gw.dor.state.ma.us/reports/rdPage.aspx
        ?rdReport=Dashboard.Cat_1_Reports.CertifiedFreeCashBudget351

Single Excel export covering all 351 municipalities and all requested fiscal
years (FY2015–present available as of 2026) — one POST request, follows the
redirect to the generated .xlsx.

Columns: DOR Code, Municipality, Fiscal Year, Date Certified,
         Certified Free Cash as of 7/1, Operating Budget Prior Year,
         Certified Free Cash as a % of the Budget
→ municipal_free_cash

Run: python scrapers/free_cash.py [--start-year 2015] [--end-year 2026]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import argparse
import requests
import pandas as pd
from sqlalchemy import text
from config import get_engine

BASE_URL = "https://dls-gw.dor.state.ma.us/reports/rdPage.aspx"
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2026

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://dls-gw.dor.state.ma.us/",
}

EXPORT_PARAMS = {
    "rdReport":           "Dashboard.Cat_1_Reports.CertifiedFreeCashBudget351",
    "rdReportFormat":     "NativeExcel",
    "rdExportTableID":    "TblCFC_PerBudg",
    "rdExportFilename":   "FreeCash",
    "rdShowGridlines":    "True",
    "rdExcelOutputFormat":"Excel2007",
}

UPSERT = text("""
    INSERT INTO municipal_free_cash
        (fiscal_year, dor_code, municipality, date_certified,
         cert_free_cash, operating_budget, free_cash_pct)
    VALUES
        (:fiscal_year, :dor_code, :municipality, :date_certified,
         :cert_free_cash, :operating_budget, :free_cash_pct)
    ON CONFLICT (fiscal_year, dor_code) DO UPDATE SET
        municipality     = EXCLUDED.municipality,
        date_certified   = EXCLUDED.date_certified,
        cert_free_cash   = EXCLUDED.cert_free_cash,
        operating_budget = EXCLUDED.operating_budget,
        free_cash_pct    = EXCLUDED.free_cash_pct,
        loaded_at        = NOW()
""")


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


def _download_excel(session: requests.Session, years: list[int]) -> bytes | None:
    url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in EXPORT_PARAMS.items())
    post_data = {"iclYear": [str(y) for y in years]}
    r = session.post(url, data=post_data, timeout=120, allow_redirects=True)
    r.raise_for_status()
    if r.content[:2] != b"PK":
        print(f"[free_cash] WARNING: response is not Excel "
              f"({len(r.content)} bytes, starts: {r.content[:40]})")
        return None
    return r.content


def run(start_year: int = DEFAULT_START_YEAR, end_year: int = DEFAULT_END_YEAR):
    years = list(range(start_year, end_year + 1))
    print(f"[free_cash] Fetching certified free cash for FY{start_year}-{end_year} "
          f"(all municipalities)...")

    session = requests.Session()
    session.headers.update(HEADERS)
    raw = _download_excel(session, years)
    if raw is None:
        print("[free_cash] FAILED to download report")
        return 0

    print(f"[free_cash] Downloaded {len(raw):,} bytes")

    df = pd.read_excel(io.BytesIO(raw), dtype=str)
    df.columns = [c.strip() for c in df.columns]
    print(f"[free_cash] Columns: {df.columns.tolist()}")

    records = []
    for _, row in df.iterrows():
        dor = _to_int(row.get("DOR Code"))
        fy = _to_int(row.get("Fiscal Year"))
        if dor is None or fy is None:
            continue
        date_certified = pd.to_datetime(row.get("Date Certified"), errors="coerce")
        records.append({
            "fiscal_year":      fy,
            "dor_code":         dor,
            "municipality":     str(row.get("Municipality") or "").strip() or None,
            "date_certified":   date_certified.date() if pd.notna(date_certified) else None,
            "cert_free_cash":   _to_int(row.get("Certified Free Cash as of 7/1")),
            "operating_budget": _to_float(row.get("Operating Budget Prior Year")),
            "free_cash_pct":    _to_float(row.get("Certified Free Cash as a % of the Budget")),
        })

    print(f"[free_cash] Parsed {len(records)} municipality-years")

    if records:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(UPSERT, records)
            conn.execute(text("""
                INSERT INTO ingest_log (source, school_year, rows_loaded, status)
                VALUES ('municipal_free_cash', NULL, :n, 'ok')
            """), {"n": len(records)})
        print(f"[free_cash] Done. {len(records)} rows upserted "
              f"(years: {sorted(set(r['fiscal_year'] for r in records))})")

    return len(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MA municipal certified free cash from DLS Gateway")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR,
                        help=f"Earliest fiscal year to load (default {DEFAULT_START_YEAR})")
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR,
                        help=f"Latest fiscal year to load (default {DEFAULT_END_YEAR})")
    args = parser.parse_args()
    run(start_year=args.start_year, end_year=args.end_year)
