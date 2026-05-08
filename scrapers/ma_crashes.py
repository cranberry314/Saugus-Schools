"""
Scrapes municipal crash statistics from the MassDOT IMPACT Open Data Platform.

Source: gis.crashdata.dot.mass.gov — MassDOT statewide open crash data
Coverage: all ~350 MA municipalities, 2021–2024.

Metrics per municipality per year:
  - total_crashes, fatal_crashes, injury_crashes, pdo_crashes
  - total_fatalities (sum of NUMB_FATAL_INJR)
  - total_injuries  (sum of NUMB_NONFATAL_INJR)

Table populated: municipal_crashes

Run:
    python scrapers/ma_crashes.py           # all years
    python scrapers/ma_crashes.py --year 2024
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import json
import argparse
import requests
from sqlalchemy import text
from config import get_engine

GIS_BASE = "https://gis.crashdata.dot.mass.gov/arcgis/rest/services/MassDOT"
HEADERS  = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

YEAR_SERVICES = {
    2021: "MASSDOT_ODP_OPEN_2021",
    2022: "MASSDOT_ODP_OPEN_2022",
    2023: "MASSDOT_ODP_OPEN_2023v",
    2024: "MASSDOT_ODP_OPEN_2024",
}

UPSERT = text("""
    INSERT INTO municipal_crashes
        (city_town_name, year, total_crashes, fatal_crashes, injury_crashes,
         pdo_crashes, total_fatalities, total_injuries)
    VALUES
        (:city_town_name, :year, :total_crashes, :fatal_crashes, :injury_crashes,
         :pdo_crashes, :total_fatalities, :total_injuries)
    ON CONFLICT (city_town_name, year) DO UPDATE SET
        total_crashes   = EXCLUDED.total_crashes,
        fatal_crashes   = EXCLUDED.fatal_crashes,
        injury_crashes  = EXCLUDED.injury_crashes,
        pdo_crashes     = EXCLUDED.pdo_crashes,
        total_fatalities= EXCLUDED.total_fatalities,
        total_injuries  = EXCLUDED.total_injuries,
        loaded_at       = NOW()
""")


def _query_stats(session, svc_name: str, group_field: str, out_stats: list) -> list[dict]:
    url = f"{GIS_BASE}/{svc_name}/FeatureServer/0/query"
    params = {
        "where": "1=1",
        "outStatistics": json.dumps(out_stats),
        "groupByFieldsForStatistics": group_field,
        "resultRecordCount": 500,
        "f": "json",
    }
    r = session.get(url, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
    return [f["attributes"] for f in data.get("features", [])]


def fetch_year(session, year: int) -> list[dict]:
    svc = YEAR_SERVICES[year]
    print(f"[ma_crashes] {year} ({svc})...")

    # Total crashes + fatality/injury counts per city/town
    total_stats = _query_stats(session, svc, "CITY_TOWN_NAME", [
        {"statisticType": "count",  "onStatisticField": "OBJECTID",        "outStatisticFieldName": "total_crashes"},
        {"statisticType": "sum",    "onStatisticField": "NUMB_FATAL_INJR",  "outStatisticFieldName": "total_fatalities"},
        {"statisticType": "sum",    "onStatisticField": "NUMB_NONFATAL_INJR","outStatisticFieldName": "total_injuries"},
    ])

    # Crash counts by severity category per city/town
    severity_stats = _query_stats(session, svc, "CITY_TOWN_NAME,CRASH_SEVERITY_DESCR", [
        {"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "n"},
    ])

    # Build severity index: {city: {severity: count}}
    sev_idx: dict[str, dict[str, int]] = {}
    for row in severity_stats:
        town = (row.get("CITY_TOWN_NAME") or "").strip().title()
        sev   = (row.get("CRASH_SEVERITY_DESCR") or "").lower()
        n     = row.get("n") or 0
        if town not in sev_idx:
            sev_idx[town] = {}
        if "non-fatal" in sev or ("injury" in sev and "fatal" not in sev):
            sev_idx[town]["injury"] = sev_idx[town].get("injury", 0) + n
        elif "fatal" in sev:
            sev_idx[town]["fatal"] = sev_idx[town].get("fatal", 0) + n
        elif "property" in sev or "pdo" in sev or "damage" in sev:
            sev_idx[town]["pdo"] = sev_idx[town].get("pdo", 0) + n

    # Combine into final rows
    rows = []
    for row in total_stats:
        town = (row.get("CITY_TOWN_NAME") or "").strip().title()
        if not town:
            continue
        sev = sev_idx.get(town, {})
        rows.append({
            "city_town_name": town,
            "year":           year,
            "total_crashes":  row.get("total_crashes"),
            "total_fatalities": int(row.get("total_fatalities") or 0),
            "total_injuries":   int(row.get("total_injuries") or 0),
            "fatal_crashes":  sev.get("fatal", 0),
            "injury_crashes": sev.get("injury", 0),
            "pdo_crashes":    sev.get("pdo", 0),
        })

    print(f"  {len(rows)} municipalities, {sum(r['total_crashes'] for r in rows):,} total crashes")
    return rows


def run(engine, years: list[int] | None = None) -> int:
    session = requests.Session()
    target_years = years or sorted(YEAR_SERVICES.keys())
    total_rows = 0

    for year in target_years:
        if year not in YEAR_SERVICES:
            print(f"[ma_crashes] No service defined for year {year}, skipping")
            continue
        rows = fetch_year(session, year)
        if rows:
            with engine.begin() as conn:
                conn.execute(UPSERT, rows)
            total_rows += len(rows)

    if total_rows:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO ingest_log (source, school_year, rows_loaded, status) "
                "VALUES ('ma_crashes', NULL, :n, 'ok')"
            ), {"n": total_rows})

    return total_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Single year to load (2021-2024)")
    args = parser.parse_args()

    years = [args.year] if args.year else None
    engine = get_engine()
    n = run(engine, years=years)
    print(f"[ma_crashes] Done — {n} total rows upserted.")
