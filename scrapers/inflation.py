"""
Loads FRED CPI inflation data from Files/FPCPITOTLZGUSA.csv into inflation_cpi table.

Series: FPCPITOTLZGUSA — Consumer Price Index: Total, All Items for the United States
        (Annual % change)
Source file already downloaded from FRED; no network request needed.

Run: python scrapers/inflation.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from sqlalchemy import text
from config import get_engine

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                        "Files", "FPCPITOTLZGUSA.csv")

UPSERT = text("""
    INSERT INTO inflation_cpi (year, cpi_pct_change)
    VALUES (:year, :cpi_pct_change)
    ON CONFLICT (year) DO UPDATE SET
        cpi_pct_change = EXCLUDED.cpi_pct_change,
        loaded_at      = NOW()
""")


def run():
    engine = get_engine()
    if not os.path.exists(CSV_PATH):
        print(f"[inflation] CSV not found: {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    print(f"[inflation] Read {len(df)} rows from {os.path.basename(CSV_PATH)}")

    records = []
    for _, row in df.iterrows():
        date_str = str(row.get("observation_date", "") or "").strip()
        val_str  = str(row.get("FPCPITOTLZGUSA", "") or "").strip()
        if not date_str or not val_str:
            continue
        try:
            year = int(date_str[:4])
            val  = float(val_str)
        except ValueError:
            continue
        records.append({"year": year, "cpi_pct_change": val})

    if not records:
        print("[inflation] No records parsed.")
        return

    with engine.begin() as conn:
        conn.execute(UPSERT, records)
        conn.execute(text("""
            INSERT INTO ingest_log (source, rows_loaded, status)
            VALUES ('inflation_cpi', :n, 'ok')
        """), {"n": len(records)})

    print(f"[inflation] Loaded {len(records)} rows "
          f"(years {min(r['year'] for r in records)}–{max(r['year'] for r in records)})")


if __name__ == "__main__":
    run()
