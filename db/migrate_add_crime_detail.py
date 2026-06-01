"""
Migration: add Part I offense sub-type columns to municipal_crime.

Adds robbery, burglary, larceny, motor_vehicle_theft, arson — the columns the
Beyond 2020 scraper never populated.  Also extends the year range back to 2010
via the FBI UCR scraper (scrapers/fbi_crime.py).

Run once:
    python db/migrate_add_crime_detail.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
ALTER TABLE municipal_crime
    ADD COLUMN IF NOT EXISTS robbery             INTEGER,
    ADD COLUMN IF NOT EXISTS burglary            INTEGER,
    ADD COLUMN IF NOT EXISTS larceny             INTEGER,
    ADD COLUMN IF NOT EXISTS motor_vehicle_theft INTEGER,
    ADD COLUMN IF NOT EXISTS arson               INTEGER,
    ADD COLUMN IF NOT EXISTS data_source         VARCHAR(50) DEFAULT 'beyond2020';

CREATE INDEX IF NOT EXISTS idx_municipal_crime_source
    ON municipal_crime(data_source);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("municipal_crime: new columns added (or already existed).")
    print("Next step: python scrapers/fbi_crime.py --year-start 2010")
