"""
Migration: create municipal_crime table.
Run once: python db/migrate_add_crime.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS municipal_crime (
    id                   SERIAL PRIMARY KEY,
    jurisdiction_slug    VARCHAR(100)  NOT NULL,
    ori_code             VARCHAR(20)   NOT NULL,
    jurisdiction_name    VARCHAR(200),
    year                 INTEGER       NOT NULL,
    total_crimes         INTEGER,
    violent_crimes       INTEGER,
    homicides            INTEGER,
    sexual_assaults      INTEGER,
    aggravated_assaults  INTEGER,
    clearance_rate_pct   NUMERIC(5,2),
    crime_rate_per_100k  NUMERIC(10,2),
    population           INTEGER,
    loaded_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (ori_code, year)
);

CREATE INDEX IF NOT EXISTS idx_municipal_crime_slug ON municipal_crime(jurisdiction_slug);
CREATE INDEX IF NOT EXISTS idx_municipal_crime_year ON municipal_crime(year);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("municipal_crime table and indexes created (or already existed).")
