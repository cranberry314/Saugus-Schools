"""
Migration: create municipal_crashes table.
Run once: python db/migrate_add_crashes.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS municipal_crashes (
    id               SERIAL PRIMARY KEY,
    city_town_name   VARCHAR(100)  NOT NULL,
    year             INTEGER       NOT NULL,
    total_crashes    INTEGER,
    fatal_crashes    INTEGER,
    injury_crashes   INTEGER,
    pdo_crashes      INTEGER,
    total_fatalities INTEGER,
    total_injuries   INTEGER,
    loaded_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (city_town_name, year)
);

CREATE INDEX IF NOT EXISTS idx_municipal_crashes_town ON municipal_crashes(city_town_name);
CREATE INDEX IF NOT EXISTS idx_municipal_crashes_year ON municipal_crashes(year);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("municipal_crashes table and indexes created (or already existed).")
