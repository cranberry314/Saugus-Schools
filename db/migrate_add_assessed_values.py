"""
Migration: add municipal_assessed_values table to an existing ma_school_data database.

Run once:
    python db/migrate_add_assessed_values.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS municipal_assessed_values (
    id                   SERIAL PRIMARY KEY,
    fiscal_year          INTEGER      NOT NULL,
    dor_code             INTEGER      NOT NULL,
    municipality         VARCHAR(100),
    res_av               BIGINT,
    open_space_av        BIGINT,
    commercial_av        BIGINT,
    industrial_av        BIGINT,
    personal_property_av BIGINT,
    total_av             BIGINT,
    exempt_av            BIGINT,
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);
CREATE INDEX IF NOT EXISTS idx_mav_fy_code ON municipal_assessed_values (fiscal_year, dor_code);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Migration complete — municipal_assessed_values created (or already existed).")
