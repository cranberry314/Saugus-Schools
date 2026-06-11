"""
Migration: add municipal_free_cash table to an existing ma_school_data database.

Run once:
    python db/migrate_add_free_cash.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
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
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Migration complete — municipal_free_cash created (or already existed).")
