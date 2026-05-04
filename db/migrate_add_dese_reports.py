"""
Migration: add DESE state report tables and Census ACS age columns.

Run once:
    python db/migrate_add_dese_reports.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS and ADD COLUMN IF NOT EXISTS.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS district_sat_scores (
    id            SERIAL PRIMARY KEY,
    school_year   INTEGER      NOT NULL,
    district_code VARCHAR(20)  NOT NULL,
    district_name VARCHAR(255),
    tests_taken   INTEGER,
    mean_ebrw     INTEGER,
    mean_math     INTEGER,
    loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (school_year, district_code)
);

CREATE TABLE IF NOT EXISTS district_postsecondary (
    id                   SERIAL PRIMARY KEY,
    school_year          INTEGER      NOT NULL,
    district_code        VARCHAR(20)  NOT NULL,
    district_name        VARCHAR(255),
    grads_n              INTEGER,
    attending_n          INTEGER,
    attending_pct        NUMERIC(6,2),
    private_2yr_pct      NUMERIC(6,2),
    private_4yr_pct      NUMERIC(6,2),
    public_2yr_pct       NUMERIC(6,2),
    public_4yr_pct       NUMERIC(6,2),
    ma_comm_college_pct  NUMERIC(6,2),
    ma_state_univ_pct    NUMERIC(6,2),
    umass_pct            NUMERIC(6,2),
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (school_year, district_code)
);

CREATE TABLE IF NOT EXISTS district_dropout (
    id            SERIAL PRIMARY KEY,
    school_year   INTEGER      NOT NULL,
    district_code VARCHAR(20)  NOT NULL,
    district_name VARCHAR(255),
    enrolled_9_12 INTEGER,
    dropout_n     INTEGER,
    dropout_pct   NUMERIC(6,2),
    gr9_pct       NUMERIC(6,2),
    gr10_pct      NUMERIC(6,2),
    gr11_pct      NUMERIC(6,2),
    gr12_pct      NUMERIC(6,2),
    loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (school_year, district_code)
);

ALTER TABLE municipal_census_acs
    ADD COLUMN IF NOT EXISTS median_age   NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS pop_under18  INTEGER,
    ADD COLUMN IF NOT EXISTS pct_under18  NUMERIC(6,2);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Migration complete — DESE report tables and ACS age columns created.")
