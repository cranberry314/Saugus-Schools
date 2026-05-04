"""
Migration: add county_unemployment, county_health_rankings tables and
expand municipal_census_acs with new demographic columns.

Run once:
    python db/migrate_add_county_demographics.py

Safe to re-run.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS county_unemployment (
    id                SERIAL PRIMARY KEY,
    state_fips        VARCHAR(2)   NOT NULL,
    county_fips       VARCHAR(5)   NOT NULL,
    county_name       VARCHAR(100),
    year              INTEGER      NOT NULL,
    month             INTEGER      NOT NULL,
    unemployment_rate NUMERIC(5,2),
    unemployed_count  NUMERIC(10,0),
    labor_force       NUMERIC(10,0),
    loaded_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (county_fips, year, month)
);
CREATE INDEX IF NOT EXISTS idx_county_unemp ON county_unemployment (county_fips, year);

CREATE TABLE IF NOT EXISTS county_health_rankings (
    id                            SERIAL PRIMARY KEY,
    ranking_year                  INTEGER      NOT NULL,
    state_fips                    VARCHAR(2),
    county_fips                   VARCHAR(5)   NOT NULL,
    county_name                   VARCHAR(100),
    pct_fair_poor_health          NUMERIC(6,2),
    avg_physically_unhealthy_days NUMERIC(6,2),
    avg_mentally_unhealthy_days   NUMERIC(6,2),
    pct_low_birthweight           NUMERIC(6,2),
    pct_smokers                   NUMERIC(6,2),
    pct_obese                     NUMERIC(6,2),
    pct_physically_inactive       NUMERIC(6,2),
    pct_excessive_drinking        NUMERIC(6,2),
    pct_uninsured                 NUMERIC(6,2),
    pct_children_poverty          NUMERIC(6,2),
    income_ratio                  NUMERIC(8,4),
    pct_children_single_parent    NUMERIC(6,2),
    pct_hs_completed              NUMERIC(6,2),
    pct_some_college              NUMERIC(6,2),
    pct_unemployed                NUMERIC(6,2),
    loaded_at                     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ranking_year, county_fips)
);

ALTER TABLE municipal_census_acs
    ADD COLUMN IF NOT EXISTS unemployment_rate NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS poverty_pct       NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pct_foreign_born  NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pct_divorced      NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS pct_single_parent NUMERIC(5,2);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Migration complete — county tables and ACS demographic columns added.")
