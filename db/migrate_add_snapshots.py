"""
Migration: add analysis snapshot tables to an existing ma_school_data database.

Run once:
    python db/migrate_add_snapshots.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS throughout.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from config import get_engine

DDL = """
CREATE TABLE IF NOT EXISTS analysis_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ DEFAULT NOW(),
    data_vintage_fy INTEGER,
    data_vintage_sy INTEGER,
    n_peer_pool     INTEGER,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS computed_peer_groups (
    id           SERIAL PRIMARY KEY,
    run_id       INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    method       VARCHAR(20)  NOT NULL,
    municipality VARCHAR(100) NOT NULL,
    ed_pct       NUMERIC(6,2),
    mahal_dist   NUMERIC(10,6),
    rank_in_set  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_cpg_run_method ON computed_peer_groups (run_id, method);

CREATE TABLE IF NOT EXISTS computed_metrics (
    id           SERIAL PRIMARY KEY,
    run_id       INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    metric       VARCHAR(60)  NOT NULL,
    fiscal_year  INTEGER,
    school_year  INTEGER,
    value        NUMERIC(12,4),
    notes        TEXT
);
CREATE INDEX IF NOT EXISTS idx_cm_run_metric ON computed_metrics (run_id, metric);

CREATE TABLE IF NOT EXISTS computed_feature_importance (
    id          SERIAL PRIMARY KEY,
    run_id      INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    rank        INTEGER     NOT NULL,
    feature     VARCHAR(60) NOT NULL,
    importance  NUMERIC(10,6)
);
"""

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Migration complete — snapshot tables created (or already existed).")
