"""
Creates the ma_school_data database and applies the schema.
Run once: python db/init_db.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import text
from config import get_engine, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.sql")


def create_database_if_not_exists():
    """Creates the database if it doesn't exist yet."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname="postgres",   # connect to default DB first
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_DB,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{POSTGRES_DB}"')
        print(f"[init_db] Created database: {POSTGRES_DB}")
    else:
        print(f"[init_db] Database already exists: {POSTGRES_DB}")
    cur.close()
    conn.close()


def apply_schema():
    """Applies schema.sql to the database (idempotent — uses IF NOT EXISTS)."""
    with open(SCHEMA_FILE, "r") as f:
        sql = f.read()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"[init_db] Schema applied from {SCHEMA_FILE}")


if __name__ == "__main__":
    create_database_if_not_exists()
    apply_schema()
    print("[init_db] Done.")
