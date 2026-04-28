"""
Database and API configuration for the Schools project.
Copy this file to config.py and fill in your values.
config.py is gitignored — never commit credentials.
"""
import os
from sqlalchemy import create_engine

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT",     "5432")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "your_db_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_db_password")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "ma_school_data")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

def get_engine():
    return create_engine(DATABASE_URL)


# MA DOE data sources
SOCRATA_BASE     = "https://educationtocareer.data.mass.gov/resource"
SOCRATA_FALLBACK = "https://ma-eoe.data.socrata.com/resource"

MCAS_DATASET_ID  = "i9w6-niyt"   # MCAS Achievement Results (Next Gen, 2017–present)

DOE_EDU       = "https://www.doe.mass.edu"
DOE_GOV       = "https://doe.mass.gov"
DOE_BASE      = DOE_EDU
PROFILES_BASE = "https://profiles.doe.mass.edu"

SOCRATA_PAGE_SIZE = 50_000
