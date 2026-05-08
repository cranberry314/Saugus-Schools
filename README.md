# Saugus Schools — Budget & Outcomes Analysis

Generates a PDF report comparing Saugus school funding to peer municipalities,
tracking MCAS outcomes, and documenting the education funding gap.

**Report output:** `Reports/municipal_finance_report.pdf`

---

## Prerequisites

- Python 3.11+ with the local virtual environment (`.venv/`)
- PostgreSQL running locally on port 5432 (database: `ma_school_data`)
- Database credentials in `config.py` (copy from `config.example.py`)

To verify Postgres is running: `pg_isready -h localhost -p 5432`

---

## Generating the Report

```bash
source .venv/bin/activate
python analysis/municipal_finance_report.py
```

Output: `Reports/municipal_finance_report.pdf` (~144 KB, 17 numbered slides + appendix divider)

Every number, chart, and peer comparison is computed live from the database.
Nothing is hardcoded. Re-running after a data update automatically reflects all new values.

---

## Annual Data Update (run each October/November)

When DESE and DLS publish new data for the prior school/fiscal year:

**Step 1 — Update the expected years in `data_loader.py`**

Open `data_loader.py` and find the `EXPECTED` dict near the top.
Bump the years for sources that have published new data:

```python
EXPECTED = {
    "municipal_revenues":    ("fiscal_year", 2026),   # ← bump from 2025
    "municipal_expenditures":("fiscal_year", 2026),
    "mcas_results":          ("school_year", 2026),   # ← bump from 2025
    "graduation_rates":      ("school_year", 2026),
    "attendance":            ("school_year", 2026),
    # staffing / per_pupil / enrollment typically lag by 1 year — check DESE
    ...
}
```

Typical release schedule:
| Source | Usually available | Covers |
|--------|------------------|--------|
| MA DLS Schedule A | October | Prior fiscal year (FY ending June) |
| MA DLS Tax Rates & Assessed Values | December | Prior fiscal year |
| DESE MCAS | October | Prior school year |
| DESE Graduation/Attendance | October | Prior school year |
| DESE SAT / Postsecondary / Dropout | October–November | Prior school year |
| DESE Staffing/Per-Pupil/Enrollment | October–December | Prior school year |
| Census ACS 5-year | December | Year ending 2 years prior |
| BLS CPI (FRED) | February | Prior calendar year |
| BLS LAUS (county unemployment) | Monthly | Prior month (1–2 month lag) |
| County Health Rankings | April | Data from ~2 years prior |

**Step 2 — For district_csv.py sources (staffing, enrollment, per-pupil)**

These scrapers read large CSV files from the `Files/` folder.
Download the latest vintage from DESE:
- Go to: `https://www.doe.mass.edu/accountability/data-requests/`
- Download "District Expenditures by Spending Category" → save as
  `Files/District_Expenditures_by_Spending_Category_YYYYMMDD.csv`
- Download "District Expenditures by Function Code" → save as
  `Files/District_Expenditures_by_Function_Code_YYYYMMDD.csv`

The scrapers use a filename glob (`*YYYYMMDD*.csv`) so they will pick up
the new file automatically. Delete the old dated file after confirming load.

**Step 3 — For CPI (inflation)**

Download the updated annual CSV from FRED:
- URL: `https://fred.stlouisfed.org/series/FPCPITOTLZGUSA` → Download Data
- Save as `Files/FPCPITOTLZGUSA.csv` (replace the existing file)

**Step 4 — Run the loader**

```bash
source .venv/bin/activate
python data_loader.py
```

The loader shows a freshness dashboard, then prompts yes/no for each
source that needs updating. All scrapers run with the correct year arguments.

**Step 5 — Refresh supplemental data (run once per year)**

```bash
# Assessed values — all ~352 MA municipalities
python scrapers/assessed_values.py

# DESE state reports — SAT, post-secondary, dropout
python scrapers/dese_state_reports.py all

# Zillow home values
python scrapers/zillow_housing.py

# County unemployment (BLS LAUS) — needs BLS_API_KEY in config.py for >25 req/day
python scrapers/bls_laus.py

# County health rankings (Robert Wood Johnson)
python scrapers/county_health.py

# Municipal crime statistics (MA State Police Beyond 2020 portal)
python scrapers/ma_crime.py
```

Note: BLS LAUS has a 25-request/day limit without an API key. Register for a free key at
https://data.bls.gov/registrationEngine/ and set `BLS_API_KEY` in `config.py`.

**Step 6 — Regenerate the report**

```bash
python analysis/municipal_finance_report.py
```

---

## File Structure

```
Schools/
├── config.py                   # Database connection — copy from config.example.py
├── config.example.py           # Credential template (safe to commit)
├── requirements.txt            # Python dependencies
├── data_loader.py              # Annual data update orchestrator ← START HERE
├── run_all.py                  # Runs all analysis reports at once
│
├── analysis/
│   ├── municipal_finance_report.py   # Main report generator (PDF)
│   ├── data_integrity.py             # Data quality checks
│   ├── peers.py / peers_comprehensive.py / peers_timeseries.py
│   └── spending_report.py / stats_report.py
│
├── scrapers/                   # One scraper per data source
│   │
│   │   # — Primary scrapers (called by data_loader.py) —
│   ├── mcas.py                 # DESE MCAS via Socrata API
│   ├── graduation_rates.py     # DESE graduation rates
│   ├── attendance.py           # DESE chronic absenteeism
│   ├── district_csv.py         # DESE staffing, enrollment, per-pupil (bulk CSV)
│   ├── selected_populations.py # DESE high-needs demographics (SPED, ELL, etc.)
│   ├── chapter70.py            # DESE Chapter 70 state aid
│   ├── municipal_finance.py    # MA DLS Schedule A revenues + expenditures
│   ├── census_acs.py           # Census ACS 5-year (demographics, income, age, etc.)
│   ├── inflation.py            # BLS CPI from FRED CSV
│   │
│   │   # — Supplemental scrapers (run annually, not via data_loader.py) —
│   ├── assessed_values.py      # MA DLS LA-4 assessed values by property class
│   ├── dese_state_reports.py   # DESE SAT scores, postsecondary, dropout rates
│   ├── zillow_housing.py       # Zillow home value index (ZHVI)
│   ├── bls_laus.py             # BLS LAUS county unemployment (monthly)
│   ├── county_health.py        # County Health Rankings (Robert Wood Johnson)
│   ├── ma_crime.py             # MA State Police Beyond 2020 crime stats (town-level)
│   │
│   │   # — Reference / backfill scrapers —
│   ├── districts.py            # Populates MA district registry (districts table)
│   ├── chapter70_historical.py # Historical Chapter 70 backfill
│   ├── enrollment.py           # Enrollment from MA DOE Excel downloads
│   ├── finance.py              # Per-pupil expenditure from MA DOE Excel downloads
│   ├── school_finance.py       # School-level per-pupil from bulk CSV
│   └── municipal_finances.py   # Schedule A alternative implementation
│
├── Files/                      # Downloaded source files (large CSVs — gitignored)
│   ├── FPCPITOTLZGUSA.csv      # BLS CPI data from FRED (update annually)
│   ├── District_Expenditures_by_Spending_Category_*.csv   # DESE bulk file
│   ├── District_Expenditures_by_Function_Code_*.csv       # DESE bulk file
│   └── School_Expenditures_by_Spending_Category_*.csv     # DESE bulk file
│
├── db/
│   ├── schema.sql                          # Full PostgreSQL schema
│   ├── init_db.py                          # One-time DB creation helper
│   ├── queries.py                          # Common query helpers
│   ├── migrate_add_snapshots.py            # Migration: analysis snapshot tables
│   ├── migrate_add_assessed_values.py      # Migration: municipal_assessed_values
│   ├── migrate_add_dese_reports.py         # Migration: DESE report tables + ACS age
│   ├── migrate_add_county_demographics.py  # Migration: county tables + ACS demographic columns
│   └── migrate_add_crime.py                # Migration: municipal_crime table
│
├── Reports/                    # Generated PDF outputs (gitignored)
└── .venv/                      # Python virtual environment (not shared)
```

---

## Database Tables (ma_school_data on localhost)

### Core Financial & Fiscal Data
| Table | Source | Key year column |
|-------|--------|----------------|
| `municipal_revenues` | MA DLS Schedule A | `fiscal_year` |
| `municipal_expenditures` | MA DLS Schedule A | `fiscal_year` |
| `municipal_tax_rates` | MA DLS Gateway | `fiscal_year` |
| `municipal_assessed_values` | MA DLS Gateway LA-4 report | `fiscal_year` |
| `municipal_income_eqv` | MA DLS Gateway (EQV) | `fiscal_year` |
| `municipal_gf_expenditures` | MA DLS Gateway | `fiscal_year` |
| `municipal_new_growth` | MA DLS Gateway | `fiscal_year` |
| `district_chapter70` | MA DESE Chapter 70 files | `fiscal_year` |
| `inflation_cpi` | BLS CPI via FRED (national) | `year` |
| `cpi_boston_msa` | FRED CUUSA103SA0 (Boston MSA CPI) | `calendar_year` |

### School Outcomes & Demographics
| Table | Source | Key year column |
|-------|--------|----------------|
| `mcas_results` | MA DESE / Socrata API | `school_year` |
| `graduation_rates` | MA DESE | `school_year` |
| `attendance` | MA DESE (chronic absenteeism) | `school_year` |
| `district_sat_scores` | MA DESE profiles | `school_year` |
| `district_postsecondary` | MA DESE (graduates attending college) | `school_year` |
| `district_dropout` | MA DESE | `school_year` |
| `staffing` | MA DESE bulk CSV | `school_year` |
| `per_pupil_expenditure` | MA DESE bulk CSV | `school_year` |
| `enrollment` | MA DESE bulk CSV | `school_year` |
| `district_selected_populations` | MA DESE (SPED, ELL, etc.) | `school_year` |
| `district_financials` | MA DESE (detailed per-pupil by category) | `school_year` |
| `demographics` | MA DESE (school-level demographics) | `school_year` |
| `school_expenditures` | MA DESE bulk CSV (school-level) | `school_year` |

### Community Demographics & Housing (Town-Level)
| Table | Source | Key year column |
|-------|--------|----------------|
| `municipal_census_acs` | Census ACS 5-year API | `acs_year` |
| `municipal_zillow_housing` | Zillow ZHVI | `data_year` |

ACS columns include: total population, median age, % under 18, % 65+, median household income,
% owner-occupied, % bachelor's degree, unemployment rate, poverty rate, % foreign-born,
% divorced, % single-parent families.

### County-Level Context
| Table | Source | Level |
|-------|--------|-------|
| `county_unemployment` | BLS LAUS monthly | All 14 MA counties, monthly |
| `county_health_rankings` | Robert Wood Johnson / UW CHR | All 14 MA counties, annual |

### Public Safety (Town-Level)
| Table | Source | Key year column |
|-------|--------|----------------|
| `municipal_crime` | MA State Police Beyond 2020 portal | `year` |

Columns: total crimes, violent crimes, homicides, sexual assaults, aggravated assaults,
clearance rate %, crime rate per 100,000, population. Coverage: all MA municipalities, 2020–2024 (NIBRS era).

### Reference & Infrastructure
| Table | Description |
|-------|-------------|
| `districts` | MA district registry (org codes, names, types) |
| `schools` | MA school registry (populated by districts.py) |
| `ch70_district_mapping` | Lookup table mapping LEA codes to DESE district names |
| `peer_districts` | Historical peer district selections (older analysis) |
| `ingest_log` | Load history written by all scrapers |

### Analysis Snapshots (computed per report run)
| Table | Description |
|-------|-------------|
| `analysis_runs` | One row per report generation |
| `computed_peer_groups` | Peer towns selected per run |
| `computed_metrics` | Key metrics saved per run |
| `computed_feature_importance` | Ridge regression feature weights per run |

---

## If Starting From Scratch (new machine)

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Create the database
createdb ma_school_data
psql ma_school_data < db/schema.sql

# 3. Set up config.py from the example template
cp config.example.py config.py
# Then open config.py and fill in your Postgres credentials

# 4. Download source files into Files/ (see Step 2 above)

# 5. Run data_loader.py with --all flags on individual scrapers to backfill
python scrapers/mcas.py --all
python scrapers/graduation_rates.py --all
python scrapers/dese_state_reports.py all
python scrapers/assessed_values.py --all
python scrapers/zillow_housing.py
python scrapers/bls_laus.py --all
python scrapers/county_health.py --all
python scrapers/ma_crime.py
# ... see each scraper's --help for options

# 6. Generate the report
python analysis/municipal_finance_report.py
```

---

## Peer Selection (how it works)

Peers are selected automatically using two independent statistical methods
applied to 6 outcome-predictive features identified by Ridge regression
(R²=0.84 on MCAS scores across all 221 MA districts):

- Chronic Absenteeism %
- Ch70 State Aid per Pupil
- % College-Educated Adults
- % SPED
- Median Household Income
- % ELL

**Mahalanobis Distance** finds the 10 closest towns in this 6-dimensional
feature space. **Ward Hierarchical Clustering** independently groups all towns
and identifies Saugus's natural cluster. The *consensus* — towns selected by
both methods — appears throughout the report.

Excluded from peer pool: Nantucket (resort/island economy), Tyringham,
Mount Washington, Wellfleet, East Brookfield (extreme population outliers).
