-- =============================================================
-- MA School Data — PostgreSQL Schema
-- Database: ma_school_data
-- =============================================================

-- -------------------------------------------------------
-- Master district / school registry
-- Org code format: DDDDSSSS (4-digit district + 4-digit school)
-- District-level rows have school portion = '0000'
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS districts (
    org_code        VARCHAR(20)  PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    town            VARCHAR(100),
    county          VARCHAR(100),
    district_type   VARCHAR(80),   -- Public School District, Charter, etc.
    grade_span      VARCHAR(20),
    is_district     BOOLEAN DEFAULT TRUE,  -- FALSE = individual school
    district_code   VARCHAR(10),           -- first 4 chars
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schools (
    org_code        VARCHAR(20)  PRIMARY KEY REFERENCES districts(org_code),
    district_code   VARCHAR(10)  REFERENCES districts(org_code),
    school_name     VARCHAR(255),
    grade_low       VARCHAR(10),
    grade_high      VARCHAR(10),
    school_type     VARCHAR(80),
    address         TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- MCAS Results  (Next Generation MCAS, 2017–present)
-- Source: Socrata dataset i9w6-niyt via educationtocareer.data.mass.gov
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS mcas_results (
    id                      SERIAL PRIMARY KEY,
    school_year             INTEGER NOT NULL,       -- ending year, e.g. 2024 = SY 2023-24
    org_code                VARCHAR(20),
    district_name           VARCHAR(255),
    school_name             VARCHAR(255),
    grade                   VARCHAR(10),
    subject                 VARCHAR(50),            -- ELA, MATH, SCI
    student_group           VARCHAR(100),           -- All, M/F, race, ELL, SPED, etc.
    tested_count            INTEGER,
    e_pct                   NUMERIC(6,2),           -- Exceeding
    m_pct                   NUMERIC(6,2),           -- Meeting
    pm_pct                  NUMERIC(6,2),           -- Partially Meeting
    nm_pct                  NUMERIC(6,2),           -- Not Meeting
    meeting_exceeding_pct   NUMERIC(6,2),           -- M+E combined
    mean_scaled_score       NUMERIC(7,2),
    avg_student_growth_pct  NUMERIC(6,2),           -- SGP
    raw_row                 JSONB,                  -- full original record for reprocessing
    loaded_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, grade, subject, student_group)
);

-- -------------------------------------------------------
-- Enrollment
-- Source: MA DOE Excel downloads (doe.mass.gov/infoservices/reports/enroll)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrollment (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    org_code        VARCHAR(20),
    district_name   VARCHAR(255),
    school_name     VARCHAR(255),
    grade           VARCHAR(20),    -- PK, K, 1–12, SP (special), Total
    total           INTEGER,
    male            INTEGER,
    female          INTEGER,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, grade)
);

-- -------------------------------------------------------
-- Demographics / Selected Populations
-- Source: MA DOE profiles / DART
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS demographics (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    org_code        VARCHAR(20),
    district_name   VARCHAR(255),
    school_name     VARCHAR(255),
    category        VARCHAR(100),   -- low_income, ell, sped, white, hispanic, black, asian, etc.
    pct             NUMERIC(6,2),
    count           INTEGER,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, category)
);

-- -------------------------------------------------------
-- Per-Pupil Expenditure
-- Source: MA DOE DART Excel / doe.mass.gov/finance/statistics/ppx
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS per_pupil_expenditure (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    org_code        VARCHAR(20),
    district_name   VARCHAR(255),
    category        VARCHAR(150),   -- In-District, Out-of-District, Total, Leadership, etc.
    amount          NUMERIC(12,2),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, category)
);

-- -------------------------------------------------------
-- District Financials (Chapter 70 funding, revenues, expenditures)
-- Source: MA DOE DART / data.mass.gov
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS district_financials (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    org_code        VARCHAR(20),
    district_name   VARCHAR(255),
    category        VARCHAR(150),   -- Chapter70, LocalContribution, TotalRevenue, etc.
    subcategory     VARCHAR(150),
    amount          NUMERIC(14,2),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, category, subcategory)
);

-- -------------------------------------------------------
-- Graduation & Dropout Rates
-- Source: MA DOE profiles / DART
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS graduation_rates (
    id                  SERIAL PRIMARY KEY,
    school_year         INTEGER NOT NULL,
    org_code            VARCHAR(20),
    district_name       VARCHAR(255),
    student_group       VARCHAR(100) DEFAULT 'All',
    four_year_grad_pct  NUMERIC(6,2),
    five_year_grad_pct  NUMERIC(6,2),
    dropout_pct         NUMERIC(6,2),
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, student_group)
);

-- -------------------------------------------------------
-- Attendance / Chronic Absenteeism
-- Source: MA DOE profiles / DART
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS attendance (
    id                          SERIAL PRIMARY KEY,
    school_year                 INTEGER NOT NULL,
    org_code                    VARCHAR(20),
    district_name               VARCHAR(255),
    school_name                 VARCHAR(255),
    student_group               VARCHAR(100) DEFAULT 'All',
    attendance_rate_pct         NUMERIC(6,2),
    chronic_absenteeism_pct     NUMERIC(6,2),
    loaded_at                   TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, student_group)
);

-- -------------------------------------------------------
-- Staffing / Educators
-- Source: MA DOE Educator Data / DART
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS staffing (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    org_code        VARCHAR(20),
    district_name   VARCHAR(255),
    category        VARCHAR(150),   -- Teachers, Administrators, Aides, etc.
    fte             NUMERIC(10,2),
    count           INTEGER,
    avg_salary      NUMERIC(12,2),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, category)
);

-- -------------------------------------------------------
-- FRED CPI Inflation (annual % change, USA)
-- Source: FRED series FPCPITOTLZGUSA  (Files/FPCPITOTLZGUSA.csv)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS inflation_cpi (
    id              SERIAL PRIMARY KEY,
    year            INTEGER NOT NULL UNIQUE,
    cpi_pct_change  NUMERIC(8,6),   -- annual % change (e.g. 4.697... = 4.7%)
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- Chapter 70 State Aid to School Districts
-- Source: DESE  doe.mass.edu/finance/chapter70/fy{YYYY}/chapter-{YYYY}-local.xlsx
-- Available: FY2023–present via current URL pattern
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS district_chapter70 (
    id                          SERIAL PRIMARY KEY,
    fiscal_year                 INTEGER NOT NULL,
    lea_code                    INTEGER,
    district_name               VARCHAR(255),
    foundation_enrollment       INTEGER,
    foundation_budget           NUMERIC(16,2),
    required_contribution       NUMERIC(16,2),
    chapter70_aid               NUMERIC(16,2),
    required_nss                NUMERIC(16,2),   -- required net school spending
    chapter70_aid_per_pupil     NUMERIC(12,2),   -- derived: aid / foundation_enrollment
    required_nss_per_pupil      NUMERIC(12,2),   -- derived: nss / foundation_enrollment
    loaded_at                   TIMESTAMP DEFAULT NOW(),
    UNIQUE (fiscal_year, lea_code)
);

CREATE INDEX IF NOT EXISTS idx_ch70_year_lea ON district_chapter70 (fiscal_year, lea_code);

-- -------------------------------------------------------
-- Census ACS 5-Year Estimates — Massachusetts municipalities
-- Source: api.census.gov  ACS 5-year, county subdivisions, state=25
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS municipal_census_acs (
    id                      SERIAL PRIMARY KEY,
    acs_year                INTEGER NOT NULL,   -- ending year of 5-year estimate (e.g. 2023)
    state_fips              VARCHAR(2) DEFAULT '25',
    county_fips             VARCHAR(3),
    cousub_fips             VARCHAR(5),
    name                    VARCHAR(150),       -- raw Census name
    municipality            VARCHAR(100),       -- cleaned town name
    total_population        INTEGER,
    pop_65_plus             INTEGER,
    pct_65_plus             NUMERIC(6,2),
    median_hh_income        INTEGER,
    total_housing_units     INTEGER,
    owner_occupied          INTEGER,
    pct_owner_occupied      NUMERIC(6,2),
    pop_25_plus             INTEGER,
    bachelors_plus          INTEGER,
    pct_bachelors_plus      NUMERIC(6,2),
    loaded_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (acs_year, state_fips, county_fips, cousub_fips)
);

CREATE INDEX IF NOT EXISTS idx_census_acs_year ON municipal_census_acs (acs_year, municipality);

-- -------------------------------------------------------
-- Municipal Finance — MA DLS Gateway
-- -------------------------------------------------------

-- Income and Equalized Valuation per capita (DOR Income & EQV Per Capita report)
-- Source: dls-gw.dor.state.ma.us rdReport=DOR_Income_EQV_Per_Capita
-- Available: FY2007–present
CREATE TABLE IF NOT EXISTS municipal_income_eqv (
    id                  SERIAL PRIMARY KEY,
    fiscal_year         INTEGER NOT NULL,
    dor_code            INTEGER,
    lea_code            INTEGER,
    municipality        VARCHAR(100),
    county              VARCHAR(100),
    population          INTEGER,
    dor_income          NUMERIC(16,2),
    income_per_capita   NUMERIC(12,2),
    eqv                 NUMERIC(16,2),
    eqv_per_capita      NUMERIC(12,2),
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);

-- Property tax rates by class
-- Source: rdReport=propertytaxinformation.taxratesbyclass.taxratesbyclass
CREATE TABLE IF NOT EXISTS municipal_tax_rates (
    id                  SERIAL PRIMARY KEY,
    fiscal_year         INTEGER NOT NULL,
    dor_code            INTEGER,
    municipality        VARCHAR(100),
    residential         NUMERIC(8,4),
    open_space          NUMERIC(8,4),
    commercial          NUMERIC(8,4),
    industrial          NUMERIC(8,4),
    personal_property   NUMERIC(8,4),
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);

-- General Fund expenditures per capita
-- Source: rdReport=351GenFunperCapita
CREATE TABLE IF NOT EXISTS municipal_gf_expenditures (
    id                          SERIAL PRIMARY KEY,
    fiscal_year                 INTEGER NOT NULL,
    dor_code                    INTEGER,
    municipality                VARCHAR(100),
    population                  INTEGER,
    total_gf_expenditure        NUMERIC(16,2),
    gf_expenditure_per_capita   NUMERIC(12,2),
    loaded_at                   TIMESTAMP DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);

-- New growth (property development added to levy base)
-- Source: rdReport=newgrowth.newgrowth_dash_v2_test
CREATE TABLE IF NOT EXISTS municipal_new_growth (
    id                              SERIAL PRIMARY KEY,
    fiscal_year                     INTEGER NOT NULL,
    dor_code                        INTEGER,
    municipality                    VARCHAR(100),
    residential_new_growth_value    NUMERIC(16,2),
    residential_new_growth_applied  NUMERIC(16,2),
    total_new_growth_value          NUMERIC(16,2),
    total_new_growth_applied        NUMERIC(16,2),
    res_pct_of_total                NUMERIC(8,4),
    prior_year_levy_limit           NUMERIC(16,2),
    new_growth_pct_of_py_levy       NUMERIC(8,4),
    loaded_at                       TIMESTAMP DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);

CREATE INDEX IF NOT EXISTS idx_muni_ieqv_year     ON municipal_income_eqv (fiscal_year, dor_code);
CREATE INDEX IF NOT EXISTS idx_muni_taxrates_year  ON municipal_tax_rates (fiscal_year, dor_code);
CREATE INDEX IF NOT EXISTS idx_muni_gfexp_year     ON municipal_gf_expenditures (fiscal_year, dor_code);
CREATE INDEX IF NOT EXISTS idx_muni_newgrowth_year ON municipal_new_growth (fiscal_year, dor_code);

-- -------------------------------------------------------
-- School-Level Expenditures (per-pupil, by spending category)
-- Source: MA DOE Education to Career Power BI / CSV export
--   Files/School_Expenditures_by_Spending_Category_*.csv
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS school_expenditures (
    id              SERIAL PRIMARY KEY,
    school_year     INTEGER NOT NULL,
    dist_code       VARCHAR(20),
    dist_name       VARCHAR(255),
    org_code        VARCHAR(20),
    school_name     VARCHAR(255),
    grades_served   VARCHAR(20),
    ind_cat         VARCHAR(150),   -- e.g. "School-Level State and Local Instructional Expenditures"
    ind_subcat      VARCHAR(150),   -- e.g. "Teachers", "Guidance and Psych"
    ind_value       NUMERIC(14,2),  -- per-pupil dollar amount
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, org_code, ind_cat, ind_subcat)
);

CREATE INDEX IF NOT EXISTS idx_school_exp_year_org ON school_expenditures (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_school_exp_cat      ON school_expenditures (ind_cat, ind_subcat);

-- -------------------------------------------------------
-- Mahalanobis peer-district cache
-- Pre-computed similarity scores for fast querying
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS peer_districts (
    id                  SERIAL PRIMARY KEY,
    school_year         INTEGER NOT NULL,
    base_org_code       VARCHAR(20),    -- e.g. Saugus
    peer_org_code       VARCHAR(20),
    mahalanobis_dist    NUMERIC(10,6),
    rank_order          INTEGER,        -- 1 = most similar
    computed_at         TIMESTAMP DEFAULT NOW(),
    UNIQUE (school_year, base_org_code, peer_org_code)
);

-- -------------------------------------------------------
-- Ingest log — tracks what has been loaded and when
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_log (
    id          SERIAL PRIMARY KEY,
    source      VARCHAR(100) NOT NULL,  -- 'mcas', 'enrollment', 'ppe', etc.
    school_year INTEGER,
    rows_loaded INTEGER,
    status      VARCHAR(20) DEFAULT 'ok',
    notes       TEXT,
    loaded_at   TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- Indexes for common query patterns
-- -------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_mcas_year_org      ON mcas_results (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_mcas_subject        ON mcas_results (subject, grade, student_group);
CREATE INDEX IF NOT EXISTS idx_enrollment_year_org ON enrollment (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_ppe_year_org        ON per_pupil_expenditure (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_fin_year_org        ON district_financials (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_demo_year_org       ON demographics (school_year, org_code);
CREATE INDEX IF NOT EXISTS idx_peers_base          ON peer_districts (school_year, base_org_code, rank_order);

-- -------------------------------------------------------
-- Zillow Housing Data — MA municipalities
-- Source: files.zillowstatic.com/research/public_csvs/
-- Aggregated to calendar-year averages from monthly data
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS municipal_zillow_housing (
    id                   SERIAL PRIMARY KEY,
    region_name          VARCHAR(100) NOT NULL,   -- city/town name (Zillow RegionName)
    county_name          VARCHAR(100),
    data_year            INTEGER NOT NULL,
    data_month           INTEGER NOT NULL,         -- 1-12
    zhvi                 NUMERIC(12,2),            -- Zillow Home Value Index (all towns)
    median_sale_price    NUMERIC(12,2),            -- USD (major cities only, ~23 MA towns)
    mean_days_to_pending NUMERIC(6,1),             -- mean days listing → pending (major cities)
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (region_name, data_year, data_month)
);

CREATE INDEX IF NOT EXISTS idx_zillow_year ON municipal_zillow_housing (data_year, data_month, region_name);

-- -------------------------------------------------------
-- DESE Selected Populations — unduplicated High Needs %
-- Source: profiles.doe.mass.edu/statereport/selectedpopulations.aspx
-- fycode = ending year (e.g. 2025 = SY2024-25)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS district_selected_populations (
    id                  SERIAL PRIMARY KEY,
    school_year         INTEGER NOT NULL,   -- ending year (fycode)
    org_code            VARCHAR(20) NOT NULL,
    district_name       VARCHAR(200),
    high_needs_count    INTEGER,
    high_needs_pct      NUMERIC(5,1),       -- unduplicated % of total enrollment
    ell_count           INTEGER,
    ell_pct             NUMERIC(5,1),
    flne_count          INTEGER,            -- First Language Not English
    flne_pct            NUMERIC(5,1),
    low_income_count    INTEGER,
    low_income_pct      NUMERIC(5,1),
    sped_count          INTEGER,
    sped_pct            NUMERIC(5,1),
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (school_year, org_code)
);

CREATE INDEX IF NOT EXISTS idx_selected_pop_year ON district_selected_populations (school_year, org_code);

-- -------------------------------------------------------
-- Ch70 LEA code → DESE org_code prefix mapping
-- Resolves two sources of mismatch:
--   1. Regional districts: member towns listed separately in Ch70
--      but combined into one DESE district (e.g. Holden→Wachusett)
--   2. Naming differences: Ch70 "Acton" → DESE "Acton-Boxborough"
-- dese_lea_prefix = first 4 digits of DESE org_code as integer
-- notes: explains the mapping type
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS ch70_district_mapping (
    lea_code        INTEGER PRIMARY KEY,   -- Ch70 lea_code
    ch70_name       VARCHAR(200),
    dese_lea_prefix INTEGER,               -- SUBSTRING(org_code,1,4)::int
    dese_name       VARCHAR(200),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
