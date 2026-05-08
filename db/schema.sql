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
-- MA DLS Schedule A — Municipal Revenues & Expenditures
-- Source: scrapers/municipal_finance.py (DLS Gateway rdPage)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS municipal_revenues (
    id                  SERIAL PRIMARY KEY,
    dor_code            INTEGER      NOT NULL,
    municipality        TEXT         NOT NULL,
    fiscal_year         INTEGER      NOT NULL,
    taxes               BIGINT,
    service_charges     BIGINT,
    licenses_permits    BIGINT,
    federal_revenue     BIGINT,
    state_revenue       BIGINT,
    intergovernmental   BIGINT,
    special_assessments BIGINT,
    fines_forfeitures   BIGINT,
    miscellaneous       BIGINT,
    other_financing     BIGINT,
    transfers           BIGINT,
    total_revenues      BIGINT,
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (dor_code, fiscal_year)
);

CREATE TABLE IF NOT EXISTS municipal_expenditures (
    id                  SERIAL PRIMARY KEY,
    dor_code            INTEGER      NOT NULL,
    municipality        TEXT         NOT NULL,
    fiscal_year         INTEGER      NOT NULL,
    general_government  BIGINT,
    public_safety       BIGINT,
    education           BIGINT,
    public_works        BIGINT,
    human_services      BIGINT,
    culture_recreation  BIGINT,
    fixed_costs         BIGINT,
    intergovernmental   BIGINT,
    other_expenditures  BIGINT,
    debt_service        BIGINT,
    total_expenditures  BIGINT,
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (dor_code, fiscal_year)
);

-- -------------------------------------------------------
-- Boston MSA CPI (for local inflation context)
-- Source: FRED series CUUSA103SA0
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS cpi_boston_msa (
    calendar_year   INTEGER      NOT NULL PRIMARY KEY,
    cpi_value       NUMERIC,
    cpi_index       NUMERIC,
    source          TEXT         DEFAULT 'FRED CUUSA103SA0',
    fetched_at      TIMESTAMP    DEFAULT NOW()
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
    median_age              NUMERIC(5,1),
    pop_under18             INTEGER,
    pct_under18             NUMERIC(6,2),
    pop_65_plus             INTEGER,
    pct_65_plus             NUMERIC(6,2),
    unemployment_rate       NUMERIC(5,2),
    poverty_pct             NUMERIC(5,2),
    pct_foreign_born        NUMERIC(5,2),
    pct_divorced            NUMERIC(5,2),
    pct_single_parent       NUMERIC(5,2),
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


-- =============================================================
-- County-Level Data
-- =============================================================
CREATE TABLE IF NOT EXISTS county_unemployment (
    id                SERIAL PRIMARY KEY,
    state_fips        VARCHAR(2)   NOT NULL,
    county_fips       VARCHAR(5)   NOT NULL,
    county_name       VARCHAR(100),
    year              INTEGER      NOT NULL,
    month             INTEGER      NOT NULL,  -- 1–12
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

-- =============================================================
-- DESE District State Reports
-- =============================================================
CREATE TABLE IF NOT EXISTS district_sat_scores (
    id            SERIAL PRIMARY KEY,
    school_year   INTEGER      NOT NULL,  -- 2024 = 2023-24 school year
    district_code VARCHAR(20)  NOT NULL,
    district_name VARCHAR(255),
    tests_taken   INTEGER,
    mean_ebrw     INTEGER,                -- Evidence-Based Reading & Writing
    mean_math     INTEGER,
    loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (school_year, district_code)
);

CREATE TABLE IF NOT EXISTS district_postsecondary (
    id                   SERIAL PRIMARY KEY,
    school_year          INTEGER      NOT NULL,
    district_code        VARCHAR(20)  NOT NULL,
    district_name        VARCHAR(255),
    grads_n              INTEGER,         -- total HS graduates
    attending_n          INTEGER,         -- attending college
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

-- =============================================================
-- Municipal Assessed Values by Property Class (from DLS LA-4 report)
-- =============================================================
CREATE TABLE IF NOT EXISTS municipal_assessed_values (
    id                  SERIAL PRIMARY KEY,
    fiscal_year         INTEGER      NOT NULL,
    dor_code            INTEGER      NOT NULL,
    municipality        VARCHAR(100),
    res_av              BIGINT,   -- Class 1: Residential
    open_space_av       BIGINT,   -- Class 2: Open Space
    commercial_av       BIGINT,   -- Class 3: Commercial
    industrial_av       BIGINT,   -- Class 4: Industrial
    personal_property_av BIGINT,  -- Class 5: Personal Property
    total_av            BIGINT,   -- Total Real & Personal (taxable)
    exempt_av           BIGINT,   -- Exempt property value
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (fiscal_year, dor_code)
);
CREATE INDEX IF NOT EXISTS idx_mav_fy_code ON municipal_assessed_values (fiscal_year, dor_code);

-- =============================================================
-- Analysis Snapshots — computed results saved per report run
-- Allows year-over-year comparison of peers, metrics, outcomes
-- =============================================================

-- One row per report generation
CREATE TABLE IF NOT EXISTS analysis_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ DEFAULT NOW(),
    data_vintage_fy INTEGER,       -- max fiscal_year in data at time of run
    data_vintage_sy INTEGER,       -- max school_year in data at time of run
    n_peer_pool     INTEGER,       -- total towns available as peers
    notes           TEXT
);

-- Which towns were selected as peers, per method, per run
CREATE TABLE IF NOT EXISTS computed_peer_groups (
    id           SERIAL PRIMARY KEY,
    run_id       INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    method       VARCHAR(20)    NOT NULL,  -- 'mahalanobis', 'ward_cluster', 'consensus'
    municipality VARCHAR(100)   NOT NULL,
    ed_pct       NUMERIC(6,2),             -- their ed% at time of run
    mahal_dist   NUMERIC(10,6),            -- NULL for ward/consensus
    rank_in_set  INTEGER                   -- 1=closest; NULL for ward/consensus
);

CREATE INDEX IF NOT EXISTS idx_cpg_run_method ON computed_peer_groups (run_id, method);

-- Key computed metrics — flexible key/value with fiscal/school year context
CREATE TABLE IF NOT EXISTS computed_metrics (
    id           SERIAL PRIMARY KEY,
    run_id       INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    metric       VARCHAR(60)    NOT NULL,  -- e.g. 'saugus_ed_pct', 'funding_gap_m'
    fiscal_year  INTEGER,                  -- NULL if metric is not FY-specific
    school_year  INTEGER,                  -- NULL if metric is not SY-specific
    value        NUMERIC(12,4),
    notes        TEXT
);

CREATE INDEX IF NOT EXISTS idx_cm_run_metric ON computed_metrics (run_id, metric);

-- RBP feature importances per run
CREATE TABLE IF NOT EXISTS computed_feature_importance (
    id          SERIAL PRIMARY KEY,
    run_id      INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    rank        INTEGER       NOT NULL,   -- 1 = most important
    feature     VARCHAR(60)   NOT NULL,
    importance  NUMERIC(10,6)             -- R² drop from leave-one-out
);

-- -------------------------------------------------------
-- Municipal Crime Statistics
-- Source: MA State Police Beyond 2020 portal (ma.beyond2020.com/ma_tops)
-- Coverage: all MA municipalities, 2020–2024 (NIBRS era)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS municipal_crime (
    id                   SERIAL PRIMARY KEY,
    jurisdiction_slug    VARCHAR(100)  NOT NULL,
    ori_code             VARCHAR(20)   NOT NULL,
    jurisdiction_name    VARCHAR(200),
    year                 INTEGER       NOT NULL,
    total_crimes         INTEGER,
    violent_crimes       INTEGER,
    homicides            INTEGER,
    sexual_assaults      INTEGER,
    aggravated_assaults  INTEGER,
    clearance_rate_pct   NUMERIC(5,2),
    crime_rate_per_100k  NUMERIC(10,2),
    population           INTEGER,
    loaded_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (ori_code, year)
);

CREATE INDEX IF NOT EXISTS idx_municipal_crime_slug ON municipal_crime(jurisdiction_slug);
CREATE INDEX IF NOT EXISTS idx_municipal_crime_year ON municipal_crime(year);

-- -------------------------------------------------------
-- Municipal Crash Statistics (MassDOT IMPACT Open Data Platform)
-- Source: gis.crashdata.dot.mass.gov — MASSDOT_ODP_OPEN_{year}
-- Coverage: all ~350 MA municipalities, 2021–2024
-- -------------------------------------------------------
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
