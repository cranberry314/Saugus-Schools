"""
SQL Query Library — MA School Data
===================================
Reusable, parameterised SQL building blocks for common metrics.

All queries accept :yr (school_year integer) as a bind parameter.
ACS-linked queries also accept :acs_yr (acs_year integer).

Usage example:
    from db.queries import FEATURE_MATRIX_FULL
    from config import get_engine
    import pandas as pd
    from sqlalchemy import text

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(FEATURE_MATRIX_FULL), conn, params={"yr": 2024, "acs_yr": 2023})
"""

# ── Combined feature matrix (used by peer_analysis_comprehensive) ─────────────

FEATURE_MATRIX_FULL = """
-- Full feature matrix: one row per district, all Tier 1-3 metrics.
-- Bind params: :yr (school_year), :acs_yr (ACS ending year, e.g. school_year - 1 capped at 2023)
WITH
enroll AS (
    SELECT org_code, total AS total_enrollment
    FROM enrollment
    WHERE school_year = :yr
      AND grade = 'Total'
      AND org_code LIKE '%0000'
),
sel_pop AS (
    SELECT org_code, high_needs_pct, ell_pct, low_income_pct, sped_pct
    FROM district_selected_populations
    WHERE school_year = :yr
),
ppe AS (
    SELECT org_code,
           MAX(CASE WHEN category = 'Total In-District Expenditures' THEN amount END) AS nss_per_pupil,
           MAX(CASE WHEN category = 'Teachers'                       THEN amount END) AS teacher_ppe,
           MAX(CASE WHEN category = 'Administration'                 THEN amount END) AS admin_ppe,
           MAX(CASE WHEN category = 'Pupil Services'                 THEN amount END) AS pupil_svcs_ppe
    FROM per_pupil_expenditure
    WHERE school_year = :yr
    GROUP BY org_code
),
staff AS (
    SELECT org_code,
           MAX(CASE WHEN category = 'teacher_fte'          THEN fte END)       AS teacher_fte,
           MAX(CASE WHEN category = 'teachers_per_100_fte' THEN fte END)       AS teachers_per_100_fte,
           MAX(CASE WHEN category = 'para_fte'             THEN fte END)       AS para_fte,
           MAX(CASE WHEN category = 'teacher_avg_salary'   THEN avg_salary END)AS teacher_avg_salary
    FROM staffing
    WHERE school_year = :yr
    GROUP BY org_code
),
ch70 AS (
    SELECT SUBSTRING(d2.org_code, 1, 4)::int AS lea_prefix,
           c.chapter70_aid_per_pupil          AS ch70_per_pupil
    FROM district_chapter70 c
    JOIN districts d2
      ON SUBSTRING(d2.org_code, 1, 4)::int = c.lea_code
     AND d2.is_district = TRUE
     AND d2.org_code LIKE '%0000'
    WHERE c.fiscal_year = :yr
),
grad AS (
    SELECT org_code, four_year_grad_pct
    FROM graduation_rates
    WHERE school_year = :yr AND student_group = 'All'
),
att AS (
    SELECT org_code, chronic_absenteeism_pct
    FROM attendance
    WHERE school_year = :yr AND student_group = 'All'
),
acs AS (
    SELECT d3.org_code,
           a.pct_65_plus,
           a.median_hh_income,
           a.pct_owner_occupied,
           a.pct_bachelors_plus
    FROM districts d3
    JOIN municipal_census_acs a
      ON LOWER(TRIM(a.municipality)) = LOWER(TRIM(d3.name))
     AND a.acs_year = :acs_yr
    WHERE d3.is_district = TRUE
      AND d3.org_code LIKE '%0000'
),
mcas AS (
    SELECT org_code,
           AVG(CASE WHEN subject ILIKE 'ELA'  THEN meeting_exceeding_pct END) AS ela_me_pct,
           AVG(CASE WHEN subject ILIKE 'MATH' THEN meeting_exceeding_pct END) AS math_me_pct
    FROM mcas_results
    WHERE school_year = :yr
      AND student_group = 'All Students'
      AND grade IN ('10', 'ALL (03-08)')
    GROUP BY org_code
)
SELECT
    d.org_code,
    d.name                          AS district_name,
    d.town,
    -- Tier 1
    e.total_enrollment,
    sp.high_needs_pct,
    sp.ell_pct,
    sp.low_income_pct,
    sp.sped_pct,
    ppe.nss_per_pupil,
    ch70.ch70_per_pupil,
    -- Tier 2
    ppe.teacher_ppe,
    ppe.admin_ppe,
    ppe.pupil_svcs_ppe,
    CASE WHEN ppe.teacher_ppe > 0
         THEN ROUND(ppe.admin_ppe / ppe.teacher_ppe, 4) END    AS admin_teacher_ratio,
    st.teacher_fte,
    st.teachers_per_100_fte,
    st.para_fte,
    st.teacher_avg_salary,
    -- Tier 3
    acs.pct_65_plus,
    acs.median_hh_income,
    acs.pct_owner_occupied,
    acs.pct_bachelors_plus,
    -- Tier 1 continued: engagement & outcomes
    grad.four_year_grad_pct,
    att.chronic_absenteeism_pct,
    -- Outcomes (validation)
    mc.ela_me_pct,
    mc.math_me_pct
FROM districts d
LEFT JOIN enroll  e    ON e.org_code  = d.org_code
LEFT JOIN sel_pop sp   ON sp.org_code = d.org_code
LEFT JOIN ppe          ON ppe.org_code = d.org_code
LEFT JOIN staff  st    ON st.org_code = d.org_code
LEFT JOIN ch70         ON ch70.lea_prefix = SUBSTRING(d.org_code, 1, 4)::int
LEFT JOIN grad         ON grad.org_code = d.org_code
LEFT JOIN att          ON att.org_code  = d.org_code
LEFT JOIN acs          ON acs.org_code = d.org_code
LEFT JOIN mcas   mc    ON mc.org_code = d.org_code
WHERE d.is_district = TRUE
  AND d.org_code LIKE '%0000'
"""

# ── Feature column definitions (for use by analysis scripts) ─────────────────

# Each entry: (column_name, tier, label, description)
FEATURE_CATALOG = [
    # Tier 1
    ("total_enrollment",    1, "Total Enrollment",        "FTE pupils — scale matters for cost comparisons"),
    ("high_needs_pct",      1, "% High Needs",            "Unduplicated ELL+low-income+SPED composite; biggest cost driver"),
    ("ell_pct",             1, "% ELL",                   "Drives Title III, bilingual staffing costs"),
    ("low_income_pct",      1, "% Low Income",            "Correlates with cost and outcome gaps"),
    ("sped_pct",            1, "% SPED",                  "Special education costs are mandated and high"),
    ("nss_per_pupil",           1, "Net School Spending/Pupil","Actual educational investment; state comparison benchmark"),
    ("ch70_per_pupil",          1, "Ch70 Aid/Pupil",          "State dependency — high Ch70 towns have less local flexibility"),
    ("four_year_grad_pct",      1, "4-Year Grad Rate %",       "Cohort graduation rate; reflects long-run district effectiveness"),
    ("chronic_absenteeism_pct", 1, "Chronic Absenteeism %",    "% students missing 10%+ of days; correlated with High Needs"),
    # Tier 2
    ("teacher_ppe",         2, "Teacher Spending/Pupil",  "Core instructional investment"),
    ("admin_ppe",           2, "Admin Spending/Pupil",    "Administrative overhead per student"),
    ("admin_teacher_ratio", 2, "Admin:Teacher Ratio",     "Structural efficiency metric"),
    ("pupil_svcs_ppe",      2, "Pupil Services/Pupil",    "High Needs cost proxy (counseling, paras, health)"),
    ("teacher_fte",         2, "Teacher FTE",             "Absolute staffing level"),
    ("teachers_per_100_fte",2, "Teachers per 100 FTE",    "Staffing adequacy ratio"),
    ("para_fte",            2, "Paraprofessional FTE",    "Tier proxy for High Needs support load"),
    ("teacher_avg_salary",  2, "Avg Teacher Salary",      "Compensation competitiveness"),
    # Tier 3
    ("pct_65_plus",         3, "% Pop 65+",               "Override political feasibility / senior voter share"),
    ("median_hh_income",    3, "Median HH Income",        "More granular than DOR income per capita"),
    ("pct_owner_occupied",  3, "% Owner-Occupied",        "Tax sensitivity — owners vs renters vote differently"),
    ("pct_bachelors_plus",  3, "% Bachelor's+",           "Parental involvement proxy; political culture"),
    # Outcomes
    ("ela_me_pct",          4, "MCAS ELA M+E %",          "Academic outcome — ELA proficiency"),
    ("math_me_pct",         4, "MCAS Math M+E %",         "Academic outcome — Math proficiency"),
]

# Ordered list of just the column names (for matrix slicing)
ALL_FEATURE_COLS     = [c for c, *_ in FEATURE_CATALOG]
TIER1_COLS           = [c for c, t, *_ in FEATURE_CATALOG if t == 1]
TIER2_COLS           = [c for c, t, *_ in FEATURE_CATALOG if t == 2]
TIER3_COLS           = [c for c, t, *_ in FEATURE_CATALOG if t == 3]
OUTCOME_COLS         = [c for c, t, *_ in FEATURE_CATALOG if t == 4]

# Active set for Mahalanobis distance — Tier 1+2 only (Tier 3 = ACS, Tier 4 = MCAS outcomes
# are excluded until ACS join coverage improves)
# Graduation rate and chronic absenteeism are Tier 1 and included here
MAHAL_FEATURE_COLS   = TIER1_COLS + TIER2_COLS

FEATURE_LABEL = {c: lbl for c, _, lbl, *_ in FEATURE_CATALOG}
FEATURE_DESC  = {c: desc for c, _, _, desc in FEATURE_CATALOG}
FEATURE_TIER  = {c: t   for c, t, *_ in FEATURE_CATALOG}


# ═════════════════════════════════════════════════════════════════════════════
# Flagship feature queries — the canonical per-district feature loads used by
# saugus_factor_analysis.load_features().  These are the authoritative versions;
# other analyses should reuse them rather than re-inline their own.  Bind params
# are noted per query (:yr school/fiscal year; :yr_lo/:yr_hi window bounds).
# ═════════════════════════════════════════════════════════════════════════════

# ── Outcomes ─────────────────────────────────────────────────────────────────
FA_MCAS_3_8 = """
    SELECT org_code, district_name,
           AVG(meeting_exceeding_pct::float) AS avg_mcas
    FROM mcas_results
    WHERE school_year = :yr
      AND student_group = 'All Students'
      AND grade = 'ALL (03-08)'
      AND subject IN ('ELA', 'MATH')
      AND org_code LIKE '%0000'
    GROUP BY org_code, district_name
"""

FA_POSTSECONDARY = """
    SELECT DISTINCT ON (district_name)
           district_name, attending_pct::float AS attending_pct
    FROM district_postsecondary
    WHERE school_year <= :yr
    ORDER BY district_name, school_year DESC
"""

FA_DROPOUT = """
    SELECT DISTINCT ON (district_name)
           district_name, dropout_pct::float AS dropout_pct
    FROM district_dropout
    WHERE school_year <= :yr
    ORDER BY district_name, school_year DESC
"""

# ── DESE school features ─────────────────────────────────────────────────────
FA_SELECTED_POP = """
    SELECT district_name,
           high_needs_pct::float, ell_pct::float,
           low_income_pct::float, sped_pct::float
    FROM district_selected_populations WHERE school_year = :yr
"""

FA_ATTENDANCE = """
    SELECT district_name, chronic_absenteeism_pct::float
    FROM attendance
    WHERE school_year = :yr AND student_group = 'All'
      AND school_name IS NULL
"""

FA_PPE_INSTRUCTIONAL = """
    SELECT district_name, category, amount::float
    FROM per_pupil_expenditure WHERE school_year = :yr
      AND category IN ('Total In-District Expenditures', 'Teachers',
                       'Other Teaching Services',
                       'Instructional Materials, Equipment and Technology',
                       'Instructional Leadership')
"""

FA_CHAPTER70 = """
    SELECT district_name,
           chapter70_aid_per_pupil::float  AS ch70_per_pupil,
           (foundation_budget::float /
            NULLIF(foundation_enrollment::float, 0)) AS foundation_budget_pp
    FROM district_chapter70 WHERE fiscal_year = :yr
"""

FA_STAFFING = """
    SELECT district_name, category, fte::float, avg_salary::float
    FROM staffing WHERE school_year = :yr
      AND category IN ('teacher_fte', 'teachers_per_100_fte',
                       'teacher_avg_salary')
"""

FA_ENROLLMENT = """
    SELECT district_name, total::float AS total_enrollment
    FROM enrollment
    WHERE school_year = :yr AND grade = 'Total' AND school_name IS NULL
"""

FA_GRADUATION = """
    SELECT DISTINCT ON (district_name)
           district_name,
           four_year_grad_pct::float AS four_yr_grad_pct,
           five_year_grad_pct::float AS five_yr_grad_pct
    FROM graduation_rates
    WHERE school_year <= :yr AND student_group = 'All Students'
      AND org_code LIKE '%%0000'
    ORDER BY district_name, school_year DESC
"""

FA_SAT = """
    SELECT DISTINCT ON (district_name)
           district_name,
           mean_ebrw::float AS sat_ebrw,
           mean_math::float  AS sat_math
    FROM district_sat_scores
    WHERE school_year <= :yr
    ORDER BY district_name, school_year DESC
"""

FA_MCAS_10 = """
    SELECT district_name,
           AVG(CASE WHEN subject='ELA'  THEN meeting_exceeding_pct::float END) AS mcas10_ela,
           AVG(CASE WHEN subject='MATH' THEN meeting_exceeding_pct::float END) AS mcas10_math
    FROM mcas_results
    WHERE school_year = :yr AND grade = '10'
      AND student_group = 'All Students'
      AND org_code LIKE '%0000'
    GROUP BY district_name
"""

# ── ACS demographics ─────────────────────────────────────────────────────────
FA_ACS = """
    SELECT DISTINCT ON (municipality)
           municipality,
           total_population::float,
           median_hh_income::float,
           pct_bachelors_plus::float,
           pct_owner_occupied::float,
           poverty_pct::float        AS acs_poverty_pct,
           pct_65_plus::float
    FROM municipal_census_acs
    ORDER BY municipality, acs_year DESC
"""

# ── Municipal finance ────────────────────────────────────────────────────────
FA_MUNI_REVENUES = """
    SELECT municipality, fiscal_year,
           taxes::float            AS muni_tax_rev,
           total_revenues::float   AS muni_total_rev
    FROM municipal_revenues
    WHERE fiscal_year = :yr
"""

FA_ASSESSED = """
    SELECT municipality, fiscal_year,
           commercial_av::float                                      AS commercial_av,
           res_av::float                                             AS res_av,
           (commercial_av::float + industrial_av::float)             AS ci_av,
           total_av::float                                           AS total_av
    FROM municipal_assessed_values
    WHERE fiscal_year = :yr
"""

FA_TAX_RATES = """
    SELECT municipality, fiscal_year,
           residential::float AS res_tax_rate,
           commercial::float  AS com_tax_rate
    FROM municipal_tax_rates WHERE fiscal_year = :yr
"""

FA_GF_EXP = """
    SELECT DISTINCT ON (municipality)
           municipality,
           gf_expenditure_per_capita::float AS gf_exp_per_capita
    FROM municipal_gf_expenditures
    WHERE fiscal_year <= :yr
    ORDER BY municipality, fiscal_year DESC
"""

FA_CRIME = """
    SELECT jurisdiction_name AS municipality,
           AVG(crime_rate_per_100k::float) AS crime_rate,
           AVG(violent_crimes::float / NULLIF(population::float, 0) * 100000)
               AS violent_rate
    FROM municipal_crime
    WHERE year BETWEEN :yr_lo AND :yr_hi
    GROUP BY jurisdiction_name
"""

FA_NEW_GROWTH = """
    SELECT DISTINCT ON (municipality)
           municipality,
           (total_new_growth_value::float /
            NULLIF(total_av::float, 0) * 100) AS new_growth_pct_av
    FROM municipal_new_growth ng
    JOIN municipal_assessed_values av
      USING (municipality, fiscal_year)
    WHERE ng.fiscal_year <= :yr
    ORDER BY municipality, ng.fiscal_year DESC
"""

FA_INCOME_EQV = """
    SELECT DISTINCT ON (municipality)
           municipality,
           dor_income::float AS equalized_income
    FROM municipal_income_eqv
    WHERE fiscal_year <= :yr
    ORDER BY municipality, fiscal_year DESC
"""

# ── County-level context ─────────────────────────────────────────────────────
FA_COUNTY_HEALTH = """
    SELECT county_name,
           AVG(pct_fair_poor_health::float)        AS health_pct_fair_poor,
           AVG(avg_mentally_unhealthy_days::float) AS health_mental_days
    FROM county_health_rankings
    WHERE ranking_year >= :yr_lo
    GROUP BY county_name
"""

FA_COUNTY_UNEMP = """
    SELECT county_name,
           AVG(unemployment_rate::float) AS county_unemployment
    FROM county_unemployment
    WHERE year >= :yr_lo
    GROUP BY county_name
"""

FA_DISTRICT_COUNTY = """
    SELECT name AS district_name, county FROM districts WHERE county IS NOT NULL
"""

# ── Budget line-item shares (MA DLS Schedule A) ──────────────────────────────
FA_BUDGET_SHARE = """
    SELECT municipality AS district_name,
           ROUND(100.0 * education       / NULLIF(total_expenditures,0), 2) AS ed_budget_share,
           ROUND(100.0 * fixed_costs     / NULLIF(total_expenditures,0), 2) AS fixed_costs_pct,
           ROUND(100.0 * debt_service    / NULLIF(total_expenditures,0), 2) AS debt_service_pct,
           ROUND(100.0 * public_safety   / NULLIF(total_expenditures,0), 2) AS public_safety_pct,
           ROUND(100.0 * public_works    / NULLIF(total_expenditures,0), 2) AS public_works_pct
    FROM municipal_expenditures
    WHERE fiscal_year = :yr
"""
