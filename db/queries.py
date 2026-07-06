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
