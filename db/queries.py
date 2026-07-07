"""
SQL Query Library — MA School Data
===================================
Canonical, parameterised feature queries used by
saugus_factor_analysis.load_features().  Named FA_* (one per source table).
Most accept :yr (school/fiscal year); window queries take :yr_lo / :yr_hi.

Usage example:
    from db.queries import FA_MCAS_3_8
    from config import get_engine
    import pandas as pd
    from sqlalchemy import text

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(FA_MCAS_3_8), conn, params={"yr": 2024})
"""

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
