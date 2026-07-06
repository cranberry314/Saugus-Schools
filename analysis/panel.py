"""
panel.py — shared MA town×year panel data layer.

Builds the town-by-year panel (education, fiscal, crime and housing features,
aligned to outcomes with a configurable year lag) plus the 16-outcome taxonomy
and its category colours. This is the reusable DATA layer; it draws no charts.

Extracted from the retired policy_backtest.py (a lagged two-way fixed-effects
regression that has been removed) so factor_portfolio.py keeps its panel builder
and outcome definitions. Features are year-over-year CHANGES, and are shifted
forward by `lag` years to align with later outcomes — see load_panel.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text


# ---------------------------------------------------------------------------
# 16 outcome definitions
# ---------------------------------------------------------------------------
OUTCOMES = [
    # (key, label, category, higher_is_better)
    ("dropout_rate",         "Dropout Rate",               "Education", False),
    ("graduation_rate",      "Graduation Rate",            "Education", True),
    ("mcas_ela_pct",         "MCAS ELA Proficient %",      "Education", True),
    ("mcas_math_pct",        "MCAS Math Proficient %",     "Education", True),
    ("postsecondary_pct",    "Postsecondary Attendance %", "Education", True),
    ("sat_mean",             "SAT Mean Score",             "Education", True),
    ("crime_rate",           "Crime Rate / 100k",          "Safety",    False),
    ("violent_crime_rate",   "Violent Crime Rate / 100k",  "Safety",    False),
    ("crash_rate",           "Crash Rate / 1k pop",        "Safety",    False),
    ("injury_crash_rate",    "Injury Crash Rate / 1k pop", "Safety",    False),
    ("absenteeism_rate",     "Chronic Absenteeism %",      "Community", False),
    ("enrollment_growth",    "Enrollment Growth %",        "Community", True),
    ("poverty_pct",          "Poverty Rate %",             "Community", False),
    ("real_zhvi_growth",     "Real Home Value Growth %",   "Market",    True),
    ("real_rev_growth",      "Real Revenue Growth %",      "Fiscal",    True),
    ("mcas_ela_residual",    "MCAS ELA Residual (SES adj)","Education", True),
]

# Feature labels for display
# All features are year-over-year changes to ask "did INCREASING X predict better outcomes?"
# rather than "do high-X towns do better?" — the latter conflates cause and effect
# (e.g. struggling districts reactively hire more teachers, creating spurious negative correlation).


def load_panel(engine, lag: int) -> pd.DataFrame:
    """
    Build a (town × year) panel DataFrame with all outcomes and features.
    'year' is the outcome year; features are lagged by `lag` years.
    """
    with engine.connect() as conn:
        frames = {}

        # --- Per-pupil expenditure (feature) ---
        frames["pp"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   amount AS pp_exp
            FROM per_pupil_expenditure
            WHERE category = 'Total In-District Expenditures'
              AND district_name IS NOT NULL
        """), conn)

        # --- Staffing (feature) ---
        frames["staff"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   fte AS teachers_fte
            FROM staffing
            WHERE category ILIKE '%teacher%'
              AND district_name IS NOT NULL
        """), conn)

        # --- Enrollment (feature + outcome) ---
        frames["enroll"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   total AS enrollment
            FROM enrollment
            WHERE grade = 'Total'
              AND district_name IS NOT NULL
        """), conn)

        # --- Chapter 70 (feature) ---
        frames["ch70"] = pd.read_sql(text("""
            SELECT district_name AS town, fiscal_year AS year,
                   chapter70_aid AS ch70_aid
            FROM district_chapter70
            WHERE district_name IS NOT NULL
        """), conn)

        # --- Municipal revenues (feature + outcome) ---
        frames["rev"] = pd.read_sql(text("""
            SELECT municipality AS town, fiscal_year AS year,
                   total_revenues
            FROM municipal_revenues
            WHERE municipality IS NOT NULL
        """), conn)

        # --- Municipal expenditures (education share, civic investment, debt) ---
        frames["exp"] = pd.read_sql(text("""
            SELECT municipality AS town, fiscal_year AS year,
                   education          AS education_exp,
                   public_works       AS public_works_exp,
                   debt_service       AS debt_service_exp,
                   public_safety      AS public_safety_exp,
                   total_expenditures
            FROM municipal_expenditures
            WHERE municipality IS NOT NULL
        """), conn)

        # --- ACS demographics (feature + outcome) ---
        frames["acs"] = pd.read_sql(text("""
            SELECT municipality AS town, acs_year AS year,
                   total_population, median_hh_income AS median_household_income,
                   poverty_pct
            FROM municipal_census_acs
            WHERE municipality IS NOT NULL
        """), conn)

        # --- Zillow (outcome) — aggregate monthly to annual average ---
        frames["zillow"] = pd.read_sql(text("""
            SELECT region_name AS town, data_year AS year,
                   AVG(zhvi) AS zhvi_all_homes
            FROM municipal_zillow_housing
            WHERE region_name IS NOT NULL AND zhvi IS NOT NULL
            GROUP BY region_name, data_year
        """), conn)

        # --- CPI (Boston MSA index for deflating) ---
        frames["cpi"] = pd.read_sql(text("""
            SELECT calendar_year AS year, cpi_index FROM cpi_boston_msa ORDER BY calendar_year
        """), conn)

        # --- MCAS outcomes ---
        frames["mcas"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   subject,
                   meeting_exceeding_pct AS proficient_pct
            FROM mcas_results
            WHERE grade = '10'
              AND student_group = 'All Students'
              AND subject IN ('ELA','MATH')
              AND school_name = district_name
              AND district_name IS NOT NULL
        """), conn)

        # --- Graduation & dropout ---
        frames["grad"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   four_year_grad_pct AS graduation_rate
            FROM graduation_rates
            WHERE student_group = 'All'
              AND district_name IS NOT NULL
        """), conn)

        frames["dropout"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   dropout_pct AS dropout_rate
            FROM district_dropout
            WHERE district_name IS NOT NULL
        """), conn)

        # --- Postsecondary (no student_group column — all students) ---
        frames["post"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   attending_pct AS postsecondary_pct
            FROM district_postsecondary
            WHERE district_name IS NOT NULL
        """), conn)

        # --- SAT (combine EBRW + Math) ---
        frames["sat"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   (mean_ebrw + mean_math) AS sat_mean
            FROM district_sat_scores
            WHERE district_name IS NOT NULL
        """), conn)

        # --- Absenteeism (district-level: aggregate over schools) ---
        frames["absent"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   AVG(chronic_absenteeism_pct) AS absenteeism_rate
            FROM attendance
            WHERE student_group = 'All'
              AND district_name IS NOT NULL
            GROUP BY district_name, school_year
        """), conn)

        # --- Crime (outcome + feature) ---
        frames["crime"] = pd.read_sql(text("""
            SELECT jurisdiction_name AS town, year,
                   crime_rate_per_100k AS crime_rate,
                   CASE WHEN population > 0
                        THEN violent_crimes::float / population * 100000
                        ELSE NULL END AS violent_crime_rate
            FROM municipal_crime
            WHERE jurisdiction_name IS NOT NULL
              AND year >= 2020
        """), conn)

        # --- Crashes (outcome + feature) ---
        frames["crash"] = pd.read_sql(text("""
            SELECT c.city_town_name AS town, c.year,
                   CASE WHEN p.total_population > 0
                        THEN c.total_crashes::float / p.total_population * 1000
                        ELSE NULL END AS crash_rate,
                   CASE WHEN p.total_population > 0
                        THEN (c.injury_crashes + c.fatal_crashes)::float
                             / p.total_population * 1000
                        ELSE NULL END AS injury_crash_rate
            FROM municipal_crashes c
            LEFT JOIN (
                SELECT municipality, acs_year, total_population
                FROM municipal_census_acs
            ) p ON LOWER(TRIM(c.city_town_name)) = LOWER(TRIM(p.municipality))
               AND c.year BETWEEN p.acs_year - 1 AND p.acs_year + 1
            WHERE c.city_town_name IS NOT NULL
        """), conn)

    # -----------------------------------------------------------------------
    # Normalise town names to Title Case throughout
    # -----------------------------------------------------------------------
    for key, df in frames.items():
        if "town" in df.columns:
            df["town"] = df["town"].str.strip().str.title()
        frames[key] = df

    # -----------------------------------------------------------------------
    # Build CPI lookup for deflating
    # -----------------------------------------------------------------------
    cpi = frames["cpi"].set_index("year")["cpi_index"].to_dict()
    base_cpi = cpi.get(max(cpi.keys()), 1.0)

    def deflate(series: pd.Series, year_series: pd.Series) -> pd.Series:
        return series * year_series.map(lambda y: base_cpi / cpi.get(y, np.nan))

    # -----------------------------------------------------------------------
    # Construct feature panel
    # -----------------------------------------------------------------------
    pp = frames["pp"].rename(columns={"pp_exp": "pp_exp_raw"})
    enroll = frames["enroll"]
    staff = frames["staff"]
    ch70 = frames["ch70"]
    rev = frames["rev"]
    exp_df = frames["exp"]
    acs = frames["acs"]

    # Per-pupil (real)
    pp["pp_exp"] = deflate(pp["pp_exp_raw"], pp["year"])
    pp["log_pp_exp"] = np.log(pp["pp_exp"].clip(lower=1))

    # Teachers per 1000 students
    feat = pp[["town","year","log_pp_exp"]].copy()
    feat = feat.merge(
        staff.groupby(["town","year"])["teachers_fte"].sum().reset_index(),
        on=["town","year"], how="left"
    )
    feat = feat.merge(
        enroll[["town","year","enrollment"]],
        on=["town","year"], how="left"
    )
    feat["teachers_per1k"] = feat["teachers_fte"] / feat["enrollment"].clip(lower=1) * 1000

    # Ch70 per pupil (real, $k)
    ch70_m = ch70.merge(enroll[["town","year","enrollment"]], on=["town","year"], how="left")
    ch70_m["ch70_per_pupil"] = (
        deflate(ch70_m["ch70_aid"], ch70_m["year"]) / ch70_m["enrollment"].clip(lower=1) / 1000
    )
    feat = feat.merge(ch70_m[["town","year","ch70_per_pupil"]], on=["town","year"], how="left")

    # Education % of municipal budget
    exp_df["ed_pct_budget"] = (
        exp_df["education_exp"] / exp_df["total_expenditures"].clip(lower=1) * 100
    )
    # Debt service % of budget (fiscal discipline: lower = less burdened)
    exp_df["debt_pct_budget"] = (
        exp_df["debt_service_exp"] / exp_df["total_expenditures"].clip(lower=1) * 100
    )
    # Public works per capita (real) — infrastructure/civic investment
    exp_df = exp_df.merge(acs[["town","year","total_population"]], on=["town","year"], how="left")
    exp_df["public_works_pc"] = (
        deflate(exp_df["public_works_exp"], exp_df["year"]) / exp_df["total_population"].clip(lower=1)
    )
    # Public safety per capita (real) — police/fire investment
    exp_df["public_safety_pc"] = (
        deflate(exp_df["public_safety_exp"], exp_df["year"]) / exp_df["total_population"].clip(lower=1)
    )
    feat = feat.merge(
        exp_df[["town","year","ed_pct_budget","debt_pct_budget","public_works_pc","public_safety_pc"]],
        on=["town","year"], how="left"
    )

    # Municipal revenue per capita (real, log)
    rev_m = rev.merge(acs[["town","year","total_population"]], on=["town","year"], how="left")
    rev_m["muni_rev_pc"] = deflate(rev_m["total_revenues"], rev_m["year"]) / rev_m["total_population"].clip(lower=1)
    rev_m["log_muni_rev_pc"] = np.log(rev_m["muni_rev_pc"].clip(lower=1))
    feat = feat.merge(rev_m[["town","year","log_muni_rev_pc"]], on=["town","year"], how="left")

    # -----------------------------------------------------------------------
    # Year-over-year CHANGE features
    # Asking "did INCREASING X predict better outcomes?" avoids reverse causality:
    # struggling towns reactively spend more (reactive hiring, aid inflow), which
    # creates a spurious negative correlation between LEVELS and outcomes.
    # pct_change() for dollar/ratio features; diff() for features already in %.
    # -----------------------------------------------------------------------
    # % change in real per-pupil spending
    pp_delta = pp[["town","year","pp_exp"]].sort_values(["town","year"]).copy()
    pp_delta["delta_pp_exp"] = pp_delta.groupby("town")["pp_exp"].pct_change() * 100
    feat = feat.merge(pp_delta[["town","year","delta_pp_exp"]], on=["town","year"], how="left")

    # % change in teachers per 1k students
    tp_delta = feat[["town","year","teachers_per1k"]].sort_values(["town","year"]).copy()
    tp_delta["delta_teachers_per1k"] = tp_delta.groupby("town")["teachers_per1k"].pct_change() * 100
    feat = feat.merge(tp_delta[["town","year","delta_teachers_per1k"]], on=["town","year"], how="left")

    # % change in Ch70 per pupil (real)
    ch70_delta = ch70_m[["town","year","ch70_per_pupil"]].sort_values(["town","year"]).copy()
    ch70_delta["delta_ch70_per_pupil"] = ch70_delta.groupby("town")["ch70_per_pupil"].pct_change() * 100
    feat = feat.merge(ch70_delta[["town","year","delta_ch70_per_pupil"]], on=["town","year"], how="left")

    # Budget share changes (already %, so use pp diff not pct_change)
    exp_delta = exp_df[["town","year","ed_pct_budget","debt_pct_budget",
                         "public_works_pc","public_safety_pc"]].sort_values(["town","year"]).copy()
    exp_delta["delta_ed_pct_budget"]    = exp_delta.groupby("town")["ed_pct_budget"].diff()
    exp_delta["delta_debt_pct_budget"]  = exp_delta.groupby("town")["debt_pct_budget"].diff()
    exp_delta["delta_public_works_pc"]  = exp_delta.groupby("town")["public_works_pc"].pct_change() * 100
    exp_delta["delta_public_safety_pc"] = exp_delta.groupby("town")["public_safety_pc"].pct_change() * 100
    feat = feat.merge(
        exp_delta[["town","year","delta_ed_pct_budget","delta_debt_pct_budget",
                   "delta_public_works_pc","delta_public_safety_pc"]],
        on=["town","year"], how="left"
    )

    # % change in real municipal revenue per capita
    rev_delta = rev_m[["town","year","muni_rev_pc"]].sort_values(["town","year"]).copy()
    rev_delta["delta_muni_rev_pc"] = rev_delta.groupby("town")["muni_rev_pc"].pct_change() * 100
    feat = feat.merge(rev_delta[["town","year","delta_muni_rev_pc"]], on=["town","year"], how="left")

    # Winsorise extreme % changes at ±50 pct to dampen one-time budget shocks
    pct_cols = ["delta_pp_exp","delta_teachers_per1k","delta_ch70_per_pupil",
                "delta_muni_rev_pc","delta_public_works_pc","delta_public_safety_pc"]
    for c in pct_cols:
        if c in feat.columns:
            feat[c] = feat[c].clip(-50, 50)
    pp_cols = ["delta_ed_pct_budget","delta_debt_pct_budget"]
    for c in pp_cols:
        if c in feat.columns:
            feat[c] = feat[c].clip(-15, 15)

    # -----------------------------------------------------------------------
    # Construct outcome panel
    # -----------------------------------------------------------------------
    # Start from feature town×year grid, then build outcomes at year+lag
    outcome_rows = []

    # Education outcomes (school_year)
    for df, col, out_col in [
        (frames["grad"],    "graduation_rate",   "graduation_rate"),
        (frames["dropout"], "dropout_rate",       "dropout_rate"),
        (frames["post"],    "postsecondary_pct",  "postsecondary_pct"),
        (frames["sat"],     "sat_mean",           "sat_mean"),
        (frames["absent"],  "absenteeism_rate",   "absenteeism_rate"),
    ]:
        outcome_rows.append(df[["town","year",col]].rename(columns={col: out_col}))

    # MCAS — pivot ELA and MATH
    mcas_ela  = frames["mcas"][frames["mcas"]["subject"]=="ELA"][["town","year","proficient_pct"]].rename(columns={"proficient_pct":"mcas_ela_pct"})
    mcas_math = frames["mcas"][frames["mcas"]["subject"]=="MATH"][["town","year","proficient_pct"]].rename(columns={"proficient_pct":"mcas_math_pct"})

    # MCAS ELA residual: detrend by income
    ela_acs = mcas_ela.merge(acs[["town","year","median_household_income","poverty_pct"]], on=["town","year"], how="left")
    if len(ela_acs.dropna()) > 50:
        from scipy import stats
        ela_valid = ela_acs.dropna(subset=["mcas_ela_pct","median_household_income"])
        slope, intercept, *_ = stats.linregress(
            np.log(ela_valid["median_household_income"].clip(lower=1)),
            ela_valid["mcas_ela_pct"]
        )
        ela_acs["mcas_ela_residual"] = (
            ela_acs["mcas_ela_pct"]
            - (slope * np.log(ela_acs["median_household_income"].clip(lower=1)) + intercept)
        )
    else:
        ela_acs["mcas_ela_residual"] = ela_acs["mcas_ela_pct"]

    outcome_rows.append(mcas_ela)
    outcome_rows.append(mcas_math)
    outcome_rows.append(ela_acs[["town","year","mcas_ela_residual"]])

    # Zillow real growth (YoY %)
    zillow = frames["zillow"].sort_values(["town","year"])
    zillow["zhvi_real"] = deflate(zillow["zhvi_all_homes"], zillow["year"])
    zillow["real_zhvi_growth"] = zillow.groupby("town")["zhvi_real"].pct_change() * 100
    outcome_rows.append(zillow[["town","year","real_zhvi_growth"]])

    # Revenue real growth (YoY %)
    rev_out = rev_m[["town","year","muni_rev_pc"]].sort_values(["town","year"])
    rev_out["real_rev_growth"] = rev_out.groupby("town")["muni_rev_pc"].pct_change() * 100
    outcome_rows.append(rev_out[["town","year","real_rev_growth"]])

    # ACS poverty (direct level)
    outcome_rows.append(acs[["town","year","poverty_pct"]].rename(columns={"poverty_pct":"poverty_pct"}))

    # Enrollment growth (YoY %)
    enroll_out = enroll.sort_values(["town","year"])
    enroll_out["enrollment_growth"] = enroll_out.groupby("town")["enrollment"].pct_change() * 100
    outcome_rows.append(enroll_out[["town","year","enrollment_growth"]])

    # Crime outcomes
    outcome_rows.append(frames["crime"][["town","year","crime_rate","violent_crime_rate"]])

    # Crash outcomes
    outcome_rows.append(frames["crash"][["town","year","crash_rate","injury_crash_rate"]])

    # -----------------------------------------------------------------------
    # Merge all outcomes into one wide outcome frame
    # -----------------------------------------------------------------------
    out = outcome_rows[0]
    for df in outcome_rows[1:]:
        out = out.merge(df, on=["town","year"], how="outer")

    # -----------------------------------------------------------------------
    # Merge features (lagged) with outcomes
    # -----------------------------------------------------------------------
    # Shift feature year forward by lag to align with future outcomes
    feat_lagged = feat.copy()
    feat_lagged["year"] = feat_lagged["year"] + lag

    # Lagged crime as feature
    crime_feat = frames["crime"][["town","year","crime_rate"]].rename(columns={"crime_rate":"crime_rate_feat"})
    crime_feat["year"] = crime_feat["year"] + lag
    crash_feat = frames["crash"][["town","year","crash_rate"]].rename(columns={"crash_rate":"crash_rate_feat"})
    crash_feat["year"] = crash_feat["year"] + lag

    feat_lagged = feat_lagged.merge(crime_feat, on=["town","year"], how="left")
    feat_lagged = feat_lagged.merge(crash_feat, on=["town","year"], how="left")

    # Outer join — each regression will dropna() on its own columns,
    # maximising observations per outcome rather than forcing one intersection.
    panel = out.merge(feat_lagged, on=["town","year"], how="outer")

    # Collapse duplicate (town, year) rows by taking the mean of numeric columns.
    # Duplicates arise when multiple source tables have different rows for the same
    # town×year (e.g. crime joined to education outcomes across different year ranges).
    num_cols = panel.select_dtypes(include=[np.number]).columns.tolist()
    panel = panel.groupby(["town","year"], as_index=False)[num_cols].mean()

    return panel


# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Shared style constants
# ---------------------------------------------------------------------------
CAT_COLOURS = {
    "Education": "#4A90D9",
    "Safety":    "#E05C4A",
    "Community": "#5CB85C",
    "Market":    "#F0AD4E",
    "Fiscal":    "#9B59B6",
}
CAT_LIGHT = {          # pastel row-background versions
    "Education": "#D6E9F8",
    "Safety":    "#FADBD8",
    "Community": "#D5F5E3",
    "Market":    "#FDEBD0",
    "Fiscal":    "#E8DAEF",
}

