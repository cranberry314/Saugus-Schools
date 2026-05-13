"""
Town Policy Backtest — 16-outcome panel regression analysis.

Tests whether lagged policy inputs (education spending, staffing, Chapter 70 aid,
fiscal composition, crime) predict 16 distinct town-level outcomes.

Two panels:
  Long  (2009–2024): education/fiscal features only — ~5,600 observations
  Short (2020–2024): adds crime + crash features — ~1,750 observations

Method: Two-way fixed effects OLS (town FE + year FE), clustered SEs at town level.

Output: Reports/backtest_report.pdf
        Reports/backtest_results.csv

Run:
    python analysis/backtest.py
    python analysis/backtest.py --lag 3        # default lag in years
    python analysis/backtest.py --lag 1
    python analysis/backtest.py --lag 5
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import warnings
import argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import TwoSlopeNorm
from sqlalchemy import text
from config import get_engine

warnings.filterwarnings("ignore")

OUTPUT_PDF = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports", "backtest_report.pdf")
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports", "backtest_results.csv")

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
FEATURE_LABELS = {
    "log_pp_exp":        "Log Per-Pupil Spending",
    "teachers_per1k":    "Teachers / 1k Students",
    "ch70_per_pupil":    "Ch70 Aid / Pupil ($k)",
    "ed_pct_budget":     "Ed % of Muni Budget",
    "log_muni_rev_pc":   "Log Muni Revenue / Capita",
    "public_works_pc":   "Public Works / Capita",
    "debt_pct_budget":   "Debt Service % of Budget",
    "public_safety_pc":  "Public Safety / Capita",
    "crime_rate_feat":   "Crime Rate / 100k (t-lag)",
    "crash_rate_feat":   "Crash Rate / 1k pop (t-lag)",
}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

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
# Panel regression (two-way FE)
# ---------------------------------------------------------------------------

def run_regression(panel: pd.DataFrame, outcome: str, features: list[str]) -> dict:
    """
    Two-way fixed effects OLS: outcome ~ features + town FE + year FE.
    Feature list is pre-screened by the caller; this function just runs the model.
    """
    from linearmodels.panel import PanelOLS
    import warnings

    use_features = [f for f in features if f in panel.columns]
    if not use_features:
        return {"n_obs": 0, "n_towns": 0}

    cols = [outcome] + use_features
    sub = panel[["town","year"] + cols].dropna()

    if len(sub) < 30 or sub["town"].nunique() < 5:
        return {"n_obs": len(sub), "n_towns": sub["town"].nunique() if len(sub) else 0}

    sub = sub.set_index(["town","year"])
    y = sub[outcome]
    X = sm_add_constant(sub[use_features])

    try:
        from linearmodels.panel import PanelOLS
        mod = PanelOLS(y, X, entity_effects=True, time_effects=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = mod.fit(cov_type="clustered", cluster_entity=True)

        result = {
            "n_obs":   int(res.nobs),
            "n_towns": sub.index.get_level_values("town").nunique(),
            "r2_within": float(res.rsquared_within) if hasattr(res, "rsquared_within") else np.nan,
        }
        for feat in use_features:
            if feat in res.params.index:
                result[f"{feat}_coef"]  = float(res.params[feat])
                result[f"{feat}_tstat"] = float(res.tstats[feat])
                result[f"{feat}_pval"]  = float(res.pvalues[feat])
        return result
    except Exception as e:
        return {"error": str(e), "n_obs": len(sub),
                "n_towns": sub.index.get_level_values("town").nunique()}


def sm_add_constant(df: pd.DataFrame) -> pd.DataFrame:
    """Add constant column (linearmodels needs it explicitly)."""
    df = df.copy()
    df.insert(0, "const", 1.0)
    return df


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_backtest(engine, lag: int = 3) -> pd.DataFrame:
    print(f"[backtest] Building panel (lag={lag} years)...")
    panel = load_panel(engine, lag)
    print(f"[backtest] Panel: {len(panel):,} rows, {panel['town'].nunique()} towns, "
          f"years {panel['year'].min()}–{panel['year'].max()}")

    # All policy input features — ordered from widest to narrowest coverage so the
    # greedy selector below can add them one at a time without collapsing the sample.
    all_features = [
        "log_pp_exp", "teachers_per1k", "ch70_per_pupil",      # DESE — ~335 districts
        "ed_pct_budget", "log_muni_rev_pc",                    # Schedule A — fiscal
        "public_works_pc", "debt_pct_budget", "public_safety_pc",  # Schedule A — civic
    ]
    MIN_TOWNS = 30  # never run a regression with fewer unique towns than this

    results = []
    for out_key, out_label, category, higher_is_better in OUTCOMES:
        if out_key not in panel.columns:
            print(f"  SKIP {out_key} — not in panel")
            continue

        # Greedy feature selection: add features in order, keeping only those
        # that maintain at least MIN_TOWNS unique towns in the joint sample.
        features = []
        for f in all_features:
            if f not in panel.columns:
                continue
            trial = panel[["town", "year", out_key] + features + [f]].dropna()
            if trial["town"].nunique() >= MIN_TOWNS:
                features.append(f)
        if not features:
            # fallback: use any feature that has ≥ MIN_TOWNS pairwise with outcome
            features = [f for f in all_features if f in panel.columns and
                        panel[["town", out_key, f]].dropna()["town"].nunique() >= MIN_TOWNS]

        print(f"  {out_label}...", end="", flush=True)
        r = run_regression(panel, out_key, features)
        r["outcome_key"]   = out_key
        r["outcome_label"] = out_label
        r["category"]      = category
        results.append(r)

        n = r.get("n_obs", 0)
        t = r.get("n_towns", 0)
        if "r2_within" in r:
            print(f" n={n:,} towns={t} R²={r.get('r2_within', np.nan):.3f}")
        else:
            err = r.get("error", "no data")
            print(f" n={n} towns={t} ERR: {err[:80]}")

    return pd.DataFrame(results)


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

def _confidence_label(abs_t: float) -> str:
    """Plain-English confidence description."""
    if abs_t > 2.58: return "★★★ 99% confident"
    if abs_t > 1.96: return "★★  95% confident"
    if abs_t > 1.645: return "★    90% confident"
    return "not significant"


# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------

def make_plain_english_legend() -> plt.Figure:
    """
    A stand-alone slide explaining the charts in plain English.
    Intended for audiences without a statistics background.
    """
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")

    ax.text(0.5, 0.96, "How to Read These Charts", ha="center", va="top",
            fontsize=18, fontweight="bold", transform=ax.transAxes)

    sections = [
        ("What are we testing?",
         "Each chart asks: when a town changed its spending or policies,\n"
         "did its outcomes improve a few years later?\n"
         "We look 1 year, 3 years, and 5 years into the future."),

        ("What do the numbers mean?",
         "Each cell shows a strength score — how strongly a policy predicts a good outcome.\n"
         "The number and the colour always mean the same thing:\n"
         "  Positive number / Green = this policy predicts BETTER outcomes\n"
         "  Negative number / Red   = this policy predicts WORSE outcomes\n"
         "The scores are already adjusted: for outcomes where lower is better\n"
         "(like dropout rate, crime, poverty), the score is flipped so that\n"
         "a policy that reduces crime shows as positive and green — not negative and red."),

        ("What do the stars mean?",
         "★★★  99% confident — almost certainly a real effect, not random chance\n"
         "★★   95% confident — strong evidence of a real effect\n"
         "★     90% confident — moderate evidence; treat with some caution\n"
         "(no stars) — not enough evidence; could easily be random"),

        ("What do the column colours mean?",
         "Each outcome (column) is colour-coded by type:\n"
         "  Blue = Education outcomes (graduation, test scores, dropouts)\n"
         "  Red = Safety outcomes (crime, crashes)\n"
         "  Green = Community outcomes (poverty, absenteeism, enrollment)\n"
         "  Orange = Market outcomes (home values)\n"
         "  Purple = Fiscal outcomes (municipal revenue)"),

        ("What is collinearity?",
         "Some policy inputs move together — e.g. towns that spend more per pupil\n"
         "also tend to receive more state aid. When two inputs are highly correlated,\n"
         "it is hard to tell which one is doing the work. We flag these pairs\n"
         "with ⚠ so you know to interpret those results cautiously."),

        ("Bottom line",
         "Look for consistent green ★★ or ★★★ cells across multiple rows.\n"
         "That means the policy input reliably predicts better outcomes\n"
         "across many different measures — not just one cherry-picked metric."),
    ]

    y = 0.88
    for title, body in sections:
        ax.text(0.05, y, title, ha="left", va="top", fontsize=11,
                fontweight="bold", color="#222", transform=ax.transAxes)
        y -= 0.04
        ax.text(0.07, y, body, ha="left", va="top", fontsize=9.5,
                color="#444", transform=ax.transAxes, linespacing=1.6)
        y -= (body.count("\n") + 1) * 0.042 + 0.015

    plt.tight_layout()
    return fig


def make_tstat_matrix(results: pd.DataFrame, lag: int,
                      collinear_pairs: list[tuple[str,str]] | None = None) -> list[plt.Figure]:
    """
    Single combined heatmap. T-statistics are sign-adjusted before display:
    outcomes where lower is better (crime, dropout, poverty…) have their
    t-stat multiplied by -1. This means the NUMBER in every cell and the
    COLOUR both carry the same message: positive/green = good, negative/red = bad.
    """
    all_features = list(FEATURE_LABELS.keys())

    # Build sign lookup: +1 if higher is better, -1 if lower is better
    sign_map = {out_label: (1 if hib else -1)
                for _, out_label, _, hib in OUTCOMES}

    outcomes = [(r["outcome_label"], r["category"]) for _, r in results.iterrows()
                if "n_obs" in r and r.get("n_obs", 0) >= 30]
    if not outcomes:
        return []

    # Build full matrix first, then drop feature rows that are entirely NaN
    mat_full = np.full((len(all_features), len(outcomes)), np.nan)
    for j, (out_label, _) in enumerate(outcomes):
        row  = results[results["outcome_label"] == out_label].iloc[0]
        sign = sign_map.get(out_label, 1)
        for i, feat in enumerate(all_features):
            raw = row.get(f"{feat}_tstat", np.nan)
            mat_full[i, j] = sign * raw if not np.isnan(raw) else np.nan

    # Only show features that have at least one non-NaN result
    used_mask = ~np.all(np.isnan(mat_full), axis=1)
    features  = [f for f, used in zip(all_features, used_mask) if used]
    mat       = mat_full[used_mask, :]

    n_feat = len(features)
    n_out  = len(outcomes)

    fig, ax = plt.subplots(figsize=(max(14, n_out * 1.1 + 3), max(5, n_feat * 0.9 + 3)))

    vmax = 3.0
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    im = ax.imshow(mat, cmap="RdYlGn", norm=norm, aspect="auto")

    # Column backgrounds by outcome category
    for j, (_, cat) in enumerate(outcomes):
        ax.axvspan(j - 0.5, j + 0.5, color=CAT_LIGHT.get(cat, "#f5f5f5"),
                   alpha=0.5, zorder=0)

    # Cell labels: sign-adjusted value + stars
    for i in range(n_feat):
        for j in range(n_out):
            t = mat[i, j]
            if np.isnan(t):
                ax.text(j, i, "—", ha="center", va="center", fontsize=7.5, color="#aaa")
            else:
                stars = ("★★★" if abs(t) > 2.58 else
                         "★★"  if abs(t) > 1.96 else
                         "★"   if abs(t) > 1.645 else "")
                cell_label = f"{t:+.1f}\n{stars}" if stars else f"{t:+.1f}"
                fg = "white" if abs(t) > 2.0 else "black"
                ax.text(j, i, cell_label, ha="center", va="center", fontsize=7,
                        fontweight="bold" if stars else "normal", color=fg,
                        linespacing=1.3)

    # Outcome labels across the top, colour-coded by category
    ax.set_xticks(range(n_out))
    ax.set_xticklabels([lbl for lbl, _ in outcomes], rotation=40, ha="left", fontsize=9)
    ax.xaxis.set_label_position("top")
    ax.xaxis.tick_top()
    for tick, (_, cat) in zip(ax.get_xticklabels(), outcomes):
        tick.set_color(CAT_COLOURS.get(cat, "#333"))
        tick.set_fontweight("bold")

    # Policy input labels on left; collinear ones in red
    collinear_feats = {f for pair in (collinear_pairs or []) for f in pair}
    ax.set_yticks(range(n_feat))
    ax.set_yticklabels([FEATURE_LABELS[f] for f in features], fontsize=9)
    for tick, feat in zip(ax.get_yticklabels(), features):
        tick.set_color("#C0392B" if feat in collinear_feats else "#222")
        tick.set_fontweight("bold" if feat in collinear_feats else "normal")

    lag_desc = ("near-term (1 year out)" if lag == 1 else
                "medium-term (3 years out)" if lag == 3 else
                "long-term (5 years out)")
    ax.set_title(
        f"Does this policy predict better town outcomes? — {lag_desc}\n"
        "Green / positive number = policy predicts IMPROVEMENT  ·  "
        "Red / negative number = policy predicts WORSENING  ·  "
        "Stars = confidence level\n"
        "(Numbers already adjusted: a policy that reduces crime shows positive, not negative)",
        fontsize=10, fontweight="bold", pad=16, loc="left"
    )

    legend_patches = [mpatches.Patch(color=CAT_COLOURS[c], label=c) for c in CAT_COLOURS
                      if any(cat == c for _, cat in outcomes)]
    ax.legend(handles=legend_patches, loc="upper left", fontsize=8,
              bbox_to_anchor=(0, -0.06), ncol=len(legend_patches),
              title="Outcome category", title_fontsize=8, framealpha=0.9)

    plt.colorbar(im, ax=ax, orientation="vertical", pad=0.02, shrink=0.8,
                 label="Positive (green) = predicts better outcomes  |  Negative (red) = predicts worse outcomes")

    if collinear_pairs:
        warn_text = ("⚠ Red row labels share a strong correlation with another input — "
                     "hard to tell which one is doing the work: " +
                     ", ".join(f"{FEATURE_LABELS.get(a,'?')} ↔ {FEATURE_LABELS.get(b,'?')}"
                               for a, b in collinear_pairs))
        fig.text(0.01, 0.01, warn_text, fontsize=7.5, color="#C0392B", ha="left", va="bottom")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    return [fig]

    # Wide figure: outcomes across the top, features down the side
    fig, ax = plt.subplots(figsize=(max(14, n_out * 1.1 + 3), max(6, n_feat * 0.75 + 3)))

    vmax = 3.0
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    im = ax.imshow(mat, cmap="RdYlGn", norm=norm, aspect="auto")

    # Colour-coded column backgrounds by outcome category
    for j, (_, cat) in enumerate(outcomes):
        ax.axvspan(j - 0.5, j + 0.5, color=CAT_LIGHT.get(cat, "#f5f5f5"),
                   alpha=0.5, zorder=0)

    # Cell labels
    for i in range(n_feat):
        for j in range(n_out):
            t = mat[i, j]
            if np.isnan(t):
                ax.text(j, i, "—", ha="center", va="center", fontsize=7.5, color="#aaa")
            else:
                stars = ("★★★" if abs(t) > 2.58 else
                         "★★"  if abs(t) > 1.96 else
                         "★"   if abs(t) > 1.645 else "")
                cell_label = f"{t:+.1f}\n{stars}" if stars else f"{t:+.1f}"
                fg = "white" if abs(t) > 2.0 else "black"
                ax.text(j, i, cell_label, ha="center", va="center", fontsize=7,
                        fontweight="bold" if stars else "normal", color=fg,
                        linespacing=1.3)

    # Outcome labels across the TOP (X-axis), colour-coded by category
    ax.set_xticks(range(n_out))
    ax.set_xticklabels([lbl for lbl, _ in outcomes], rotation=40, ha="left", fontsize=9)
    ax.xaxis.set_label_position("top")
    ax.xaxis.tick_top()
    for tick, (_, cat) in zip(ax.get_xticklabels(), outcomes):
        tick.set_color(CAT_COLOURS.get(cat, "#333"))
        tick.set_fontweight("bold")

    # Policy input labels on the LEFT (Y-axis), red for collinear
    collinear_feats = {f for pair in (collinear_pairs or []) for f in pair}
    ax.set_yticks(range(n_feat))
    ax.set_yticklabels([FEATURE_LABELS[f] for f in features], fontsize=9)
    for tick, feat in zip(ax.get_yticklabels(), features):
        if feat in collinear_feats:
            tick.set_color("#C0392B")
        tick.set_fontweight("bold" if feat in collinear_feats else "normal")

    lag_desc = ("near-term (1 year out)" if lag == 1 else
                "medium-term (3 years out)" if lag == 3 else
                "long-term (5 years out)")
    ax.set_title(
        f"Does this policy predict better outcomes — {lag_desc}?\n"
        "Policy inputs → rows  |  Outcomes → columns  |  "
        "Green = improvement  ·  Red = worsening  ·  Stars = confidence level",
        fontsize=10, fontweight="bold", pad=16, loc="left"
    )

    # Colour legend for outcome categories — below the chart
    legend_patches = [mpatches.Patch(color=CAT_COLOURS[c], label=c) for c in CAT_COLOURS]
    ax.legend(handles=legend_patches, loc="upper left", fontsize=8,
              bbox_to_anchor=(0, -0.06), ncol=len(CAT_COLOURS),
              title="Outcome category colours", title_fontsize=8, framealpha=0.9)

    plt.colorbar(im, ax=ax, orientation="vertical", pad=0.02, shrink=0.8,
                 label="Strength score  (positive/green = positive association, negative/red = negative association)")

    # Collinearity footnote
    if collinear_pairs:
        warn_text = (
            "⚠ Red row labels share a strong correlation with another policy input — "
            "hard to tell which one is doing the work: " +
            ", ".join(f"{FEATURE_LABELS.get(a,'?')} ↔ {FEATURE_LABELS.get(b,'?')}"
                      for a, b in collinear_pairs)
        )
        fig.text(0.01, 0.01, warn_text, fontsize=7.5, color="#C0392B",
                 ha="left", va="bottom")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    return fig


def make_r2_table(results: pd.DataFrame) -> plt.Figure:
    valid = results.dropna(subset=["r2_within"] if "r2_within" in results.columns else [])
    if valid.empty:
        return None

    fig, ax = plt.subplots(figsize=(11, max(4, len(valid) * 0.42 + 2)))
    ax.axis("off")

    cols = ["Outcome", "Type", "Towns\nanalysed", "Data\npoints", "How well\nmodel fits"]
    rows = []
    cats = []
    for _, r in valid.iterrows():
        r2 = r.get("r2_within", np.nan)
        fit = ("Strong" if r2 > 0.3 else "Moderate" if r2 > 0.1 else
               "Weak" if r2 > 0 else "—")
        rows.append([
            r.get("outcome_label", ""),
            r.get("category", ""),
            f"{int(r.get('n_towns', 0)):,}",
            f"{int(r.get('n_obs', 0)):,}",
            f"{fit}  ({r2:.2f})",
        ])
        cats.append(r.get("category", ""))

    tbl = ax.table(cellText=rows, colLabels=cols, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)

    # Colour rows by category
    for i, cat in enumerate(cats):
        for j in range(len(cols)):
            tbl[(i + 1, j)].set_facecolor(CAT_LIGHT.get(cat, "#f9f9f9"))
        tbl[(i + 1, 0)].get_text().set_color(CAT_COLOURS.get(cat, "#333"))
        tbl[(i + 1, 0)].get_text().set_fontweight("bold")

    # Title is set by caller (main) to include the lag label — don't set one here
    plt.tight_layout()
    return fig


def make_cover(lag: int) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.text(0.5, 0.72, "Town Policy Backtest", ha="center", va="center",
            fontsize=30, fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.60, "16-Outcome Panel Regression Analysis",
            ha="center", va="center", fontsize=16, color="#555", transform=ax.transAxes)
    ax.text(0.5, 0.48, f"Lag = {lag} year{'s' if lag != 1 else ''}  |  "
            "Two-Way Fixed Effects (Town + Year FE)\n"
            "Clustered Standard Errors at Town Level",
            ha="center", va="center", fontsize=12, color="#777",
            transform=ax.transAxes, linespacing=1.8)
    lines = [
        "Features:  Per-pupil spending  ·  Teacher staffing  ·  Chapter 70 aid",
        "           Education % of budget  ·  Municipal revenue per capita",
        "           Public works per capita  ·  Debt service % of budget",
        "           Public safety per capita  ·  Lagged crime rate  ·  Lagged crash rate",
        "",
        "Outcomes:  6 Education  ·  4 Safety  ·  3 Community  ·  2 Market  ·  1 Fiscal",
    ]
    ax.text(0.5, 0.28, "\n".join(lines), ha="center", va="center",
            fontsize=10, color="#444", transform=ax.transAxes,
            fontfamily="monospace", linespacing=1.7)
    ax.text(0.5, 0.08, "Saugus Schools Project  —  MA Municipal Data",
            ha="center", va="center", fontsize=9, color="#aaa", transform=ax.transAxes)
    return fig


# ---------------------------------------------------------------------------
# Signal decay chart — t-stats across lag 1, 3, 5 per feature
# ---------------------------------------------------------------------------

def make_decay_chart(all_results: dict[int, pd.DataFrame]) -> plt.Figure:
    """
    Heatmap showing how each feature's mean |t-stat| varies across lags.
    Rows = outcomes, columns = lags, cells = t-stat at that lag for the
    feature with the strongest signal at any lag (one chart per feature group).
    Keeps it simple and fast to render.
    """
    lags = sorted(all_results.keys())
    features = list(FEATURE_LABELS.keys())
    cat_colours = {"Education": "#4A90D9", "Safety": "#E05C4A",
                   "Community": "#5CB85C", "Market": "#F0AD4E", "Fiscal": "#9B59B6"}

    # Build a (feature × lag) matrix of mean |t| across outcomes
    feat_lag_mat = np.full((len(features), len(lags)), np.nan)
    for j, lag in enumerate(lags):
        res_df = all_results[lag]
        for i, feat in enumerate(features):
            col = f"{feat}_tstat"
            if col not in res_df.columns:
                continue
            vals = res_df[col].dropna()
            if len(vals) > 0:
                feat_lag_mat[i, j] = vals.abs().mean()

    fig, ax = plt.subplots(figsize=(7, max(4, len(features) * 0.6 + 2)))
    norm = plt.Normalize(vmin=0, vmax=np.nanmax(feat_lag_mat) if not np.all(np.isnan(feat_lag_mat)) else 3)
    im = ax.imshow(feat_lag_mat, cmap="YlOrRd", norm=norm, aspect="auto")

    for i in range(len(features)):
        for j in range(len(lags)):
            v = feat_lag_mat[i, j]
            if not np.isnan(v):
                stars = "***" if v > 2.58 else "**" if v > 1.96 else "*" if v > 1.645 else ""
                ax.text(j, i, f"{v:.2f}{stars}", ha="center", va="center",
                        fontsize=8.5, color="white" if v > 2.0 else "black",
                        fontweight="bold" if stars else "normal")
            else:
                ax.text(j, i, "—", ha="center", va="center", fontsize=8, color="#aaa")

    ax.set_xticks(range(len(lags)))
    ax.set_xticklabels([f"Lag {l}yr" for l in lags], fontsize=10)
    ax.set_yticks(range(len(features)))
    ax.set_yticklabels([FEATURE_LABELS[f] for f in features], fontsize=9)
    ax.set_title("Signal Decay — Mean |T-Statistic| by Feature and Lag\n"
                 "Higher = stronger average effect across all outcomes",
                 fontsize=10, fontweight="bold", pad=12)
    plt.colorbar(im, ax=ax, label="Mean |T-Statistic|", pad=0.02)
    plt.tight_layout()
    return fig


def compute_collinearity(panel: pd.DataFrame,
                         threshold: float = 0.70) -> list[tuple[str, str]]:
    """Return pairs of features with absolute correlation above threshold."""
    features = [f for f in FEATURE_LABELS if f in panel.columns]
    sub = panel[features].dropna()
    if len(sub) < 10:
        return []
    corr = sub.corr().abs()
    pairs = []
    for i, f1 in enumerate(features):
        for f2 in features[i+1:]:
            if corr.loc[f1, f2] >= threshold:
                pairs.append((f1, f2))
    return pairs


def _trend_label(t1: float, t3: float, t5: float) -> str:
    """Plain-English description of how a signal changes across lags."""
    vals = [v for v in [t1, t3, t5] if not np.isnan(v)]
    if len(vals) < 2:
        return "—"
    first, last = vals[0], vals[-1]
    if np.isnan(t1) or np.isnan(t5):
        return "partial data"
    delta = last - first
    # Check for sign reversal
    if first * last < 0:
        return "⚠ Reverses direction"
    if abs(delta) < 0.3:
        return "→ Stable across years"
    peak_idx = int(np.argmax([abs(v) for v in vals]))
    if peak_idx == len(vals) - 1:
        return "↑ Grows stronger over time"
    if peak_idx == 0:
        return "↓ Weakens over time"
    return "▲ Peaks in medium term"


def make_feature_ranking(all_results: dict[int, pd.DataFrame]) -> plt.Figure:
    """
    Summary slide: rank policy inputs by how consistently and strongly they
    predict good outcomes across all 16 outcomes and all 3 lags.

    Sign-adjusted t-statistic: for outcomes where lower is better (dropout,
    crime, poverty) the t-stat is flipped so positive always means beneficial.

    Metrics reported per feature:
      - Mean beneficial t-stat  (average signal strength across all cells)
      - % of cells significant at 10% level (|t| > 1.645)
      - % of cells significant at 5%  level (|t| > 1.96)
      - Best lag (where mean |t| is highest)
    """
    lags = sorted(all_results.keys())
    features = list(FEATURE_LABELS.keys())

    # Sign multipliers: +1 if higher outcome is good, -1 if lower is good
    sign = {out_key: (1 if hib else -1)
            for out_key, _, _, hib in OUTCOMES}

    # Collect all sign-adjusted t-stats per (feature, lag)
    records = []  # (feature, lag, outcome_label, beneficial_t)
    for lag in lags:
        res_df = all_results[lag]
        for out_key, out_label, _, hib in OUTCOMES:
            multiplier = 1 if hib else -1
            match = res_df[res_df["outcome_label"] == out_label]
            if len(match) == 0:
                continue
            row = match.iloc[0]
            for feat in features:
                col = f"{feat}_tstat"
                if col not in row.index or pd.isna(row[col]):
                    continue
                records.append({
                    "feature":       feat,
                    "lag":           lag,
                    "outcome":       out_label,
                    "beneficial_t":  multiplier * float(row[col]),
                })

    if not records:
        return None

    df = pd.DataFrame(records)

    # Aggregate per feature across all outcomes and lags
    agg = df.groupby("feature").agg(
        mean_t       = ("beneficial_t", "mean"),
        pct_sig10    = ("beneficial_t", lambda x: (x.abs() > 1.645).mean() * 100),
        pct_sig05    = ("beneficial_t", lambda x: (x.abs() > 1.960).mean() * 100),
        n_cells      = ("beneficial_t", "count"),
    ).reset_index()

    # Best lag per feature (highest mean |t| at that lag)
    lag_means = df.groupby(["feature","lag"])["beneficial_t"].apply(
        lambda x: x.abs().mean()
    ).reset_index(name="mean_abs_t")
    best_lag = lag_means.loc[lag_means.groupby("feature")["mean_abs_t"].idxmax(),
                             ["feature","lag"]].rename(columns={"lag":"best_lag"})
    agg = agg.merge(best_lag, on="feature", how="left")

    # Sort weakest at top, strongest at bottom (as requested)
    agg = agg.sort_values("mean_t", ascending=True)

    # Compute per-feature trend across lags
    lags_sorted = sorted(all_results.keys())
    trend_col = {}
    for feat in features:
        lag_means = {}
        for lag in lags_sorted:
            col = f"{feat}_tstat"
            res_df = all_results[lag]
            if col in res_df.columns:
                # sign-adjust before averaging
                vals = []
                for out_key, out_label, _, hib in OUTCOMES:
                    m = res_df[res_df["outcome_label"] == out_label]
                    if len(m) > 0 and col in m.columns and not pd.isna(m.iloc[0][col]):
                        sign = 1 if hib else -1
                        vals.append(sign * m.iloc[0][col])
                lag_means[lag] = np.mean(vals) if vals else np.nan
        t1 = lag_means.get(lags_sorted[0], np.nan)
        t3 = lag_means.get(lags_sorted[1], np.nan) if len(lags_sorted) > 1 else np.nan
        t5 = lag_means.get(lags_sorted[2], np.nan) if len(lags_sorted) > 2 else np.nan
        trend_col[feat] = _trend_label(t1, t3, t5)

    fig, axes = plt.subplots(1, 2, figsize=(16, max(5, len(agg) * 0.75 + 3)),
                             gridspec_kw={"width_ratios": [1.8, 2.2]})
    ax_bar, ax_tbl = axes

    # --- Bar chart ---
    colours = ["#E05C4A" if v < 0 else "#4A90D9" if v < 0.8 else "#27AE60"
               for v in agg["mean_t"]]
    ax_bar.barh(range(len(agg)), agg["mean_t"], color=colours,
                edgecolor="white", height=0.65)

    ax_bar.axvline(0,     color="#333", linewidth=1.0)
    ax_bar.axvline(1.645, color="#F0AD4E", linewidth=1.2, linestyle="--",
                   label="90% confident")
    ax_bar.axvline(1.960, color="#4A90D9", linewidth=1.2, linestyle="--",
                   label="95% confident")
    ax_bar.axvline(2.576, color="#27AE60", linewidth=1.2, linestyle="--",
                   label="99% confident")
    ax_bar.axvline(-1.645, color="#F0AD4E", linewidth=1.0, linestyle="--")

    ax_bar.set_yticks(range(len(agg)))
    ax_bar.set_yticklabels([FEATURE_LABELS.get(f, f) for f in agg["feature"]], fontsize=9)
    ax_bar.set_xlabel(
        "Average strength score across all outcomes and time horizons\n"
        "(positive = predicts better town outcomes, negative = predicts worse)",
        fontsize=8.5
    )
    ax_bar.set_title("What works?\n(Weakest at top → Strongest at bottom)",
                     fontsize=10, fontweight="bold")
    ax_bar.legend(fontsize=8, loc="lower right", title="Confidence thresholds")
    ax_bar.invert_yaxis()

    for i, val in enumerate(agg["mean_t"]):
        ax_bar.text(val + (0.04 if val >= 0 else -0.04), i,
                    f"{val:+.2f}", va="center",
                    ha="left" if val >= 0 else "right", fontsize=8)

    # --- Summary table (sorted strongest → weakest for reading top-to-bottom) ---
    tbl_data = []
    tbl_rows_sorted = agg.sort_values("mean_t", ascending=False)
    for _, row_ in tbl_rows_sorted.iterrows():
        conf = ("★★★ 99%" if row_["pct_sig05"] > 50 else
                "★★  95%" if row_["pct_sig05"] > 25 else
                "★   90%" if row_["pct_sig10"] > 25 else
                "not significant")
        tbl_data.append([
            FEATURE_LABELS.get(row_["feature"], row_["feature"]),
            f"{row_['mean_t']:+.2f}",
            conf,
            f"{int(row_['best_lag'])} year{'s' if row_['best_lag'] != 1 else ''}",
            trend_col.get(row_["feature"], "—"),
        ])

    ax_tbl.axis("off")
    col_labels = ["Policy Input", "Avg\nStrength", "Confidence", "Best\nTime Horizon", "Trend over time"]
    tbl = ax_tbl.table(cellText=tbl_data, colLabels=col_labels,
                       loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.5)
    tbl.scale(1, 1.55)
    tbl.auto_set_column_width([0, 1, 2, 3, 4])

    for i, row_vals in enumerate(tbl_data):
        mt = float(row_vals[1])
        bg = ("#d4efdf" if mt > 1.0 else "#fef9e7" if mt > 0 else "#fadbd8")
        for j in range(len(col_labels)):
            tbl[(i+1, j)].set_facecolor(bg)
            tbl[(i+1, j)].set_text_props(ha="left")

    ax_tbl.set_title(
        "Strongest effects at top  |  ★★★ = very strong evidence  ★★ = strong  ★ = moderate\n"
        "Trend: does the effect get stronger (↑), weaker (↓), or peak in the middle (▲)?",
        fontsize=8.5, fontweight="bold", pad=12
    )

    fig.suptitle("Which Policy Inputs Predict Better Town Outcomes?",
                 fontsize=13, fontweight="bold", y=1.01)
    plt.tight_layout()
    return fig


def make_multi_lag_cover() -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.text(0.5, 0.72, "Town Policy Backtest", ha="center", va="center",
            fontsize=30, fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.60, "16 Outcomes  ×  3 Lags  ×  10 Features",
            ha="center", va="center", fontsize=16, color="#555", transform=ax.transAxes)
    ax.text(0.5, 0.50, "Two-Way Fixed Effects (Town + Year FE)\n"
            "Clustered Standard Errors at Town Level",
            ha="center", va="center", fontsize=12, color="#777",
            transform=ax.transAxes, linespacing=1.8)
    lines = [
        "Lags tested:  1 year (fast)  ·  3 years (medium)  ·  5 years (structural)",
        "",
        "Features:  Per-pupil spending  ·  Teacher staffing  ·  Chapter 70 aid",
        "           Ed % of budget  ·  Muni revenue/capita  ·  Public works/capita",
        "           Debt service %  ·  Public safety/capita  ·  Crime rate  ·  Crash rate",
        "",
        "Outcomes:  6 Education  ·  4 Safety  ·  3 Community  ·  2 Market  ·  1 Fiscal",
        "",
        "Signal decay chart shows how t-statistics evolve across lags —",
        "peak lag reveals whether an effect is operational or structural.",
    ]
    ax.text(0.5, 0.28, "\n".join(lines), ha="center", va="center",
            fontsize=9.5, color="#444", transform=ax.transAxes,
            fontfamily="monospace", linespacing=1.7)
    ax.text(0.5, 0.06, "Saugus Schools Project  —  MA Municipal Data",
            ha="center", va="center", fontsize=9, color="#aaa", transform=ax.transAxes)
    return fig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

LAGS = [1, 3, 5]

def main(lag: int | None = None):
    engine = get_engine()
    os.makedirs(os.path.dirname(OUTPUT_PDF), exist_ok=True)

    target_lags = [lag] if lag else LAGS

    all_results: dict[int, pd.DataFrame] = {}
    collinear_pairs: list[tuple[str,str]] = []

    for l in target_lags:
        results = run_backtest(engine, lag=l)
        results["lag"] = l
        all_results[l] = results
        # Compute collinearity once from the first panel
        if not collinear_pairs:
            panel_for_corr = load_panel(engine, lag=l)
            collinear_pairs = compute_collinearity(panel_for_corr)
            if collinear_pairs:
                print(f"[backtest] Collinear feature pairs (|r|≥0.70): "
                      + ", ".join(f"{a}↔{b}" for a, b in collinear_pairs))

    # Save combined CSV
    combined = pd.concat(all_results.values(), ignore_index=True)
    combined.to_csv(OUTPUT_CSV, index=False)
    print(f"[backtest] Results saved to {OUTPUT_CSV}")

    def _save(pdf, fig, label=""):
        if fig is None:
            return
        try:
            pdf.savefig(fig, bbox_inches="tight")
        except Exception as e:
            print(f"  [warn] Could not save {label}: {e}")
        finally:
            plt.close(fig)

    print("[backtest] Generating PDF...")
    with PdfPages(OUTPUT_PDF) as pdf:
        _save(pdf, make_multi_lag_cover() if len(target_lags) > 1 else make_cover(lag), "cover")
        _save(pdf, make_plain_english_legend(), "legend")

        for l in target_lags:
            for fig in make_tstat_matrix(all_results[l], l, collinear_pairs):
                _save(pdf, fig, f"heatmap lag={l}")

        if len(target_lags) > 1:
            _save(pdf, make_decay_chart(all_results), "decay chart")

        for l in target_lags:
            fig2 = make_r2_table(all_results[l])
            if fig2:
                lag_word = "1-Year" if l == 1 else "3-Year" if l == 3 else "5-Year"
                fig2.text(0.5, 0.98,
                          f"Model Summary — {lag_word} Lag",
                          ha="center", va="top", fontsize=12, fontweight="bold",
                          transform=fig2.transFigure)
                fig2.text(0.5, 0.94,
                          "How many towns were analysed, how many data points used, "
                          "and how well do the policy inputs explain the outcomes?\n"
                          "('How well model fits': 0 = explains nothing, 1 = explains everything — "
                          "most real-world policy effects are weak, so 0.05–0.20 is typical)",
                          ha="center", va="top", fontsize=8, color="#555",
                          transform=fig2.transFigure)
                fig2.subplots_adjust(top=0.88)
            _save(pdf, fig2, f"r2 table lag={l}")

        if len(target_lags) > 1:
            _save(pdf, make_feature_ranking(all_results), "feature ranking")

    print(f"[backtest] Report saved to {OUTPUT_PDF}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lag", type=int, default=None,
                        help="Single lag in years (default: runs all three — 1, 3, 5)")
    args = parser.parse_args()
    main(lag=args.lag)
