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
    # (key, label, category)
    ("dropout_rate",         "Dropout Rate",               "Education"),
    ("graduation_rate",      "Graduation Rate",            "Education"),
    ("mcas_ela_pct",         "MCAS ELA Proficient %",      "Education"),
    ("mcas_math_pct",        "MCAS Math Proficient %",     "Education"),
    ("postsecondary_pct",    "Postsecondary Attendance %", "Education"),
    ("sat_mean",             "SAT Mean Score",             "Education"),
    ("crime_rate",           "Crime Rate / 100k",          "Safety"),
    ("violent_crime_rate",   "Violent Crime Rate / 100k",  "Safety"),
    ("crash_rate",           "Crash Rate / 1k pop",        "Safety"),
    ("injury_crash_rate",    "Injury Crash Rate / 1k pop", "Safety"),
    ("absenteeism_rate",     "Chronic Absenteeism %",      "Community"),
    ("enrollment_growth",    "Enrollment Growth %",        "Community"),
    ("poverty_pct",          "Poverty Rate %",             "Community"),
    ("real_zhvi_growth",     "Real Home Value Growth %",   "Market"),
    ("real_rev_growth",      "Real Revenue Growth %",      "Fiscal"),
    ("mcas_ela_residual",    "MCAS ELA Residual (SES adj)","Education"),
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
    Returns dict with coefficient, t-stat, p-value, N, R2_within per feature.
    """
    from linearmodels.panel import PanelOLS
    import warnings

    # Start with core features, then greedily add extras that don't halve the sample
    base_feats = [f for f in features if f in panel.columns]
    if not base_feats:
        return {"n_obs": 0, "n_towns": 0}

    # Compute baseline sample with all features
    full_sub = panel[["town","year", outcome] + base_feats].dropna()
    baseline_n = len(full_sub)

    if baseline_n >= 80:
        use_features = base_feats
    else:
        # Fall back: greedily include features that keep sample ≥ 80
        use_features = []
        for f in base_feats:
            trial = panel[["town","year", outcome] + use_features + [f]].dropna()
            if len(trial) >= 80:
                use_features.append(f)
        # If still empty, just use features that have pairwise coverage
        if not use_features:
            use_features = [
                f for f in base_feats
                if panel[[outcome, f]].dropna().shape[0] >= 30
            ]

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

    # Core features — available for ~335 districts (no Schedule A needed)
    core_features = ["log_pp_exp", "teachers_per1k", "ch70_per_pupil"]
    # Schedule A features — available for ~61 municipalities
    sched_a_features = ["ed_pct_budget", "log_muni_rev_pc",
                        "public_works_pc", "debt_pct_budget", "public_safety_pc"]
    # Safety features — lagged crime/crash
    safety_features = ["crime_rate_feat", "crash_rate_feat"]

    # All features tried; regression selects subset with ≥100 joint obs
    all_features = core_features + sched_a_features + safety_features
    MIN_OBS = 80

    results = []
    for out_key, out_label, category in OUTCOMES:
        if out_key not in panel.columns:
            print(f"  SKIP {out_key} — not in panel")
            continue

        features = all_features

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
# Visualisation
# ---------------------------------------------------------------------------

def make_tstat_matrix(results: pd.DataFrame, lag: int) -> None:
    features = list(FEATURE_LABELS.keys())
    outcomes = [(r["outcome_label"], r["category"]) for _, r in results.iterrows()
                if "n_obs" in r and r.get("n_obs", 0) >= 30]

    if not outcomes:
        print("[backtest] No outcomes with sufficient data to plot.")
        return

    # Build t-stat matrix
    mat = np.full((len(outcomes), len(features)), np.nan)
    for i, (out_label, _) in enumerate(outcomes):
        row = results[results["outcome_label"] == out_label].iloc[0]
        for j, feat in enumerate(features):
            mat[i, j] = row.get(f"{feat}_tstat", np.nan)

    fig, axes = plt.subplots(1, 2, figsize=(17, max(8, len(outcomes) * 0.55 + 2)),
                             gridspec_kw={"width_ratios": [6, 1]})
    ax, ax_meta = axes

    # Diverging colormap centred at 0, clipped at ±3
    vmax = 3.0
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    im = ax.imshow(mat, cmap="RdYlGn", norm=norm, aspect="auto")

    # Significance markers
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            t = mat[i, j]
            if np.isnan(t):
                ax.text(j, i, "—", ha="center", va="center", fontsize=8, color="#999")
            else:
                stars = "***" if abs(t) > 2.58 else "**" if abs(t) > 1.96 else "*" if abs(t) > 1.645 else ""
                label = f"{t:+.1f}{stars}"
                color = "white" if abs(t) > 2.0 else "black"
                ax.text(j, i, label, ha="center", va="center", fontsize=7.5,
                        fontweight="bold" if stars else "normal", color=color)

    ax.set_xticks(range(len(features)))
    ax.set_xticklabels([FEATURE_LABELS[f] for f in features],
                       rotation=35, ha="right", fontsize=9)
    ax.set_yticks(range(len(outcomes)))
    ax.set_yticklabels([lbl for lbl, _ in outcomes], fontsize=9)
    ax.set_title(f"Panel Regression T-Statistics  |  Lag = {lag} year{'s' if lag != 1 else ''}  |  "
                 "Two-Way Fixed Effects (Town + Year)",
                 fontsize=11, fontweight="bold", pad=12)

    # Category colour bar on right
    categories = [cat for _, cat in outcomes]
    cat_colours = {"Education": "#4A90D9", "Safety": "#E05C4A",
                   "Community": "#5CB85C", "Market": "#F0AD4E", "Fiscal": "#9B59B6"}
    for i, cat in enumerate(categories):
        ax_meta.barh(i, 1, color=cat_colours.get(cat, "#aaa"), edgecolor="white", height=0.9)
        ax_meta.text(0.5, i, cat, ha="center", va="center", fontsize=7.5, color="white", fontweight="bold")
    ax_meta.set_xlim(0, 1)
    ax_meta.set_ylim(-0.5, len(outcomes) - 0.5)
    ax_meta.invert_yaxis()
    ax_meta.axis("off")

    plt.colorbar(im, ax=ax, orientation="vertical", pad=0.01,
                 label="T-Statistic  (green = positive effect, red = negative effect)")

    # Legend
    legend_items = [
        mpatches.Patch(color="white", label="*** p<0.01   ** p<0.05   * p<0.10"),
    ]
    ax.legend(handles=legend_items, loc="upper left", fontsize=8,
              framealpha=0.9, bbox_to_anchor=(0, -0.18))

    plt.tight_layout()
    return fig


def make_r2_table(results: pd.DataFrame) -> plt.Figure:
    valid = results[results.get("n_obs", pd.Series(dtype=float)).notna() if "n_obs" in results.columns else
                    results["n_obs"].notna()].copy() if "n_obs" in results.columns else results.copy()
    valid = results.dropna(subset=["r2_within"] if "r2_within" in results.columns else [])
    if valid.empty:
        return None

    fig, ax = plt.subplots(figsize=(10, max(4, len(valid) * 0.4 + 1.5)))
    ax.axis("off")

    cols = ["Outcome", "Category", "N Obs", "N Towns", "R² (within)"]
    rows = []
    for _, r in valid.iterrows():
        rows.append([
            r.get("outcome_label", ""),
            r.get("category", ""),
            f"{int(r.get('n_obs', 0)):,}",
            f"{int(r.get('n_towns', 0)):,}",
            f"{r.get('r2_within', np.nan):.3f}",
        ])

    tbl = ax.table(cellText=rows, colLabels=cols, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.4)
    ax.set_title("Regression Summary — Observations and Model Fit", fontsize=11,
                 fontweight="bold", pad=16)
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
    For each outcome, plot how each feature's t-statistic evolves across lags.
    Shows whether effects peak early (operational) or late (structural).
    """
    lags = sorted(all_results.keys())
    features = list(FEATURE_LABELS.keys())
    cat_colours = {"Education": "#4A90D9", "Safety": "#E05C4A",
                   "Community": "#5CB85C", "Market": "#F0AD4E", "Fiscal": "#9B59B6"}

    # Collect all (outcome, feature) pairs that have at least one non-nan t-stat
    valid_outcomes = []
    for out_key, out_label, category in OUTCOMES:
        for feat in features:
            has_data = any(
                not np.isnan(all_results[lag].loc[
                    all_results[lag]["outcome_label"] == out_label,
                    f"{feat}_tstat"
                ].values[0] if out_label in all_results[lag]["outcome_label"].values
                  and f"{feat}_tstat" in all_results[lag].columns else np.nan)
                for lag in lags
            )
            if has_data:
                break
        else:
            continue
        valid_outcomes.append((out_key, out_label, category))

    n_outcomes = len(valid_outcomes)
    if n_outcomes == 0:
        return None

    # One row per outcome, columns = features, three lines per cell (lag 1, 3, 5)
    n_cols = min(4, len(features))
    n_rows = n_outcomes
    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(n_cols * 3.2, n_rows * 1.8),
        squeeze=False
    )
    fig.suptitle("Signal Decay — T-Statistic by Lag  (1, 3, 5 Years)",
                 fontsize=13, fontweight="bold", y=1.01)

    lag_styles = {1: ("o", "-", "#E05C4A"), 3: ("s", "--", "#4A90D9"), 5: ("^", ":", "#5CB85C")}

    for row_i, (out_key, out_label, category) in enumerate(valid_outcomes):
        for col_j, feat in enumerate(features[:n_cols]):
            ax = axes[row_i][col_j]
            tvals, lvals = [], []
            for lag in lags:
                res_df = all_results[lag]
                match = res_df[res_df["outcome_label"] == out_label]
                col = f"{feat}_tstat"
                tval = match[col].values[0] if len(match) > 0 and col in res_df.columns else np.nan
                tvals.append(tval)
                lvals.append(lag)

            # Plot line
            valid_pairs = [(l, t) for l, t in zip(lvals, tvals) if not np.isnan(t)]
            if valid_pairs:
                xs, ys = zip(*valid_pairs)
                ax.plot(xs, ys, "o-", color=cat_colours.get(category, "#888"),
                        linewidth=1.8, markersize=5)
                for x, y in zip(xs, ys):
                    stars = "***" if abs(y) > 2.58 else "**" if abs(y) > 1.96 else "*" if abs(y) > 1.645 else ""
                    ax.annotate(f"{y:+.1f}{stars}", (x, y),
                                textcoords="offset points", xytext=(0, 5),
                                ha="center", fontsize=6.5)

            ax.axhline(0, color="#aaa", linewidth=0.8, linestyle="--")
            ax.axhline(1.96,  color="#4A90D9", linewidth=0.6, linestyle=":", alpha=0.6)
            ax.axhline(-1.96, color="#4A90D9", linewidth=0.6, linestyle=":", alpha=0.6)
            ax.set_xticks(lags)
            ax.set_xticklabels([f"L{l}" for l in lags], fontsize=7)
            ax.tick_params(axis="y", labelsize=7)

            if col_j == 0:
                ax.set_ylabel(out_label, fontsize=7.5, fontweight="bold",
                              color=cat_colours.get(category, "#333"))
            if row_i == 0:
                ax.set_title(FEATURE_LABELS[feat], fontsize=7.5, fontweight="bold")

    # Hide unused axes
    for row_i in range(n_outcomes):
        for col_j in range(len(features[:n_cols]), n_cols):
            axes[row_i][col_j].axis("off")

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
    for l in target_lags:
        results = run_backtest(engine, lag=l)
        results["lag"] = l
        all_results[l] = results

    # Save combined CSV
    combined = pd.concat(all_results.values(), ignore_index=True)
    combined.to_csv(OUTPUT_CSV, index=False)
    print(f"[backtest] Results saved to {OUTPUT_CSV}")

    print("[backtest] Generating PDF...")
    with PdfPages(OUTPUT_PDF) as pdf:
        # Cover
        pdf.savefig(make_multi_lag_cover() if len(target_lags) > 1 else make_cover(lag),
                    bbox_inches="tight")
        plt.close()

        # One heatmap per lag
        for l in target_lags:
            fig = make_tstat_matrix(all_results[l], l)
            if fig:
                pdf.savefig(fig, bbox_inches="tight")
                plt.close()

        # Signal decay chart (only when multiple lags run)
        if len(target_lags) > 1:
            fig_decay = make_decay_chart(all_results)
            if fig_decay:
                pdf.savefig(fig_decay, bbox_inches="tight")
                plt.close()

        # Summary table for each lag
        for l in target_lags:
            fig2 = make_r2_table(all_results[l])
            if fig2:
                fig2.suptitle(f"Model Fit Summary — Lag {l} Year{'s' if l != 1 else ''}",
                              fontsize=11, fontweight="bold")
                pdf.savefig(fig2, bbox_inches="tight")
                plt.close()

    print(f"[backtest] Report saved to {OUTPUT_PDF}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lag", type=int, default=None,
                        help="Single lag in years (default: runs all three — 1, 3, 5)")
    args = parser.parse_args()
    main(lag=args.lag)
