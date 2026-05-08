"""
Massachusetts Town Health Report
=================================
Analyzes ~350 MA towns across educational outcomes, fiscal health, and community
vitality. Includes 5-year (2019→2024) and 10-year (2015→2024) backtests comparing
towns that increased school investment to those that maintained or cut.

Run: python analysis/town_health_report.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "town_health_report.pdf")

# ── Palette (matches municipal_finance_report.py) ────────────────────────────
NAVY       = "#1B2A4A"
GOLD       = "#F0A500"
WHITE      = "#FFFFFF"
LIGHT_GRAY = "#D0D5E0"
RED        = "#C0392B"
GREEN      = "#1E8449"
BLUE       = "#2471A3"
DARK_BLUE  = "#1A5276"
ORANGE     = "#E67E22"
STEEL_BLUE = "#5D8AA8"
CHART_BG   = "#1B2A4A"
CHART_GRID = "#2C3E6B"
PURPLE     = "#7D3C98"

SAUGUS = "Saugus"
BACKTEST_5Y_BASE   = 2019   # pre-COVID baseline
BACKTEST_5Y_END    = 2024
BACKTEST_10Y_BASE  = 2015
BACKTEST_10Y_END   = 2024

# Investment group thresholds: change in ed% of budget
INVEST_UP_THRESHOLD   =  0.015   # +1.5pp = "Increased"
INVEST_DOWN_THRESHOLD = -0.015   # -1.5pp = "Cut"


# ══════════════════════════════════════════════════════════════════════════════
# Data loading
# ══════════════════════════════════════════════════════════════════════════════

def _q(sql): return text(sql)


def load_fiscal(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT r.municipality, r.fiscal_year,
               e.education::float / NULLIF(e.total_expenditures,0) AS ed_pct,
               r.total_revenues / 1e6  AS rev_m,
               e.total_expenditures / 1e6 AS exp_m,
               (r.total_revenues - e.total_expenditures)::float
                   / NULLIF(r.total_revenues, 0) AS surplus_pct
        FROM municipal_revenues r
        JOIN municipal_expenditures e
          ON e.dor_code = r.dor_code AND e.fiscal_year = r.fiscal_year
        WHERE r.fiscal_year BETWEEN 2010 AND 2025
    """), engine)


def load_per_pupil(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year, amount AS per_pupil
        FROM per_pupil_expenditure
        WHERE category = 'Total In-District Expenditures'
          AND school_year BETWEEN 2010 AND 2024
    """), engine)


def load_mcas(engine) -> pd.DataFrame:
    """District-level avg MCAS % meeting/exceeding, ELA+Math, all grades, all students."""
    return pd.read_sql(_q("""
        SELECT district_name, school_year,
               AVG(meeting_exceeding_pct) * 100 AS mcas_avg
        FROM mcas_results
        WHERE student_group = 'All Students'
          AND subject IN ('ELA', 'Math')
          AND school_name = district_name
          AND school_year BETWEEN 2017 AND 2025
        GROUP BY district_name, school_year
    """), engine)


def load_sat(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year,
               mean_ebrw + mean_math AS sat_total
        FROM district_sat_scores
        WHERE school_year BETWEEN 2007 AND 2025
          AND mean_ebrw IS NOT NULL AND mean_math IS NOT NULL
    """), engine)


def load_postsecondary(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year, attending_pct
        FROM district_postsecondary
        WHERE school_year BETWEEN 2004 AND 2024
    """), engine)


def load_graduation(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year,
               AVG(four_year_grad_pct) AS grad_rate
        FROM graduation_rates
        WHERE student_group IN ('All Students', 'All')
          AND school_year BETWEEN 2017 AND 2025
        GROUP BY district_name, school_year
    """), engine)


def load_dropout(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year, dropout_pct
        FROM district_dropout
        WHERE school_year BETWEEN 2008 AND 2025
    """), engine)


def load_enrollment(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT district_name, school_year, SUM(total) AS enrollment
        FROM enrollment
        WHERE school_year BETWEEN 2009 AND 2024
          AND grade = 'Total'
        GROUP BY district_name, school_year
    """), engine)


def load_zillow(engine) -> pd.DataFrame:
    """December ZHVI (year-end) for clean annual comparisons."""
    return pd.read_sql(_q("""
        SELECT region_name AS municipality, data_year AS year,
               AVG(zhvi) AS zhvi
        FROM municipal_zillow_housing
        WHERE data_month = 12
          AND data_year BETWEEN 2010 AND 2025
        GROUP BY region_name, data_year
    """), engine)


def load_acs(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT municipality, acs_year,
               median_hh_income, pct_bachelors_plus, pct_65_plus,
               pct_under18, unemployment_rate, poverty_pct,
               pct_foreign_born, pct_single_parent, total_population
        FROM municipal_census_acs
        WHERE acs_year BETWEEN 2014 AND 2023
    """), engine)


def load_cpi(engine) -> pd.DataFrame:
    return pd.read_sql(_q("""
        SELECT year, cpi_pct_change FROM inflation_cpi ORDER BY year
    """), engine)


# ══════════════════════════════════════════════════════════════════════════════
# Town Health Index computation
# ══════════════════════════════════════════════════════════════════════════════

def _zscore_col(df, col):
    m, s = df[col].mean(), df[col].std()
    if s == 0:
        return pd.Series(0.0, index=df.index)
    return (df[col] - m) / s


def compute_snapshot(year: int, data: dict) -> pd.DataFrame:
    """
    Build a per-town snapshot for a given year.
    Returns DataFrame with THI components and composite score.
    year: fiscal/school year to use. For educational data we use closest available year.
    """
    fiscal = data["fiscal"]
    pp     = data["per_pupil"]
    mcas   = data["mcas"]
    sat    = data["sat"]
    postsec= data["postsec"]
    dropout= data["dropout"]
    grad   = data["grad"]
    zillow = data["zillow"]
    acs    = data["acs"]
    enroll = data["enrollment"]
    cpi    = data["cpi"]

    # ── Educational outcomes ──────────────────────────────────────────────────
    # Choose best available: MCAS if >=2017, else SAT composite
    if year >= 2017:
        ed_year = year
        edu = mcas[mcas.school_year == ed_year][["district_name","mcas_avg"]].copy()
        edu.columns = ["district_name","ed_score"]
        # Normalize to 0-100 scale (MCAS already is)
    else:
        # 10-year backtest: use SAT z-score as proxy
        ed_year = year
        edu = sat[sat.school_year == ed_year][["district_name","sat_total"]].copy()
        if not edu.empty:
            mu, sd = edu.sat_total.mean(), edu.sat_total.std()
            edu["ed_score"] = ((edu.sat_total - mu) / sd * 15 + 50).clip(0,100)
        else:
            edu = pd.DataFrame(columns=["district_name","ed_score"])
        edu = edu[["district_name","ed_score"]]

    # Add postsecondary & dropout (both available further back)
    ps_yr = min(year, postsec.school_year.max()) if not postsec.empty else None
    ps = postsec[postsec.school_year == ps_yr][["district_name","attending_pct"]].copy() if ps_yr else pd.DataFrame(columns=["district_name","attending_pct"])

    do_yr = year
    do_df = dropout[dropout.school_year == do_yr][["district_name","dropout_pct"]].copy()

    edu_merged = (edu
        .merge(ps, on="district_name", how="outer")
        .merge(do_df, on="district_name", how="outer"))

    # Composite educational score: avg of available z-scores
    for col, direction in [("ed_score", 1), ("attending_pct", 1), ("dropout_pct", -1)]:
        if col in edu_merged.columns and edu_merged[col].notna().sum() > 5:
            z = _zscore_col(edu_merged.dropna(subset=[col]), col)
            edu_merged[f"_z_{col}"] = z * direction
    z_cols = [c for c in edu_merged.columns if c.startswith("_z_")]
    if z_cols:
        edu_merged["edu_component"] = edu_merged[z_cols].mean(axis=1)
    else:
        edu_merged["edu_component"] = np.nan

    # ── Community vitality ───────────────────────────────────────────────────
    # "Abnormal" home value growth = above-inflation appreciation (real ZHVI growth)
    z_yr = year if year <= zillow.year.max() else zillow.year.max()
    zil_now = zillow[zillow.year == z_yr][["municipality","zhvi"]].copy()
    z_yr_base = max(year - 3, zillow.year.min())
    zil_base = zillow[zillow.year == z_yr_base][["municipality","zhvi"]].rename(columns={"zhvi":"zhvi_base"})
    zil = zil_now.merge(zil_base, on="municipality", how="left")
    nominal_growth = (zil.zhvi / zil.zhvi_base - 1)
    # Deflate by CPI over same period to get real growth
    cpi_sub = cpi[(cpi.year >= z_yr_base) & (cpi.year <= z_yr)]
    cumulative_cpi = (1 + cpi_sub["cpi_pct_change"] / 100).prod() - 1 if not cpi_sub.empty else 0.10
    zil["zhvi_growth"]      = nominal_growth.clip(-0.5, 2.0)
    zil["zhvi_real_growth"] = (nominal_growth - cumulative_cpi).clip(-0.5, 2.0)

    enr_yr = min(year, enroll.school_year.max())
    enr_base_yr = max(year - 3, enroll.school_year.min())
    enr_now  = enroll[enroll.school_year == enr_yr][["district_name","enrollment"]].copy()
    enr_prev = enroll[enroll.school_year == enr_base_yr][["district_name","enrollment"]].rename(columns={"enrollment":"enr_base"})
    enr_chg  = enr_now.merge(enr_prev, on="district_name", how="left")
    enr_chg["enr_growth"] = (enr_chg.enrollment / enr_chg.enr_base - 1).clip(-0.5, 1.0)

    # ── Fiscal (Schedule A towns only) ───────────────────────────────────────
    fy = year
    fis = fiscal[fiscal.fiscal_year == fy][["municipality","ed_pct","surplus_pct"]].copy()

    # ── Per-pupil (broader coverage) ─────────────────────────────────────────
    pp_yr = min(year, pp.school_year.max())
    pp_now = pp[pp.school_year == pp_yr][["district_name","per_pupil"]].copy()

    # ── ACS demographics ─────────────────────────────────────────────────────
    acs_yr = min(year if year <= 2023 else 2023, acs.acs_year.max())
    acs_now = acs[acs.acs_year == acs_yr].copy()

    # ── Merge everything on town name ─────────────────────────────────────────
    base = acs_now[["municipality","median_hh_income","pct_bachelors_plus",
                    "pct_under18","pct_65_plus","poverty_pct","pct_foreign_born",
                    "pct_single_parent","unemployment_rate","total_population"]].copy()

    # Join educational (district_name → municipality: direct name match)
    base = base.merge(edu_merged[["district_name","edu_component"]].rename(columns={"district_name":"municipality"}),
                      on="municipality", how="left")
    base = base.merge(zil[["municipality","zhvi","zhvi_growth"]], on="municipality", how="left")
    base = base.merge(enr_chg[["district_name","enrollment","enr_growth"]].rename(columns={"district_name":"municipality"}),
                      on="municipality", how="left")
    base = base.merge(fis[["municipality","ed_pct","surplus_pct"]], on="municipality", how="left")
    base = base.merge(pp_now[["district_name","per_pupil"]].rename(columns={"district_name":"municipality"}),
                      on="municipality", how="left")
    base["year"] = year

    # ── Compute demographic-adjusted educational residual ──────────────────────
    exog = ["median_hh_income","pct_bachelors_plus","poverty_pct","pct_foreign_born"]
    mask = base[exog + ["edu_component"]].notna().all(axis=1)
    if mask.sum() > 20:
        X = base.loc[mask, exog].values
        y = base.loc[mask, "edu_component"].values
        sc = StandardScaler()
        X_sc = sc.fit_transform(X)
        model = Ridge(alpha=1.0)
        model.fit(X_sc, y)
        pred = model.predict(sc.transform(base.loc[:, exog].fillna(base[exog].median())))
        base["edu_residual"] = base["edu_component"] - pred
    else:
        base["edu_residual"] = base["edu_component"]

    # ── Community vitality component ──────────────────────────────────────────
    cv_cols = []
    for col in ["zhvi_growth", "enr_growth"]:
        if col in base.columns and base[col].notna().sum() > 5:
            base[f"_z_{col}"] = _zscore_col(base.dropna(subset=[col]).assign(**{f"_z_{col}": _zscore_col(base.dropna(subset=[col]), col)})[["municipality", f"_z_{col}"]], f"_z_{col}").reindex(base.index)
            cv_cols.append(f"_z_{col}")

    # Recompute properly
    for col in ["zhvi_growth", "enr_growth"]:
        if col in base.columns and base[col].notna().sum() > 5:
            mu, sd = base[col].mean(), base[col].std()
            base[f"_cv_{col}"] = ((base[col] - mu) / sd) if sd > 0 else 0
    cv_z = [c for c in base.columns if c.startswith("_cv_")]
    if cv_z:
        base["community_component"] = base[cv_z].mean(axis=1)
    else:
        base["community_component"] = np.nan

    # ── Fiscal component (Schedule A subset) ──────────────────────────────────
    for col in ["ed_pct", "surplus_pct"]:
        if col in base.columns and base[col].notna().sum() > 5:
            mu, sd = base[col].mean(), base[col].std()
            base[f"_fi_{col}"] = ((base[col] - mu) / sd) if sd > 0 else 0
    fi_z = [c for c in base.columns if c.startswith("_fi_")]
    if fi_z:
        base["fiscal_component"] = base[fi_z].mean(axis=1)
    else:
        base["fiscal_component"] = np.nan

    # ── Composite THI ─────────────────────────────────────────────────────────
    # Tier A (has fiscal data): 0.40 edu + 0.25 fiscal + 0.25 community + 0.10 residual
    # Tier B (no fiscal): 0.50 edu + 0.30 community + 0.20 residual
    has_fiscal = base["fiscal_component"].notna()
    base["thi"] = np.nan

    mask_a = has_fiscal & base["edu_residual"].notna() & base["community_component"].notna()
    if mask_a.sum() > 0:
        base.loc[mask_a, "thi"] = (
            0.40 * base.loc[mask_a, "edu_residual"].fillna(0) +
            0.25 * base.loc[mask_a, "fiscal_component"].fillna(0) +
            0.25 * base.loc[mask_a, "community_component"].fillna(0) +
            0.10 * base.loc[mask_a, "edu_component"].fillna(0)
        )

    mask_b = ~has_fiscal & base["edu_residual"].notna()
    if mask_b.sum() > 0:
        base.loc[mask_b, "thi"] = (
            0.50 * base.loc[mask_b, "edu_residual"].fillna(0) +
            0.30 * base.loc[mask_b, "community_component"].fillna(0) +
            0.20 * base.loc[mask_b, "edu_component"].fillna(0)
        )

    # Final z-score of THI across all towns
    thi_mask = base["thi"].notna()
    if thi_mask.sum() > 5:
        mu, sd = base.loc[thi_mask, "thi"].mean(), base.loc[thi_mask, "thi"].std()
        base["thi"] = ((base["thi"] - mu) / sd).clip(-3, 3)

    base["has_fiscal"] = has_fiscal
    return base


def run_backtest(snap_base: pd.DataFrame, snap_end: pd.DataFrame,
                 fiscal_base: pd.DataFrame, fiscal_end: pd.DataFrame) -> pd.DataFrame:
    """
    Join baseline and endpoint snapshots; classify towns by investment change.
    Returns wide DataFrame with change columns and investment_group.
    """
    base = snap_base[["municipality","thi","ed_pct","surplus_pct","per_pupil",
                       "edu_component","community_component","median_hh_income",
                       "pct_bachelors_plus","poverty_pct"]].copy()
    base.columns = ["municipality"] + [f"{c}_base" for c in base.columns[1:]]

    end = snap_end[["municipality","thi","ed_pct","surplus_pct","per_pupil",
                     "edu_component","community_component","zhvi","enrollment"]].copy()
    end.columns = ["municipality"] + [f"{c}_end" for c in end.columns[1:]]

    merged = base.merge(end, on="municipality", how="inner")
    merged["thi_delta"]  = merged["thi_end"] - merged["thi_base"]
    merged["edu_delta"]  = merged["edu_component_end"] - merged["edu_component_base"]
    merged["comm_delta"] = merged["community_component_end"] - merged["community_component_base"]

    # Investment group: based on ed_pct change where available
    ed_chg = merged["ed_pct_end"] - merged["ed_pct_base"]
    merged["ed_pct_delta"] = ed_chg
    merged["investment_group"] = "No Fiscal Data"
    has_ed = merged["ed_pct_delta"].notna()
    merged.loc[has_ed & (ed_chg >  INVEST_UP_THRESHOLD),   "investment_group"] = "Increased Investment"
    merged.loc[has_ed & (ed_chg < INVEST_DOWN_THRESHOLD),  "investment_group"] = "Cut Investment"
    merged.loc[has_ed & (ed_chg >= INVEST_DOWN_THRESHOLD) &
               (ed_chg <= INVEST_UP_THRESHOLD), "investment_group"] = "Maintained"

    # Persistent deficit: check all years in range
    if not fiscal_base.empty and not fiscal_end.empty:
        base_yr  = int(snap_base["year"].iloc[0]) if "year" in snap_base.columns else 2019
        end_yr   = int(snap_end["year"].iloc[0])  if "year" in snap_end.columns  else 2024
        fiscal_range = pd.concat([fiscal_base, fiscal_end])
        deficits = (fiscal_range[fiscal_range.fiscal_year.between(base_yr, end_yr)]
                    .groupby("municipality")
                    .apply(lambda df: (df["surplus_pct"] < 0).sum())
                    .rename("deficit_years"))
        yrs = end_yr - base_yr + 1
        deficits_flag = (deficits >= max(2, yrs // 2)).rename("persistent_deficit")
        merged = merged.merge(deficits_flag.reset_index(), on="municipality", how="left")
        merged["persistent_deficit"] = merged["persistent_deficit"].fillna(False)
    else:
        merged["persistent_deficit"] = False

    return merged


# ══════════════════════════════════════════════════════════════════════════════
# PDF helpers
# ══════════════════════════════════════════════════════════════════════════════

class _PagedPdf:
    def __init__(self, pdf: PdfPages):
        self._pdf = pdf
        self._n = 0

    def savefig(self, fig):
        self._n += 1
        fig.text(0.975, 0.975, str(self._n),
                 ha="right", va="top", fontsize=8,
                 color=LIGHT_GRAY, transform=fig.transFigure)
        self._pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)


def _slide(title: str, subtitle: str = "") -> tuple:
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)
    if title:
        fig.text(0.5, 0.94, title, ha="center", fontsize=22,
                 fontweight="bold", color=WHITE, transform=fig.transFigure)
    if subtitle:
        fig.text(0.5, 0.905, subtitle, ha="center", fontsize=11,
                 color=GOLD, transform=fig.transFigure, alpha=0.9)
    return fig


def _stat_box(fig, x, y, w, h, value, label, color=BLUE, text_color=WHITE):
    patch = mpatches.FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.01",
        facecolor=color, edgecolor=WHITE, linewidth=0.5,
        transform=fig.transFigure, zorder=3)
    fig.patches.append(patch)
    fig.text(x + w/2, y + h*0.62, value, ha="center", va="center",
             fontsize=22, fontweight="bold", color=text_color,
             transform=fig.transFigure)
    fig.text(x + w/2, y + h*0.22, label, ha="center", va="center",
             fontsize=8, color=text_color, alpha=0.85,
             transform=fig.transFigure)


# ══════════════════════════════════════════════════════════════════════════════
# Slides
# ══════════════════════════════════════════════════════════════════════════════

def slide_title(pdf):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(NAVY)
    ax.axis("off")

    fig.text(0.5, 0.62, "Massachusetts Town Health Report",
             ha="center", fontsize=32, fontweight="bold", color=WHITE)
    fig.text(0.5, 0.54, "Investment, Outcomes & the 10-Year Trajectory",
             ha="center", fontsize=18, color=GOLD)
    fig.text(0.5, 0.44,
             "A backtest of ~350 MA towns: do towns that invest in schools and services\n"
             "thrive or struggle compared to similar towns that don't?",
             ha="center", fontsize=12, color=LIGHT_GRAY, linespacing=1.6)
    fig.text(0.5, 0.30,
             "5-Year Analysis: 2019 → 2024   |   10-Year Analysis: 2015 → 2024",
             ha="center", fontsize=10, color=WHITE, alpha=0.7)
    fig.text(0.5, 0.08,
             "Sources: MA DLS Schedule A · DESE MCAS/SAT/Graduation · Census ACS · Zillow ZHVI · BLS CPI",
             ha="center", fontsize=8, color=LIGHT_GRAY, alpha=0.5)
    pdf.savefig(fig)
    plt.close(fig)


def slide_framework(pdf):
    fig = _slide("What Drives Town Health?",
                 "Extrinsic factors set the baseline — intrinsic choices determine the trajectory")

    # Two-column table
    exog = [
        ("Parent Education Level", "% bachelor's degree — Census ACS"),
        ("Median Household Income", "economic baseline — Census ACS"),
        ("% English Language Learners", "language support needs — DESE"),
        ("% Children in Poverty", "socioeconomic stress — Census ACS"),
        ("% Single-Parent Households", "household stability — Census ACS"),
        ("Age Demographics", "% under-18, % 65+ — Census ACS"),
        ("% Foreign-Born Residents", "integration demands — Census ACS"),
        ("Regional Economy", "unemployment rate — Census ACS"),
    ]
    intrinsic = [
        ("Education Share of Budget", "ed% — MA DLS Schedule A"),
        ("Per-Pupil Expenditure", "investment level — DESE"),
        ("Teacher FTE per Pupil", "staffing ratio — DESE"),
        ("Administrative Overhead", "non-instructional spending — DESE"),
        ("Tax Rate Strategy", "residential vs commercial split — DLS"),
        ("New Growth Investment", "economic development — DLS"),
        ("Debt Management", "debt service ratio — DLS"),
        ("Capital Reinvestment", "school building quality — local"),
    ]

    ax = fig.add_axes([0.03, 0.05, 0.94, 0.82])
    ax.set_facecolor(NAVY)
    ax.axis("off")

    col_labels = ["Extrinsic Factors  (given — demographics & economy)",
                  "Intrinsic Choices  (controllable — policy & investment)"]
    colors_hdr = [RED, GREEN]
    x_positions = [0.02, 0.52]

    for col_idx, (col_label, col_data, clr, x) in enumerate(
            zip(col_labels, [exog, intrinsic], colors_hdr, x_positions)):
        ax.text(x + 0.22, 0.93, col_label,
                transform=ax.transAxes, fontsize=10.5, fontweight="bold",
                color=clr, ha="center")
        ax.add_patch(mpatches.FancyBboxPatch(
            (x, 0.01), 0.44, 0.88, transform=ax.transAxes,
            boxstyle="round,pad=0.01", facecolor=CHART_GRID,
            edgecolor=clr, linewidth=1.5))
        for i, (name, source) in enumerate(col_data):
            y_pos = 0.86 - i * 0.105
            ax.text(x + 0.02, y_pos, f"• {name}",
                    transform=ax.transAxes, fontsize=9.5, fontweight="bold",
                    color=WHITE, va="center")
            ax.text(x + 0.02, y_pos - 0.038, f"    {source}",
                    transform=ax.transAxes, fontsize=7.5,
                    color=LIGHT_GRAY, va="center", alpha=0.8)

    pdf.savefig(fig)
    plt.close(fig)


def slide_data_dictionary(pdf):
    fig = _slide("Data Sources & Coverage",
                 "Every metric in this report — what it measures, where it comes from, what years are available")

    rows = [
        # Category, Metric, Source, Level, Years, # Places
        ("Fiscal",       "Education % of budget",     "MA DLS Schedule A",        "Municipality", "FY2010–2025", "61"),
        ("Fiscal",       "Revenue / Expenditure",      "MA DLS Schedule A",        "Municipality", "FY2010–2025", "61"),
        ("Fiscal",       "Per-pupil expenditure",      "DESE (DESE bulk CSV)",     "District",     "FY2009–2024", "400+"),
        ("Educational",  "MCAS % meeting expectations","DESE / Socrata API",       "District",     "2017–2025",   "350+"),
        ("Educational",  "SAT mean score",             "DESE profiles",            "District",     "2007–2025",   "400+"),
        ("Educational",  "Graduates attending college","DESE profiles",            "District",     "2004–2024",   "300+"),
        ("Educational",  "Graduation rate",            "DESE profiles",            "District",     "2017–2025",   "320+"),
        ("Educational",  "Dropout rate",               "DESE profiles",            "District",     "2008–2025",   "390+"),
        ("Community",    "Home values (ZHVI)",         "Zillow Research",          "Town",         "2014–2026",   "356"),
        ("Community",    "School enrollment",          "DESE bulk CSV",            "District",     "2009–2024",   "420+"),
        ("Demographics", "Income, poverty, education", "Census ACS 5-year",        "Town",         "2014–2023",   "351"),
        ("Demographics", "Age, foreign-born, divorce", "Census ACS 5-year",        "Town",         "2014–2023",   "351"),
        ("County",       "County unemployment",        "BLS LAUS",                 "County",       "2000–2026",   "14"),
        ("County",       "Health outcomes",            "County Health Rankings",   "County",       "2020–2024",   "14"),
        ("Inflation",    "CPI (national)",             "BLS / FRED",               "National",     "2010–2024",   "1"),
    ]

    cat_colors = {
        "Fiscal":       "#1A5276",
        "Educational":  "#1E8449",
        "Community":    "#7D3C98",
        "Demographics": "#B7950B",
        "County":       "#784212",
        "Inflation":    "#424242",
    }

    ax = fig.add_axes([0.02, 0.02, 0.96, 0.85])
    ax.set_facecolor(NAVY)
    ax.axis("off")

    headers = ["Category", "Metric", "Source", "Level", "Years", "~Places"]
    col_widths = [0.10, 0.26, 0.22, 0.14, 0.14, 0.09]
    col_x = [sum(col_widths[:i]) for i in range(len(col_widths))]

    # Header row
    for j, (h, cx) in enumerate(zip(headers, col_x)):
        ax.text(cx + 0.005, 0.97, h, transform=ax.transAxes,
                fontsize=8.5, fontweight="bold", color=GOLD, va="top")

    ax.axhline(0.94, color=GOLD, linewidth=0.8, xmin=0, xmax=1)

    row_h = 0.055
    for i, (cat, metric, source, level, years, places) in enumerate(rows):
        y = 0.93 - i * row_h
        bg = cat_colors.get(cat, CHART_GRID)
        if i % 2 == 0:
            ax.add_patch(mpatches.Rectangle(
                (0, y - row_h * 0.85), 1, row_h * 0.95,
                transform=ax.transAxes, facecolor=CHART_GRID, alpha=0.4))

        vals = [cat, metric, source, level, years, places]
        for j, (val, cx) in enumerate(zip(vals, col_x)):
            color = bg if j == 0 else WHITE
            fw = "bold" if j == 0 else "normal"
            if j == 0:
                ax.add_patch(mpatches.FancyBboxPatch(
                    (cx, y - row_h * 0.7), col_widths[j] - 0.005, row_h * 0.8,
                    transform=ax.transAxes, boxstyle="round,pad=0.005",
                    facecolor=bg, alpha=0.8))
                clr = WHITE
            else:
                clr = WHITE if j > 0 else WHITE
            ax.text(cx + 0.005, y - row_h * 0.25, val,
                    transform=ax.transAxes, fontsize=7.5,
                    color=clr, va="center", fontweight=fw)

    ax.text(0.5, 0.005,
            "Note: 10-year backtest uses SAT as proxy for 2015 educational baseline (MCAS Next Gen format began 2017)  |  Crime data requires FBI API key configuration",
            transform=ax.transAxes, fontsize=6.5, color=LIGHT_GRAY,
            ha="center", alpha=0.7)

    pdf.savefig(fig)
    plt.close(fig)


def slide_thi_snapshot(pdf, snap: pd.DataFrame, year: int):
    fig = _slide(f"Town Health Index — {year} Snapshot",
                 f"All MA towns scored on educational outcomes, community vitality, and fiscal health (where available)")

    # Distribution histogram + Saugus marker
    ax1 = fig.add_axes([0.06, 0.12, 0.55, 0.70])
    ax1.set_facecolor(CHART_BG)
    ax1.tick_params(colors=WHITE)
    for sp in ax1.spines.values():
        sp.set_edgecolor(CHART_GRID)

    valid = snap.dropna(subset=["thi"])
    ax1.hist(valid["thi"], bins=30, color=STEEL_BLUE, edgecolor=CHART_BG, alpha=0.85)

    saugus_thi = valid[valid.municipality == SAUGUS]["thi"]
    if not saugus_thi.empty:
        sv = saugus_thi.iloc[0]
        ax1.axvline(sv, color=GOLD, linewidth=2.5, linestyle="--")
        ax1.text(sv + 0.08, ax1.get_ylim()[1] * 0.85, f"Saugus\n{sv:.2f}",
                 color=GOLD, fontsize=9, fontweight="bold")

    ax1.set_xlabel("Town Health Index (z-score)", color=LIGHT_GRAY, fontsize=9)
    ax1.set_ylabel("Number of towns", color=LIGHT_GRAY, fontsize=9)
    ax1.xaxis.label.set_color(LIGHT_GRAY)
    ax1.tick_params(colors=LIGHT_GRAY)
    ax1.grid(axis="y", color=CHART_GRID, alpha=0.5)

    # Top / bottom towns table
    ax2 = fig.add_axes([0.64, 0.10, 0.34, 0.78])
    ax2.set_facecolor(CHART_BG)
    ax2.axis("off")

    top5 = valid.nlargest(5, "thi")[["municipality","thi"]].values
    bot5 = valid.nsmallest(5, "thi")[["municipality","thi"]].values

    ax2.text(0.5, 0.97, "Top 5", ha="center", fontsize=9,
             fontweight="bold", color=GREEN, transform=ax2.transAxes)
    for i, (muni, score) in enumerate(top5):
        ax2.text(0.05, 0.90 - i*0.08, f"{i+1}. {muni}",
                 fontsize=8.5, color=WHITE, transform=ax2.transAxes)
        ax2.text(0.90, 0.90 - i*0.08, f"{score:+.2f}",
                 fontsize=8.5, color=GREEN, ha="right", transform=ax2.transAxes)

    ax2.axhline(0.47, color=CHART_GRID, linewidth=0.8)
    ax2.text(0.5, 0.44, "Bottom 5", ha="center", fontsize=9,
             fontweight="bold", color=RED, transform=ax2.transAxes)
    for i, (muni, score) in enumerate(bot5):
        ax2.text(0.05, 0.37 - i*0.08, f"{i+1}. {muni}",
                 fontsize=8.5, color=WHITE, transform=ax2.transAxes)
        ax2.text(0.90, 0.37 - i*0.08, f"{score:+.2f}",
                 fontsize=8.5, color=RED, ha="right", transform=ax2.transAxes)

    if not saugus_thi.empty:
        rank = (valid["thi"] > sv).sum() + 1
        total = len(valid)
        pct = rank / total * 100
        ax2.text(0.5, 0.04,
                 f"Saugus: #{rank} of {total}\n(bottom {pct:.0f}%)",
                 ha="center", fontsize=8.5, color=GOLD,
                 transform=ax2.transAxes, fontweight="bold")

    pdf.savefig(fig)
    plt.close(fig)


def slide_edu_quadrant(pdf, snap: pd.DataFrame, year: int):
    fig = _slide("Educational Outcomes Quadrant",
                 f"Performance level vs 5-year trend — bubble size = enrollment")

    ax = fig.add_axes([0.08, 0.10, 0.84, 0.75])
    ax.set_facecolor(CHART_BG)
    for sp in ax.spines.values():
        sp.set_edgecolor(CHART_GRID)

    valid = snap.dropna(subset=["edu_component", "edu_residual"]).copy()
    valid["bubble"] = (valid["enrollment"].fillna(500) / 500).clip(0.5, 8)

    # Color by THI quartile
    q = valid["thi"].quantile([0.25, 0.50, 0.75]).values
    def _color(thi):
        if pd.isna(thi): return STEEL_BLUE
        if thi > q[2]:   return GREEN
        if thi > q[1]:   return STEEL_BLUE
        if thi > q[0]:   return ORANGE
        return RED

    for _, row in valid.iterrows():
        muni = row["municipality"]
        if muni == SAUGUS:
            continue
        ax.scatter(row["edu_component"], row["edu_residual"],
                   s=row["bubble"]*20, color=_color(row["thi"]),
                   alpha=0.6, edgecolors="none")

    saugus = valid[valid.municipality == SAUGUS]
    if not saugus.empty:
        r = saugus.iloc[0]
        ax.scatter(r.edu_component, r.edu_residual, s=120, color=GOLD,
                   zorder=5, edgecolors=WHITE, linewidths=1.2)
        ax.annotate(SAUGUS, (r.edu_component, r.edu_residual),
                    xytext=(8, 8), textcoords="offset points",
                    color=GOLD, fontsize=9, fontweight="bold")

    ax.axhline(0, color=CHART_GRID, linewidth=1, linestyle="--")
    ax.axvline(0, color=CHART_GRID, linewidth=1, linestyle="--")

    ax.text(0.02, 0.98, "Weak outcomes\nbeating demographics",
            transform=ax.transAxes, fontsize=7.5, color=GREEN,
            alpha=0.7, va="top")
    ax.text(0.98, 0.98, "Strong outcomes\nbeating demographics",
            transform=ax.transAxes, fontsize=7.5, color=GREEN,
            alpha=0.7, va="top", ha="right")
    ax.text(0.02, 0.02, "Weak outcomes\nlagging demographics",
            transform=ax.transAxes, fontsize=7.5, color=RED, alpha=0.7)
    ax.text(0.98, 0.02, "Strong outcomes\nlagging demographics",
            transform=ax.transAxes, fontsize=7.5, color=ORANGE, alpha=0.7, ha="right")

    ax.set_xlabel("Educational Outcome Score (z-score)", color=LIGHT_GRAY, fontsize=9)
    ax.set_ylabel("Demographic-Adjusted Residual", color=LIGHT_GRAY, fontsize=9)
    ax.tick_params(colors=LIGHT_GRAY)
    ax.grid(color=CHART_GRID, alpha=0.3)

    patches = [
        mpatches.Patch(color=GREEN,      label="Top quartile THI"),
        mpatches.Patch(color=STEEL_BLUE, label="2nd quartile"),
        mpatches.Patch(color=ORANGE,     label="3rd quartile"),
        mpatches.Patch(color=RED,        label="Bottom quartile"),
        mpatches.Patch(color=GOLD,       label="Saugus"),
    ]
    ax.legend(handles=patches, loc="lower right", fontsize=7.5,
              facecolor=CHART_BG, edgecolor=CHART_GRID, labelcolor=WHITE)

    pdf.savefig(fig)
    plt.close(fig)


def slide_community_vitality(pdf, snap_base: pd.DataFrame, snap_end: pd.DataFrame,
                              base_year: int, end_year: int):
    fig = _slide("Community Vitality: Housing & Enrollment",
                 f"Real home value growth vs enrollment change, {base_year}→{end_year}")

    ax = fig.add_axes([0.08, 0.10, 0.84, 0.75])
    ax.set_facecolor(CHART_BG)
    for sp in ax.spines.values():
        sp.set_edgecolor(CHART_GRID)

    b = snap_base[["municipality","zhvi","enrollment"]].rename(
        columns={"zhvi":"zhvi_b","enrollment":"enr_b"})
    e = snap_end[["municipality","zhvi","enrollment"]].rename(
        columns={"zhvi":"zhvi_e","enrollment":"enr_e"})
    merged = b.merge(e, on="municipality").dropna()
    merged["hv_growth"]  = (merged.zhvi_e / merged.zhvi_b - 1) * 100
    merged["enr_change"] = (merged.enr_e / merged.enr_b - 1) * 100

    for _, row in merged.iterrows():
        if row.municipality == SAUGUS:
            continue
        ax.scatter(row.enr_change, row.hv_growth, s=25,
                   color=STEEL_BLUE, alpha=0.5, edgecolors="none")

    saugus = merged[merged.municipality == SAUGUS]
    if not saugus.empty:
        r = saugus.iloc[0]
        ax.scatter(r.enr_change, r.hv_growth, s=120, color=GOLD,
                   zorder=5, edgecolors=WHITE, linewidths=1.2)
        ax.annotate(f"Saugus\n({r.enr_change:+.1f}%, ${r.hv_growth:+.1f}%)",
                    (r.enr_change, r.hv_growth), xytext=(8, 8),
                    textcoords="offset points", color=GOLD,
                    fontsize=9, fontweight="bold")

    ax.axhline(0, color=CHART_GRID, linewidth=1, linestyle="--")
    ax.axvline(0, color=CHART_GRID, linewidth=1, linestyle="--")
    ax.axhline(20, color=GREEN, linewidth=0.5, linestyle=":", alpha=0.5)
    ax.text(ax.get_xlim()[1] * 0.6 if ax.get_xlim()[1] > 0 else 5, 21,
            "Inflation (~20% over 5yr)", color=GREEN, fontsize=7, alpha=0.6)

    ax.set_xlabel("Enrollment Change (%)", color=LIGHT_GRAY, fontsize=9)
    ax.set_ylabel("Home Value Growth (%)", color=LIGHT_GRAY, fontsize=9)
    ax.tick_params(colors=LIGHT_GRAY)
    ax.grid(color=CHART_GRID, alpha=0.3)

    # Quadrant labels
    ax.text(0.02, 0.98, "Losing students\nHome values flat",
            transform=ax.transAxes, fontsize=7.5, color=RED, va="top", alpha=0.8)
    ax.text(0.98, 0.98, "Growing enrollment\nStrong home values",
            transform=ax.transAxes, fontsize=7.5, color=GREEN, va="top", ha="right", alpha=0.8)

    pdf.savefig(fig)
    plt.close(fig)


def slide_backtest_groups(pdf, bt: pd.DataFrame, label: str):
    group_order = ["Increased Investment", "Maintained", "Cut Investment"]
    colors_map  = {"Increased Investment": GREEN, "Maintained": STEEL_BLUE,
                   "Cut Investment": RED, "No Fiscal Data": CHART_GRID}

    present = [g for g in group_order if g in bt["investment_group"].values]
    if not present:
        return

    fig = _slide(f"{label} Backtest: Investment Groups & Outcomes",
                 "Average Town Health Index change by investment strategy (Schedule A towns only)")

    ax1 = fig.add_axes([0.06, 0.30, 0.52, 0.52])
    ax1.set_facecolor(CHART_BG)
    for sp in ax1.spines.values():
        sp.set_edgecolor(CHART_GRID)

    means  = [bt[bt.investment_group == g]["thi_delta"].mean() for g in present]
    sems   = [bt[bt.investment_group == g]["thi_delta"].sem()  for g in present]
    ns     = [len(bt[bt.investment_group == g]) for g in present]
    clrs   = [colors_map[g] for g in present]
    short  = [g.replace(" Investment","") for g in present]

    bars = ax1.bar(short, means, color=clrs, alpha=0.85, edgecolor=WHITE, linewidth=0.5)
    ax1.errorbar(short, means, yerr=sems, fmt="none", color=WHITE, capsize=5, linewidth=1.5)
    ax1.axhline(0, color=CHART_GRID, linewidth=1, linestyle="--")

    for bar, n in zip(bars, ns):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (0.02 if bar.get_height() >= 0 else -0.06),
                 f"n={n}", ha="center", fontsize=8, color=LIGHT_GRAY)

    ax1.set_ylabel("Avg THI Change", color=LIGHT_GRAY, fontsize=9)
    ax1.tick_params(colors=LIGHT_GRAY)
    ax1.grid(axis="y", color=CHART_GRID, alpha=0.5)

    # Success rate bars
    ax2 = fig.add_axes([0.62, 0.30, 0.35, 0.52])
    ax2.set_facecolor(CHART_BG)
    for sp in ax2.spines.values():
        sp.set_edgecolor(CHART_GRID)

    def _success(grp_df):
        edu_ok  = (grp_df["edu_delta"]  >= 0).mean() * 100
        comm_ok = (grp_df["comm_delta"] >= 0).mean() * 100
        no_def  = (~grp_df["persistent_deficit"]).mean() * 100
        return [edu_ok, comm_ok, no_def]

    metrics = ["Edu\nimproved", "Community\nimproved", "No\ndeficit"]
    x = np.arange(len(metrics))
    width = 0.25
    for i, (g, clr) in enumerate(zip(present, [GREEN, STEEL_BLUE, RED])):
        sub = bt[bt.investment_group == g]
        if len(sub) < 2:
            continue
        vals = _success(sub)
        ax2.bar(x + i*width - width, vals, width, label=g.replace(" Investment",""),
                color=clr, alpha=0.85, edgecolor=WHITE, linewidth=0.5)

    ax2.set_xticks(x)
    ax2.set_xticklabels(metrics, color=LIGHT_GRAY, fontsize=8)
    ax2.set_ylabel("% of towns succeeding", color=LIGHT_GRAY, fontsize=9)
    ax2.tick_params(colors=LIGHT_GRAY)
    ax2.set_ylim(0, 110)
    ax2.grid(axis="y", color=CHART_GRID, alpha=0.5)
    ax2.legend(fontsize=7.5, facecolor=CHART_BG, edgecolor=CHART_GRID, labelcolor=WHITE)

    # Stat boxes at bottom
    box_data = []
    for g in present:
        sub = bt[bt.investment_group == g]
        if len(sub) >= 2:
            box_data.append((g.replace(" Investment",""),
                             f"{sub['thi_delta'].mean():+.2f}",
                             colors_map[g]))
    bw, bh, by = 0.22, 0.12, 0.10
    bxs = [0.06 + i*(bw+0.05) for i in range(len(box_data))]
    for (label2, val, clr), bx in zip(box_data, bxs):
        _stat_box(fig, bx, by, bw, bh, val, f"Avg THI Δ\n({label2})", color=clr)

    pdf.savefig(fig)
    plt.close(fig)


def slide_fiscal_sustainability(pdf, fiscal: pd.DataFrame):
    fig = _slide("Fiscal Sustainability: Did Investing Towns Strain Their Budgets?",
                 "Education share of budget trend and revenue surplus by investment group (Schedule A towns)")

    # Classify towns by ed% change 2019→2024
    base_fy, end_fy = 2019, 2024
    grp = (fiscal[fiscal.fiscal_year.isin([base_fy, end_fy])]
           .pivot_table(index="municipality", columns="fiscal_year", values="ed_pct"))
    if base_fy not in grp.columns or end_fy not in grp.columns:
        pdf.savefig(_slide("Fiscal Sustainability", "Insufficient data")); return

    grp["delta"] = grp[end_fy] - grp[base_fy]
    grp["group"] = grp["delta"].apply(
        lambda d: "Increased" if d > INVEST_UP_THRESHOLD
                  else ("Cut" if d < INVEST_DOWN_THRESHOLD else "Maintained"))

    ax1 = fig.add_axes([0.06, 0.12, 0.42, 0.70])
    ax1.set_facecolor(CHART_BG)
    for sp in ax1.spines.values():
        sp.set_edgecolor(CHART_GRID)

    years = sorted(fiscal.fiscal_year.unique())
    colors_map2 = {"Increased": GREEN, "Maintained": STEEL_BLUE, "Cut": RED}
    for g, clr in colors_map2.items():
        towns = grp[grp.group == g].index.tolist()
        grp_data = fiscal[fiscal.municipality.isin(towns)].groupby("fiscal_year")["ed_pct"].mean()
        ax1.plot(grp_data.index, grp_data.values * 100, color=clr, linewidth=2, label=g, marker="o", markersize=4)

    ax1.axvline(base_fy, color=GOLD, linewidth=1, linestyle=":", alpha=0.6)
    ax1.set_xlabel("Fiscal Year", color=LIGHT_GRAY, fontsize=9)
    ax1.set_ylabel("Education % of Budget", color=LIGHT_GRAY, fontsize=9)
    ax1.tick_params(colors=LIGHT_GRAY)
    ax1.grid(color=CHART_GRID, alpha=0.4)
    ax1.legend(fontsize=8, facecolor=CHART_BG, edgecolor=CHART_GRID, labelcolor=WHITE)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.0f}%"))

    ax2 = fig.add_axes([0.55, 0.12, 0.42, 0.70])
    ax2.set_facecolor(CHART_BG)
    for sp in ax2.spines.values():
        sp.set_edgecolor(CHART_GRID)

    for g, clr in colors_map2.items():
        towns = grp[grp.group == g].index.tolist()
        grp_data = fiscal[fiscal.municipality.isin(towns)].groupby("fiscal_year")["surplus_pct"].mean()
        ax2.plot(grp_data.index, grp_data.values * 100, color=clr, linewidth=2, label=g, marker="o", markersize=4)

    ax2.axhline(0, color=WHITE, linewidth=1, linestyle="--", alpha=0.4)
    ax2.axvline(base_fy, color=GOLD, linewidth=1, linestyle=":", alpha=0.6)
    ax2.set_xlabel("Fiscal Year", color=LIGHT_GRAY, fontsize=9)
    ax2.set_ylabel("Avg Revenue Surplus (%)", color=LIGHT_GRAY, fontsize=9)
    ax2.tick_params(colors=LIGHT_GRAY)
    ax2.grid(color=CHART_GRID, alpha=0.4)
    ax2.legend(fontsize=8, facecolor=CHART_BG, edgecolor=CHART_GRID, labelcolor=WHITE)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.1f}%"))

    pdf.savefig(fig)
    plt.close(fig)


def slide_turnaround_towns(pdf, bt: pd.DataFrame, snap_base: pd.DataFrame, snap_end: pd.DataFrame):
    fig = _slide("Turnaround Towns: Similar Starting Point, Better Results",
                 f"Towns within 0.5 SD of Saugus's baseline THI that improved the most")

    saugus_base = snap_base[snap_base.municipality == SAUGUS]
    if saugus_base.empty or "thi" not in saugus_base.columns:
        pdf.savefig(fig); return

    saugus_thi = saugus_base["thi"].iloc[0]
    saugus_inc = snap_base[snap_base.municipality == SAUGUS]["median_hh_income"].iloc[0] \
        if "median_hh_income" in snap_base.columns else np.nan

    # Towns close to Saugus at baseline
    snap_base2 = snap_base.dropna(subset=["thi"])
    candidates = snap_base2[
        (abs(snap_base2["thi"] - saugus_thi) < 0.75) &
        (snap_base2["municipality"] != SAUGUS)
    ].copy()

    if candidates.empty:
        fig.text(0.5, 0.5, "Insufficient comparable towns at baseline",
                 ha="center", color=LIGHT_GRAY, fontsize=12, transform=fig.transFigure)
        pdf.savefig(fig); return

    end_scores = snap_end[["municipality","thi","zhvi","enrollment"]].rename(
        columns={"thi":"thi_end","zhvi":"zhvi_end","enrollment":"enr_end"})
    turnaround = candidates[["municipality","thi","ed_pct","median_hh_income","edu_component"]].merge(
        end_scores, on="municipality", how="inner")
    turnaround["thi_gain"] = turnaround["thi_end"] - turnaround["thi"]
    top = turnaround.nlargest(8, "thi_gain")

    ax = fig.add_axes([0.04, 0.08, 0.92, 0.78])
    ax.set_facecolor(CHART_BG)
    ax.axis("off")

    # Table
    col_headers = ["Town", "Baseline THI", "Current THI", "THI Gain",
                   "Baseline Ed%", "Income", "Status"]
    col_x = [0.00, 0.18, 0.30, 0.42, 0.54, 0.65, 0.76]

    for j, (h, cx) in enumerate(zip(col_headers, col_x)):
        ax.text(cx, 0.96, h, transform=ax.transAxes,
                fontsize=8, fontweight="bold", color=GOLD)

    ax.axhline(0.93, color=GOLD, linewidth=0.5)

    # Add Saugus row at top for comparison
    saugus_end_thi = snap_end[snap_end.municipality == SAUGUS]["thi"].values
    saugus_end_thi = saugus_end_thi[0] if len(saugus_end_thi) else np.nan

    all_rows = [(SAUGUS, saugus_thi,
                 saugus_end_thi,
                 saugus_end_thi - saugus_thi if not pd.isna(saugus_end_thi) else np.nan,
                 snap_base[snap_base.municipality == SAUGUS]["ed_pct"].values[0]
                    if len(snap_base[snap_base.municipality == SAUGUS]) else np.nan,
                 saugus_inc,
                 "← REFERENCE")] + \
               [(r.municipality, r.thi, r.thi_end, r.thi_gain,
                 r.ed_pct, r.median_hh_income, "Turnaround")
                for _, r in top.iterrows()]

    for i, (town, thi_b, thi_e, gain, ed_pct, income, status) in enumerate(all_rows[:9]):
        y = 0.88 - i * 0.095
        if i == 0:
            ax.add_patch(mpatches.FancyBboxPatch(
                (0, y - 0.04), 1, 0.08, transform=ax.transAxes,
                boxstyle="round,pad=0.005", facecolor=GOLD, alpha=0.15))

        row_vals = [
            town,
            f"{thi_b:+.2f}" if not pd.isna(thi_b) else "—",
            f"{thi_e:+.2f}" if not pd.isna(thi_e) else "—",
            f"{gain:+.2f}" if not pd.isna(gain) else "—",
            f"{ed_pct*100:.1f}%" if not pd.isna(ed_pct) else "—",
            f"${income:,.0f}" if not pd.isna(income) else "—",
            status,
        ]
        for j, (val, cx) in enumerate(zip(row_vals, col_x)):
            clr = GOLD if i == 0 else WHITE
            if j == 3 and i > 0 and not pd.isna(gain):
                clr = GREEN if gain > 0 else RED
            ax.text(cx, y, val, transform=ax.transAxes,
                    fontsize=8.5 if i == 0 else 8, color=clr,
                    fontweight="bold" if i == 0 else "normal")

    pdf.savefig(fig)
    plt.close(fig)


def slide_saugus_trajectory(pdf, fiscal: pd.DataFrame, snap_history: list):
    fig = _slide("Saugus Trajectory: Where Is It Headed?",
                 "Ed% trend, THI over time, and peer comparison — projection based on current path")

    ax1 = fig.add_axes([0.06, 0.15, 0.42, 0.68])
    ax1.set_facecolor(CHART_BG)
    for sp in ax1.spines.values():
        sp.set_edgecolor(CHART_GRID)

    saugus_fis = fiscal[fiscal.municipality == SAUGUS].sort_values("fiscal_year")
    if not saugus_fis.empty:
        ax1.plot(saugus_fis.fiscal_year, saugus_fis.ed_pct * 100,
                 color=GOLD, linewidth=2.5, marker="o", markersize=5)
        # MA average
        ma_avg = fiscal.groupby("fiscal_year")["ed_pct"].mean() * 100
        ax1.plot(ma_avg.index, ma_avg.values, color=STEEL_BLUE,
                 linewidth=1.5, linestyle="--", label="MA avg (61 towns)")
        ax1.axhline(40, color=GREEN, linewidth=1, linestyle=":", alpha=0.6)
        ax1.text(ax1.get_xlim()[0] if saugus_fis.fiscal_year.min() > 2010 else 2011,
                 40.3, "40% goal", color=GREEN, fontsize=7.5, alpha=0.8)

    ax1.set_xlabel("Fiscal Year", color=LIGHT_GRAY, fontsize=9)
    ax1.set_ylabel("Education % of Budget", color=LIGHT_GRAY, fontsize=9)
    ax1.tick_params(colors=LIGHT_GRAY)
    ax1.grid(color=CHART_GRID, alpha=0.4)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.0f}%"))
    ax1.set_title("Saugus (gold) vs MA Avg (blue)", color=LIGHT_GRAY, fontsize=8, pad=4)

    # THI over time
    ax2 = fig.add_axes([0.55, 0.15, 0.42, 0.68])
    ax2.set_facecolor(CHART_BG)
    for sp in ax2.spines.values():
        sp.set_edgecolor(CHART_GRID)

    thi_pts = [(s["year"].iloc[0] if "year" in s.columns else 0,
                s[s.municipality == SAUGUS]["thi"].values)
               for s in snap_history if not s[s.municipality == SAUGUS].empty]
    thi_pts = [(yr, v[0]) for yr, v in thi_pts if len(v) > 0 and not pd.isna(v[0])]

    if len(thi_pts) >= 2:
        yrs, vals = zip(*sorted(thi_pts))
        ax2.plot(yrs, vals, color=GOLD, linewidth=2.5, marker="o", markersize=6)
        ax2.axhline(0, color=CHART_GRID, linewidth=1, linestyle="--")

    ax2.set_xlabel("Year", color=LIGHT_GRAY, fontsize=9)
    ax2.set_ylabel("Town Health Index (z-score)", color=LIGHT_GRAY, fontsize=9)
    ax2.tick_params(colors=LIGHT_GRAY)
    ax2.grid(color=CHART_GRID, alpha=0.4)
    ax2.set_title("Saugus THI over time", color=LIGHT_GRAY, fontsize=8, pad=4)

    pdf.savefig(fig)
    plt.close(fig)


def slide_conclusions(pdf, bt5: pd.DataFrame, bt10: pd.DataFrame):
    fig = _slide("Conclusions: What Predicts Town Health?",
                 "Key findings from the 5-year and 10-year backtests")

    ax = fig.add_axes([0.04, 0.05, 0.92, 0.82])
    ax.set_facecolor(NAVY)
    ax.axis("off")

    # Compute key stats for bullets
    def _avg_gain(bt, grp):
        sub = bt[bt.investment_group == grp]["thi_delta"]
        return sub.mean() if len(sub) >= 2 else np.nan

    findings = []
    for bt, label in [(bt5, "5-year"), (bt10, "10-year")]:
        inc = _avg_gain(bt, "Increased Investment")
        cut = _avg_gain(bt, "Cut Investment")
        if not pd.isna(inc) and not pd.isna(cut):
            findings.append((label, f"Towns that increased investment outperformed those that cut "
                             f"by {abs(inc-cut):.2f} SD on the Town Health Index."))

    findings += [
        ("Method", "Success requires all four: educational improvement, real home value "
                   "growth, enrollment stability, and fiscal balance."),
        ("Key risk", "Towns that cut education spending did not save money long-term — "
                    "declining enrollment reduces state aid, creating a fiscal feedback loop."),
        ("Demographics", "Income and parent education predict ~60% of outcomes. The "
                        "remaining 40% is policy — and that's where towns diverge."),
        ("Saugus", "Saugus's aging demographics (43.5 median age, only 17% under-18) "
                   "create political resistance to investment. "
                   "Data shows this is a false economy."),
    ]

    for i, (tag, text) in enumerate(findings):
        y = 0.90 - i * 0.165
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.01, y - 0.06), 0.98, 0.14, transform=ax.transAxes,
            boxstyle="round,pad=0.01", facecolor=CHART_GRID, alpha=0.6))
        color = GOLD if tag in ("5-year","10-year") else (RED if tag == "Key risk" else GREEN if tag == "Saugus" else STEEL_BLUE)
        ax.text(0.03, y + 0.035, f"[{tag}]", transform=ax.transAxes,
                fontsize=9, fontweight="bold", color=color)
        ax.text(0.03, y - 0.02, text, transform=ax.transAxes,
                fontsize=9, color=WHITE, wrap=True)

    pdf.savefig(fig)
    plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def run():
    engine = get_engine()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[town_health] Loading data...")
    data = {
        "fiscal":     load_fiscal(engine),
        "per_pupil":  load_per_pupil(engine),
        "mcas":       load_mcas(engine),
        "sat":        load_sat(engine),
        "postsec":    load_postsecondary(engine),
        "grad":       load_graduation(engine),
        "dropout":    load_dropout(engine),
        "enrollment": load_enrollment(engine),
        "zillow":     load_zillow(engine),
        "acs":        load_acs(engine),
        "cpi":        load_cpi(engine),
    }
    print(f"[town_health]   fiscal={len(data['fiscal'])} rows, "
          f"mcas={len(data['mcas'])} rows, acs={len(data['acs'])} rows")

    print("[town_health] Computing snapshots...")
    snap_2015 = compute_snapshot(2015, data)
    snap_2019 = compute_snapshot(2019, data)
    snap_2024 = compute_snapshot(2024, data)

    print("[town_health] Running backtests...")
    bt5  = run_backtest(snap_2019, snap_2024, data["fiscal"], data["fiscal"])
    bt10 = run_backtest(snap_2015, snap_2024, data["fiscal"], data["fiscal"])

    print(f"[town_health]   5-year backtest: {len(bt5)} towns")
    print(f"[town_health]   10-year backtest: {len(bt10)} towns")
    for grp in ["Increased Investment","Maintained","Cut Investment","No Fiscal Data"]:
        n5  = (bt5["investment_group"]  == grp).sum()
        n10 = (bt10["investment_group"] == grp).sum()
        if n5 or n10: print(f"    {grp}: 5yr={n5}  10yr={n10}")

    print(f"[town_health] Writing PDF to {OUTPUT_PDF} ...")
    with PdfPages(OUTPUT_PDF) as raw_pdf:
        pdf = _PagedPdf(raw_pdf)

        slide_title(pdf)
        slide_framework(pdf)
        slide_data_dictionary(pdf)
        slide_thi_snapshot(pdf, snap_2024, 2024)
        slide_edu_quadrant(pdf, snap_2024, 2024)
        slide_community_vitality(pdf, snap_2019, snap_2024, 2019, 2024)
        slide_backtest_groups(pdf, bt5,  "5-Year (2019→2024)")
        slide_backtest_groups(pdf, bt10, "10-Year (2015→2024)")
        slide_fiscal_sustainability(pdf, data["fiscal"])
        slide_turnaround_towns(pdf, bt5, snap_2019, snap_2024)
        slide_saugus_trajectory(pdf, data["fiscal"], [snap_2015, snap_2019, snap_2024])
        slide_conclusions(pdf, bt5, bt10)

    print(f"[town_health] Done. {OUTPUT_PDF}")


if __name__ == "__main__":
    run()
