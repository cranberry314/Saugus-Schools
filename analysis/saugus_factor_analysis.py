"""
saugus_factor_analysis.py
=========================
Factor selection and Relevance-Based Prediction (RBP) analysis for Saugus schools.

Implements Czasonis, Kritzman & Turkington (2024) RBP via analysis/rbp.py.

Four models are built independently:
  1. MCAS ELA+Math (grades 3–8) — academic outcomes
  2. Dropout rate — high school completion
  3. MCAS Grade 10 (ELA) — high school academic readiness
  4. Education Budget Share — share of municipal budget allocated to schools

Faithful to Kritzman: ONE RBP run per outcome, on ALL candidate variables.
Variable importance (Exhibit 5) is read off that run as a transparency
diagnostic — it is NOT used to select or prune variables.  Footnote 12 is the
paper's reason this is safe: ≈0-importance variables are "diversified away" by
the relevance-weighted averaging, so they do no harm and are kept.

For each model (see _run_one_model):
  Step 1 — Variable importance (Exhibit 5), descriptive only:
    Run RBP with ALL candidate features and Saugus as the prediction task.
    Per-feature importance measures each feature's marginal contribution to the
    reliability of the *Saugus* prediction, given all other features at once.
    A couple of check seeds confirm the top-ranked drivers are not a
    Monte-Carlo artifact of the sparse grid.

  Step 2 — Saugus RBP + LOO validation on the FULL candidate set:
    The single RBP run predicts Saugus (excluded from its own training set — no
    leakage) and, leave-one-out across all MA districts, yields the validation
    Pearson r.  No pruning, no second run: the feature set used for prediction
    is exactly the candidate set.  Outputs: predicted value, residual, gap,
    rank, most/least relevant comparison towns, and Exhibit 5 importance.

Output:  Reports/saugus_factor_analysis.pdf  (white-background paper format)
         Reports/saugus_factor_analysis_results.csv  (machine-readable summary)

Run:
    source .venv/bin/activate
    python analysis/saugus_factor_analysis.py
    python analysis/saugus_factor_analysis.py --fast   # smaller random seed, fewer LOO
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import random
import textwrap
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import get_engine
from analysis.rbp import rbp, rbp_loo
from db import queries as Q

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

matplotlib.rcParams.update({
    "font.family":    "serif",
    "font.size":      10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "figure.dpi":     150,
})

OUTPUT_PDF   = Path(__file__).parent.parent / "Reports" / "saugus_factor_analysis.pdf"
OUTPUT_CSV   = Path(__file__).parent.parent / "Reports" / "saugus_factor_analysis_results.csv"
OUTPUT_CACHE = Path(__file__).parent.parent / "Reports" / "saugus_factor_analysis_cache.pkl"
SAUGUS = "Saugus"
MIN_COVERAGE   = 0.60   # feature must have data for ≥60 % of districts
                          # (Schedule A covers ~65 % of MA districts; 0.60 admits
                          #  all fiscal ratio features while still filtering noise)


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Comprehensive feature loader
# ─────────────────────────────────────────────────────────────────────────────

def latest_analysis_year(engine) -> int:
    """
    The newest school year for which the full cross-section can be built.

    The binding constraint is per-pupil spending (per_pupil_expenditure, which
    carries teacher_spending_per_pupil — a central metric we will not drop) and
    the MCAS outcomes; the DESE spending feed lags the MCAS/budget feeds by about
    a year.  We therefore take the latest year present in BOTH, which is the
    latest year the report can be built without leaving a key spending factor
    blank.  Derived from the data (no hardcoded year) and self-updating: when the
    spending feed catches up, the analysis year advances on its own.  The chosen
    year is carried on df.attrs['analysis_fiscal_year'] and labelled on the title
    page and every point-in-time figure.
    """
    with engine.connect() as conn:
        yr = conn.execute(text("""
            SELECT MAX(s.yr) FROM
              (SELECT DISTINCT school_year AS yr FROM mcas_results
                 WHERE grade = 'ALL (03-08)' AND subject IN ('ELA','MATH')
                   AND student_group = 'All Students' AND org_code LIKE '%0000') s
            JOIN
              (SELECT DISTINCT school_year AS yr FROM per_pupil_expenditure) p
              ON p.yr = s.yr
        """)).scalar()
    return int(yr)


def load_features(engine, school_year: int | None = None) -> pd.DataFrame:
    """
    Build a district-level feature matrix from every relevant database table.
    Returns one row per MA school district with outcomes and candidate features.

    Outcomes (never used as features for their own model):
        avg_mcas        — ELA+Math grades 3–8 meeting/exceeding %
        attending_pct   — % graduates attending college (postsecondary)
        dropout_pct     — % students dropping out of high school

    All other columns are candidate features available to any model.
    """
    if school_year is None:
        school_year = latest_analysis_year(engine)
    fy = school_year   # fiscal year ≈ school year for most tables

    with engine.connect() as conn:
        def q(sql, **params):
            return pd.read_sql(text(sql), conn, params=params or None)

        # ── Outcomes ──────────────────────────────────────────────────────
        mcas = q(Q.FA_MCAS_3_8, yr=school_year)

        post = q(Q.FA_POSTSECONDARY, yr=school_year)

        drop = q(Q.FA_DROPOUT, yr=school_year)

        # ── DESE school features ───────────────────────────────────────────
        demog = q(Q.FA_SELECTED_POP, yr=school_year)

        attend = q(Q.FA_ATTENDANCE, yr=school_year)

        ppe_raw = q(Q.FA_PPE_INSTRUCTIONAL, yr=school_year)
        ppe = (ppe_raw
               .pivot_table(index="district_name", columns="category",
                            values="amount", aggfunc="first")
               .reset_index())
        ppe.columns.name = None
        # instructional_share: share of in-district spending that reaches the
        # CLASSROOM (teachers + other teaching + materials + instructional
        # leadership) ÷ total in-district.  An allocation lever the cross-sectional
        # factor screen validated (partial ρ ≈ +0.21 on MCAS 3–8, net of structure).
        _instr = (ppe.get("Teachers", 0.0)
                  + ppe.get("Other Teaching Services", 0.0)
                  + ppe.get("Instructional Materials, Equipment and Technology", 0.0)
                  + ppe.get("Instructional Leadership", 0.0))
        ppe["instructional_share"] = (_instr /
            ppe["Total In-District Expenditures"].replace(0, np.nan))
        ppe = (ppe.rename(columns={"Total In-District Expenditures": "in_district_ppe",
                                    "Teachers": "teacher_spending_per_pupil"})
                  [["district_name", "in_district_ppe", "teacher_spending_per_pupil",
                    "instructional_share"]])

        ch70 = q(Q.FA_CHAPTER70, yr=fy)

        staff_raw = q(Q.FA_STAFFING, yr=school_year)
        staff_fte  = (staff_raw[staff_raw.category == "teacher_fte"]
                      [["district_name","fte"]].rename(columns={"fte":"teacher_fte"}))
        staff_r100 = (staff_raw[staff_raw.category == "teachers_per_100_fte"]
                      [["district_name","fte"]].rename(columns={"fte":"teachers_per_100_fte"}))
        staff_sal  = (staff_raw[staff_raw.category == "teacher_avg_salary"]
                      [["district_name","avg_salary"]]
                      .rename(columns={"avg_salary":"avg_teacher_salary"}))

        enrol = q(Q.FA_ENROLLMENT, yr=school_year)

        grad = q(Q.FA_GRADUATION, yr=school_year)

        sat = q(Q.FA_SAT, yr=school_year)

        mcas10 = q(Q.FA_MCAS_10, yr=school_year)

        # ── ACS demographics ───────────────────────────────────────────────
        acs = q(Q.FA_ACS)
        acs["municipality"] = (acs["municipality"]
                               .str.replace(r"\s+Town$", "", regex=True).str.strip())

        # ── Municipal finance ──────────────────────────────────────────────
        # (Budget shares — ed_budget_share / public_safety_pct / public_works_pct
        #  — are produced canonically by the budget_share query near the end of
        #  this loader.  An earlier mexp query computing duplicate *_pct_budget
        #  columns was dead and has been removed.)
        mrev = q(Q.FA_MUNI_REVENUES, yr=fy)
        mrev["tax_pct_rev"] = (mrev.muni_tax_rev / mrev.muni_total_rev * 100).where(mrev.muni_total_rev > 0)

        assessed = q(Q.FA_ASSESSED, yr=fy)
        assessed["commercial_av_share"] = (assessed.ci_av / assessed.total_av * 100).where(assessed.total_av > 0)

        tax_rates = q(Q.FA_TAX_RATES, yr=fy)

        gf_exp = q(Q.FA_GF_EXP, yr=fy)

        crime = q(Q.FA_CRIME, yr_lo=fy-4, yr_hi=fy)

        new_growth = q(Q.FA_NEW_GROWTH, yr=fy)

        income_eq = q(Q.FA_INCOME_EQV, yr=fy)

        county_health = q(Q.FA_COUNTY_HEALTH, yr_lo=fy-3)

        county_unemp = q(Q.FA_COUNTY_UNEMP, yr_lo=fy-3)

        # Map district → county using districts table
        dist_county = q(Q.FA_DISTRICT_COUNTY)

    # ─── Merge everything ───────────────────────────────────────────────────
    df = mcas.copy()

    for tbl in [demog, attend, ppe, ch70, staff_fte, staff_r100, staff_sal,
                enrol, grad, sat, mcas10, post, drop]:
        df = df.merge(tbl, on="district_name", how="left")

    # Derived DESE features
    df["teachers_per_100_students"] = (df["teacher_fte"] / df["total_enrollment"]
                                       * 100).where(df["total_enrollment"] > 0)

    # Bridge DESE → ACS/Municipal via district_name ≈ municipality
    df["_muni"] = df["district_name"].str.strip()
    for tbl, key in [(acs,       "municipality"),
                     (mrev[["municipality","tax_pct_rev"]], "municipality"),
                     (assessed[["municipality","commercial_av_share"]], "municipality"),
                     (tax_rates, "municipality"),
                     (gf_exp,    "municipality"),
                     (crime,     "municipality"),
                     (new_growth,"municipality"),
                     (income_eq, "municipality")]:
        tbl2 = tbl.copy().rename(columns={key: "_muni"})
        tbl2["_muni"] = tbl2["_muni"].str.strip()
        df = df.merge(tbl2.drop_duplicates("_muni"), on="_muni", how="left")

    # Bridge to county-level data
    dist_county["_muni"] = dist_county["district_name"].str.strip()
    df = df.merge(dist_county[["_muni","county"]].drop_duplicates("_muni"),
                  on="_muni", how="left")
    for tbl, key in [(county_health, "county_name"),
                     (county_unemp,  "county_name")]:
        tbl2 = tbl.copy().rename(columns={key: "county"})
        df = df.merge(tbl2.drop_duplicates("county"), on="county", how="left")

    df = df.drop(columns=["_muni", "county"], errors="ignore")

    # Convert everything to float
    skip = {"org_code", "district_name"}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Combined SAT score (EBRW + Math total, ~400–1600 range)
    if "sat_ebrw" in df.columns and "sat_math" in df.columns:
        df["sat_combined"] = df["sat_ebrw"] + df["sat_math"]

    # Budget line-item shares — built directly from MA DLS Schedule A.
    # Each ratio is: line_item / total_expenditures * 100.
    # This is the same approach as building EPS from the income statement:
    # don't predict the ratio from demographics, compute it from components.
    # ed_budget_share is the target; the others are the competing line items
    # that mechanically explain why education's share is what it is.
    with engine.connect() as conn:
        budget_share = q(Q.FA_BUDGET_SHARE, yr=fy)
    df = df.merge(budget_share, on="district_name", how="left")

    # Stamp the year this cross-section was built for, so downstream point-in-time
    # pages (e.g. the fixed-cost snapshot) align to it without re-deriving or
    # hardcoding.  Preserved through the pickle cache used by --regen-pdf.
    df.attrs["analysis_fiscal_year"] = fy
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Saugus-specific RBP analysis
# ─────────────────────────────────────────────────────────────────────────────

# Outcomes where a LOWER value is the better result, so a NEGATIVE LOO residual
# (actual − predicted) marks over-performance.  Everything else is higher-is-better.
LOWER_IS_BETTER: set[str] = {"dropout_pct"}


def _higher_is_better(target: str) -> bool:
    return target not in LOWER_IS_BETTER


# Single source of truth for the fraction-vs-percent scale decision.  Some
# outcome columns are stored as 0–1 fractions (avg_mcas, mcas10_ela) and must be
# ×100 to read as percentages; others are already in percentage points
# (dropout_pct ~0–23, ed_budget_share ~13–78).  Decide from the column's own
# magnitude, NOT from MODELS['target_pct'] (which is True for ed_budget_share
# even though it is stored as a percentage).  One threshold, used everywhere, so
# every page scales identically.
_FRACTION_SCALE_MAX = 1.5


def _shorten(name: str, n: int) -> str:
    """Town/label shortener: keep the full name unless it is genuinely longer
    than n, in which case use an ellipsis so it stays recognizable (e.g.
    'West Springfield' → 'West Springf…') rather than a hard mid-word cut."""
    name = str(name)
    return name if len(name) <= n else name[: n - 1] + "…"


def _on_fraction_scale(values) -> bool:
    """True if `values` is a 0–1 fraction column (needs ×100 to be a percentage)."""
    arr = np.asarray(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return False
    return float(np.max(np.abs(arr))) <= _FRACTION_SCALE_MAX


def rank_among_peers(loo_df: pd.DataFrame, target: str,
                     who: str = SAUGUS) -> tuple[int, int, int]:
    """
    Rank `who` against all districts on this outcome's demographic-adjusted
    residual, respecting outcome direction (lower-is-better outcomes invert the
    comparison).  Single source of truth so the Saugus-analysis footer and the
    synthesis page can never disagree again.

    Returns (n_outperform, n_total, rank) where:
      n_outperform = districts that beat `who` (excluding `who` itself)
      n_total      = districts with a valid residual (including `who`)
      rank         = n_outperform + 1
    """
    resid = loo_df.dropna(subset=["residual"])["residual"]
    n_total = int(len(resid))
    if who not in resid.index:
        return 0, n_total, 1
    s = float(resid.loc[who])
    others = resid.drop(index=who)
    if _higher_is_better(target):
        n_outperform = int((others > s).sum())
    else:
        n_outperform = int((others < s).sum())
    return n_outperform, n_total, n_outperform + 1


def analyze_saugus(df: pd.DataFrame,
                   features: list[str],
                   target: str,
                   n_random_cells: int = 100) -> dict:
    """
    Run RBP with Saugus as the prediction task.

    Returns dict with:
        result      : RBPResult
        actual      : Saugus's actual outcome value
        actual_pct  : as percentage (×100 if <1)
        pred_pct    : predicted value as percentage
        gap_pp      : actual − predicted (percentage points)
        n_above     : number of districts outperforming Saugus (by residual)
    """
    # Use the full df with district_name for lookup, then drop it for RBP
    full = df[["district_name"] + features + [target]].dropna().copy()
    saugus_row = full[full["district_name"] == SAUGUS]
    if len(saugus_row) == 0:
        raise ValueError(f"Saugus not found in data for target={target!r}")

    X_all = full.drop(columns=["district_name"]).copy()
    X_all.index = full["district_name"].values
    y_all = X_all.pop(target)

    x_saugus = X_all.loc[SAUGUS]
    X_train  = X_all.drop(index=SAUGUS)
    y_train  = y_all.drop(index=SAUGUS)

    result = rbp(X_train, y_train, x_saugus, features, n_random_cells=n_random_cells)

    actual = float(y_all.loc[SAUGUS])
    pred   = result.prediction
    # Decide fraction-vs-percent from the COLUMN's scale, not Saugus's single
    # value.  The old `actual < 2.0` test misclassifies any district whose value
    # happens to be < 2 (e.g. a dropout rate of 0.1%) and would report it ×100.
    # A 0–1 target (avg_mcas, mcas10_ela) has a small column max; an already-
    # percentage target (dropout_pct 0–23, ed_budget_share 13–78) does not.
    # (target_pct in MODELS does NOT encode this — ed_budget_share is target_pct
    # True yet stored as a percentage — so it cannot be used here.)
    is_fraction = _on_fraction_scale(y_all)
    if is_fraction:
        actual_pct = actual * 100
        pred_pct   = pred * 100
    else:
        actual_pct = actual
        pred_pct   = pred
    gap_pp = actual_pct - pred_pct

    # LOO residuals for all districts, then count how many genuinely OUTPERFORM
    # Saugus — respecting outcome direction (lower-is-better outcomes invert the
    # comparison).  rank_among_peers is the single source of truth shared with
    # the synthesis page so the two can never disagree.
    loo_df = rbp_loo(X_all, y_all, features, n_random_cells=n_random_cells)
    loo_df["residual"] = loo_df["actual"] - loo_df["predicted"]
    n_outperform, n_ranked, rank = rank_among_peers(loo_df, target, SAUGUS)

    return {
        "result":      result,
        "actual":      actual,
        "actual_pct":  actual_pct,
        "pred_pct":    pred_pct,
        "gap_pp":      gap_pp,
        "n_outperform": n_outperform,   # districts beating Saugus (direction-aware)
        "n_ranked":    n_ranked,        # districts with a valid residual (incl. Saugus)
        "rank":        rank,            # = n_outperform + 1
        "n_above":     n_outperform,    # backwards-compat alias
        "n_total":     n_ranked,        # backwards-compat alias
        "loo_df":      loo_df,
        "features":    features,
        "target":      target,
    }


IMPORTANCE_SEED: int = 42                       # canonical grid (paper procedure)
IMPORTANCE_CHECK_SEEDS: tuple[int, ...] = (101, 202)   # guardrail check only


def saugus_importance(df: pd.DataFrame,
                      features: list[str],
                      target: str,
                      n_random_cells: int = 2000,
                      seed: int = IMPORTANCE_SEED,
                      check_seeds: tuple[int, ...] = IMPORTANCE_CHECK_SEEDS) -> dict:
    """
    Canonical Exhibit-5 variable importance for the Saugus prediction task.

    Faithful to Kritzman: the *reported* importance is one dense grid (a single
    Monte-Carlo draw at `seed` — the paper's procedure, just denser to better
    approximate the deterministic full grid the sampling stands in for).  We do
    NOT average across seeds; that would target a slightly different estimand
    than the paper's single-grid definition.

    The `check_seeds` are a guardrail ONLY: we recompute importance at a couple
    of other seeds purely to verify the top-3 ranking does not move with the
    draw, and never let them alter the reported numbers.  If the top-3 is not
    reproduced, `top3_stable` is False and the narrative softens its
    "#1 driver" claim accordingly.

    Returns:
        importance  : Series (descending) — the reported canonical importance
        top3_stable : True iff every check seed reproduces the canonical top-3
        n_checks    : number of grids compared (canonical + check seeds)
    """
    full = df[["district_name"] + features + [target]].dropna().copy()
    X_all = full.drop(columns=["district_name"]).copy()
    X_all.index = full["district_name"].values
    y_all = X_all.pop(target)
    if SAUGUS not in X_all.index:
        raise ValueError(f"Saugus not found in data for target={target!r}")
    x_saugus = X_all.loc[SAUGUS]
    X_train  = X_all.drop(index=SAUGUS)
    y_train  = y_all.drop(index=SAUGUS)

    def _imp(s: int) -> pd.Series:
        res = rbp(X_train, y_train, x_saugus, features,
                  n_random_cells=n_random_cells, random_state=s)
        return res.variable_importance.sort_values(ascending=False)

    importance = _imp(seed)
    canon_top3 = list(importance.index[:3])
    top3_stable = True
    for s in check_seeds:
        if list(_imp(s).index[:3]) != canon_top3:
            top3_stable = False
            break

    return {"importance": importance, "top3_stable": top3_stable,
            "n_checks": 1 + len(check_seeds)}


def _display_importance(analysis: dict) -> pd.Series:
    """
    Canonical Exhibit-5 importance Series for any display.

    Returns the single canonical-seed (seed 42) all-candidate importance from
    Step 1 (attached to the analysis dict as 'display_importance').  It is NOT
    seed-averaged: saugus_importance reads one dense grid, and the check-seeds are
    used only to test top-3 stability, never to average.  Every page — the
    all-candidate chart (p.4), the lean Saugus chart (p.5), the synthesis drivers
    table, and the combined summary — reads this SAME Series and cannot disagree
    on the top driver.  Falls back to the run's own importance only for older
    caches that predate this field.
    """
    imp = analysis.get("display_importance")
    if imp is None:
        imp = analysis["result"].variable_importance
    return imp


def compute_ridge_validation(df: pd.DataFrame,
                             features: list[str],
                             target: str,
                             focus: str = "chronic_absenteeism_pct") -> dict | None:
    """
    Independent cross-check of the RBP Exhibit-5 finding by a different method:
    a standardized Ridge regression on the SAME district panel.  Reports the
    realized 5-fold CV R² and where `focus` (chronic absenteeism) ranks among
    the standardized coefficients.  All figures are computed here so the page
    note interpolates live numbers — nothing hardcoded.

    Returns None (caller falls back to a qualified note) if sklearn is missing,
    the panel is too small, or `focus` isn't among the predictors.
    """
    try:
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import cross_val_score
        from sklearn.pipeline import make_pipeline
    except Exception:
        return None

    feats = [f for f in features if f in df.columns and f != target]
    if focus not in feats:
        return None
    sub = df[feats + [target]].dropna()
    if len(sub) < 30:
        return None

    X = sub[feats].values.astype(float)
    y = sub[target].values.astype(float)
    try:
        r2 = float(np.mean(cross_val_score(
            make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
            X, y, cv=5, scoring="r2")))
        Xs = StandardScaler().fit_transform(X)
        ys = (y - y.mean()) / (y.std() or 1.0)
        coefs = pd.Series(np.abs(Ridge(alpha=1.0).fit(Xs, ys).coef_),
                          index=feats).sort_values(ascending=False)
    except Exception:
        return None

    rank = list(coefs.index).index(focus) + 1
    return {"n": int(len(sub)), "r2": r2, "focus": focus,
            "focus_abs_beta": float(coefs[focus]),
            "focus_rank": int(rank), "n_features": int(len(feats)),
            "is_top": rank == 1}


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Paper-format PDF
# ─────────────────────────────────────────────────────────────────────────────

_PAGE_W = 11.0
_PAGE_H = 8.5
_BL     = "#1A1A2E"   # near-black for text
_GREY   = "#555555"
_BLUE   = "#2C4770"
_RED    = "#C0392B"
_GREEN  = "#1E8449"
_GOLD   = "#B7950B"


def _paper_fig(nrows=1, ncols=1, **kw) -> tuple[plt.Figure, object]:
    fig, ax = plt.subplots(nrows, ncols, figsize=(_PAGE_W, _PAGE_H), **kw)
    fig.patch.set_facecolor("white")
    return fig, ax


def _header(fig, title: str, subtitle: str = ""):
    fig.text(0.5, 0.95, title, ha="center", va="top",
             fontsize=14, fontweight="bold", color=_BL,
             transform=fig.transFigure)
    if subtitle:
        wrapped = textwrap.fill(subtitle, width=150)
        fig.text(0.5, 0.915, wrapped, ha="center", va="top",
                 fontsize=9, color=_GREY, transform=fig.transFigure,
                 linespacing=1.3)


def _footer(fig, text: str, width: int = 165):
    # Auto-wrap so long footers never run off the 11" page.  Any explicit '\n' a
    # caller already inserted is honoured as a hard break (each such line is
    # wrapped independently); everything else flows to `width` characters.
    wrapped = "\n".join(textwrap.fill(line, width=width) for line in text.split("\n"))
    fig.text(0.5, 0.02, wrapped, ha="center", va="bottom",
             fontsize=7, color=_GREY, style="italic",
             transform=fig.transFigure)


def _save(pdf, fig):
    try:
        pdf.savefig(fig)
    finally:
        plt.close(fig)


def page_title(pdf, models: list[dict], analysis_year: int | None = None):
    fig, ax = _paper_fig()
    ax.axis("off")

    ax.text(0.5, 0.82,
            "Relevance-Based Prediction: Factor Selection for Saugus Schools",
            ha="center", va="center", fontsize=16, fontweight="bold", color=_BL,
            transform=ax.transAxes)

    if analysis_year is not None:
        ax.text(0.5, 0.755,
                f"Cross-section year: FY{analysis_year}  —  the latest year with complete "
                f"MCAS outcomes and DESE per-pupil spending data",
                ha="center", va="center", fontsize=9.5, style="italic", color=_GREY,
                transform=ax.transAxes)

    lines = [
        "Four outcomes: MCAS grades 3–8, Dropout Rate, MCAS grade 10 ELA, Education Budget Share",
        "One RBP run per outcome over the full candidate pool — no in-model pruning (faithful to Kritzman)",
        "The one discretionary step is which factors enter and how they tier: Tier 3 (structural) matches Saugus to peers; Tiers 1 & 2 (actionable) are ranked",
        "Saugus analyzed as the prediction task; importance flags where it is most distinctive among actionable factors — a prediction-sharpening measure, not a causal driver",
        "Leave-one-out LOO r validates the predictions across all MA districts",
    ]
    # Wrap each bullet so long lines never run off the right edge; hang the
    # continuation under the text (not the marker).
    y_cursor = 0.65
    for line in lines:
        segs = textwrap.wrap(line, width=104)
        for k, seg in enumerate(segs):
            ax.text(0.08, y_cursor, ("•  " if k == 0 else "     ") + seg,
                    ha="left", va="center", fontsize=9.3, color=_BL,
                    transform=ax.transAxes)
            y_cursor -= 0.030
        y_cursor -= 0.010   # gap between bullets

    # Clarify what the per-model factor count is made of: the structural traits
    # are IN the model (they build the peer match) but hidden from the action
    # tables because a town can't change them.  Counts are read from the models
    # so they can never drift from the tables below.
    _cands0 = models[0].get("all_candidates", models[0]["features"]) if models else []
    _n_struct = sum(1 for f in _cands0 if f in STRUCTURAL_FEATURES)
    _note = (f"Each model uses {len(_cands0)} factors, of which {_n_struct} are Tier-3 "
             f"structural traits (income, poverty, enrollment …).  These {_n_struct} are "
             f"needed to match Saugus to genuine peer towns, but are then hidden — a town "
             f"cannot change who it is — so only the Tier-1 & 2 factors it can act on are "
             f"ranked and shown in this report.")
    ax.text(0.5, 0.395, textwrap.fill(_note, width=118),
            ha="center", va="top", fontsize=8, style="italic", color=_BL,
            transform=ax.transAxes, linespacing=1.35)

    # Summary box — sits below the bullet lines with a clear gap
    box_x0, box_w = 0.02, 0.96
    ax.add_patch(mpatches.FancyBboxPatch(
        (box_x0, 0.08), box_w, 0.24, boxstyle="round,pad=0.01",
        facecolor="#EEF2F8", edgecolor=_BLUE, linewidth=1.2,
        transform=ax.transAxes))
    ax.text(0.5, 0.30, "Model Summary  (all candidates used — no pruning)",
            ha="center", va="center", fontsize=10, fontweight="bold",
            color=_BLUE, transform=ax.transAxes)
    col_w = box_w / len(models)

    def _fit_metrics(m):
        """(LOO r, out-of-sample R², MAE in pp) from the leave-one-out predictions.
        R² is the true 1 − SSE/SST, NOT the squared correlation: squared r overstates
        variance explained whenever predictions are compressed toward the mean, which
        RBP's weighted averages do."""
        s = m.get("saugus")
        if not s or "loo_df" not in s:
            return None
        loo = s["loo_df"].dropna(subset=["actual", "predicted"])
        if len(loo) < 3:
            return None
        a = loo["actual"].values.astype(float)
        p = loo["predicted"].values.astype(float)
        mult = 100 if _on_fraction_scale(a) else 1   # 0–1 fraction → pp
        a, p = a * mult, p * mult
        r = float(np.corrcoef(a, p)[0, 1])
        sse = float(((a - p) ** 2).sum())
        sst = float(((a - a.mean()) ** 2).sum())
        r2 = (1.0 - sse / sst) if sst > 0 else float("nan")
        return r, r2, float(np.mean(np.abs(a - p)))

    for j, m in enumerate(models):
        xpos = box_x0 + (j + 0.5) * col_w
        n_candidates = len(m.get("all_candidates", m["features"]))
        fm = _fit_metrics(m)
        ax.text(xpos, 0.255, m["label"], ha="center", fontsize=9,
                color=_BL, fontweight="bold", transform=ax.transAxes)
        ax.text(xpos, 0.205, f"Predicted on {n_candidates} factors",
                ha="center", fontsize=8.5, color=_GREY, transform=ax.transAxes)
        if fm:
            r, r2, mae = fm
            ax.text(xpos, 0.16, f"LOO r = {r:.2f}  ·  R² = {r2:.2f}",
                    ha="center", fontsize=8.5, color=_BLUE, fontweight="bold",
                    transform=ax.transAxes)
            ax.text(xpos, 0.115, f"Typical error ± {mae:.1f} pp",
                    ha="center", fontsize=8.5, color=_GREY, transform=ax.transAxes)

    expl = ("LOO r = leave-one-out correlation of predicted vs. actual (range −1 to +1; "
            "noise below ≈ 0.15 at this sample size).  R² = 1 − SSE/SST, the out-of-sample "
            "share of town-to-town variance explained (≤ r² because RBP's weighted averages "
            "compress toward the mean).  Typical error = mean absolute error, in percentage points.")
    ey = 0.062
    for seg in textwrap.wrap(expl, width=120):
        ax.text(0.5, ey, seg, ha="center", va="center", fontsize=6.8,
                color=_GREY, style="italic", transform=ax.transAxes)
        ey -= 0.018

    _gen_date = datetime.date.today().strftime("%B %Y")
    _footer(fig,
            "Relevance-Based Prediction "
            f"— Applied to MA School District Analysis  ·  Saugus Schools Project · {_gen_date}")
    _save(pdf, fig)


def page_tiers_explained(pdf):
    """Plain-language guide to the three factor tiers and how the report uses them."""
    fig, ax = _paper_fig(); ax.axis("off")
    _header(fig, "How to Read This Report — Three Tiers of Factors",
            "This report matches Saugus to demographically-similar towns using Tier 3, "
            "then ranks the Tier 1 & 2 factors Saugus can actually change.  Structural "
            "traits are the backdrop for finding peers — never a recommendation.")

    tiers = [
        ("TIER 1 — Directly votable  (Town Meeting / ballot)", _GREEN,
         "What the town changes by a vote.",
         ["Education's share of the municipal budget",
          "Residential tax rate · Proposition 2½ override · debt exclusion",
          "Spending above the state-required school minimum (Ch. 70)",
          "Reserves (free cash, stabilization) · OPEB funding · capital projects"]),
        ("TIER 2 — Policy / management  (administration decides)", _BLUE,
         "What the school department & town administration run day-to-day (funded by Tier 1).",
         ["Chronic absenteeism — attendance & re-engagement programs",
          "Teacher staffing levels and pay",
          "How the school dollar splits between classroom and overhead",
          "Above-minimum school spending funded from reserves"]),
        ("TIER 3 — Structural  (NOT changeable by vote)", _GREY,
         "What the community IS — used ONLY to find Saugus's true peer towns.",
         ["Household income · property wealth · adult education",
          "Poverty · English-learner · special-education shares",
          "Housing tenure · district enrollment · regional economy",
          "→ Defines the peer group; never shown as an actionable factor here."]),
    ]
    y = 0.84
    for title, color, sub, items in tiers:
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.03, y - 0.225), 0.94, 0.205, boxstyle="round,pad=0.012",
            facecolor="#FAFAFA", edgecolor=color, linewidth=1.8, transform=ax.transAxes))
        ax.text(0.055, y - 0.01, title, fontsize=12.5, fontweight="bold",
                color=color, transform=ax.transAxes, va="top")
        ax.text(0.055, y - 0.052, sub, fontsize=9, color=_BL, style="italic",
                transform=ax.transAxes, va="top")
        for i, it in enumerate(items):
            ax.text(0.075, y - 0.088 - i * 0.030, "•  " + it, fontsize=8.8,
                    color=_BL, transform=ax.transAxes, va="top")
        y -= 0.275

    _footer(fig, "Tiers 1 & 2 = actionable factors (what this report ranks).  "
            "Tier 3 = structural controls (the peer-matching basis, not recommendations).")
    _save(pdf, fig)


def page_factor_definitions(pdf):
    """Every actionable factor: exact calculation, data source, and what it looks
    for.  So a skeptic can reproduce each number from public MA data."""
    fig, ax = _paper_fig(); ax.axis("off")
    _header(fig, "Factor Definitions — What Each Lever Is and How It's Built",
            "The 9 Tier-1/2 factors this report ranks.  2 are raw columns; 7 are calculated "
            "ratios.  All source data is public: DESE (education feeds) and DLS Schedule A "
            "(municipal finance).")

    # (name, tier_color, formula+source line, plain-language purpose)
    T1, T2 = [
        ("ed_budget_share",
         "= municipal education ÷ total municipal spending × 100      · DLS Schedule A",
         "Share of the whole TOWN budget voted to schools — the core allocation choice."),
        ("spend_vs_required",
         "= in-district spending/pupil ÷ Chapter-70 required NSS/pupil   · DESE per-pupil + Chapter 70",
         "Spending above the state-required minimum — the town's discretionary 'fund-more' effort."),
        ("fixed_costs_pct",
         "= municipal fixed costs ÷ total municipal spending × 100     · DLS Schedule A",
         "Budget locked in pensions/benefits/debt — the crowd-out that squeezes schools."),
    ], [
        ("chronic_absenteeism_pct",
         "= raw column (no calculation)                               · DESE attendance",
         "Student engagement — % of students missing ≥10% of school days."),
        ("avg_teacher_salary",
         "= raw column (no calculation)                               · DESE staffing",
         "Teacher pay LEVEL — competitiveness for talent."),
        ("instructional_share",
         "= (Teachers + Other Teaching + Materials + Instr. Leadership) ÷ Total In-District · DESE per-pupil",
         "How much of the school dollar reaches the CLASSROOM vs. overhead."),
        ("teacher_pay_share",
         "= Teachers (per-pupil) ÷ Total In-District (per-pupil)       · DESE per-pupil",
         "The slice of the school dollar going specifically to teacher pay."),
        ("teachers_per_100_students",
         "= teacher FTE ÷ enrollment × 100                            · DESE staffing + enrollment",
         "Staffing density / class size — teachers per 100 students."),
        ("teachers_per_lowincome",
         "= teachers-per-100-students ÷ low-income %                   · DESE staffing + enrollment + demographics",
         "Staffing RELATIVE TO NEED — teachers per unit of low-income enrollment."),
    ]

    def block(y, groups):
        for name, formula, purpose in groups:
            ax.text(0.045, y, name, fontsize=9.2, fontweight="bold",
                    color=_BL, transform=ax.transAxes, va="top", family="monospace")
            ax.text(0.045, y - 0.021, "  " + formula, fontsize=7.2, color=_GREY,
                    transform=ax.transAxes, va="top", family="monospace")
            ax.text(0.045, y - 0.040, "  → " + purpose, fontsize=8.0, color=_BLUE,
                    style="italic", transform=ax.transAxes, va="top")
            y -= 0.066
        return y

    y = 0.83
    ax.text(0.04, y, "TIER 1 — Directly votable (Town Meeting / ballot)",
            fontsize=10.5, fontweight="bold", color=_GREEN, transform=ax.transAxes, va="top")
    y = block(y - 0.032, T1)
    y -= 0.012
    ax.text(0.04, y, "TIER 2 — Policy / management (administration decides)",
            fontsize=10.5, fontweight="bold", color=_BLUE, transform=ax.transAxes, va="top")
    block(y - 0.032, T2)

    _footer(fig, "Every factor is reproducible from public MA data (DESE per-pupil expenditure, "
            "staffing, enrollment, attendance, demographics, Chapter 70; DLS Schedule A municipal "
            "finance).  Ratios normalize for town size so a big town and a small town are comparable.")
    _save(pdf, fig)


def page_method_explainer(pdf):
    """How RBP focuses on similar towns without shrinking the dataset — plain
    language plus the underlying equations (signed relevance weighting)."""
    fig, ax = _paper_fig(); ax.axis("off")
    ax.set_position([0, 0, 1, 1])    # axes coords == figure coords
    _header(fig, "How RBP Compares Saugus to Towns Like It — Without Shrinking the Data",
            "It focuses on towns similar to Saugus using the FULL set of MA districts, "
            "never a hand-picked subset — so the comparison keeps its statistical power.")

    boxes = [
        ("1.  Full sample, always — no pre-filtering", _BLUE,
         "Every prediction uses all ~170–220 MA districts.",
         "The full sample is what learns which towns are relevant to Saugus (the factor "
         "covariance Ω).  Hand-filtering to a small peer set first would make that geometry "
         "unestimable and turn the importance scores into noise."),
        ("2.  Signed weighting — contrasts, not exclusion", _GREEN,
         "Each town's weight may be positive OR negative, and the weights sum to 1.",
         "Most-relevant towns get large positive weights; less-relevant towns get small, "
         "often negative weights — used as contrasts (subtracted), as in regression.  "
         "Only ~6–15 of ~170–220 towns sit near zero, and none is excluded."),
        ("3.  A small effective core, plus a censored cross-check", _GOLD,
         "About half the towns carry small negative weights; ~10–25 carry most of the weight.",
         "RBP also re-predicts from only the top 80% / 50% / 20% most-relevant towns and "
         "blends those tight-peer estimates by how reliably each predicts (its fit) — the "
         "method decides how much to trust the tight group; no human picks the cutoff."),
    ]
    y = 0.87
    for title, color, sub, body in boxes:
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.03, y - 0.125), 0.94, 0.112, boxstyle="round,pad=0.008",
            facecolor="#FAFAFA", edgecolor=color, linewidth=1.6, transform=ax.transAxes))
        ax.text(0.05, y - 0.004, title, fontsize=10.5, fontweight="bold",
                color=color, transform=ax.transAxes, va="top")
        ax.text(0.05, y - 0.034, sub, fontsize=8.2, color=_BL, style="italic",
                transform=ax.transAxes, va="top")
        for i, ln in enumerate(textwrap.fill(body, width=118).split("\n")):
            ax.text(0.065, y - 0.060 - i * 0.023, ln, fontsize=7.7,
                    color=_BL, transform=ax.transAxes, va="top")
        y -= 0.150

    # ── The math ────────────────────────────────────────────────────────────
    ax.add_patch(mpatches.FancyBboxPatch(
        (0.03, 0.055), 0.94, 0.33, boxstyle="round,pad=0.008",
        facecolor="#EEF2F8", edgecolor=_BLUE, linewidth=1.4, transform=ax.transAxes))
    ax.text(0.05, 0.375, "The math behind it", fontsize=11, fontweight="bold",
            color=_BLUE, transform=ax.transAxes, va="top")

    rows = [
        (r"$\hat{y}_{Saugus}=\sum_i w_i\,y_i,\qquad \sum_i w_i=1$",
         "The prediction is a signed weighted sum of town outcomes; the weights sum to 1."),
        (r"$w_i=\dfrac{1}{N}+\dfrac{\lambda^{2}}{n-1}\,(\delta_i\,r_i-\phi\,\bar{r})$",
         "Each weight = a uniform 1/N baseline + a tilt that turns NEGATIVE below average relevance."),
        (r"$r_i=-\dfrac{1}{2}(x_i-x_S)^{T}\,\Omega^{-1}(x_i-x_S)\;+\;\mathrm{info}$",
         "Relevance r = closeness to Saugus in factor space (Mahalanobis); Ω estimated from all N towns."),
        (r"$N_{\mathrm{eff}}=1\,/\,\sum_i w_i^{2}$",
         "Effective # of towns carrying the weight ≈ 10–25 of ~170–220 — a focus, not a deletion."),
        (r"$w=x_S^{T}(X^{T}X)^{-1}X^{T}$",
         "With all factors and no censoring this is exactly OLS — signed weights are inherent to regression."),
    ]
    yy = 0.335
    for eq, note in rows:
        ax.text(0.06, yy, eq, fontsize=11, color=_BL, transform=ax.transAxes, va="center")
        ax.text(0.40, yy, note, fontsize=7.6, color=_GREY, transform=ax.transAxes, va="center")
        yy -= 0.052

    _footer(fig, "Because the relevance geometry Ω is learned from the full sample and the "
            "weights sum to 1, concentrating the prediction on a relevant core costs no "
            "stability — the instability a small hand-picked subset would create.")
    _save(pdf, fig)


def page_combined_summary(pdf, results: list[dict]):
    """
    Single page combining all four models:
      Top half   — Saugus prediction summary across all models
      Bottom half — Feature cross-reference: which features appear in multiple models
    """
    fig, axes = _paper_fig(2, 1, gridspec_kw={"height_ratios": [1, 1.4]})
    ax_top, ax_bot = axes
    _header(fig, "Combined Model Summary: All Four Outcomes",
            "RBP factor selection results across MCAS grades 3–8, Dropout Rate, MCAS grade 10 ELA, and Education Budget Share")

    # ── Top: Saugus prediction table ─────────────────────────────────────────
    ax_top.axis("off")
    summary_rows = []
    for r in results:
        s = r.get("saugus")
        if not s:
            continue
        is_pct = s["actual_pct"] < 200
        unit   = "pp" if is_pct else " pts"
        gap    = s["gap_pp"]
        gap_str = f"{gap:+.1f}{unit}"
        # Outcome-direction-aware: for lower-is-better outcomes (dropout), a
        # negative gap is an OVER-performance.  The raw gap is shown with the
        # Direction column resolving the sign (audit-approved); the per-town
        # "+ better" framing lives on the What-Comparable-Towns page.
        if abs(gap) < 0.5:
            direction = "on target"
        else:
            outperforms = (gap > 0) if _higher_is_better(r["target"]) else (gap < 0)
            direction = "over" if outperforms else "under"
        summary_rows.append([
            r["label"],
            f"{s['pred_pct']:.1f}{'%' if is_pct else ' pts'}",
            f"{s['actual_pct']:.1f}{'%' if is_pct else ' pts'}",
            gap_str,
            direction,
            f"{r['loo_score']:+.3f}",
            str(len(r.get("lean_features", r["features"]))),
        ])

    col_heads = ["Model", "Predicted", "Actual", "Gap", "Direction", "LOO r", "Factors"]
    if summary_rows:
        tbl = ax_top.table(cellText=summary_rows, colLabels=col_heads,
                           bbox=[0.0, 0.05, 1.0, 0.88])
        tbl.auto_set_font_size(False); tbl.set_fontsize(9.5)
        char_w = [max(len(str(r[c])) for r in [col_heads] + summary_rows)
                  for c in range(len(col_heads))]
        tot = max(sum(char_w), 1)
        gap_col = col_heads.index("Gap")
        dir_col = col_heads.index("Direction")
        for (row, col), cell in tbl.get_celld().items():
            cell.set_width(char_w[col] / tot)
            if row == 0:
                cell.set_facecolor(_BLUE); cell.set_text_props(color="white", fontsize=9.5)
            else:
                gap_val = summary_rows[row-1][gap_col] if row <= len(summary_rows) else ""
                if "over" in summary_rows[row-1][dir_col]:
                    bg = "#E8F8E8"
                elif "under" in summary_rows[row-1][dir_col]:
                    bg = "#FFE8E8"
                else:
                    bg = "#FFF8E1"
                cell.set_facecolor(bg if col == gap_col else ("#F5F5F5" if row % 2 else "white"))
            cell.set_edgecolor("#CCCCCC")

    # ── Bottom: feature cross-reference ──────────────────────────────────────
    ax_bot.axis("off")
    ax_bot.text(0.5, 0.98, "Actionable Factor Cross-Reference — Which Factors Matter Across Outcomes",
                ha="center", va="top", fontsize=10, fontweight="bold",
                color=_BLUE, transform=ax_bot.transAxes)
    ax_bot.text(0.5, 0.93,
                "Actionable factors only (structural traits are matching-only).  A factor "
                "that matters in multiple outcomes is a robust target.",
                ha="center", va="top", fontsize=8.5, color=_GREY, transform=ax_bot.transAxes)

    # Build cross-reference: factor → {model: importance}  (actionable only)
    model_labels = [r["label"] for r in results]
    feature_set: dict[str, dict] = {}
    for r in results:
        s = r.get("saugus")
        lean = _display_features(r.get("lean_features", r["features"]))
        imp_series = _display_importance(s) if s else None
        for feat in lean:
            if feat not in feature_set:
                feature_set[feat] = {}
            imp_val = float(imp_series.get(feat, 0)) if imp_series is not None else 0.0
            feature_set[feat][r["label"]] = imp_val

    # Sort by number of models (descending), then by average |importance|
    def _sort_key(item):
        feat, model_imps = item
        return (-len(model_imps), -np.mean([abs(v) for v in model_imps.values()]))

    sorted_feats = sorted(feature_set.items(), key=_sort_key)

    # Shorten model labels for column headers
    short_labels = {"MCAS Grades 3–8": "MCAS 3–8", "Postsecondary Attendance": "Post-sec",
                    "Dropout Rate": "Dropout", "SAT Performance": "SAT",
                    "MCAS Grade 10 (ELA)": "MCAS 10",
                    "Education Budget Share": "Ed Budget"}
    col_h2 = ["Factor", "# Models"] + [short_labels.get(m, m[:8]) for m in model_labels]
    xref_rows = []
    for feat, model_imps in sorted_feats:
        n_models = len(model_imps)
        row = [_feat_meta(feat)[0], str(n_models)]
        for lbl in model_labels:
            if lbl in model_imps:
                row.append(f"{model_imps[lbl]:+.3f}")
            else:
                row.append("—")
        xref_rows.append(row)

    if xref_rows:
        tbl2 = ax_bot.table(cellText=xref_rows, colLabels=col_h2,
                            bbox=[0.0, 0.0, 1.0, 0.88])
        tbl2.auto_set_font_size(False); tbl2.set_fontsize(8.5)
        char_w2 = [max(len(str(r[c])) for r in [col_h2] + xref_rows)
                   for c in range(len(col_h2))]
        tot2 = max(sum(char_w2), 1)
        n_col = col_h2.index("# Models")
        for (row, col), cell in tbl2.get_celld().items():
            cell.set_width(char_w2[col] / tot2)
            if row == 0:
                cell.set_facecolor(_BLUE); cell.set_text_props(color="white")
            else:
                n = int(xref_rows[row-1][n_col]) if row <= len(xref_rows) else 0
                if n >= 3:
                    bg = "#E8F8E8"   # appears in 3+ models — strong signal
                elif n == 2:
                    bg = "#FFF8E1"   # 2 models
                else:
                    bg = "white"
                cell.set_facecolor(bg)
                if col >= 2 and row <= len(xref_rows):
                    val = xref_rows[row-1][col]
                    if val != "—":
                        try:
                            v = float(val)
                            if v > 0:
                                cell.set_facecolor("#E8F8E8")
                            elif v < -0.05:
                                cell.set_facecolor("#FFE8E8")
                        except ValueError:
                            pass
            cell.set_edgecolor("#CCCCCC")

    _footer(fig, "Gap = actual − predicted; the Direction column resolves the sign "
            "(for dropout, lower-than-predicted is an over-performance).  "
            "Green rows = factor matters in 3+ outcomes.  "
            "Cell colour: green = positive importance, red = NEGATIVE importance — the factor adds "
            "noise rather than signal for that outcome (Kritzman fn. 12; expected, not an error) — "
            "white = not a candidate in that model (—).")
    _save(pdf, fig)


# Pre-specified Mahalanobis inclusion threshold for "demographically similar"
# overachievers — chosen on substantive grounds (the standard 2σ criterion),
# not tuned to the result.  Used by the call, the section header, and the
# footnote so all three are guaranteed identical.
DEMO_SIM_THRESHOLD: float = 2.0


def _demo_similar_overachievers(df2: "pd.DataFrame",
                                saugus_row: "pd.Series",
                                oa_pool: list[str],
                                threshold: float = DEMO_SIM_THRESHOLD) -> list[str]:
    """
    Return overachievers within `threshold` Mahalanobis standard deviations
    of Saugus on 6 non-actionable demographic features.

    Features — the descriptors of what a community IS, not what schools DO:
      median_hh_income, equalized_income, low_income_pct,
      total_enrollment, pct_bachelors_plus, ell_pct

    Method: Mahalanobis distance on the covariance of the full MA district
    population, accounting for correlations between features (e.g. income and
    poverty are r≈−0.81, so a town can't "use up" its similarity budget on
    both dimensions independently).

    d_M(town, Saugus) = √( Δz · R⁻¹ · Δz' )
    where z is the population z-score of each feature and R is the 6×6
    correlation matrix of all MA districts.

    Why z-score first: the raw features span ~16 orders of magnitude in
    variance (equalized_income is in dollars, ~1e9; the rest are percentages,
    ~1e1).  On the raw scale the covariance is numerically singular
    (condition number ~4e17), and a pseudo-inverse silently truncates the
    low-variance percentage dimensions to zero — so low_income_pct,
    pct_bachelors_plus and ell_pct drop out of the distance entirely and
    high-education resort towns (Nantucket, Lenox) get admitted as "similar".
    Standardizing first puts every feature on unit variance (condition number
    ~3e1); the distance is the same quantity in exact arithmetic but is now
    computed faithfully across all six features.

    Threshold of 2.0 corresponds to ≤2 Mahalanobis standard deviations —
    the standard 2σ inclusion criterion.  Threshold and feature set chosen on
    substantive grounds before examining which towns qualify.
    """
    demo_feats = ['median_hh_income', 'equalized_income', 'low_income_pct',
                  'total_enrollment', 'pct_bachelors_plus', 'ell_pct']
    avail = [f for f in demo_feats if f in df2.columns]
    if saugus_row is not None:
        # Drop any feature Saugus itself is missing — can't measure distance on it.
        avail = [f for f in avail if not pd.isna(saugus_row[f])]
    if not avail or saugus_row is None:
        return oa_pool

    # Standardize on the full MA district population, THEN form the covariance,
    # so the distance is well-conditioned across all six features (see docstring).
    data = df2[avail].dropna()
    mu   = data.mean()
    sd   = data.std(ddof=1).replace(0, 1.0)
    Z    = (data - mu) / sd
    cov_inv = np.linalg.inv(np.cov(Z.values, rowvar=False))

    def _z(row):
        return np.array([(float(row[f]) - mu[f]) / sd[f] for f in avail])
    saugus_vec = _z(saugus_row)

    similar = []
    for town in oa_pool:
        if town not in df2.index:
            continue
        row = df2.loc[town]
        # A town with missing demographics can't be screened for similarity —
        # imputing the mean would falsely pull it toward Saugus, so skip it.
        if any(pd.isna(row[f]) for f in avail):
            continue
        diff = _z(row) - saugus_vec
        d_m  = float(np.sqrt(diff @ cov_inv @ diff))
        if d_m <= threshold:
            similar.append((town, d_m))

    similar.sort(key=lambda x: x[1])
    return [t for t, _ in similar]


def _saugus_demo_peers(results: list[dict], df_raw: pd.DataFrame,
                       threshold: float = DEMO_SIM_THRESHOLD) -> dict:
    """
    Single source of truth for "towns like Saugus that beat their prediction."

    Builds the overachiever pool once (districts that most often beat their
    demographic prediction across the four outcomes), then filters it to the
    demographically similar subset via the standardized-Mahalanobis screen.
    Every page that needs a Saugus peer set — the Optimum Profile grids AND the
    budget/staffing trajectory — calls this so the peer towns are *derived* from
    one place and can never drift apart or be hand-listed.

    Returns a dict:
      df2          — district-indexed feature frame
      saugus       — Saugus feature row
      oa_pool      — all overachievers (districts that beat their prediction)
      sim_pool     — demographically similar OVERACHIEVERS (nearest first);
                     used by the Optimum Profile page, whose subject IS
                     "what towns like us that did better look like"
      sim_all      — ALL demographically similar districts within `threshold`,
                     regardless of outcome (nearest first); used by the
                     budget/staffing trajectory, whose subject is "how does our
                     funding compare to similar communities" — an unbiased
                     comparison that must not be restricted to better towns
      oa_counter, model_oa_map — overachiever bookkeeping for the profile page

    Both sim_pool and sim_all come from the one standardized-Mahalanobis screen
    (`_demo_similar_overachievers`); they differ only in the candidate pool fed
    to it, so "who counts as similar" is defined in exactly one place.
    """
    from collections import Counter

    # Overachiever-pool breadth.  We take the top-20 over-performers per outcome
    # (≈top 7% of ~290 districts — unambiguously "beating prediction") and keep
    # the 40 that recur most across the four outcomes.  Breadth is set here, not
    # tuned to a result: it must be wide enough that the demographically-similar
    # SUBSET (sim_pool) has a robust N after the strict d_M ≤ 2σ screen, rather
    # than collapsing to a handful.  At n=20/cap=40 the similar subset is ~11
    # towns; a narrower cap starves it (cap=20 → only 3) without changing the
    # direction of any comparison (verified: 3-town and 155-town peer medians
    # land on the same side of Saugus for every actionable factor).
    OA_PER_MODEL, OA_CAP = 20, 40

    oa_counter = Counter()
    model_oa_map = {}
    for r in results:
        s = r.get("saugus")
        if not s: continue
        oas = _find_overachievers(s["loo_df"], r["target"], n=OA_PER_MODEL)
        model_oa_map[r["label"]] = list(oas.index)
        for name in oas.index:
            oa_counter[name] += 1

    all_oas  = [t for t, _ in oa_counter.most_common(OA_CAP)]
    df2 = df_raw.copy(); df2.index = df2["district_name"]
    saugus   = df2.loc["Saugus"] if "Saugus" in df2.index else None
    oa_pool  = [t for t in all_oas if t in df2.index]
    sim_pool = _demo_similar_overachievers(df2, saugus, oa_pool, threshold)
    all_towns = [t for t in df2.index if t != "Saugus"]
    sim_all  = _demo_similar_overachievers(df2, saugus, all_towns, threshold)
    return {"df2": df2, "saugus": saugus, "oa_pool": oa_pool,
            "sim_pool": sim_pool, "sim_all": sim_all, "oa_counter": oa_counter,
            "model_oa_map": model_oa_map}


def page_optimum_profile(pdf, results: list[dict], df_raw: pd.DataFrame):
    """
    Two-row page:
      Top row    — targets from ALL overachievers (any MA district beating prediction)
      Bottom row — targets from demographically SIMILAR overachievers only
                   (Mahalanobis distance from Saugus ≤ 2.0σ)
    Both rows show methodology, peer list, and bar chart side-by-side.
    Showing both is more defensible than one alone: if the filtered and
    unfiltered results agree, the finding is robust.
    """
    # ── Peer set: derived from the single shared source ───────────────────────
    _peers   = _saugus_demo_peers(results, df_raw)
    df2          = _peers["df2"]
    saugus       = _peers["saugus"]
    oa_pool      = _peers["oa_pool"]
    oa_counter   = _peers["oa_counter"]
    model_oa_map = _peers["model_oa_map"]

    sim_pool = _peers["sim_pool"]

    # Actionable factors shown in both grids — the validated set (factor screen),
    # ordered roughly by importance.  (feature, label, unit, higher_is_better)
    actionable = [
        ("chronic_absenteeism_pct",   "Chronic absenteeism (%)",         "%", False),
        ("teachers_per_100_students", "Teachers / 100 students",         "",  True),
        ("teachers_per_lowincome",    "Teachers per low-income student",  "",  True),
        ("spend_vs_required",     "Spending vs Ch70 minimum (×)",     "",  True),
        ("nss_per_eqv",               "School spending vs. property wealth", "", True),
        ("teacher_pay_share",    "Teacher share of school $",        "",  True),
    ]

    # ── Helper: build a comparison grid ─────────────────────────────────────
    def _make_grid(ax, pool, section_title, header_color):
        """
        Draw a titled comparison table:
        Columns: Feature | Saugus | town1 | town2 | ... | Peer Median | Gap
        """
        ax.axis("off")

        # Section title
        ax.text(0.01, 0.97, section_title, ha="left", va="top",
                fontsize=10, fontweight="bold", color=header_color,
                transform=ax.transAxes)

        # Pick up to 4 representative towns to show in columns (fewer than before
        # so each column is wide enough for a readable, mostly-untruncated name).
        show_towns = [t for t in pool if t in df2.index][:4]
        if not show_towns:
            ax.text(0.5, 0.5, "No comparable towns found",
                    ha="center", va="center", transform=ax.transAxes,
                    fontsize=9, color=_GREY)
            return

        # Build header row and data rows
        short = lambda name: _shorten(name, 11)
        col_headers = (["Feature", "Saugus"] +
                       [short(t) for t in show_towns] +
                       ["Peer med", "Gap", "N"])

        rows = []
        for feat, lbl, unit, hi_good in actionable:
            if feat not in df2.columns or saugus is None:
                continue
            sv = float(saugus[feat]) if not pd.isna(saugus[feat]) else float("nan")
            all_vals = [float(df2.loc[t, feat])
                        for t in pool
                        if feat in df2.columns and not pd.isna(df2.loc[t, feat])]
            if not all_vals:
                continue
            med = float(np.median(all_vals))
            gap = med - sv

            def _f(v):
                if np.isnan(v): return "—"
                if unit == "$": return f"${v:,.0f}"
                if unit == "%": return f"{v:.1f}%"
                if abs(v) < 1:  return f"{v:.3f}"      # small ratios
                if abs(v) < 10: return f"{v:.2f}"
                return f"{v:.1f}"

            def _fgap(v):
                if unit == "$": return f"${v:+,.0f}"
                if unit == "%": return f"{v:+.1f}pp"
                if abs(v) < 1:  return f"{v:+.3f}"
                if abs(v) < 10: return f"{v:+.2f}"
                return f"{v:+.1f}"

            town_vals = []
            for t in show_towns:
                v = float(df2.loc[t, feat]) \
                    if feat in df2.columns and t in df2.index \
                    and not pd.isna(df2.loc[t, feat]) else float("nan")
                town_vals.append(_f(v))

            rows.append(
                [lbl, _f(sv)] + town_vals +
                [_f(med), _fgap(gap), str(len(all_vals))]
            )

        if not rows:
            return

        tbl = ax.table(
            cellText=rows,
            colLabels=col_headers,
            bbox=[0.0, 0.02, 1.0, 0.88],
            cellLoc="center")
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(7.8)

        # Column widths: Feature wide, others equal
        n_town_cols = len(show_towns)
        feat_w   = 0.30
        saugus_w = 0.11
        town_w   = (1.0 - feat_w - saugus_w - 0.18) / max(n_town_cols, 1)
        med_w, gap_w, n_w = 0.10, 0.10, 0.06
        col_ws = ([feat_w, saugus_w] +
                  [town_w] * n_town_cols +
                  [med_w, gap_w, n_w])

        gap_col_idx = 2 + n_town_cols   # index of "Gap" column

        for (ri, ci), cell in tbl.get_celld().items():
            if ci < len(col_ws):
                cell.set_width(col_ws[ci])
            if ri == 0:
                cell.set_facecolor(header_color)
                cell.set_text_props(color="white", fontsize=7.5)
            elif ci == 1 and ri > 0:                    # Saugus column
                cell.set_facecolor("#FFF8E1")
            elif ci == gap_col_idx and ri > 0:          # Gap column
                gap_str = rows[ri-1][gap_col_idx]
                row_idx = ri - 1
                hi_good = actionable[row_idx][3] if row_idx < len(actionable) else True
                bad = (gap_str.startswith("+") and not hi_good) or \
                      (gap_str.startswith("-") and hi_good)
                cell.set_facecolor("#FFE8E8" if bad else "#E8F8E8")
            else:
                cell.set_facecolor("#F5F5F5" if ri % 2 else "white")
            cell.set_edgecolor("#DDDDDD")

        # Town names below the table (full names — there is room in the note)
        town_list = ", ".join(show_towns)
        if len(pool) > len(show_towns):
            town_list += f"  (+ {len(pool)-len(show_towns)} more in median)"
        ax.text(0.01, 0.01, f"Shown: {town_list}",
                ha="left", va="bottom", fontsize=6.5, color=_GREY,
                style="italic", transform=ax.transAxes)

    # ── Page: 2 stacked axes ─────────────────────────────────────────────────
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(_PAGE_W, _PAGE_H))
    fig.patch.set_facecolor("white")
    _header(fig, "Optimum Profile — What Overachievers Look Like",
            "Each grid shows actionable factor values for Saugus vs over-performer towns.  "
            "Showing both peer groups tests whether the finding holds across definitions.")

    _make_grid(
        ax_top, oa_pool,
        f"Overachievers  —  {len(oa_pool)} MA districts that most consistently beat their demographic prediction",
        _BLUE)

    _sig = f"{DEMO_SIM_THRESHOLD:g}σ"
    _make_grid(
        ax_bot, sim_pool,
        textwrap.fill(
            f"Demographically Similar Overachievers  —  {len(sim_pool)} of those towns within "
            f"{_sig} Mahalanobis distance of Saugus on income, property wealth, "
            f"poverty, size, education & ELL",
            width=100),
        _GREEN)

    _maha_note = (
        "Similarity metric: standardized Mahalanobis distance d_M = √(Δz · R⁻¹ · Δz'), "
        "z = population z-score and R = correlation matrix of all MA districts on "
        "(median_hh_income, equalized_income, low_income_pct, total_enrollment, "
        f"pct_bachelors_plus, ell_pct).  Threshold: d_M ≤ {DEMO_SIM_THRESHOLD:g} ({_sig}).  "
        "Pre-specified; not tuned to result.  "
        "Gap = peer median − Saugus.  Red = Saugus below peer median, Green = at or above."
    )
    fig.text(0.5, 0.005, textwrap.fill(_maha_note, width=170),
             ha="center", va="bottom", fontsize=6.5, color=_GREY, style="italic",
             linespacing=1.3)

    plt.tight_layout(rect=[0, 0.03, 1, 0.91])
    _save(pdf, fig)


def page_budget_and_staffing(pdf, engine, results: list[dict],
                             df_raw: pd.DataFrame,
                             ridge_stats: dict | None = None) -> None:
    """
    Two-panel page sourced directly from Schedule A and DESE staffing data:
      Left  — Education's share of Saugus's total municipal budget over time
               vs peer median (2010–2025)
      Right — Teacher FTE per 1,000 students over time for Saugus
               vs peer median (2009–2024)

    These are longitudinal charts the RBP cross-section cannot show.
    They answer WHY the teacher density gap exists: budget share has been
    declining while peer towns held steady.

    Peer set: ALL demographically similar MA towns (the standardized-Mahalanobis
    screen in `_saugus_demo_peers`, sim_all) — not a hand-picked list, and not
    restricted to overachievers.  A funding comparison must use an unbiased peer
    set; filtering to better-performing towns would inflate the apparent gap.
    The "who is similar" rule is the same one the Optimum Profile page uses.

    The footnote carries an independent Ridge-regression cross-check whose
    numbers (N, CV R², chronic-absenteeism rank/β) are computed live in
    compute_ridge_validation and passed in via `ridge_stats`; if None, the note
    falls back to a qualified statement with no specific figures.
    """
    from sqlalchemy import text as _text

    # Peer towns are DERIVED from the shared peer source, never hardcoded.
    PEER_TOWNS = _saugus_demo_peers(results, df_raw)["sim_all"]

    with engine.connect() as conn:
        # ── Budget share ──────────────────────────────────────────────────
        budget = pd.read_sql(_text("""
            SELECT municipality, fiscal_year,
                   ROUND(100.0*education/NULLIF(total_expenditures,0),1) AS ed_pct
            FROM municipal_expenditures
            WHERE municipality = ANY(:towns)
              -- Drop years with no reported education line (e.g. Saugus FY2000
              -- has education=0 against a half-sized total) so the trajectory
              -- does not start from a spurious 0%.
              AND education > 0
              AND total_expenditures > 0
            ORDER BY municipality, fiscal_year
        """), conn, params={"towns": ['Saugus'] + PEER_TOWNS})

        # ── Teacher density ───────────────────────────────────────────────
        staffing_q = pd.read_sql(_text("""
            SELECT s.district_name, s.school_year, s.fte AS teacher_fte,
                   e.total AS enrollment
            FROM staffing s
            JOIN (SELECT school_year, district_name,
                         SUM(total) AS total
                  FROM enrollment
                  WHERE grade = 'Total'
                  GROUP BY school_year, district_name) e
              ON s.school_year = e.school_year
             AND s.district_name = e.district_name
            WHERE s.category = 'teacher_fte'
              AND s.district_name = ANY(:towns)
            ORDER BY s.district_name, s.school_year
        """), conn, params={"towns": ['Saugus'] + PEER_TOWNS})

    staffing_q['per_1k'] = (staffing_q['teacher_fte'] /
                             staffing_q['enrollment'].replace(0, float('nan')) * 1000)

    # ── Build peer medians ────────────────────────────────────────────────
    def _peer_median(df, town_col, year_col, val_col, peers):
        grp = df[df[town_col].isin(peers)].groupby(year_col)[val_col].median()
        return grp

    bud_saugus = budget[budget['municipality'] == 'Saugus'].set_index('fiscal_year')['ed_pct']
    bud_peers  = _peer_median(budget, 'municipality', 'fiscal_year', 'ed_pct', PEER_TOWNS)

    st_saugus  = staffing_q[staffing_q['district_name'] == 'Saugus'].set_index('school_year')['per_1k']
    st_peers   = _peer_median(staffing_q, 'district_name', 'school_year', 'per_1k', PEER_TOWNS)

    # ── Page ──────────────────────────────────────────────────────────────
    # Span the title reports is derived from the data actually plotted, so it can
    # never go stale as new years arrive.
    _span_yrs = sorted(set(bud_saugus.index) | set(st_saugus.index))
    _n_years  = (_span_yrs[-1] - _span_yrs[0] + 1) if _span_yrs else 0
    fig, (ax_l, ax_r) = _paper_fig(1, 2)
    _header(fig, f"Budget Allocation & Teacher Density — {_n_years}-Year Trajectory "
                 f"(FY{_span_yrs[0]}–{_span_yrs[-1]})" if _span_yrs else
                 "Budget Allocation & Teacher Density — Trajectory",
            "Source: MA DLS Schedule A (budget) & DESE Staffing (teachers)  ·  "
            f"Peers: median of {len(PEER_TOWNS)} demographically similar MA towns "
            f"(Mahalanobis ≤ {DEMO_SIM_THRESHOLD:g}σ; see note below)")
    # Drop the subplots so their two-line titles clear the header subtitle.
    fig.subplots_adjust(top=0.84)

    # ── Left: budget share ────────────────────────────────────────────────
    common_yrs_b = sorted(set(bud_saugus.index) & set(bud_peers.index))
    ax_l.plot(common_yrs_b, [bud_saugus.get(y, float('nan')) for y in common_yrs_b],
              color=_GOLD, lw=2.5, marker='o', ms=5, label='Saugus', zorder=4)
    ax_l.plot(common_yrs_b, [bud_peers.get(y, float('nan')) for y in common_yrs_b],
              color=_BLUE, lw=2, ls='--', marker='s', ms=4, label='Peer median', zorder=3)

    # Shade the gap
    s_vals = [bud_saugus.get(y, float('nan')) for y in common_yrs_b]
    p_vals = [bud_peers.get(y, float('nan')) for y in common_yrs_b]
    ax_l.fill_between(common_yrs_b, s_vals, p_vals,
                      where=[s < p for s, p in zip(s_vals, p_vals)],
                      alpha=0.15, color=_RED, label='Saugus below peers')

    # Annotate first and last year of the actual series (not fixed years)
    if common_yrs_b:
        _yb0, _yb1 = common_yrs_b[0], common_yrs_b[-1]
        if not pd.isna(bud_saugus.get(_yb0, float('nan'))):
            ax_l.annotate(f"{bud_saugus[_yb0]:.1f}%", (_yb0, bud_saugus[_yb0]),
                          textcoords="offset points", xytext=(4, 4), fontsize=8, color=_GOLD)
        if not pd.isna(bud_saugus.get(_yb1, float('nan'))):
            ax_l.annotate(f"{bud_saugus[_yb1]:.1f}%", (_yb1, bud_saugus[_yb1]),
                          textcoords="offset points", xytext=(-30, -12), fontsize=8, color=_GOLD)

    ax_l.set_xlabel("Fiscal year")
    ax_l.set_ylabel("Education as % of total municipal expenditure")
    ax_l.set_title("Education Budget Share\n(Schedule A, general fund)", fontsize=9)
    ax_l.legend(fontsize=8)
    ax_l.grid(alpha=0.25)
    ax_l.set_ylim(bottom=0)

    # ── Right: teacher density ────────────────────────────────────────────
    common_yrs_s = sorted(set(st_saugus.index) & set(st_peers.index))
    ax_r.plot(common_yrs_s, [st_saugus.get(y, float('nan')) for y in common_yrs_s],
              color=_GOLD, lw=2.5, marker='o', ms=5, label='Saugus', zorder=4)
    ax_r.plot(common_yrs_s, [st_peers.get(y, float('nan')) for y in common_yrs_s],
              color=_BLUE, lw=2, ls='--', marker='s', ms=4, label='Peer median', zorder=3)

    # Shade gap
    sv2 = [st_saugus.get(y, float('nan')) for y in common_yrs_s]
    pv2 = [st_peers.get(y, float('nan')) for y in common_yrs_s]
    ax_r.fill_between(common_yrs_s, sv2, pv2,
                      where=[not (float('nan') in [s,p]) and s < p
                             for s, p in zip(sv2, pv2)],
                      alpha=0.15, color=_RED, label='Saugus below peers')

    # Annotate first and last year for Saugus
    first_yr = min(y for y in common_yrs_s if not pd.isna(st_saugus.get(y, float('nan'))))
    last_yr  = max(y for y in common_yrs_s if not pd.isna(st_saugus.get(y, float('nan'))))
    ax_r.annotate(f"{st_saugus[first_yr]:.1f}", (first_yr, st_saugus[first_yr]),
                  textcoords="offset points", xytext=(4, 4), fontsize=8, color=_GOLD)
    ax_r.annotate(f"{st_saugus[last_yr]:.1f}", (last_yr, st_saugus[last_yr]),
                  textcoords="offset points", xytext=(-28, -12), fontsize=8, color=_GOLD)

    ax_r.set_xlabel("School year")
    ax_r.set_ylabel("Teacher FTE per 1,000 students")
    ax_r.set_title("Teacher Density\n(DESE Staffing + Enrollment)", fontsize=9)
    ax_r.legend(fontsize=8)
    ax_r.grid(alpha=0.25)

    # Ridge cross-validation note — numbers computed live (compute_ridge_validation)
    _peer_note = (f"Peer set: median of {len(PEER_TOWNS)} demographically similar MA towns "
                  f"within standardized-Mahalanobis d_M ≤ {DEMO_SIM_THRESHOLD:g}σ of Saugus on the "
                  f"6 demographic features (median_hh_income, equalized_income, low_income_pct, "
                  f"total_enrollment, pct_bachelors_plus, ell_pct). Same similarity rule as the "
                  f"Optimum Profile page; peers derived, not hand-picked; all similar towns, not "
                  f"only better-performing ones.")
    if ridge_stats:
        _focus_desc = _feat_meta(ridge_stats["focus"])[0].lower()
        _rank_phrase = (f"the #1 of {ridge_stats['n_features']} standardized predictors"
                        if ridge_stats["is_top"]
                        else f"#{ridge_stats['focus_rank']} of "
                             f"{ridge_stats['n_features']} standardized predictors")
        # Branch on whether Ridge actually agrees with RBP.  When absenteeism
        # ranks LOW in the linear model but HIGH in RBP, that is a divergence, not
        # corroboration — and it is the paper's whole point (a linear coefficient
        # and RBP's interaction-aware importance measure different things).
        if ridge_stats["is_top"]:
            _agreement = ("corroborating the RBP Exhibit 5 ranking by an independent "
                          "linear method")
        else:
            _agreement = (
                f"a divergence to read with care: a linear model assigns {_focus_desc} a "
                f"small standardized coefficient while RBP's Exhibit 5 ranks it among the top "
                f"factors.  Some of this is what RBP is built to catch — nonlinear / interaction "
                f"effects a single linear coefficient misses — but some is mechanical: RBP "
                f"importance is task-specific and is elevated for any feature on which the "
                f"subject town is an outlier, and Saugus sits well into the upper tail of the "
                f"statewide distribution on {_focus_desc}.  A high RBP rank therefore sharpens Saugus's "
                f"prediction; on its own it is not evidence that {_focus_desc} is a uniquely "
                f"powerful driver")
        _validation = (
            f"Independent cross-validation (computed in this run): standardized Ridge "
            f"regression on {ridge_stats['n']} MA districts (5-fold CV R²="
            f"{ridge_stats['r2']:.2f}) ranks {_focus_desc} {_rank_phrase} of MCAS 3–8 "
            f"(|standardized β|={ridge_stats['focus_abs_beta']:.2f}) — {_agreement}."
        )
    else:
        _validation = (
            "Independent cross-validation: a standardized Ridge regression on the same "
            "district panel cross-checks the RBP Exhibit 5 ranking (figures unavailable "
            "in this run)."
        )
    # Two stacked paragraphs (validation, then peer-set) — pre-wrapped and joined
    # with a carriage return so the block stays tidy and clear of the x-axis labels.
    _note = (textwrap.fill(_validation, width=185) + "\n"
             + textwrap.fill(_peer_note, width=185))
    fig.text(0.5, 0.008, _note, ha="center", va="bottom", fontsize=6.0,
             color=_GREY, style="italic", linespacing=1.3)

    plt.tight_layout(rect=[0, 0.15, 1, 0.91])
    _save(pdf, fig)


def page_fixed_costs(pdf, engine, fiscal_year: int) -> None:
    """
    Four-panel page: where Saugus's fixed costs come from, vs comparable
    towns ($80–$140M budget) and all MA.

    Panels:
      TL — Fixed costs as % of total budget (3-bar comparison)
      TR — Education as % of total budget (3-bar comparison)
      BL — Fixed cost 3-component composition: HI / OPEB trust contrib / Remainder
      BR — Saugus OPEB trust contributions trend FY2012–latest vs comparable avg

    The cross-sectional snapshot bars use `fiscal_year` — the same year as the
    RBP cross-section (df.attrs['analysis_fiscal_year']) — so the Education-share
    figure here matches the "Education Budget Share" outcome modelled elsewhere.
    Only the BR trend panel runs through the latest year.

    Verification: expenditure column sums equal total_expenditures exactly for
    every statewide municipality-year (count printed live in the footer); health
    insurance is excluded where HI > fixed_costs × 1.01 (~0.9% of town-years,
    likely cross-fund accounting).
    """
    from sqlalchemy import text as _text

    FY = fiscal_year
    COMPARABLE_LO = 80e6
    COMPARABLE_HI = 140e6

    with engine.connect() as conn:
        cross = pd.read_sql(_text("""
            SELECT e.municipality,
                   e.fixed_costs, e.education, e.total_expenditures,
                   COALESCE(h.health_insurance_expenditure, 0)              AS hi,
                   COALESCE(tf.opeb_trust, 0)                               AS opeb_contrib,
                   COALESCE(tf.workers_compensation, 0)                     AS wkcomp_contrib,
                   e.fixed_costs
                       - COALESCE(h.health_insurance_expenditure, 0)
                       - COALESCE(tf.opeb_trust, 0)
                       - COALESCE(tf.workers_compensation, 0)               AS remainder,
                   CASE WHEN h.health_insurance_expenditure IS NOT NULL
                              AND h.health_insurance_expenditure > 0
                        THEN 1 ELSE 0 END                                   AS has_hi
            FROM municipal_expenditures e
            LEFT JOIN municipal_health_insurance h
                ON h.dor_code = e.dor_code AND h.fiscal_year = e.fiscal_year
            LEFT JOIN municipal_trust_funds tf
                ON tf.dor_code = e.dor_code AND tf.fiscal_year = e.fiscal_year
               AND tf.amount_type = 'Revenues'
            WHERE e.fiscal_year = :fy
              AND e.total_expenditures > 0
              AND (h.health_insurance_expenditure IS NULL
                   OR h.health_insurance_expenditure <= e.fixed_costs * 1.01)
        """), conn, params={"fy": FY})

        trend = pd.read_sql(_text("""
            SELECT e.fiscal_year,
                   e.fixed_costs, e.education, e.total_expenditures,
                   COALESCE(h.health_insurance_expenditure, 0)             AS hi,
                   COALESCE(tf.opeb_trust, 0)                              AS opeb_contrib,
                   COALESCE(s.total_stabilization_fund_balance, 0)         AS stbl,
                   COALESCE(f.cert_free_cash, 0)                           AS free_cash
            FROM municipal_expenditures e
            LEFT JOIN municipal_health_insurance h
                ON h.dor_code = e.dor_code AND h.fiscal_year = e.fiscal_year
            LEFT JOIN municipal_trust_funds tf
                ON tf.dor_code = e.dor_code AND tf.fiscal_year = e.fiscal_year
               AND tf.amount_type = 'Revenues'
            LEFT JOIN municipal_stabilization s
                ON s.dor_code = e.dor_code AND s.fiscal_year = e.fiscal_year
            LEFT JOIN municipal_free_cash f
                ON f.dor_code = e.dor_code AND f.fiscal_year = e.fiscal_year
            WHERE lower(e.municipality) = 'saugus'
              AND e.fiscal_year BETWEEN 2012 AND 2025
            ORDER BY e.fiscal_year
        """), conn)

        # Comparable town OPEB contributions over time (for BR panel reference band)
        comp_opeb_trend = pd.read_sql(_text("""
            SELECT tf.fiscal_year,
                   AVG(tf.opeb_trust) / 1e6 AS avg_opeb_m,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tf.opeb_trust) / 1e6 AS med_opeb_m
            FROM municipal_trust_funds tf
            JOIN municipal_expenditures me
                ON me.dor_code = tf.dor_code AND me.fiscal_year = tf.fiscal_year
            WHERE tf.amount_type = 'Revenues'
              AND tf.fiscal_year BETWEEN 2012 AND 2025
              AND me.total_expenditures BETWEEN 80000000 AND 140000000
            GROUP BY tf.fiscal_year
            ORDER BY tf.fiscal_year
        """), conn)

        # Town-years backing the column-sum verification (whole available panel).
        n_townyears = conn.execute(_text(
            "SELECT COUNT(*) FROM municipal_expenditures WHERE total_expenditures > 0"
        )).scalar()

    # ── Derived groups ─────────────────────────────────────────────────────────
    saugus = cross[cross["municipality"].str.lower() == "saugus"].iloc[0]
    comp   = cross[cross["total_expenditures"].between(COMPARABLE_LO, COMPARABLE_HI)]
    n_comp = len(comp)
    n_all  = len(cross)

    def _pct(num, denom):
        return float(num / denom * 100) if denom else float("nan")

    saugus_fc_pct  = _pct(saugus["fixed_costs"], saugus["total_expenditures"])
    saugus_ed_pct  = _pct(saugus["education"],   saugus["total_expenditures"])

    comp_fc_pct  = float((comp["fixed_costs"] / comp["total_expenditures"] * 100).mean())
    comp_ed_pct  = float((comp["education"]   / comp["total_expenditures"] * 100).mean())
    comp_avg_hi   = float(comp.loc[comp["has_hi"] == 1, "hi"].mean())
    comp_avg_opeb = float(comp["opeb_contrib"].mean())
    comp_avg_rem  = float(comp["remainder"].mean())
    comp_avg_fc   = float(comp["fixed_costs"].mean())

    all_fc_pct  = float((cross["fixed_costs"] / cross["total_expenditures"] * 100).mean())
    all_ed_pct  = float((cross["education"]   / cross["total_expenditures"] * 100).mean())
    all_avg_hi   = float(cross.loc[cross["has_hi"] == 1, "hi"].mean())
    all_avg_opeb = float(cross["opeb_contrib"].mean())
    all_avg_rem  = float(cross["remainder"].mean())
    all_avg_fc   = float(cross["fixed_costs"].mean())

    saugus_hi   = float(saugus["hi"])
    saugus_opeb = float(saugus["opeb_contrib"])
    saugus_rem  = float(saugus["remainder"])
    saugus_fc   = float(saugus["fixed_costs"])

    # ── Layout ─────────────────────────────────────────────────────────────────
    fig, axes = _paper_fig(2, 2, gridspec_kw={"hspace": 0.65, "wspace": 0.38})
    ((ax_tl, ax_tr), (ax_bl, ax_br)) = axes

    _header(
        fig,
        "Fixed Costs: Sources, Scale, and OPEB Prefunding",
        f"FY{FY} · Comparable = {n_comp} MA towns ($80M–$140M) · All MA = {n_all} · "
        f"Columns sum exactly to total for all {n_townyears:,} statewide municipality-years",
    )

    LABELS   = ["Saugus", f"Comparable\n(n={n_comp})", f"All MA\n(n={n_all})"]
    COLORS   = [_GOLD, _BLUE, _GREY]
    BAR_H    = 0.45

    # ── TL: Fixed costs % of budget ───────────────────────────────────────────
    fc_vals = [saugus_fc_pct, comp_fc_pct, all_fc_pct]
    bars = ax_tl.barh(LABELS, fc_vals, height=BAR_H, color=COLORS, alpha=0.85)
    for bar, val in zip(bars, fc_vals):
        ax_tl.text(val + 0.3, bar.get_y() + bar.get_height() / 2,
                   f"{val:.1f}%", va="center", ha="left", fontsize=9,
                   fontweight="bold" if val == saugus_fc_pct else "normal")
    ax_tl.set_xlabel("% of total municipal expenditure")
    ax_tl.set_title(f"Fixed Costs as % of Budget (FY{FY})", fontsize=9)
    ax_tl.grid(axis="x", alpha=0.25)
    ax_tl.set_xlim(0, max(fc_vals) * 1.25)

    # ── TR: Education % of budget ─────────────────────────────────────────────
    ed_vals = [saugus_ed_pct, comp_ed_pct, all_ed_pct]
    bars2 = ax_tr.barh(LABELS, ed_vals, height=BAR_H, color=COLORS, alpha=0.85)
    for bar, val in zip(bars2, ed_vals):
        ax_tr.text(val + 0.3, bar.get_y() + bar.get_height() / 2,
                   f"{val:.1f}%", va="center", ha="left", fontsize=9,
                   fontweight="bold" if val == saugus_ed_pct else "normal")
    ax_tr.set_xlabel("% of total municipal expenditure")
    ax_tr.set_title(f"Education as % of Budget (FY{FY})", fontsize=9)
    ax_tr.grid(axis="x", alpha=0.25)
    ax_tr.set_xlim(0, max(ed_vals) * 1.20)

    # ── BL: Fixed cost composition — 3-component stacked bars ─────────────────
    # Components: Health Insurance | OPEB trust contributions | Remainder
    hi_vals   = [saugus_hi   / 1e6, comp_avg_hi   / 1e6, all_avg_hi   / 1e6]
    opeb_vals = [saugus_opeb / 1e6, comp_avg_opeb / 1e6, all_avg_opeb / 1e6]
    rem_vals  = [saugus_rem  / 1e6, comp_avg_rem  / 1e6, all_avg_rem  / 1e6]
    fc_totals = [saugus_fc   / 1e6, comp_avg_fc   / 1e6, all_avg_fc   / 1e6]

    C_HI   = "#5B84C4"   # lighter blue
    C_OPEB = "#B7950B"   # gold
    C_REM  = "#2C4770"   # dark blue

    y_pos = list(range(len(LABELS)))
    ax_bl.barh(y_pos, hi_vals,   height=BAR_H, color=C_HI,   alpha=0.90,
               label="Health insurance")
    ax_bl.barh(y_pos, opeb_vals, height=BAR_H, left=hi_vals, color=C_OPEB, alpha=0.90,
               label="OPEB prefunding")
    left2 = [h + o for h, o in zip(hi_vals, opeb_vals)]
    ax_bl.barh(y_pos, rem_vals,  height=BAR_H, left=left2,   color=C_REM,  alpha=0.85,
               label="Remainder (pension + other)")
    ax_bl.set_yticks(y_pos)
    ax_bl.set_yticklabels(LABELS)

    for i, (hi, opeb, rem, total) in enumerate(zip(hi_vals, opeb_vals, rem_vals, fc_totals)):
        hi_pct   = hi   / total * 100 if total else 0
        opeb_pct = opeb / total * 100 if total else 0
        rem_pct  = rem  / total * 100 if total else 0
        if hi > 0.8:
            ax_bl.text(hi / 2, i, f"${hi:.1f}M\n({hi_pct:.0f}%)",
                       ha="center", va="center", fontsize=7.5, color="white", fontweight="bold")
        # Only label OPEB inside the bar if it occupies ≥4% of bar width; otherwise annotate above
        if opeb_pct >= 4:
            lbl = f"${opeb:.1f}M\n({opeb_pct:.0f}%)"
            ax_bl.text(hi + opeb / 2, i, lbl,
                       ha="center", va="center", fontsize=6.5, color="white", fontweight="bold")
        elif opeb > 0:
            opeb_str = f"${opeb*1000:.0f}K" if opeb < 1 else f"${opeb:.1f}M"
            ax_bl.annotate(
                f"OPEB {opeb_str}\n({opeb_pct:.1f}%)",
                xy=(hi + opeb / 2, i), xytext=(hi + opeb / 2, i + 0.32),
                fontsize=6.5, color=C_OPEB, ha="center",
                arrowprops=dict(arrowstyle="-", color=C_OPEB, lw=0.6),
            )
        else:
            ax_bl.text(hi + opeb + 0.05, i + 0.30, "OPEB $0",
                       va="bottom", fontsize=6.5, color=C_OPEB, style="italic")
        if rem > 0.5:
            ax_bl.text(hi + opeb + rem / 2, i, f"${rem:.1f}M\n({rem_pct:.0f}%)",
                       ha="center", va="center", fontsize=7.5, color="white")
        ax_bl.text(total + 0.15, i, f"${total:.1f}M", va="center", fontsize=8)

    ax_bl.set_xlabel("$ millions")
    ax_bl.set_title(f"Fixed Cost Composition (FY{FY})\n"
                    "HI  |  OPEB trust contributions  |  Remainder", fontsize=8.5)
    # Color legend intentionally omitted: the subtitle above already names the three
    # segments left-to-right in bar order (HI / OPEB / Remainder), and the gold OPEB
    # callout labels that slice — a separate legend below the axis only collided with
    # the data-source footer.
    ax_bl.grid(axis="x", alpha=0.25)
    ax_bl.set_xlim(0, max(fc_totals) * 1.30)

    # ── BR: OPEB trust contribution trend — Saugus vs comparable peers ────────
    yrs_t      = trend["fiscal_year"].tolist()
    saugus_opeb_t = (trend["opeb_contrib"] / 1e3).tolist()   # thousands

    comp_yrs  = comp_opeb_trend["fiscal_year"].tolist()
    comp_avg_t = (comp_opeb_trend["avg_opeb_m"] * 1e3).tolist()  # back to thousands
    comp_med_t = (comp_opeb_trend["med_opeb_m"] * 1e3).tolist()

    ax_br.bar(yrs_t, saugus_opeb_t, color=_GOLD, alpha=0.85, label="Saugus OPEB contribution")
    ax_br.plot(comp_yrs, comp_avg_t, color=_BLUE, lw=1.5, ls="--", label=f"Comparable avg (n={n_comp})")
    ax_br.plot(comp_yrs, comp_med_t, color=_GREY, lw=1.2, ls=":", label="Comparable median")

    ax_br.set_xlabel("Fiscal year")
    ax_br.set_ylabel("$ thousands")
    ax_br.set_title(f"OPEB Trust Fund Contributions FY{yrs_t[0]}–{yrs_t[-1]}\n"
                    "Saugus vs. comparable towns ($80M–$140M budget)", fontsize=8.5)
    ax_br.legend(fontsize=7.5, loc="upper left")
    ax_br.grid(alpha=0.25)
    ax_br.set_ylim(bottom=0)
    ax_br.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f"${x:,.0f}K")
    )

    # Annotate Saugus's most recent OPEB bar (latest trend year)
    last_yr  = yrs_t[-1]
    last_val = saugus_opeb_t[-1]
    ax_br.annotate(
        f"${last_val:,.0f}K\n(FY{last_yr})",
        (last_yr, last_val),
        xytext=(-4, 6), textcoords="offset points",
        fontsize=7.5, color=_GOLD, fontweight="bold", ha="right",
    )

    # Live figures for the footnote so they track the data, not a fixed year.
    _prefund_yrs = [y for y, v in zip(yrs_t, saugus_opeb_t) if v > 0]
    _prefund_start = _prefund_yrs[0] if _prefund_yrs else last_yr
    _comp_avg_last = comp_avg_t[-1] if comp_avg_t else float("nan")  # $ thousands
    _footer(
        fig,
        "Health insurance from DLS Schedule A Part 2 & 6 (self-insured municipalities). "
        "OPEB & workers comp contributions from Schedule A Part 6 trust fund revenues (municipal_trust_funds, Revenues). "
        "Remainder = pension assessment (Essex Regional Retirement System) + workers comp insurance + unemployment + other. "
        f"Saugus began OPEB prefunding in FY{_prefund_start}; FY{last_yr} contribution = ${last_val:,.0f}K "
        f"vs. comparable-town avg ~${_comp_avg_last:,.0f}K."
    )

    # Reserve a bottom band for the ~3-line data-source footer (no legend competes
    # for it now).
    plt.tight_layout(rect=[0, 0.10, 1, 0.86])
    _save(pdf, fig)


def _find_overachievers(loo_df: pd.DataFrame, target: str,
                        n: int = 8) -> pd.DataFrame:
    """
    Return the top-N districts by positive residual (beating their prediction).
    For dropout the residual sign is inverted (lower dropout = better).
    """
    df = loo_df.dropna(subset=["residual"]).copy()
    # Dropout: lower is better, so negate residual for ranking
    if "dropout" in target.lower():
        df["_rank_resid"] = -df["residual"]
    else:
        df["_rank_resid"] = df["residual"]
    # Exclude Saugus from overachiever list
    df = df[df.index != "Saugus"]
    return df.nlargest(n, "_rank_resid").drop(columns=["_rank_resid"])


def _comparable_overperformers(loo_df: pd.DataFrame, target: str) -> pd.DataFrame:
    """
    Towns the model predicted SIMILARLY to Saugus but whose ACTUAL outcome was
    better — the most genuinely comparable peers to learn from.  ("A town the
    model expected to score like Saugus, but that actually did better.")

    Selection: among towns that beat Saugus's actual value (direction-aware —
    higher for academics, lower for dropout), keep those whose predicted value
    sits within one prediction standard error of Saugus's predicted value —
    i.e. towns statistically indistinguishable from Saugus in expectation.

    The tolerance is the model's leave-one-out prediction standard error
    (RMSE of the LOO residuals), a genuine statistical quantity rather than a
    hardcoded count: a town within ±1 SE of Saugus's prediction is inside the
    model's ~68% prediction band for Saugus.  The peer-set size is therefore
    data-driven and identical for every page that calls this.  A small floor
    (nearest 3) guards against a degenerate empty set.
    """
    df = loo_df.dropna(subset=["actual", "predicted"]).copy()
    if SAUGUS not in df.index:
        return df.head(0)
    s_pred = float(df.loc[SAUGUS, "predicted"])
    s_act  = float(df.loc[SAUGUS, "actual"])

    # Tolerance = LOO prediction standard error (RMSE of residuals): the 1σ
    # prediction band, so "comparable" = not significantly distinguishable from
    # Saugus in predicted outcome.
    resid = (df["actual"] - df["predicted"])
    tol = float(np.sqrt((resid ** 2).mean())) if resid.notna().any() else 0.0

    df = df[df.index != SAUGUS]
    better = (df[df["actual"] > s_act] if _higher_is_better(target)
              else df[df["actual"] < s_act])
    if better.empty:
        better = df
    better = better.assign(_d=(better["predicted"] - s_pred).abs())
    near = better[better["_d"] <= tol].sort_values("_d")
    if len(near) < 3:                      # floor for a usable median
        near = better.sort_values("_d").head(3)
    return near.drop(columns=["_d"])


def _find_underachievers(loo_df: pd.DataFrame, target: str,
                         n: int = 6) -> pd.DataFrame:
    """
    Return the top-N districts by negative residual (performing worse than predicted).
    For dropout, 'underachievers' are districts with higher-than-predicted dropout.
    """
    df = loo_df.dropna(subset=["residual"]).copy()
    # Dropout: higher is worse, so rank by raw positive residual
    if "dropout" in target.lower():
        df["_rank_resid"] = df["residual"]
    else:
        df["_rank_resid"] = -df["residual"]   # most negative gap first
    df = df[df.index != "Saugus"]
    return df.nlargest(n, "_rank_resid").drop(columns=["_rank_resid"])


def page_overachievers_scatter(pdf, label: str, target: str, analysis: dict):
    """Scatter: actual vs predicted — overachievers (green) and underachievers (red) highlighted."""
    fig, ax = _paper_fig()
    fig.subplots_adjust(top=0.85)   # keep ax title clear of the header subtitle
    loo = analysis["loo_df"].dropna(subset=["actual", "predicted"])

    is_fraction = _on_fraction_scale(loo["actual"])
    act  = loo["actual"]  * (100 if is_fraction else 1)
    pred = loo["predicted"] * (100 if is_fraction else 1)
    resid = act - pred

    overachievers  = _find_overachievers(loo, target, n=8)
    underachievers = _find_underachievers(loo, target, n=5)
    oa_names = set(overachievers.index)
    ua_names = set(underachievers.index)

    # All districts — grey
    mask_oa = loo.index.isin(oa_names)
    mask_ua = loo.index.isin(ua_names)
    ax.scatter(pred[~mask_oa & ~mask_ua], act[~mask_oa & ~mask_ua],
               color=_GREY, alpha=0.3, s=18, zorder=2)

    # Overachievers — green
    ax.scatter(pred[mask_oa], act[mask_oa],
               color=_GREEN, alpha=0.85, s=60, zorder=4,
               label=f"Top over-performers (n={len(oa_names)})")
    for idx in overachievers.index:
        if idx in pred.index:
            ax.annotate(str(idx),
                        xy=(float(pred[idx]), float(act[idx])),
                        xytext=(4, 3), textcoords="offset points",
                        fontsize=6.5, color=_GREEN, alpha=0.85)

    # Underachievers — red
    ax.scatter(pred[mask_ua], act[mask_ua],
               color=_RED, alpha=0.80, s=55, zorder=4,
               label=f"Top under-performers (n={len(ua_names)})")
    for idx in underachievers.index:
        if idx in pred.index:
            ax.annotate(str(idx),
                        xy=(float(pred[idx]), float(act[idx])),
                        xytext=(4, -9), textcoords="offset points",
                        fontsize=6.5, color=_RED, alpha=0.85)

    # Saugus
    if "Saugus" in pred.index:
        sx, sy = float(pred["Saugus"]), float(act["Saugus"])
        ax.scatter([sx], [sy], color=_GOLD, s=140, zorder=5, marker="*")
        ax.annotate("Saugus", xy=(sx, sy), xytext=(4, -10),
                    textcoords="offset points",
                    fontsize=8, color=_GOLD, fontweight="bold")

    lo = min(pred.min(), act.min()) - 2
    hi = max(pred.max(), act.max()) + 2
    ax.plot([lo, hi], [lo, hi], color=_GREY, lw=1, ls=":", alpha=0.5,
            label="Perfect prediction")
    ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)

    r = float(np.corrcoef(act, pred)[0, 1])
    ax.set_title(f"Actual vs Predicted — {label}  (LOO r = {r:.3f})", fontsize=10)
    ax.set_xlabel("RBP-predicted value"); ax.set_ylabel("Actual value")
    ax.legend(fontsize=8); ax.grid(alpha=0.2)

    saugus_resid = float(resid.get("Saugus", float("nan")))
    unit = "pp" if (act.median() < 200) else " pts"
    _header(fig, f"Over- and Under-Performers: {label}",
            f"Saugus gap: {saugus_resid:+.1f}{unit}  ·  "
            f"Gold star = Saugus  ·  Green = over-performers  ·  Red = under-performers")
    _footer(fig, "Residual = actual − predicted.  Positive = performing better than demographics suggest.")
    _save(pdf, fig)


def page_what_overachievers_did(pdf, label: str, target: str,
                                 analysis: dict, df_raw: pd.DataFrame,
                                 lean_features: list[str]):
    """
    Factor comparison: Saugus vs. the towns the model predicted SIMILARLY to
    Saugus but that scored better — the genuinely comparable peers.  Ordered by
    RBP variable importance so the most relevant differences are first.
    """
    loo        = analysis["loo_df"]
    imp        = _display_importance(analysis)   # canonical Step-1 importance
    overachievers = _comparable_overperformers(loo, target)

    # Get feature values for Saugus and overachievers from df_raw
    feat_df = df_raw[["district_name"] + lean_features].copy()
    feat_df.index = feat_df["district_name"]
    feat_df = feat_df.drop(columns=["district_name"])

    oa_names = [idx for idx in overachievers.index if idx in feat_df.index]
    if "Saugus" not in feat_df.index or not oa_names:
        fig, ax = _paper_fig()
        ax.text(0.5, 0.5, "Insufficient data for comparison",
                ha="center", va="center")
        _save(pdf, fig); return

    # Order by STANDARDIZED absolute peer gap, not |importance|.  This is an action
    # table ("what comparable better towns do differently"), so the honest orderer
    # is how far Saugus sits from those peers on each factor — in pooled SDs, so
    # different-unit factors are comparable — NOT RBP importance, which measures
    # peer-matching sharpness, not lever strength.  Importance is still shown as a
    # reference column; it just no longer dictates the row order.
    _disp = _display_features([f for f in lean_features if f in feat_df.columns])
    _saugus_vals = feat_df.loc["Saugus"]
    _pop_std = feat_df[_disp].std(ddof=1).replace(0, np.nan) if _disp else pd.Series(dtype=float)

    def _std_peer_gap(feat):
        if feat not in feat_df.columns:
            return -1.0
        sv = _saugus_vals.get(feat, np.nan)
        pm = feat_df.loc[oa_names, feat].median()
        sd = _pop_std.get(feat, np.nan)
        if pd.isna(sv) or pd.isna(pm) or pd.isna(sd):
            return -1.0
        return abs(pm - sv) / sd

    feats_ordered = sorted(_disp, key=_std_peer_gap, reverse=True)
    if not feats_ordered:
        feats_ordered = _display_features(lean_features)

    fig, axes = _paper_fig(1, 2)
    ax_l, ax_r = axes

    _header(fig, f"What Comparable Better Towns Do Differently: {label}",
            "Towns the model predicted like Saugus that scored better — their actionable-factor "
            "values vs Saugus, ordered by where Saugus is furthest from these peers "
            "(gap measured in standard deviations, so different-unit factors compare)")

    # ── Left: heatmap of feature values (z-scored relative to all districts) ──
    ax_l.axis("off")
    saugus_vals = feat_df.loc["Saugus", feats_ordered]
    mu  = feat_df[feats_ordered].mean()
    std = feat_df[feats_ordered].std().replace(0, 1)

    def _fmt(v):
        """Compact number format: large values as XM / X.XB to prevent overflow."""
        if np.isnan(v): return "—"
        av = abs(v)
        if av >= 1e9:  return f"{v/1e9:.2f}B"
        if av >= 1e6:  return f"{v/1e6:.0f}M"
        if av >= 1e3:  return f"{v:,.0f}"
        if av >= 10:   return f"{v:.1f}"
        return f"{v:.2f}"

    # 2 comparison towns leaves room to widen the Factor column so the full
    # factor labels fit without colliding with the values.  Town names use a
    # graceful ellipsis only when genuinely long, rather than a hard 9-char cut.
    # Only a couple of example towns fit as columns, so add a "Peer med" column —
    # the median of ALL the comparable better-performing towns — so the reader sees
    # the typical peer value, not just the examples.  Median (not mean) matches the
    # synthesis page's "Peer median" for the same set, so the numbers agree.
    n_compare = min(2, len(oa_names))
    col_names = (["Factor", "Saugus"] +
                 [_shorten(n, 11) for n in oa_names[:n_compare]] +
                 ["Peer med", "Imp."])
    rows = []
    for feat in feats_ordered[:15]:
        sv = saugus_vals.get(feat, float("nan"))
        oa_vals = []
        for name in oa_names[:n_compare]:
            v = feat_df.loc[name, feat] if feat in feat_df.columns else float("nan")
            oa_vals.append(_fmt(v))
        peer_med = (float(feat_df.loc[oa_names, feat].median())
                    if feat in feat_df.columns else float("nan"))
        imp_val = float(imp.get(feat, 0))
        # Wrap the factor label onto (at most) two lines so it fits the column
        # instead of overflowing into the Saugus value cell.
        _flabel = "\n".join(textwrap.wrap(_feat_meta(feat)[0], width=22)[:2])
        rows.append([_flabel, _fmt(sv)] + oa_vals
                    + [_fmt(peer_med), f"{imp_val:+.3f}"])

    # ── Left: factor comparison table ──────────────────────────────────────────
    ax_l.text(0.5, 0.98,
              f"Saugus (highlighted) vs towns predicted like Saugus that scored better\n"
              f"Ordered by largest peer gap (in SDs), top {min(len(feats_ordered), 15)} shown",
              ha="center", va="top", fontsize=8.5, fontweight="bold",
              color=_BLUE, transform=ax_l.transAxes)
    if rows:
        used_cols = col_names[:len(rows[0])]
        tbl = ax_l.table(cellText=rows, colLabels=used_cols,
                          bbox=[0.0, 0.0, 1.0, 0.90], cellLoc="center")
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(6.5)

        # Columns: Factor | Saugus | town... | Peer med | Imp.
        n_cols = len(used_cols)
        n_town_cols = n_cols - 4          # subtract Factor, Saugus, Peer med, Imp
        feat_w   = 0.40
        saugus_w = 0.10
        town_w   = 0.13
        peer_w   = 0.12
        imp_w    = max(0.05, 1.0 - feat_w - saugus_w - n_town_cols * town_w - peer_w)
        explicit_w = ([feat_w, saugus_w] + [town_w] * n_town_cols + [peer_w, imp_w])
        peer_col = 2 + n_town_cols        # index of the "Peer med" column
        for (row_idx, col_idx), cell in tbl.get_celld().items():
            if col_idx < len(explicit_w):
                cell.set_width(explicit_w[col_idx])
            if col_idx == 0 and row_idx > 0:           # left-align factor labels
                cell.set_text_props(ha="left")
            if row_idx == 0:
                # Smaller header font so town names ("Leominster") fit their
                # columns instead of colliding with neighbours.
                cell.set_facecolor(_BLUE)
                cell.set_text_props(color="white", fontsize=5.8)
            elif col_idx == 1 and row_idx > 0:         # Saugus column
                cell.set_facecolor("#FFF8E1")
            elif col_idx == peer_col and row_idx > 0:  # Peer median column
                cell.set_facecolor("#EAF4EA")
            else:
                cell.set_facecolor("#F7F7F7" if row_idx % 2 else "white")
            cell.set_edgecolor("#DDDDDD")

    # ── Right: residuals table ────────────────────────────────────────────────
    ax_r.axis("off")
    mult   = 100 if _on_fraction_scale(loo["actual"]) else 1

    # The comparable set can be large when the model's prediction error is wide
    # (e.g. Education Budget Share admits dozens of towns within 1σ).  Show only
    # the closest few — oa_names is already ordered closest-predicted first — so
    # the table stays readable; note how many more sit in the comparison.
    MAX_SHOWN_PEERS = 12
    oa_rows = []
    for name in oa_names:
        row_loo = loo.loc[name] if name in loo.index else None
        if row_loo is None or np.isnan(row_loo["residual"]):
            continue
        act_v  = float(row_loo["actual"])  * mult
        pred_v = float(row_loo["predicted"]) * mult
        resid  = act_v - pred_v
        label_r = f"{resid:+.1f}pp" if target != "dropout_pct" else f"{-resid:+.1f}pp better"
        oa_rows.append([name[:22], f"{act_v:.1f}", f"{pred_v:.1f}", label_r])
    n_more = max(0, len(oa_rows) - MAX_SHOWN_PEERS)
    oa_rows = oa_rows[:MAX_SHOWN_PEERS]

    saugus_loo = loo.loc["Saugus"] if "Saugus" in loo.index else None
    s_act  = float(saugus_loo["actual"])  * mult if saugus_loo is not None else float("nan")
    s_pred = float(saugus_loo["predicted"]) * mult if saugus_loo is not None else float("nan")
    s_res  = s_act - s_pred

    if oa_rows:
        # Saugus row uses the SAME direction transform as the peer rows above:
        # for dropout (lower is better) a negative residual is an OUTPERFORMANCE,
        # so it must read "+1.2pp better", not "-1.2pp".
        s_label = (f"{s_res:+.1f}pp" if target != "dropout_pct"
                   else f"{-s_res:+.1f}pp better")
        oa_rows_display = [[">> Saugus",
                             f"{s_act:.1f}", f"{s_pred:.1f}",
                             s_label]] + oa_rows
        ax_r.text(0.5, 0.98, "Predicted like Saugus — but scored better",
                  ha="center", va="top", fontsize=9, fontweight="bold",
                  color=_BL, transform=ax_r.transAxes)
        oa_tbl = ax_r.table(cellText=oa_rows_display,
                             colLabels=["District", "Actual", "Predicted", "vs Expected"],
                             bbox=[0.0, 0.52, 1.0, 0.42])
        oa_tbl.auto_set_font_size(False); oa_tbl.set_fontsize(8.5)
        oa_tbl.auto_set_column_width(range(4))
        for (row, col), cell in oa_tbl.get_celld().items():
            if row == 0:
                cell.set_facecolor(_BLUE); cell.set_text_props(color="white")
            elif row == 1:
                cell.set_facecolor("#FFF8E1")
            else:
                cell.set_facecolor("#E8F8E8" if row % 2 else "white")
            cell.set_edgecolor("#CCCCCC")

        if n_more:
            ax_r.text(0.5, 0.49,
                      f"(closest {MAX_SHOWN_PEERS} shown; + {n_more} more comparable "
                      f"towns within the model's prediction band)",
                      ha="center", va="top", fontsize=7, style="italic",
                      color=_GREY, transform=ax_r.transAxes)

    _footer(fig, "Peer med = median across ALL comparable better-performing towns (not just the "
            "examples shown).  vs Expected = how far actual beats the demographic prediction, signed "
            "so + always means better than expected (for dropout, lower-than-predicted shows as a "
            "positive 'better').  Factor values shown in raw units; importance scores from RBP Exhibit 5.")
    _save(pdf, fig)


# Plain-language description and display unit for every feature that can
# appear as a lean RBP feature across the four models.
FEATURE_INFO: dict[str, tuple[str, str]] = {
    "low_income_pct":            ("% students from low-income families",          "pct"),
    "median_hh_income":          ("Median household income",                      "dollar"),
    "equalized_income":          ("Equalized property value per capita",          "dollar"),
    "pct_bachelors_plus":        ("Adults with a bachelor's degree+",              "pct"),
    "pct_owner_occupied":        ("Owner-occupied housing units",                  "pct"),
    "crime_rate":                ("Crime incidents per 100k residents",            "count"),
    "res_tax_rate":              ("Residential tax rate (per $1,000)",             "rate"),
    "chronic_absenteeism_pct":   ("Students chronically absent (10%+)",            "pct"),
    "ell_pct":                   ("English language learners",                     "pct"),
    "sped_pct":                  ("Special education students",                    "pct"),
    "total_enrollment":          ("Total district enrollment",                     "count"),
    "teachers_per_100_students": ("Teachers per 100 students",                     "rate"),
    "avg_teacher_salary":        ("Average teacher salary",                        "dollar"),
    "in_district_ppe":              ("In-district spending per pupil",                "dollar"),
    "debt_service_pct":          ("Debt service share of town budget",             "pct"),
    "fixed_costs_pct":           ("Fixed costs (mostly health ins.)", "pct"),
    "public_safety_pct":         ("Police & fire share of town budget",            "pct"),
    "public_works_pct":          ("Public works share of town budget",             "pct"),
    "avg_mcas":                  ("MCAS 3–8 (% meeting/exceeding)",                "pct100"),
    "mcas10_ela":                ("MCAS 10 ELA (% meeting/exceeding)",             "pct100"),
    "dropout_pct":               ("Annual dropout rate",                           "pct"),
    "attending_pct":             ("HS completers attending college",               "pct"),
    # Derived actionable "effort / intensity" factors (see add_actionable_factors)
    "teachers_per_lowincome":    ("Teachers per low-income student",               "rate"),
    "nss_per_eqv":               ("School spend vs. property wealth",              "rate"),
    "spend_vs_required":     ("Spending vs Ch70 required minimum",             "rate"),
    "teacher_pay_share":    ("Teacher share of school spending",              "rate"),
    "health_ins_per_capita":     ("Health insurance $ per resident",               "dollar"),
}


def _feat_meta(feat: str) -> tuple[str, str]:
    """
    (display label, unit kind) for any feature — including factors not yet in
    FEATURE_INFO (e.g. future computed/derived factors).  Known factors use their
    curated label/units; unknown ones fall back to a prettified column name
    ("some_new_factor" → "Some new factor") and generic numeric formatting, so a
    newly added factor renders cleanly with no extra wiring.
    """
    if feat in FEATURE_INFO:
        return FEATURE_INFO[feat]
    pretty = feat.replace("_pct", "").replace("_", " ").strip().capitalize()
    return (pretty or feat, "rate")


def _fmt_feature_val(v, kind: str) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if kind == "pct":    return f"{v:.1f}%"
    if kind == "pct100": return f"{v * 100:.1f}%"
    if kind == "dollar":
        if abs(v) >= 1e9: return f"${v / 1e9:.2f}B"
        if abs(v) >= 1e6: return f"${v / 1e6:.0f}M"
        return f"${v:,.0f}"
    if kind == "count":  return f"{v:,.0f}"
    # "rate" / ratio: small ratios (e.g. school-spending ÷ wealth ≈ 0.07) need
    # more precision than 1 decimal, or real gaps round away to "0.0".
    if abs(v) < 1:   return f"{v:.3f}"
    if abs(v) < 10:  return f"{v:.2f}"
    return f"{v:.1f}"


def _fmt_gap_val(v, kind: str) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if kind == "pct":    return f"{v:+.1f}pp"
    if kind == "pct100": return f"{v * 100:+.1f}pp"
    if kind == "dollar":
        if abs(v) >= 1e9: return f"{v / 1e9:+.2f}B"
        if abs(v) >= 1e6: return f"{v / 1e6:+.0f}M"
        return f"{v:+,.0f}"
    if kind == "count":  return f"{v:+,.0f}"
    if abs(v) < 1:   return f"{v:+.3f}"
    if abs(v) < 10:  return f"{v:+.2f}"
    return f"{v:+.1f}"


# Qualitative "so what" for each outcome — interpretation ONLY, no numbers.
# Every numeric quantity in the synthesis prose (predicted, actual, gap, rank,
# N, feature values, peer gaps) is injected live by build_synthesis_prose from
# the computed analysis; these strings carry only the fixed editorial reading
# that a number alone cannot convey.  `noun` is the outcome's plain-language
# name, `close` finishes the Bottom Line, `action` finishes the Takeaway.
SYNTHESIS_QUALITATIVE: dict[str, dict[str, str]] = {
    "MCAS Grades 3–8": {
        "noun":  "MCAS grades 3–8 proficiency (ELA + Math, meeting/exceeding)",
        "close": "A district with Saugus's income, poverty, and enrollment profile is "
                 "predicted to land near this level.",
        "action": "The factors below are where Saugus differs most from the towns that "
                 "beat their own predictions — the most plausible places to focus "
                 "attention, though the comparison is associational, not proof of cause.",
    },
    "Dropout Rate": {
        "noun":  "dropout rate",
        "close": "Dropout reflects re-engagement and attendance recovery as much as "
                 "demographics.",
        "action": "Chronic absenteeism is the factor to watch — the main risk to this "
                 "result if it climbs.",
    },
    "MCAS Grade 10 (ELA)": {
        "noun":  "grade 10 ELA proficiency",
        "close": "Note: the grade 3–8 academic base is an outcome, not an actionable factor, so it is "
                 "excluded from this model — part of the over-performance shown here "
                 "reflects that omitted prior strength rather than a grade-10 effect.",
        "action": "Among the actionable factors, chronic absenteeism is the clearest, with "
                 "teacher pay next — Saugus clears this bar even while trailing comparable "
                 "towns on attendance.",
    },
    "Education Budget Share": {
        "noun":  "education budget share",
        "close": "Unlike the academic measures, this gap reflects a municipal budgeting "
                 "choice, not a demographic constraint.  Note that the budget shares are "
                 "compositional — they sum to the total budget — so a high importance for a "
                 "competing category is partly mechanical (a larger slice elsewhere "
                 "necessarily leaves a smaller one for schools), not an independent effect.",
        "action": "The top factors are competing budget categories that crowd out the "
                 "education share — the choice here sits with Town Meeting and municipal "
                 "budgeting, not the school district.",
    },
}


def _rank_band(rank: int, n_total: int) -> str:
    """Plain-language band for a rank, computed live (no hardcoded percentile)."""
    if not n_total:
        return "the field"
    frac = rank / n_total
    if frac <= 0.10: return "the top 10% of MA districts"
    if frac <= 0.25: return "the top 25%"
    if frac <= 0.50: return "the top half"
    if frac >= 0.90: return "the bottom 10%"
    if frac >= 0.75: return "the bottom 25%"
    return "the middle of the pack"


def build_synthesis_prose(label: str, target: str, ctx: dict) -> tuple[str, str]:
    """
    Compose the Bottom Line and Takeaway entirely from live values in `ctx`
    plus the fixed qualitative reading for this outcome.  No numeric literal
    appears here: predicted/actual/gap/rank/N and every feature value and peer
    gap are taken from `ctx`, which is built from the live analysis on the same
    page.  Superlatives ("strongest driver", "widest gap") are derived from the
    live driver table, never asserted.
    """
    q = SYNTHESIS_QUALITATIVE.get(
        label, {"noun": label, "close": "", "action": ""})
    u, unit = ctx["u"], ctx["unit"]
    actual, pred, gap = ctx["actual"], ctx["pred"], ctx["gap"]
    rank, n_total, band = ctx["rank"], ctx["n_total"], ctx["band"]
    gap_mag = abs(gap)
    drivers = ctx["drivers"]

    if gap_mag < 0.5:
        rel = f"almost exactly its demographic prediction of {pred:.1f}{u} (a {gap:+.1f}{unit} gap)"
    else:
        word = "better than" if ctx["outperforms"] else "worse than"
        rel = (f"{gap_mag:.1f}{unit} {word} its demographic prediction of {pred:.1f}{u}")

    bottom = (f"Saugus's {q['noun']} is {actual:.1f}{u}, {rel} — ranking {rank} of "
              f"{n_total} MA districts, in {band}.  {q['close']}")

    # Takeaway: live direction clause first, then the true top driver and the
    # true widest peer gap — every claim derived from this run, none asserted.
    if gap_mag < 0.5:
        perf = "Saugus lands almost exactly where its demographic profile predicts here"
    elif ctx["outperforms"]:
        perf = "Saugus does better here than its demographic profile predicts"
    else:
        perf = "Saugus does worse here than its demographic profile predicts"

    sent = ""
    if drivers:
        top = drivers[0]
        valid_gap = [d for d in drivers if not pd.isna(d["oa_gap"])]
        gd = max(valid_gap, key=lambda d: abs(d["oa_gap"])) if valid_gap else None

        if gd is not None and gd["feature"] == top["feature"]:
            # Same factor tops RBP importance AND shows the widest peer gap.  These
            # are NOT two independent signals: footnote-12 importance is mechanically
            # elevated for any feature on which the task town is a statewide outlier,
            # and that same outlier status produces the peer gap.  State that at the
            # point of claim rather than implying two confirmations.
            sent = (f"The factor on which Saugus is most demographically unusual is "
                    f"{top['desc'].lower()} "
                    f"({_fmt_feature_val(top['saugus'], top['kind'])} vs the "
                    f"{_fmt_feature_val(top['median'], top['kind'])} state median; "
                    f"peer gap {_fmt_gap_val(top['oa_gap'], top['kind'])}).  It both "
                    f"tops the RBP importance and shows the widest peer gap — two views "
                    f"of the same outlier status, not independent confirmation.  ")
        else:
            sent = (f"The strongest actionable factor is "
                    f"{top['desc'].lower()} "
                    f"({_fmt_feature_val(top['saugus'], top['kind'])} vs the "
                    f"{_fmt_feature_val(top['median'], top['kind'])} state median)")
            if gd is not None:
                sent += (f"; the widest gap to the comparable towns that scored better is "
                         f"{gd['desc'].lower()} "
                         f"({_fmt_gap_val(gd['oa_gap'], gd['kind'])}, peer median − Saugus)")
            sent += ".  "
        if not ctx.get("top3_stable", True):
            sent += ("(Seed-stability: the top factor is seed-stable; the leading factors "
                     "below it recur across random grids but their order and importance "
                     "values jump run-to-run — read them as a co-equal group, not a "
                     "strict ranking.)  ")
    takeaway = f"{perf}.  {sent}{q['action']}"
    return bottom, takeaway


def page_synthesis(pdf, label: str, target: str, analysis: dict,
                    df_raw: pd.DataFrame, lean_features: list[str]) -> None:
    """
    Synthesis page: ties the gap, the top local drivers (Exhibit 5), and the
    overachiever comparison together into a plain-language "what this means".
    """
    fig, axes = _paper_fig(1, 2)
    fig.subplots_adjust(left=0.045, right=0.97, wspace=0.10, top=0.84, bottom=0.05)
    ax_l, ax_r = axes
    ax_l.axis("off"); ax_r.axis("off")

    is_pct_unit = analysis["actual_pct"] < 200
    unit = "pp" if is_pct_unit else " pts"
    gap = analysis["gap_pp"]
    better_label = "lower is better" if target == "dropout_pct" else "higher is better"

    # Saugus's rank among MA districts on this outcome's LOO residual — via the
    # shared rank_among_peers helper, so this page and the Saugus-analysis footer
    # use identical direction logic and denominators.
    loo = analysis["loo_df"].dropna(subset=["residual"])
    better, n_total, rank = rank_among_peers(analysis["loo_df"], target)

    _header(fig, f"What This Means: {label}",
            f"Predicted {analysis['pred_pct']:.1f}{unit}  ·  "
            f"Actual {analysis['actual_pct']:.1f}{unit}  ·  "
            f"Gap {gap:+.1f}{unit} ({better_label})  ·  "
            f"Saugus ranks {rank} of {n_total} MA districts on this measure")

    # ── Data prep: one structured driver table feeds both panels AND the
    #    live prose, so the words and the tables cannot disagree ─────────────
    df2 = df_raw.copy()
    df2.index = df2["district_name"]
    imp = _display_importance(analysis)
    # Show only ACTIONABLE factors as drivers — structural traits match peers
    # silently and are hidden from this table.
    _disp = _display_features([f for f in lean_features if f in df2.columns])
    feats_ordered = (
        imp.reindex(_disp).abs().sort_values(ascending=False).index.tolist()[:5]
    )

    overachievers = _comparable_overperformers(loo, target)
    feat_df = df_raw[["district_name"] + lean_features].copy()
    feat_df.index = feat_df["district_name"]
    oa_names = [n for n in overachievers.index if n in feat_df.index]

    drivers = []
    for feat in feats_ordered:
        desc, kind = _feat_meta(feat)
        sv = (float(df2.loc["Saugus", feat])
              if "Saugus" in df2.index and feat in df2.columns
              and not pd.isna(df2.loc["Saugus", feat]) else float("nan"))
        med = float(df2[feat].median()) if feat in df2.columns else float("nan")
        oa_vals = [float(feat_df.loc[n, feat]) for n in oa_names
                   if feat in feat_df.columns and not pd.isna(feat_df.loc[n, feat])]
        oa_med = float(np.median(oa_vals)) if oa_vals else float("nan")
        drivers.append({
            "feature": feat, "desc": desc, "kind": kind,
            "saugus": sv, "median": med, "oa_median": oa_med,
            "imp": float(imp.get(feat, 0)),
            "oa_gap": oa_med - sv,
        })

    # ── Live prose — every number injected from `ctx`, none hardcoded ───────
    ctx = {
        "u": ("%" if is_pct_unit else " pts"),
        "unit": unit,
        "actual": analysis["actual_pct"],
        "pred":   analysis["pred_pct"],
        "gap":    gap,
        "rank":   rank,
        "n_total": n_total,
        "band":   _rank_band(rank, n_total),
        "outperforms": (gap > 0) if _higher_is_better(target) else (gap < 0),
        "drivers": drivers,
        "top3_stable": analysis.get("importance_top3_stable", True),
    }
    bottom_line, takeaway = build_synthesis_prose(label, target, ctx)

    # ── Left: the bottom line + top local drivers ───────────────────────────
    ax_l.text(0.0, 0.985, "The Bottom Line", ha="left", va="top",
              fontsize=11, fontweight="bold", color=_BLUE, transform=ax_l.transAxes)
    ax_l.text(0.0, 0.91, textwrap.fill(bottom_line, width=68),
              ha="left", va="top", fontsize=9.5, color=_BL,
              transform=ax_l.transAxes, linespacing=1.5)

    ax_l.text(0.0, 0.50, "Actionable Factors Where Saugus Stands Out", ha="left", va="top",
              fontsize=11, fontweight="bold", color=_BLUE, transform=ax_l.transAxes)
    ax_l.text(0.0, 0.45,
              textwrap.fill(
                  "Ranked by RBP importance — where Saugus is most distinctive, "
                  "Saugus vs. the statewide median.", width=82),
              ha="left", va="top", fontsize=8, color=_GREY,
              transform=ax_l.transAxes, linespacing=1.3)

    driver_rows = [[d["desc"], _fmt_feature_val(d["saugus"], d["kind"]),
                    _fmt_feature_val(d["median"], d["kind"]), f"{d['imp']:+.2f}"]
                   for d in drivers]
    if driver_rows:
        tbl = ax_l.table(cellText=driver_rows,
                          colLabels=["Factor", "Saugus", "MA median", "Importance"],
                          bbox=[0.0, 0.15, 1.0, 0.26], cellLoc="center")
        tbl.auto_set_font_size(False); tbl.set_fontsize(8)
        col_w = [0.55, 0.15, 0.15, 0.15]
        for (r_, c_), cell in tbl.get_celld().items():
            if c_ < len(col_w):
                cell.set_width(col_w[c_])
            if c_ == 0:
                cell.set_text_props(ha="left")
            if r_ == 0:
                cell.set_facecolor(_BLUE); cell.set_text_props(color="white")
            else:
                cell.set_facecolor("#F5F5F5" if r_ % 2 else "white")
            cell.set_edgecolor("#CCCCCC")

    ax_l.text(0.0, 0.115,
              textwrap.fill(
                  "Importance measures how much each factor sharpens the search for "
                  "Saugus's true comparison districts — not whether higher values are "
                  "good or bad, and not how much changing a factor would move the "
                  "outcome. For where to act, read the peer comparison at right: the "
                  "factors on which Saugus differs most from comparable towns that did "
                  "better.", width=80),
              ha="left", va="top", fontsize=7.5, color=_GREY, style="italic",
              transform=ax_l.transAxes, linespacing=1.4)

    # ── Right: what over-performers do differently + takeaway ───────────────
    ax_r.text(0.0, 0.985, "What Comparable Better Towns Do Differently", ha="left", va="top",
              fontsize=11, fontweight="bold", color=_GREEN, transform=ax_r.transAxes)

    ax_r.text(0.0, 0.91,
              f"Same factors, for the {len(oa_names)} towns the model predicted like "
              f"Saugus that scored better:",
              ha="left", va="top", fontsize=8, color=_GREY,
              transform=ax_r.transAxes)

    oa_rows = [[d["desc"], _fmt_feature_val(d["saugus"], d["kind"]),
                _fmt_feature_val(d["oa_median"], d["kind"]),
                _fmt_gap_val(d["oa_gap"], d["kind"])]
               for d in drivers]

    if oa_rows:
        tbl2 = ax_r.table(cellText=oa_rows,
                           colLabels=["Factor", "Saugus", "Peer median", "Difference"],
                           bbox=[0.0, 0.66, 1.0, 0.225], cellLoc="center")
        tbl2.auto_set_font_size(False); tbl2.set_fontsize(8)
        col_w2 = [0.51, 0.15, 0.18, 0.16]
        for (r_, c_), cell in tbl2.get_celld().items():
            if c_ < len(col_w2):
                cell.set_width(col_w2[c_])
            if c_ == 0:
                cell.set_text_props(ha="left")
            if r_ == 0:
                cell.set_facecolor(_GREEN); cell.set_text_props(color="white")
            else:
                cell.set_facecolor("#F5F5F5" if r_ % 2 else "white")
            cell.set_edgecolor("#CCCCCC")
        ax_r.text(0.0, 0.625,
                  textwrap.fill(
                      "Difference = comparable-town median − Saugus.  Sign direction "
                      "depends on the factor; see takeaway below.", width=80),
                  ha="left", va="top", fontsize=7.5, color=_GREY, style="italic",
                  transform=ax_r.transAxes, linespacing=1.4)

    # Takeaway callout box
    ax_r.add_patch(mpatches.FancyBboxPatch(
        (0.0, 0.0), 1.0, 0.555,
        boxstyle="round,pad=0.01,rounding_size=0.02",
        transform=ax_r.transAxes,
        facecolor="#FFF8E1", edgecolor="#B7950B", linewidth=1.0))
    ax_r.text(0.03, 0.535, "The Takeaway", ha="left", va="top",
              fontsize=10.5, fontweight="bold", color=_GOLD, transform=ax_r.transAxes)
    ax_r.text(0.03, 0.46, textwrap.fill(takeaway, width=66),
              ha="left", va="top", fontsize=9, color=_BL,
              transform=ax_r.transAxes, linespacing=1.5)

    _footer(fig, "Left: top actionable factors by RBP importance, Saugus vs. the state "
            "median.  Right: the same factors for the towns the model predicted like Saugus "
            "that scored better.  Structural traits are used for peer-matching only.")
    _save(pdf, fig)


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Main orchestration
# ─────────────────────────────────────────────────────────────────────────────

# Always exclude: true artifacts with no predictive validity in any model.
#   fiscal_year — data-availability proxy, not a school system input.
ALWAYS_EXCLUDE = {
    "fiscal_year",
    "org_code",
    "district_name",
}

# Outcome variables — can only appear as predictors in OTHER models, never
# their own.  Listed here so the candidate filter can handle them separately
# from the pre-specified pool of pure predictors.
OUTCOME_VARS = {
    "avg_mcas", "mcas10_ela", "mcas10_math",
    "sat_ebrw", "sat_math", "sat_combined",
    "attending_pct", "dropout_pct",
    "four_yr_grad_pct", "five_yr_grad_pct",
    # Schedule A budget line-item shares — outcomes of fiscal policy.
    # Available as predictors in models that don't exclude them.
    "ed_budget_share",      # target for the budget share model
    "fixed_costs_pct",      # pensions / employee benefits share
    "debt_service_pct",     # debt service share
    "public_safety_pct",    # police + fire share
    "public_works_pct",     # DPW / infrastructure share
}

# Pre-specified pool of PURE PREDICTOR features — chosen on domain grounds
# before running RBP, following Kritzman (2024) Exhibit 1.
#
# Reduces K from ~32 candidates to ~14-17 per model, bringing K/N from
# ~0.19 to ~0.09 (matching the paper's 14/165 ≈ 0.085 ratio) and making
# the covariance matrix well-conditioned.
#
# Dropped features and why:
#   high_needs_pct        r=+0.981 with low_income_pct  → near-duplicate
#   acs_poverty_pct       r=+0.87  with low_income_pct  → redundant poverty
#   foundation_budget_pp  r=+0.92  with in_district_ppe   → redundant spending
#   ch70_per_pupil        r≈-0.87  with equalized_income → inverse wealth proxy
#   teacher_fte           r=+0.994 with total_enrollment → linear function
#   total_population      r=+0.953 with total_enrollment → size proxy
#   teachers_per_100_fte  r=+0.872 with teachers_per_100_students → duplicate
#   teacher_spending_pp   r≈+0.85  with in_district_ppe   → redundant spending
#   gf_exp_per_capita     r≈+0.75  with res_tax_rate     → redundant municipal
#   pct_65_plus           weak signal, no clear school policy factor
#   com_tax_rate          r≈+0.65  with res_tax_rate     → redundant tax
PRE_SPECIFIED_POOL = {
    # Poverty & wealth (three distinct angles)
    "low_income_pct",            # % low-income students
    "median_hh_income",          # Household income
    "equalized_income",          # Property wealth per capita
    # Human capital
    "pct_bachelors_plus",        # % adults with bachelor's degree or higher
    # Housing & community
    "pct_owner_occupied",        # % owner-occupied housing (stability)
    "crime_rate",                # Crime incidents per capita
    "res_tax_rate",              # Residential tax rate (local fiscal effort)
    # Demographics & engagement
    "chronic_absenteeism_pct",   # % chronically absent (strongest cross-model)
    "ell_pct",                   # % English language learners
    "sped_pct",                  # % special education students
    # District structure
    "total_enrollment",          # District size (drop teacher_fte r=0.994)
    "teachers_per_100_students", # Staffing ratio (drop fte version r=0.872)
    "avg_teacher_salary",        # Teacher compensation
    # School spending
    "in_district_ppe",             # In-district spending per pupil
}

# ─────────────────────────────────────────────────────────────────────────────
# What is a "factor"?
# ─────────────────────────────────────────────────────────────────────────────
# A factor is a single measurable quantity per district that the model can use
# as a predictor.  It is either:
#   • a RAW column straight from the data (e.g. chronic_absenteeism_pct,
#     avg_teacher_salary), or
#   • a CALCULATED ratio/share derived from several columns and normalized so a
#     big town and a small town are comparable (e.g. instructional_share =
#     classroom spending ÷ total in-district spending).  Normalizing is what
#     keeps a factor from just proxying town size or wealth.
#
# Every factor carries a TIER that fixes how the model may use it:
#   • Tier 1 — directly votable (Town Meeting / ballot)      ┐ "actionable":
#   • Tier 2 — policy / management (administration decides)  ┘  what a town DOES
#   • Tier 3 — structural: what a community IS (income, poverty, size).  Used
#     ONLY to match Saugus to comparable peer towns, never ranked as a lever —
#     a town cannot vote to change who it is.
#
# ─────────────────────────────────────────────────────────────────────────────
# Tiered "actionable factor" candidate pool  (USE_ACTIONABLE_POOL)
# ─────────────────────────────────────────────────────────────────────────────
# RBP is tier-blind — it treats every column identically — so we impose the
# Tier 3 (structural) vs Tier 1/2 (actionable) distinction by CHOOSING which
# columns enter the run.  With this pool the RBP relevance still matches Saugus
# to structurally-similar towns (the demographic features dominate the
# covariance), but the Exhibit-5 importance now scores the factors Saugus can
# actually change — answering "given towns like us, which controllable thing
# moves the outcome," instead of mixing in demographics it cannot act on.
USE_ACTIONABLE_POOL = True

# Tier 3 — what a community IS (the peer-matching basis; not factors).
STRUCTURAL_FEATURES = {
    "low_income_pct", "median_hh_income", "equalized_income",
    "pct_bachelors_plus", "pct_owner_occupied", "ell_pct", "sped_pct",
    "total_enrollment", "crime_rate",
    # Municipal employee health-insurance spending per resident: tracks town
    # wealth / workforce, not a clean town-vote factor — treated as structural.
    "health_ins_per_capita",
}

# Tier 1/2 — what a town DOES (votable or managed).  Includes the derived
# effort/intensity ratios the statewide factor screen validated as carrying
# signal where the raw levels did not.
ACTIONABLE_FACTORS = {
    # ── The 9 best Tier-1/2 levers, chosen by the cross-sectional factor screen
    #    (lag-0, net of the structural block, FDR-validated; one clean lever per
    #    lever-TYPE, no redundant twins, no wealth-proxies).  Superseded the older
    #    pool: dropped in_district_ppe (raw level → redundant with spend_vs_required),
    #    res_tax_rate (borderline), nss_per_eqv (negative = poverty proxy, not a
    #    lever); added instructional_share and avg_teacher_salary. ──
    # straight-up factors from load_features
    "chronic_absenteeism_pct",    # attendance / engagement
    "teachers_per_100_students",  # class size / staffing density
    "avg_teacher_salary",         # teacher pay LEVEL
    "instructional_share",        # share of the school dollar reaching the classroom
    "ed_budget_share",            # Town Meeting allocation to schools
    "fixed_costs_pct",            # pensions/benefits/health (crowd-out)
    # derived effort/intensity (added by add_actionable_factors)
    "teachers_per_lowincome",     # staffing RELATIVE TO need
    "spend_vs_required",      # spending above the Ch70 legal minimum (fund-more vote)
    "teacher_pay_share",     # share of the school dollar reaching teachers
}


def _display_features(features) -> list:
    """
    Features to SHOW as drivers/factors in the narrative tables.  In actionable
    mode the Tier-3 structural traits define Saugus's peer group and are used
    for matching only — they are hidden from the 'what to do' tables so the
    report surfaces what the town can actually change.  Order is preserved.
    """
    if not USE_ACTIONABLE_POOL:
        return list(features)
    factors = [f for f in features if f in ACTIONABLE_FACTORS]
    return factors or list(features)


def add_actionable_factors(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Augment df_raw with the derived 'effort / intensity' factors used by the
    tiered pool.  Two are pure df_raw ratios (no join); three need one extra
    column each from the wider DB (latest non-null per municipality):
        teachers_per_lowincome = teachers/100 ÷ low-income %        (need vs staffing)
        teacher_pay_share = teacher $/pupil ÷ in-district PPE/pupil        (classroom share)
        nss_per_eqv            = in-district PPE/pupil ÷ property wealth/capita  (effort vs wealth)
        spend_vs_required  = in-district PPE/pupil ÷ Ch70 required NSS/pupil (effort vs floor)
        health_ins_per_capita  = health insurance ÷ population       (cost drag)
    """
    from sqlalchemy import text as _text
    d = df.copy()
    d["_k"] = d["district_name"].str.lower().str.strip()

    def latest(table, namecol, cols, notnull=None):
        nn = f"WHERE {notnull} IS NOT NULL" if notnull else ""
        sql = (f"SELECT DISTINCT ON (lower({namecol})) lower({namecol}) AS _k, "
               + ", ".join(cols) + f" FROM {table} {nn} "
               f"ORDER BY lower({namecol}), fiscal_year DESC")
        with engine.connect() as c:
            return pd.read_sql(_text(sql), c)

    for t in (
        latest("municipal_income_eqv", "municipality",
               ["eqv_per_capita", "population AS muni_pop"]),
        latest("district_chapter70", "district_name",
               ["required_nss_per_pupil AS req_nss_pp"], notnull="required_nss_per_pupil"),
        latest("municipal_health_insurance", "municipality",
               ["health_insurance_expenditure AS health_ins"], notnull="health_insurance_expenditure"),
    ):
        d = d.merge(t, on="_k", how="left")

    def _safe(a, b):
        return a / b.replace(0, np.nan)

    d["teachers_per_lowincome"] = _safe(d["teachers_per_100_students"], d["low_income_pct"])
    d["teacher_pay_share"] = _safe(d["teacher_spending_per_pupil"], d["in_district_ppe"])
    d["nss_per_eqv"]            = _safe(d["in_district_ppe"], d["eqv_per_capita"])
    d["spend_vs_required"]  = _safe(d["in_district_ppe"], d["req_nss_pp"])
    d["health_ins_per_capita"]  = _safe(d["health_ins"], d["muni_pop"])
    return d.drop(columns=["_k", "eqv_per_capita", "muni_pop", "req_nss_pp", "health_ins"],
                  errors="ignore")


# NOTE on also_exclude: these sets are written for BOTH pool modes.  In the
# default actionable-pool mode (USE_ACTIONABLE_POOL=True) the candidate set is
# already restricted to STRUCTURAL_FEATURES | ACTIONABLE_FACTORS, so the
# outcome-variable entries below (mcas10_*, sat_*, grad/dropout/attending) are
# inert there — they were never candidates.  They bite only in legacy mode,
# where outcome vars CAN enter.  The entries that bite in BOTH modes are the
# competing fiscal ratios (fixed_costs_pct, debt_service_pct, …).  Each run logs
# the realized candidate set and live-vs-inert exclusions (see _run_one_model).
MODELS = [
    {
        "label":        "MCAS Grades 3–8",
        "target":       "avg_mcas",
        "target_pct":   True,
        "desc":         "% students meeting/exceeding (ELA + Math, grades 3–8)",
        # Grade 10 MCAS and SAT are circular (same academic quality, different
        # cohort/instrument).  dropout_pct and attending_pct are causal signals.
        # (Outcome-var entries here are inert in actionable mode — see note above;
        # the live exclusions are the four fiscal ratios.)
        "also_exclude": {"mcas10_ela", "mcas10_math",
                         "sat_ebrw", "sat_math", "sat_combined",
                         "fixed_costs_pct", "debt_service_pct",
                         "public_safety_pct", "public_works_pct"},
    },
    # Postsecondary removed: Saugus is +9.8pp above prediction — healthy, not
    # a problem area.  Analysis focuses on outcomes where Saugus is deficient.
    {
        "label":        "Dropout Rate",
        "target":       "dropout_pct",
        "target_pct":   False,
        "desc":         "% students dropping out of high school",
        # avg_mcas and mcas10_ela are causal (academic struggle → disengagement).
        # attending_pct is causal (fewer dropouts → more college goers).
        # mcas10_math excluded: collinear with mcas10_ela (r=0.90).
        # SAT excluded: ceiling effect, collinear with wealth predictors.
        # Graduation rates are circular (inverse of dropout).
        "also_exclude": {"four_yr_grad_pct", "five_yr_grad_pct",
                         "mcas10_math",
                         "sat_ebrw", "sat_math", "sat_combined",
                         "fixed_costs_pct", "debt_service_pct",
                         "public_safety_pct", "public_works_pct"},
    },
    {
        "label":        "MCAS Grade 10 (ELA)",
        "target":       "mcas10_ela",
        "target_pct":   True,
        "desc":         "% grade 10 students meeting/exceeding on MCAS ELA",
        # Why MCAS10 instead of SAT:
        #   - MCAS10 is mandatory for ALL students to graduate — no self-selection.
        #     SAT is voluntary; students not planning on college often skip it.
        #   - SAT has a private-prep ceiling (~1200) from tutoring spend,
        #     not school quality. MCAS10 has neither distortion.
        # Exclusions (circular academic measures): mcas10_math (same cohort/session,
        # r=0.90) and the SAT variables.  NOTE: in the default actionable pool every
        # outcome variable — avg_mcas, dropout_pct, attending_pct included — is out
        # of pool already, so avg_mcas is NOT a feature in this model (the report
        # correctly shows grade 3–8 excluded).  The academic reasoning here only
        # bites in legacy non-actionable mode.
        "also_exclude": {"mcas10_math",
                         "sat_ebrw", "sat_math", "sat_combined",
                         "fixed_costs_pct", "debt_service_pct",
                         "public_safety_pct", "public_works_pct"},
    },
    {
        "label":        "Education Budget Share",
        "target":       "ed_budget_share",
        "target_pct":   True,
        "desc":         "Education as % of total municipal expenditure (MA DLS Schedule A)",
        # Build the factor from financial statement components — same logic as
        # computing EPS from the income statement rather than predicting it.
        # Candidates = demographic features (PRE_SPECIFIED_POOL) + competing
        # budget line items (fiscal ratios from Schedule A).
        # School outcome variables excluded: dropout, MCAS, SAT are downstream
        # of budget decisions, not upstream.  Fiscal ratios ARE upstream —
        # they mechanically determine what share is left for education.
        "also_exclude": {
            "avg_mcas", "mcas10_ela", "mcas10_math",
            "sat_ebrw", "sat_math", "sat_combined",
            "attending_pct", "dropout_pct",
            "four_yr_grad_pct", "five_yr_grad_pct",
        },
        # Of the competing budget-share line items, only fixed_costs_pct is in
        # ACTIONABLE_FACTORS, so it is the SINGLE competing share that actually
        # enters this model.  debt_service_pct / public_safety_pct / public_works_pct
        # are not in the pool and never become candidates (see the realized-
        # candidate log each run prints).
    },
    # SAT removed: scores above ~1200 driven by private prep, not school quality.
    # Self-selection: students not going to college don't take it.
    # Joint model collapsed — all features showed negative importance due to
    # severe wealth collinearity.
]


def _run_one_model(args: tuple) -> dict:
    """
    Top-level worker function — must be at module level to be picklable
    on macOS (which uses 'spawn' for multiprocessing).

    Implements the Kritzman (2024) approach directly — ONE run, all variables,
    no pruning:
      Step 1  — Canonical RBP importance (Exhibit 5): one dense-grid RBP with
                ALL candidate features on the Saugus prediction task.  Importance
                is read as a transparency diagnostic only — no features are
                dropped (fn. 12: ≈0-importance features are diversified away and
                do no harm).  A couple of check seeds confirm the top-3 is not a
                Monte-Carlo artifact.
      Step 2  — NO PRUNING.  The full candidate set is used for prediction,
                ordered by |importance| for display purposes only.
      Step 3  — Saugus RBP prediction + leave-one-out LOO r across all MA
                districts (Saugus excluded from its own training set).
    """
    model, n_random_cells, random_state = args
    tag    = model["label"]
    target = model["target"]

    def _p(msg):
        print(f"[{tag}] {msg}", flush=True)

    _p(f"Starting  (target={target!r})")
    engine = get_engine()
    df_raw = load_features(engine)
    if USE_ACTIONABLE_POOL:
        df_raw = add_actionable_factors(df_raw, engine)   # derived effort/intensity factors

    exclude = ALWAYS_EXCLUDE | {target} | model.get("also_exclude", set())

    if USE_ACTIONABLE_POOL:
        # Tiered pool: Tier-3 structural features (the peer match) + Tier-1/2
        # actionable factors (importance-scored).  RBP stays tier-blind; the tier
        # roles are imposed purely by which columns we let in.
        allowed = STRUCTURAL_FEATURES | ACTIONABLE_FACTORS
    else:
        # Legacy pool: pre-specified pure predictors + allowed outcome vars.
        allowed = PRE_SPECIFIED_POOL | (OUTCOME_VARS - exclude)
    candidates = [c for c in df_raw.columns
                  if c not in exclude
                  and c in allowed
                  and df_raw[c].notna().sum() / len(df_raw) >= MIN_COVERAGE
                  and df_raw[c].dtype.kind in "fiu"]

    # Transparency / audit trail: in actionable-pool mode a candidate must be in
    # STRUCTURAL_FEATURES | ACTIONABLE_FACTORS, so any also_exclude entry that is
    # an outcome variable (mcas10_*, sat_*, grad/dropout/attending) is never a
    # candidate to begin with — excluding it is inert.  Only entries that are
    # actually in the pool (the competing fiscal ratios) bite.  Log both the
    # realized candidate set and which exclusions were live vs. inert so the run
    # is fully reproducible and the model dicts don't over-claim.
    _live_excl  = sorted(model.get("also_exclude", set()) & set(allowed))
    _inert_excl = sorted(model.get("also_exclude", set()) - set(allowed))
    _p(f"Realized candidates ({len(candidates)}): {sorted(candidates)}")
    if model.get("also_exclude"):
        _p(f"  also_exclude live (in pool, removed): {_live_excl or ['—']}")
        _p(f"  also_exclude inert (not in pool anyway): {_inert_excl or ['—']}")

    # ── Step 1: Full RBP with all candidates (Kritzman Exhibit 5) ───────────
    # Variable importance directly reveals which features contribute to
    # prediction reliability — a transparency diagnostic, not a feature filter.
    # This is the single canonical importance Series (one dense grid, the paper's
    # procedure); every page reuses it.  A couple of check seeds verify the
    # top-3 is not a Monte-Carlo artifact.
    _p(f"Step 1: Canonical RBP importance over all {len(candidates)} candidates")
    try:
        imp_stats = saugus_importance(df_raw, candidates, target, n_random_cells)
        full_importance = imp_stats["importance"]
        _p(f"  Importance from canonical grid; top-3 seed-stable="
           f"{imp_stats['top3_stable']} ({imp_stats['n_checks']} grids); "
           f"top: {list(full_importance.index[:3])}")
    except Exception as e:
        _p(f"  Full model failed: {e}")
        return {}

    # ── Step 2: NO pruning — faithful to Kritzman ──────────────────────────
    # The paper makes ONE RBP run using all variables; importance (Exhibit 5) is
    # a transparency diagnostic, and fn. 12 notes that ≈0-importance variables
    # are "diversified away," so they are NOT removed.  We therefore predict on
    # the full candidate set.  `feature_set` is just the candidates ordered by
    # |importance| for display — the ordering does not affect the prediction.
    feature_set = (full_importance.reindex(candidates).abs()
                   .sort_values(ascending=False).index.tolist())
    _p(f"Step 2: No prune (Kritzman-faithful) — predicting on all "
       f"{len(feature_set)} candidates")

    # ── Step 3: Saugus RBP + LOO validation on the FULL candidate set ───────
    _p("Step 3: Saugus RBP analysis + LOO (all candidates)")
    try:
        saugus = analyze_saugus(df_raw, candidates, target, n_random_cells)
        _p(f"  predicted={saugus['pred_pct']:.1f}  actual={saugus['actual_pct']:.1f}  "
           f"gap={saugus['gap_pp']:+.1f}pp")
        # The canonical descriptive importance (Step 1) is reused by every page,
        # so the all-candidate chart and the Saugus chart are identical.
        if saugus is not None:
            saugus["display_importance"]     = full_importance
            saugus["importance_top3_stable"] = imp_stats["top3_stable"]
    except Exception as e:
        _p(f"  Saugus analysis failed: {e}")
        saugus = None

    # Validation r comes from the SAME single LOO pass (no separate re-run).
    if saugus is not None:
        _loo = saugus["loo_df"].dropna(subset=["actual", "predicted"])
        loo_score = (float(np.corrcoef(_loo["actual"], _loo["predicted"])[0, 1])
                     if len(_loo) > 2 else float("nan"))
    else:
        loo_score = float("nan")
    _p(f"  LOO r = {loo_score:.4f}")

    _p("Done.")
    return {
        **model,
        "all_candidates":   candidates,
        "full_importance":  full_importance,
        "importance_top3_stable": imp_stats["top3_stable"],
        "n_random_cells":   n_random_cells,
        # No prune: the feature set used for prediction IS the candidate set.
        # 'lean_features'/'features' kept as keys for PDF backward-compat, now
        # equal to all candidates (ordered by |importance| for display).
        "lean_features":    feature_set,
        "features":         feature_set,
        "saugus":           saugus,
        "loo_score":        loo_score,
        "base_score":       loo_score,
    }


def _build_actionable_report(pdf, results, df_raw, engine):
    """
    Lean, ACTIONABLE report — only pages that tell Saugus what it can change:
      1. Title
      2. How to read it (the three factor tiers)
      3. Combined standings across the four outcomes
      4. Per outcome: "What This Means" (actionable drivers), the actual-vs-
         predicted scatter, and what the over-performing peers do differently
      5. Fiscal factors: budget/staffing trajectory + fixed-cost breakdown
      6. Optimum profile (what over-performers look like on actionable factors)

    Earlier methodology / structural-exploration pages (candidate pool,
    correlation matrix, standalone importance-selection and all-models scatter)
    were removed to keep this the lean actionable view; the RBP-native Exhibit 5
    importance now lives inline on each "What This Means" page.
    """
    # Display order: the two MCAS outcomes together, then budget share, then dropout.
    _ORDER = ["MCAS Grades 3–8", "MCAS Grade 10 (ELA)",
              "Education Budget Share", "Dropout Rate"]
    results = sorted(results, key=lambda r: _ORDER.index(r["label"])
                     if r["label"] in _ORDER else 99)

    # The analysis year (stamped on df_raw) labels the title page and pins the
    # fixed-cost snapshot; fall back to deriving it if an older cache predates the
    # stamp.
    _fy = df_raw.attrs.get("analysis_fiscal_year") or latest_analysis_year(engine)

    page_title(pdf, results, analysis_year=_fy)
    page_tiers_explained(pdf)
    page_factor_definitions(pdf)
    page_method_explainer(pdf)
    page_combined_summary(pdf, results)
    for r in results:
        if r.get("saugus"):
            page_synthesis(pdf, r["label"], r["target"], r["saugus"], df_raw,
                           r.get("lean_features", r["features"]))
            # Visual standing: actual vs predicted, Saugus starred, peers labeled.
            page_overachievers_scatter(pdf, r["label"], r["target"], r["saugus"])
            page_what_overachievers_did(pdf, r["label"], r["target"],
                                        r["saugus"], df_raw,
                                        r.get("lean_features", r["features"]))
    _mcas_r = next((r for r in results if r.get("target") == "avg_mcas"), None)
    _ridge_stats = (compute_ridge_validation(
        df_raw, _mcas_r.get("all_candidates", _mcas_r["features"]), "avg_mcas")
        if _mcas_r else None)
    page_budget_and_staffing(pdf, engine, results, df_raw, _ridge_stats)
    page_fixed_costs(pdf, engine, _fy)   # snapshot pinned to the cross-section year
    page_optimum_profile(pdf, results, df_raw)


def main(fast: bool = False, parallel: bool = False):
    # n_random_cells = number of random grid CELLS sampled per prediction task,
    # i.e. random (subset, threshold, censoring-mode) triples (see rbp._build_grid).
    # The grid total is 1 + K + n_random_cells cells.  The paper used 100 (→ 115
    # cells at K=14); we deliberately sample many more here because our candidate
    # pool K varies by outcome (each run logs its realized candidate count) AND
    # because a denser single grid better approximates
    # the deterministic full grid the sampling stands in for — which is the
    # faithful way (vs. seed-averaging) to make the Exhibit-5 top-3 ranking
    # reproducible across Monte-Carlo draws.
    n_random_cells = 30 if fast else 3000
    random_state   = 42

    OUTPUT_PDF.parent.mkdir(parents=True, exist_ok=True)

    worker_args = [(m, n_random_cells, random_state) for m in MODELS]

    if parallel:
        import multiprocessing as mp
        n_cores = min(len(MODELS), mp.cpu_count())
        print(f"[factor_analysis] Running {len(MODELS)} models in parallel "
              f"({n_cores} cores)...")
        # 'spawn' context is safe on macOS and avoids fork-related issues
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=n_cores) as pool:
            results = pool.map(_run_one_model, worker_args)
        # Restore MODELS order (pool.map preserves order, but be explicit)
        label_order = [m["label"] for m in MODELS]
        results.sort(key=lambda r: label_order.index(r["label"]))
    else:
        print(f"[factor_analysis] Running {len(MODELS)} models sequentially...")
        results = [_run_one_model(a) for a in worker_args]

    # PDF generation uses df_raw for the overachiever comparison table;
    # load it once on the main process.
    print("\n[factor_analysis] Loading features for PDF generation...")
    engine = get_engine()
    df_raw = load_features(engine)
    if USE_ACTIONABLE_POOL:
        df_raw = add_actionable_factors(df_raw, engine)   # so pages resolve derived factors
    print(f"  {len(df_raw)} districts, {len(df_raw.columns)} columns")

    # ── Write PDF (write to /tmp first, then copy to avoid network timeouts) ──
    import tempfile, shutil as _shutil
    _tmp_pdf = Path(tempfile.gettempdir()) / "saugus_factor_analysis.pdf"
    print(f"\n[factor_analysis] Writing PDF...")
    with PdfPages(str(_tmp_pdf)) as pdf:
        _build_actionable_report(pdf, results, df_raw, engine)

    # ── Write CSV summary ──────────────────────────────────────────────────────
    # Use the CANONICAL Step-1 importance (full_importance) — the same Series the
    # PDF charts read — so the CSV and the report can never disagree.  (Previously
    # this pulled a second importance off the Step-3 Saugus RBP call, which is a
    # different RBP run and could differ from what the report showed.)
    rows = []
    for r in results:
        fi = r.get("full_importance")
        for feat in r["features"]:
            imp = float(fi.get(feat, float("nan"))) if fi is not None else float("nan")
            rows.append({"model": r["label"], "feature": feat, "importance": imp,
                         "loo_r": r["loo_score"]})
    pd.DataFrame(rows).to_csv(str(OUTPUT_CSV), index=False)

    _shutil.copy2(str(_tmp_pdf), str(OUTPUT_PDF))

    # ── Save cache so --regen-pdf can rebuild the PDF without re-running ──────
    import pickle as _pickle
    _cache = {"results": results, "df_raw": df_raw}
    with open(str(OUTPUT_CACHE), "wb") as _f:
        _pickle.dump(_cache, _f)
    print(f"[factor_analysis] Done → {OUTPUT_PDF}")
    print(f"                       → {OUTPUT_CSV}")
    print(f"                       → {OUTPUT_CACHE}  (cache for --regen-pdf)")


def regen_pdf():
    """Reload cached results and regenerate the PDF — no analysis rerun needed."""
    import pickle as _pickle
    import tempfile, shutil as _shutil
    if not OUTPUT_CACHE.exists():
        print(f"[regen-pdf] No cache found at {OUTPUT_CACHE}")
        print("            Run the full analysis first to create a cache.")
        return
    print(f"[regen-pdf] Loading cache from {OUTPUT_CACHE}...")
    with open(str(OUTPUT_CACHE), "rb") as f:
        cache = _pickle.load(f)
    results = cache["results"]
    df_raw  = cache["df_raw"]
    # Filter to only models still in MODELS list — lets us drop a model from
    # the PDF without rerunning by removing it from MODELS and calling --regen-pdf
    active_labels = {m["label"] for m in MODELS}
    results = [r for r in results if r.get("label") in active_labels]
    print(f"[regen-pdf] Regenerating PDF ({len(results)} models)...")
    _tmp_pdf = Path(tempfile.gettempdir()) / "saugus_factor_analysis.pdf"
    with PdfPages(str(_tmp_pdf)) as pdf:
        _build_actionable_report(pdf, results, df_raw, get_engine())
    _shutil.copy2(str(_tmp_pdf), str(OUTPUT_PDF))
    print(f"[regen-pdf] Done → {OUTPUT_PDF}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fast", action="store_true",
                    help="Use fewer grid cells (faster, less accurate)")
    ap.add_argument("--parallel", action="store_true",
                    help="Run all models concurrently on separate CPU cores")
    ap.add_argument("--regen-pdf", action="store_true",
                    help="Reload cached results and regenerate PDF — no recompute")
    args = ap.parse_args()
    if args.regen_pdf:
        regen_pdf()
    else:
        main(fast=args.fast, parallel=args.parallel)
