"""
What Makes Towns Succeed?  Lessons for Saugus.

Approach: trajectory study, not regression.
  1. Find all MA towns demographically comparable to Saugus in ~2012.
  2. Rank them by improvement on a composite "quality of life" score by 2023.
  3. Show what the "risers" did differently from the "stagnators".
  4. Case-study the most instructive peers (Revere, Woburn, Peabody, Natick).
  5. Show Saugus's own trajectory and flag the levers available.

Run:
    python analysis/peer_trajectory_study.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats as scipy_stats

warnings.filterwarnings("ignore")

from config import get_engine
from sqlalchemy import text

matplotlib.rcParams.update({"pdf.use14corefonts": True,
                             "font.family": "sans-serif"})

_FINAL_PDF = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "Reports", "saugus_peer_improvement_trajectories.pdf"
)
OUTPUT_PDF = "/tmp/saugus_peer_improvement_trajectories.pdf"

SAUGUS = "Saugus"
BASELINE_YEAR  = 2013   # "where were towns?" window
CURRENT_YEAR   = 2023   # "where are they now?"
CASE_STUDIES   = ["Natick", "Revere", "Woburn", "Peabody", "Wakefield", "Braintree"]

CAT = {
    "Education": "#4A90D9",
    "Safety":    "#E05C4A",
    "Community": "#5CB85C",
    "Market":    "#F0AD4E",
    "Fiscal":    "#9B59B6",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_all(engine) -> dict:
    with engine.connect() as conn:
        frames = {}

        frames["mcas"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   subject, meeting_exceeding_pct AS proficient_pct
            FROM mcas_results
            WHERE grade = '10' AND student_group = 'All Students'
              AND subject IN ('ELA','MATH')
              AND school_name = district_name
              AND district_name IS NOT NULL
        """), conn)

        frames["grad"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   four_year_grad_pct AS graduation_rate
            FROM graduation_rates
            WHERE student_group = 'All' AND district_name IS NOT NULL
        """), conn)

        frames["dropout"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   dropout_pct AS dropout_rate
            FROM district_dropout WHERE district_name IS NOT NULL
        """), conn)

        frames["absent"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   AVG(chronic_absenteeism_pct) AS absenteeism_rate
            FROM attendance
            WHERE student_group = 'All' AND district_name IS NOT NULL
            GROUP BY district_name, school_year
        """), conn)

        frames["pp"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   amount AS pp_exp
            FROM per_pupil_expenditure
            WHERE category = 'Total In-District Expenditures'
              AND district_name IS NOT NULL
        """), conn)

        frames["staff"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   SUM(fte) AS teachers_fte
            FROM staffing
            WHERE category ILIKE '%teacher%' AND district_name IS NOT NULL
            GROUP BY district_name, school_year
        """), conn)

        frames["enroll"] = pd.read_sql(text("""
            SELECT district_name AS town, school_year AS year,
                   total AS enrollment
            FROM enrollment
            WHERE grade = 'Total' AND district_name IS NOT NULL
        """), conn)

        frames["ch70"] = pd.read_sql(text("""
            SELECT district_name AS town, fiscal_year AS year,
                   chapter70_aid AS ch70_aid
            FROM district_chapter70 WHERE district_name IS NOT NULL
        """), conn)

        frames["acs"] = pd.read_sql(text("""
            SELECT municipality AS town, acs_year AS year,
                   median_hh_income, poverty_pct, pct_bachelors_plus AS bachelor_pct,
                   total_population, pct_owner_occupied AS owner_occupied_pct,
                   unemployment_rate
            FROM municipal_census_acs WHERE municipality IS NOT NULL
        """), conn)

        frames["zillow"] = pd.read_sql(text("""
            SELECT region_name AS town, data_year AS year,
                   AVG(zhvi) AS zhvi
            FROM municipal_zillow_housing
            WHERE region_name IS NOT NULL AND zhvi IS NOT NULL
            GROUP BY region_name, data_year
        """), conn)

        frames["rev"] = pd.read_sql(text("""
            SELECT municipality AS town, fiscal_year AS year,
                   education AS education_exp, total_expenditures
            FROM municipal_expenditures
            WHERE municipality IS NOT NULL
        """), conn)

        frames["crime"] = pd.read_sql(text("""
            SELECT jurisdiction_name AS town, year,
                   crime_rate_per_100k AS crime_rate,
                   violent_crimes,
                   population AS crime_pop
            FROM municipal_crime WHERE jurisdiction_name IS NOT NULL
        """), conn)

        frames["cpi"] = pd.read_sql(text("""
            SELECT calendar_year AS year, cpi_index FROM cpi_boston_msa
            ORDER BY calendar_year
        """), conn)

    for key, df in frames.items():
        if "town" in df.columns:
            df["town"] = df["town"].str.strip().str.title()
        frames[key] = df

    return frames


def build_town_snapshot(frames: dict, year_window: tuple) -> pd.DataFrame:
    """
    Build a single-row-per-town snapshot averaging over year_window.
    Returns wide DataFrame with all key metrics.
    """
    y_lo, y_hi = year_window

    def avg(df, val_col, year_col="year"):
        return (df[df[year_col].between(y_lo, y_hi)]
                .groupby("town")[val_col].mean().reset_index())

    # MCAS ELA
    ela = frames["mcas"][frames["mcas"]["subject"] == "ELA"]
    snap_ela = avg(ela, "proficient_pct").rename(columns={"proficient_pct": "mcas_ela"})

    # Graduation
    snap_grad = avg(frames["grad"], "graduation_rate")

    # Dropout
    snap_drop = avg(frames["dropout"], "dropout_rate")

    # Absenteeism
    snap_abs = avg(frames["absent"], "absenteeism_rate")

    # Per-pupil (CPI-deflated to latest year)
    cpi = frames["cpi"].set_index("year")["cpi_index"].to_dict()
    base_cpi = cpi.get(max(cpi), 1.0)
    pp = frames["pp"].copy()
    pp["pp_real"] = pp["pp_exp"] * pp["year"].map(lambda y: base_cpi / cpi.get(y, np.nan))
    snap_pp = avg(pp, "pp_real")

    # Teachers per 1k + raw FTE and enrollment for context
    se = frames["staff"].merge(frames["enroll"], on=["town","year"], how="inner")
    se["teachers_per1k"] = se["teachers_fte"] / se["enrollment"].clip(lower=1) * 1000
    snap_tp  = avg(se, "teachers_per1k")
    snap_fte = avg(se, "teachers_fte")
    snap_enr = avg(se, "enrollment")

    # ACS
    snap_acs = (frames["acs"][frames["acs"]["year"].between(y_lo, y_hi)]
                .groupby("town")[["median_hh_income","poverty_pct","bachelor_pct",
                                  "total_population","owner_occupied_pct"]]
                .mean().reset_index())

    # Zillow
    snap_zh = avg(frames["zillow"], "zhvi")

    # Crime — total rate and violent rate per 100k
    snap_crime = avg(frames["crime"], "crime_rate")
    cr = frames["crime"].copy()
    cr["violent_rate"] = cr["violent_crimes"] / cr["crime_pop"].clip(lower=1) * 100_000
    snap_viol = avg(cr, "violent_rate")

    # Ed spending share
    rev = frames["rev"].copy()
    rev["ed_pct"] = rev["education_exp"] / rev["total_expenditures"].clip(lower=1) * 100
    snap_rev = avg(rev, "ed_pct")

    # Merge all
    out = snap_ela
    for df in [snap_grad, snap_drop, snap_abs, snap_pp, snap_tp, snap_fte,
               snap_enr, snap_acs, snap_zh, snap_crime, snap_viol, snap_rev]:
        out = out.merge(df, on="town", how="outer")

    return out


def build_trajectories(frames: dict) -> pd.DataFrame:
    """
    For each town: baseline snapshot (2011-2014) and current snapshot (2021-2024).
    Returns a DataFrame with _base and _curr suffixes, plus change columns.
    """
    base = build_town_snapshot(frames, (2011, 2014)).add_suffix("_base").rename(columns={"town_base": "town"})
    curr = build_town_snapshot(frames, (2021, 2024)).add_suffix("_curr").rename(columns={"town_curr": "town"})
    both = base.merge(curr, on="town", how="inner")

    # Change = current - baseline
    metric_pairs = [
        ("mcas_ela",       True),   # higher is better
        ("graduation_rate",True),
        ("dropout_rate",   False),
        ("absenteeism_rate",False),
        ("pp_real",        True),
        ("teachers_per1k", True),   # kept for display charts; NOT in improvement score
        ("teachers_fte",   True),
        ("enrollment",     True),
        ("median_hh_income",True),
        ("poverty_pct",    False),
        ("zhvi",           True),
        ("crime_rate",     False),
        ("ed_pct",         True),
    ]
    for col, hib in metric_pairs:
        b = f"{col}_base"
        c = f"{col}_curr"
        if b in both.columns and c in both.columns:
            both[f"{col}_chg"] = both[c] - both[b]

    # FTE growth % — actual hiring decisions, immune to enrollment-driven ratio swings
    both["fte_pct_chg"] = (both["teachers_fte_chg"]
                           / both["teachers_fte_base"].replace(0, np.nan) * 100)
    both["enr_pct_chg"] = (both["enrollment_chg"]
                           / both["enrollment_base"].replace(0, np.nan) * 100)

    # Composite improvement score uses fte_pct_chg, not teachers_per1k_chg
    score_metrics = [
        ("mcas_ela_chg",        True),
        ("graduation_rate_chg", True),
        ("dropout_rate_chg",    False),
        ("absenteeism_rate_chg",False),
        ("pp_real_chg",         True),
        ("fte_pct_chg",         True),
        ("median_hh_income_chg",True),
        ("poverty_pct_chg",     False),
        ("zhvi_chg",            True),
        ("crime_rate_chg",      False),
        ("ed_pct_chg",          True),
    ]
    score_cols = []
    for chg, hib in score_metrics:
        if chg in both.columns:
            raw = both[chg].dropna()
            z = (both[chg] - raw.mean()) / (raw.std() + 1e-9)
            score_cols.append(z if hib else -z)
    if score_cols:
        both["improvement_score"] = pd.concat(score_cols, axis=1).mean(axis=1)

    return both


# ─────────────────────────────────────────────────────────────────────────────
# Peer selection: towns with similar 2011-2014 demographics to Saugus
# ─────────────────────────────────────────────────────────────────────────────

def _scale(X: np.ndarray) -> np.ndarray:
    """Z-score each column."""
    mu  = X.mean(axis=0)
    sig = X.std(axis=0)
    sig[sig == 0] = 1.0
    return (X - mu) / sig


def find_saugus_peers(traj: pd.DataFrame, n_mahal: int = 30) -> tuple[list[str], list[str], list[str]]:
    """
    Two-method peer selection mirroring the main Municipal Finance Report:

      Method 1 — Mahalanobis distance
        Accounts for correlations between features (income/poverty ~-0.7 in MA).
        Covariance matrix is ridge-regularised (1e-6 * I) before inversion to
        avoid numerical issues from the large scale differences across features
        (income ~$76k vs percentages ~0-100).
        Returns the n_mahal closest towns to Saugus in the feature space.

      Method 2 — Ward hierarchical clustering
        Independently groups all towns using the same z-scored features.
        k=30 is used: the MA town dataset has a strong 2-cluster structure
        (urban/rural), which is too coarse for peer selection. k=30 gives
        sub-cohort granularity (~14 towns), separating inner-ring suburbs
        from small rural towns that match socioeconomically but differ in scale.
        Returns all towns in Saugus's natural cluster (excluding Saugus).

      Consensus = intersection of both
        Towns that appear in BOTH the Mahalanobis top-n AND Saugus's Ward
        cluster. This double-confirmation makes the group robust: a town
        must be statistically close to Saugus AND independently grouped with
        it by a completely different algorithm.

    Returns (mahal_peers, ward_peers, consensus_peers).
    """
    from scipy.spatial.distance import mahalanobis
    from scipy.cluster.hierarchy import linkage as hc_linkage, fcluster

    candidate_cols = [
        "median_hh_income_base", "poverty_pct_base", "total_population_base",
        "bachelor_pct_base", "pp_real_base", "dropout_rate_base",
        "teachers_per1k_base", "violent_rate_base",
    ]
    saugus_full = traj[traj["town"] == SAUGUS]
    if saugus_full.empty:
        return [], [], []
    avail = [c for c in candidate_cols
             if c in traj.columns and not pd.isna(saugus_full[c].values[0])]
    if not avail:
        return [], [], []

    sub = traj[["town"] + avail].dropna()
    saugus = sub[sub["town"] == SAUGUS]
    if saugus.empty:
        return [], [], []

    all_towns = sub.copy().reset_index(drop=True)
    others    = all_towns[all_towns["town"] != SAUGUS].copy().reset_index(drop=True)
    X_all  = all_towns[avail].values
    X_oth  = others[avail].values
    s      = saugus[avail].values[0]

    # ── Method 1: Mahalanobis ────────────────────────────────────────────────
    cov     = np.cov(X_oth.T)
    inv_cov = np.linalg.pinv(cov + 1e-6 * np.eye(cov.shape[0]))
    dists   = np.array([mahalanobis(row, s, inv_cov) for row in X_oth])
    others_m = others.copy()
    others_m["dist"] = dists
    mahal_peers = others_m.nsmallest(n_mahal, "dist")["town"].tolist()

    # ── Method 2: Ward hierarchical clustering ───────────────────────────────
    # k=30 chosen for the ~160-town MA dataset: the dendrogram elbow sits at
    # k=2 (urban vs rural MA), which is too coarse. k=10 gives clusters of
    # ~23 towns that are too broad — small rural towns (pop ~8k) end up in the
    # same cluster as inner-ring suburbs (pop ~27k). k=30 provides sub-cohort
    # granularity: Saugus's cluster separates cleanly at ~14 comparable towns.
    optimal_k = 30
    X_scaled = _scale(X_all)
    Z = hc_linkage(X_scaled, method="ward", metric="euclidean")
    labels    = fcluster(Z, optimal_k, criterion="maxclust")
    all_towns["cluster"] = labels

    saugus_cluster = int(all_towns.loc[all_towns["town"] == SAUGUS, "cluster"].iloc[0])
    ward_peers = all_towns.loc[
        (all_towns["cluster"] == saugus_cluster) & (all_towns["town"] != SAUGUS),
        "town"
    ].tolist()

    # ── Consensus ────────────────────────────────────────────────────────────
    mahal_set    = set(mahal_peers)
    ward_set     = set(ward_peers)
    consensus    = sorted(mahal_set & ward_set)

    print(f"  [peers] Mahalanobis top-{n_mahal}: {len(mahal_peers)} towns")
    print(f"  [peers] Ward cluster (k={optimal_k}): {len(ward_peers)} towns "
          f"in Saugus's cluster")
    print(f"  [peers] Consensus (both methods): {len(consensus)} towns")

    return mahal_peers, ward_peers, consensus


# ─────────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────────

def make_cover() -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.text(0.5, 0.76, "What Makes Towns Succeed?",
            ha="center", va="center", fontsize=26, fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.65, "Lessons for Saugus from MA Towns That Improved",
            ha="center", va="center", fontsize=15, color="#555",
            transform=ax.transAxes)
    ax.text(0.5, 0.54,
            "Trajectory study: which towns in similar circumstances to Saugus\n"
            "improved most since 2012, and what did they do differently?",
            ha="center", va="center", fontsize=11, color="#777",
            transform=ax.transAxes, linespacing=1.8)
    lines = [
        "Method:   Compare baseline (2011-2014) to current (2021-2024) snapshots",
        "          Identify 'risers' vs 'stagnators' among Saugus-comparable towns",
        "",
        "Metrics:  MCAS ELA  |  Graduation rate  |  Dropout rate  |  Absenteeism",
        "          Median household income  |  Home values  |  Crime rate",
        "          Per-pupil spending  |  Teacher density  |  Ed share of budget",
        "",
        "Case studies:  Revere  |  Woburn  |  Peabody  |  Wakefield  |  Natick",
    ]
    ax.text(0.5, 0.30, "\n".join(lines),
            ha="center", va="center", fontsize=9.5, color="#444",
            transform=ax.transAxes, fontfamily="monospace", linespacing=1.7)
    ax.text(0.5, 0.06,
            "Proximity matters, but policy choices compound over time. This report\n"
            "separates structural advantages from actionable investments.",
            ha="center", va="center", fontsize=9, color="#888",
            transform=ax.transAxes, linespacing=1.6)
    return fig


def make_saugus_snapshot(traj: pd.DataFrame, peers: list[str]) -> plt.Figure:
    """
    Saugus vs. peer median on key metrics: baseline and current side by side.
    """
    metrics = [
        ("mcas_ela",        "MCAS ELA\nProficient %",      CAT["Education"]),
        ("graduation_rate", "Grad Rate %",                  CAT["Education"]),
        ("dropout_rate",    "Dropout Rate %",               CAT["Education"]),
        ("absenteeism_rate","Chronic\nAbsenteeism %",       CAT["Education"]),
        ("median_hh_income","Median HH\nIncome ($k)",       CAT["Community"]),
        ("poverty_pct",     "Poverty %",                    CAT["Community"]),
        ("zhvi",            "Home Values\n($k Zillow)",     CAT["Market"]),
        ("crime_rate",      "Crime Rate\n/ 100k",           CAT["Safety"]),
        ("pp_real",         "Per-Pupil\nSpending ($)",      CAT["Education"]),
        ("ed_pct",          "Ed % of\nMuni Budget",         CAT["Fiscal"]),
    ]
    n = len(metrics)

    saugus = traj[traj["town"] == SAUGUS]
    peer_rows = traj[traj["town"].isin(peers)]

    fig, axes = plt.subplots(2, n, figsize=(n * 1.8, 6),
                              gridspec_kw={"height_ratios": [3, 1]})
    fig.suptitle(
        "Saugus Today vs. Comparable MA Towns\n"
        "Blue = 2011-14 baseline   |   Green = 2021-24 current   |   "
        "Bar = Saugus   |   Line = peer median (30 most similar towns)",
        fontsize=10, fontweight="bold"
    )

    for j, (col, label, colour) in enumerate(metrics):
        ax = axes[0, j]
        ax_chg = axes[1, j]

        b_col = f"{col}_base"
        c_col = f"{col}_curr"

        s_base = saugus[b_col].values[0] if b_col in saugus.columns and len(saugus) > 0 else np.nan
        s_curr = saugus[c_col].values[0] if c_col in saugus.columns and len(saugus) > 0 else np.nan
        p_base = peer_rows[b_col].median() if b_col in peer_rows.columns else np.nan
        p_curr = peer_rows[c_col].median() if c_col in peer_rows.columns else np.nan

        # Scale for display
        scale = 1.0
        if col in ("median_hh_income", "zhvi", "pp_real"):
            scale = 1 / 1000

        vals = [v * scale for v in [s_base, s_curr, p_base, p_curr]]
        x    = [0.2, 0.8]

        ax.bar([x[0]], [vals[0]], width=0.25, color="#4A90D9", alpha=0.75, label="Baseline")
        ax.bar([x[1]], [vals[1]], width=0.25, color="#27AE60", alpha=0.75, label="Current")
        if not np.isnan(vals[2]):
            ax.plot(x[0], vals[2], "o-", color="#333", markersize=5, linewidth=1, zorder=5)
        if not np.isnan(vals[3]):
            ax.plot(x[1], vals[3], "o-", color="#333", markersize=5, linewidth=1, zorder=5)

        ax.set_xticks([])
        ax.set_facecolor("#f9f9f9")
        ax.tick_params(labelsize=7)
        ax.set_title(label, fontsize=7.5, fontweight="bold", color=colour, pad=3)

        # Change bar
        s_chg = (vals[1] - vals[0]) if not np.isnan(vals[0]) and not np.isnan(vals[1]) else np.nan
        p_chg = (vals[3] - vals[2]) if not np.isnan(vals[2]) and not np.isnan(vals[3]) else np.nan
        # For "lower is better" metrics, negate so positive = improvement
        lower_better = col in ("dropout_rate", "absenteeism_rate", "poverty_pct", "crime_rate")
        if lower_better:
            s_chg = -s_chg if not np.isnan(s_chg) else np.nan
            p_chg = -p_chg if not np.isnan(p_chg) else np.nan

        if not np.isnan(s_chg):
            ax_chg.bar([0.5], [s_chg], width=0.4,
                       color="#27AE60" if s_chg >= 0 else "#E05C4A", alpha=0.8)
        if not np.isnan(p_chg):
            ax_chg.axhline(p_chg, color="#333", linewidth=1.2, linestyle="--")
        ax_chg.axhline(0, color="#999", linewidth=0.7)
        ax_chg.set_xticks([])
        ax_chg.tick_params(labelsize=6)
        ax_chg.set_facecolor("#f0f0f0")
        ax_chg.set_title("Improvement\n(Saugus vs peer)", fontsize=6, pad=2)

    handles = [
        mpatches.Patch(color="#4A90D9", alpha=0.75, label="Saugus baseline (2011-14)"),
        mpatches.Patch(color="#27AE60", alpha=0.75, label="Saugus current (2021-24)"),
        plt.Line2D([0],[0], color="#333", marker="o", markersize=5, label="Peer median"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=8,
               bbox_to_anchor=(0.5, -0.02))
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    return fig


def make_peer_leaderboard(traj: pd.DataFrame, peers: list[str]) -> plt.Figure:
    """
    Rank peers (+ Saugus) by composite improvement score.
    Colour by improvement quartile.
    """
    sub = traj[traj["town"].isin(peers + [SAUGUS])].copy()
    if sub.empty or "improvement_score" not in sub.columns:
        return None

    sub = sub.sort_values("improvement_score", ascending=True)
    n = len(sub)

    fig, ax = plt.subplots(figsize=(8, max(5, n * 0.38 + 2)))

    colours = []
    for _, row in sub.iterrows():
        s = row["improvement_score"]
        if row["town"] == SAUGUS:
            colours.append("#9B59B6")
        elif s > 0.5:
            colours.append("#27AE60")
        elif s > 0:
            colours.append("#5CB85C")
        elif s > -0.5:
            colours.append("#F0AD4E")
        else:
            colours.append("#E05C4A")

    bars = ax.barh(range(n), sub["improvement_score"], color=colours,
                   edgecolor="white", height=0.7)

    ax.set_yticks(range(n))
    ax.set_yticklabels(sub["town"], fontsize=8.5)
    for tick, town in zip(ax.get_yticklabels(), sub["town"]):
        if town == SAUGUS:
            tick.set_color("#9B59B6")
            tick.set_fontweight("bold")

    for bar, (_, row) in zip(bars, sub.iterrows()):
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                f"{row['improvement_score']:+.2f}",
                va="center", ha="left", fontsize=7.5)

    ax.axvline(0, color="#333", linewidth=1)
    ax.set_xlabel(
        "Composite improvement score (2011-14 to 2021-24)\n"
        "Combines MCAS, graduation, dropout, income, home values, crime (sign-adjusted)",
        fontsize=9
    )
    ax.set_title(
        "Who Improved Most Among Towns Similar to Saugus?\n"
        "Green = strong improvers  |  Red = stagnated or declined  |  Purple = Saugus",
        fontsize=10, fontweight="bold"
    )
    plt.tight_layout()
    return fig


def make_riser_policy_profile(traj: pd.DataFrame, peers: list[str]) -> plt.Figure:
    """
    Split peers into 'risers' (top half improvement) and 'stagnators' (bottom half).
    Compare their policy inputs: spending, teacher density, ed budget share.
    """
    sub = traj[traj["town"].isin(peers + [SAUGUS])].copy()
    if sub.empty or "improvement_score" not in sub.columns:
        return None

    median_score = sub[sub["town"] != SAUGUS]["improvement_score"].median()
    sub["group"] = "Risers"
    sub.loc[sub["improvement_score"] < median_score, "group"] = "Stagnators"
    sub.loc[sub["town"] == SAUGUS, "group"] = "Saugus"

    policy_metrics = [
        ("pp_real_base",       "Per-Pupil Spending\nBaseline ($)"),
        ("pp_real_curr",       "Per-Pupil Spending\nCurrent ($)"),
        ("teachers_per1k_base","Teachers/1k Students\nBaseline"),
        ("teachers_per1k_curr","Teachers/1k Students\nCurrent"),
        ("ed_pct_base",        "Ed % of Budget\nBaseline"),
        ("ed_pct_curr",        "Ed % of Budget\nCurrent"),
    ]

    n = len(policy_metrics)
    fig, axes = plt.subplots(1, n, figsize=(n * 2.2, 5))

    group_colours = {"Risers": "#27AE60", "Stagnators": "#E05C4A", "Saugus": "#9B59B6"}

    for ax, (col, label) in zip(axes, policy_metrics):
        if col not in sub.columns:
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=8)
            continue
        scale = 1/1000 if "pp_real" in col else 1.0
        for g_name in ["Risers", "Stagnators"]:
            g_sub = sub[sub["group"] == g_name][col].dropna() * scale
            if g_sub.empty:
                continue
            x_pos = 0 if g_name == "Risers" else 1
            bp = ax.boxplot(g_sub, positions=[x_pos], widths=0.4,
                            patch_artist=True,
                            boxprops=dict(facecolor=group_colours[g_name], alpha=0.6),
                            medianprops=dict(color="white", linewidth=2),
                            whiskerprops=dict(color="#555"),
                            capprops=dict(color="#555"),
                            flierprops=dict(marker="o", markersize=3, alpha=0.3))

        # Saugus dot
        s_val = sub[sub["group"] == "Saugus"][col].values
        if len(s_val) > 0 and not np.isnan(s_val[0]):
            ax.plot(0.5, s_val[0] * scale, "D", color="#9B59B6",
                    markersize=8, zorder=5, label="Saugus")
            ax.text(0.55, s_val[0] * scale, " Saugus",
                    va="center", fontsize=7.5, color="#9B59B6")

        ax.set_xticks([0, 1])
        ax.set_xticklabels(["Risers", "Stagnators"], fontsize=8.5)
        ax.set_title(label, fontsize=8.5, fontweight="bold", pad=4)
        ax.tick_params(labelsize=7.5)
        ax.set_facecolor("#f9f9f9")

    fig.suptitle(
        "Policy Profile: Risers vs. Stagnators (towns comparable to Saugus)\n"
        "Green boxes = towns that improved most  |  Red = improved least  |  Diamond = Saugus",
        fontsize=10, fontweight="bold"
    )
    plt.tight_layout()
    return fig


def make_mcas_trajectory(frames: dict, towns: list[str], title: str) -> plt.Figure:
    """
    Line chart: MCAS ELA proficiency over time for a set of towns.
    Saugus is thicker/purple; others are thin and coloured by 2023 rank.
    """
    ela = frames["mcas"][frames["mcas"]["subject"] == "ELA"].copy()
    ela = ela[ela["town"].isin(towns)]

    years = sorted(ela["year"].unique())
    pivoted = ela.pivot_table(index="year", columns="town", values="proficient_pct")

    # Rank towns by 2023 (latest) score for colouring
    latest = pivoted.iloc[-1].dropna().sort_values(ascending=False)
    n = len(latest)

    fig, ax = plt.subplots(figsize=(11, 6))

    cmap = plt.cm.RdYlGn
    for i, town in enumerate(latest.index):
        if town == SAUGUS:
            continue
        colour = cmap(i / max(n - 1, 1))
        series = pivoted[town].dropna()
        ax.plot(series.index, series.values, color=colour, alpha=0.5, linewidth=1.2)

    # Saugus on top
    if SAUGUS in pivoted.columns:
        s = pivoted[SAUGUS].dropna()
        ax.plot(s.index, s.values, color="#9B59B6", linewidth=3.0, zorder=5,
                label=f"Saugus ({s.iloc[-1]:.0f}%)" if len(s) > 0 else "Saugus")
        if len(s) > 0:
            ax.annotate(f"Saugus {s.iloc[-1]:.0f}%",
                        xy=(s.index[-1], s.iloc[-1]),
                        xytext=(10, 0), textcoords="offset points",
                        color="#9B59B6", fontsize=9, fontweight="bold")

    # MA median line
    ma_med = pivoted.median(axis=1)
    ax.plot(ma_med.index, ma_med.values, "k--", linewidth=1.5, alpha=0.6,
            label="Peer median")

    ax.set_xlabel("School Year", fontsize=10)
    ax.set_ylabel("MCAS ELA Proficient %", fontsize=10)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.legend(fontsize=9, loc="upper left")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(0, n - 1))
    sm.set_array([])
    plt.colorbar(sm, ax=ax, orientation="vertical", pad=0.02, shrink=0.6,
                 label="2023 rank: best (top) to worst (bottom) among peers")

    plt.tight_layout()
    return fig


def make_case_study(traj: pd.DataFrame, frames: dict, town: str, note: str) -> plt.Figure:
    """
    One page per case-study town: trajectory vs. Saugus on 4 key metrics.
    """
    towns_to_plot = [SAUGUS, town]

    fig, axes = plt.subplots(2, 2, figsize=(11, 7))
    fig.suptitle(f"Case Study: {town}\n{note}", fontsize=12, fontweight="bold")

    metric_configs = [
        ("mcas_ela", "MCAS ELA Proficient %", "ELA", CAT["Education"], True),
        ("graduation_rate", "Graduation Rate %", None, CAT["Education"], True),
        ("zhvi", "Median Home Value ($)", None, CAT["Market"], True),
        ("pp_real", "Per-Pupil Spending (real $)", None, CAT["Education"], True),
    ]

    ela = frames["mcas"][frames["mcas"]["subject"] == "ELA"]
    grad = frames["grad"]
    zh   = frames["zillow"]

    cpi = frames["cpi"].set_index("year")["cpi_index"].to_dict()
    base_cpi = cpi.get(max(cpi), 1.0)
    pp = frames["pp"].copy()
    pp["pp_real"] = pp["pp_exp"] * pp["year"].map(lambda y: base_cpi / cpi.get(y, np.nan))

    series_map = {
        "mcas_ela":        ela.pivot_table(index="year", columns="town", values="proficient_pct"),
        "graduation_rate": grad.pivot_table(index="year", columns="town", values="graduation_rate"),
        "zhvi":            zh.pivot_table(index="year", columns="town", values="zhvi"),
        "pp_real":         pp.pivot_table(index="year", columns="town", values="pp_real"),
    }

    colours = {SAUGUS: "#9B59B6", town: "#E05C4A" if town != SAUGUS else "#9B59B6"}

    for ax, (col, ylabel, _, colour, _) in zip(axes.flat, metric_configs):
        piv = series_map.get(col)
        if piv is None:
            continue
        for t in towns_to_plot:
            if t not in piv.columns:
                continue
            s = piv[t].dropna()
            lw = 2.5 if t == SAUGUS else 2.0
            ls = "-" if t == SAUGUS else "--"
            c  = colours[t]
            scale = 1/1000 if col in ("zhvi","pp_real") else 1
            ax.plot(s.index, s.values * scale, color=c, linewidth=lw,
                    linestyle=ls, label=t)

        ax.set_ylabel(ylabel + (" ($k)" if col in ("zhvi","pp_real") else ""), fontsize=9)
        ax.legend(fontsize=9)
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.tick_params(labelsize=8)

    plt.tight_layout()
    return fig


def make_natick_deep_dive(traj: pd.DataFrame, frames: dict) -> plt.Figure:
    """
    Natick-specific analysis: structural vs. policy advantages.
    """
    towns_to_compare = ["Natick", SAUGUS, "Wellesley", "Revere", "Woburn"]
    metrics = ["mcas_ela_curr", "graduation_rate_curr", "median_hh_income_curr",
               "zhvi_curr", "pp_real_curr", "crime_rate_curr"]
    labels  = ["MCAS ELA %", "Grad Rate %", "Median Income\n($k)",
               "Home Value\n($k Zillow)", "Per-Pupil\nSpending ($k)", "Crime Rate\n/100k"]

    sub = traj[traj["town"].isin(towns_to_compare)].set_index("town")
    n = len(metrics)

    fig, axes = plt.subplots(1, n, figsize=(n * 2, 5))
    town_colours = {
        "Wellesley": "#E05C4A",
        "Natick":    "#F0AD4E",
        "Woburn":    "#4A90D9",
        "Revere":    "#27AE60",
        SAUGUS:      "#9B59B6",
    }

    for ax, col, label in zip(axes, metrics, labels):
        if col not in sub.columns:
            continue
        vals = sub[col].reindex(towns_to_compare).dropna()
        scale = 1/1000 if any(x in col for x in ("income","zhvi","pp_real")) else 1
        bars = ax.bar(range(len(vals)), vals.values * scale,
                      color=[town_colours.get(t, "#999") for t in vals.index],
                      edgecolor="white", alpha=0.85)
        ax.set_xticks(range(len(vals)))
        ax.set_xticklabels(vals.index, rotation=30, ha="right", fontsize=7.5)
        for tick, t in zip(ax.get_xticklabels(), vals.index):
            tick.set_color(town_colours.get(t, "#333"))
            if t == SAUGUS:
                tick.set_fontweight("bold")
        ax.set_title(label, fontsize=8.5, fontweight="bold", pad=3)
        ax.tick_params(labelsize=7.5)
        ax.set_facecolor("#f9f9f9")
        ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.suptitle(
        "Context: Where Each Town Stands Today (2021-24 average)\n"
        "Wellesley = structural wealth advantage  |  Natick = Route 9 + investment  |  "
        "Revere = rapid improver  |  Woburn = Saugus-like trajectory",
        fontsize=9.5, fontweight="bold"
    )
    plt.tight_layout()
    return fig


def make_saugus_levers(traj: pd.DataFrame, peers: list[str]) -> plt.Figure:
    """
    What levers does Saugus have?
    Compare Saugus to 'risers' on specific actionable dimensions.
    """
    sub = traj[traj["town"].isin(peers + [SAUGUS])].copy()
    if sub.empty:
        return None

    median_score = sub[sub["town"] != SAUGUS]["improvement_score"].median()
    risers   = sub[(sub["improvement_score"] >= median_score) & (sub["town"] != SAUGUS)]
    saugus_r = sub[sub["town"] == SAUGUS]

    levers = [
        ("pp_real_curr",       "Per-Pupil Spending\n(current, $)",      1/1000,
         "Risers average more\nper-pupil spend than Saugus",
         "Catch-up spending to\npeer median"),
        ("teachers_per1k_curr","Teachers / 1k Students\n(current)",     1.0,
         "Teacher density: is Saugus\nunder-staffed vs peers?",
         "Add teachers in shortage areas\n(math, science, special ed)"),
        ("ed_pct_curr",        "Ed % of Muni Budget\n(current)",        1.0,
         "Priority: how much of the\nbudget goes to schools?",
         "Rebalance budget toward\neducation"),
        ("graduation_rate_curr","Graduation Rate %\n(current)",         1.0,
         "Graduation rate: how does\nSaugus compare to risers?",
         "Reduce dropout risk:\ntargeted interventions"),
        ("absenteeism_rate_curr","Chronic Absenteeism %\n(current)",    1.0,
         "Attendance: high absenteeism\ncorrelates with poor outcomes",
         "Community outreach programs;\nattendance accountability"),
    ]

    n = len(levers)
    fig, axes = plt.subplots(1, n, figsize=(n * 2.3, 6))

    for ax, (col, label, scale, problem, action) in zip(axes, levers):
        if col not in sub.columns:
            continue
        r_vals = risers[col].dropna() * scale
        s_val  = saugus_r[col].values[0] * scale if len(saugus_r) > 0 and col in saugus_r.columns else None

        if r_vals.empty:
            continue

        ax.boxplot(r_vals, positions=[0.5], widths=0.4, patch_artist=True,
                   boxprops=dict(facecolor="#27AE60", alpha=0.6),
                   medianprops=dict(color="white", linewidth=2),
                   whiskerprops=dict(color="#555"), capprops=dict(color="#555"),
                   flierprops=dict(marker="o", markersize=3, alpha=0.3))

        if s_val is not None and not np.isnan(s_val):
            ax.plot(0.5, s_val, "D", color="#9B59B6", markersize=10, zorder=5)
            ax.text(0.6, s_val, f" Saugus\n {s_val:.1f}",
                    va="center", fontsize=8, color="#9B59B6", fontweight="bold")
            r_med = r_vals.median()
            gap = s_val - r_med
            colour = "#27AE60" if gap > 0 else "#E05C4A"
            ax.text(0.5, ax.get_ylim()[0],
                    f"Gap: {gap:+.1f}",
                    ha="center", va="bottom", fontsize=7.5, color=colour)

        ax.set_xticks([])
        ax.set_title(label, fontsize=8, fontweight="bold", color="#333", pad=3)
        ax.tick_params(labelsize=7.5)
        ax.set_facecolor("#f9f9f9")

        # Footnote action
        ax.text(0.5, -0.18, action, transform=ax.transAxes,
                ha="center", va="top", fontsize=6.5, color="#555",
                style="italic", wrap=True)

    fig.suptitle(
        "Saugus vs. Riser Towns: Where Are the Gaps? (purple diamond = Saugus  |  green box = riser towns)\n"
        "Bottom label: actionable lever for Saugus",
        fontsize=10, fontweight="bold"
    )
    plt.tight_layout(rect=[0, 0.05, 1, 1])
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

CASE_STUDY_NOTES = {
    "Natick":    "Structural advantages: Route 9, Natick Mall, Route 128 tech corridor proximity.\n"
                 "But also made deliberate investments in schools and town services.",
    "Revere":    "Rapid improver: beach town that was struggling 15 years ago.\n"
                 "Benefited from Boston spillover, new development, and school investment.",
    "Woburn":    "Comparable start to Saugus: industrial history, Route 128 access.\n"
                 "Steady improver -- consistent education investment and commercial development.",
    "Peabody":   "Same county as Saugus (Essex), similar demographics and income.\n"
                 "Has pulled ahead on MCAS and home values through sustained investment.",
    "Wakefield": "Northern neighbor of Saugus. Higher income, but similar town character.\n"
                 "Shows what Saugus's upside could look like with sustained improvement.",
    "Braintree": "South Shore analog: suburban blue-collar town that has improved markedly.\n"
                 "Invested in schools and attracted commercial development on Route 3 corridor.",
}


def main():
    engine = get_engine()
    os.makedirs(os.path.dirname(_FINAL_PDF), exist_ok=True)

    print("[success] Loading data...")
    frames = load_all(engine)

    print("[success] Building town trajectories (2011-14 baseline, 2021-24 current)...")
    traj = build_trajectories(frames)
    print(f"[success] {len(traj)} towns with complete trajectory data")

    print("[success] Finding Saugus peer group...")
    mahal_peers, ward_peers, consensus_peers = find_saugus_peers(traj, n_mahal=30)
    peers = consensus_peers if len(consensus_peers) >= 10 else mahal_peers
    print(f"[success] Using {'consensus' if peers is consensus_peers else 'Mahalanobis'} "
          f"peers: {len(peers)} towns — {peers[:8]}...")

    saugus_row = traj[traj["town"] == SAUGUS]
    if not saugus_row.empty and "improvement_score" in saugus_row.columns:
        s_score = saugus_row["improvement_score"].values[0]
        all_scores = traj["improvement_score"].dropna()
        s_pct = (all_scores < s_score).mean() * 100
        print(f"[success] Saugus improvement score: {s_score:+.2f}  ({s_pct:.0f}th pct overall)")

    def _save(pdf, fig, label=""):
        if fig is None:
            return
        try:
            pdf.savefig(fig, bbox_inches="tight")
        except Exception as e:
            print(f"  [warn] Could not save {label}: {e}")
        finally:
            plt.close(fig)

    print("[success] Generating PDF...")
    with PdfPages(OUTPUT_PDF) as pdf:
        _save(pdf, make_cover(), "cover")

        print("  Saugus snapshot...")
        _save(pdf, make_saugus_snapshot(traj, peers), "saugus snapshot")

        print("  Peer leaderboard...")
        _save(pdf, make_peer_leaderboard(traj, peers), "leaderboard")

        print("  MCAS trajectory (peers)...")
        peer_towns = peers + [SAUGUS]
        _save(pdf, make_mcas_trajectory(
            frames, peer_towns,
            "MCAS ELA Trajectory: Saugus vs. 30 Demographically Comparable Towns\n"
            "Purple = Saugus | Colour = 2023 rank (green=best, red=worst among peers)"
        ), "mcas trajectory")

        print("  Riser policy profile...")
        _save(pdf, make_riser_policy_profile(traj, peers), "riser policy")

        print("  Context: Natick deep dive...")
        _save(pdf, make_natick_deep_dive(traj, frames), "natick")

        for town in CASE_STUDIES:
            if town in traj["town"].values:
                print(f"  Case study: {town}...")
                note = CASE_STUDY_NOTES.get(town, "")
                _save(pdf, make_case_study(traj, frames, town, note), f"case {town}")

        print("  Saugus levers...")
        _save(pdf, make_saugus_levers(traj, peers), "levers")

    import shutil
    shutil.copy2(OUTPUT_PDF, _FINAL_PDF)
    print(f"[success] Report saved to {_FINAL_PDF}")


if __name__ == "__main__":
    main()
