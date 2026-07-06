"""
Town Factor Portfolio Analysis

Groups MA towns into quartile "baskets" by each policy input factor —
both the current LEVEL and the year-over-year CHANGE — then shows how
those baskets perform on 15 outcome metrics.

Inspired by quantitative factor investing:
  - "Long" basket = Q1 towns (highest factor value)
  - "Short" basket = Q4 towns (lowest factor value)
  - "Alpha" = outcome difference Q1 - Q4

Run:
    python analysis/factor_portfolio.py
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

# Use PDF core fonts to avoid TrueType embedding (which can timeout on large PDFs)
matplotlib.rcParams.update({
    "pdf.use14corefonts": True,
    "font.family":        "sans-serif",
    "font.sans-serif":    ["Helvetica", "DejaVu Sans", "Arial"],
})

from analysis.panel import load_panel, OUTCOMES, CAT_COLOURS, CAT_LIGHT
from config import get_engine

_FINAL_PDF = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "Reports", "town_factor_quartile_portfolio.pdf"
)
OUTPUT_PDF = "/tmp/town_factor_quartile_portfolio.pdf"

SAUGUS = "Saugus"

# Formation window: avg factor scores over these years
FORMATION_START = 2012
FORMATION_END   = 2020

# ──────────────────────────────────────────────────────────────────────────────
# Factor definitions — level AND change factors
# ──────────────────────────────────────────────────────────────────────────────

LEVEL_FACTORS = {
    "teachers_per1k": "Teachers / 1k Students  (level)",
    "log_pp_exp":     "Per-Pupil Spending  (level, log $)",
    "ch70_per_pupil": "Ch70 State Aid / Pupil  (level, $k)",
}

CHANGE_FACTORS = {
    "delta_teachers_per1k": "Teacher Staffing Growth %  (YoY change)",
    "delta_pp_exp":         "Per-Pupil Spending Growth %  (YoY change)",
    "delta_ch70_per_pupil": "Ch70 Aid Growth % / Pupil  (YoY change)",
}

ALL_FACTORS = {**LEVEL_FACTORS, **CHANGE_FACTORS}

# Sign convention for outcomes (same as backtest.py)
SIGN_MAP = {out_label: (1 if hib else -1) for _, out_label, _, hib in OUTCOMES}

# Outcomes used in portfolio charts (skip those with very limited data)
PORTFOLIO_OUTCOMES = [
    ("dropout_rate",       "Dropout Rate",           "Education", False),
    ("graduation_rate",    "Graduation Rate",         "Education", True),
    ("mcas_ela_pct",       "MCAS ELA Proficient %",   "Education", True),
    ("mcas_math_pct",      "MCAS Math Proficient %",  "Education", True),
    ("sat_mean",           "SAT Mean Score",           "Education", True),
    ("crime_rate",         "Crime Rate / 100k",        "Safety",    False),
    ("violent_crime_rate", "Violent Crime Rate",       "Safety",    False),
    ("absenteeism_rate",   "Chronic Absenteeism %",   "Community", False),
    ("enrollment_growth",  "Enrollment Growth %",     "Community", True),
    ("poverty_pct",        "Poverty Rate %",           "Community", False),
    ("real_zhvi_growth",   "Home Value Growth %",     "Market",    True),
]


# ──────────────────────────────────────────────────────────────────────────────
# Core computation
# ──────────────────────────────────────────────────────────────────────────────

def compute_town_factor_scores(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Average each factor column over the formation window per town.
    Returns a wide DataFrame: rows=towns, cols=factor columns.
    """
    form = panel[panel["year"].between(FORMATION_START, FORMATION_END)]
    avail = [f for f in ALL_FACTORS if f in panel.columns]
    scores = form.groupby("town")[avail].mean().reset_index()
    return scores


def assign_quartiles(scores: pd.DataFrame) -> pd.DataFrame:
    """
    Add Q1–Q4 label and percentile columns for each factor.
    Q1 = highest factor value, Q4 = lowest.
    """
    result = scores[["town"]].copy()
    for f, label in ALL_FACTORS.items():
        if f not in scores.columns:
            continue
        valid = scores[f].dropna()
        if len(valid) < 20:
            continue
        # qcut assigns labels in ascending order, so reverse them so Q1=highest
        result[f"{f}_q"] = pd.qcut(
            scores[f], q=4, labels=["Q4","Q3","Q2","Q1"], duplicates="drop"
        )
        result[f"{f}_score"]  = scores[f].values
        result[f"{f}_pct"]    = scores[f].rank(pct=True) * 100  # 100=best
    return result


def quartile_outcome_means(
    panel: pd.DataFrame,
    quartiles: pd.DataFrame,
    factor: str,
    out_key: str,
    lag: int,
) -> pd.DataFrame:
    """
    For each Q1–Q4 basket (formed over FORMATION window), compute the mean
    of `out_key` in outcome years = FORMATION_END + lag.

    Returns DataFrame with columns [quartile, mean_outcome, n_towns, stderr].
    """
    q_col = f"{factor}_q"
    if q_col not in quartiles.columns or out_key not in panel.columns:
        return pd.DataFrame()

    eval_year = FORMATION_END + lag
    outcomes = panel[panel["year"] == eval_year][["town", out_key]].dropna()
    merged = outcomes.merge(quartiles[["town", q_col]], on="town", how="inner")
    if merged.empty or merged[q_col].isna().all():
        return pd.DataFrame()

    def _agg(g):
        return pd.Series({
            "mean_outcome": g[out_key].mean(),
            "stderr":       g[out_key].sem(),
            "n_towns":      g["town"].nunique(),
        })

    return (
        merged.groupby(q_col, observed=True)
              .apply(_agg)
              .reset_index()
              .rename(columns={q_col: "quartile"})
    )


def compute_composite_score(scores: pd.DataFrame) -> pd.DataFrame:
    """
    Multi-factor composite: z-score each change factor, then sum.
    Higher = town is growing faster on all dimensions simultaneously.
    """
    z_cols = []
    for f in CHANGE_FACTORS:
        if f not in scores.columns:
            continue
        col = scores[f]
        z = (col - col.mean()) / (col.std() + 1e-9)
        scores[f"{f}_z"] = z
        z_cols.append(f"{f}_z")

    if not z_cols:
        scores["composite_z"] = np.nan
    else:
        scores["composite_z"] = scores[z_cols].mean(axis=1)

    # Also add level composite
    level_z = []
    for f in LEVEL_FACTORS:
        if f not in scores.columns:
            continue
        col = scores[f]
        z = (col - col.mean()) / (col.std() + 1e-9)
        scores[f"{f}_z"] = z
        level_z.append(f"{f}_z")
    if level_z:
        scores["level_composite_z"] = scores[level_z].mean(axis=1)

    return scores


# ──────────────────────────────────────────────────────────────────────────────
# Charts
# ──────────────────────────────────────────────────────────────────────────────

def make_cover() -> plt.Figure:
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    ax.text(0.5, 0.76, "Town Factor Portfolio Analysis",
            ha="center", va="center", fontsize=28, fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.64, "Quartile Baskets  ·  Level & Growth Factors  ·  16 Outcomes",
            ha="center", va="center", fontsize=15, color="#555",
            transform=ax.transAxes)
    ax.text(0.5, 0.54,
            "Inspired by quantitative factor investing:\n"
            "Q1 basket (top quartile) vs Q4 basket (bottom quartile) — how do they diverge?",
            ha="center", va="center", fontsize=11, color="#777",
            transform=ax.transAxes, linespacing=1.8)
    lines = [
        "Level factors:   Teachers / 1k Students  ·  Per-Pupil Spending  ·  Ch70 State Aid",
        "Change factors:  Teacher Staffing Growth %  ·  Spending Growth %  ·  Ch70 Aid Growth %",
        "",
        "Formation window:  2012–2020 (pre-COVID averages)",
        "Outcomes measured: 1, 3, and 5 years after formation",
        "",
        "Saugus is highlighted throughout for context.",
    ]
    ax.text(0.5, 0.30, "\n".join(lines),
            ha="center", va="center", fontsize=9.5, color="#444",
            transform=ax.transAxes, fontfamily="monospace", linespacing=1.7)
    ax.text(0.5, 0.06, "Saugus Schools Project  —  MA Municipal Data",
            ha="center", va="center", fontsize=9, color="#aaa",
            transform=ax.transAxes)
    return fig


def make_factor_distribution(scores: pd.DataFrame, quartiles: pd.DataFrame,
                              factor: str, label: str) -> plt.Figure:
    """
    Histogram of the factor across all towns, with Saugus highlighted,
    plus quartile boundaries.
    """
    if f"{factor}_score" not in quartiles.columns:
        return None

    vals = quartiles[f"{factor}_score"].dropna()
    saugus_row = quartiles[quartiles["town"] == SAUGUS]
    saugus_val = saugus_row[f"{factor}_score"].values[0] if len(saugus_row) > 0 else None
    saugus_pct = saugus_row[f"{factor}_pct"].values[0] if len(saugus_row) > 0 else None

    q_bounds = [vals.quantile(0.25), vals.quantile(0.50), vals.quantile(0.75)]

    fig, ax = plt.subplots(figsize=(10, 4))
    n, bins, patches = ax.hist(vals, bins=40, color="#4A90D9", alpha=0.7, edgecolor="white")

    # Colour by quartile
    for patch, left in zip(patches, bins):
        if left < q_bounds[0]:
            patch.set_facecolor("#E05C4A")   # Q4 — lowest
        elif left < q_bounds[1]:
            patch.set_facecolor("#F0AD4E")   # Q3
        elif left < q_bounds[2]:
            patch.set_facecolor("#5CB85C")   # Q2
        else:
            patch.set_facecolor("#4A90D9")   # Q1 — highest

    for q, qname, colour in zip(q_bounds, ["25th\n(Q3/Q4 boundary)", "50th", "75th\n(Q1/Q2 boundary)"],
                                 ["#E05C4A", "#888", "#4A90D9"]):
        ax.axvline(q, color=colour, linewidth=1.5, linestyle="--")

    if saugus_val is not None:
        ax.axvline(saugus_val, color="#9B59B6", linewidth=2.5, linestyle="-",
                   label=f"Saugus  (pct={saugus_pct:.0f}th)")
        ax.text(saugus_val, ax.get_ylim()[1] * 0.95, f" Saugus\n {saugus_val:.1f}",
                color="#9B59B6", fontsize=8.5, va="top", ha="left" if saugus_val < vals.median() else "right")

    ax.set_xlabel(label, fontsize=10)
    ax.set_ylabel("Number of towns", fontsize=10)
    ax.set_title(
        f"Distribution of  '{label}'\nacross all MA towns  (formation avg 2012–2020)\n"
        "Colour: Q1 (blue) = top quartile — Q4 (red) = bottom quartile",
        fontsize=10, fontweight="bold"
    )

    legend_patches = [
        mpatches.Patch(color="#4A90D9", label="Q1 — highest 25%"),
        mpatches.Patch(color="#5CB85C", label="Q2"),
        mpatches.Patch(color="#F0AD4E", label="Q3"),
        mpatches.Patch(color="#E05C4A", label="Q4 — lowest 25%"),
    ]
    if saugus_val is not None:
        legend_patches.append(mpatches.Patch(color="#9B59B6", label=f"Saugus ({saugus_pct:.0f}th pct)"))
    ax.legend(handles=legend_patches, fontsize=8, loc="upper right")

    plt.tight_layout()
    return fig


def make_quartile_outcome_chart(panel: pd.DataFrame, quartiles: pd.DataFrame,
                                 factor: str, label: str, lag: int) -> plt.Figure:
    """
    For one factor, show how each outcome's mean differs across Q1–Q4
    at a given lag.  Side-by-side bars for Q1/Q2/Q3/Q4.
    """
    outcomes_to_plot = [(k, l, c, h) for k, l, c, h in PORTFOLIO_OUTCOMES
                        if k in panel.columns]
    if not outcomes_to_plot:
        return None

    n_out = len(outcomes_to_plot)
    fig, axes = plt.subplots(1, n_out, figsize=(max(14, n_out * 1.6), 5),
                              sharey=False)
    if n_out == 1:
        axes = [axes]

    q_colours = {"Q1": "#4A90D9", "Q2": "#5CB85C", "Q3": "#F0AD4E", "Q4": "#E05C4A"}
    quartile_order = ["Q1", "Q2", "Q3", "Q4"]

    any_data = False
    for ax, (out_key, out_label, cat, hib) in zip(axes, outcomes_to_plot):
        df = quartile_outcome_means(panel, quartiles, factor, out_key, lag)
        if df.empty:
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=8, color="#aaa")
            ax.set_xticks([])
            ax.set_title(out_label, fontsize=7.5, color=CAT_COLOURS.get(cat, "#333"),
                         fontweight="bold")
            continue
        any_data = True
        df = df.set_index("quartile").reindex(quartile_order).reset_index()
        bars = ax.bar(df["quartile"].astype(str),
                      df["mean_outcome"],
                      color=[q_colours.get(q, "#999") for q in df["quartile"]],
                      alpha=0.85, edgecolor="white")
        # Error bars
        for bar, se in zip(bars, df["stderr"].fillna(0)):
            ax.errorbar(bar.get_x() + bar.get_width()/2,
                        bar.get_height(), yerr=se, fmt="none",
                        color="#333", capsize=3, linewidth=1)

        ax.set_xticks(range(4))
        ax.set_xticklabels(["Q1","Q2","Q3","Q4"], fontsize=7.5)
        ax.set_facecolor(CAT_LIGHT.get(cat, "#f9f9f9"))
        ax.set_title(out_label, fontsize=8, color=CAT_COLOURS.get(cat, "#333"),
                     fontweight="bold", pad=4)
        ax.tick_params(axis="y", labelsize=7)
        ax.grid(axis="y", linestyle="--", alpha=0.4)

        # Lower/higher annotation
        direction = "lower=better" if not hib else "higher=better"
        ax.text(0.01, 0.99, direction, transform=ax.transAxes,
                fontsize=6, color="#888", va="top", ha="left")

    if not any_data:
        plt.close(fig)
        return None

    lag_desc = f"{lag} year{'s' if lag != 1 else ''} after formation"
    fig.suptitle(
        f"Factor: '{label}'  —  outcomes {lag_desc}\n"
        "Q1 (blue) = top quartile towns  ·  Q4 (red) = bottom quartile towns  ·  "
        "Error bars = ± 1 standard error",
        fontsize=10, fontweight="bold", y=1.02
    )
    plt.tight_layout()
    return fig


def make_spread_heatmap(panel: pd.DataFrame, quartiles: pd.DataFrame,
                         lag: int) -> plt.Figure:
    """
    Heatmap: rows = factors, columns = outcomes.
    Cell = sign-adjusted Q1–Q4 mean difference (positive = Q1 towns do better).
    """
    outcomes_to_plot = [(k, l, c, h) for k, l, c, h in PORTFOLIO_OUTCOMES
                        if k in panel.columns]
    factors = [(f, lab) for f, lab in ALL_FACTORS.items()
               if f"{f}_q" in quartiles.columns]

    if not outcomes_to_plot or not factors:
        return None

    n_f = len(factors)
    n_o = len(outcomes_to_plot)

    mat = np.full((n_f, n_o), np.nan)
    pval_mat = np.full((n_f, n_o), np.nan)

    for i, (factor, _) in enumerate(factors):
        for j, (out_key, out_label, cat, hib) in enumerate(outcomes_to_plot):
            df = quartile_outcome_means(panel, quartiles, factor, out_key, lag)
            if df.empty:
                continue
            df = df.set_index("quartile").reindex(["Q1","Q4"]).reset_index()
            q1 = df[df["quartile"]=="Q1"]["mean_outcome"].values
            q4 = df[df["quartile"]=="Q4"]["mean_outcome"].values
            if len(q1) == 0 or len(q4) == 0:
                continue
            diff = float(q1[0]) - float(q4[0])
            sign = 1 if hib else -1
            mat[i, j] = sign * diff  # positive = Q1 better

            # t-test between Q1 and Q4 raw town values
            eval_year = FORMATION_END + lag
            outs = panel[panel["year"] == eval_year][["town", out_key]].dropna()
            merged = outs.merge(quartiles[["town", f"{factor}_q"]], on="town", how="inner")
            g1 = merged[merged[f"{factor}_q"]=="Q1"][out_key]
            g4 = merged[merged[f"{factor}_q"]=="Q4"][out_key]
            if len(g1) >= 5 and len(g4) >= 5:
                _, pv = scipy_stats.ttest_ind(g1, g4, equal_var=False)
                pval_mat[i, j] = pv

    # Normalise to z-scores across all cells for display
    valid = mat[~np.isnan(mat)]
    if len(valid) == 0:
        return None
    vmax = np.percentile(np.abs(valid), 90) if len(valid) > 5 else 1.0

    from matplotlib.colors import TwoSlopeNorm
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)

    fig, ax = plt.subplots(figsize=(max(12, n_o * 1.2 + 3), max(5, n_f * 0.85 + 3)))
    im = ax.imshow(mat, cmap="RdYlGn", norm=norm, aspect="auto")

    # Column backgrounds
    for j, (_, _, cat, _) in enumerate(outcomes_to_plot):
        ax.axvspan(j-0.5, j+0.5, color=CAT_LIGHT.get(cat, "#f5f5f5"), alpha=0.5, zorder=0)

    # Cell labels
    for i in range(n_f):
        for j in range(n_o):
            v = mat[i, j]
            pv = pval_mat[i, j]
            if np.isnan(v):
                ax.text(j, i, "--", ha="center", va="center", fontsize=7.5, color="#aaa")
            else:
                stars = ("***" if pv < 0.01 else "**" if pv < 0.05 else
                         "*"  if pv < 0.10  else "")
                txt = f"{v:+.1f}\n{stars}" if stars else f"{v:+.1f}"
                fg = "white" if abs(v) > vmax * 0.6 else "black"
                ax.text(j, i, txt, ha="center", va="center", fontsize=7,
                        fontweight="bold" if stars else "normal", color=fg,
                        linespacing=1.3)

    # Outcome labels across top
    ax.set_xticks(range(n_o))
    ax.set_xticklabels([l for _, l, _, _ in outcomes_to_plot],
                       rotation=35, ha="left", fontsize=8.5)
    ax.xaxis.set_label_position("top")
    ax.xaxis.tick_top()
    for tick, (_, _, cat, _) in zip(ax.get_xticklabels(), outcomes_to_plot):
        tick.set_color(CAT_COLOURS.get(cat, "#333"))
        tick.set_fontweight("bold")

    # Factor labels on left; level vs change differentiation
    fac_labels = []
    for f, lab in factors:
        prefix = "(chg) " if f in CHANGE_FACTORS else "(lvl) "
        fac_labels.append(prefix + lab)
    ax.set_yticks(range(n_f))
    ax.set_yticklabels(fac_labels, fontsize=8.5)
    for tick, (f, _) in zip(ax.get_yticklabels(), factors):
        tick.set_color("#27AE60" if f in CHANGE_FACTORS else "#4A90D9")

    lag_desc = f"{lag} year{'s' if lag != 1 else ''} after formation"
    ax.set_title(
        f"Q1 vs Q4 Outcome Spread  —  {lag_desc}\n"
        "Green = Q1 (top) towns outperform Q4 (bottom)  ·  "
        "Stars = statistically significant difference\n"
        "(chg) = Change factors (green labels)  |  (lvl) = Level factors (blue labels)",
        fontsize=10, fontweight="bold", pad=14, loc="left"
    )

    legend_patches = [mpatches.Patch(color=CAT_COLOURS[c], label=c)
                      for c in CAT_COLOURS if any(cat==c for _,_,cat,_ in outcomes_to_plot)]
    ax.legend(handles=legend_patches, loc="upper left", fontsize=8,
              bbox_to_anchor=(0, -0.06), ncol=len(legend_patches),
              title="Outcome category", title_fontsize=8)

    plt.colorbar(im, ax=ax, orientation="vertical", pad=0.02, shrink=0.8,
                 label="Q1 - Q4 mean difference (sign-adjusted; positive = Q1 towns better)")
    plt.tight_layout(rect=[0, 0.04, 1, 1])
    return fig


def make_composite_slide(panel: pd.DataFrame, scores: pd.DataFrame,
                          lag: int) -> plt.Figure:
    """
    Composite multi-factor score: top-half vs bottom-half basket.
    Left: distribution with Saugus. Right: outcome comparison (top vs bottom).
    """
    if "composite_z" not in scores.columns or scores["composite_z"].isna().all():
        return None

    scores_clean = scores.dropna(subset=["composite_z"]).copy()
    median_z = scores_clean["composite_z"].median()
    scores_clean["basket"] = np.where(scores_clean["composite_z"] >= median_z,
                                      "Top 50%\n(high growth)", "Bottom 50%\n(low growth)")

    saugus_row = scores_clean[scores_clean["town"] == SAUGUS]
    saugus_z   = saugus_row["composite_z"].values[0] if len(saugus_row) > 0 else None
    saugus_pct = (scores_clean["composite_z"] < saugus_z).mean() * 100 if saugus_z is not None else None

    outcomes_to_plot = [(k, l, c, h) for k, l, c, h in PORTFOLIO_OUTCOMES
                        if k in panel.columns]
    n_o = len(outcomes_to_plot)

    fig = plt.figure(figsize=(16, 5))
    gs  = fig.add_gridspec(1, 2, width_ratios=[1, 2.5], wspace=0.35)
    ax_hist = fig.add_subplot(gs[0])
    ax_bar  = fig.add_subplot(gs[1])

    # --- Histogram ---
    vals = scores_clean["composite_z"]
    ax_hist.hist(vals[vals < median_z], bins=25, color="#E05C4A", alpha=0.75, label="Bottom 50%")
    ax_hist.hist(vals[vals >= median_z], bins=25, color="#4A90D9", alpha=0.75, label="Top 50%")
    if saugus_z is not None:
        ax_hist.axvline(saugus_z, color="#9B59B6", linewidth=2.5)
        ax_hist.text(saugus_z, ax_hist.get_ylim()[1] * 0.85,
                     f" Saugus\n {saugus_pct:.0f}th pct",
                     color="#9B59B6", fontsize=8.5, va="top")
    ax_hist.axvline(median_z, color="#333", linewidth=1.2, linestyle="--", label="Median")
    ax_hist.set_xlabel("Composite growth score\n(avg z-score: spending + teachers + Ch70)", fontsize=9)
    ax_hist.set_ylabel("Number of towns", fontsize=9)
    ax_hist.set_title("Town Composite\nGrowth Score", fontsize=10, fontweight="bold")
    ax_hist.legend(fontsize=8)

    # --- Outcome bar chart: top vs bottom ---
    eval_year = FORMATION_END + lag
    basket_outcomes = scores_clean.merge(
        panel[panel["year"] == eval_year], on="town", how="inner"
    )

    x = np.arange(n_o)
    w = 0.35
    top_means, top_sems, bot_means, bot_sems = [], [], [], []
    for out_key, *_ in outcomes_to_plot:
        if out_key not in basket_outcomes.columns:
            top_means.append(np.nan); top_sems.append(np.nan)
            bot_means.append(np.nan); bot_sems.append(np.nan)
            continue
        top = basket_outcomes[basket_outcomes["basket"].str.startswith("Top")][out_key].dropna()
        bot = basket_outcomes[basket_outcomes["basket"].str.startswith("Bottom")][out_key].dropna()
        top_means.append(top.mean()); top_sems.append(top.sem())
        bot_means.append(bot.mean()); bot_sems.append(bot.sem())

    ax_bar.bar(x - w/2, top_means, w, color="#4A90D9", alpha=0.85,
               label="Top 50% (high growth basket)", edgecolor="white")
    ax_bar.bar(x + w/2, bot_means, w, color="#E05C4A", alpha=0.85,
               label="Bottom 50% (low growth basket)", edgecolor="white")
    ax_bar.errorbar(x - w/2, top_means, top_sems, fmt="none", color="#333",
                    capsize=3, linewidth=1)
    ax_bar.errorbar(x + w/2, bot_means, bot_sems, fmt="none", color="#333",
                    capsize=3, linewidth=1)

    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels([l for _, l, _, _ in outcomes_to_plot],
                            rotation=35, ha="right", fontsize=8.5)
    for tick, (_, _, cat, _) in zip(ax_bar.get_xticklabels(), outcomes_to_plot):
        tick.set_color(CAT_COLOURS.get(cat, "#333"))
    ax_bar.legend(fontsize=9, loc="upper right")
    ax_bar.grid(axis="y", linestyle="--", alpha=0.4)
    lag_desc = f"{lag} year{'s' if lag != 1 else ''} after formation"
    ax_bar.set_title(
        f"Top vs Bottom Composite Basket — outcomes {lag_desc}\n"
        f"Top basket = towns growing fastest across all 3 spending/staffing factors",
        fontsize=10, fontweight="bold"
    )
    ax_bar.set_ylabel("Mean outcome value", fontsize=9)

    return fig


def make_saugus_positioning(quartiles: pd.DataFrame, scores: pd.DataFrame) -> plt.Figure:
    """
    Summary page: where does Saugus sit on each factor?
    Horizontal bar chart with percentile rank, quartile label.
    """
    saugus_row = quartiles[quartiles["town"] == SAUGUS]
    saugus_scores = scores[scores["town"] == SAUGUS]

    if saugus_row.empty:
        return None

    rows = []
    for f, label in ALL_FACTORS.items():
        pct_col = f"{f}_pct"
        q_col   = f"{f}_q"
        if pct_col not in quartiles.columns:
            continue
        pct = saugus_row[pct_col].values[0] if len(saugus_row) > 0 else np.nan
        q   = str(saugus_row[q_col].values[0]) if q_col in saugus_row.columns else "n/a"
        rows.append({"factor": label, "pct": pct, "quartile": q,
                     "is_change": f in CHANGE_FACTORS})

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("pct")

    fig, ax = plt.subplots(figsize=(10, max(4, len(df) * 0.75 + 2)))
    colours = ["#27AE60" if r["is_change"] else "#4A90D9" for _, r in df.iterrows()]
    bars = ax.barh(range(len(df)), df["pct"], color=colours, alpha=0.8, edgecolor="white")

    ax.axvline(25,  color="#E05C4A",  linewidth=1.2, linestyle="--", alpha=0.7)
    ax.axvline(50,  color="#F0AD4E",  linewidth=1.2, linestyle="--", alpha=0.7)
    ax.axvline(75,  color="#27AE60",  linewidth=1.2, linestyle="--", alpha=0.7)
    ax.text(25,  -0.7, "Q4/Q3\nboundary", ha="center", va="top", fontsize=7, color="#E05C4A")
    ax.text(50,  -0.7, "median",          ha="center", va="top", fontsize=7, color="#888")
    ax.text(75,  -0.7, "Q2/Q1\nboundary", ha="center", va="top", fontsize=7, color="#27AE60")

    ax.set_yticks(range(len(df)))
    ax.set_yticklabels(df["factor"], fontsize=9)
    for tick, clr in zip(ax.get_yticklabels(), colours):
        tick.set_color(clr)

    # Quartile labels at end of bars
    for i, (bar, row) in enumerate(zip(bars, df.itertuples())):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                f"{row.pct:.0f}th pct  {row.quartile}",
                va="center", ha="left", fontsize=8.5, color="#333")

    ax.set_xlim(0, 110)
    ax.set_xlabel("Percentile rank among all MA towns\n(higher = more / growing faster)", fontsize=9)
    ax.set_title(
        f"Saugus Factor Positioning (avg 2012–2020)\n"
        "Green bars = growth-rate factors  ·  Blue bars = level factors\n"
        "Percentile 100 = highest in the state",
        fontsize=10, fontweight="bold"
    )

    legend_patches = [
        mpatches.Patch(color="#27AE60", label="Growth-rate factor (YoY change)"),
        mpatches.Patch(color="#4A90D9", label="Level factor (absolute value)"),
    ]
    ax.legend(handles=legend_patches, fontsize=8.5, loc="lower right")

    if "composite_z" in scores.columns:
        all_z = scores["composite_z"].dropna()
        saugus_z = saugus_scores["composite_z"].values[0] if len(saugus_scores) > 0 else None
        if saugus_z is not None:
            saugus_pct = (all_z < saugus_z).mean() * 100
            ax.text(0.98, 0.02,
                    f"Composite growth score: {saugus_pct:.0f}th percentile",
                    transform=ax.transAxes, ha="right", va="bottom",
                    fontsize=9.5, fontweight="bold", color="#9B59B6",
                    bbox=dict(boxstyle="round,pad=0.3", fc="#E8DAEF", ec="#9B59B6"))

    plt.tight_layout()
    return fig


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    engine = get_engine()
    os.makedirs(os.path.dirname(OUTPUT_PDF), exist_ok=True)

    print("[portfolio] Building panel (lag=1 for factor scores)...")
    panel = load_panel(engine, lag=1)
    print(f"[portfolio] Panel: {len(panel):,} rows, {panel['town'].nunique()} towns, "
          f"years {panel['year'].min():.0f}–{panel['year'].max():.0f}")

    print("[portfolio] Computing factor scores (2012–2020 formation window)...")
    scores    = compute_town_factor_scores(panel)
    scores    = compute_composite_score(scores)
    quartiles = assign_quartiles(scores)

    n_towns_with_data = quartiles["town"].nunique()
    print(f"[portfolio] {n_towns_with_data} towns have factor scores")

    saugus_in = SAUGUS in quartiles["town"].values
    if saugus_in:
        s = quartiles[quartiles["town"] == SAUGUS].iloc[0]
        print(f"[portfolio] Saugus found:")
        for f in ALL_FACTORS:
            pct_col = f"{f}_pct"
            q_col   = f"{f}_q"
            if pct_col in s.index:
                print(f"  {f}: {s[pct_col]:.0f}th pct  (quartile {s.get(q_col,'—')})")
    else:
        print(f"[portfolio] WARNING: Saugus not found in factor data")

    def _save(pdf, fig, label=""):
        if fig is None:
            return
        try:
            pdf.savefig(fig, bbox_inches="tight")
        except Exception as e:
            print(f"  [warn] Could not save {label}: {e}")
        finally:
            plt.close(fig)

    print("[portfolio] Generating PDF...")
    with PdfPages(OUTPUT_PDF) as pdf:
        _save(pdf, make_cover(), "cover")

        # Saugus positioning slide
        _save(pdf, make_saugus_positioning(quartiles, scores), "saugus")

        # For each lag: Q1-Q4 spread heatmap
        for lag in [1, 3, 5]:
            print(f"  Spread heatmap lag={lag}...")
            _save(pdf, make_spread_heatmap(panel, quartiles, lag), f"spread heatmap lag={lag}")

        # For each lag: composite basket comparison
        for lag in [1, 3, 5]:
            print(f"  Composite basket lag={lag}...")
            _save(pdf, make_composite_slide(panel, scores, lag), f"composite lag={lag}")

        # Factor distributions
        print("  Factor distributions...")
        for f, lab in ALL_FACTORS.items():
            _save(pdf, make_factor_distribution(scores, quartiles, f, lab), f"dist {f}")

        # Per-factor quartile outcome charts (lag 3 only — most balanced horizon)
        print("  Per-factor quartile charts (lag=3)...")
        for f, lab in ALL_FACTORS.items():
            _save(pdf, make_quartile_outcome_chart(panel, quartiles, f, lab, lag=3),
                  f"quartile chart {f}")

    import shutil
    shutil.copy2(OUTPUT_PDF, _FINAL_PDF)
    print(f"[portfolio] Report saved to {_FINAL_PDF}")


if __name__ == "__main__":
    main()
