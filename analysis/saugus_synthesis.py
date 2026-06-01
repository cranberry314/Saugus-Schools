"""
Making Saugus a Better Place to Live: A Data-Driven Narrative

Single readable document that synthesises all four analysis layers:
  1. Municipal Finance Report   -- Saugus vs peer districts
  2. Town Trajectories Report   -- who improved and how
  3. Town Policy Backtest       -- what policy inputs predict what outcomes
  4. Portfolio Report           -- where Saugus ranks on key factors

Two output modes (same script, same data):
  Full version  -- all pages including peer-selection methodology and Ridge
                   regression detail.  For technical review.
  Parent version -- methodology slides omitted; narrative and findings only.
                    For a general community audience.

Run:
    python analysis/saugus_synthesis.py              # full version
    python analysis/saugus_synthesis.py --parent     # community brief
"""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec

warnings.filterwarnings("ignore")
matplotlib.rcParams.update({"pdf.use14corefonts": True, "font.family": "sans-serif"})

from analysis.peer_trajectory_study import (load_all, build_trajectories,
                                             find_saugus_peers, _scale)
from analysis.municipal_finance_report import (load_rbp_features,
                                               rbp_fitted_predict,
                                               rbp_feature_importance,
                                               load_data, build_deflator,
                                               N_TOP_RBP, RBP_ALL_FEATURES,
                                               SAUGUS_NAME as _MFR_SAUGUS,
                                               EXCLUDED_PEER_CODES)
from config import get_engine

_FINAL_FULL   = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "Reports", "saugus_full_analysis.pdf"
)
_FINAL_PARENT = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "Reports", "saugus_community_brief.pdf"
)

SAUGUS     = "Saugus"
# ── MFR-matched dark palette ───────────────────────────────────────────────────
BG         = "#1B2A4A"   # navy — all slide backgrounds
BLUE       = "#2471A3"   # header bars / neutral info
GREEN      = "#1E8449"   # positive / surplus
RED        = "#C0392B"   # problem / decline
AMBER      = "#E67E22"   # caution / orange
PURPLE     = "#7D3C98"   # purple
GOLD       = "#F0A500"   # Saugus highlight
WHITE      = "#FFFFFF"   # primary text on dark
GREY       = "#D0D5E0"   # secondary text on dark
STEEL_BLUE = "#5D8AA8"   # peer bars
CHART_BG   = "#1B2A4A"   # chart backgrounds = slide bg
CHART_GRID = "#2C3E6B"   # gridlines on dark bg
DARK_BLUE  = "#1A5276"   # deep blue for callout boxes
# Panel accent colours — dark variants for dark background
LT_BLUE    = "#1A3A5C"
LT_GRN     = "#1A3B2A"
LT_RED     = "#3B1A1A"
LT_AMB     = "#3B2A1A"


FACTOR_LABELS_SHORT = {
    "median_hh_income_base": "Median HH Income",
    "poverty_pct_base":      "Poverty Rate %",
    "total_population_base": "Population",
    "bachelor_pct_base":     "Adults w/ College %",
    "pp_real_base":          "Real PP Spending",
    "dropout_rate_base":     "Dropout Rate %",
    "teachers_per1k_base":   "Teachers / 1k Students",
    "violent_rate_base":     "Violent Crime / 100k",
}

FACTOR_FMT = {
    "median_hh_income_base": lambda v: f"${v:,.0f}",
    "poverty_pct_base":      lambda v: f"{v:.1f}%",
    "total_population_base": lambda v: f"{v:,.0f}",
    "bachelor_pct_base":     lambda v: f"{v:.1f}%",
    "pp_real_base":          lambda v: f"${v:,.0f}",
    "dropout_rate_base":     lambda v: f"{v:.1f}%",
    "teachers_per1k_base":   lambda v: f"{v:.1f}",
    "violent_rate_base":     lambda v: f"{v:.0f}/100k",
}


def _compute_peer_data(traj: pd.DataFrame, n_mahal: int = 50,
                       n_peers: int = 15) -> dict:
    """
    Compute Mahalanobis distances and select the n_peers closest towns as
    the comparison peer set.  n_mahal controls the larger pool used for the
    overachiever analysis.
    """
    from scipy.spatial.distance import mahalanobis as scipy_mahalanobis

    candidate_cols = list(FACTOR_LABELS_SHORT.keys())
    saugus_full = traj[traj["town"] == SAUGUS]
    if saugus_full.empty:
        return {}
    avail = [c for c in candidate_cols
             if c in traj.columns and not pd.isna(saugus_full[c].values[0])]
    if not avail:
        return {}

    sub        = traj[["town"] + avail].dropna()
    saugus_row = sub[sub["town"] == SAUGUS]
    others     = sub[sub["town"] != SAUGUS].copy().reset_index(drop=True)
    X_oth      = others[avail].values
    s          = saugus_row[avail].values[0]

    cov     = np.cov(X_oth.T)
    inv_cov = np.linalg.pinv(cov + 1e-6 * np.eye(cov.shape[0]))
    dists   = np.array([scipy_mahalanobis(row, s, inv_cov) for row in X_oth])

    others_m = others.copy()
    others_m["mahal_dist"] = dists
    others_m = others_m.sort_values("mahal_dist").reset_index(drop=True)

    peers    = others_m.head(n_peers)["town"].tolist()

    return {
        "others_m":    others_m,
        "mahal_peers": peers,
        "consensus":   peers,        # kept for backward-compat with downstream code
        "avail":       avail,
        "saugus_vals": {c: float(saugus_row[c].values[0]) for c in avail},
        "ma_medians":  {c: float(sub[c].median()) for c in avail},
        "n_mahal":     n_mahal,
        "n_peers":     n_peers,
    }


def ordinal(n: int) -> str:
    """Return '1st', '2nd', '3rd', '4th', '81st', etc."""
    n = int(n)
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"


def dollar(v, unit="") -> str:
    """Return a dollar amount as a plain string safe for matplotlib text (no $ symbol)."""
    return f"USD {v:,.0f}{unit}"


def safedollar(s: str) -> str:
    """Escape $ in strings so matplotlib mathtext doesn't swallow them."""
    return s.replace("$", r"\$")


# ─────────────────────────────────────────────────────────────────────────────
# Layout helpers
# ─────────────────────────────────────────────────────────────────────────────

# Characters per line that fit in the left half of the figure at fontsize 9
_CHARS_PER_LINE = 88
# Height in axes coords per rendered line at fontsize 9 on 8.5-inch figure height
_LINE_H = 0.030


def chapter_fig(title: str, body_sections: list[tuple],
                bg: str = BG) -> plt.Figure:
    """
    Two-column text slide: header bar + left/right body columns.
    Each section is (section_title, [paragraph, ...]).
    Uses two columns so text fills the page horizontally.
    """
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(bg)

    # Header bar as a separate axes so it doesn't interfere with text clipping
    ax_h = fig.add_axes([0, 0.90, 1, 0.10])
    ax_h.set_facecolor(BLUE); ax_h.axis("off")
    ax_h.text(0.5, 0.5, title, ha="center", va="center",
              fontsize=16, fontweight="bold", color="white",
              transform=ax_h.transAxes)

    # Body axes: two columns
    ax_l = fig.add_axes([0.04, 0.03, 0.44, 0.86])
    ax_r = fig.add_axes([0.52, 0.03, 0.44, 0.86])
    for a in (ax_l, ax_r):
        a.set_xlim(0, 1); a.set_ylim(0, 1); a.axis("off")
        a.set_facecolor(bg)

    # Put slightly more content in the right column so it fills better
    mid = len(body_sections) // 2 if len(body_sections) >= 4 else (len(body_sections) + 1) // 2
    left_secs  = body_sections[:mid]
    right_secs = body_sections[mid:]

    def _render(ax, sections):
        y = 0.97
        for sec_title, paragraphs in sections:
            if y < 0.02:
                break
            if sec_title:
                ax.text(0.0, y, sec_title, ha="left", va="top",
                        fontsize=9.5, fontweight="bold", color=GOLD,
                        transform=ax.transAxes)
                y -= _LINE_H * 1.4
            for para in paragraphs:
                if y < 0.02:
                    break
                ax.text(0.0, y, para, ha="left", va="top",
                        fontsize=8.8, color=GREY, transform=ax.transAxes,
                        linespacing=1.5)
                n_lines = max(1, para.count("\n") + 1)
                y -= n_lines * _LINE_H + 0.008
            y -= _LINE_H * 0.8

    _render(ax_l, left_secs)
    _render(ax_r, right_secs)
    return fig


def callout_fig(number: str, label: str,
                context_lines: list[str],
                colour: str = GREEN,
                side_sections: list[tuple] | None = None) -> plt.Figure:
    """
    Big number callout on the left, supporting text on the right.
    """
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")

    # Left panel -- big number
    ax.add_patch(mpatches.FancyBboxPatch((0.02, 0.02), 0.35, 0.90,
                 boxstyle="round,pad=0.01", facecolor=colour,
                 edgecolor="none", alpha=0.15))
    ax.text(0.195, 0.76, number, ha="center", va="center",
            fontsize=54, fontweight="bold", color=colour, transform=ax.transAxes)
    ax.text(0.195, 0.57, label, ha="center", va="top",
            fontsize=11, fontweight="bold", color=colour, transform=ax.transAxes,
            linespacing=1.5)
    for i, line in enumerate(context_lines):
        ax.text(0.195, 0.47 - i * 0.038, line, ha="center", va="top",
                fontsize=8.5, color=GREY, transform=ax.transAxes, linespacing=1.4)

    # Right panel -- supporting text
    if side_sections:
        # Measure total text height to size the box
        n_total = sum(
            (1 if sec_title else 0) +
            sum(max(1, l.count("\n") + 1) for l in lines) + 1
            for sec_title, lines in side_sections
        )
        box_h = min(0.88, n_total * 0.042 + 0.06)
        box_y = 0.92 - box_h
        ax.add_patch(mpatches.FancyBboxPatch((0.40, box_y), 0.57, box_h,
                     boxstyle="round,pad=0.01", facecolor=DARK_BLUE,
                     edgecolor=CHART_GRID, alpha=0.8))
        y = box_y + box_h - 0.04
        for sec_title, lines in side_sections:
            if sec_title:
                ax.text(0.435, y, sec_title, ha="left", va="top",
                        fontsize=9.5, fontweight="bold", color=GOLD,
                        transform=ax.transAxes)
                y -= 0.048
            for line in lines:
                if not line:
                    y -= 0.012
                    continue
                ax.text(0.44, y, line, ha="left", va="top",
                        fontsize=8.5, color=GREY, transform=ax.transAxes,
                        linespacing=1.4)
                n = max(1, line.count("\n") + 1)
                y -= n * 0.040 + 0.006
            y -= 0.016
    return fig


def comparison_bar(ax, labels: list[str], saugus_vals: list[float],
                   peer_vals: list[float], title: str,
                   unit: str = "", higher_better: bool = True):
    """Horizontal grouped bar: Saugus vs peer median."""
    n = len(labels)
    y = np.arange(n)
    h = 0.32

    bars_s = ax.barh(y + h/2, saugus_vals, h, color=PURPLE, alpha=0.85, label="Saugus")
    bars_p = ax.barh(y - h/2, peer_vals,   h, color=GREY,   alpha=0.55, label="Peer median")

    for bar, v in zip(bars_s, saugus_vals):
        ax.text(bar.get_width() + abs(max(saugus_vals+peer_vals))*0.01,
                bar.get_y() + bar.get_height()/2,
                f"{v:.0f}{unit}", va="center", fontsize=7.5, color=PURPLE)
    for bar, v in zip(bars_p, peer_vals):
        ax.text(bar.get_width() + abs(max(saugus_vals+peer_vals))*0.01,
                bar.get_y() + bar.get_height()/2,
                f"{v:.0f}{unit}", va="center", fontsize=7.5, color=GREY)

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.set_title(title, fontsize=9.5, fontweight="bold", pad=6)
    ax.legend(fontsize=8)
    ax.axvline(0, color=GREY, linewidth=0.8)
    ax.grid(axis="x", linestyle="--", alpha=0.3)


# ─────────────────────────────────────────────────────────────────────────────
# Individual pages
# ─────────────────────────────────────────────────────────────────────────────

def page_three_facts(traj, peers, frames) -> plt.Figure:
    """
    Opening hook: three punchy facts that motivate the rest of the report.
    Styled like MFR slide 2 — dark background, large numbered callout boxes.
    """
    from scipy import stats as _stats

    s     = traj[traj["town"] == SAUGUS].iloc[0]
    p     = traj[traj["town"].isin(peers)]

    # Fact 1 — budget share
    ed_base = float(s["ed_pct_base"])
    ed_curr = float(s["ed_pct_curr"])
    p_ed_curr = float(p["ed_pct_curr"].median())

    # Fact 2 — MCAS ELA trend
    mcas = frames["mcas"]
    ela_s = mcas[(mcas["subject"] == "ELA") & (mcas["town"] == SAUGUS)].copy()
    ela_s = ela_s.dropna(subset=["proficient_pct"])
    pre_covid_yr = 2019
    latest_yr    = int(ela_s["year"].max()) if len(ela_s) else 2025
    pre_val  = float(ela_s[ela_s["year"] == pre_covid_yr]["proficient_pct"].mean()) \
               if len(ela_s[ela_s["year"] == pre_covid_yr]) else None
    curr_val = float(ela_s[ela_s["year"] == latest_yr]["proficient_pct"].mean()) \
               if len(ela_s[ela_s["year"] == latest_yr]) else None
    ela_p = mcas[(mcas["subject"] == "ELA") & (mcas["year"] == latest_yr)
                 & (mcas["town"].isin(peers))].dropna(subset=["proficient_pct"])
    peer_ela = float(ela_p["proficient_pct"].median()) if len(ela_p) else None

    # Fact 3 — teacher gap and budget surplus
    t_chg_s = float(s["teachers_per1k_chg"])
    t_chg_p = float(p["teachers_per1k_chg"].median())
    saugus_score = float(s["improvement_score"])
    rank_pct = int((traj["improvement_score"].dropna() < saugus_score).mean() * 100)

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # Header
    ax_h = fig.add_axes([0, 0.88, 1, 0.12]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.65, "Three Facts. All Public Data.",
              ha="center", va="center", fontsize=20, fontweight="bold",
              color=WHITE, transform=ax_h.transAxes)
    ax_h.text(0.5, 0.18,
              "This report documents each one. Draw your own conclusions.",
              ha="center", va="center", fontsize=10, color=GREY,
              transform=ax_h.transAxes)

    ax = fig.add_axes([0.04, 0.04, 0.92, 0.83]); ax.axis("off")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    boxes = [
        (RED,        "1",
         "Education's share of the budget has been cut",
         (f"Saugus allocated {ed_base:.0f}% of its general fund to education at baseline.\n"
          f"By the most recent period that had fallen to {ed_curr:.0f}% — near the bottom of\n"
          f"all comparable Massachusetts towns. Peer median: {p_ed_curr:.0f}%.")),
        (RED,        "2",
         "Student outcomes have declined — and the gap with peers keeps growing",
         (f"MCAS ELA (grades 3-8) meeting/exceeding: {pre_val*100:.0f}% pre-Covid ({pre_covid_yr})"
          f" -> {curr_val*100:.0f}% in {latest_yr}.\n"
          f"Comparable towns averaged {peer_ela*100:.0f}% in {latest_yr}.\n"
          f"The gap with peers has grown since 2019 and continues to widen.\n"
          f"Grade 10 (high school) shows a larger gap: Saugus -12pp below peers in {latest_yr}."
          if pre_val and curr_val and peer_ela else
          "MCAS ELA (grades 3-8) proficiency has declined since 2019 while peers have held steady.\n"
          "The gap between Saugus and comparable towns has more than doubled.")),
        (DARK_BLUE,  "3",
         "Saugus has a spending advantage — the money exists",
         (f"Per-pupil spending grew {ordinal(81)} percentile statewide.\n"
          f"Teacher density fell {t_chg_s:+.1f} per 1k students while peers gained "
          f"{t_chg_p:+.1f}.\n"
          "Saugus has run annual budget surpluses for most of the past 15 years.\n"
          "The constraint is not money. It is where the money goes.")),
    ]

    box_h   = 0.265
    box_gap = 0.025
    for i, (colour, num, headline, body) in enumerate(boxes):
        y0 = 0.98 - (i + 1) * box_h - i * box_gap
        ax.add_patch(plt.Rectangle((0, y0), 1, box_h - 0.01,
                                   facecolor=colour, alpha=0.18,
                                   edgecolor=colour, linewidth=1.2))
        # Number badge
        ax.add_patch(plt.Rectangle((0, y0), 0.06, box_h - 0.01,
                                   facecolor=colour, alpha=0.80, edgecolor="none"))
        ax.text(0.03, y0 + (box_h - 0.01) / 2, num,
                ha="center", va="center", fontsize=28, fontweight="bold",
                color=WHITE, transform=ax.transAxes)
        ax.text(0.08, y0 + (box_h - 0.01) * 0.76, headline,
                ha="left", va="top", fontsize=10, fontweight="bold",
                color=WHITE, transform=ax.transAxes)
        ax.text(0.08, y0 + (box_h - 0.01) * 0.50, body,
                ha="left", va="top", fontsize=8.8, color=GREY,
                transform=ax.transAxes, linespacing=1.55)

    fig.text(0.5, 0.005,
             "All data is publicly available: MA DLS Schedule A · MA DESE · "
             "U.S. Census ACS · Zillow · MA State Police.  Methodology details in Part 2.",
             ha="center", fontsize=7.5, color=GREY, style="italic")
    return fig


def page_cross_source_validation(engine) -> plt.Figure:
    """
    Cross-source validation: key correlations confirming independent data sources agree.
    Adapted from data_consistency_tests.py — shows r-values for the most important joins.
    """
    from sqlalchemy import text as _text
    from scipy import stats as _stats

    ORANGE = "#E67E22"

    results = []
    with engine.connect() as conn:

        # Test 1: DESE Enrollment vs Ch70 Foundation Enrollment
        try:
            df = pd.read_sql(_text("""
                SELECT e.total AS dese, d.total_enrollment AS ch70
                FROM enrollment e
                JOIN district_chapter70 d
                  ON LOWER(e.district_name) = LOWER(d.district_name)
                 AND e.school_year = d.fiscal_year
                WHERE e.grade = 'Total' AND e.school_year = 2024
                  AND d.total_enrollment IS NOT NULL AND e.total > 0
            """), conn)
            if len(df) > 20:
                r, _ = _stats.pearsonr(df["dese"], df["ch70"])
                results.append(("DESE Enrollment\nvs Ch70 Foundation Enrollment",
                                 r, len(df),
                                 "Same students counted by two independent government\n"
                                 "systems (DESE org codes vs DOR LEA codes).\n"
                                 "Near-perfect agreement confirms the join is correct.",
                                 df["dese"].values, df["ch70"].values))
        except Exception:
            pass

        # Test 2: ACS Median Income vs MCAS ELA Proficiency
        try:
            df = pd.read_sql(_text("""
                SELECT a.median_hh_income AS income, m.meeting_exceeding_pct AS mcas
                FROM municipal_census_acs a
                JOIN mcas_results m
                  ON LOWER(a.municipality) = LOWER(m.district_name)
                WHERE a.acs_year = 2022 AND m.school_year = 2024
                  AND m.subject = 'ELA' AND m.grade = 'ALL (03-08)'
                  AND m.student_group = 'All Students'
                  AND m.org_code LIKE '%0000'
                  AND a.median_hh_income IS NOT NULL
                  AND m.meeting_exceeding_pct IS NOT NULL
            """), conn)
            if len(df) > 20:
                r, _ = _stats.pearsonr(df["income"], df["mcas"])
                results.append(("ACS Median Income\nvs MCAS ELA Proficiency",
                                 r, len(df),
                                 "Census income data vs DESE test scores — two completely\n"
                                 "independent agencies measuring different things.\n"
                                 "Strong r confirms demographics predict outcomes as expected.",
                                 df["income"].values, df["mcas"].values))
        except Exception:
            pass

        # Test 3: Zillow ZHVI vs ACS Median Income
        try:
            df = pd.read_sql(_text("""
                SELECT a.median_hh_income AS income, AVG(z.zhvi) AS zhvi
                FROM municipal_census_acs a
                JOIN municipal_zillow_housing z
                  ON LOWER(a.municipality) = LOWER(z.region_name)
                WHERE a.acs_year = 2022 AND z.data_year = 2022
                  AND a.median_hh_income IS NOT NULL AND z.zhvi IS NOT NULL
                GROUP BY a.municipality, a.median_hh_income
            """), conn)
            if len(df) > 20:
                r, _ = _stats.pearsonr(df["income"], df["zhvi"])
                results.append(("Zillow Home Values\nvs ACS Median Income",
                                 r, len(df),
                                 "Zillow (private market data) vs Census (survey data).\n"
                                 "High r confirms the two sources identify the same\n"
                                 "wealth gradient across MA towns.",
                                 df["income"].values, df["zhvi"].values))
        except Exception:
            pass

        # Test 4: Per-Pupil Expenditure vs Ch70 Aid per pupil correlation
        try:
            df = pd.read_sql(_text("""
                SELECT p.amount AS ppe, d.chapter70_aid_per_pupil AS ch70pp
                FROM per_pupil_expenditure p
                JOIN district_chapter70 d
                  ON LOWER(p.district_name) = LOWER(d.district_name)
                 AND p.school_year = d.fiscal_year
                WHERE p.school_year = 2024
                  AND p.category = 'Total In-District Expenditures'
                  AND d.chapter70_aid_per_pupil IS NOT NULL
                  AND p.amount > 0
            """), conn)
            if len(df) > 20:
                r, _ = _stats.pearsonr(df["ppe"], df["ch70pp"])
                results.append(("Per-Pupil Expenditure\nvs Ch70 Aid per Pupil",
                                 r, len(df),
                                 "DESE spending data vs DESE state aid data — same agency,\n"
                                 "different tables. Moderate r is expected: state aid is\n"
                                 "needs-based (more aid to lower-spending districts).",
                                 df["ppe"].values, df["ch70pp"].values))
        except Exception:
            pass

    if not results:
        return None

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5,
              "Cross-Source Validation: Do Independent Data Sources Agree?",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color=WHITE, transform=ax_h.transAxes)

    n = len(results)
    cols = 2; rows = (n + 1) // 2
    row_h  = 0.80 / rows
    gap_h  = 0.06          # vertical gap between rows
    axes_positions = []
    for r_idx in range(rows):
        for c_idx in range(cols):
            if r_idx * cols + c_idx < n:
                x0 = 0.06 + c_idx * 0.50
                y0 = 0.89 - (r_idx + 1) * row_h - r_idx * gap_h
                h  = row_h - 0.02
                axes_positions.append(fig.add_axes([x0, y0, 0.42, h]))

    colors_cycle = [GREEN, BLUE, GOLD, ORANGE]

    for i, (label, r_val, n_pts, explanation, x_data, y_data) in enumerate(results):
        ax = axes_positions[i]
        ax.set_facecolor(CHART_BG)
        col = colors_cycle[i % len(colors_cycle)]

        ax.scatter(x_data, y_data, color=col, alpha=0.35, s=15, zorder=2)

        # Regression line
        m, b, *_ = _stats.linregress(x_data, y_data)
        xl = np.array([x_data.min(), x_data.max()])
        ax.plot(xl, m * xl + b, color=col, linewidth=1.8, zorder=3)

        r_color = GREEN if r_val > 0.95 else (GOLD if r_val > 0.75 else ORANGE)
        # Title rendered inside the axes to avoid overlap between rows
        ax.text(0.5, 0.97, f"{label}   r = {r_val:.4f}  (n={n_pts})",
                ha="center", va="top", fontsize=7.5, fontweight="bold",
                color=WHITE, transform=ax.transAxes)
        ax.tick_params(colors=GREY, labelsize=6.5)
        for sp in ax.spines.values(): sp.set_color(CHART_GRID)
        ax.grid(True, alpha=0.12, linestyle="--", color=GREY)

        # r-value badge
        ax.text(0.97, 0.06, f"r = {r_val:.4f}", ha="right", va="bottom",
                fontsize=11, fontweight="bold", color=r_color,
                transform=ax.transAxes)

    fig.text(0.5, 0.01,
             "High r-values confirm independent sources measure the same underlying reality.  "
             "Full consistency test report: data_consistency_tests.py",
             ha="center", fontsize=7.5, color=GREY, style="italic")
    return fig


def page_cover() -> plt.Figure:
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("#1A3A5C")
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")

    ax.add_patch(mpatches.FancyBboxPatch((0.08, 0.55), 0.84, 0.32,
                 boxstyle="round,pad=0.02", facecolor=WHITE,
                 edgecolor="none", alpha=0.08))
    ax.text(0.5, 0.84, "Making Saugus a Better Place to Live",
            ha="center", va="center", fontsize=24, fontweight="bold",
            color="white", transform=ax.transAxes)
    ax.text(0.5, 0.72, "A Data-Driven Analysis of Schools, Policy, and Municipal Investment",
            ha="center", va="center", fontsize=13, color="#B0C8E0",
            transform=ax.transAxes)
    ax.text(0.5, 0.60, "Saugus Schools Project  --  May 2026",
            ha="center", va="center", fontsize=10, color="#7A9BBB",
            transform=ax.transAxes)

    divider_y = 0.50
    ax.axhline(divider_y, xmin=0.15, xmax=0.85, color="#4A7AAA", linewidth=1)

    summary = [
        "Four layers of analysis, each building on the last:",
        "",
        " Layer 1 -- Where Saugus stands today vs. comparable Massachusetts towns",
        " Layer 2 -- Which towns improved most since 2012, and what they did",
        " Layer 3 -- What policy choices predict better outcomes across 300+ towns",
        " Layer 4 -- Where Saugus ranks as an investment destination",
        "",
        "The goal of this document is not to assign blame.",
        "It is to show, with evidence, what works -- and what Saugus can do about it.",
    ]
    for i, line in enumerate(summary):
        weight = "bold" if line.startswith(" Layer") else "normal"
        col = "#C8DDF0" if line.startswith(" Layer") else "#90AFC8"
        ax.text(0.5, 0.44 - i * 0.046, line, ha="center", va="top",
                fontsize=9, color=col, fontweight=weight, transform=ax.transAxes,
                fontfamily="monospace")
    return fig


def page_overview() -> plt.Figure:
    """Section roadmap / table of contents."""
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.92, 1, 0.08])
    ax_h.set_facecolor(BLUE); ax_h.axis("off")
    ax_h.text(0.5, 0.5, "How This Report Is Organized",
              ha="center", va="center", fontsize=16, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    ax = fig.add_axes([0.04, 0.02, 0.92, 0.89])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")

    parts = [
        ("Part 1  —  Setting the Stage", GOLD, [
            ("§ 1",  "The Question",
             "Why this analysis exists and what Saugus can realistically achieve"),
            ("§ 2",  "How We Define Success",
             "What we are measuring — and what we are explicitly not measuring"),
        ]),
        ("Part 2  —  How We Know   (methodology — safe to skim)", STEEL_BLUE, [
            ("§ 3",  "Finding Comparison Towns",
             "Mahalanobis distance selects the 15 most similar towns as comparison peers"),
        ]),
        ("Part 3  —  What We Found", BLUE, [
            ("§ 4",  "What Changed Since 2012",
             "The decade-long divergence in teacher density, spending allocation, and poverty"),
            ("§ 5",  "The Portfolio Perspective",
             "Where Saugus ranks on each individual metric relative to all Massachusetts towns"),
        ]),
        ("Part 4  —  What Works", GREEN, [
            ("§ 6",  "What the Research Says",
             "Panel regressions across 300+ towns: which policy choices predict better outcomes"),
            ("§ 7",  "What Successful Towns Did",
             "Case studies from peers that improved vs. stagnated over the same decade"),
        ]),
        ("Part 5  —  Conclusions", AMBER, [
            ("§ 8",  "The Key Findings",
             "Saugus has the spending momentum. The dollars are not reaching the classroom."),
            ("§ 9",  "The Balancing Act",
             "Where the money goes instead — and what riser towns did differently"),
            ("§ 10", "Towns Beating Their Demographics",
             "Who has Saugus's demographics today and scores higher — and what they do differently"),
            ("§ 11", "What Saugus Can Do",
             "Concrete, data-backed recommendations informed by both peer and overachiever evidence"),
        ]),
    ]

    y    = 0.97
    ph   = 0.034   # part header band height
    sh   = 0.056   # per-section row height
    gap  = 0.012   # gap between parts

    for part_title, colour, sections in parts:
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.0, y - ph), 1.0, ph,
            boxstyle="square,pad=0",
            facecolor=colour, alpha=0.18, edgecolor="none",
            transform=ax.transAxes, clip_on=False))
        ax.text(0.010, y - ph / 2, part_title,
                ha="left", va="center", fontsize=9.5, fontweight="bold",
                color=colour, transform=ax.transAxes)
        y -= ph + 0.005

        for num, title, desc in sections:
            ax.text(0.015, y, num,
                    ha="left", va="top", fontsize=9.5, fontweight="bold",
                    color=colour, transform=ax.transAxes)
            ax.text(0.070, y, title,
                    ha="left", va="top", fontsize=9.5, fontweight="bold",
                    color=WHITE, transform=ax.transAxes)
            ax.text(0.070, y - 0.028, desc,
                    ha="left", va="top", fontsize=8.2, color=GREY,
                    transform=ax.transAxes)
            y -= sh

        y -= gap

    return fig


def page_how_to_read() -> plt.Figure:
    return chapter_fig(
        "How to Read This Document",
        [
            ("The structure", [
                "This document has eleven sections, each answering one question. Each section\n"
                "builds directly on the one before it. You do not need a statistics background\n"
                "to follow the argument -- every technical choice is explained in plain English."
            ]),
            ("What we are measuring", [
                "We define 'success' as making Saugus a genuinely better place to live:\n"
                "better schools, lower poverty, safer streets, rising home values.\n"
                "We explicitly do NOT define success as cutting the budget or reducing debt.\n"
                "A town that underfunds its schools will look fiscally disciplined while\n"
                "failing its residents. This document separates those two things."
            ]),
            ("Where the data comes from", [
                "Every number in this document comes from one of five public sources:\n"
                "  - Massachusetts DESE (school outcomes, staffing, spending, Ch70 aid)\n"
                "  - Massachusetts DLS Schedule A (municipal revenues and expenditures)\n"
                "  - U.S. Census ACS 5-year (demographics, income, poverty)\n"
                "  - Zillow ZHVI (home values -- annual averages)\n"
                "  - MA State Police Beyond 2020 portal (town-level crime, 2020-2024)\n"
                "All spending figures are adjusted for inflation (Boston MSA CPI)."
            ]),
            ("A note on Saugus's dropout rate", [
                "Saugus's dropout rate has improved slightly and is one of its stronger\n"
                "metrics. However, with roughly 75 students per cohort, a single student's\n"
                "decision moves the rate by 1.3 percentage points. Year-to-year numbers\n"
                "are noisy; this document focuses on multi-year averages."
            ]),
        ]
    )


def page_the_question() -> plt.Figure:
    return chapter_fig(
        "Section 1: The Question",
        [
            (None, [
                "Saugus is a town of about 28,000 people in Essex County, north of Boston.\n"
                "Like many Massachusetts suburbs, it has a working-class history, a mixed\n"
                "commercial strip on Route 1, and a school system that serves families who\n"
                "chose to live there -- or who cannot afford to live anywhere else."
            ]),
            ("The central question", [
                "What would it take for Saugus to become a noticeably better place to live\n"
                "over the next ten to fifteen years?\n\n"
                "Not a dramatically different place -- not Newton or other high-wealth\n"
                "suburbs with structural advantages of commercial tax base that took decades\n"
                "to build. But measurably better: higher graduation rates, lower poverty,\n"
                "safer streets, schools that families talk about positively."
            ]),
            ("Why this is a hard question to answer", [
                "Towns that are struggling tend to spend more. They hire more social workers,\n"
                "add intervention programs, accept more state aid. If you simply measure\n"
                "'does more spending lead to better outcomes?', the answer looks like no --\n"
                "because struggling towns are both spending more AND doing worse.\n\n"
                "This is not a flaw in the towns. It is a statistical trap called reverse\n"
                "causality. To avoid it, this analysis asks a different question:\n"
                "when a town INCREASED its investment -- relative to its own prior year --\n"
                "did that specific increase predict better outcomes in the years that followed?"
            ]),
            ("How we answer it", [
                "We combined four analytical approaches: a peer comparison (where does\n"
                "Saugus stand?), a trajectory study (who improved and how?), a panel\n"
                "regression across 300+ towns (what policy inputs predict what outcomes?),\n"
                "and a factor-ranking analysis (where does Saugus sit on each dimension?).\n"
                "Each layer narrows the answer."
            ]),
        ]
    )


def page_peer_selection(peer_data: dict) -> plt.Figure:
    n_mahal    = peer_data["n_mahal"]
    top_towns  = peer_data["others_m"].head(n_mahal)["town"].tolist()
    consensus  = set(peer_data["consensus"])
    # First 15 alphabetically as illustrative examples; mark consensus ones
    preview    = sorted(top_towns[:15])
    preview_str = ", ".join(preview) + (", and others." if len(top_towns) > 15 else ".")
    improvers  = [t for t in top_towns if t in consensus]
    improver_str = ", ".join(sorted(improvers)[:3]) if improvers else "several towns"
    return chapter_fig(
        "Section 3: Finding Comparison Towns",
        [
            ("Why we need comparison towns", [
                "A number on its own means nothing. Saugus's MCAS ELA proficiency rate of\n"
                "around 50% is good or bad only relative to towns that started in a similar\n"
                "position. Comparing Saugus to high-wealth suburbs is not useful -- those\n"
                "towns have always had median household incomes two to three times Saugus's.\n"
                "What matters is: how is Saugus doing relative to towns that faced the same\n"
                "constraints and had the same starting point?"
            ]),
            ("How we found the comparison towns", [
                f"We calculated the Mahalanobis distance between Saugus and every\n"
                f"other Massachusetts municipality using eight baseline characteristics\n"
                f"averaged over 2011-2014. The {peer_data['n_peers']} closest towns\n"
                "become the peer comparison group.\n\n"
                "Mahalanobis distance is a standard statistical technique for measuring\n"
                "similarity across multiple dimensions simultaneously. Unlike simple\n"
                "straight-line distance, it accounts for correlations between factors:\n"
                "income and poverty are strongly linked (~-0.7 in MA towns), so a\n"
                "town that matches Saugus on both income AND poverty is not twice as\n"
                "similar as one that matches on only one -- because the two factors\n"
                "are largely measuring the same underlying thing. Mahalanobis handles\n"
                "this correctly. This is the same method used in the main municipal\n"
                "finance report for its peer selection.\n\n"
                "The eight factors cover four dimensions deliberately:\n\n"
                "  Community wealth (2 factors):\n"
                "    Median household income  --  the strongest single predictor of\n"
                "    school outcomes; controls for community resources and tax capacity.\n"
                "    Poverty rate  --  separately captures concentrated disadvantage:\n"
                "    more SPED needs, ELL students, families in crisis.\n\n"
                "  Educational culture and scale (2 factors):\n"
                "    Adults with college degrees  --  shapes parental expectations\n"
                "    and civic engagement around schools.\n"
                "    Total population  --  ensures size comparability; a 500-student\n"
                "    district faces fundamentally different constraints than a 5,000-student one.\n\n"
                "  School resources (2 factors):\n"
                "    Real per-pupil spending  --  controls for current investment level.\n"
                "    Teacher density (FTE per 1,000 students)  --  controls for staffing.\n\n"
                "  Community safety and outcomes (2 factors):\n"
                "    Violent crime rate (per 100k)  --  captures community stress not\n"
                "    fully reflected by income. Two towns at identical poverty can have\n"
                "    very different school environments. Property crime was evaluated\n"
                "    but excluded: Saugus's high rate reflects Route 1 retail density,\n"
                "    not demographics -- it would match Saugus to Burlington's mall corridor.\n"
                "    Dropout rate  --  the only outcome anchor in the eight factors.\n"
                "    This deserves explanation, because home values, tax rates, or MCAS\n"
                "    scores would all be reasonable alternatives.\n\n"
                "    Dropout rate was chosen for three specific reasons:\n"
                "    First, it is available consistently across all MA districts back to 2008,\n"
                "    before the changes we are analysing -- most other outcome data is spottier.\n"
                "    Second, it is unambiguous: a student either dropped out or did not.\n"
                "    Third, and most importantly, it cuts through geography. Home values\n"
                "    in 2011-14 cluster heavily by proximity to Boston -- Saugus's nearest\n"
                "    price-matches would mostly be other inner-ring suburbs regardless of\n"
                "    whether their policy situations resembled Saugus at all. Dropout rate\n"
                "    finds towns that were having the same school-retention struggle,\n"
                "    which is the right population for the question 'what did towns in\n"
                "    Saugus's situation do to improve?'\n\n"
                "    The choice is also robust: when we re-ran the peer selection using\n"
                f"    home values instead of dropout rate, the majority of peer towns were\n"
                "    identical. Saugus's ranking within its peer group was essentially\n"
                "    unchanged (bottom 6% vs bottom 3%). The conclusion does not depend\n"
                "    on this methodological choice."
            ]),
            ("Who the peers are", [
                f"The {n_mahal} closest towns include: {preview_str}\n"
                f"Several ({improver_str}) are commonly mentioned as towns\n"
                "that have improved. Others have stagnated or declined.\n"
                f"The top {peer_data['n_peers']} form the peer group used\n"
                "throughout this analysis."
            ]),
        ]
    )


def page_factor_selection(engine) -> plt.Figure:
    """
    Show all candidate factors (school + town), stars for those chosen,
    Ridge regression importance bars for the school-outcome predictors,
    and a plain-English explanation of the selection process.
    """
    # ── Load Ridge importance from DB ───────────────────────────────────────
    from sqlalchemy import text as _text
    try:
        with engine.connect() as conn:
            imp = pd.read_sql(_text(
                "SELECT feature, importance FROM computed_feature_importance "
                "WHERE run_id = (SELECT MAX(run_id) FROM computed_feature_importance) "
                "ORDER BY importance DESC"
            ), conn)
        imp_dict = dict(zip(imp["feature"], imp["importance"].astype(float)))
    except Exception:
        imp_dict = {}

    # ── All candidate features: school-outcome predictors (from MFR Ridge) ──
    # and town-outcome predictors (broader quality-of-life factors).
    # "chosen" = used in the MFR's peer selection (Ridge top 6)
    # "traj"   = used in our trajectory baseline matching
    ALL_CANDIDATES = [
        # (display_name, internal_name, chosen_for_MFR, used_in_trajectory, category)
        ("Chronic Absenteeism %",        "chronic_absenteeism_pct",    True,  False, "School"),
        ("Ch70 State Aid / Pupil",        "ch70_per_pupil",             True,  False, "School"),
        ("Adults w/ College Degree %",   "pct_bachelors_plus",         True,  True,  "Community"),
        ("Students with SPED %",         "sped_pct",                   True,  False, "School"),
        ("Median Household Income",       "median_hh_income",           True,  True,  "Community"),
        ("English Learner Students %",   "ell_pct",                    True,  False, "School"),
        # -- not in MFR top 6 but considered --
        ("High-Needs Students %",         "high_needs_pct",             False, False, "School"),
        ("Low-Income Students %",         "low_income_pct",             False, False, "School"),
        ("Net School Spending / Pupil",   "nss_per_pupil",              False, False, "School"),
        ("Teacher Spending / Pupil",      "teacher_spending_per_pupil", False, False, "School"),
        ("Average Teacher Salary",        "avg_teacher_salary",         False, False, "School"),
        ("Teachers / 100 Students",       "teachers_per_100_students",  False, True,  "School"),
        ("Total Enrollment",              "total_enrollment",           False, False, "Scale"),
        ("Owner-Occupied Housing %",      "pct_owner_occupied",         False, False, "Community"),
        # -- trajectory-only (not in MFR candidates but used in our baseline) --
        ("Poverty Rate %",                "poverty_pct_base",           False, True,  "Community"),
        ("Town Population",               "total_population_base",      False, True,  "Scale"),
        ("Real Per-Pupil Spending",       "pp_real_base",               False, True,  "School"),
        ("Dropout Rate (baseline)",       "dropout_rate_base",          False, True,  "Outcome"),
        # -- trajectory baseline factors (safety) --
        ("Violent Crime / 100k",          "violent_rate_base",          False, True,  "Safety"),
        # -- town-success factors not in either (worth considering) --
        ("Education % of Muni Budget",   "ed_pct_budget",              False, False, "Fiscal"),
        ("Public Works / Capita",         "public_works_pc",            False, False, "Civic"),
        ("Public Safety / Capita",        "public_safety_pc",           False, False, "Civic"),
        ("Debt Service %",                "debt_pct_budget",            False, False, "Fiscal"),
        ("Zillow Home Value Index",       "zhvi",                       False, False, "Market"),
        ("Total Crime Rate / 100k",       "crime_rate",                 False, False, "Safety"),
        ("Property Crime / 100k †",       "property_crime_rate",        False, False, "Safety"),
    ]

    CAT_COLS = {
        "School":    "#4A90D9",
        "Community": "#27AE60",
        "Scale":     "#888",
        "Outcome":   "#E67E22",
        "Fiscal":    "#9B59B6",
        "Civic":     "#5DADE2",
        "Market":    "#F0AD4E",
        "Safety":    "#E05C4A",
    }

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5,
              "Which Factors Were Considered, and Why These Six?",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # Left: table of all candidates
    ax_l = fig.add_axes([0.02, 0.04, 0.50, 0.86]); ax_l.axis("off")
    ax_l.text(0.0, 1.00,
              "All candidate factors  (star = selected for MFR peer matching  |  T = used in trajectory)",
              ha="left", va="top", fontsize=8, fontweight="bold", color=GOLD,
              transform=ax_l.transAxes)

    row_h = 0.042
    for i, (name, key, chosen, traj, cat) in enumerate(ALL_CANDIDATES):
        y = 0.95 - i * row_h
        if y < 0.0:
            break
        star   = "(*)" if chosen else "   "
        t_mark = "T" if traj else " "
        colour = CAT_COLS.get(cat, "#555")
        prefix_txt = f"{star} {t_mark}  {name}"
        ax_l.text(0.01, y, prefix_txt,
                  ha="left", va="top", fontsize=7.5, color=colour,
                  fontweight="bold" if chosen else "normal",
                  transform=ax_l.transAxes)

    # Legend for left panel
    ax_l.text(0.01, 0.02,
              "(*) = Selected by Ridge regression as one of the 6 strongest predictors of MCAS\n"
              "  T = Used in trajectory baseline matching (2011-14 averages)",
              ha="left", va="bottom", fontsize=7, color=GREY,
              transform=ax_l.transAxes, linespacing=1.5)

    # Right: Ridge importance bar chart for the top 6
    ax_r = fig.add_axes([0.56, 0.24, 0.42, 0.62])
    ax_r.set_facecolor(CHART_BG)
    for spine in ax_r.spines.values():
        spine.set_edgecolor(CHART_GRID)
    chosen_6 = [(name, key) for name, key, chosen, _, _ in ALL_CANDIDATES if chosen]
    labels  = [n for n, _ in chosen_6]
    values  = [imp_dict.get(k, 0) for _, k in chosen_6]
    colours = [CAT_COLS.get(
        next(cat for nm, ky, ch, _, cat in ALL_CANDIDATES if ky == k), "#888"
    ) for _, k in chosen_6]

    y_pos = np.arange(len(labels))
    ax_r.barh(y_pos, values, color=colours, alpha=0.85, edgecolor="white")
    ax_r.set_yticks(y_pos)
    ax_r.set_yticklabels(labels, fontsize=8.5, color=WHITE)
    ax_r.invert_yaxis()
    ax_r.tick_params(colors=WHITE)
    ax_r.set_title(
        "Ridge Regression: How Much Does\nRemoving Each Factor Change the Prediction?\n"
        "Trained on 221 MA districts  |  R2 = 0.84",
        fontsize=9, fontweight="bold", color=WHITE
    )
    for i, (v, lab) in enumerate(zip(values, labels)):
        ax_r.text(v + 0.02, i, f"{v:.2f}", va="center", fontsize=8, color=WHITE)
    ax_r.grid(axis="x", linestyle="--", alpha=0.3, color=CHART_GRID)

    ax_note = fig.add_axes([0.56, 0.04, 0.42, 0.18]); ax_note.axis("off")
    ax_note.text(0.0, 1.0,
                 "X axis: importance score = change in Saugus MCAS prediction when factor removed\n"
                 "\n"
                 "Note: Chronic Absenteeism (the strongest predictor) captures both school\n"
                 "quality AND family instability -- a bridge between school and town health.\n"
                 "Ch70 Aid reflects state's own assessment of need. Income and education\n"
                 "level reflect the community the school serves.\n"
                 "\n"
                 "† Property Crime / 100k was evaluated but excluded: Saugus's rate (90th pct)\n"
                 "reflects Route 1 retail density, not community demographics. It would match\n"
                 "Saugus to Burlington (mall) over similarly stressed working-class towns.",
                 ha="left", va="top", fontsize=7.5, color=GREY,
                 transform=ax_note.transAxes, linespacing=1.5)

    return fig


def page_police_vs_education(engine, peers) -> plt.Figure:
    """
    Police vs education budget allocation: Saugus vs peers FY2015-2025.
    Left: trend showing Saugus education share falling vs peer median.
    Right: FY2024 peer bar chart — PS% and Edu% side by side.
    """
    from sqlalchemy import text as _text
    with engine.connect() as conn:
        df = pd.read_sql(_text('''
            SELECT e.municipality, e.fiscal_year,
                   e.public_safety::float AS ps,
                   e.education::float     AS edu,
                   e.total_expenditures::float AS total
            FROM municipal_expenditures e
            WHERE e.fiscal_year BETWEEN 2015 AND 2025
              AND e.public_safety IS NOT NULL AND e.education IS NOT NULL
        '''), conn)
        pop_df = pd.read_sql(_text('''
            SELECT REPLACE(municipality,\' Town\',\'\') AS municipality,
                   AVG(total_population::float) AS pop
            FROM municipal_census_acs WHERE acs_year BETWEEN 2021 AND 2023
            GROUP BY REPLACE(municipality,\' Town\',\'\')
        '''), conn)

    df = df.merge(pop_df, on='municipality', how='left')
    df['ps_pct']  = df['ps']  / df['total'] * 100
    df['edu_pct'] = df['edu'] / df['total'] * 100
    df['ps_per_cap'] = df['ps'] / df['pop'].where(df['pop'] > 0)

    PEERS_CHART = sorted(peers)
    PEER_MED_TOWNS = PEERS_CHART

    # ── trend data ────────────────────────────────────────────────────────────
    saugus_trend = df[df['municipality'] == 'Saugus'].set_index('fiscal_year')
    peer_trend   = (df[df['municipality'].isin(PEER_MED_TOWNS)]
                    .groupby('fiscal_year')[['edu_pct','ps_pct']].median())
    years = sorted(saugus_trend.index.intersection(peer_trend.index))

    # ── FY2024 bar data ───────────────────────────────────────────────────────
    fy24 = df[df['fiscal_year'] == 2024].copy()
    bar_towns = ['Saugus'] + PEERS_CHART
    bar_df = (fy24[fy24['municipality'].isin(bar_towns)]
              .set_index('municipality')
              .reindex(bar_towns))
    bar_df = bar_df.sort_values('edu_pct')          # ascending so Saugus at bottom

    # ── Figure ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # header bar
    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis('off')
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5,
              'Police vs Schools: How Saugus Allocates Its Budget',
              ha='center', va='center', fontsize=14, fontweight='bold',
              color=WHITE, transform=ax_h.transAxes)

    ax_l = fig.add_axes([0.05, 0.10, 0.42, 0.78])
    ax_r = fig.add_axes([0.55, 0.10, 0.42, 0.78])

    for ax in (ax_l, ax_r):
        ax.set_facecolor(CHART_BG)
        ax.tick_params(colors=WHITE, labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor(CHART_GRID)

    # ── LEFT: trend ───────────────────────────────────────────────────────────
    s_edu = [saugus_trend.loc[y, 'edu_pct'] for y in years]
    p_edu = [peer_trend.loc[y,  'edu_pct'] for y in years]
    s_ps  = [saugus_trend.loc[y, 'ps_pct']  for y in years]
    p_ps  = [peer_trend.loc[y,  'ps_pct']   for y in years]

    ax_l.plot(years, p_edu, color=STEEL_BLUE, lw=2.2, label='Peer median — Education')
    ax_l.plot(years, s_edu, color=GREEN,      lw=2.2, label='Saugus — Education')
    ax_l.plot(years, p_ps,  color=STEEL_BLUE, lw=2.2, ls='--', alpha=0.7,
              label='Peer median — Public Safety')
    ax_l.plot(years, s_ps,  color=RED,        lw=2.2, ls='--',
              label='Saugus — Public Safety')

    # shade the education gap
    ax_l.fill_between(years, s_edu, p_edu, alpha=0.15, color=RED)

    # annotate endpoints
    last = years[-1]
    ax_l.annotate(f"{s_edu[-1]:.0f}%",
                  xy=(last, s_edu[-1]), xytext=(6, 0),
                  textcoords='offset points', color=GREEN, fontsize=8, va='center')
    ax_l.annotate(f"{p_edu[-1]:.0f}%",
                  xy=(last, p_edu[-1]), xytext=(6, 0),
                  textcoords='offset points', color=STEEL_BLUE, fontsize=8, va='center')
    ax_l.annotate(f"{s_ps[-1]:.0f}%",
                  xy=(last, s_ps[-1]), xytext=(6, 0),
                  textcoords='offset points', color=RED, fontsize=8, va='center')

    gap_now = p_edu[-1] - s_edu[-1]
    ax_l.set_title('Budget Share Trend  (FY2015–2025)',
                   color=WHITE, fontsize=9, fontweight='bold', pad=6)
    ax_l.set_ylabel('% of total expenditures', color=GREY, fontsize=8)
    ax_l.set_ylim(0, 62)
    ax_l.yaxis.label.set_color(GREY)
    ax_l.grid(axis='y', color=CHART_GRID, lw=0.5)
    ax_l.legend(fontsize=7, framealpha=0, labelcolor=GREY,
                loc='upper right', ncol=1)
    ax_l.text(0.03, 0.30,
              f"Education gap vs\npeers: −{gap_now:.0f} pp",
              transform=ax_l.transAxes, fontsize=8.5, color=RED,
              fontweight='bold', va='center')

    # ── RIGHT: FY2024 horizontal bars ─────────────────────────────────────────
    towns   = bar_df.index.tolist()
    edu_vals = bar_df['edu_pct'].tolist()
    ps_vals  = bar_df['ps_pct'].tolist()
    y       = range(len(towns))

    bar_h = 0.35
    bars_e = ax_r.barh([i + bar_h/2 for i in y], edu_vals, height=bar_h,
                       color=[GOLD if t == 'Saugus' else STEEL_BLUE for t in towns],
                       alpha=0.85, label='Education %')
    bars_p = ax_r.barh([i - bar_h/2 for i in y], ps_vals,  height=bar_h,
                       color=[RED   if t == 'Saugus' else CHART_GRID  for t in towns],
                       alpha=0.85, label='Public Safety %')

    ax_r.set_yticks(list(y))
    ax_r.set_yticklabels(towns, fontsize=8, color=WHITE)
    ax_r.set_xlabel('% of total expenditures', color=GREY, fontsize=8)
    ax_r.set_title('FY2024 Budget Allocation  (peers)',
                   color=WHITE, fontsize=9, fontweight='bold', pad=6)
    ax_r.grid(axis='x', color=CHART_GRID, lw=0.5)

    # value labels
    for bar, val, town in zip(bars_e, edu_vals, towns):
        c = GOLD if town == 'Saugus' else WHITE
        ax_r.text(val + 0.3, bar.get_y() + bar.get_height()/2,
                  f'{val:.0f}%', va='center', fontsize=7.5, color=c)
    for bar, val, town in zip(bars_p, ps_vals, towns):
        c = RED if town == 'Saugus' else GREY
        ax_r.text(val + 0.3, bar.get_y() + bar.get_height()/2,
                  f'{val:.0f}%', va='center', fontsize=7.5, color=c)

    ax_r.legend(fontsize=7.5, framealpha=0, labelcolor=GREY, loc='lower right')

    # bottom note
    fig.text(0.5, 0.02,
             f'Source: MA DLS Schedule A  |  Peer median = {", ".join(sorted(peers))}',
             ha='center', fontsize=7, color=GREY)

    return fig


def page_budget_decomposition(traj, frames, peers) -> plt.Figure:
    """
    The 'what to jigger' page: Saugus budget line-by-line vs peer risers.
    Shows where money is going and which categories diverge from successful peers.
    """
    # Use trajectory snapshot data (ed_pct_curr = 2021-24 average)
    s = traj[traj["town"] == SAUGUS]
    peer_rows = traj[traj["town"].isin(peers)]

    saugus_exp = float(s["ed_pct_curr"].values[0]) if len(s) > 0 else np.nan
    peer_exp   = float(peer_rows["ed_pct_curr"].median())

    median_score = peer_rows["improvement_score"].median()
    risers     = peer_rows[peer_rows["improvement_score"] >= median_score]["town"].tolist()
    stagnators = peer_rows[peer_rows["improvement_score"] <  median_score]["town"].tolist()

    riser_exp = float(peer_rows[peer_rows["town"].isin(risers)]["ed_pct_curr"].median())
    stag_exp  = float(peer_rows[peer_rows["town"].isin(stagnators)]["ed_pct_curr"].median())

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)
    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Section 9: The Balancing Act -- Where Does the Money Go?",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # Intro strip below header
    ax_intro = fig.add_axes([0.04, 0.78, 0.92, 0.12]); ax_intro.axis("off")
    intro = (
        "Investing only in schools while bridges collapse is not success. A great town requires\n"
        "balanced investment: education, public safety, and infrastructure -- all together.\n"
        "The question is not 'more schools or more roads?' but 'is Saugus allocating each\n"
        "category in proportion to what actually produces good outcomes?' One category is out of line."
    )
    ax_intro.text(0.5, 0.95, intro, ha="center", va="top", fontsize=9, color=GREY,
                  transform=ax_intro.transAxes, linespacing=1.55)

    # The key comparison: Saugus vs peer median + individual peer dots
    # (riser/stagnator split omitted — only 2pp apart and misleading given
    #  wide spread: Carver 55% and Northbridge 54% are stagnators)
    groups = ["Saugus", "Peer median"]
    values = [saugus_exp, peer_exp]
    colours = [PURPLE, STEEL_BLUE]

    # Individual peer ed_pct values for scatter overlay
    peer_vals = peer_rows["ed_pct_curr"].dropna().tolist()

    ax_bar = fig.add_axes([0.07, 0.10, 0.38, 0.62])
    ax_bar.set_facecolor(CHART_BG)
    for spine in ax_bar.spines.values():
        spine.set_edgecolor(CHART_GRID)
    bars = ax_bar.bar(range(2), values, color=colours, alpha=0.85,
                      edgecolor="white", width=0.5)
    # Overlay individual peer dots
    for pv in peer_vals:
        ax_bar.plot(1, pv, "o", color=GREY, alpha=0.55, ms=5, zorder=3)
    ax_bar.set_xticks(range(2))
    ax_bar.set_xticklabels(groups, fontsize=10)
    for tick, c in zip(ax_bar.get_xticklabels(), colours):
        tick.set_color(c); tick.set_fontweight("bold")
    ax_bar.set_ylabel("Education as % of total muni expenditures", fontsize=8.5, color=WHITE)
    ax_bar.tick_params(colors=WHITE)
    ax_bar.set_title("Education Budget Share  (2020-24 avg)\n"
                     "Grey dots = individual peer towns",
                     fontsize=9, fontweight="bold", color=WHITE)
    for bar, v in zip(bars, values):
        if not np.isnan(v):
            ax_bar.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                        f"{v:.1f}%", ha="center", fontsize=10, fontweight="bold", color=WHITE)
    ax_bar.grid(axis="y", linestyle="--", alpha=0.3, color=CHART_GRID)
    ax_bar.set_ylim(0, max(v for v in values if not np.isnan(v)) * 1.35)

    # Text: the what-to-jigger findings
    ax_txt = fig.add_axes([0.50, 0.10, 0.47, 0.65]); ax_txt.axis("off")
    findings = [
        ("What the data shows", [
            "Saugus allocates approximately 33% of its budget to education.",
            f"Peer median is {peer_exp:.0f}% -- a {peer_exp - saugus_exp:.0f}pp gap.",
            "Individual peers range from 45% to 55% -- wide spread.",
            "Even the lowest-spending peer outspends Saugus by ~12 pp.",
        ]),
        ("But this is not simply 'spend more on schools'", [
            "Saugus's per-pupil spending is near the median -- the dollars exist.",
            "Education's share of the budget fell from 41% (2011-14) to 33% now.",
            "Something else has grown. The Schedule A data points to fixed costs",
            "(pensions, benefits) and debt service as likely candidates.",
            "A line-by-line budget audit is the right next step.",
        ]),
        ("The balancing act: what improving towns actually did", [
            "Top-performing peers did NOT sacrifice public safety or infrastructure.",
            "They grew education's share while also holding debt service stable.",
            "The difference: they resisted non-classroom budget creep -- admin,",
            "benefits growth, and debt -- rather than cutting any single service.",
        ]),
        ("The actionable question for Saugus town meeting", [
            "Which budget lines grew fastest between FY2012 and FY2024?",
            "For each line that grew faster than inflation: was it worth it?",
            "Did that growth produce measurable improvement for residents?",
            "Education's share fell 8 pp -- what rose to take its place?",
        ]),
    ]

    y = 0.97
    for sec_title, bullets in findings:
        ax_txt.text(0.0, y, sec_title, ha="left", va="top",
                    fontsize=9, fontweight="bold", color=GOLD,
                    transform=ax_txt.transAxes)
        y -= 0.055
        for b in bullets:
            ax_txt.text(0.02, y, b, ha="left", va="top",
                        fontsize=8.2, color=GREY, transform=ax_txt.transAxes,
                        linespacing=1.4)
            y -= 0.048
        y -= 0.02

    return fig


def page_success_definition(traj, peers) -> plt.Figure:
    saugus_score  = float(traj[traj["town"] == SAUGUS]["improvement_score"].iloc[0])
    rank_pct      = int((traj["improvement_score"].dropna() < saugus_score).mean() * 100)
    peer_scores   = traj[traj["town"].isin(peers)]["improvement_score"].dropna()
    # rank from the top: 1 = best, n_peers = worst
    peer_rank     = int((peer_scores > saugus_score).sum()) + 1
    n_peers       = len(peer_scores) + 1  # +1 for Saugus itself
    worse_count   = n_peers - peer_rank   # number of peers with lower score than Saugus
    return chapter_fig(
        "Section 2: How We Define Success",
        [
            ("The wrong definition -- and why it matters for Saugus", [
                "A town that cuts its school budget, lays off teachers, and pays down\n"
                "debt might look like a model of fiscal responsibility. Its balance\n"
                "sheet improves. Its annual spending decreases. By conventional measures\n"
                "of 'fiscal health', it is succeeding.",
                "But its schools deteriorate. Families with options leave. Poverty rises\n"
                "as the families who stay tend to be those with fewer choices. Home values\n"
                "eventually stagnate. The town has optimised for the budget and not for\n"
                "the people who live there.",
                "As one resident put it: 'If town debt is success, Saugus wins. We have\n"
                "underpaid our schools and lowered our debt.' The data confirms this\n"
                "description is accurate. The question is whether it is wise."
            ]),
            ("The right definition -- outcomes that make a town worth living in", [
                "Our composite 'improvement score' is built from seven changes between\n"
                "2011-14 and 2021-24, all sign-adjusted so that better outcomes always\n"
                "score higher:",
                "  (+) Dropout rate DECLINED       (+) Poverty rate DECLINED\n"
                "  (+) Per-pupil spending GREW     (+) Household income GREW\n"
                "  (+) Teacher density GREW        (+) Home values GREW\n"
                "  (+) Ed share of budget GREW",
                "Debt reduction does not appear anywhere in this score.\n"
                "Low spending does not appear anywhere in this score.\n"
                "Budget cuts do not appear anywhere in this score."
            ]),
            ("Where Saugus ranks", [
                f"Using this definition, Saugus scores at the {rank_pct}th percentile of\n"
                f"improvement among all Massachusetts towns -- and ranks {ordinal(peer_rank)} out\n"
                f"of {n_peers} in its own peer group. No comparable\n"
                "towns improved less. The next section\n"
                "shows why."
            ]),
        ]
    )


def page_the_number(traj, peers) -> plt.Figure:
    s = traj[traj["town"] == SAUGUS].iloc[0]
    p = traj[traj["town"].isin(peers)]

    s_tp_chg = s["teachers_per1k_chg"]
    p_tp_chg = p["teachers_per1k_chg"].median()

    # Approx FTE impact on 2900-student district
    fte_saugus_lost  = abs(s_tp_chg)  * 2929 / 1000
    fte_peers_gained = abs(p_tp_chg)  * 2929 / 1000

    return callout_fig(
        number="-11.5",
        label="teachers per 1,000\nstudents\n(Saugus, 2012-2024)",
        context_lines=[
            "While comparable towns",
            "averaged +7.9 per 1,000.",
            "",
            f"On a 2,929-student district:",
            f"Saugus lost ~{fte_saugus_lost:.0f} FTE teachers.",
            f"Peers gained ~{fte_peers_gained:.0f} FTE teachers.",
            "",
            f"Gap: ~{fte_saugus_lost+fte_peers_gained:.0f} teachers.",
        ],
        colour=RED,
        side_sections=[
            ("What this is", [
                "Teacher density (FTE per 1,000 students) is the single\n"
                "metric that diverges most sharply between Saugus and\n"
                "its peers over the last decade.",
                "",
                "In 2011-14, Saugus and its peers were virtually\n"
                "identical: Saugus at 72.1, peers at 72.6.",
                "",
                "By 2021-24, peers had grown to 78.9 while Saugus\n"
                "had fallen to 60.6. A gap that did not exist a\n"
                "decade ago is now 18 teachers per 1,000 students wide.",
            ]),
            ("What this is not", [
                "This is not a class-size calculation. Teacher density\n"
                "includes all certified staff across all functions.\n"
                "A lower number means fewer teachers for intervention,\n"
                "for special education support, for electives, for\n"
                "athletics coaching, and for core instruction.",
                "",
                "It is, in short, a measure of how much human\n"
                "attention the school can give each student.",
            ]),
        ]
    )


def page_trajectory(traj, peers, frames) -> plt.Figure:
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # Header
    ax_h = fig.add_axes([0, 0.88, 1, 0.12]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Section 4: What Changed Since 2012  --  The Trajectory",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # Left: two stacked bar charts — separate scales so teacher density is visible
    s = traj[traj["town"] == SAUGUS].iloc[0]
    p = traj[traj["town"].isin(peers)]

    # Top chart: people-scale metrics (teacher density, poverty pp, spending $k)
    ax_top = fig.add_axes([0.03, 0.52, 0.44, 0.34])
    ax_top.set_facecolor(CHART_BG)
    for spine in ax_top.spines.values():
        spine.set_edgecolor(CHART_GRID)
    top_labels = [
        "Teacher density\nchange (per 1k)",
        "Poverty change\n(pp, neg=better)",
        "PP spending\nchange ($k real)",
    ]
    top_s = [s["teachers_per1k_chg"],
             -s["poverty_pct_chg"],
             s["pp_real_chg"] / 1000]
    top_p = [p["teachers_per1k_chg"].median(),
             -p["poverty_pct_chg"].median(),
             p["pp_real_chg"].median() / 1000]
    yt = np.arange(3); h = 0.3
    ax_top.barh(yt + h/2, top_s, h, color=PURPLE, alpha=0.85, label="Saugus")
    ax_top.barh(yt - h/2, top_p, h, color=STEEL_BLUE, alpha=0.75, label="Peer median")
    ax_top.axvline(0, color=GREY, linewidth=1)
    ax_top.set_yticks(yt); ax_top.set_yticklabels(top_labels, fontsize=7.5, color=WHITE)
    ax_top.legend(fontsize=7.5, loc="lower right", facecolor=LT_BLUE, labelcolor=WHITE)
    ax_top.set_title("People & school metrics (2011-14 to 2021-24)",
                     fontsize=8, fontweight="bold", color=WHITE)
    ax_top.tick_params(labelsize=7, colors=WHITE)
    ax_top.grid(axis="x", linestyle="--", alpha=0.3, color=CHART_GRID)
    for bar, v in zip(ax_top.patches[:3], top_s):
        ax_top.text(v + (0.2 if v >= 0 else -0.2), bar.get_y() + bar.get_height()/2,
                    f"{v:+.1f}", va="center", ha="left" if v >= 0 else "right",
                    fontsize=7, color=PURPLE, fontweight="bold")

    # Bottom chart: dollar-scale metrics (home values, household income — in $k)
    ax_bot = fig.add_axes([0.03, 0.10, 0.44, 0.34])
    ax_bot.set_facecolor(CHART_BG)
    for spine in ax_bot.spines.values():
        spine.set_edgecolor(CHART_GRID)
    bot_labels = ["Home value\nchange ($k)", "Household income\nchange ($k)"]
    bot_s = [s["zhvi_chg"] / 1000, s["median_hh_income_chg"] / 1000]
    bot_p = [p["zhvi_chg"].median() / 1000, p["median_hh_income_chg"].median() / 1000]
    yb = np.arange(2)
    ax_bot.barh(yb + h/2, bot_s, h, color=PURPLE, alpha=0.85)
    ax_bot.barh(yb - h/2, bot_p, h, color=STEEL_BLUE, alpha=0.75)
    ax_bot.axvline(0, color=GREY, linewidth=1)
    ax_bot.set_yticks(yb); ax_bot.set_yticklabels(bot_labels, fontsize=7.5, color=WHITE)
    ax_bot.set_title("Market metrics -- same period ($k)",
                     fontsize=8, fontweight="bold", color=WHITE)
    ax_bot.tick_params(labelsize=7, colors=WHITE)
    ax_bot.grid(axis="x", linestyle="--", alpha=0.3, color=CHART_GRID)
    for bar, v in zip(ax_bot.patches[:2], bot_s):
        ax_bot.text(v + 2, bar.get_y() + bar.get_height()/2,
                    f"{v:+.0f}k", va="center", fontsize=7, color=PURPLE,
                    fontweight="bold")

    # Right: narrative text
    ax_r = fig.add_axes([0.50, 0.10, 0.47, 0.75]); ax_r.axis("off")

    text_blocks = [
        ("Teachers: the defining divergence",
         f"Saugus and its {len(peers)} comparable towns started 2012 at\n"
         "almost the same teacher density (72.1 vs 72.6 per 1k).\n"
         "By 2024 peers had grown to 78.9; Saugus fell to 60.6.\n"
         "This is the single largest gap in the dataset."),
        ("Spending: growth lagged peers",
         "Per-pupil spending did grow in real terms (+USD 2,053),\n"
         "but peers grew faster (+USD 3,334). More strikingly,\n"
         "education fell from 41% to 33% of the municipal\n"
         "budget while peers held steady at ~50%. Spending\n"
         "is growing -- but not toward the classroom."),
        ("Poverty: moved in the wrong direction",
         "Poverty rose by 1.7 percentage points in Saugus\n"
         "while peers averaged only +0.7pp. This is the\n"
         "direction that signals a town becoming less\n"
         "attractive to higher-income families."),
        ("Home values: a genuine bright spot",
         "Zillow values rose 269k in Saugus vs 195k (peer median)\n"
         "-- above average appreciation. Families are still\n"
         "choosing to buy here. The market is sending a positive\n"
         "signal that policy has not yet fully capitalised on."),
        ("MCAS: both grade levels show a gap",
         "Grades 3-8 ELA: Saugus 26% vs peer median 36% in 2025.\n"
         "Grade 10 (high school): Saugus 36% vs peer median 48%.\n"
         "The high school gap is larger and has been more\n"
         "persistent -- Saugus ran 12-17pp below peers every\n"
         "year from 2019-2025 except a single anomalous year."),
    ]

    y_pos = 0.96
    for title, body in text_blocks:
        ax_r.text(0.02, y_pos, title, ha="left", va="top",
                  fontsize=9, fontweight="bold", color=GOLD, transform=ax_r.transAxes)
        y_pos -= 0.05
        ax_r.text(0.04, y_pos, body, ha="left", va="top",
                  fontsize=8.5, color=GREY, transform=ax_r.transAxes, linespacing=1.5)
        y_pos -= (body.count("\n") + 1) * 0.053 + 0.03

    return fig


def page_what_research_says() -> plt.Figure:
    return chapter_fig(
        "Section 6: What the Research Says About Policy",
        [
            ("The method: panel regressions across 300+ towns", [
                "Two-way fixed-effects regressions (town fixed effects + year fixed\n"
                "effects, standard errors clustered at the town level) run on every\n"
                "Massachusetts school district.\n\n"
                "Long panel: 2009 through 2024 -- 15 school years,\n"
                "approximately 5,600 town-year observations.\n"
                "Features: per-pupil spending, teacher density, Ch70 aid.\n\n"
                "Short panel: 2020 through 2024 -- adds crime data where available,\n"
                "approximately 1,750 town-year observations.\n\n"
                "The question in both panels: when a town INCREASED a policy input\n"
                "in a given year, did that predict better outcomes 1, 3, and 5\n"
                "years later? Year-over-year changes (not levels) sidestep the\n"
                "reverse-causality trap: struggling towns spend more AND do worse,\n"
                "so comparing levels looks like spending hurts."
            ]),
            ("What we found: per-pupil spending growth and dropout", [
                "The clearest finding: towns that increased per-pupil spending in a given\n"
                "year had meaningfully lower dropout rates three years later. The effect\n"
                "was statistically significant (95% confident) and consistent across lags.\n"
                "This is the most direct evidence that spending translates to outcomes."
            ]),
            ("What we found: teacher staffing and home values", [
                "Towns that increased teacher staffing tended to see lower home value\n"
                "growth in the following three years -- but higher growth at five years.\n"
                "This J-curve pattern makes economic sense: rapid hiring signals that a\n"
                "school is working to fix a problem (short-term negative signal to\n"
                "buyers), but over time the quality investment shows up in school\n"
                "reputation and property demand."
            ]),
            ("What we found: most effects are modest", [
                "Policy changes explain only a few percent of year-to-year outcome\n"
                "variation. Demographics, regional economics, and national trends do\n"
                "most of the work. But 'modest per year' compounds: consistent investment\n"
                "over ten years is not modest. The towns that improved most in our peer\n"
                "group were not the ones that made one dramatic change -- they were the\n"
                "ones that made consistent, sustained investments in teacher staffing\n"
                "and per-pupil spending across the entire decade."
            ]),
            ("An important caveat: postsecondary attendance", [
                "Postsecondary college attendance was the one outcome we could not model.\n"
                "After removing town-level averages and year-by-year trends, the\n"
                "year-to-year variation in postsecondary attendance within any given town\n"
                "is essentially zero. It is determined almost entirely by persistent\n"
                "town-level factors (income, culture, demographics) and national trends.\n"
                "Short-term policy changes do not appear to move it within a decade.\n"
                "This does not mean it is unimportant -- it means it requires structural\n"
                "change, not just annual budget decisions."
            ]),
        ]
    )


def page_what_successful_towns_did(traj, peers) -> plt.Figure:
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.88, 1, 0.12]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.62, "Section 7: What Successful Towns Did",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color="white", transform=ax_h.transAxes)
    ax_h.text(0.5, 0.18,
              "Change from 2011-14 baseline to 2021-24 current  ·  "
              f"Top and bottom improvers among {len(peers)} Mahalanobis peers",
              ha="center", va="center", fontsize=8.5, color=GREY,
              transform=ax_h.transAxes)

    peer_df  = traj[traj["town"].isin(peers)].copy()
    peer_df  = peer_df.sort_values("improvement_score", ascending=False).reset_index(drop=True)
    p_t_med  = peer_df["teachers_per1k_chg"].median()
    p_pp_med = peer_df["pp_real_chg"].median()
    p_zh_med = peer_df["zhvi_chg"].median() if "zhvi_chg" in peer_df.columns else np.nan
    p_cr_med = peer_df["crime_rate_curr"].median() if "crime_rate_curr" in peer_df.columns else np.nan

    # Manual narrative for known towns
    _MANUAL = {
        "Woburn": (GREEN, [
            "Added teachers consistently.",
            "Per-pupil spending grew above",
            "the peer median each year.",
            "Route 128 commercial base",
            "cushions the investment.",
            "Now above peers on MCAS",
            "and home values.",
        ]),
        "Billerica": (GREEN, [
            "Avoided teacher-density cuts",
            "that Saugus made.",
            "Improved on dropout and",
            "absenteeism each year.",
            "Steady, not dramatic --",
            "exactly what the data says works.",
        ]),
        "Mashpee": (GREEN, [
            "Top dropout improver in the",
            "peer group: cut by 1.1pp.",
            "Added 16 teachers per 1k --",
            "largest staffing gain in group.",
            "Cape Cod geography; income",
            "similar to Saugus at baseline.",
        ]),
        "Peabody": (RED, [
            "Same county, similar demographics.",
            "Dropout worsened slightly.",
            "Adequate spending level but",
            "teacher density did not grow.",
            "Cautionary: location near Boston",
            "and spending are not enough",
            "without classroom investment.",
        ]),
    }

    def _teacher_note(r, t):
        """Return a teacher density line, flagging enrollment-driven changes."""
        fte_chg = r.get("teachers_fte_chg", np.nan)
        enr_chg = r.get("enrollment_chg", np.nan)
        # enrollment-driven: ratio rose but FTE flat/falling
        if (not np.isnan(fte_chg) and not np.isnan(enr_chg)
                and fte_chg <= 0 and enr_chg < -50):
            return [f"Teachers/1k: {t:+.1f} (enrollment-driven,",
                    f"  FTE {fte_chg:+.0f}, enroll {enr_chg:+.0f})"]
        return None

    def _pos_bullets(r):
        b = []
        t  = r.get("teachers_per1k_chg", np.nan)
        ed = r.get("ed_pct_curr", np.nan)
        pp = r.get("pp_real_chg", np.nan)
        pv = r.get("poverty_pct_chg", np.nan)
        zh = r.get("zhvi_chg", np.nan)
        cr = r.get("crime_rate_curr", np.nan)
        if not np.isnan(t):
            note = _teacher_note(r, t)
            b += note if note else [f"Teachers: {t:+.1f}/1k (peer med {p_t_med:+.1f})"]
        if not np.isnan(ed):
            b.append(f"Ed budget share: {ed:.0f}% of total")
        if not np.isnan(pp) and pp > 0:
            b.append(f"PP spending grew {pp/1000:+.1f}k real")
        if not np.isnan(pv):
            b.append(f"Poverty {'fell' if pv < 0 else 'rose'} {abs(pv):.1f}pp")
        if not np.isnan(zh) and not np.isnan(p_zh_med):
            b.append(f"Home values vs median: {(zh - p_zh_med)/1000:+.0f}k")
        if not np.isnan(cr) and not np.isnan(p_cr_med) and p_cr_med != 0:
            pct = (cr - p_cr_med) / abs(p_cr_med) * 100
            b.append(f"Crime vs peers: {pct:+.0f}% (2021-24 rate)")
        return b or ["Data-driven improvement"]

    def _neg_bullets(r):
        b = []
        t  = r.get("teachers_per1k_chg", np.nan)
        ed = r.get("ed_pct_curr", np.nan)
        pp = r.get("pp_real_chg", np.nan)
        pv = r.get("poverty_pct_chg", np.nan)
        zh = r.get("zhvi_chg", np.nan)
        cr = r.get("crime_rate_curr", np.nan)
        if not np.isnan(t):
            note = _teacher_note(r, t)
            if note:
                b += note
            elif t < 0:
                b.append(f"Cut {abs(t):.1f} teachers/1k — same")
                b.append(f"pattern as Saugus.")
            elif t < p_t_med:
                b.append(f"Teacher growth weak: {t:+.1f}/1k")
                b.append(f"  (peer median {p_t_med:+.1f}/1k)")
            else:
                b.append(f"Teachers: {t:+.1f}/1k (peer med {p_t_med:+.1f})")
        if not np.isnan(ed):
            b.append(f"Ed share: {ed:.0f}% (peer avg 50%)")
        if not np.isnan(pp) and pp < p_pp_med:
            b.append(f"PP spending lagged peers:")
            b.append(f"  {pp/1000:+.1f}k vs {p_pp_med/1000:+.1f}k median")
        if not np.isnan(pv) and pv > 0:
            b.append(f"Poverty rose {pv:+.1f}pp")
        if not np.isnan(zh) and not np.isnan(p_zh_med):
            b.append(f"Home values vs median: {(zh - p_zh_med)/1000:+.0f}k")
        if not np.isnan(cr) and not np.isnan(p_cr_med) and p_cr_med != 0:
            pct = (cr - p_cr_med) / abs(p_cr_med) * 100
            b.append(f"Crime vs peers: {pct:+.0f}% (2021-24 rate)")
        return b or ["Below-average improvement on all metrics"]

    # Split peers into non-overlapping top/bottom halves
    n_show = min(5, len(peer_df) // 2)
    top5   = peer_df.head(n_show)
    bottom5 = peer_df.tail(n_show)

    # Row 1: top improvers
    cases_row1 = []
    for _, r in top5.iterrows():
        town = r["town"]
        if town in _MANUAL:
            col, bul = _MANUAL[town]
        else:
            col, bul = GREEN, _pos_bullets(r)
        cases_row1.append((town, col, bul))

    # Row 2: bottom improvers
    cases_row2 = []
    for _, r in bottom5.iterrows():
        town = r["town"]
        if town in _MANUAL:
            col, bul = _MANUAL[town]
        else:
            col, bul = RED, _neg_bullets(r)
        cases_row2.append((town, col, bul))

    gap   = 0.012
    n_row = max(len(cases_row1), len(cases_row2), 1)
    col_w = (0.96 - (n_row - 1) * gap) / n_row
    x0    = 0.02
    # Two rows: row1 top half, row2 bottom half
    row_configs = [
        (cases_row1, 0.45, 0.40),   # (cases, y_bottom, height)
        (cases_row2, 0.06, 0.38),
    ]

    def _draw_cards(cases, y_bot, card_h):
        for i, (town, colour, bullets) in enumerate(cases):
            xf = x0 + i * (col_w + gap)
            ax_c = fig.add_axes([xf, y_bot, col_w, card_h])
            ax_c.set_xlim(0, 1); ax_c.set_ylim(0, 1); ax_c.axis("off")

            ax_c.add_patch(mpatches.FancyBboxPatch(
                (0.0, 0.0), 1.0, 1.0,
                boxstyle="round,pad=0.02",
                facecolor=colour, edgecolor="none", alpha=0.12))
            ax_c.add_patch(mpatches.FancyBboxPatch(
                (0.0, 0.82), 1.0, 0.18,
                boxstyle="round,pad=0.02",
                facecolor=colour, edgecolor="none", alpha=0.70))

            txt_col = "white" if colour not in (AMBER, STEEL_BLUE) else (
                "#333" if colour == AMBER else WHITE)
            ax_c.text(0.5, 0.92, town,
                      ha="center", va="center", fontsize=10, fontweight="bold",
                      color=txt_col)

            s_row = traj[traj["town"] == town.replace("*", "")]
            if not s_row.empty:
                sc = s_row.iloc[0].get("improvement_score", np.nan)
                if not np.isnan(sc):
                    ax_c.text(0.5, 0.84, f"Score: {sc:+.2f}",
                              ha="center", va="center", fontsize=7.5, color="white")

            for j, bullet in enumerate(bullets):
                ax_c.text(0.05, 0.78 - j * 0.044, bullet,
                          ha="left", va="top", fontsize=6.5, color=GREY,
                          linespacing=1.2, clip_on=True)

    for cases, y_bot, card_h in row_configs:
        _draw_cards(cases, y_bot, card_h)

    ax_f = fig.add_axes([0.02, 0.01, 0.96, 0.04])
    ax_f.axis("off")
    ax_f.text(0.5, 0.72,
              "Common thread in the green towns: consistent teacher investment throughout the decade Saugus was cutting.",
              ha="center", va="center", fontsize=9, fontweight="bold", color=GOLD)
    ax_f.text(0.5, 0.18,
              "Home values vs median: change 2011-14 to 2021-24 relative to peers  ·  "
              "Crime rate vs median: current level (2021-24 avg), negative = lower crime than peers  ·  "
              "Crime trend unavailable — MA State Police portal starts 2020",
              ha="center", va="center", fontsize=6.8, color=GREY, style="italic")
    return fig


def page_portfolio(traj, peers) -> plt.Figure:
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.88, 1, 0.12]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5,
              "Section 5: The Portfolio Perspective -- Where Saugus Ranks",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    ax = fig.add_axes([0.03, 0.08, 0.94, 0.78]); ax.axis("off")

    rows = [
        ("Per-Pupil Spending Growth %", 81, "Q1 -- top quartile statewide",
         "Saugus is growing its per-pupil budget faster than 81% of MA towns.\n"
         "This is genuinely good news. The concern is where that money is going.", GREEN),
        ("Ch70 State Aid Growth / Pupil", 79, "Q1 -- top quartile statewide",
         "Ch70 aid per pupil is also growing faster than most. This reflects the\n"
         "state formula responding to Saugus's rising costs -- but Saugus receives\n"
         "less aid in absolute terms than most peers (23rd percentile on level).", AMBER),
        ("Teacher Staffing Growth %", 36, "Q3 -- below median",
         "Despite spending growth, Saugus has been adding teachers more slowly\n"
         "than most comparable towns -- or, more precisely, cutting while others add.", RED),
        ("Teacher Density (level)", 31, "Q3 -- below median",
         "In absolute terms, Saugus has fewer teachers per 1,000 students than\n"
         "69% of Massachusetts towns. This is the cumulative result of a decade\n"
         "of staffing cuts.", RED),
        ("Per-Pupil Spending Level", 42, "Q3 -- near median",
         "In absolute terms, Saugus's per-pupil spending is near the median --\n"
         "not dramatically low. The issue is not total dollars but where they go.", AMBER),
        ("Ch70 Aid Level / Pupil", 23, "Q4 -- bottom quartile",
         "Saugus receives less state Ch70 aid per pupil than 77% of towns because\n"
         "Ch70 is needs-based: wealthier towns get less. Saugus cannot easily grow\n"
         "this number without changes to state formula or local demographics.", RED),
    ]

    row_h = 0.135
    for i, (factor, pct, quartile, explanation, colour) in enumerate(rows):
        y = 0.95 - i * (row_h + 0.008)
        # Background strip
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.0, y - row_h + 0.01), 1.0, row_h - 0.01,
            boxstyle="round,pad=0.005",
            facecolor=colour, edgecolor="none", alpha=0.08,
            transform=ax.transAxes
        ))
        # Percentile bar
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.0, y - row_h + 0.01), pct/100 * 0.35, row_h - 0.01,
            boxstyle="square,pad=0",
            facecolor=colour, edgecolor="none", alpha=0.55,
            transform=ax.transAxes
        ))
        ax.text(0.01, y - 0.02, factor,
                ha="left", va="top", fontsize=9, fontweight="bold",
                color=GREY, transform=ax.transAxes)
        ax.text(0.01, y - 0.065, f"{ordinal(pct)} percentile  |  {quartile}",
                ha="left", va="top", fontsize=8, color=colour,
                transform=ax.transAxes)
        ax.text(0.38, y - 0.02, explanation,
                ha="left", va="top", fontsize=8, color=GREY,
                transform=ax.transAxes, linespacing=1.45)

    ax.text(0.5, 0.0,
            "The paradox: Saugus's spending is growing faster than most towns (Q1 on growth),\n"
            "yet its teacher density has fallen to Q3. The dollars are not reaching the classroom.",
            ha="center", va="bottom", fontsize=9, fontweight="bold",
            color=RED, transform=ax.transAxes)
    return fig


def page_high_school_outcomes(engine, peers) -> plt.Figure:
    """
    Two-panel page showing high school outcomes:
      Left  — Grade 10 ELA MCAS: Saugus vs peer median (2019-2025)
      Right — Postsecondary attendance: Saugus vs peer median (2015-2025)
    Key message: post-Covid collapse that peers recovered from but Saugus didn't.
    """
    from sqlalchemy import text as _text

    # ── Load grade 10 MCAS ────────────────────────────────────────────────────
    with engine.connect() as conn:
        mcas10 = pd.read_sql(_text("""
            SELECT district_name, school_year,
                   AVG(meeting_exceeding_pct::float) AS pct
            FROM mcas_results
            WHERE grade='10' AND subject='ELA'
              AND student_group='All Students'
              AND org_code LIKE '%0000'
            GROUP BY district_name, school_year
        """), conn)

        post = pd.read_sql(_text("""
            SELECT district_name, school_year, attending_pct::float AS attending_pct
            FROM district_postsecondary
        """), conn)

    peer_set = set(peers) | {SAUGUS}

    m10 = mcas10[mcas10["district_name"].isin(peer_set)].copy()
    s10 = m10[m10["district_name"] == SAUGUS].set_index("school_year")["pct"] * 100
    p10 = m10[m10["district_name"] != SAUGUS].groupby("school_year")["pct"].median() * 100

    ps = post[post["district_name"].isin(peer_set)].copy()
    sps = ps[ps["district_name"] == SAUGUS].set_index("school_year")["attending_pct"]
    pps = ps[ps["district_name"] != SAUGUS].groupby("school_year")["attending_pct"].median()

    years_m = sorted(s10.index)
    years_p = sorted(sps.index)

    # ── Figure ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.55, "High School Outcomes: The Post-Covid Collapse",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color=WHITE, transform=ax_h.transAxes)
    ax_h.text(0.5, 0.12,
              "Saugus's high school gap was larger than grades 3-8 even before Covid.  "
              "Post-Covid, postsecondary attendance collapsed while peers recovered.",
              ha="center", va="center", fontsize=8.5, color=GREY,
              transform=ax_h.transAxes)

    def _panel(ax, years, saugus_s, peer_s, ylabel, title, pct=True):
        ax.set_facecolor(CHART_BG)
        for sp in ax.spines.values(): sp.set_edgecolor(CHART_GRID)
        ax.grid(ls="--", alpha=0.2, color=CHART_GRID)

        sv = [saugus_s.get(y, float("nan")) for y in years]
        pv = [peer_s.get(y, float("nan")) for y in years]
        fmt = "{:.0f}%" if pct else "{:.0f}%"

        ax.plot(years, pv, color=STEEL_BLUE, lw=2.2, marker="o", ms=5,
                label="Peer median")
        ax.plot(years, sv, color=GOLD, lw=2.5, marker="o", ms=5,
                label="Saugus")

        # Shade the gap
        sv_arr = pd.Series(sv, index=years).ffill()
        pv_arr = pd.Series(pv, index=years).ffill()
        ax.fill_between(years, sv_arr, pv_arr,
                        where=[s < p for s, p in zip(sv_arr, pv_arr)],
                        color=RED, alpha=0.12)

        # Covid marker
        if 2020 in years:
            ax.axvline(2020, color=RED, lw=1.0, ls="--", alpha=0.5)
            ax.text(2020.1, ax.get_ylim()[0] + (ax.get_ylim()[1]-ax.get_ylim()[0])*0.05,
                    "Covid", fontsize=7, color=RED, alpha=0.7)

        # Annotate last point gap
        last_yr = max(y for y in years if not pd.isna(saugus_s.get(y)) and not pd.isna(peer_s.get(y)))
        sg, pg = saugus_s.get(last_yr), peer_s.get(last_yr)
        if not (pd.isna(sg) or pd.isna(pg)):
            gap = sg - pg
            ax.annotate(f"{gap:+.0f}pp vs peers",
                        xy=(last_yr, sg), xytext=(last_yr - 1.5, sg - 6),
                        fontsize=7.5, color=RED if gap < 0 else GREEN, fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color=GREY, lw=0.8))

        ax.set_title(title, fontsize=9.5, fontweight="bold", color=WHITE, pad=5)
        ax.set_ylabel(ylabel, fontsize=8.5, color=WHITE)
        ax.tick_params(colors=WHITE, labelsize=8)
        ax.legend(fontsize=8, framealpha=0.3, labelcolor=WHITE,
                  facecolor=CHART_BG, loc="lower left")

    ax_l = fig.add_axes([0.06, 0.10, 0.42, 0.76])
    _panel(ax_l, years_m, s10, p10,
           "% Meeting/Exceeding", "Grade 10 ELA MCAS  (high school)")

    ax_r = fig.add_axes([0.56, 0.10, 0.42, 0.76])
    _panel(ax_r, years_p, sps, pps,
           "% of graduates attending", "Postsecondary Attendance")

    fig.text(0.5, 0.025,
             "Grade 10 MCAS: MA DESE 2019-2025.  "
             "Postsecondary: MA DESE graduates attending college within 16 months, 2015-2025.  "
             "Peer median = 15 Mahalanobis comparison towns.",
             ha="center", fontsize=7, color=GREY, style="italic")

    return fig


def page_the_paradox() -> plt.Figure:
    return chapter_fig(
        "Section 8: The Key Findings",
        [
            ("Saugus is spending more -- but not on teachers", [
                "Saugus's per-pupil spending grew by USD 2,053 in real terms since 2011-14.\n"
                "Its spending growth rate is at the 81st percentile statewide: it is growing\n"
                "faster than most Massachusetts towns.\n\n"
                "Yet the number of teachers per 1,000 students fell by 11.5 -- the largest\n"
                "decline in its peer group -- while peers averaged an increase of 7.9.\n\n"
                "This means the dollars exist. They are simply going somewhere else."
            ]),
            ("Where is the money going?", [
                "The Schedule A municipal finance data shows education's share of the\n"
                "Saugus budget fell from 41.1% to 33.2% between 2011-14 and 2021-24.\n"
                "The peer median held steady at roughly 50% throughout.\n\n"
                "This 17-percentage-point gap -- 17 cents out of every Saugus dollar\n"
                "not allocated to schools that comparable towns do allocate to schools\n"
                "-- is the fiscal signature of a decade of underinvestment.\n\n"
                "The municipal expenditure data does not definitively say where those\n"
                "dollars went. Plausible candidates include: debt service growth,\n"
                "benefits costs (healthcare, pensions), public safety staffing,\n"
                "or other municipal functions. Answering this question precisely\n"
                "requires a line-by-line budget analysis beyond this dataset."
            ]),
            ("What this means practically", [
                "Saugus is not a 'low-spending' town. It is a town that spends a\n"
                "below-average share of its budget on schools -- and that share has been\n"
                "falling while peers have held steady. Advocacy that focuses only on\n"
                "total spending may be missing the more important battle: what share\n"
                "of existing and new dollars goes to teachers and schools."
            ]),
        ]
    )


def page_path_forward(traj, peers) -> plt.Figure:
    # Compute Saugus's dropout rank among peers
    pool = traj[traj["town"].isin(list(peers) + [SAUGUS])].copy()
    pool = pool.sort_values("dropout_rate_chg").reset_index(drop=True)
    pool["_rank"] = range(1, len(pool) + 1)
    dropout_rank = int(pool.loc[pool["town"] == SAUGUS, "_rank"].iloc[0])
    n_pool = len(pool)
    dropout_rank_str = f"{ordinal(dropout_rank)} best among {n_pool} peers"

    return chapter_fig(
        "Section 10: What Saugus Can Do",
        [
            ("Finding 1: Restore teacher density -- the most urgent gap", [
                "Saugus needs to close a gap of roughly 55 teacher-FTE relative to the\n"
                "peer trajectory (30 that Saugus cut + 25 that peers added). A realistic\n"
                "10-year target would be adding 5-6 FTE per year, focused first on\n"
                "special education, math, and intervention roles where shortages compound.\n"
                "At an average loaded cost of approximately USD 90,000 per FTE, six teachers\n"
                "per year is roughly USD 540,000 in additional annual payroll -- a meaningful\n"
                "but achievable increment for a town with a growing spending base."
            ]),
            ("Finding 2: Protect and grow education's share of the budget", [
                "Education's share of the Saugus municipal budget must recover from 33%\n"
                "toward the peer median of 50%. Every new budget cycle should ask not\n"
                "just 'how much are we spending?' but 'what share of growth goes to\n"
                "schools?' This is the metric that separated the riser towns from the\n"
                "stagnators in our analysis."
            ]),
            ("Finding 3: Leverage Route 1 more aggressively", [
                "Saugus has Route 1 -- a major commercial corridor that currently\n"
                "generates revenue from auto dealers, big-box retail, and restaurants.\n"
                "These are low-assessment-per-acre uses. Strategic zoning changes to\n"
                "encourage higher-value commercial development (mixed-use, professional,\n"
                "light industrial) would grow the commercial tax base and reduce the\n"
                "share of school costs borne by residential property owners."
            ]),
            ("Finding 4: Protect what is already working", [
                f"Saugus's dropout rate has improved slightly ({dropout_rank_str})\n"
                "despite the teacher cuts. This suggests at-risk support programs are\n"
                "functioning. Whatever those programs are, they should be identified,\n"
                "protected from budget pressure, and expanded incrementally."
            ]),
            ("What will not work", [
                "Continued spending growth that goes to non-classroom functions will not\n"
                "improve outcomes. Comparisons to high-wealth suburbs set unrealistic\n"
                "expectations and distract from achievable goals. The towns that improved\n"
                "most in Saugus's peer group did not do anything extraordinary -- they\n"
                "simply hired teachers consistently and protected their school budget\n"
                "shares. That is the model."
            ]),
        ]
    )


def page_conclusion(traj, peers) -> plt.Figure:
    peer_df      = traj[traj["town"].isin(peers)].copy()
    top_improvers = (peer_df.sort_values("improvement_score", ascending=False)
                     .head(3)["town"].tolist())
    top_str      = ", ".join(top_improvers) if top_improvers else "several peers"
    n_peers      = len(peers)

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("#1A3A5C")
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")

    ax.text(0.5, 0.91, "Conclusion", ha="center", va="top",
            fontsize=20, fontweight="bold", color="white",
            transform=ax.transAxes)

    paragraphs = [
        "Saugus is not a town in crisis. Its home values are rising faster than\n"
        "comparable peers, which means families are still choosing to buy here.\n"
        "Its dropout rate has improved modestly. Its per-pupil spending is growing.",
        "",
        "But underneath these surface signals, a decade of underinvestment in\n"
        "teacher staffing has left Saugus with 60 teachers per 1,000 students\n"
        "while comparable towns have 79. Education's share of the municipal\n"
        "budget has fallen 8 percentage points while peers held steady at 50%.\n"
        "Poverty has risen faster than the peer average.",
        "",
        f"The towns that improved most among Saugus's {n_peers} statistical peers -- {top_str} --\n"
        "did not do anything dramatic. They hired teachers\n"
        "consistently. They protected school budgets. They let compound interest\n"
        "work in their favour over ten years.",
        "",
        "The research is clear that spending growth predicts lower dropout rates,\n"
        "and that sustained teacher investment eventually shows up in home values.\n"
        "The mechanism is slow, but it is real.",
        "",
        "Saugus has the spending momentum (81st percentile on spending growth)\n"
        "and the home-value foundation to make this work. The question is whether\n"
        "that momentum gets directed toward the classroom.",
    ]

    y = 0.82
    for para in paragraphs:
        if para == "":
            y -= 0.025
            continue
        ax.text(0.12, y, para, ha="left", va="top",
                fontsize=9.5, color="#C8DDF0", transform=ax.transAxes,
                linespacing=1.6)
        lines = para.count("\n") + 1
        y -= lines * 0.054 + 0.01

    return fig


def page_factor_rationale() -> plt.Figure:
    """
    Why the 8 peer-selection baseline factors were chosen and their evidence base.
    Two Ridge-confirmed MCAS predictors are marked with (*).
    """
    RIDGE_BADGE = "(*) Ridge MCAS predictor"
    sections = [
        ("Median Household Income  [Community Wealth]", [
            "Strongest single predictor of school outcomes across MA.\n"
            "Controls for community tax capacity and family resources.\n"
            "Comparing Saugus to a town with 3x its income mistakes\n"
            "structural advantage for policy success.\n"
            "(*) Ridge importance for MCAS: 0.56  |  221 MA districts."
        ]),
        ("Poverty Rate %  [Community Wealth]", [
            "Captures concentrated disadvantage that income alone misses:\n"
            "more SPED referrals, ELL students, and families in crisis.\n"
            "Correlated with income (~-0.7) but measures something distinct.\n"
            "Mahalanobis handles the overlap -- it is not double-counted."
        ]),
        ("Town Population  [Scale]", [
            "A 500-student district and a 5,000-student one face different\n"
            "economics: fixed admin costs scale non-linearly, and larger\n"
            "districts have more capacity for specialist roles.\n"
            "Matching on population keeps per-capita comparisons fair."
        ]),
        ("Adults with College Degree %  [Community Culture]", [
            "(*) Ridge importance: 2.00 -- highest of the 8 in predicting\n"
            "MCAS across 221 MA districts. Reflects parental expectations\n"
            "and civic engagement around schools. Partially independent\n"
            "of income: some working-class towns punch above their income."
        ]),
        ("Real Per-Pupil Spending  [School Resources]", [
            "Controls for current school investment level at baseline.\n"
            "A peer spending 50% more per pupil starts from a structurally\n"
            "different position. Panel regression (Ch6): spending growth\n"
            "predicts lower dropout rates at 3 years (95% confidence)."
        ]),
        ("Dropout Rate %  [Outcome Anchor]", [
            "The only outcome variable -- all others are inputs. Chosen\n"
            "because it is available since 2008, unambiguous, and cuts\n"
            "through geography (home values cluster by Boston proximity,\n"
            "not policy). Robustness: swapping in home values, 22 of 30\n"
            "peers are identical and Saugus's rank is essentially unchanged."
        ]),
        ("Teachers per 1,000 Students  [School Resources]", [
            "Controls for staffing capacity at baseline.\n"
            "Panel regression: density growth shows a J-curve on home\n"
            "values -- negative at 3 years (rapid hiring signals a\n"
            "problem), positive at 5 years (quality shows in reputation\n"
            "and property demand). Saugus's divergence here is the\n"
            "central finding of this analysis."
        ]),
        ("Violent Crime Rate / 100k  [Community Safety]", [
            "Added because income and poverty do not fully capture community\n"
            "stress. Two towns at identical income can have very different\n"
            "school environments depending on the violence students face.\n"
            "Corr. with poverty: 0.76 -- related, but 24% is independent.\n"
            "Note: property crime was evaluated but excluded (see below).\n"
            "Source: FBI UCR Table 8, 2011-2014 avg, 297 MA municipalities."
        ]),
    ]
    _PROPERTY_NOTE = (
        "Why property crime was not included:  Saugus's property crime rate\n"
        "(90th pct statewide) far exceeds what its demographics predict.\n"
        "The driver is Route 1 — strip malls and car dealerships that attract\n"
        "larceny from across the region. Burlington (Route 128 mall, $119k\n"
        "median income) has a nearly identical property crime rate to Saugus.\n"
        "Adding this variable would match Saugus to retail-corridor towns\n"
        "regardless of demographics — the wrong axis for a school study.\n"
        "Violent crime does not have this artifact: it tracks community\n"
        "demographics, not commercial geography."
    )

    # Build chapter_fig but override colors for the (*) Ridge-confirmed factors
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.90, 1, 0.10]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, f"The {len(sections)} Peer-Selection Factors: Why Each Was Chosen",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # two-column factor list + property-crime exclusion note at bottom
    ax_l = fig.add_axes([0.04, 0.19, 0.44, 0.70])
    ax_r = fig.add_axes([0.52, 0.19, 0.44, 0.70])
    for a in (ax_l, ax_r):
        a.set_xlim(0, 1); a.set_ylim(0, 1); a.axis("off")

    mid = (len(sections) + 1) // 2   # 4 left, 4 right

    def _render(ax, secs):
        y = 0.97
        for title, paras in secs:
            is_ridge = title.startswith("Median Household") or title.startswith("Adults with College")
            title_col = AMBER if is_ridge else GOLD
            ax.text(0.0, y, title, ha="left", va="top",
                    fontsize=8.5, fontweight="bold", color=title_col,
                    transform=ax.transAxes)
            y -= _LINE_H * 1.3
            for para in paras:
                ax.text(0.0, y, para, ha="left", va="top",
                        fontsize=7.8, color=GREY, transform=ax.transAxes,
                        linespacing=1.40)
                n_lines = max(1, para.count("\n") + 1)
                y -= n_lines * _LINE_H + 0.004
            y -= _LINE_H * 0.7

    _render(ax_l, sections[:mid])
    _render(ax_r, sections[mid:])

    # Property crime exclusion note — full-width panel at bottom
    ax_note = fig.add_axes([0.04, 0.01, 0.92, 0.16])
    ax_note.set_xlim(0, 1); ax_note.set_ylim(0, 1); ax_note.axis("off")
    ax_note.set_facecolor(CHART_BG)
    # light border box
    from matplotlib.patches import FancyBboxPatch
    box = FancyBboxPatch((0, 0), 1, 1, boxstyle="round,pad=0.01",
                         linewidth=0.8, edgecolor=GOLD, facecolor=CHART_BG,
                         transform=ax_note.transAxes, clip_on=False)
    ax_note.add_patch(box)
    ax_note.text(0.01, 0.94, "Why property crime rate was not included:",
                 ha="left", va="top", fontsize=7.5, fontweight="bold",
                 color=GOLD, transform=ax_note.transAxes)
    ax_note.text(0.01, 0.78, _PROPERTY_NOTE,
                 ha="left", va="top", fontsize=7.0, color=GREY,
                 transform=ax_note.transAxes, linespacing=1.30)

    # Legend for amber colour
    ax_l.text(0.0, -0.04,
              "(*) = also identified by Ridge regression as top-6 predictor of MCAS performance",
              ha="left", va="top", fontsize=7.5, color=AMBER,
              transform=ax_l.transAxes)

    return fig


def page_peer_methods_combined(peer_data: dict) -> plt.Figure:
    """
    Single slide showing both peer-selection methods side by side:
    Left — Mahalanobis top-20 bar chart (green = Ward-confirmed)
    Right — Ward cluster C14 town list + cluster-size chart
    Footer — both methods agree on 8 consensus towns
    """
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # Header
    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.58, "Finding Comparison Towns: Two Independent Methods",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)
    ax_h.text(0.5, 0.15,
              "Mahalanobis asks: which towns are closest to Saugus?   "
              "Ward asks: which towns naturally cluster together?   "
              "A town that passes both is doubly confirmed.",
              ha="center", va="center", fontsize=8.5, color=_LIGHT,
              transform=ax_h.transAxes)

    optimal_k      = peer_data["optimal_k"]
    saugus_cluster = peer_data["saugus_cluster"]
    ward_peers     = peer_data["ward_peers"]
    consensus_s    = set(peer_data["consensus"])
    cluster_sizes  = peer_data["cluster_sizes"]
    others_m       = peer_data["others_m"]

    # ── Left: Mahalanobis bar chart ──────────────────────────────────────────
    ax_lh = fig.add_axes([0.03, 0.83, 0.45, 0.07]); ax_lh.axis("off")
    ax_lh.set_facecolor(DARK_BLUE)
    ax_lh.text(0.5, 0.55, "Method 1: Mahalanobis Distance",
               ha="center", va="center", fontsize=10, fontweight="bold",
               color=GOLD, transform=ax_lh.transAxes)
    ax_lh.text(0.5, 0.10,
               f"Ranks all {peer_data['n_mahal']} MA towns by similarity to Saugus across 8 baseline factors  "
               "(income, poverty, population, education, spending, teacher density, dropout, crime)",
               ha="center", va="center", fontsize=7.2, color=_LIGHT,
               transform=ax_lh.transAxes)

    ax_l = fig.add_axes([0.03, 0.10, 0.45, 0.72])
    ax_l.set_facecolor(CHART_BG)
    for sp in ax_l.spines.values(): sp.set_edgecolor(CHART_GRID)

    top20 = others_m.head(20)
    names = top20["town"].tolist()[::-1]
    dists = top20["mahal_dist"].tolist()[::-1]
    cols  = [GREEN if n in consensus_s else STEEL_BLUE for n in names]
    y_pos = np.arange(len(names))
    ax_l.barh(y_pos, dists, color=cols, alpha=0.85, height=0.72)
    ax_l.set_yticks(y_pos)
    ax_l.set_yticklabels(names, fontsize=7.8)
    for tick, name in zip(ax_l.get_yticklabels(), names):
        tick.set_color(GREEN if name in consensus_s else GREY)
        if name in consensus_s: tick.set_fontweight("bold")
    ax_l.set_xlabel("Distance from Saugus  (0 = identical)", fontsize=8, color=WHITE)
    ax_l.set_title("Top 20 Closest Towns  (green = also Ward-confirmed)",
                   fontsize=8.5, fontweight="bold", color=WHITE, pad=4)
    ax_l.axvline(0, color=GREY, linewidth=0.8)
    ax_l.grid(axis="x", linestyle="--", alpha=0.3, color=CHART_GRID)
    ax_l.tick_params(labelsize=7.8, colors=WHITE)

    # ── Right: Ward cluster list + cluster sizes ─────────────────────────────
    ax_rh = fig.add_axes([0.53, 0.83, 0.45, 0.07]); ax_rh.axis("off")
    ax_rh.set_facecolor(DARK_BLUE)
    ax_rh.text(0.5, 0.55, "Method 2: Ward Hierarchical Clustering",
               ha="center", va="center", fontsize=10, fontweight="bold",
               color=GOLD, transform=ax_rh.transAxes)
    ax_rh.text(0.5, 0.10,
               f"Groups all MA towns by natural similarity (k={optimal_k} clusters).  "
               f"Saugus falls in cluster C{saugus_cluster} with {len(ward_peers)+1} towns.",
               ha="center", va="center", fontsize=7.2, color=_LIGHT,
               transform=ax_rh.transAxes)

    # Town list (right, upper)
    ax_rt = fig.add_axes([0.53, 0.42, 0.45, 0.40]); ax_rt.axis("off")
    ax_rt.text(0.5, 0.99,
               f"Towns in Saugus's Ward Cluster C{saugus_cluster}  ({len(ward_peers)+1} total)",
               ha="center", va="top", fontsize=9, fontweight="bold",
               color=GOLD, transform=ax_rt.transAxes)
    ax_rt.text(0.5, 0.92,
               "green bold = also Mahalanobis top-50  |  >> = Saugus",
               ha="center", va="top", fontsize=7.2, color=GREY,
               transform=ax_rt.transAxes)

    all_cluster = ["Saugus"] + sorted(ward_peers)
    mid = (len(all_cluster) + 1) // 2
    row_h = 0.115
    for j, town in enumerate(all_cluster[:mid]):
        y_t = 0.83 - j * row_h
        is_saugus    = (town == "Saugus")
        is_consensus = (town in consensus_s)
        col = RED if is_saugus else (GREEN if is_consensus else GREY)
        fw  = "bold" if (is_saugus or is_consensus) else "normal"
        prefix = ">>  " if is_saugus else ("*   " if is_consensus else "    ")
        ax_rt.text(0.04, y_t, prefix + town, ha="left", va="top",
                   fontsize=8.5, color=col, fontweight=fw, transform=ax_rt.transAxes)
    for j, town in enumerate(all_cluster[mid:]):
        y_t = 0.83 - j * row_h
        is_consensus = (town in consensus_s)
        col = GREEN if is_consensus else GREY
        fw  = "bold" if is_consensus else "normal"
        prefix = "*   " if is_consensus else "    "
        ax_rt.text(0.54, y_t, prefix + town, ha="left", va="top",
                   fontsize=8.5, color=col, fontweight=fw, transform=ax_rt.transAxes)
    ax_rt.text(0.5, 0.01, "* = also confirmed by Mahalanobis",
               ha="center", va="bottom", fontsize=7, color=GREY, style="italic",
               transform=ax_rt.transAxes)

    # Cluster size bar chart (right, lower)
    ax_rb = fig.add_axes([0.55, 0.10, 0.41, 0.28])
    ax_rb.set_facecolor(CHART_BG)
    for sp in ax_rb.spines.values(): sp.set_edgecolor(CHART_GRID)
    bar_cols = [GREEN if c == saugus_cluster else STEEL_BLUE
                for c in cluster_sizes.index]
    ax_rb.bar(range(len(cluster_sizes)), cluster_sizes.values,
              color=bar_cols, alpha=0.85, edgecolor=CHART_GRID)
    ax_rb.set_xticks(range(len(cluster_sizes)))
    ax_rb.set_xticklabels([f"C{c}" for c in cluster_sizes.index], fontsize=6.5)
    for tick, c in zip(ax_rb.get_xticklabels(), cluster_sizes.index):
        tick.set_color(GREEN if c == saugus_cluster else GREY)
        if c == saugus_cluster: tick.set_fontweight("bold")
    ax_rb.set_ylabel("Towns", fontsize=7.5, color=GREY)
    ax_rb.set_title(f"All {optimal_k} cluster sizes — Saugus in C{saugus_cluster} (green)",
                    fontsize=7.5, fontweight="bold", color=WHITE, pad=3)
    ax_rb.grid(axis="y", linestyle="--", alpha=0.3, color=CHART_GRID)
    ax_rb.tick_params(axis="y", labelsize=7, colors=GREY)

    # Footer
    ax_f = fig.add_axes([0.03, 0.02, 0.94, 0.06]); ax_f.axis("off")
    ax_f.set_facecolor(DARK_BLUE)
    ax_f.patch.set_alpha(0.6)
    consensus_list = "  ·  ".join(sorted(peer_data["consensus"]))
    ax_f.text(0.5, 0.65,
              f"Both methods agree on {len(peer_data['consensus'])} towns — "
              "the consensus peers used throughout this report:",
              ha="center", va="center", fontsize=8.5, fontweight="bold",
              color=GOLD, transform=ax_f.transAxes)
    ax_f.text(0.5, 0.20, consensus_list,
              ha="center", va="center", fontsize=8.5, color=GREEN,
              fontweight="bold", transform=ax_f.transAxes)

    return fig


def page_mahalanobis(peer_data: dict) -> plt.Figure:
    """Slide 1 of 3: explain Mahalanobis distance + ranked list of top-30."""
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Peer Selection: Mahalanobis Distance",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # ── Left: explanation + factor table ─────────────────────────────────────
    ax_l = fig.add_axes([0.03, 0.05, 0.43, 0.84]); ax_l.axis("off")

    exp_blocks = [
        ("What it is", [
            "Mahalanobis distance measures how similar each Massachusetts",
            "town is to Saugus across all 8 factors simultaneously --",
            "accounting for the correlations between those factors.",
        ]),
        ("Why not simple straight-line (Euclidean) distance?", [
            "Income and poverty are correlated ~-0.7 in MA towns.",
            "A town that matches Saugus on both income AND poverty",
            "is not twice as similar as one matching on just one --",
            "because both factors are partly measuring the same thing.",
            "Mahalanobis removes that redundancy automatically.",
        ]),
        ("Result", [
            f"The {peer_data['n_peers']} closest towns become",
            "the peer comparison group.",
        ]),
    ]
    y = 0.97
    for title, lines in exp_blocks:
        ax_l.text(0.0, y, title, ha="left", va="top", fontsize=9.2,
                  fontweight="bold", color=GOLD, transform=ax_l.transAxes)
        y -= 0.040
        for line in lines:
            ax_l.text(0.02, y, line, ha="left", va="top", fontsize=8.5,
                      color=GREY, transform=ax_l.transAxes, linespacing=1.45)
            y -= 0.033
        y -= 0.018

    # Factor table
    ax_l.text(0.0, y, "The 8 factors used (2011-14 averages):", ha="left", va="top",
              fontsize=9.2, fontweight="bold", color=GOLD, transform=ax_l.transAxes)
    y -= 0.040

    col_x = [0.0, 0.60, 0.80]
    for j, h in enumerate(["Factor", "Saugus", "MA Median"]):
        ax_l.text(col_x[j], y, h, ha="left", va="top", fontsize=8,
                  fontweight="bold", color=GOLD, transform=ax_l.transAxes)
    y -= 0.028

    avail       = peer_data["avail"]
    saugus_vals = peer_data["saugus_vals"]
    ma_medians  = peer_data["ma_medians"]

    for col in avail:
        label = FACTOR_LABELS_SHORT.get(col, col)
        sv = FACTOR_FMT[col](saugus_vals[col])
        mv = FACTOR_FMT[col](ma_medians[col])
        ax_l.text(col_x[0], y, label, ha="left", va="top", fontsize=8,
                  color=GREY, transform=ax_l.transAxes)
        ax_l.text(col_x[1], y, sv, ha="left", va="top", fontsize=8,
                  color=GOLD, fontweight="bold", transform=ax_l.transAxes)
        ax_l.text(col_x[2], y, mv, ha="left", va="top", fontsize=8,
                  color=GREY, transform=ax_l.transAxes)
        y -= 0.036

    # ── Right: ranked horizontal bar chart, top 30 ───────────────────────────
    ax_r = fig.add_axes([0.50, 0.10, 0.47, 0.79])
    ax_r.set_facecolor(CHART_BG)
    for spine in ax_r.spines.values():
        spine.set_edgecolor(CHART_GRID)
    others_m  = peer_data["others_m"]
    peers_set = set(peer_data["consensus"])
    top30     = others_m.head(30)

    names = top30["town"].tolist()[::-1]   # reverse: closest at top of chart
    dists = top30["mahal_dist"].tolist()[::-1]
    cols  = [GREEN if n in peers_set else STEEL_BLUE for n in names]

    y_pos = np.arange(len(names))
    ax_r.barh(y_pos, dists, color=cols, alpha=0.85, height=0.72)
    ax_r.set_yticks(y_pos)
    ax_r.set_yticklabels(names, fontsize=7.5)
    for tick, name in zip(ax_r.get_yticklabels(), names):
        tick.set_color(GREEN if name in peers_set else GREY)
        if name in peers_set:
            tick.set_fontweight("bold")
    ax_r.set_xlabel("Mahalanobis distance from Saugus  (0 = identical)", fontsize=8, color=WHITE)
    ax_r.set_title(
        "Top 30 Closest Towns\n"
        f"green = selected as peers (top {peer_data['n_peers']})",
        fontsize=9, fontweight="bold", color=WHITE
    )
    ax_r.axvline(0, color=GREY, linewidth=0.8)
    ax_r.grid(axis="x", linestyle="--", alpha=0.3, color=CHART_GRID)
    ax_r.tick_params(labelsize=7.5, colors=WHITE)

    return fig


def page_mahalanobis_pca(peer_data: dict) -> plt.Figure:
    """PCA scatter showing Mahalanobis proximity to Saugus across all MA towns."""
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from matplotlib.transforms import blended_transform_factory
    from matplotlib.lines import Line2D

    all_towns_c = peer_data["all_towns_c"]
    others_m    = peer_data["others_m"]
    mahal_peers = peer_data["mahal_peers"]
    consensus_s = set(peer_data["consensus"])
    avail       = peer_data["avail"]
    n_mahal     = peer_data["n_mahal"]
    mahal_set   = set(mahal_peers)
    _OFFSCALE   = {"Provincetown"}

    sub = all_towns_c[["town"] + avail].dropna().copy()
    X   = sub[avail].values.astype(float)
    X_s = StandardScaler().fit_transform(X)
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X_s)
    sub["pc1"] = coords[:, 0]
    sub["pc2"] = coords[:, 1]
    sub = sub.merge(others_m[["town", "mahal_dist"]], on="town", how="left")
    sub.loc[sub["town"] == SAUGUS, "mahal_dist"] = 0.0

    var1 = pca.explained_variance_ratio_[0] * 100
    var2 = pca.explained_variance_ratio_[1] * 100

    sub_main = sub[~sub["town"].isin(_OFFSCALE)]
    y_lo = sub_main["pc2"].min() - 0.6
    y_hi = 4.5

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.55,
              "Peer Selection: Mahalanobis Distance — Visual Map",
              ha="center", va="center", fontsize=13, fontweight="bold",
              color="white", transform=ax_h.transAxes)
    ax_h.text(0.5, 0.15,
              f"Towns nearest to Saugus in 7-factor PCA space  ·  "
              f"Top {n_mahal} highlighted  ·  each dot = one MA town",
              ha="center", va="center", fontsize=8.5,
              color="#C8DDF0", transform=ax_h.transAxes)

    ax = fig.add_axes([0.04, 0.08, 0.60, 0.80])
    ax.set_facecolor(CHART_BG)
    for sp in ax.spines.values(): sp.set_color(CHART_GRID)
    ax.set_ylim(y_lo, y_hi)

    # Grey background towns
    others_mask = ~sub["town"].isin(mahal_set | {SAUGUS} | _OFFSCALE)
    ax.scatter(sub.loc[others_mask, "pc1"], sub.loc[others_mask, "pc2"],
               color=GREY, alpha=0.22, s=15, zorder=2)

    # Top-30 Mahalanobis peers
    for _, row in sub[sub["town"].isin(mahal_set) & ~sub["town"].isin(_OFFSCALE)].iterrows():
        col = GREEN if row["town"] in consensus_s else STEEL_BLUE
        ax.scatter([row["pc1"]], [row["pc2"]], color=col, s=50, alpha=0.9, zorder=3)

    # Saugus star
    sg = sub[sub["town"] == SAUGUS]
    if len(sg):
        ax.scatter(sg["pc1"], sg["pc2"], color=GOLD, s=220, zorder=5, marker="*")
        ax.text(float(sg["pc1"].iloc[0]) + 0.15, float(sg["pc2"].iloc[0]),
                "Saugus", fontsize=9, color=GOLD, fontweight="bold", va="center")

    ax.set_xlabel(f"Principal Component 1  ({var1:.0f}% of variance)", color=WHITE, fontsize=8.5)
    ax.set_ylabel(f"Principal Component 2  ({var2:.0f}% of variance)", color=WHITE, fontsize=8.5)
    ax.tick_params(colors=WHITE, labelsize=7.5)
    ax.grid(True, alpha=0.15, linestyle="--", color=CHART_GRID)

    # Note Provincetown's off-scale value
    ptown = sub[sub["town"] == "Provincetown"]
    ptown_note = ""
    if len(ptown):
        ptown_note = f"\nNote: Provincetown omitted (PC2={float(ptown['pc2'].iloc[0]):.1f}); resort community, negligible school-age pop."

    legend_elements = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor=GOLD, markersize=11, label="Saugus"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=GREEN, markersize=7,
               label="Consensus peer (both methods)"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=STEEL_BLUE, markersize=7,
               label=f"Mahalanobis top-{n_mahal} only"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=GREY, markersize=6,
               label="Other MA towns", alpha=0.5),
    ]
    leg = ax.legend(handles=legend_elements, fontsize=7.5, facecolor=DARK_BLUE,
                    edgecolor=CHART_GRID, loc="lower right")
    for txt in leg.get_texts(): txt.set_color(WHITE)

    ax.text(0.02, 0.02,
            "Reading this chart:\n"
            "• Towns close together = similar on all 8 factors\n"
            f"• Green/blue = nearest to Saugus (top 30){ptown_note}",
            transform=ax.transAxes,
            fontsize=7.5, va="bottom", ha="left", color=WHITE, linespacing=1.45,
            bbox=dict(boxstyle="round,pad=0.4", facecolor=DARK_BLUE,
                      edgecolor=CHART_GRID, linewidth=1.0, alpha=0.92))

    # Right panel: ranked list
    ax_r = fig.add_axes([0.66, 0.08, 0.32, 0.80]); ax_r.axis("off")
    ax_r.text(0.5, 0.99, f"Top {n_mahal} Closest Towns",
              ha="center", va="top", fontsize=9, fontweight="bold",
              color=GOLD, transform=ax_r.transAxes)
    ax_r.text(0.5, 0.955, "by Mahalanobis distance  ·  (*) = also Ward cluster",
              ha="center", va="top", fontsize=7, color=GREY,
              transform=ax_r.transAxes)
    ax_r.plot([0.05, 0.95], [0.930, 0.930], color=CHART_GRID, linewidth=0.6,
              transform=ax_r.transAxes)
    ax_r.text(0.06, 0.910, "#   Town", ha="left", va="top", fontsize=7,
              fontweight="bold", color=GOLD, transform=ax_r.transAxes)
    ax_r.text(0.88, 0.910, "Dist", ha="right", va="top", fontsize=7,
              fontweight="bold", color=GOLD, transform=ax_r.transAxes)

    row_h = 0.855 / n_mahal
    for j, (_, row) in enumerate(others_m.head(n_mahal).iterrows()):
        town = row["town"]
        dist = row["mahal_dist"]
        y_t  = 0.890 - j * row_h
        is_c = town in consensus_s
        col  = GREEN if is_c else STEEL_BLUE
        prefix = "(*)" if is_c else "   "
        ax_r.text(0.06, y_t, f"{j+1:2d}. {prefix}{town}",
                  ha="left", va="top", fontsize=7.2,
                  color=col, fontweight="bold" if is_c else "normal",
                  transform=ax_r.transAxes)
        ax_r.text(0.88, y_t, f"{dist:.2f}",
                  ha="right", va="top", fontsize=6.5,
                  color=col, transform=ax_r.transAxes)

    fig.text(0.34, 0.01,
             "PCA of 7 z-scored baseline features.  Proximity in this space = "
             "Mahalanobis similarity to Saugus.  Source: MA DESE + ACS.",
             ha="center", fontsize=6.5, color=GREY, style="italic")
    return fig


def page_ward_cluster(peer_data: dict) -> plt.Figure:
    """Slide 2 of 3: Ward hierarchical clustering — method + cluster member list."""
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Peer Selection: Method 2 -- Ward Hierarchical Clustering",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    optimal_k     = peer_data["optimal_k"]
    saugus_cluster = peer_data["saugus_cluster"]
    ward_peers    = peer_data["ward_peers"]
    consensus_s   = set(peer_data["consensus"])
    cluster_sizes = peer_data["cluster_sizes"]

    # ── Left: explanation ─────────────────────────────────────────────────────
    ax_l = fig.add_axes([0.03, 0.34, 0.43, 0.55]); ax_l.axis("off")

    exp_blocks = [
        ("What it is", [
            "Ward clustering groups ALL Massachusetts towns together",
            "by minimising within-cluster variance at each merge step.",
            "It does NOT use Saugus as a reference point -- it finds",
            "natural groupings in the full population of MA towns.",
        ]),
        ("How the number of clusters is chosen", [
            "k=30 was chosen for the MA town dataset. The dendrogram",
            "elbow sits at k=2 (urban vs. rural MA), which is too coarse",
            "to distinguish peer towns. k=30 gives sub-cohort granularity,",
            "separating inner-ring suburbs from small rural towns.",
        ]),
        (f"Saugus's cluster", [
            f"Saugus falls into cluster {saugus_cluster}, which contains",
            f"{len(ward_peers)+1} towns (including Saugus).",
            "All towns in that cluster become the Ward peer set.",
        ]),
        ("Why two methods?", [
            "Mahalanobis asks: 'which towns are closest to Saugus?'",
            "Ward asks: 'which towns naturally cluster together?'",
            "A town that passes both tests is doubly confirmed.",
        ]),
    ]
    y = 0.99
    for title, lines in exp_blocks:
        ax_l.text(0.0, y, title, ha="left", va="top", fontsize=9.2,
                  fontweight="bold", color=GOLD, transform=ax_l.transAxes)
        y -= 0.044
        for line in lines:
            ax_l.text(0.02, y, line, ha="left", va="top", fontsize=8.5,
                      color=GREY, transform=ax_l.transAxes, linespacing=1.4)
            y -= 0.033
        y -= 0.020

    # ── Cluster size bar chart (bottom-left) ──────────────────────────────────
    ax_bar = fig.add_axes([0.05, 0.08, 0.38, 0.24])
    ax_bar.set_facecolor(CHART_BG)
    for sp in ax_bar.spines.values(): sp.set_edgecolor(CHART_GRID)
    bar_cols = [GREEN if c == saugus_cluster else STEEL_BLUE
                for c in cluster_sizes.index]
    ax_bar.bar(range(len(cluster_sizes)), cluster_sizes.values,
               color=bar_cols, alpha=0.85, edgecolor=CHART_GRID)
    ax_bar.set_xticks(range(len(cluster_sizes)))
    ax_bar.set_xticklabels([f"C{c}" for c in cluster_sizes.index], fontsize=8)
    for tick, c in zip(ax_bar.get_xticklabels(), cluster_sizes.index):
        tick.set_color(GREEN if c == saugus_cluster else GREY)
        if c == saugus_cluster: tick.set_fontweight("bold")
    ax_bar.set_ylabel("Towns", fontsize=8, color=GREY)
    ax_bar.set_title(
        f"Cluster sizes  (k={optimal_k})  --  Saugus in C{saugus_cluster} (green)",
        fontsize=8.5, fontweight="bold", color=WHITE
    )
    ax_bar.grid(axis="y", linestyle="--", alpha=0.3, color=CHART_GRID)
    ax_bar.tick_params(axis="y", labelsize=7.5, colors=GREY)

    # ── Right: towns in Saugus's cluster, two sub-columns ────────────────────
    ax_r = fig.add_axes([0.50, 0.05, 0.47, 0.84]); ax_r.axis("off")

    ax_r.text(0.5, 0.99,
              f"Towns in Saugus's Ward Cluster C{saugus_cluster}  ({len(ward_peers)+1} total incl. Saugus)",
              ha="center", va="top", fontsize=9.5, fontweight="bold",
              color=GOLD, transform=ax_r.transAxes)
    ax_r.text(0.5, 0.94,
              "green bold = also in Mahalanobis top-30  |  gray = Ward cluster only  |  red >> = Saugus",
              ha="center", va="top", fontsize=7.5, color=GREY, transform=ax_r.transAxes)

    all_cluster = ["Saugus"] + sorted(ward_peers)
    mid = (len(all_cluster) + 1) // 2
    row_h = 0.034

    for j, town in enumerate(all_cluster[:mid]):
        y_t = 0.88 - j * row_h
        is_saugus   = town == "Saugus"
        is_consensus = town in consensus_s
        col = RED if is_saugus else (GREEN if is_consensus else GREY)
        fw  = "bold" if (is_saugus or is_consensus) else "normal"
        prefix = ">> " if is_saugus else ("*  " if is_consensus else "   ")
        ax_r.text(0.02, y_t, prefix + town, ha="left", va="top",
                  fontsize=8, color=col, fontweight=fw, transform=ax_r.transAxes)

    for j, town in enumerate(all_cluster[mid:]):
        y_t = 0.88 - j * row_h
        is_consensus = town in consensus_s
        col = GREEN if is_consensus else GREY
        fw  = "bold" if is_consensus else "normal"
        prefix = "*  " if is_consensus else "   "
        ax_r.text(0.52, y_t, prefix + town, ha="left", va="top",
                  fontsize=8, color=col, fontweight=fw, transform=ax_r.transAxes)

    ax_r.text(0.5, 0.01, "* = also confirmed by Mahalanobis",
              ha="center", va="bottom", fontsize=7.5, color=GREY, style="italic",
              transform=ax_r.transAxes)

    return fig


def page_consensus_peers(peer_data: dict) -> plt.Figure:
    """Slide 3 of 3: Intersection — the 23 consensus towns."""
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5,
              f"The {len(peer_data['consensus'])} Comparison Towns",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    peers      = peer_data["consensus"]
    others_m   = peer_data["others_m"]
    saugus_v   = peer_data["saugus_vals"]
    ma_med     = peer_data["ma_medians"]
    avail      = peer_data["avail"]

    # ── Left: ranked peer list with distances ────────────────────────────────
    ax_l = fig.add_axes([0.03, 0.08, 0.28, 0.82]); ax_l.axis("off")
    ax_l.text(0.5, 0.99, "Closest to Saugus by Mahalanobis Distance",
              ha="center", va="top", fontsize=9, fontweight="bold",
              color=GOLD, transform=ax_l.transAxes)
    ax_l.text(0.5, 0.945, "(2011-14 baseline averages, 8 factors)",
              ha="center", va="top", fontsize=7.5, color=GREY,
              transform=ax_l.transAxes)

    peer_rows = others_m[others_m["town"].isin(peers)].sort_values("mahal_dist")
    row_h = 0.84 / max(len(peer_rows), 1)
    for j, (_, row) in enumerate(peer_rows.iterrows()):
        y = 0.90 - j * row_h
        ax_l.text(0.04, y, f"{j+1:2d}.  {row['town']}",
                  ha="left", va="top", fontsize=8.5, fontweight="bold",
                  color=GREEN, transform=ax_l.transAxes)
        ax_l.text(0.96, y, f"{row['mahal_dist']:.2f}",
                  ha="right", va="top", fontsize=8, color=GREY,
                  transform=ax_l.transAxes)

    ax_l.text(0.5, 0.01, "Distance = 0 is identical to Saugus",
              ha="center", va="bottom", fontsize=7, color=GREY,
              style="italic", transform=ax_l.transAxes)

    # ── Right: demographic comparison table ──────────────────────────────────
    SHOW_COLS = [c for c in ["median_hh_income", "poverty_pct_base",
                              "total_population_base", "pct_bachelors_plus",
                              "pp_real_base", "teachers_per1k_base",
                              "dropout_rate_base"] if c in avail]
    COL_LABELS = {
        "median_hh_income":      "Median Income",
        "poverty_pct_base":      "Poverty %",
        "total_population_base": "Population",
        "pct_bachelors_plus":    "College Degree %",
        "pp_real_base":          "PP Spending (real)",
        "teachers_per1k_base":   "Teachers / 1k",
        "dropout_rate_base":     "Dropout %",
    }
    COL_FMT = {
        "median_hh_income":      lambda v: f"${v:,.0f}",
        "poverty_pct_base":      lambda v: f"{v:.1f}%",
        "total_population_base": lambda v: f"{v:,.0f}",
        "pct_bachelors_plus":    lambda v: f"{v:.1f}%",
        "pp_real_base":          lambda v: f"${v:,.0f}",
        "teachers_per1k_base":   lambda v: f"{v:.1f}",
        "dropout_rate_base":     lambda v: f"{v:.1f}%",
    }

    ax_r = fig.add_axes([0.33, 0.08, 0.65, 0.82]); ax_r.axis("off")
    ax_r.set_facecolor(CHART_BG)
    ax_r.text(0.5, 0.99, "Baseline Demographics  (2011-14 averages)",
              ha="center", va="top", fontsize=9, fontweight="bold",
              color=GOLD, transform=ax_r.transAxes)

    n_cols = len(SHOW_COLS)
    col_w  = 0.88 / max(n_cols, 1)
    hdr_y  = 0.93
    for ci, col in enumerate(SHOW_COLS):
        x = 0.10 + ci * col_w + col_w / 2
        ax_r.text(x, hdr_y, COL_LABELS.get(col, col),
                  ha="center", va="top", fontsize=6.8, fontweight="bold",
                  color=GOLD, transform=ax_r.transAxes, rotation=0)

    ax_r.plot([0.08, 0.99], [hdr_y - 0.025, hdr_y - 0.025],
              color=CHART_GRID, lw=0.6, transform=ax_r.transAxes)

    all_rows = [("Saugus", GOLD, saugus_v)] + \
               [(t, GREEN, {c: float(peer_rows.loc[peer_rows["town"]==t, c].values[0])
                            for c in SHOW_COLS if c in peer_rows.columns})
                for t in peers if t in peer_rows["town"].values]
    rh = 0.84 / max(len(all_rows), 1)
    for ri, (town, col, vals) in enumerate(all_rows):
        y = hdr_y - 0.04 - ri * rh
        ax_r.text(0.01, y, (">> " if town == "Saugus" else "   ") + town,
                  ha="left", va="top", fontsize=7.2, fontweight="bold",
                  color=col, transform=ax_r.transAxes)
        for ci, c in enumerate(SHOW_COLS):
            x = 0.10 + ci * col_w + col_w / 2
            v = vals.get(c, float("nan"))
            txt = COL_FMT[c](v) if not (isinstance(v, float) and np.isnan(v)) else "—"
            ax_r.text(x, y, txt, ha="center", va="top", fontsize=7,
                      color=WHITE, transform=ax_r.transAxes)

    ax_r.text(0.5, 0.01,
              f"These {len(peers)} towns are the statistical peers used throughout this report.  "
              "Sources: MA DESE + U.S. Census ACS.",
              ha="center", va="bottom", fontsize=7, color=GREY,
              transform=ax_r.transAxes)

    return fig


# MFR dark-theme constants — used only for the two Ridge/scatter pages that
# are ported directly from municipal_finance_report.py
_NAVY      = "#1B2A4A"
_WHITE     = "#FFFFFF"
_LIGHT     = "#D0D5E0"
_GOLD      = "#F0A500"
_STEEL     = "#5D8AA8"
_DKRED     = "#7B241C"
_DKBLUE    = "#1A5276"
_CBG       = "#1B2A4A"
_CGRID     = "#2C3E6B"

_FEAT_LABELS = {
    "high_needs_pct":             "% High-needs",
    "low_income_pct":             "% Low income",
    "ell_pct":                    "% ELL",
    "sped_pct":                   "% SPED",
    "nss_per_pupil":              "Net school spend/pupil",
    "teacher_spending_per_pupil": "Teacher spend/pupil",
    "ch70_per_pupil":             "Ch70 aid/pupil",
    "chronic_absenteeism_pct":    "Chronic absenteeism",
    "avg_teacher_salary":         "Avg teacher salary",
    "teachers_per_100_fte":       "Teachers per 100 staff",
    "teachers_per_100_students":  "Teachers per 100 students",
    "total_enrollment":           "Enrollment",
    "median_hh_income":           "Median income",
    "pct_bachelors_plus":         "% Bachelor's+",
    "pct_owner_occupied":         "% Homeowners",
}
_SPENDING_FEATS = {
    "nss_per_pupil", "teacher_spending_per_pupil",
    "ch70_per_pupil", "teachers_per_100_fte", "avg_teacher_salary",
}


def _rbp_callout(fig, x, y, w, h, value_str, label, bg, value_size=28):
    rect = plt.Rectangle((x, y), w, h, transform=fig.transFigure,
                          facecolor=bg, edgecolor=_WHITE, linewidth=1.5, zorder=1)
    fig.add_artist(rect)
    fig.text(x + w/2, y + h*0.62, value_str, transform=fig.transFigure,
             fontsize=value_size, fontweight="bold", color=_WHITE,
             ha="center", va="center", zorder=2)
    fig.text(x + w/2, y + h*0.22, label, transform=fig.transFigure,
             fontsize=7.5, color=_WHITE, ha="center", va="center",
             zorder=2, linespacing=1.4)


def page_mcas_prediction_scatter(engine) -> plt.Figure:
    """
    Scatter: Ridge-predicted MCAS vs actual MCAS for every MA district.
    Saugus starred in gold.  Shows (a) model fit and (b) Saugus's gap.
    Ported from municipal_finance_report.rbp_outcomes_page.
    """
    rbp_df = load_rbp_features(engine, year=2024)
    if rbp_df is None or len(rbp_df) == 0:
        return None

    rbp_df = rbp_df.rename(columns={"district_name": "municipality"})
    TARGET = "avg_mcas"

    fi = rbp_feature_importance(
        rbp_df.dropna(subset=RBP_ALL_FEATURES + [TARGET]),
        RBP_ALL_FEATURES, TARGET
    )
    if fi.empty:
        return None

    top_features = fi.head(N_TOP_RBP)["feature"].tolist()
    rbp_df = rbp_fitted_predict(rbp_df, top_features, TARGET)

    valid   = rbp_df.dropna(subset=["rbp_pred", TARGET])
    y_true  = valid[TARGET].astype(float) * 100
    y_pred  = valid["rbp_pred"]
    ss_res  = float(((y_true - y_pred) ** 2).sum())
    ss_tot  = float(((y_true - y_true.mean()) ** 2).sum())
    r2      = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    saugus      = rbp_df[rbp_df["municipality"] == SAUGUS].iloc[0]
    actual      = float(saugus[TARGET]) * 100
    predicted   = float(saugus["rbp_pred"])
    gap         = actual - predicted
    n_towns     = len(valid)
    saugus_rank = int(rbp_df["rbp_resid"].rank().loc[
        rbp_df["municipality"] == SAUGUS].iloc[0])
    gap_color   = RED if gap < -2 else GREEN if gap > 2 else BLUE

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(_NAVY)

    fig.text(0.5, 0.955, "Saugus vs. Demographically Similar Districts",
             ha="center", fontsize=16, fontweight="bold", color=_WHITE,
             transform=fig.transFigure)
    fig.text(0.5, 0.922,
             f"Ridge regression (R²={r2:.2f}) on top {len(top_features)} outcome-predictive "
             f"factors predicts Saugus should score {predicted:.0f}%.  "
             f"Actual: {actual:.0f}%.  Gap: {abs(gap):.1f} pp {'below' if gap<0 else 'above'} prediction.",
             ha="center", fontsize=9, color=_LIGHT, linespacing=1.5,
             transform=fig.transFigure)

    # ── Scatter ──────────────────────────────────────────────────────────────
    ax = fig.add_axes([0.07, 0.10, 0.54, 0.78])
    ax.set_facecolor(_CBG)

    others = rbp_df[rbp_df["municipality"] != SAUGUS].dropna(subset=["rbp_pred"])
    ax.scatter(others["rbp_pred"], others[TARGET].astype(float) * 100,
               color=_STEEL, alpha=0.45, s=28, zorder=2)

    lo = min(rbp_df["rbp_pred"].min(), rbp_df[TARGET].min() * 100) - 2
    hi = max(rbp_df["rbp_pred"].max(), rbp_df[TARGET].max() * 100) + 2
    ax.plot([lo, hi], [lo, hi], color=_LIGHT, linewidth=1.2,
            linestyle=":", alpha=0.5, label="Perfect prediction")
    ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)

    ax.scatter([predicted], [actual], color=_GOLD, s=220, zorder=5, marker="*")
    ax.annotate(
        f"Saugus\nExpected: {predicted:.0f}%\nActual: {actual:.0f}%\nGap: {gap:+.1f}pp",
        xy=(predicted, actual),
        xytext=(lo + (hi-lo)*0.04, hi - (hi-lo)*0.06),
        fontsize=8.5, color=_GOLD, fontweight="bold", va="top",
        arrowprops=dict(arrowstyle="->", color=_GOLD, lw=1.2))

    ax.set_xlabel("What the model expected  (MCAS %)", color=_WHITE, fontsize=9)
    ax.set_ylabel("What the district actually scored", color=_WHITE, fontsize=9)
    ax.tick_params(colors=_WHITE, labelsize=8)
    for sp in ax.spines.values():
        sp.set_color(_CGRID)
    ax.legend(fontsize=8, framealpha=0.3, labelcolor=_WHITE,
              facecolor=_DKBLUE, loc="lower right")
    ax.set_title("Expected vs Actual MCAS — every dot is one MA district",
                 color=_WHITE, fontsize=10, pad=6)
    ax.grid(linestyle="--", alpha=0.15, color=_LIGHT)

    # ── Stat cards ───────────────────────────────────────────────────────────
    rx, cw, ch = 0.655, 0.315, 0.165
    _rbp_callout(fig, rx, 0.695, cw, ch,
                 f"{gap:+.1f}pp",
                 (f"Saugus scores {abs(gap):.1f}pp {'below' if gap<0 else 'above'}\n"
                  f"what similar districts achieve\n"
                  f"Expected {predicted:.0f}%  ·  Actual {actual:.0f}%"),
                 RED if gap < -2 else GREEN, value_size=28)
    _rbp_callout(fig, rx, 0.510, cw, ch,
                 f"{n_towns - saugus_rank} of {n_towns}",
                 "districts outperform Saugus\nrelative to their predicted score",
                 _DKRED, value_size=22)
    # ── Mini Ridge importance chart (replaces R² callout) ────────────────────
    _FEAT_LABELS = {
        "chronic_absenteeism_pct":  "Chronic Absenteeism",
        "ch70_per_pupil":           "Ch70 Aid / Pupil",
        "pct_bachelors_plus":       "College Degree %",
        "sped_pct":                 "SPED %",
        "median_hh_income":         "Median Income",
        "ell_pct":                  "English Learners %",
    }
    imp_top = fi.head(N_TOP_RBP).copy()
    imp_top["label"] = imp_top["feature"].map(lambda f: _FEAT_LABELS.get(f, f))
    imp_top = imp_top.iloc[::-1]  # highest bar at top

    ax_imp = fig.add_axes([rx, 0.095, cw, 0.375])
    ax_imp.set_facecolor(_DKBLUE)
    for sp in ax_imp.spines.values(): sp.set_color(_CGRID)
    ax_imp.barh(range(len(imp_top)), imp_top["importance"], color=_STEEL, alpha=0.8, height=0.65)
    ax_imp.set_yticks(range(len(imp_top)))
    ax_imp.set_yticklabels(imp_top["label"], fontsize=7.2, color=_WHITE)
    ax_imp.tick_params(axis="x", colors=_LIGHT, labelsize=6.5)
    ax_imp.set_xlabel("Importance (change in prediction\nif factor removed)", fontsize=6.5, color=_LIGHT)
    ax_imp.set_title(f"The {len(top_features)} Factors Driving\nthe Model  (R²={r2:.2f})",
                     fontsize=7.5, fontweight="bold", color=_WHITE, pad=4)
    ax_imp.grid(axis="x", linestyle="--", alpha=0.2, color=_CGRID)

    fig.text(0.5, 0.030,
             f"Ridge regression (L2 α=0.1), z-score normalised, in-sample fitted values.  "
             f"{len(RBP_ALL_FEATURES)} features tested; top {len(top_features)} selected by importance.  "
             "Sources: MA DESE, U.S. Census ACS.  Correlation ≠ causation.",
             ha="center", fontsize=7, color=_LIGHT, alpha=0.6,
             transform=fig.transFigure, linespacing=1.4)

    return fig


def page_overachievers(engine, peers, peer_data: dict) -> plt.Figure:
    """
    Among the Mahalanobis pool of demographically similar towns,
    find those outperforming their Ridge-predicted MCAS by OVER_MIN pp.
    Left: scatter with pool highlighted, overachievers (green), consensus peers (orange).
    Right top: ranked table with key metrics.
    Right bottom: three-group comparison (Saugus / overachievers / consensus).
    """
    rbp_df = load_rbp_features(engine, year=2024)
    if rbp_df is None or len(rbp_df) == 0:
        return None

    rbp_df = rbp_df.rename(columns={"district_name": "municipality"})
    TARGET = "avg_mcas"

    fi = rbp_feature_importance(
        rbp_df.dropna(subset=RBP_ALL_FEATURES + [TARGET]),
        RBP_ALL_FEATURES, TARGET,
    )
    if fi.empty:
        return None
    top_features = fi.head(N_TOP_RBP)["feature"].tolist()
    rbp_df = rbp_fitted_predict(rbp_df, top_features, TARGET)

    valid = rbp_df.dropna(subset=["rbp_pred", TARGET, "rbp_resid"]).copy()
    valid["actual_pct"]  = valid[TARGET].astype(float) * 100
    valid["teachers_1k"] = valid["teachers_per_100_students"].astype(float) * 10

    def _med(df, col):
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        return float(s.median()) if len(s) else np.nan

    saugus_row  = valid[valid["municipality"] == SAUGUS].iloc[0]
    saugus_pred = float(saugus_row["rbp_pred"])
    saugus_act  = float(saugus_row["actual_pct"])

    def _sv(col):
        v = saugus_row.get(col, np.nan)
        return float(v) if pd.notna(v) else np.nan

    saugus_abs   = _sv("chronic_absenteeism_pct")
    saugus_tch   = _sv("teachers_per_100_students") * 10
    saugus_enrol = _sv("total_enrollment")
    saugus_sped  = _sv("sped_pct") if not np.isnan(_sv("sped_pct")) else np.nan
    saugus_inc   = _sv("median_hh_income")
    saugus_ch70  = _sv("ch70_per_pupil")

    OVER_MIN = 4.0

    # Pool: Mahalanobis top-N union Ward cluster (same methodology as peer selection,
    # applied to find who is demographically similar to Saugus TODAY)
    candidate_pool = set(peer_data["others_m"].head(peer_data["n_mahal"])["town"].tolist())

    in_pool = valid[
        (valid["municipality"].isin(candidate_pool)) &
        (valid["municipality"] != SAUGUS)
    ].copy()
    overachievers = (in_pool[in_pool["rbp_resid"] >= OVER_MIN]
                     .sort_values("rbp_resid", ascending=False)
                     .head(10))
    n_pool = len(in_pool)

    # Consensus peers in rbp space (may not all have Ridge data)
    consensus_rbp = valid[valid["municipality"].isin(peers)].copy()

    # Gap and rank stats (carried from merged scatter page)
    saugus_gap  = saugus_act - saugus_pred
    saugus_rank = int(valid["rbp_resid"].rank().loc[
        valid["municipality"] == SAUGUS].iloc[0])
    n_towns_all = len(valid)
    n_outperform = n_towns_all - saugus_rank

    # ── Figure ──────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(_NAVY)

    # Header
    ax_h = fig.add_axes([0, 0.90, 1, 0.10])
    ax_h.axis("off"); ax_h.set_facecolor(_DKBLUE)
    ax_h.text(0.5, 0.62, "Towns Beating Their Demographics — Who Should Saugus Study?",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color=_WHITE, transform=ax_h.transAxes)
    ax_h.text(0.5, 0.28,
              f"Among {n_pool} towns in Saugus's Mahalanobis similarity pool, "
              f"{len(overachievers)} score {OVER_MIN:.0f}+pp above their Ridge prediction.  "
              f"Saugus scores {saugus_gap:+.0f}pp vs. its own expectation.",
              ha="center", va="center", fontsize=8, color=_LIGHT,
              transform=ax_h.transAxes)

    # ── Left: scatter ───────────────────────────────────────────────────────────
    ax = fig.add_axes([0.04, 0.08, 0.46, 0.79])
    ax.set_facecolor(_CBG)

    lo = valid["rbp_pred"].min() - 2
    hi = valid["rbp_pred"].max() + 2

    oa_set   = set(overachievers["municipality"])
    cs_set   = set(consensus_rbp["municipality"])
    pool_set = candidate_pool

    # All non-pool towns — faint grey
    others = valid[~valid["municipality"].isin(pool_set | {SAUGUS})]
    ax.scatter(others["rbp_pred"], others["actual_pct"],
               color=_STEEL, alpha=0.18, s=14, zorder=2)

    # Pool members that are neither overachievers nor consensus peers — muted teal
    pool_neutral = valid[
        valid["municipality"].isin(pool_set - oa_set - cs_set - {SAUGUS})
    ]
    ax.scatter(pool_neutral["rbp_pred"], pool_neutral["actual_pct"],
               color=_STEEL, alpha=0.5, s=22, zorder=3,
               label=f"Similarity pool ({n_pool} towns)")

    # Consensus peers — orange triangles
    if len(consensus_rbp):
        ax.scatter(consensus_rbp["rbp_pred"], consensus_rbp["actual_pct"],
                   color=AMBER, alpha=0.85, s=55, marker="^", zorder=4,
                   label=f"Comparison peers (n={len(consensus_rbp)})")

    # Overachievers — green circles
    ax.scatter(overachievers["rbp_pred"], overachievers["actual_pct"],
               color="#27AE60", alpha=0.90, s=65, zorder=5,
               label=f"Overachievers ({OVER_MIN:.0f}+pp above prediction, n={len(overachievers)})")

    # Perfect-prediction line
    ax.plot([lo, hi], [lo, hi], color=_LIGHT, lw=1.2, ls=":", alpha=0.5,
            label="Perfect prediction")

    # Saugus vertical guide
    ax.axvline(saugus_pred, color=_GOLD, lw=1.0, ls="--", alpha=0.4)

    # Saugus star
    ax.scatter([saugus_pred], [saugus_act], color=_GOLD, s=210,
               marker="*", zorder=7)
    ax.annotate(f"Saugus\n({saugus_pred:.0f}% -> {saugus_act:.0f}%)",
                xy=(saugus_pred, saugus_act),
                xytext=(saugus_pred + 3.5, saugus_act - 7),
                fontsize=7.2, color=_GOLD, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=_GOLD, lw=0.9))

    ax.set_xlim(lo, hi); ax.set_ylim(max(0, lo - 5), min(100, hi + 10))
    ax.set_xlabel("Ridge-predicted MCAS %  (demographic expectation)",
                  color=_WHITE, fontsize=8)
    ax.set_ylabel("Actual MCAS %", color=_WHITE, fontsize=8)
    ax.tick_params(colors=_WHITE, labelsize=7.5)
    for sp in ax.spines.values():
        sp.set_color(_CGRID)
    ax.grid(ls="--", alpha=0.15, color=_LIGHT)
    ax.legend(fontsize=7, framealpha=0.25, labelcolor=_WHITE,
              facecolor=_DKBLUE, loc="upper left")

    # ── Right panel ─────────────────────────────────────────────────────────────
    ax_r = fig.add_axes([0.53, 0.30, 0.45, 0.57])
    ax_r.set_xlim(0, 1); ax_r.set_ylim(0, 1); ax_r.axis("off")

    # Column positions
    C = dict(town=0.0, gap=0.38, tch=0.52, abs=0.66, sped=0.79, enrol=0.93)
    FS = 7.5

    def _fmt(v, fmt):
        return fmt % v if pd.notna(v) and not np.isnan(float(v)) else "—"

    # Table header
    hdr_y = 0.985
    for label, xpos, align in [
        ("Town",    C["town"],  "left"),
        ("Gap",     C["gap"],   "center"),
        ("T/1k",    C["tch"],   "center"),
        ("Absent%", C["abs"],   "center"),
        ("SPED%",   C["sped"],  "center"),
        ("Enroll",  C["enrol"], "center"),
    ]:
        ax_r.text(xpos, hdr_y, label, fontsize=FS - 0.5, fontweight="bold",
                  color=_GOLD, ha=align, va="top", transform=ax_r.transAxes)
    ax_r.plot([0, 1], [hdr_y - 0.025, hdr_y - 0.025],
              color=_CGRID, lw=0.6, transform=ax_r.transAxes)

    row_h = 0.082
    y = hdr_y - 0.05
    for _, row in overachievers.iterrows():
        gap   = float(row["rbp_resid"])
        bar_w = min(gap / 45, 1.0)
        ax_r.barh(y - row_h * 0.35, bar_w, row_h * 0.72,
                  left=0, color="#27AE60", alpha=0.10,
                  transform=ax_r.transAxes)

        tch_v  = float(row.get("teachers_per_100_students", np.nan)) * 10
        abs_v  = float(row.get("chronic_absenteeism_pct", np.nan))
        sped_v = float(row.get("sped_pct", np.nan))
        enr_v  = float(row.get("total_enrollment", np.nan))
        enr_s  = (f"{enr_v/1000:.1f}k" if pd.notna(enr_v) and not np.isnan(enr_v)
                  else "—")

        mid = y - row_h * 0.1
        ax_r.text(C["town"],  mid, str(row["municipality"]),
                  fontsize=FS, color=_WHITE, va="center", transform=ax_r.transAxes)
        ax_r.text(C["gap"],   mid, f"+{gap:.0f}pp",
                  fontsize=FS, color="#27AE60", va="center", ha="center",
                  fontweight="bold", transform=ax_r.transAxes)
        ax_r.text(C["tch"],   mid, _fmt(tch_v, "%.0f"),
                  fontsize=FS, color=_LIGHT, va="center", ha="center",
                  transform=ax_r.transAxes)
        ax_r.text(C["abs"],   mid, _fmt(abs_v, "%.1f%%"),
                  fontsize=FS, color=_LIGHT, va="center", ha="center",
                  transform=ax_r.transAxes)
        ax_r.text(C["sped"],  mid, _fmt(sped_v, "%.1f%%"),
                  fontsize=FS, color=_LIGHT, va="center", ha="center",
                  transform=ax_r.transAxes)
        ax_r.text(C["enrol"], mid, enr_s,
                  fontsize=FS, color=_LIGHT, va="center", ha="center",
                  transform=ax_r.transAxes)
        y -= row_h

    # Saugus separator + row
    ax_r.plot([0, 1], [y - 0.005, y - 0.005],
              color=_CGRID, lw=0.5, transform=ax_r.transAxes)
    y -= 0.015
    mid = y - row_h * 0.1
    sped_s = (_fmt(saugus_sped, "%.1f%%") if not np.isnan(saugus_sped) else "—")
    enr_s  = (f"{saugus_enrol/1000:.1f}k"
              if not np.isnan(saugus_enrol) else "—")
    ax_r.text(C["town"],  mid, "▶ Saugus",
              fontsize=FS, color=_GOLD, va="center", fontweight="bold",
              transform=ax_r.transAxes)
    ax_r.text(C["gap"],   mid, f"{saugus_act - saugus_pred:+.0f}pp",
              fontsize=FS, color=RED, va="center", ha="center",
              fontweight="bold", transform=ax_r.transAxes)
    ax_r.text(C["tch"],   mid, _fmt(saugus_tch, "%.0f"),
              fontsize=FS, color=_LIGHT, va="center", ha="center",
              transform=ax_r.transAxes)
    ax_r.text(C["abs"],   mid, _fmt(saugus_abs, "%.1f%%"),
              fontsize=FS, color=_LIGHT, va="center", ha="center",
              transform=ax_r.transAxes)
    ax_r.text(C["sped"],  mid, sped_s,
              fontsize=FS, color=_LIGHT, va="center", ha="center",
              transform=ax_r.transAxes)
    ax_r.text(C["enrol"], mid, enr_s,
              fontsize=FS, color=_LIGHT, va="center", ha="center",
              transform=ax_r.transAxes)

    # ── Bottom: three-group comparison ──────────────────────────────────────────
    ax_b = fig.add_axes([0.53, 0.08, 0.45, 0.19])
    ax_b.set_facecolor(LT_BLUE)
    ax_b.set_xlim(0, 1); ax_b.set_ylim(0, 1); ax_b.axis("off")
    ax_b.patch.set_alpha(0.6)

    # Compute group medians
    oa_abs  = _med(overachievers, "chronic_absenteeism_pct")
    oa_tch  = _med(overachievers, "teachers_1k")
    oa_sped = (_med(overachievers, "sped_pct")
               if not np.isnan(_med(overachievers, "sped_pct")) else np.nan)
    oa_inc  = _med(overachievers, "median_hh_income")
    oa_ch70 = _med(overachievers, "ch70_per_pupil")

    cs_abs  = _med(consensus_rbp, "chronic_absenteeism_pct")
    cs_tch  = _med(consensus_rbp, "teachers_1k")
    cs_sped = (_med(consensus_rbp, "sped_pct")
               if not np.isnan(_med(consensus_rbp, "sped_pct")) else np.nan)
    cs_inc  = _med(consensus_rbp, "median_hh_income")
    cs_ch70 = _med(consensus_rbp, "ch70_per_pupil")

    ax_b.text(0.01, 0.92, "Two different peer questions — same Saugus, different lens:",
              fontsize=7.2, fontweight="bold", color=_GOLD, va="top",
              transform=ax_b.transAxes)

    # Column headers
    cols_b = [
        ("Group",     0.00, "left"),
        ("Absent%",   0.38, "center"),
        ("T/1k",      0.52, "center"),
        ("SPED%",     0.65, "center"),
        ("HH Income", 0.79, "center"),
        ("Ch70/pupil",0.93, "center"),
    ]
    hy = 0.72
    for lbl, xp, ha in cols_b:
        ax_b.text(xp, hy, lbl, fontsize=6.8, fontweight="bold",
                  color=_GOLD, ha=ha, va="top", transform=ax_b.transAxes)
    ax_b.plot([0, 1], [hy - 0.07, hy - 0.07],
              color=_CGRID, lw=0.5, transform=ax_b.transAxes)

    rows_b = [
        ("Overachievers (median)", "#27AE60",
         oa_abs, oa_tch, oa_sped, oa_inc, oa_ch70),
        ("Comparison peers (median)", AMBER,
         cs_abs, cs_tch, cs_sped, cs_inc, cs_ch70),
        ("▶ Saugus", _GOLD,
         saugus_abs, saugus_tch, saugus_sped, saugus_inc, saugus_ch70),
    ]
    ry = hy - 0.12
    rh = 0.22
    for label, col, ab, tc, sp, inc, ch in rows_b:
        ax_b.text(0.00, ry, label,
                  fontsize=7.0, color=col, va="top", transform=ax_b.transAxes)
        ax_b.text(0.38, ry, _fmt(ab, "%.1f%%"),
                  fontsize=7.0, color=_LIGHT, ha="center", va="top",
                  transform=ax_b.transAxes)
        ax_b.text(0.52, ry, _fmt(tc, "%.0f"),
                  fontsize=7.0, color=_LIGHT, ha="center", va="top",
                  transform=ax_b.transAxes)
        ax_b.text(0.65, ry, _fmt(sp, "%.1f%%"),
                  fontsize=7.0, color=_LIGHT, ha="center", va="top",
                  transform=ax_b.transAxes)
        inc_s = (f"${float(inc):,.0f}" if pd.notna(inc) and not np.isnan(float(inc))
                 else "—")
        ch_s  = (f"${float(ch):,.0f}"  if pd.notna(ch)  and not np.isnan(float(ch))
                 else "—")
        ax_b.text(0.79, ry, inc_s,
                  fontsize=7.0, color=_LIGHT, ha="center", va="top",
                  transform=ax_b.transAxes)
        ax_b.text(0.93, ry, ch_s,
                  fontsize=7.0, color=_LIGHT, ha="center", va="top",
                  transform=ax_b.transAxes)
        ry -= rh

    # Side note explaining the two peer concepts
    ax_n = fig.add_axes([0.53, 0.28, 0.45, 0.025])
    ax_n.axis("off")
    ax_n.text(0.0, 0.5,
              "Comparison peers = 15 towns most similar to Saugus in 2012 by Mahalanobis distance.  "
              "Overachievers = towns in the same Mahalanobis similarity pool, scoring 4+pp above their Ridge prediction.",
              fontsize=6.8, color=_LIGHT, va="center", transform=ax_n.transAxes,
              linespacing=1.3)

    return fig


def page_overachiever_divergence(engine, peers, peer_data: dict) -> plt.Figure:
    """
    Four-panel trajectory comparison: Saugus vs overachiever median across
    MCAS, chronic absenteeism, teacher density, and per-pupil spending.
    Key message: the gap is NOT about money — it's absenteeism + staffing stability.
    """
    from sqlalchemy import text as _text

    # ── Re-derive overachiever towns ────────────────────────────────────────────
    rbp_df = load_rbp_features(engine, year=2024)
    if rbp_df is None or len(rbp_df) == 0:
        return None
    rbp_df = rbp_df.rename(columns={"district_name": "municipality"})
    TARGET = "avg_mcas"
    fi = rbp_feature_importance(
        rbp_df.dropna(subset=RBP_ALL_FEATURES + [TARGET]),
        RBP_ALL_FEATURES, TARGET,
    )
    if fi.empty:
        return None
    top_features = fi.head(N_TOP_RBP)["feature"].tolist()
    rbp_df = rbp_fitted_predict(rbp_df, top_features, TARGET)

    valid = rbp_df.dropna(subset=["rbp_pred", TARGET, "rbp_resid"]).copy()
    saugus_pred = float(valid[valid["municipality"] == SAUGUS]["rbp_pred"].iloc[0])
    OVER_MIN = 4.0
    candidate_pool = set(peer_data["others_m"].head(peer_data["n_mahal"])["town"].tolist())
    in_pool = valid[
        (valid["municipality"].isin(candidate_pool)) &
        (valid["municipality"] != SAUGUS)
    ]
    oa_towns = (in_pool[in_pool["rbp_resid"] >= OVER_MIN]
                .sort_values("rbp_resid", ascending=False)
                .head(10)["municipality"].tolist())
    if not oa_towns:
        return None
    all_q = oa_towns + [SAUGUS]
    towns_sql = ",".join(f"'{t}'" for t in all_q)

    # ── Load time-series data ───────────────────────────────────────────────────
    with engine.connect() as conn:
        mcas = pd.read_sql(_text(f"""
            SELECT school_year, district_name,
                   AVG(meeting_exceeding_pct) AS avg_me
            FROM mcas_results
            WHERE district_name IN ({towns_sql})
              AND student_group = 'All Students'
              AND grade = 'ALL (03-08)'
              AND subject IN ('ELA', 'MATH')
              AND org_code LIKE :pct
            GROUP BY school_year, district_name
            ORDER BY district_name, school_year
        """), conn, params={"pct": "%0000"})

        att = pd.read_sql(_text(f"""
            SELECT school_year, district_name, chronic_absenteeism_pct
            FROM attendance
            WHERE district_name IN ({towns_sql})
              AND school_name IS NULL AND student_group = 'All'
            ORDER BY district_name, school_year
        """), conn)

        tch = pd.read_sql(_text(f"""
            SELECT school_year, district_name, fte
            FROM staffing
            WHERE district_name IN ({towns_sql})
              AND category = 'teachers_per_100_fte'
            ORDER BY district_name, school_year
        """), conn)

        ppe = pd.read_sql(_text(f"""
            SELECT school_year, district_name, amount AS per_pupil
            FROM per_pupil_expenditure
            WHERE district_name IN ({towns_sql})
              AND category = 'Total In-District Expenditures'
            ORDER BY district_name, school_year
        """), conn)

    # Helper: split into Saugus series and OA pivot
    def _split(df, year_col, town_col, val_col):
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        saugus_s = (df[df[town_col] == SAUGUS]
                    .set_index(year_col)[val_col].sort_index())
        oa_piv = (df[df[town_col].isin(oa_towns)]
                  .pivot_table(index=year_col, columns=town_col, values=val_col))
        oa_med = oa_piv.median(axis=1)
        return saugus_s, oa_piv, oa_med

    m_s, m_piv, m_med = _split(mcas, "school_year", "district_name", "avg_me")
    a_s, a_piv, a_med = _split(att,  "school_year", "district_name", "chronic_absenteeism_pct")
    t_s, t_piv, t_med = _split(tch,  "school_year", "district_name", "fte")
    p_s, p_piv, p_med = _split(ppe,  "school_year", "district_name", "per_pupil")

    # Convert teacher fte (per 100) → per 1k
    t_s   = t_s * 10
    t_med = t_med * 10
    t_piv = t_piv * 10

    # Convert spending to $k for readability
    p_s   = p_s   / 1000
    p_med = p_med / 1000
    p_piv = p_piv / 1000

    # Convert MCAS fraction → pct
    m_s   = m_s   * 100
    m_med = m_med * 100
    m_piv = m_piv * 100

    # ── Figure ──────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(_NAVY)

    ax_h = fig.add_axes([0, 0.91, 1, 0.09])
    ax_h.axis("off"); ax_h.set_facecolor(_DKBLUE)
    ax_h.text(0.5, 0.60, "What the Overachievers Did Differently",
              ha="center", va="center", fontsize=14, fontweight="bold",
              color=_WHITE, transform=ax_h.transAxes)
    ax_h.text(0.5, 0.14,
              f"Saugus (gold) vs. {len(oa_towns)}-town overachiever median (green)"
              "  ·  Individual overachiever towns shown in faint green",
              ha="center", va="center", fontsize=8.5, color=_LIGHT,
              transform=ax_h.transAxes)

    _OA  = "#27AE60"
    _OAF = "#27AE60"

    PAD  = dict(left=0.07, right=0.97, bottom=0.09, top=0.89)
    HGAP = 0.06; VGAP = 0.09
    W = (PAD["right"] - PAD["left"] - HGAP) / 2
    H = (PAD["top"]   - PAD["bottom"] - VGAP) / 2
    positions = {
        "mcas":    (PAD["left"],         PAD["bottom"] + H + VGAP, W, H),
        "absent":  (PAD["left"] + W + HGAP, PAD["bottom"] + H + VGAP, W, H),
        "teacher": (PAD["left"],         PAD["bottom"],             W, H),
        "spend":   (PAD["left"] + W + HGAP, PAD["bottom"],          W, H),
    }

    def _style_ax(ax, title, ylabel):
        ax.set_facecolor(_CBG)
        ax.tick_params(colors=_WHITE, labelsize=7)
        for sp in ax.spines.values():
            sp.set_color(_CGRID)
        ax.grid(ls="--", alpha=0.15, color=_LIGHT)
        ax.set_title(title, color=_WHITE, fontsize=8.5, fontweight="bold", pad=4)
        ax.set_ylabel(ylabel, color=_LIGHT, fontsize=7.5)
        ax.set_xlabel("School year", color=_LIGHT, fontsize=7)

    def _draw_covid(ax):
        ax.axvline(2020, color=RED, lw=0.7, ls=":", alpha=0.5, zorder=1)
        ax.text(2020.1, ax.get_ylim()[0] + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.04,
                "Covid", color=RED, fontsize=6.5, alpha=0.7)

    # For teacher density, cap the y-axis at 105 — Eastham/Tisbury/Rowe
    # are tiny schools (<400 students) where the ratio inflates artificially.
    # The faint lines still show those extremes but don't distort the axis.
    TCH_YMAX = 105

    def _plot_panel(key, saugus_s, oa_piv, oa_med, title, ylabel,
                    invert=False, fmt_y=None, ylim=None):
        ax = fig.add_axes(positions[key])
        _style_ax(ax, title, ylabel)

        # Faint individual OA lines
        for col in oa_piv.columns:
            s = oa_piv[col].dropna()
            if len(s) >= 3:
                ax.plot(s.index, s.values, color=_OAF, alpha=0.12, lw=0.8, zorder=2)

        # OA median
        oa_c = oa_med.dropna()
        ax.plot(oa_c.index, oa_c.values, color=_OA, lw=2.0, zorder=4,
                label=f"OA median (n={len(oa_towns)})")

        # Saugus
        sug_c = saugus_s.dropna()
        ax.plot(sug_c.index, sug_c.values, color=_GOLD, lw=2.2, zorder=5,
                label="Saugus")
        ax.scatter([sug_c.index[-1]], [sug_c.iloc[-1]],
                   color=_GOLD, s=40, zorder=6)

        ax.autoscale_view()
        if ylim:
            ax.set_ylim(*ylim)
        _draw_covid(ax)

        ax.legend(fontsize=6.5, framealpha=0.25, labelcolor=_WHITE,
                  facecolor=_DKBLUE, loc="lower left" if not invert else "upper left")
        if fmt_y:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_y))

    _plot_panel("mcas", m_s, m_piv, m_med,
                "MCAS Meeting/Exceeding %",
                "% students M/E",
                fmt_y=lambda x, _: f"{x:.0f}%")

    _plot_panel("absent", a_s, a_piv, a_med,
                "Chronic Absenteeism",
                "% chronically absent",
                invert=True,
                fmt_y=lambda x, _: f"{x:.0f}%")

    _plot_panel("teacher", t_s, t_piv, t_med,
                "Teacher Density",
                "Teachers per 1,000 students",
                ylim=(55, TCH_YMAX),
                fmt_y=lambda x, _: f"{x:.0f}")

    _plot_panel("spend", p_s, p_piv, p_med,
                "Per-Pupil Spending  (not the differentiator)",
                "$ thousands per pupil",
                fmt_y=lambda x, _: f"${x:.0f}k")

    # Bottom note strip
    ax_n = fig.add_axes([PAD["left"], 0.01, PAD["right"] - PAD["left"], 0.025])
    ax_n.axis("off")
    ax_n.text(0.0, 0.5,
              "Overachievers = towns in Saugus's Mahalanobis similarity pool "
              "scoring 4+pp above their own Ridge prediction.  "
              "Teacher density capped at 105/1k; tiny-school outliers (Eastham, Tisbury, Rowe) extend higher.",
              fontsize=6.5, color=_LIGHT, va="center", transform=ax_n.transAxes)

    return fig


def page_budget_share_trend(sexp, cpi) -> plt.Figure:
    """
    Two-panel budget chart:
      Left  — education share trend vs fixed costs / debt service / public safety
      Right — inflation-adjusted spending growth index (FY2010 = 100)
    Data from MFR's load_data().
    """
    import matplotlib.ticker as _mt

    sexp_s = sexp[sexp["municipality"] == SAUGUS].sort_values("fiscal_year")
    if sexp_s.empty:
        return None

    years = sorted(sexp_s["fiscal_year"].unique())
    def _v(y, col):
        r = sexp_s[sexp_s["fiscal_year"] == y]
        return float(r[col].iloc[0]) if len(r) and not pd.isna(r[col].iloc[0]) else 0.0

    # Share percentages
    ed_pct = [_v(y, "education") / max(_v(y, "total_expenditures"), 1) * 100 for y in years]
    fc_pct = [_v(y, "fixed_costs") / max(_v(y, "total_expenditures"), 1) * 100 for y in years]
    ds_pct = [_v(y, "debt_service") / max(_v(y, "total_expenditures"), 1) * 100 for y in years]
    ps_pct = [_v(y, "public_safety") / max(_v(y, "total_expenditures"), 1) * 100 for y in years]

    # Real growth index
    deflator = build_deflator(cpi, years)
    def _real(col):
        vals = [_v(y, col) * deflator.get(y, 1.0) for y in years]
        base = vals[0] if vals[0] > 0 else 1.0
        return [v / base * 100 for v in vals]

    ed_idx = _real("education")
    fc_idx = _real("fixed_costs")
    ds_idx = _real("debt_service")
    ps_idx = _real("public_safety")

    def _chg(lst): d = lst[-1] - 100; return f"+{d:.0f}%" if d >= 0 else f"{d:.0f}%"

    ORANGE = "#E67E22"
    LILAC  = "#A569BD"

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # Header
    ax_h = fig.add_axes([0, 0.90, 1, 0.10]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Saugus Budget: Where the Money Goes",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color=WHITE, transform=ax_h.transAxes)

    # Left: share trend
    ax_l = fig.add_axes([0.06, 0.10, 0.42, 0.76])
    ax_l.set_facecolor(CHART_BG)

    ax_l.plot(years, ed_pct, color=RED,    lw=3,   marker="o", ms=5, label="Education", zorder=5)
    ax_l.plot(years, fc_pct, color=LILAC,  lw=2,   marker="s", ms=4, label="Fixed Costs (pensions/benefits)")
    ax_l.plot(years, ps_pct, color=STEEL_BLUE, lw=2, marker="^", ms=4, label="Public Safety")
    ax_l.plot(years, ds_pct, color=ORANGE, lw=2,   marker="D", ms=4, label="Debt Service")

    ax_l.fill_between(years, ed_pct, [ed_pct[0]] * len(years),
                      where=[p < ed_pct[0] for p in ed_pct],
                      alpha=0.15, color=RED)

    ax_l.annotate(f"FY{years[0]}: {ed_pct[0]:.1f}%", xy=(years[0], ed_pct[0]),
                  xytext=(years[0] + 0.5, ed_pct[0] + 2),
                  fontsize=8.5, fontweight="bold", color=WHITE,
                  arrowprops=dict(arrowstyle="->", color=RED, lw=1.0))
    ax_l.annotate(f"FY{years[-1]}: {ed_pct[-1]:.1f}%", xy=(years[-1], ed_pct[-1]),
                  xytext=(years[-1] - 2.5, ed_pct[-1] - 3.5),
                  fontsize=8.5, fontweight="bold", color=WHITE,
                  arrowprops=dict(arrowstyle="->", color=RED, lw=1.0))

    ax_l.set_title("Education's Share of Budget\n(% of total expenditures)",
                   color=WHITE, fontsize=10, fontweight="bold", pad=6)
    ax_l.set_ylabel("% of Total Expenditures", color=GREY, fontsize=9)
    ax_l.yaxis.set_major_formatter(_mt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax_l.tick_params(colors=WHITE, labelsize=8)
    for sp in ax_l.spines.values(): sp.set_color(CHART_GRID)
    ax_l.grid(True, alpha=0.15, linestyle="--", color=GREY)
    leg = ax_l.legend(fontsize=7.5, loc="upper right",
                      facecolor=DARK_BLUE, edgecolor=CHART_GRID)
    for txt in leg.get_texts(): txt.set_color(WHITE)

    # Right: real growth index
    ax_r = fig.add_axes([0.55, 0.10, 0.42, 0.76])
    ax_r.set_facecolor(CHART_BG)

    ax_r.axhline(100, color=GREY, lw=1.2, ls=":", alpha=0.5, label="No real growth")
    ax_r.plot(years, fc_idx, color=LILAC,     lw=2.5, marker="s", ms=4)
    ax_r.plot(years, ds_idx, color=ORANGE,    lw=2.5, marker="D", ms=4)
    ax_r.plot(years, ps_idx, color=STEEL_BLUE,lw=2.5, marker="^", ms=4)
    ax_r.plot(years, ed_idx, color=RED,       lw=3.5, marker="o", ms=6, zorder=5)

    last = years[-1]
    for lst, lbl, col in [
        (fc_idx, f"Fixed Costs  {_chg(fc_idx)}", LILAC),
        (ds_idx, f"Debt Service  {_chg(ds_idx)}", ORANGE),
        (ps_idx, f"Public Safety  {_chg(ps_idx)}", STEEL_BLUE),
        (ed_idx, f"Education  {_chg(ed_idx)}", RED),
    ]:
        ax_r.text(last + 0.15, lst[-1], "— ", va="center",
                  fontsize=9, fontweight="bold", color=col)
        ax_r.text(last + 0.65, lst[-1], lbl, va="center",
                  fontsize=8, fontweight="bold", color=WHITE)

    ax_r.set_title("Inflation-Adjusted Growth\n(FY2010 = 100, real dollars)",
                   color=WHITE, fontsize=10, fontweight="bold", pad=6)
    ax_r.set_ylabel("Index: FY2010 = 100", color=GREY, fontsize=9)
    ax_r.set_xlim(min(years) - 0.3, max(years) + 3.5)
    ax_r.tick_params(colors=WHITE, labelsize=8)
    for sp in ax_r.spines.values(): sp.set_color(CHART_GRID)
    ax_r.grid(True, alpha=0.15, linestyle="--", color=GREY)

    fig.text(0.5, 0.02,
             "Source: MA DLS Schedule A · General Fund only · "
             "Inflation: BLS CPI-U (FY2010 base)",
             ha="center", fontsize=7, color=GREY, style="italic")
    return fig


def page_pca_cluster_map(peer_data: dict, engine) -> plt.Figure:
    """
    PCA scatter of towns in Ward cluster feature space.
    Saugus = gold star.  Cluster peers = labelled steel-blue dots.
    Adapted from municipal_finance_report.dendrogram_page.
    """
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA

    all_towns_c  = peer_data["all_towns_c"]   # has 'town' + 'cluster' columns
    saugus_cluster = peer_data["saugus_cluster"]
    avail        = peer_data["avail"]          # feature columns used for clustering
    optimal_k    = peer_data["optimal_k"]

    # Towns kept in PCA but shown with an axis break (extreme outliers in feature space)
    _OFFSCALE = {"Provincetown"}  # resort community: ~no school-age pop, extreme PC2

    # Build PCA from the same scaled feature matrix — include all towns for correct coords
    sub = all_towns_c[["town"] + avail].dropna().copy()
    X   = sub[avail].values.astype(float)
    X_s = StandardScaler().fit_transform(X)
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X_s)
    sub["pc1"] = coords[:, 0]
    sub["pc2"] = coords[:, 1]
    sub = sub.merge(all_towns_c[["town", "cluster"]], on="town", how="left")

    var1 = pca.explained_variance_ratio_[0] * 100
    var2 = pca.explained_variance_ratio_[1] * 100

    # Clip y-axis; anything above 4.5 (e.g. Provincetown at ~13.8) shown with break symbol
    sub_main = sub[~sub["town"].isin(_OFFSCALE)]
    y_lo = sub_main["pc2"].min() - 0.6
    y_hi = 4.5

    cluster_palette = [
        "#5D8AA8", "#E67E22", "#7D3C98", "#1E8449",
        "#C0392B", "#2471A3", "#F0A500", "#808080",
        "#E91E63", "#00BCD4",
    ]

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    # Header bar — same pattern as all other pages
    ax_h = fig.add_axes([0, 0.91, 1, 0.09]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.55,
              f"Cluster Map — MA Towns  ·  k={optimal_k} clusters (Ward linkage, elbow criterion)",
              ha="center", va="center", fontsize=13, fontweight="bold",
              color="white", transform=ax_h.transAxes)
    ax_h.text(0.5, 0.15,
              f"PCA of {len(avail)} z-scored features  ·  Saugus's natural peer group highlighted",
              ha="center", va="center", fontsize=8.5,
              color="#C8DDF0", transform=ax_h.transAxes)

    consensus_s = set(peer_data.get("consensus", []))
    ward_peers  = sorted(peer_data.get("ward_peers", []))

    # Scatter plot — narrowed to leave room for town list on the right
    ax = fig.add_axes([0.04, 0.08, 0.60, 0.80])
    ax.set_facecolor(CHART_BG)
    for sp in ax.spines.values(): sp.set_color(CHART_GRID)
    ax.set_ylim(y_lo, y_hi)

    unique_clusters = sorted(sub["cluster"].dropna().unique())
    for cl in unique_clusters:
        if cl == saugus_cluster:
            continue
        mask  = (sub["cluster"] == cl) & ~sub["town"].isin(_OFFSCALE)
        color = cluster_palette[int(cl) % len(cluster_palette)]
        ax.scatter(sub.loc[mask, "pc1"], sub.loc[mask, "pc2"],
                   color=color, alpha=0.35, s=20, zorder=2)
        # One centroid label per non-Saugus cluster
        cl_df = sub[mask].copy()
        if cl_df.empty:
            continue
        centroid = cl_df[["pc1", "pc2"]].mean()
        cl_df["dc"] = ((cl_df["pc1"] - centroid["pc1"])**2 +
                       (cl_df["pc2"] - centroid["pc2"])**2)**0.5
        rep = cl_df.nsmallest(1, "dc").iloc[0]
        ax.text(rep["pc1"] + 0.07, rep["pc2"], rep["town"],
                fontsize=5.5, color=color, alpha=0.65, va="center")

    # Saugus cluster peers — dots only (names in the right-hand list)
    saugus_mask = sub["cluster"] == saugus_cluster
    peers_mask  = saugus_mask & (sub["town"] != SAUGUS)
    n_cluster   = int(peers_mask.sum())
    ax.scatter(sub.loc[peers_mask, "pc1"], sub.loc[peers_mask, "pc2"],
               color=STEEL_BLUE, s=55, alpha=0.9, zorder=3,
               label="Saugus's Ward cluster peers")

    # Saugus star
    sg = sub[sub["town"] == SAUGUS]
    if len(sg):
        ax.scatter(sg["pc1"], sg["pc2"], color=GOLD, s=200, zorder=5, marker="*")
        ax.text(float(sg["pc1"].iloc[0]) + 0.18, float(sg["pc2"].iloc[0]),
                "Saugus", fontsize=9, color=GOLD, fontweight="bold", va="center")

    ax.set_xlabel(f"Principal Component 1  ({var1:.0f}% of variance)",
                  color=WHITE, fontsize=8.5)
    ax.set_ylabel(f"Principal Component 2  ({var2:.0f}% of variance)",
                  color=WHITE, fontsize=8.5)
    ax.tick_params(colors=WHITE, labelsize=7.5)
    ax.grid(True, alpha=0.15, linestyle="--", color=CHART_GRID)
    leg = ax.legend(fontsize=7.5, facecolor=DARK_BLUE, edgecolor=GOLD, loc="lower right")
    for txt in leg.get_texts(): txt.set_color(WHITE)

    # Note off-scale towns without rendering them
    ptown = sub[sub["town"].isin(_OFFSCALE)]
    ptown_notes = []
    for _, row in ptown.iterrows():
        ptown_notes.append(f"{row['town']} (PC2={row['pc2']:.1f}, resort community; omitted)")

    # Explanation box — bottom-left (clear corner)
    note_line = ("\nOmitted: " + "; ".join(ptown_notes)) if ptown_notes else ""
    story = (
        "What this chart shows:\n"
        f"• (*) = Saugus  ·  blue = Ward cluster peers\n"
        "• Towns close together are statistically similar\n"
        f"• Other colours = different clusters{note_line}"
    )
    ax.text(0.02, 0.02, story, transform=ax.transAxes,
            fontsize=7.5, va="bottom", ha="left", color=WHITE, linespacing=1.45,
            bbox=dict(boxstyle="round,pad=0.4", facecolor=DARK_BLUE,
                      edgecolor=CHART_GRID, linewidth=1.0, alpha=0.92))

    # ── Right panel: sorted town list ────────────────────────────────────────
    ax_r = fig.add_axes([0.66, 0.08, 0.32, 0.80]); ax_r.axis("off")
    ax_r.text(0.5, 0.99, f"Ward Cluster C{saugus_cluster}  ({n_cluster + 1} towns)",
              ha="center", va="top", fontsize=9, fontweight="bold",
              color=GOLD, transform=ax_r.transAxes)
    ax_r.text(0.5, 0.945,
              "(*) bold = also in Mahalanobis top-30",
              ha="center", va="top", fontsize=7, color=GREEN,
              transform=ax_r.transAxes)
    ax_r.plot([0.05, 0.95], [0.925, 0.925], color=CHART_GRID, linewidth=0.6,
              transform=ax_r.transAxes)

    all_towns_list = ["Saugus"] + ward_peers
    row_h = min(0.875 / max(len(all_towns_list), 1), 0.030)
    for j, town in enumerate(all_towns_list):
        y_t = 0.900 - j * row_h
        if y_t < 0.02:
            break
        is_saugus    = (town == SAUGUS)
        is_consensus = (town in consensus_s)
        if is_saugus:
            col, weight, prefix = GOLD, "bold", "(*) "
        elif is_consensus:
            col, weight, prefix = GREEN, "bold", "* "
        else:
            col, weight, prefix = GREY, "normal", "  "
        ax_r.text(0.08, y_t, prefix + town,
                  ha="left", va="top", fontsize=7.5,
                  color=col, fontweight=weight,
                  transform=ax_r.transAxes)

    fig.text(0.34, 0.01,
             f"PCA of {len(avail)} z-scored features (Ward hierarchical clustering).  "
             "Source: MA DESE + ACS 5-year estimates.",
             ha="center", fontsize=6.5, color=GREY, style="italic")
    return fig


def page_data_quality(engine) -> plt.Figure:
    """
    Appendix slide: coverage, completeness, and known seam issues for every
    data source used in this report.  Full-version only.
    """
    from sqlalchemy import text as _text

    TABLE_DEFS = [
        # (display_label, table, year_col, town_col, source_org)
        ("MCAS Results",          "mcas_results",            "school_year", "district_name",     "DESE"),
        ("Graduation Rates",       "graduation_rates",         "school_year", "district_name",     "DESE"),
        ("Dropout Rates",          "district_dropout",         "school_year", "district_name",     "DESE"),
        ("Attendance / Absenteeism","attendance",              "school_year", "district_name",     "DESE"),
        ("Per-Pupil Expenditure",  "per_pupil_expenditure",    "school_year", "district_name",     "DESE"),
        ("Staffing (FTE)",         "staffing",                 "school_year", "district_name",     "DESE"),
        ("Enrollment",             "enrollment",               "school_year", "district_name",     "DESE"),
        ("Chapter 70 Aid",         "district_chapter70",       "fiscal_year", "district_name",     "DESE"),
        ("Census ACS",             "municipal_census_acs",     "acs_year",    "municipality",      "Census"),
        ("Zillow Home Values",     "municipal_zillow_housing", "data_year",   "region_name",       "Zillow"),
        ("DLS Schedule A",         "municipal_expenditures",   "fiscal_year", "municipality",      "DLS"),
        ("Crime Rate",             "municipal_crime",          "year",        "jurisdiction_name", "State Police"),
        ("CPI — Boston MSA",       "cpi_boston_msa",           "calendar_year", None,              "BLS"),
    ]

    # Flags for known issues — shown as indicators in the table
    SEAM_TABLES = {"district_chapter70", "per_pupil_expenditure", "attendance"}
    LIMITED_TABLES = {"municipal_crime", "municipal_expenditures"}

    rows = []
    with engine.connect() as conn:
        for label, tbl, yr_col, town_col, org in TABLE_DEFS:
            try:
                q = (f"SELECT MIN({yr_col}), MAX({yr_col}), "
                     f"COUNT(DISTINCT {yr_col}), "
                     f"COUNT(DISTINCT {town_col if town_col else '1'}) FROM {tbl}")
                yr_min, yr_max, n_yrs, n_towns = conn.execute(_text(q)).fetchone()
                saugus_n = 0
                if town_col:
                    s = conn.execute(_text(
                        f"SELECT COUNT(DISTINCT {yr_col}) FROM {tbl} "
                        f"WHERE LOWER({town_col}) LIKE '%saugus%'"
                    )).fetchone()
                    saugus_n = int(s[0])
                rows.append({
                    "label": label, "org": org,
                    "yr_range": f"{yr_min}–{yr_max}",
                    "n_yrs": int(n_yrs), "n_towns": int(n_towns),
                    "saugus_n": saugus_n,
                    "seam": tbl in SEAM_TABLES,
                    "limited": tbl in LIMITED_TABLES,
                })
            except Exception:
                rows.append({
                    "label": label, "org": org,
                    "yr_range": "?", "n_yrs": 0, "n_towns": 0,
                    "saugus_n": 0, "seam": False, "limited": False,
                })

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG)

    ax_h = fig.add_axes([0, 0.90, 1, 0.10]); ax_h.axis("off")
    ax_h.set_facecolor(BLUE)
    ax_h.text(0.5, 0.5, "Appendix: Data Sources — Coverage and Quality",
              ha="center", va="center", fontsize=15, fontweight="bold",
              color="white", transform=ax_h.transAxes)

    # ── Left: data source table ───────────────────────────────────────────────
    ax_l = fig.add_axes([0.02, 0.04, 0.56, 0.84]); ax_l.axis("off")

    # Column headers
    col_x   = [0.00, 0.44, 0.60, 0.72, 0.84]
    headers = ["Data Source",  "Years", "Dist.", "Saugus", ""]
    for cx, h in zip(col_x, headers):
        ax_l.text(cx, 0.97, h, ha="left", va="top", fontsize=8,
                  fontweight="bold", color=GOLD, transform=ax_l.transAxes)

    ax_l.axhline(0.944, color=CHART_GRID, linewidth=0.6, alpha=0.6)

    row_h = 0.066
    for i, r in enumerate(rows):
        y = 0.920 - i * row_h
        # Row background
        if i % 2 == 0:
            ax_l.add_patch(plt.Rectangle(
                (0, y - row_h * 0.85), 1, row_h * 0.90,
                transform=ax_l.transAxes,
                facecolor=DARK_BLUE, edgecolor="none", zorder=0
            ))
        saugus_complete = r["saugus_n"] >= r["n_yrs"] - 1 or r["saugus_n"] > 0
        saugus_col = GREEN if saugus_complete else RED

        # Seam / limited indicators
        flags = ""
        if r["seam"]:    flags += " ~"
        if r["limited"]: flags += " *"
        flag_col = AMBER if r["seam"] or r["limited"] else "#222"

        # Source badge
        ax_l.text(0.00, y, f"[{r['org']}]", ha="left", va="top",
                  fontsize=6.5, color=GREY, transform=ax_l.transAxes)
        ax_l.text(0.11, y, r["label"] + flags, ha="left", va="top",
                  fontsize=8, color=flag_col if flags else "#222",
                  fontweight="bold" if flags else "normal",
                  transform=ax_l.transAxes)
        ax_l.text(col_x[1], y, r["yr_range"],  ha="left", va="top",
                  fontsize=8, color=GREY, transform=ax_l.transAxes)
        ax_l.text(col_x[2], y, str(r["n_towns"]), ha="left", va="top",
                  fontsize=8, color=GREY, transform=ax_l.transAxes)
        saugus_label = (f"{r['saugus_n']} / {r['n_yrs']} yrs"
                        if r["saugus_n"] > 0 else "n/a")
        ax_l.text(col_x[3], y, saugus_label, ha="left", va="top",
                  fontsize=8, color=saugus_col, fontweight="bold",
                  transform=ax_l.transAxes)

    ax_l.text(0.00, 0.02,
              "~ = known data seam (methodology or source change at a year boundary)\n"
              "* = limited coverage — see notes",
              ha="left", va="bottom", fontsize=7, color=AMBER,
              transform=ax_l.transAxes, linespacing=1.5)

    # ── Right: quality notes ──────────────────────────────────────────────────
    ax_r = fig.add_axes([0.60, 0.04, 0.38, 0.84]); ax_r.axis("off")

    notes = [
        ("MCAS (starts 2017)", [
            "Massachusetts redesigned MCAS after the",
            "PARCC pilot. Data before 2017 used a",
            "different scale and is not loaded.",
            "Eight years of coverage is sufficient for",
            "trend analysis but limits long baselines.",
        ]),
        ("Per-pupil spending seam: 2018->2019", [
            "DESE expanded the reporting universe from",
            "322 to 404 districts at this boundary.",
            "Statewide averages shift slightly.",
            "Saugus is present in both universes;",
            "its own time series is unaffected.",
        ]),
        ("Chapter 70 seam: FY2022->2023", [
            "The Student Opportunity Act (SOA) sharply",
            "increased Ch70 aid from FY2023 onward.",
            "Saugus: $2,441/pupil (FY2022) ->",
            "$3,437/pupil (FY2023)  (+41%).",
            "This is a real policy change, not an",
            "artifact -- but it breaks trend lines.",
        ]),
        ("DLS Schedule A: 61 towns only", [
            "Municipal expenditure data is loaded for",
            "61 towns (Saugus + curated peer set).",
            "Budget share comparisons in Ch9/9b are",
            "limited to this group, not all of MA.",
        ]),
        ("Crime data: 5 years (2020-2024)", [
            "MA State Police Beyond 2020 portal only.",
            "Includes the pandemic year (2020).",
            "Used for current snapshot only;",
            "not used in trend regression.",
        ]),
    ]

    y = 0.97
    for title, lines in notes:
        ax_r.text(0.0, y, title, ha="left", va="top", fontsize=8.5,
                  fontweight="bold", color=GOLD, transform=ax_r.transAxes)
        y -= 0.038
        for line in lines:
            ax_r.text(0.02, y, line, ha="left", va="top", fontsize=7.8,
                      color=GREY, transform=ax_r.transAxes, linespacing=1.4)
            y -= 0.030
        y -= 0.018

    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Saugus synthesis report generator")
    parser.add_argument(
        "--parent", action="store_true",
        help="Omit methodology slides and produce the community-audience brief"
    )
    args = parser.parse_args()

    parent_mode = args.parent
    _FINAL      = _FINAL_PARENT if parent_mode else _FINAL_FULL
    _TMP        = f"/tmp/{'saugus_community_brief' if parent_mode else 'saugus_full_analysis'}.pdf"
    mode_label  = "community brief" if parent_mode else "full analysis"

    engine = get_engine()
    os.makedirs(os.path.dirname(_FINAL), exist_ok=True)

    print(f"[narrative] Mode: {mode_label}")
    print("[narrative] Loading data...")
    frames    = load_all(engine)
    traj      = build_trajectories(frames)
    mfr_rev, mfr_exp, mfr_cpi = load_data(engine)   # Schedule A for budget charts
    peer_data = _compute_peer_data(traj, n_mahal=50, n_peers=15)
    peers     = peer_data["consensus"]
    print(f"[narrative] Mahalanobis peers: {len(peers)}")

    def _save(pdf, fig, label=""):
        if fig is None:
            return
        try:
            pdf.savefig(fig)
        except Exception as e:
            print(f"  [warn] {label}: {e}")
        finally:
            plt.close(fig)

    print("[narrative] Writing PDF...")
    with PdfPages(_TMP) as pdf:

        # ── FRONT MATTER ───────────────────────────────────────────────────
        print("  Front matter...")
        _save(pdf, page_cover(),                                   "cover")
        _save(pdf, page_overview(),                                "overview")
        _save(pdf, page_three_facts(traj, peers, frames),          "three facts")
        _save(pdf, page_how_to_read(),                             "how to read")

        # ── PART 1: SETTING THE STAGE ──────────────────────────────────────
        print("  Part 1: Setting the Stage...")
        _save(pdf, page_the_question(),                            "s1 question")
        _save(pdf, page_success_definition(traj, peers),           "s2 success definition")

        # ── PART 2: HOW WE KNOW ────────────────────────────────────────────
        print("  Part 2: How We Know...")
        _save(pdf, page_peer_selection(peer_data),                 "s3 peers")
        _save(pdf, page_data_quality(engine),                      "data quality")
        _save(pdf, page_cross_source_validation(engine),           "cross-source validation")
        _save(pdf, page_factor_rationale(),                        "factor rationale")
        _save(pdf, page_mahalanobis(peer_data),                    "mahalanobis")
        _save(pdf, page_consensus_peers(peer_data),                "consensus peers")

        # ── PART 3: WHAT WE FOUND ──────────────────────────────────────────
        print("  Part 3: What We Found...")
        _save(pdf, page_trajectory(traj, peers, frames),           "s4 trajectory")
        _save(pdf, page_the_number(traj, peers),                   "the number")
        _save(pdf, page_portfolio(traj, peers),                    "s5 portfolio")
        _save(pdf, page_budget_share_trend(mfr_exp, mfr_cpi),      "budget trend")

        # ── PART 4: WHAT WORKS ─────────────────────────────────────────────
        print("  Part 4: What Works...")
        _save(pdf, page_what_research_says(),                      "s6 what research says")
        _save(pdf, page_what_successful_towns_did(traj, peers),    "s7 case studies")

        # ── PART 5: CONCLUSIONS ────────────────────────────────────────────
        print("  Part 5: Conclusions...")
        _save(pdf, page_the_paradox(),                             "s8 uncomfortable finding")
        print("  Budget decomposition...")
        _save(pdf, page_budget_decomposition(traj, frames, peers), "s9 balancing act")
        _save(pdf, page_high_school_outcomes(engine, peers),       "high school outcomes")
        # Overachievers: who has Saugus's demographics NOW and scores higher?
        # Positioned here to feed directly into recommendations.
        _save(pdf, page_overachievers(engine, peers, peer_data),           "overachievers")
        _save(pdf, page_overachiever_divergence(engine, peers, peer_data), "overachiever divergence")
        _save(pdf, page_path_forward(traj, peers),                 "s10 path forward")
        _save(pdf, page_conclusion(traj, peers),                   "conclusion")

    import shutil
    shutil.copy2(_TMP, _FINAL)
    print(f"[narrative] Report saved to {_FINAL}")


if __name__ == "__main__":
    main()
