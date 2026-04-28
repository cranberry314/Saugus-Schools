"""
Comprehensive Mahalanobis peer analysis for MA districts.

Uses all Tier 1-3 variables from db/queries.py:
  Tier 1: enrollment, high_needs_pct, ell_pct, low_income_pct, sped_pct,
          nss_per_pupil, ch70_per_pupil
  Tier 2: teacher_ppe, admin_ppe, admin_teacher_ratio, pupil_svcs_ppe,
          teacher_fte, teachers_per_100_fte, para_fte, teacher_avg_salary
  Tier 3: pct_65_plus, median_hh_income, pct_owner_occupied, pct_bachelors_plus
  Bonus:  ela_me_pct, math_me_pct

Outputs:
  Reports/saugus_peers_comprehensive.xlsx  — Excel workbook (multi-sheet)
  Reports/saugus_peers_comprehensive.pdf   — PDF report with plain-language explanations

Run: python analysis/peers_comprehensive.py [--district 02620000] [--top 20]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import textwrap
import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine
from db.queries import (
    FEATURE_MATRIX_FULL, ALL_FEATURE_COLS, FEATURE_LABEL,
    FEATURE_TIER, FEATURE_CATALOG, TIER1_COLS, TIER2_COLS, TIER3_COLS, OUTCOME_COLS
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_DISTRICT = "02620000"   # Saugus
YEARS_TO_TRY     = [2024]
TOP_N            = 20
OUTPUT_DIR       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
XLSX_PATH        = os.path.join(OUTPUT_DIR, "saugus_peers_comprehensive.xlsx")
PDF_PATH         = os.path.join(OUTPUT_DIR, "saugus_peers_comprehensive.pdf")

# Features used for Mahalanobis: all tiers (Tier 1 + Tier 2 + Tier 3 + outcomes)
MAHAL_COLS = ALL_FEATURE_COLS

# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_matrix(engine, school_year: int) -> pd.DataFrame:
    """Fetch full feature matrix for a single year, ACS lagged by 1 year."""
    acs_yr = min(school_year - 1, 2023)
    acs_yr = max(acs_yr, 2014)
    with engine.connect() as conn:
        df = pd.read_sql(
            text(FEATURE_MATRIX_FULL), conn,
            params={"yr": school_year, "acs_yr": acs_yr}
        )
    df = df.set_index("org_code")
    return df


# ── Distance computation ──────────────────────────────────────────────────────

def _rank_by_distance(feat_filled: pd.DataFrame, base_org: str,
                      feature_cols: list) -> pd.DataFrame:
    """
    Compute Mahalanobis distance from base_org to all other rows.
    Returns DataFrame: org_code, mahal_dist, rank — sorted ascending.
    """
    use_cols = [c for c in feature_cols if c in feat_filled.columns]
    if not use_cols:
        return pd.DataFrame(columns=["org_code", "mahal_dist", "rank"])

    X    = feat_filled[use_cols].values.astype(float)
    base = feat_filled.loc[base_org, use_cols].values.astype(float)

    cov = np.cov(X, rowvar=False)
    if cov.ndim == 0:
        cov = np.array([[float(cov)]])
    cov_inv = np.linalg.pinv(cov + 1e-6 * np.eye(cov.shape[0]))

    rows = []
    for oc in feat_filled.index:
        if oc == base_org:
            continue
        diff = feat_filled.loc[oc, use_cols].values.astype(float) - base
        dist = float(np.sqrt(max(0.0, diff @ cov_inv @ diff)))
        rows.append({"org_code": oc, "mahal_dist": round(dist, 4)})

    result = (pd.DataFrame(rows)
                .sort_values("mahal_dist")
                .reset_index(drop=True))
    result["rank"] = result.index + 1
    return result


def compute_year(df: pd.DataFrame, base_org: str, top_n: int):
    """
    For one year's feature matrix, return:
      (full_ranked_df, loo_dict, saugus_features, active_cols)
    or None if insufficient data.
    """
    feat = df[MAHAL_COLS].copy().astype(float)

    # Drop all-NaN columns (e.g. MCAS cancelled in 2020, Ch70 before 2023)
    feat = feat.dropna(axis=1, how="all")
    if len(feat.columns) < 3:
        print(f"  [skip] only {len(feat.columns)} usable features")
        return None

    if base_org not in feat.index:
        print(f"  [skip] base district not in feature matrix")
        return None
    if feat.loc[base_org].notna().sum() < 3:
        print(f"  [skip] base district has < 3 non-null features")
        return None

    # Drop districts with fewer than 3 valid features
    feat = feat.dropna(thresh=3)
    if base_org not in feat.index:
        return None

    # Fill remaining NaN with column median
    feat_filled = feat.copy()
    for col in feat_filled.columns:
        med = feat_filled[col].median()
        if not np.isnan(med):
            feat_filled[col] = feat_filled[col].fillna(med)
    feat_filled = feat_filled.dropna(axis=1, how="all")

    active_cols = list(feat_filled.columns)
    saugus_features = feat.loc[base_org].to_dict()

    # Full distance ranking
    full_ranked = _rank_by_distance(feat_filled, base_org, active_cols)

    # Merge in district name / town
    meta = df[["district_name", "town"]].copy()
    full_ranked = full_ranked.merge(meta.reset_index(), on="org_code", how="left")

    # Attach peer feature values to each peer row for reference
    feat_vals = feat.reset_index()
    full_ranked = full_ranked.merge(feat_vals, on="org_code", how="left")

    # Select final column order
    avail_feat = [c for c in active_cols if c in full_ranked.columns]
    full_ranked = full_ranked[
        ["rank", "org_code", "district_name", "town", "mahal_dist"] + avail_feat
    ].head(top_n)

    return full_ranked, saugus_features, active_cols


# ── PDF helpers ───────────────────────────────────────────────────────────────

NAVY  = "#1F3864"
STEEL = "#2F5496"
LIGHT = "#D6E4F0"
GOLD  = "#C9A800"
RED   = "#C00000"

def _title_page(pdf: PdfPages):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.set_facecolor(NAVY)
    fig.patch.set_facecolor(NAVY)
    ax.axis("off")

    ax.text(0.5, 0.72, "Saugus Public Schools",
            ha="center", va="center", fontsize=26, color="white",
            fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.62, "Comprehensive Peer District Analysis",
            ha="center", va="center", fontsize=20, color=GOLD,
            transform=ax.transAxes)
    ax.text(0.5, 0.52, "Mahalanobis Distance — All Tiers",
            ha="center", va="center", fontsize=15, color=LIGHT,
            transform=ax.transAxes)
    ax.text(0.5, 0.38,
            "23 variables across demographics, spending, staffing,\n"
            "community characteristics & academic outcomes | School Years 2017–2025",
            ha="center", va="center", fontsize=12, color=LIGHT,
            transform=ax.transAxes, linespacing=1.8)
    ax.text(0.5, 0.08, "Massachusetts Department of Elementary and Secondary Education  •  "
            "Census ACS  •  MA DOR  •  DESE Chapter 70",
            ha="center", va="center", fontsize=8, color=LIGHT,
            transform=ax.transAxes, alpha=0.7)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _methodology_page(pdf: PdfPages):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0.08, 0.05, 0.84, 0.88])
    ax.axis("off")

    ax.text(0.5, 0.97, "How Peer Districts Are Identified",
            ha="center", va="top", fontsize=16, fontweight="bold",
            color=NAVY, transform=ax.transAxes)
    ax.axhline(y=0.93, color=STEEL, linewidth=1.5, xmin=0.05, xmax=0.95)

    body = textwrap.dedent("""\
    What is Mahalanobis distance?
    Mahalanobis distance is a statistical measure of how similar one district is to another when
    you look at many variables simultaneously. Unlike simply comparing two numbers, it accounts
    for the fact that some variables are naturally more spread out than others — so a $500
    difference in teacher spending carries a different weight than a 0.5% difference in ELL
    enrollment. A distance of 0 would mean a perfect twin; larger numbers mean more different.

    Why use it instead of simple ranking?
    A district might look similar to Saugus on enrollment and income, but differ sharply on
    % High Needs students. Mahalanobis distance finds districts that are simultaneously similar
    on all dimensions, not just one at a time. It is the standard approach used by state
    accountability systems and academic researchers for peer benchmarking.

    What variables are included?

    Tier 1 — Demographic & Fiscal
      • Total Enrollment            Scale of the district
      • % High Needs                Unduplicated ELL + Low Income + SPED composite — the single
                                    biggest driver of educational cost
      • % ELL                       English Language Learners; drives bilingual staffing costs
      • % Low Income                Correlates with both cost and outcome gaps
      • % SPED                      Special education is state-mandated and highly variable
      • Net School Spending/Pupil   The actual educational investment per student
      • Ch70 Aid/Pupil              State subsidy level; high Ch70 → less local fiscal flexibility
      • 4-Year Grad Rate            Outcome signal for longer-run district effectiveness
      • Chronic Absenteeism %       Attendance health of the student body

    Tier 2 — Spending & Staffing Detail
      • Teacher/Admin/Pupil Services spending per pupil
      • Admin-to-Teacher spending ratio
      • Teacher FTE, Teachers per 100 FTE, Paraprofessional FTE
      • Average teacher salary

    Tier 3 — Community Context (Census ACS, lagged 1 year)
      • % Age 65+                   Senior population share; affects override appetite & enrollment
      • Median Household Income     Community wealth; drives local contribution capacity
      • % Owner-Occupied Housing    Housing stability; correlates with enrollment stability
      • % Bachelor's Degree +       Adult education level; associated with parental engagement

    Tier 4 — Academic Outcomes (MCAS, when available)
      • ELA % Meeting/Exceeding     Not available SY2020 (COVID cancellation)
      • Math % Meeting/Exceeding    Not available SY2020 (COVID cancellation)

    How to read the results
    Districts are ranked 1 = most similar to Saugus across all available variables simultaneously.
    When a Tier 3 or Tier 4 variable is missing for a given year, it is dropped from that year's
    calculation (median-imputed where only a few districts are missing, excluded entirely if the
    whole column is null). The number of features used is shown on each year's page.
    """)

    ax.text(0.02, 0.89, body,
            ha="left", va="top", fontsize=8.5,
            color="#1a1a1a", transform=ax.transAxes,
            family="monospace", linespacing=1.55)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _features_table_page(pdf: PdfPages, active_by_year: dict):
    """Show which features were available in each year."""
    years = sorted(active_by_year.keys())
    all_feats = ALL_FEATURE_COLS[:]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    ax.axis("off")
    ax.set_title("Feature Availability by Year", fontsize=14, fontweight="bold",
                 color=NAVY, pad=12)

    rows_data = []
    for feat in all_feats:
        row = [FEATURE_LABEL.get(feat, feat), f"Tier {FEATURE_TIER.get(feat,'?')}"]
        for yr in years:
            row.append("✓" if feat in active_by_year.get(yr, []) else "–")
        rows_data.append(row)

    col_labels = ["Variable", "Tier"] + [str(y) for y in years]
    tbl = ax.table(
        cellText=rows_data,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.scale(1, 1.3)

    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_facecolor(STEEL)
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#F0F4FA")
        if c == 0:
            cell.set_text_props(ha="left")
        # Color check marks green
        if r > 0 and c >= 2 and cell.get_text().get_text() == "✓":
            cell.set_facecolor("#C6EFCE")
        elif r > 0 and c >= 2 and cell.get_text().get_text() == "–":
            cell.set_facecolor("#FFCCCC")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _year_page(pdf: PdfPages, year: int, full_ranked: pd.DataFrame,
               saugus_features: dict, active_cols: list,
               grad_corr: dict | None = None):
    """One page per school year: bar chart of top peers + Saugus feature table."""
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    gs = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[1.8, 1], wspace=0.04)

    # ── Left: horizontal bar chart ────────────────────────────────────────────
    ax_bar = fig.add_subplot(gs[0])
    plot_df = full_ranked.head(15).copy()
    labels  = plot_df["district_name"].fillna(plot_df["org_code"])
    dists   = plot_df["mahal_dist"]

    colors = [STEEL if i < 5 else LIGHT for i in range(len(plot_df))]
    bars = ax_bar.barh(range(len(plot_df)), dists, color=colors, edgecolor="white")
    ax_bar.set_yticks(range(len(plot_df)))
    ax_bar.set_yticklabels(
        [f"{r}. {n}" for r, n in zip(plot_df["rank"], labels)],
        fontsize=8
    )
    ax_bar.invert_yaxis()
    ax_bar.set_xlabel("Mahalanobis Distance (lower = more similar)", fontsize=8)
    ax_bar.set_title(
        f"SY {year}  —  Top 15 Peer Districts for Saugus\n"
        f"({len(active_cols)} features used)",
        fontsize=11, fontweight="bold", color=NAVY, loc="left"
    )
    ax_bar.spines[["top", "right"]].set_visible(False)
    ax_bar.tick_params(axis="both", labelsize=8)

    # Annotate bar values
    for bar, val in zip(bars, dists):
        ax_bar.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2,
                    f"{val:.2f}", va="center", ha="left", fontsize=7, color="#555")

    # ── Right: Saugus feature values table ───────────────────────────────────
    ax_tbl = fig.add_subplot(gs[1])
    ax_tbl.axis("off")

    tbl_rows = []
    for col in active_cols:
        val = saugus_features.get(col)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            fmt = "—"
        elif col in ("total_enrollment", "teacher_fte", "para_fte"):
            fmt = f"{int(round(val)):,}"
        elif col in ("nss_per_pupil", "teacher_ppe", "admin_ppe", "pupil_svcs_ppe",
                     "median_hh_income", "teacher_avg_salary", "ch70_per_pupil"):
            fmt = f"${val:,.0f}"
        elif col == "admin_teacher_ratio":
            fmt = f"{val:.3f}"
        else:
            fmt = f"{val:.1f}%"
        r_val = grad_corr.get(col) if grad_corr else None
        corr_fmt = f"{r_val:+.2f}" if r_val is not None and not np.isnan(r_val) else "—"
        tbl_rows.append([FEATURE_LABEL.get(col, col), fmt, corr_fmt])

    if tbl_rows:
        tbl = ax_tbl.table(
            cellText=tbl_rows,
            colLabels=["Feature (Saugus)", "Value", "r vs Grad"],
            loc="upper center",
            cellLoc="left",
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(7)
        tbl.scale(1, 1.2)
        for (r, c), cell in tbl.get_celld().items():
            if r == 0:
                cell.set_facecolor(STEEL)
                cell.set_text_props(color="white", fontweight="bold")
            elif r % 2 == 0:
                cell.set_facecolor("#F0F4FA")
            cell.set_edgecolor("#cccccc")
            # Colour the correlation column: green=positive, red=negative
            if r > 0 and c == 2:
                txt = tbl_rows[r - 1][2]
                try:
                    rv = float(txt)
                    if rv >= 0.3:
                        cell.set_facecolor("#C6EFCE")
                    elif rv <= -0.3:
                        cell.set_facecolor("#FFCCCC")
                except ValueError:
                    pass
        ax_tbl.text(0.5, -0.02,
                    "r vs Grad = Pearson correlation with 4-year grad rate across all MA districts",
                    ha="center", va="top", fontsize=6.5, color="#666",
                    style="italic", transform=ax_tbl.transAxes)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)



def _feature_trends_page(pdf: PdfPages, saugus_by_year: dict):
    """Line charts of Saugus's own feature values over time."""
    if not saugus_by_year:
        return

    years = sorted(saugus_by_year.keys())
    plot_groups = [
        ("Demographics", ["high_needs_pct", "ell_pct", "low_income_pct", "sped_pct"], "%"),
        ("Spending / Pupil", ["nss_per_pupil", "teacher_ppe", "admin_ppe", "pupil_svcs_ppe"], "$"),
        ("Staffing FTE", ["teacher_fte", "para_fte"], "FTE"),
        ("Community (ACS)", ["median_hh_income", "pct_65_plus", "pct_owner_occupied",
                              "pct_bachelors_plus"], "mixed"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.patch.set_facecolor("white")
    fig.suptitle("Saugus — Feature Trends Over Time", fontsize=14,
                 fontweight="bold", color=NAVY)

    colors_cycle = [STEEL, GOLD, RED, "#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#3B1F2B"]

    for ax, (title, cols, unit) in zip(axes.flat, plot_groups):
        ax.set_title(title, fontsize=10, fontweight="bold", color=NAVY)
        ax.set_facecolor("#FAFBFF")
        ax.spines[["top", "right"]].set_visible(False)

        for col, color in zip(cols, colors_cycle):
            vals = [saugus_by_year[y].get(col) for y in years]
            vals_clean = [
                (y, v) for y, v in zip(years, vals)
                if v is not None and not (isinstance(v, float) and np.isnan(v))
            ]
            if not vals_clean:
                continue
            ys, vs = zip(*vals_clean)
            ax.plot(ys, vs, marker="o", markersize=4, linewidth=1.8,
                    label=FEATURE_LABEL.get(col, col), color=color)

        ax.legend(fontsize=6.5, loc="best", framealpha=0.7)
        ax.set_xlabel("School Year", fontsize=8)
        if unit == "%":
            ax.set_ylabel("Percent", fontsize=8)
        elif unit == "$":
            ax.set_ylabel("Dollars per Pupil", fontsize=8)
        elif unit == "FTE":
            ax.set_ylabel("FTE", fontsize=8)
        ax.tick_params(labelsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _absenteeism_graduation_page(pdf: PdfPages, engine):
    """Two charts: chronic absenteeism and graduation rate over time.
    All districts shown in gray; Saugus in red. Pearson correlation annotated."""
    from sqlalchemy import text as _text

    with engine.connect() as conn:
        att_rows = conn.execute(_text("""
            SELECT school_year, org_code, chronic_absenteeism_pct
            FROM attendance WHERE student_group='All'
              AND chronic_absenteeism_pct IS NOT NULL
            ORDER BY school_year, org_code
        """)).fetchall()

        grad_rows = conn.execute(_text("""
            SELECT school_year, org_code, four_year_grad_pct
            FROM graduation_rates
            WHERE student_group='All'
              AND four_year_grad_pct IS NOT NULL
            ORDER BY school_year, org_code
        """)).fetchall()

    att_df  = pd.DataFrame(att_rows,  columns=["year", "org_code", "value"])
    grad_df = pd.DataFrame(grad_rows, columns=["year", "org_code", "value"])

    SAUGUS = "02620000"

    fig, axes = plt.subplots(1, 2, figsize=(13, 7))
    fig.patch.set_facecolor("white")
    fig.suptitle("Chronic Absenteeism & Graduation Rate Over Time",
                 fontsize=13, fontweight="bold", color=NAVY)

    def _plot_metric(ax, df, title, ylabel, fmt_pct=True, invert=False):
        years = sorted(df["year"].unique())
        # Gray lines for all districts
        for org, grp in df.groupby("org_code"):
            if org == SAUGUS:
                continue
            grp = grp.sort_values("year")
            ax.plot(grp["year"], grp["value"],
                    color="#cccccc", linewidth=0.4, alpha=0.5, zorder=1)

        # State mean line
        means = df.groupby("year")["value"].mean()
        ax.plot(means.index, means.values,
                color=STEEL, linewidth=2.0, linestyle="--",
                label="State mean", zorder=3)

        # Saugus in red
        saugus = df[df["org_code"] == SAUGUS].sort_values("year")
        if not saugus.empty:
            ax.plot(saugus["year"], saugus["value"],
                    color=RED, linewidth=2.2, marker="o", markersize=5,
                    label="Saugus", zorder=5)

        # Pearson correlation across all districts × years
        by_org = df.groupby("org_code")["value"].mean()
        if len(by_org) > 5:
            r = by_org.corr(by_org)   # placeholder — we correlate the two metrics below

        ax.set_title(title, fontsize=11, fontweight="bold", color=NAVY)
        ax.set_xlabel("School Year", fontsize=9)
        ax.set_ylabel(ylabel, fontsize=9)
        if fmt_pct:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_facecolor("#FAFBFF")
        ax.tick_params(labelsize=8)
        ax.legend(fontsize=8)
        return ax

    _plot_metric(axes[0], att_df,
                 "Chronic Absenteeism Rate", "% Chronically Absent")
    _plot_metric(axes[1], grad_df,
                 "4-Year Graduation Rate", "4-Year Grad Rate %")

    # Pearson correlation between the two metrics at district level
    # Use most recent year where both exist
    common_years = sorted(set(att_df["year"]) & set(grad_df["year"]), reverse=True)
    corr_note = ""
    if common_years:
        yr = common_years[0]
        a = att_df[att_df["year"] == yr].set_index("org_code")["value"]
        g = grad_df[grad_df["year"] == yr].set_index("org_code")["value"]
        common = a.index.intersection(g.index)
        if len(common) > 10:
            r = a.loc[common].corr(g.loc[common])
            corr_note = (f"Pearson r = {r:.3f} between chronic absenteeism and graduation rate "
                         f"across {len(common)} districts in SY{yr}  "
                         f"({'strong negative' if r < -0.5 else 'moderate negative' if r < -0.3 else 'weak'} correlation)")

    if corr_note:
        fig.text(0.5, 0.01, corr_note,
                 ha="center", va="bottom", fontsize=8.5,
                 color="#333333", style="italic")

    fig.tight_layout(rect=[0, 0.04, 1, 0.94])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _summary_page(pdf: PdfPages, summary_rows: list, year_dfs: dict):
    """Summary: who are Saugus's most consistent peers across all years?"""
    if not summary_rows:
        return

    all_ranked = pd.DataFrame(summary_rows)

    # Count how often each district appears in the top-N
    counts = (all_ranked.groupby(["org_code", "district_name"])
                        .agg(appearances=("year", "count"),
                             avg_rank=("rank", "mean"),
                             avg_dist=("mahal_dist", "mean"))
                        .sort_values(["appearances", "avg_rank"], ascending=[False, True])
                        .reset_index()
                        .head(20))

    fig, axes = plt.subplots(1, 2, figsize=(13, 7))
    fig.patch.set_facecolor("white")
    fig.suptitle("Most Consistent Peer Districts for Saugus Across All Years",
                 fontsize=13, fontweight="bold", color=NAVY)

    # Left: bar chart of appearances
    ax1 = axes[0]
    ax1.barh(range(len(counts)), counts["appearances"],
             color=[STEEL if i < 5 else LIGHT for i in range(len(counts))],
             edgecolor="white")
    ax1.set_yticks(range(len(counts)))
    ax1.set_yticklabels(
        [f"{r}  (avg rank {ar:.1f})"
         for r, ar in zip(counts["district_name"].fillna(counts["org_code"]),
                          counts["avg_rank"])],
        fontsize=8
    )
    ax1.invert_yaxis()
    ax1.set_xlabel("Years Appearing in Top-20 Peers", fontsize=9)
    ax1.set_title("Frequency in Peer Set", fontsize=10, fontweight="bold", color=NAVY)
    ax1.spines[["top", "right"]].set_visible(False)

    # Right: table — top-5 peers per year
    ax2 = axes[1]
    ax2.axis("off")
    years = sorted(year_dfs.keys())
    tbl_rows = []
    for yr in years:
        top5 = year_dfs[yr].head(5)["district_name"].fillna("?").tolist()
        tbl_rows.append([str(yr)] + top5 + [""] * (5 - len(top5)))

    tbl = ax2.table(
        cellText=tbl_rows,
        colLabels=["Year", "Peer 1", "Peer 2", "Peer 3", "Peer 4", "Peer 5"],
        loc="center",
        cellLoc="left",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.scale(1, 1.6)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_facecolor(STEEL)
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#F0F4FA")
        cell.set_edgecolor("#cccccc")
    ax2.set_title("Top-5 Peers by Year", fontsize=10, fontweight="bold",
                  color=NAVY, pad=8)

    fig.tight_layout(rect=[0, 0, 1, 0.92])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def _interpretation_page(pdf: PdfPages):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0.08, 0.05, 0.84, 0.88])
    ax.axis("off")

    ax.text(0.5, 0.97, "How to Use This Analysis",
            ha="center", va="top", fontsize=16, fontweight="bold",
            color=NAVY, transform=ax.transAxes)
    ax.axhline(y=0.93, color=STEEL, linewidth=1.5, xmin=0.05, xmax=0.95)

    body = textwrap.dedent("""\
    Reading the peer lists
    The districts listed as Saugus's peers are those that, when you look at enrollment,
    demographics, spending, staffing, community characteristics, and academic outcomes
    simultaneously, most closely resemble Saugus. These are the districts most appropriate
    for budget comparisons, contract negotiations, and policy benchmarking.

    A peer appearing in the top-5 across multiple years is a "structural peer" —
    a district that resembles Saugus year after year regardless of short-term fluctuations.
    Occasional appearances may reflect data gaps or transitional years.

    How missing data is handled
    When a variable is missing for a given year (e.g. MCAS in 2020, Tier 3 ACS for smaller
    districts), it is dropped from that year's calculation if the entire column is null.
    For variables missing only in a few districts, the column median is substituted so those
    districts remain in the comparison. The number of features actually used is shown at the
    top of each year's page.

    Important limitations
      • Ch70 per-pupil data covers FY2007–present (historical + current scrapers). Earlier
        years without Ch70 data use one fewer Tier 1 variable.
      • Census ACS data is lagged 1 year (5-year rolling average), so community variables
        reflect conditions roughly 2–3 years prior to the school year.
      • MCAS data was not collected in school year 2020 (COVID-19). That year's peers
        are computed without Tier 4 outcome variables.
      • Regional districts (e.g., Acton-Boxborough, Wachusett) appear as single entries
        in DESE but span multiple towns; their ACS data is matched to the primary town only.
      • SY2025 has limited data coverage (many variables not yet published by DESE);
        peer results for that year should be treated as preliminary.

    Data sources
      DESE Education to Career CSV (enrollment, demographics, spending, staffing)
      DESE Selected Populations (unduplicated High Needs %)
      DESE Chapter 70 Aid reports + keyfactors.xlsx (FY2007–present)
      DESE Graduation Rates & Attendance profiles
      Census ACS 5-year estimates (via Census API)
      MA Department of Revenue DLS Gateway (EQV, income per capita)
    """)

    ax.text(0.02, 0.89, body,
            ha="left", va="top", fontsize=8.5,
            color="#1a1a1a", transform=ax.transAxes,
            family="monospace", linespacing=1.55)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── Excel export ──────────────────────────────────────────────────────────────

def export_excel(year_dfs, summary_rows, all_sens_rows, saugus_by_year):
    print(f"[comprehensive] Writing Excel → {XLSX_PATH}")
    with pd.ExcelWriter(XLSX_PATH, engine="xlsxwriter") as writer:
        wb = writer.book
        hdr = wb.add_format({"bold": True, "bg_color": "#2F5496",
                              "font_color": "white", "border": 1})

        def _write(sheet, df, widths=None):
            df.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            ws.set_row(0, 18, hdr)
            if widths:
                for i, w in enumerate(widths):
                    ws.set_column(i, i, w)

        # Summary
        summary_df = pd.DataFrame(summary_rows)
        if not summary_df.empty:
            pivot = (summary_df.pivot_table(
                        index="year", columns="rank",
                        values="district_name", aggfunc="first")
                               .reset_index())
            pivot.columns = ["Year"] + [f"Peer #{c}" for c in pivot.columns[1:]]
            saugus_df = pd.DataFrame([
                {"year": yr, **{FEATURE_LABEL.get(k, k): v
                                for k, v in feats.items()}}
                for yr, feats in sorted(saugus_by_year.items())
            ])
            merged = pivot.merge(saugus_df, left_on="Year", right_on="year",
                                 how="left").drop(columns=["year"], errors="ignore")
            _write("Summary", merged)

        # Per-year sheets
        for yr, df_yr in sorted(year_dfs.items()):
            rename = {c: FEATURE_LABEL.get(c, c) for c in ALL_FEATURE_COLS}
            _write(f"Peers_{yr}", df_yr.rename(columns=rename))



# ── Main ──────────────────────────────────────────────────────────────────────

def run(district: str = DEFAULT_DISTRICT, top_n: int = TOP_N):
    engine = get_engine()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    year_dfs       = {}
    summary_rows   = []
    saugus_by_year = {}
    active_by_year = {}

    for year in YEARS_TO_TRY:
        print(f"\n[comprehensive] Year {year} ...")
        try:
            df = fetch_matrix(engine, year)
        except Exception as e:
            print(f"  [skip] fetch failed: {e}")
            continue

        result = compute_year(df, district, top_n)
        if result is None:
            continue

        full_ranked, saugus_feats, active_cols = result
        print(f"  {len(full_ranked)} peers ranked | "
              f"{len(active_cols)} features | top-3: "
              + ", ".join(full_ranked.head(3)["district_name"].fillna("?").tolist()))

        year_dfs[year]       = full_ranked
        saugus_by_year[year] = saugus_feats
        active_by_year[year] = active_cols

        for _, row in full_ranked.iterrows():
            summary_rows.append({
                "year": year, "rank": row["rank"],
                "district_name": row["district_name"],
                "town": row["town"],
                "org_code": row["org_code"],
                "mahal_dist": row["mahal_dist"],
            })

    if not year_dfs:
        print("[comprehensive] No data — nothing to export.")
        return

    # ── Per-feature Pearson correlation with grad rate (most recent year) ─────
    grad_corr = {}
    most_recent_yr = max(year_dfs.keys())
    try:
        df_recent = fetch_matrix(engine, most_recent_yr)
        grad_col  = df_recent["four_year_grad_pct"].dropna()
        for col in ALL_FEATURE_COLS:
            if col == "four_year_grad_pct" or col not in df_recent.columns:
                continue
            common = df_recent[col].dropna().index.intersection(grad_col.index)
            if len(common) > 20:
                grad_corr[col] = float(df_recent.loc[common, col].corr(grad_col.loc[common]))
        print(f"[comprehensive] Grad correlations computed for {len(grad_corr)} features (SY{most_recent_yr})")
    except Exception as e:
        print(f"[comprehensive] Warning: could not compute grad correlations: {e}")

    # ── Excel ─────────────────────────────────────────────────────────────────
    export_excel(year_dfs, summary_rows, [], saugus_by_year)

    # ── PDF ───────────────────────────────────────────────────────────────────
    print(f"[comprehensive] Writing PDF → {PDF_PATH}")
    with PdfPages(PDF_PATH) as pdf:
        _title_page(pdf)
        _methodology_page(pdf)
        _features_table_page(pdf, active_by_year)

        for yr in sorted(year_dfs):
            _year_page(pdf, yr, year_dfs[yr],
                       saugus_by_year[yr], active_by_year[yr],
                       grad_corr=grad_corr)
        _absenteeism_graduation_page(pdf, engine)

        _feature_trends_page(pdf, saugus_by_year)
        _summary_page(pdf, summary_rows, year_dfs)
        _interpretation_page(pdf)

    print(f"\n[comprehensive] Done.")
    print(f"  Excel: {XLSX_PATH}")
    print(f"  PDF:   {PDF_PATH}")
    print(f"  Years: {sorted(year_dfs.keys())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Comprehensive Mahalanobis peer analysis (all Tier 1-3 variables)")
    parser.add_argument("--district", default=DEFAULT_DISTRICT)
    parser.add_argument("--top", type=int, default=TOP_N)
    args = parser.parse_args()
    run(district=args.district, top_n=args.top)
