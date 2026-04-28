"""
Saugus Spending & Peer Distribution Report
===========================================

Produces Reports/saugus_spending_peers.pdf with:

  Page 1  — Title
  Page 2  — Per-pupil spending: nominal dollars (2009–2024)
  Page 3  — Per-pupil spending: inflation-adjusted 2024 dollars
  Pages 4+ — "Saugus vs State": for each feature, a histogram of all ~400 MA
             districts with Saugus marked and state percentile annotated
  Pages N+ — "Saugus vs Peer Group": for each feature, dot strip of the 20
             Mahalanobis peers with mean ± 1 SD band and Saugus marked
  Last    — Interpretation guide + full peer list

Run: python analysis/spending_report.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import textwrap
import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine
from db.queries import (
    FEATURE_MATRIX_FULL, MAHAL_FEATURE_COLS, FEATURE_LABEL, FEATURE_TIER,
)
from analysis.peers_comprehensive import fetch_matrix, compute_year

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy.stats import gaussian_kde

# ── Config ────────────────────────────────────────────────────────────────────
PEER_YEAR  = 2024
TOP_N      = 20
SAUGUS     = "02620000"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "saugus_spending_peers.pdf")

FEATS_PER_PAGE = 6   # 2 cols × 3 rows per page

# ── Colours ───────────────────────────────────────────────────────────────────
NAVY  = "#1F3864"
STEEL = "#2F5496"
LIGHT = "#D6E4F0"
GOLD  = "#C9A800"
RED   = "#C00000"
GRAY  = "#AAAAAA"

SPEND_COLORS = [STEEL, GOLD, "#4472C4", "#375623", "#E36C09"]


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_spending(engine) -> pd.DataFrame:
    sql = text("""
        SELECT school_year,
               MAX(CASE WHEN category='Total In-District Expenditures' THEN amount END) AS "Net School Spending",
               MAX(CASE WHEN category='Teachers'                       THEN amount END) AS "Teacher Spending",
               MAX(CASE WHEN category='Instructional Leadership'       THEN amount END) AS "Instructional Leadership",
               MAX(CASE WHEN category='Administration'                 THEN amount END) AS "Administration",
               MAX(CASE WHEN category='Pupil Services'                 THEN amount END) AS "Pupil Services"
        FROM per_pupil_expenditure
        WHERE org_code = :org
        GROUP BY school_year
        ORDER BY school_year
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"org": SAUGUS})
    return df.set_index("school_year").apply(pd.to_numeric, errors="coerce")


def load_cpi(engine) -> pd.Series:
    """Return cumulative price index, normalised so 2024 = 100."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT year, cpi_pct_change FROM inflation_cpi ORDER BY year")
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    cpi_pct = pd.Series({r[0]: float(r[1]) for r in rows})
    index = {}
    base = 100.0
    for i, yr in enumerate(cpi_pct.index):
        index[yr] = base if i == 0 else list(index.values())[-1] * (1 + cpi_pct[yr] / 100.0)
    idx = pd.Series(index)
    return idx / idx.get(2024, idx.iloc[-1]) * 100


def inflate_to_2024(df: pd.DataFrame, cpi: pd.Series) -> pd.DataFrame:
    result = df.copy().astype(float)
    for yr in result.index:
        if yr in cpi.index:
            result.loc[yr] *= 100.0 / cpi[yr]
        else:
            result.loc[yr] = np.nan
    return result


def _fmt(col: str):
    """Return a formatter function appropriate for a feature column."""
    dollar_cols = {"nss_per_pupil", "teacher_ppe", "admin_ppe", "pupil_svcs_ppe",
                   "instr_lead_ppe", "ch70_per_pupil", "teacher_avg_salary",
                   "median_hh_income"}
    if col in dollar_cols:
        return lambda x, _=None: f"${x:,.0f}"
    return lambda x, _=None: f"{x:.1f}"


def _is_dollar(col):
    return col in {"nss_per_pupil", "teacher_ppe", "admin_ppe", "pupil_svcs_ppe",
                   "instr_lead_ppe", "ch70_per_pupil", "teacher_avg_salary",
                   "median_hh_income"}


# ── Page builders ─────────────────────────────────────────────────────────────

def _title_page(pdf, n_state):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.set_facecolor(NAVY); fig.patch.set_facecolor(NAVY); ax.axis("off")
    ax.text(0.5, 0.72, "Saugus Public Schools",
            ha="center", fontsize=26, color="white", fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.62, "Spending Trends & District Benchmarks",
            ha="center", fontsize=20, color=GOLD, transform=ax.transAxes)
    ax.text(0.5, 0.49,
            "Section 1 — Per-Pupil Spending: Nominal & Inflation-Adjusted (2024 $)\n"
            f"Section 2 — Saugus vs State: position among all {n_state} MA districts\n"
            "Section 3 — Saugus vs Peer Group: 20 closest Mahalanobis peers\n"
            f"17 variables  |  School Year {PEER_YEAR}",
            ha="center", fontsize=12, color=LIGHT, transform=ax.transAxes, linespacing=2.0)
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _spending_page(pdf, nominal_df, real_df, cpi):
    cats = [c for c in nominal_df.columns if nominal_df[c].notna().any()]
    years_nom  = nominal_df.index.tolist()
    years_real = real_df.dropna(how="all").index.tolist()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 10))
    fig.patch.set_facecolor("white")
    fig.suptitle("Saugus Public Schools — Per-Pupil Expenditure",
                 fontsize=15, fontweight="bold", color=NAVY, y=0.99)

    for ax, df, years, title, ylab in [
        (ax1, nominal_df,  years_nom,  "Current (Nominal) Dollars",      "Dollars per Pupil"),
        (ax2, real_df,     years_real, "Inflation-Adjusted (2024 Dollars)", "2024 Dollars per Pupil"),
    ]:
        for cat, color in zip(cats, SPEND_COLORS):
            vals = df.loc[years, cat].astype(float)
            ax.plot(years, vals, marker="o", markersize=4, linewidth=2,
                    label=cat, color=color)
        ax.set_title(title, fontsize=11, fontweight="bold", color=NAVY, loc="left")
        ax.set_ylabel(ylab, fontsize=9)
        ax.set_xlabel("School Year", fontsize=9)
        ax.legend(fontsize=8, loc="upper left", framealpha=0.8)
        ax.set_facecolor("#FAFBFF")
        ax.spines[["top","right"]].set_visible(False)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax.set_xticks(years)
        ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=7)

    if cpi.index[0] > min(years_nom):
        ax2.text(0.01, 0.04, f"CPI data from {int(cpi.index[0])} onward; earlier years omitted.",
                 transform=ax2.transAxes, fontsize=7, color="#666", style="italic")

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: Saugus vs State
# ─────────────────────────────────────────────────────────────────────────────

def _state_section_title(pdf):
    fig, ax = plt.subplots(figsize=(11, 3))
    fig.patch.set_facecolor(STEEL); ax.set_facecolor(STEEL); ax.axis("off")
    ax.text(0.5, 0.60, "Section 2 — Saugus vs State",
            ha="center", fontsize=20, fontweight="bold", color="white",
            transform=ax.transAxes)
    ax.text(0.5, 0.20,
            "Where does Saugus fall in the statewide distribution of each variable?",
            ha="center", fontsize=11, color=LIGHT, transform=ax.transAxes)
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _state_distribution_pages(pdf, state_df, saugus_row, active_cols):
    """
    For each feature: KDE histogram of all MA districts (gray) with
    Saugus marked as a red vertical line + percentile annotation.
    """
    pages = [active_cols[i:i+FEATS_PER_PAGE]
             for i in range(0, len(active_cols), FEATS_PER_PAGE)]

    for page_cols in pages:
        n = len(page_cols)
        nrows = (n + 1) // 2
        fig, axes = plt.subplots(nrows, 2, figsize=(13, 3.2 * nrows))
        fig.patch.set_facecolor("white")
        fig.suptitle(f"Saugus vs State — SY{PEER_YEAR}  (all MA districts, n≈400)",
                     fontsize=12, fontweight="bold", color=NAVY)
        axes_flat = axes.flat if hasattr(axes, "flat") else [axes]

        for ax, col in zip(axes_flat, page_cols):
            _state_panel(ax, col, state_df, saugus_row)

        for ax in list(axes_flat)[len(page_cols):]:
            ax.set_visible(False)

        fig.tight_layout(rect=[0, 0, 1, 0.94])
        pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _state_panel(ax, col, state_df, saugus_row):
    label     = FEATURE_LABEL.get(col, col)
    tier      = FEATURE_TIER.get(col, "?")
    saugus_val = saugus_row.get(col)
    if saugus_val is None or (isinstance(saugus_val, float) and np.isnan(float(saugus_val))):
        saugus_val = None
    else:
        saugus_val = float(saugus_val)

    vals = state_df[col].dropna().astype(float) if col in state_df.columns else pd.Series([], dtype=float)

    ax.set_facecolor("#FAFBFF")
    ax.spines[["top","right","left"]].set_visible(False)
    ax.set_title(f"{label}  (Tier {tier})", fontsize=8.5, fontweight="bold",
                 color=NAVY, loc="left", pad=3)

    if vals.empty:
        ax.text(0.5, 0.5, "No state data", ha="center", va="center",
                transform=ax.transAxes, fontsize=8, color="#888")
        ax.set_yticks([]); return

    # KDE curve
    xmin, xmax = vals.min(), vals.max()
    if xmax > xmin:
        try:
            kde = gaussian_kde(vals, bw_method="scott")
            xs  = np.linspace(xmin - 0.05*(xmax-xmin),
                               xmax + 0.05*(xmax-xmin), 300)
            ys  = kde(xs)
            ax.fill_between(xs, ys, alpha=0.25, color=GRAY)
            ax.plot(xs, ys, color=GRAY, linewidth=1.2)
        except Exception:
            ax.hist(vals, bins=25, color=GRAY, alpha=0.4, edgecolor="white")

    # State mean
    state_mean = vals.mean()
    ax.axvline(state_mean, color=STEEL, linewidth=1.2, linestyle="--",
               alpha=0.7, label=f"State mean")

    # Saugus
    if saugus_val is not None:
        ax.axvline(saugus_val, color=RED, linewidth=2.0, label="Saugus")

        # Percentile
        pct = float((vals < saugus_val).mean() * 100)
        pct_label = f"{pct:.0f}th percentile"
        ax.set_xlabel(pct_label, fontsize=7.5, color=RED)

        # Text label on line
        ylim = ax.get_ylim()
        y_pos = ylim[1] * 0.88
        ax.text(saugus_val, y_pos, " Saugus", fontsize=7, color=RED,
                va="top", ha="left" if pct < 70 else "right")

        # State mean annotation
        ax.text(state_mean, y_pos * 0.65, f" Mean\n {_fmt(col)(state_mean)}",
                fontsize=6, color=STEEL, va="top",
                ha="left" if state_mean < saugus_val else "right")

    ax.set_yticks([])
    ax.tick_params(axis="x", labelsize=7)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(_fmt(col)))
    ax.set_xlim(xmin - 0.08*(xmax-xmin), xmax + 0.08*(xmax-xmin))


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: Saugus vs Peer Group
# ─────────────────────────────────────────────────────────────────────────────

def _peer_section_title(pdf, full_ranked):
    fig, ax = plt.subplots(figsize=(11, 3.5))
    fig.patch.set_facecolor(NAVY); ax.set_facecolor(NAVY); ax.axis("off")
    ax.text(0.5, 0.75, "Section 3 — Saugus vs Peer Group",
            ha="center", fontsize=20, fontweight="bold", color="white",
            transform=ax.transAxes)
    ax.text(0.5, 0.45,
            f"20 closest districts by 17-variable Mahalanobis distance  |  SY{PEER_YEAR}",
            ha="center", fontsize=11, color=LIGHT, transform=ax.transAxes)

    peers_str = "  ".join(
        f"{int(r['rank'])}. {r['district_name']}"
        for _, r in full_ranked.head(10).iterrows()
    )
    peers_str2 = "  ".join(
        f"{int(r['rank'])}. {r['district_name']}"
        for _, r in full_ranked.iloc[10:20].iterrows()
    )
    ax.text(0.5, 0.22, peers_str,  ha="center", fontsize=7, color=LIGHT, transform=ax.transAxes)
    ax.text(0.5, 0.08, peers_str2, ha="center", fontsize=7, color=LIGHT, transform=ax.transAxes)

    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _peer_distribution_pages(pdf, full_ranked, saugus_features, active_cols):
    peers = full_ranked[["district_name", "org_code"]
                        + [c for c in active_cols if c in full_ranked.columns]].copy()

    pages = [active_cols[i:i+FEATS_PER_PAGE]
             for i in range(0, len(active_cols), FEATS_PER_PAGE)]

    for page_cols in pages:
        n = len(page_cols)
        nrows = (n + 1) // 2
        fig, axes = plt.subplots(nrows, 2, figsize=(13, 3.2 * nrows))
        fig.patch.set_facecolor("white")
        fig.suptitle(
            f"Saugus vs Peer Group — SY{PEER_YEAR}  "
            f"(Top-{TOP_N} Mahalanobis peers  |  band = peer mean ± 1 SD)",
            fontsize=11, fontweight="bold", color=NAVY
        )
        axes_flat = axes.flat if hasattr(axes, "flat") else [axes]

        for ax, col in zip(axes_flat, page_cols):
            _peer_panel(ax, col, peers, saugus_features)

        for ax in list(axes_flat)[len(page_cols):]:
            ax.set_visible(False)

        fig.tight_layout(rect=[0, 0, 1, 0.94])
        pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _peer_panel(ax, col, peers, saugus_features):
    label = FEATURE_LABEL.get(col, col)
    tier  = FEATURE_TIER.get(col, "?")

    peer_vals = peers[col].dropna().astype(float) if col in peers.columns else pd.Series([], dtype=float)
    sv = saugus_features.get(col)
    saugus_val = float(sv) if sv is not None and not (isinstance(sv, float) and np.isnan(float(sv))) else None

    ax.set_facecolor("#FAFBFF")
    ax.spines[["top","right","left"]].set_visible(False)
    ax.set_title(f"{label}  (Tier {tier})", fontsize=8.5, fontweight="bold",
                 color=NAVY, loc="left", pad=3)

    if peer_vals.empty and saugus_val is None:
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=8, color="#888")
        ax.set_yticks([]); return

    peer_mean = peer_vals.mean() if not peer_vals.empty else np.nan
    peer_sd   = peer_vals.std()  if len(peer_vals) > 1 else 0.0

    # SD band
    if not np.isnan(peer_mean) and peer_sd > 0:
        ax.axvspan(peer_mean - peer_sd, peer_mean + peer_sd,
                   alpha=0.15, color=STEEL)
        ax.axvline(peer_mean - peer_sd, color=STEEL, lw=0.8, linestyle="--", alpha=0.5)
        ax.axvline(peer_mean + peer_sd, color=STEEL, lw=0.8, linestyle="--", alpha=0.5)

    # Mean line
    if not np.isnan(peer_mean):
        ax.axvline(peer_mean, color=STEEL, linewidth=1.5)

    # Peer dots (jittered vertically)
    n = len(peer_vals)
    jitter = np.random.default_rng(42).uniform(-0.28, 0.28, size=n)
    ax.scatter(peer_vals.values, np.ones(n) + jitter,
               color=STEEL, alpha=0.75, s=40, zorder=3)

    # Saugus star
    if saugus_val is not None:
        ax.scatter([saugus_val], [1.0], marker="*", color=RED, s=250, zorder=5)

        if not np.isnan(peer_mean) and peer_sd > 0:
            sd_dist = (saugus_val - peer_mean) / peer_sd
            sign    = "above" if sd_dist > 0 else "below"
            ax.annotate(
                f"{abs(sd_dist):.1f} SD {sign}",
                xy=(saugus_val, 1.0), xytext=(saugus_val, 1.45),
                fontsize=6.5, ha="center", color=RED,
                arrowprops=dict(arrowstyle="-", color=RED, lw=0.8),
            )

    ax.set_yticks([])
    ax.set_ylim(0.45, 1.85)
    ax.tick_params(axis="x", labelsize=7)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(_fmt(col)))

    # Stats footer
    if not np.isnan(peer_mean):
        fmt = _fmt(col)
        stats = f"Peer mean {fmt(peer_mean)}  ±  {fmt(peer_sd)}"
        if saugus_val is not None:
            diff = saugus_val - peer_mean
            sign = "+" if diff >= 0 else ""
            stats += f"   Saugus: {fmt(saugus_val)} ({sign}{fmt(diff)})"
        ax.text(0.02, -0.18, stats, transform=ax.transAxes,
                fontsize=6.5, color="#444", va="top",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="#F0F4FA",
                          edgecolor="#ccc", alpha=0.8))


def _interpretation_page(pdf):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0.07, 0.05, 0.86, 0.90])
    ax.axis("off")
    ax.text(0.5, 0.98, "How to Read This Report",
            ha="center", va="top", fontsize=15, fontweight="bold",
            color=NAVY, transform=ax.transAxes)

    body = textwrap.dedent(f"""\
    Spending charts (pages 2–3)
    ───────────────────────────
    Nominal chart: actual dollars spent per pupil, showing whether the budget grew in
    raw terms.  Inflation-adjusted chart: same figures converted to 2024 purchasing power
    using the US CPI (available from 2014).  A flat real line means spending kept pace
    with inflation; a falling real line means purchasing power declined even if the dollar
    amount stayed the same.

    Section 2 — Saugus vs State  (pages 4+)
    ─────────────────────────────────────────
    Each panel shows the full distribution of all ~400 Massachusetts school districts
    for one variable (grey density curve).  The red vertical line marks Saugus.
    The annotation gives Saugus's statewide percentile — e.g. "72nd percentile"
    means Saugus is higher than 72% of all MA districts on that metric.
    The blue dashed line marks the statewide mean.

    Use this section to understand Saugus's absolute position in the state.
    A high percentile on % High Needs, for example, means Saugus serves a
    disproportionately high-need population relative to most MA districts.

    Section 3 — Saugus vs Peer Group  (pages N+)
    ──────────────────────────────────────────────
    The peer group is the 20 districts most similar to Saugus when you look at
    all 17 variables simultaneously (Mahalanobis distance).  Each panel shows:

      Blue dots   = each of the 20 peer districts' values
      Blue band   = peer group mean ± 1 standard deviation
      Dashed lines= boundaries of the ±1 SD band
      Red star ★  = Saugus

    The annotation shows how many SDs above or below the peer mean Saugus falls:
      0 SD  = Saugus is exactly average within its peer group
      ±1 SD = near the edge of normal peer variation (~top or bottom 16%)
      ±2 SD = Saugus is an outlier even within its chosen peer set

    Why the two sections are different
    ────────────────────────────────────
    Mahalanobis distance minimises overall multivariate distance, not any single
    variable.  A district might be a close peer overall but differ on one dimension.
    Section 2 (vs State) shows Saugus's absolute statewide rank.
    Section 3 (vs Peers) shows how Saugus compares to its own most-similar group.
    The interesting story is where Saugus is an outlier within its peer set on a
    variable where it looks average statewide — or vice versa.

    SY{PEER_YEAR} Peer Districts
    Variables: 17  |  Method: Mahalanobis distance  |  Peers: top 20
    """)

    ax.text(0.02, 0.90, body,
            ha="left", va="top", fontsize=8.5, color="#1a1a1a",
            transform=ax.transAxes, family="monospace", linespacing=1.55)
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    engine = get_engine()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[spending_report] Loading spending data ...")
    spending_nom  = load_spending(engine)
    cpi           = load_cpi(engine)
    spending_real = inflate_to_2024(spending_nom, cpi)

    print(f"[spending_report] Building {PEER_YEAR} feature matrix ...")
    df_matrix = fetch_matrix(engine, PEER_YEAR)

    # Full state data (all districts, for vs-state charts)
    state_df = df_matrix[MAHAL_FEATURE_COLS].copy().astype(float)
    n_state  = len(state_df)

    # Saugus row from state data
    saugus_row = {col: df_matrix.loc[SAUGUS, col]
                  if col in df_matrix.columns and SAUGUS in df_matrix.index
                  else None
                  for col in MAHAL_FEATURE_COLS}

    print(f"[spending_report] Computing peer group ...")
    result = compute_year(df_matrix, SAUGUS, TOP_N)
    if result is None:
        print("[spending_report] ERROR: cannot compute peer group"); return
    full_ranked, _, saugus_features, active_cols = result
    print(f"  {len(full_ranked)} peers | {len(active_cols)} features | top-3: "
          + ", ".join(full_ranked.head(3)["district_name"].fillna("?").tolist()))

    print(f"[spending_report] Writing PDF → {OUTPUT_PDF}")
    with PdfPages(OUTPUT_PDF) as pdf:
        _title_page(pdf, n_state)
        _spending_page(pdf, spending_nom, spending_real, cpi)

        # Section 2 — vs State
        _state_section_title(pdf)
        _state_distribution_pages(pdf, state_df, saugus_row, active_cols)

        # Section 3 — vs Peer Group
        _peer_section_title(pdf, full_ranked)
        _peer_distribution_pages(pdf, full_ranked, saugus_features, active_cols)

        _interpretation_page(pdf)

    print(f"[spending_report] Done → {OUTPUT_PDF}")


if __name__ == "__main__":
    run()
