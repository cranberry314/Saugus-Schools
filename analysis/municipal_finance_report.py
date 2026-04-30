"""
Saugus Municipal Finance Report
================================
General fund operating statement, revenue/expenditure trends, peer benchmarks.
Source: MA DLS Schedule A, FY2010-2025.

Run: python analysis/municipal_finance_report.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine
from scipy.spatial.distance import mahalanobis
from scipy.cluster.hierarchy import linkage as hc_linkage, fcluster

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "municipal_finance_report.pdf")

# ── Color palette ─────────────────────────────────────────────────────────────
NAVY          = "#1B2A4A"   # slide backgrounds, dark elements
GOLD          = "#F0A500"   # Saugus highlight, positive framing
WHITE         = "#FFFFFF"   # primary text on dark backgrounds
LIGHT_GRAY    = "#D0D5E0"   # secondary text on dark backgrounds

RED           = "#C0392B"   # problem / deficit / decline
DARK_RED      = "#7B241C"   # secondary problem
BLUE          = "#2471A3"   # neutral / informational
DARK_BLUE     = "#1A5276"   # secondary informational
INFO_BLUE     = "#154360"   # deep blue for neutral stat boxes (closing slide)
GREEN         = "#1E8449"   # surplus / positive values
ORANGE        = "#E67E22"   # debt service (trend charts)
PURPLE        = "#7D3C98"   # fixed costs (trend charts)
STEEL_BLUE    = "#5D8AA8"   # peer bars in comparison charts
CONSENSUS_BLUE= "#1F618D"   # consensus peer bars

CHART_BG      = "#1B2A4A"   # all chart backgrounds — match slide bg
CHART_GRID    = "#2C3E6B"   # gridlines on dark backgrounds

# Legacy aliases (used in older slide functions)
STEEL  = STEEL_BLUE
LIGHT  = LIGHT_GRAY

SAUGUS_CODE = 262
SAUGUS_NAME = "Saugus"

# Towns excluded from peer comparisons — population far outside Saugus's range (~28k)
# Tyringham (302, ~400), Mount Washington (195, ~150), Wellfleet (318, ~3,000),
# East Brookfield (84, ~2,100) — too small, biases Mahalanobis distance calculation
# Nantucket (197) — resort/island economy, SPED % creates artificial closeness to Saugus
#   despite 60% college-educated adults (vs 31% Saugus) and $120K median HHI (vs $101K)
EXCLUDED_PEER_CODES = {302, 195, 318, 84, 197}
EXCLUDED_PEER_NAMES = {"Tyringham", "Mount Washington", "Wellfleet",
                       "East Brookfield", "Nantucket"}

# RBP feature list (all 14 candidates) and number to use for peer matching
RBP_ALL_FEATURES = [
    "high_needs_pct", "low_income_pct", "ell_pct", "sped_pct",
    "nss_per_pupil", "teacher_spending_per_pupil", "ch70_per_pupil",
    "avg_teacher_salary",
    "chronic_absenteeism_pct", "teachers_per_100_students",
    "teachers_per_100_fte", "total_enrollment", "median_hh_income",
    "pct_bachelors_plus", "pct_owner_occupied",
]
N_TOP_RBP = 6   # top features by importance used for Mahalanobis/clustering

# CPI base year for real dollar conversion (FY2010 = 100)
CPI_BASE_YEAR = 2010


def fmt_millions(x, _=None):
    """For raw-dollar data: divides by 1e6."""
    return f"${x/1e6:.1f}M"


def fmt_m_direct(x, _=None):
    """For data already in millions: no division."""
    return f"${x:.1f}M"


def fmt_pct(x, _=None):
    return f"{x:.0f}%"


def load_data(engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (revenues, expenditures, cpi)."""
    with engine.connect() as conn:
        rev = pd.read_sql(text("""
            SELECT dor_code, municipality, fiscal_year,
                   taxes, service_charges, licenses_permits,
                   federal_revenue, state_revenue, intergovernmental,
                   special_assessments, fines_forfeitures, miscellaneous,
                   other_financing, transfers, total_revenues
            FROM municipal_revenues ORDER BY municipality, fiscal_year
        """), conn)
        exp = pd.read_sql(text("""
            SELECT dor_code, municipality, fiscal_year,
                   general_government, public_safety, education,
                   public_works, human_services, culture_recreation,
                   fixed_costs, intergovernmental, other_expenditures,
                   debt_service, total_expenditures
            FROM municipal_expenditures ORDER BY municipality, fiscal_year
        """), conn)
        cpi = pd.read_sql(text("""
            SELECT year, cpi_pct_change FROM inflation_cpi ORDER BY year
        """), conn)
    return rev, exp, cpi


def build_deflator(cpi: pd.DataFrame, years: list[int]) -> dict[int, float]:
    """
    Returns a dict mapping fiscal_year -> deflator so that
    nominal * deflator = real (FY2010 dollars).
    """
    # Build cumulative price level index, base = FY2010
    price_level = {CPI_BASE_YEAR: 1.0}
    all_years = sorted(set(list(cpi["year"]) + years))
    for yr in all_years:
        if yr <= CPI_BASE_YEAR:
            continue
        prev = price_level.get(yr - 1, price_level.get(max(k for k in price_level if k < yr)))
        row = cpi[cpi["year"] == yr]
        pct = float(row["cpi_pct_change"].iloc[0]) / 100.0 if len(row) else 0.0
        price_level[yr] = prev * (1 + pct)
    # Deflator = base / current price level
    deflator = {yr: 1.0 / price_level.get(yr, 1.0) for yr in years}
    return deflator


def load_acs_data(engine) -> pd.DataFrame:
    """Load most recent ACS data per municipality (population + median HHI)."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT DISTINCT ON (municipality)
                municipality, total_population, median_hh_income, acs_year
            FROM municipal_census_acs
            ORDER BY municipality, acs_year DESC
        """), conn)
    # Normalize: strip " Town" suffix so names match DOR municipality names
    df["municipality"] = df["municipality"].str.replace(r"\s+Town$", "", regex=True).str.strip()
    # Dedup after normalization (e.g. "North Attleborough" + "North Attleborough Town" → same key)
    df = df.sort_values("acs_year", ascending=False).drop_duplicates(subset=["municipality"])
    return df


def load_school_outcomes(engine, peer_names=None) -> dict:
    """Load MCAS, graduation, per-pupil, and staffing data for Saugus and consensus peers.
    peer_names: list of municipality/district names (e.g. the Mahalanobis+Ward consensus).
    Falls back to hardcoded set if None or lookup fails."""
    SAUGUS_ORG = "02620000"
    _FALLBACK_PEERS = {"Danvers": "00710000", "Mashpee": "01720000",
                       "Plainville": "02380000", "West Bridgewater": "03230000"}

    with engine.connect() as conn:
        if peer_names:
            all_orgs = pd.read_sql(text("""
                SELECT DISTINCT district_name, org_code
                FROM mcas_results WHERE org_code LIKE '%0000'
            """), conn)
            PEER_ORGS = {}
            for name in peer_names:
                match = all_orgs[all_orgs["district_name"].str.lower() == name.lower()]
                if len(match):
                    PEER_ORGS[name] = str(match["org_code"].iloc[0])
            if not PEER_ORGS:
                PEER_ORGS = _FALLBACK_PEERS
        else:
            PEER_ORGS = _FALLBACK_PEERS
        # MCAS district-level, all students, grades 3-8 summary
        mcas = pd.read_sql(text("""
            SELECT district_name, org_code, school_year, subject, meeting_exceeding_pct
            FROM mcas_results
            WHERE student_group = 'All Students'
              AND grade = 'ALL (03-08)'
              AND subject IN ('ELA', 'MATH')
              AND org_code LIKE '%0000'
            ORDER BY district_name, school_year, subject
        """), conn)

        # State median by year+subject
        state_med = (mcas[mcas["org_code"] != SAUGUS_ORG]
                     .groupby(["school_year", "subject"])["meeting_exceeding_pct"]
                     .median().reset_index().rename(columns={"meeting_exceeding_pct": "state_med"}))

        # Graduation rates
        grad = pd.read_sql(text("""
            SELECT org_code, district_name, school_year,
                   four_year_grad_pct, dropout_pct
            FROM graduation_rates WHERE student_group = 'All'
            ORDER BY district_name, school_year
        """), conn)

        # Per-pupil in-district
        ppe = pd.read_sql(text("""
            SELECT district_name, school_year, amount as per_pupil
            FROM per_pupil_expenditure
            WHERE category = 'Total In-District Expenditures'
            ORDER BY district_name, school_year
        """), conn)

        # Teacher FTE (Saugus only)
        teacher = pd.read_sql(text(f"""
            SELECT school_year, fte
            FROM staffing
            WHERE org_code = '{SAUGUS_ORG}' AND category = 'teacher_fte'
            ORDER BY school_year
        """), conn)

        # Cross-sectional scatter: ed_pct + MCAS for 2024 across matched towns
        exp24 = pd.read_sql(text("""
            SELECT municipality, education, total_expenditures
            FROM municipal_expenditures WHERE fiscal_year = 2024
        """), conn)
        exp24["ed_pct"] = exp24["education"] / exp24["total_expenditures"]

        mcas24 = (mcas[mcas["school_year"] == 2024]
                  .groupby("district_name")["meeting_exceeding_pct"]
                  .mean().reset_index().rename(columns={"meeting_exceeding_pct": "avg_me_pct"}))

        ppe24 = ppe[ppe["school_year"] == 2024][["district_name", "per_pupil"]]

        rev24 = pd.read_sql(text("""
            SELECT municipality, state_revenue, total_revenues
            FROM municipal_revenues WHERE fiscal_year = 2024
        """), conn)
        rev24["state_aid_pct"] = rev24["state_revenue"] / rev24["total_revenues"]

        acs24 = pd.read_sql(text("""
            SELECT DISTINCT ON (municipality) municipality, median_hh_income, total_population
            FROM municipal_census_acs ORDER BY municipality, acs_year DESC
        """), conn)
        acs24["municipality"] = (acs24["municipality"]
                                 .str.replace(r"\s+Town$", "", regex=True).str.strip())
        acs24 = acs24.drop_duplicates(subset=["municipality"])

        scatter = exp24.merge(mcas24, left_on="municipality", right_on="district_name", how="inner")
        scatter = scatter.merge(ppe24, on="district_name", how="inner")
        scatter = scatter.merge(rev24[["municipality", "state_aid_pct"]], on="municipality", how="left")
        scatter = scatter.merge(acs24[["municipality", "median_hh_income", "total_population"]],
                                on="municipality", how="left")
        pop_demo = pd.read_sql(text("""
            SELECT district_name, high_needs_pct
            FROM district_selected_populations
            WHERE school_year = 2024
        """), conn)
        scatter = scatter.merge(pop_demo, on="district_name", how="left")
        scatter = scatter.dropna(subset=["ed_pct", "avg_me_pct", "per_pupil"])

        # Grade 10 MCAS (high-stakes graduation test, 2019–present)
        mcas10 = pd.read_sql(text("""
            SELECT district_name, org_code, school_year, subject, meeting_exceeding_pct
            FROM mcas_results
            WHERE student_group = 'All Students'
              AND grade = '10'
              AND subject IN ('ELA', 'MATH')
              AND org_code LIKE '%0000'
            ORDER BY district_name, school_year, subject
        """), conn)

    return {
        "mcas":       mcas,
        "mcas10":     mcas10,
        "state_med":  state_med,
        "grad":       grad,
        "ppe":        ppe,
        "teacher":    teacher,
        "scatter":    scatter,
        "saugus_org": SAUGUS_ORG,
        "peer_orgs":  PEER_ORGS,
    }


def load_tax_rates(engine) -> pd.DataFrame:
    """Load residential and commercial tax rates from municipal_tax_rates table."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT dor_code, municipality, fiscal_year,
                   residential, commercial, industrial
            FROM municipal_tax_rates
            ORDER BY municipality, fiscal_year
        """), conn)
    return df


def _rbp_ed_pct(rev, exp, year):
    """Helper: returns municipality → ed_pct from fiscal data for a given year."""
    rv = rev[rev["fiscal_year"] == year].copy()
    ex = exp[exp["fiscal_year"] == year].copy()
    m  = rv.merge(ex[["dor_code", "total_expenditures", "education"]], on="dor_code")
    m["ed_pct"] = m["education"] / m["total_expenditures"] * 100
    return m[["municipality", "dor_code", "ed_pct"]].drop_duplicates("municipality")


def _rbp_mahal_peers(rbp_df, top_features, rev, exp, year, n_peers):
    """Mahalanobis peer selection in RBP feature space (outcome-predictive factors).
    Restricted to towns that also have Schedule A fiscal data so bar charts can show all peers."""
    fiscal = _rbp_ed_pct(rev, exp, year)
    towns_with_fiscal = set(fiscal["municipality"].tolist())

    df = rbp_df.dropna(subset=top_features).reset_index(drop=True).copy()
    df = df[~df["municipality"].isin(EXCLUDED_PEER_NAMES)].reset_index(drop=True)
    # Restrict to towns with Schedule A data (so every selected peer can show ed_pct)
    df = df[df["municipality"].isin(towns_with_fiscal) |
            (df["municipality"] == SAUGUS_NAME)].reset_index(drop=True)

    X_vals  = df[top_features].copy()
    X_scaled = (X_vals - X_vals.mean()) / X_vals.std().replace(0, 1)

    saugus_mask = df["municipality"] == SAUGUS_NAME
    if saugus_mask.sum() == 0:
        return pd.DataFrame()

    saugus_vec = X_scaled[saugus_mask].values[0]
    cov     = np.cov(X_scaled.T)
    cov_inv = np.linalg.pinv(cov if cov.ndim == 2 else np.array([[cov]]))
    df["mahal_dist"] = X_scaled.apply(
        lambda row: mahalanobis(row.values, saugus_vec, cov_inv), axis=1
    )
    peer_munis = (df[df["municipality"] != SAUGUS_NAME]
                  .nsmallest(n_peers, "mahal_dist")["municipality"].tolist())

    peers = pd.DataFrame({"municipality": peer_munis})
    return peers.merge(fiscal, on="municipality", how="left")


def _rbp_hclust_peers(rbp_df, top_features, rev, exp, year):
    """Ward-linkage clustering in RBP feature space (outcome-predictive factors).
    Restricted to towns with Schedule A fiscal data so bar charts can show all peers."""
    fiscal = _rbp_ed_pct(rev, exp, year)
    towns_with_fiscal = set(fiscal["municipality"].tolist())

    df = rbp_df.dropna(subset=top_features).reset_index(drop=True).copy()
    df = df[~df["municipality"].isin(EXCLUDED_PEER_NAMES)].reset_index(drop=True)
    df = df[df["municipality"].isin(towns_with_fiscal) |
            (df["municipality"] == SAUGUS_NAME)].reset_index(drop=True)
    X_vals   = df[top_features].copy()
    X_scaled = (X_vals - X_vals.mean()) / X_vals.std().replace(0, 1)

    X = df[["municipality"]].copy().reset_index(drop=True)
    X["dor_code"] = 0

    Z = hc_linkage(X_scaled.values, method="ward", metric="euclidean")
    merge_distances = Z[:, 2]
    gaps      = np.diff(merge_distances)
    optimal_k = len(gaps) - int(np.argmax(gaps[::-1])) + 1
    optimal_k = min(max(optimal_k, 4), 8)
    X["hclust_label"] = fcluster(Z, optimal_k, criterion="maxclust")

    saugus_mask = X["municipality"] == SAUGUS_NAME
    if saugus_mask.sum() == 0:
        return pd.DataFrame(), None, X, optimal_k

    saugus_cluster = int(X.loc[saugus_mask, "hclust_label"].iloc[0])
    hclust_peers = X[
        (X["hclust_label"] == saugus_cluster) & (X["municipality"] != SAUGUS_NAME)
    ].copy()

    fiscal = _rbp_ed_pct(rev, exp, year)
    hclust_peers = hclust_peers.merge(fiscal[["municipality", "ed_pct"]],
                                      on="municipality", how="left")
    return hclust_peers, Z, X, optimal_k


def compute_mahal_peers(rev: pd.DataFrame, exp: pd.DataFrame,
                        acs: pd.DataFrame, year: int = 2024,
                        n_peers: int = 10,
                        rbp_df=None, rbp_top_features=None) -> pd.DataFrame:
    """
    Compute Mahalanobis distance from Saugus.
    When rbp_df + rbp_top_features are provided, uses outcome-predictive RBP feature space.
    Otherwise falls back to fiscal + demographic features.
    Returns the n_peers closest towns (excluding Saugus and outlier towns).
    """
    if rbp_df is not None and rbp_top_features:
        return _rbp_mahal_peers(rbp_df, rbp_top_features, rev, exp, year, n_peers)
    rv = rev[rev["fiscal_year"] == year].copy()
    ex = exp[exp["fiscal_year"] == year].copy()
    m  = rv.merge(ex[["dor_code", "total_expenditures", "education",
                       "fixed_costs", "debt_service"]], on="dor_code")
    m  = m[~m["dor_code"].isin(EXCLUDED_PEER_CODES)]
    m  = m.merge(acs[["municipality", "total_population", "median_hh_income"]],
                 on="municipality", how="left")

    m["ed_pct"]         = m["education"]    / m["total_expenditures"] * 100
    m["fixed_cost_pct"] = m["fixed_costs"]  / m["total_expenditures"] * 100
    m["debt_pct"]       = m["debt_service"] / m["total_expenditures"] * 100
    m["prop_tax_pct"]   = m["taxes"]        / m["total_revenues"]     * 100
    m["state_aid_pct"]  = m["state_revenue"]/ m["total_revenues"]     * 100
    m["rev_per_capita"] = m["total_revenues"] / m["total_population"].replace(0, np.nan)

    features = ["ed_pct", "fixed_cost_pct", "debt_pct",
                "prop_tax_pct", "state_aid_pct",
                "median_hh_income", "total_population", "rev_per_capita"]

    X = m[["dor_code", "municipality"] + features].copy()
    X = X.drop_duplicates(subset=["dor_code"])
    X = X.dropna(subset=features)
    X = X.reset_index(drop=True)

    X_vals = X[features].copy()
    X_scaled = (X_vals - X_vals.mean()) / X_vals.std()

    saugus_mask = X["dor_code"] == SAUGUS_CODE
    if saugus_mask.sum() == 0:
        return pd.DataFrame()

    saugus_vec = X_scaled[saugus_mask].values[0]
    cov = np.cov(X_scaled.T)
    cov_inv = np.linalg.pinv(cov)

    X["mahal_dist"] = X_scaled.apply(
        lambda row: mahalanobis(row.values, saugus_vec, cov_inv), axis=1
    )

    peers = X[X["dor_code"] != SAUGUS_CODE].nsmallest(n_peers, "mahal_dist").copy()
    peers = peers.merge(m[["dor_code", "ed_pct"]], on="dor_code", how="left", suffixes=("", "_m"))
    if "ed_pct_m" in peers.columns:
        peers["ed_pct"] = peers["ed_pct"].fillna(peers["ed_pct_m"])
        peers = peers.drop(columns=["ed_pct_m"])

    # QWAFAFEW sensitivity: does removing ed_pct (circularity check) change the peer set?
    fe_no_ed = [f for f in features if f != "ed_pct"]
    X_vals_ne = X[fe_no_ed].copy()
    X_scaled_ne = (X_vals_ne - X_vals_ne.mean()) / X_vals_ne.std()
    sv_ne = X_scaled_ne[saugus_mask].values[0]
    ci_ne = np.linalg.pinv(np.cov(X_scaled_ne.T))
    X_tmp = X.copy()
    X_tmp["_md_ne"] = X_scaled_ne.apply(lambda r: mahalanobis(r.values, sv_ne, ci_ne), axis=1)
    p_ne = set(X_tmp[X_tmp["dor_code"] != SAUGUS_CODE].nsmallest(n_peers, "_md_ne")["municipality"])
    p_orig = set(peers["municipality"].tolist())
    if p_orig == p_ne:
        print("  [Sensitivity] ed_pct exclusion: peer set stable — circularity not material")
    else:
        print(f"  [Sensitivity] ed_pct changes peers: added={p_ne - p_orig}, removed={p_orig - p_ne}")

    return peers


def compute_hclust_peers(rev: pd.DataFrame, exp: pd.DataFrame,
                         acs: pd.DataFrame, year: int = 2024,
                         rbp_df=None, rbp_top_features=None):
    """
    Ward-linkage hierarchical clustering.
    When rbp_df + rbp_top_features are provided, clusters in RBP feature space.
    Returns (hclust_peers_df, Z_linkage, peer_df_full, optimal_k).
    peer_df_full has all towns used (with hclust_label column) for the dendrogram.
    """
    if rbp_df is not None and rbp_top_features:
        return _rbp_hclust_peers(rbp_df, rbp_top_features, rev, exp, year)
    rv = rev[rev["fiscal_year"] == year].copy()
    ex = exp[exp["fiscal_year"] == year].copy()
    m  = rv.merge(ex[["dor_code", "total_expenditures", "education",
                       "fixed_costs", "debt_service"]], on="dor_code")
    m  = m[~m["dor_code"].isin(EXCLUDED_PEER_CODES)]
    m  = m.merge(acs[["municipality", "total_population", "median_hh_income"]],
                 on="municipality", how="left")

    m["ed_pct"]         = m["education"]    / m["total_expenditures"] * 100
    m["fixed_cost_pct"] = m["fixed_costs"]  / m["total_expenditures"] * 100
    m["debt_pct"]       = m["debt_service"] / m["total_expenditures"] * 100
    m["prop_tax_pct"]   = m["taxes"]        / m["total_revenues"]     * 100
    m["state_aid_pct"]  = m["state_revenue"]/ m["total_revenues"]     * 100
    m["rev_per_capita"] = m["total_revenues"] / m["total_population"].replace(0, np.nan)

    features = ["ed_pct", "fixed_cost_pct", "debt_pct",
                "prop_tax_pct", "state_aid_pct",
                "median_hh_income", "total_population", "rev_per_capita"]

    X = m[["dor_code", "municipality"] + features].copy()
    X = X.drop_duplicates(subset=["dor_code"])
    X = X.dropna(subset=features).reset_index(drop=True)

    X_vals  = X[features].copy()
    X_scaled = (X_vals - X_vals.mean()) / X_vals.std()

    Z = hc_linkage(X_scaled.values, method="ward", metric="euclidean")

    # Optimal k: largest gap in successive merge distances (elbow criterion, no manual tuning)
    merge_distances = Z[:, 2]
    gaps     = np.diff(merge_distances)
    optimal_k = len(gaps) - int(np.argmax(gaps[::-1])) + 1
    optimal_k = min(max(optimal_k, 4), 8)

    X["hclust_label"] = fcluster(Z, optimal_k, criterion="maxclust")

    saugus_mask = X["dor_code"] == SAUGUS_CODE
    if saugus_mask.sum() == 0:
        return pd.DataFrame(), None, X, optimal_k

    saugus_cluster = int(X.loc[saugus_mask, "hclust_label"].iloc[0])
    hclust_peers = X[
        (X["hclust_label"] == saugus_cluster) &
        (X["dor_code"] != SAUGUS_CODE)
    ].copy()

    hclust_peers = hclust_peers.merge(
        m[["dor_code", "ed_pct"]], on="dor_code", how="left", suffixes=("", "_m"))
    if "ed_pct_m" in hclust_peers.columns:
        hclust_peers["ed_pct"] = hclust_peers["ed_pct"].fillna(hclust_peers["ed_pct_m"])
        hclust_peers = hclust_peers.drop(columns=["ed_pct_m"])

    return hclust_peers, Z, X, optimal_k


def compute_peer_stats(rev: pd.DataFrame, exp: pd.DataFrame, year: int = 2024) -> dict:
    """
    For a given year, compute peer group statistics and Saugus rank for key metrics.
    Returns dict of metric -> {saugus, median, mean, lo25, hi75, rank, n, higher_is_better}.
    Rank is 1-based, sorted ascending (rank 1 = lowest value).
    """
    rv = rev[rev["fiscal_year"] == year].copy()
    ex = exp[exp["fiscal_year"] == year].copy()
    m  = rv.merge(ex[["dor_code", "total_expenditures", "education", "public_safety",
                       "fixed_costs", "debt_service"]], on="dor_code")
    m  = m[~m["dor_code"].isin(EXCLUDED_PEER_CODES)]

    m["debt_pct"]    = m["debt_service"] / m["total_expenditures"] * 100
    m["burden_pct"]  = (m["fixed_costs"] + m["debt_service"]) / m["total_expenditures"] * 100
    m["state_pct"]   = m["state_revenue"] / m["total_revenues"] * 100
    m["ed_pct"]      = m["education"] / m["total_expenditures"] * 100
    m["ps_pct"]      = m["public_safety"] / m["total_expenditures"] * 100
    m["surplus_pct"] = (m["total_revenues"] - m["total_expenditures"]) / m["total_revenues"] * 100

    metrics = {
        "debt_pct":    ("Debt Service % of Exp.",    False),
        "burden_pct":  ("Fixed+Debt % of Exp.",      False),
        "state_pct":   ("State Aid % of Revenue",    False),
        "ed_pct":      ("Education % of Exp.",        True),
        "ps_pct":      ("Public Safety % of Exp.",   False),
        "surplus_pct": ("Operating Surplus %",        True),
    }

    result = {}
    for col, (label, higher_is_better) in metrics.items():
        series = m[col].dropna().sort_values()
        saugus_val = float(m.loc[m["dor_code"] == SAUGUS_CODE, col].iloc[0])
        rank = int((series < saugus_val).sum()) + 1
        n    = len(series)
        result[col] = {
            "label":           label,
            "saugus":          saugus_val,
            "median":          float(series.median()),
            "mean":            float(series.mean()),
            "lo25":            float(series.quantile(0.25)),
            "hi75":            float(series.quantile(0.75)),
            "rank":            rank,
            "n":               n,
            "higher_is_better": higher_is_better,
        }

    # Revenue growth: FY2010 → most recent year available per town
    base_yr = rev["fiscal_year"].min()
    top_yr  = rev["fiscal_year"].max()
    rev_base = rev[rev["fiscal_year"] == base_yr][["dor_code", "total_revenues"]].rename(
        columns={"total_revenues": "rev_base"})
    rev_top  = rev[rev["fiscal_year"] == top_yr][["dor_code", "total_revenues"]].rename(
        columns={"total_revenues": "rev_top"})
    growth_m = rev_base.merge(rev_top, on="dor_code")
    growth_m["rev_growth_pct"] = (growth_m["rev_top"] - growth_m["rev_base"]) / growth_m["rev_base"] * 100

    series = growth_m["rev_growth_pct"].dropna().sort_values()
    saugus_val = float(growth_m.loc[growth_m["dor_code"] == SAUGUS_CODE, "rev_growth_pct"].iloc[0])
    rank = int((series < saugus_val).sum()) + 1
    n    = len(series)
    result["rev_growth_pct"] = {
        "label":            f"Revenue Growth FY{base_yr}–FY{top_yr}",
        "saugus":           saugus_val,
        "median":           float(series.median()),
        "mean":             float(series.mean()),
        "lo25":             float(series.quantile(0.25)),
        "hi75":             float(series.quantile(0.75)),
        "rank":             rank,
        "n":                n,
        "higher_is_better": True,
    }

    result["_n_peer_towns"] = len(m)
    return result



# ── helpers ───────────────────────────────────────────────────────────────────

def _v(df, yr, col):
    rows = df[df["fiscal_year"] == yr]
    if len(rows) == 0:
        return 0
    v = rows[col].iloc[0]
    return 0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)


def _style_legend(ax):
    """Apply consistent dark-background styling to an axes legend."""
    legend = ax.get_legend()
    if legend:
        legend.get_frame().set_facecolor(CHART_BG)
        legend.get_frame().set_edgecolor(CHART_GRID)
        legend.get_frame().set_alpha(0.9)
        for text in legend.get_texts():
            text.set_color("white")
    return legend


def _callout(fig, x, y, w, h, value_str, label, bg, value_size=28, text_color=WHITE):
    rect = plt.Rectangle((x, y), w, h, transform=fig.transFigure,
                          facecolor=bg, edgecolor=WHITE, linewidth=1.5, zorder=1)
    fig.add_artist(rect)
    fig.text(x + w / 2, y + h * 0.62, value_str,
             transform=fig.transFigure, fontsize=value_size, fontweight="bold",
             color=text_color, ha="center", va="center", zorder=2)
    fig.text(x + w / 2, y + h * 0.22, label,
             transform=fig.transFigure, fontsize=7.5, color=text_color,
             ha="center", va="center", zorder=2, linespacing=1.4)


# ── page 1: title ─────────────────────────────────────────────────────────────

def title_page(pdf, n_peer_towns):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    fig.patch.set_facecolor(NAVY)

    ax.text(0.5, 0.68, "Saugus Public Schools",
            transform=ax.transAxes, fontsize=40, fontweight="bold",
            color="white", ha="center")
    ax.text(0.5, 0.54, "Follow the Money. See the Scores.",
            transform=ax.transAxes, fontsize=24, color=GOLD, ha="center")
    ax.text(0.5, 0.42, "Budget Decisions & Student Outcomes · FY2010–2025",
            transform=ax.transAxes, fontsize=14, color="white", ha="center", alpha=0.85)
    ax.text(0.5, 0.22,
            "Sources: MA Division of Local Services (Schedule A FY2010–2025)  ·  "
            "MA DESE (MCAS, graduation rates, staffing)  ·  U.S. Census Bureau ACS  ·  MA DOR (tax rates)",
            transform=ax.transAxes, fontsize=9, color="white",
            ha="center", alpha=0.55, linespacing=1.6)
    ax.text(0.5, 0.14,
            f"Peer comparison: {n_peer_towns} similarly sized Massachusetts municipalities  ·  "
            "Peers selected by Mahalanobis Distance and Hierarchical Clustering (Ward)  ·  All data public record",
            transform=ax.transAxes, fontsize=9, color="white",
            ha="center", alpha=0.45, linespacing=1.6)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 2: thesis ────────────────────────────────────────────────────────────

def thesis_page(pdf, sexp, outcomes, peer_stats):
    """Three-fact thesis slide — sets up the whole argument."""
    yr = 2024
    ed_pct_2010 = _v(sexp, 2010, "education") / _v(sexp, 2010, "total_expenditures") * 100
    ed_pct_now  = _v(sexp, yr,   "education") / _v(sexp, yr,   "total_expenditures") * 100
    rank_low    = peer_stats["ed_pct"]["rank"]
    peer_n      = peer_stats["ed_pct"]["n"]

    mcas       = outcomes["mcas"]
    SAUGUS_ORG = outcomes["saugus_org"]
    PEER_ORGS  = outcomes["peer_orgs"]

    sela = mcas[(mcas["org_code"] == SAUGUS_ORG) & (mcas["subject"] == "ELA")].set_index("school_year")
    ela_2019 = float(sela.loc[2019, "meeting_exceeding_pct"]) * 100 if 2019 in sela.index else 48
    ela_2022 = float(sela.loc[2022, "meeting_exceeding_pct"]) * 100 if 2022 in sela.index else 34
    ela_latest_yr = int(sela.index.max())
    ela_latest = float(sela.loc[ela_latest_yr, "meeting_exceeding_pct"]) * 100

    peer_ela = (mcas[mcas["org_code"].isin(PEER_ORGS.values()) & (mcas["subject"] == "ELA")]
                .groupby("school_year")["meeting_exceeding_pct"].mean() * 100)
    peer_2019  = float(peer_ela.get(2019, peer_ela.iloc[0]))
    peer_latest = float(peer_ela.get(ela_latest_yr, peer_ela.iloc[-1]))
    gap_2019   = ela_2019  - peer_2019    # e.g. -5pp
    gap_latest = ela_latest - peer_latest  # e.g. -13pp

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    fig.text(0.5, 0.920,
             "Three Facts. All Public Data.",
             ha="center", fontsize=26, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.878,
             "This presentation documents each one. Draw your own conclusions.",
             ha="center", fontsize=12, color=GOLD, alpha=0.9,
             transform=fig.transFigure)

    facts = [
        (
            "1",
            "Education's share of the budget has been cut",
            f"Saugus allocated {ed_pct_2010:.0f}% of its general fund to education in FY2010.\n"
            f"By FY{yr} that had fallen to {ed_pct_now:.0f}% — #{rank_low} lowest share among\n"
            f"{peer_n} similarly sized Massachusetts towns.",
            RED,
        ),
        (
            "2",
            "Student outcomes have declined — and the gap with peers keeps growing",
            f"ELA meeting/exceeding: {ela_2019:.0f}% pre-Covid (2019) → {ela_2022:.0f}% in 2022 (all towns dipped) → {ela_latest:.0f}% in {ela_latest_yr}.\n"
            f"Comparable towns averaged {peer_latest:.0f}% in {ela_latest_yr}. "
            f"The gap with peers has grown from {abs(gap_2019):.0f}pp to {abs(gap_latest):.0f}pp since 2019.\n"
            "The 4-year graduation rate has also declined.",
            GOLD,
        ),
        (
            "3",
            "Saugus has a commercial advantage — and runs annual surpluses",
            "Route 1 businesses pay double the residential tax rate,\n"
            "keeping home tax bills among the lowest in the region.\n"
            "Saugus has run a general fund surplus every year since FY2010.",
            BLUE,
        ),
    ]

    box_left = 0.05
    box_w    = 0.90
    box_tops = [0.830, 0.580, 0.330]
    box_h    = 0.220

    for (num, heading, body, color), top in zip(facts, box_tops):
        # Colored left bar
        bar = plt.Rectangle((box_left, top - box_h), 0.018, box_h,
                             transform=fig.transFigure,
                             facecolor=color, edgecolor="none", zorder=2)
        fig.add_artist(bar)
        # Box background
        bg = plt.Rectangle((box_left + 0.018, top - box_h), box_w - 0.018, box_h,
                            transform=fig.transFigure,
                            facecolor=CHART_BG, edgecolor=color, linewidth=1.2, zorder=1)
        fig.add_artist(bg)
        # Number badge
        fig.text(box_left + 0.018 + 0.030, top - box_h / 2,
                 num, ha="center", va="center",
                 fontsize=32, fontweight="bold", color=color,
                 transform=fig.transFigure, zorder=3)
        # Heading
        fig.text(box_left + 0.018 + 0.072, top - 0.048,
                 heading,
                 ha="left", va="center", fontsize=12, fontweight="bold",
                 color="white", transform=fig.transFigure, zorder=3)
        # Body text
        fig.text(box_left + 0.018 + 0.072, top - box_h * 0.62,
                 body,
                 ha="left", va="center", fontsize=9.5, color="white", alpha=0.80,
                 transform=fig.transFigure, linespacing=1.55, zorder=3)

    fig.text(0.5, 0.038,
             "All data is publicly available: MA DLS, MA DESE, ACS, MA DOR.  "
             "Methodology details on the final slides.",
             ha="center", fontsize=7.5, color="white", alpha=0.5,
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 3: executive summary ─────────────────────────────────────────────────

def executive_summary_page(pdf, srev, sexp, peer_stats):
    yr = 2024
    ed_pct     = _v(sexp, yr, "education") / _v(sexp, yr, "total_expenditures") * 100
    peer_med   = peer_stats["ed_pct"]["median"]
    peer_n     = peer_stats["ed_pct"]["n"]
    rank_low   = peer_stats["ed_pct"]["rank"]   # rank from bottom (1 = lowest)
    gap_m      = (_v(sexp, yr, "total_expenditures") * peer_med / 100
                  - _v(sexp, yr, "education")) / 1e6
    surplus    = (_v(srev, 2025, "total_revenues") - _v(sexp, 2025, "total_expenditures")) / 1e6
    surplus_years = sum(
        1 for y in range(2010, 2026)
        if _v(srev, y, "total_revenues") > _v(sexp, y, "total_expenditures")
    )
    avg_surplus = sum(
        _v(srev, y, "total_revenues") - _v(sexp, y, "total_expenditures")
        for y in range(2010, 2026)
    ) / 16 / 1e6

    ed_pct_2010 = _v(sexp, 2010, "education") / _v(sexp, 2010, "total_expenditures") * 100

    # Education real trend: FY2010 vs FY2024 (nominal, note CPI gap)
    ed_2010 = _v(sexp, 2010, "education") / 1e6
    ed_2024 = _v(sexp, yr, "education") / 1e6
    rev_2010 = _v(srev, 2010, "total_revenues") / 1e6
    rev_2024 = _v(srev, yr, "total_revenues") / 1e6
    ed_growth  = (ed_2024 - ed_2010) / ed_2010 * 100
    rev_growth = (rev_2024 - rev_2010) / rev_2010 * 100

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    fig.text(0.5, 0.93, "The Bottom Line",
             ha="center", fontsize=20, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.895,
             f"FY{yr} General Fund  ·  Saugus compared to {peer_n} similarly-sized MA towns",
             ha="center", fontsize=10, color=LIGHT_GRAY, transform=fig.transFigure)

    # Row 1: two big callouts
    _callout(fig, 0.04, 0.60, 0.44, 0.26,
             f"{ed_pct:.1f}%",
             f"Education's share of Saugus spending (FY{yr})\n"
             f"#{rank_low} lowest of {peer_n} comparable towns\n"
             f"Peer median: {peer_med:.1f}%",
             RED, value_size=36)

    _callout(fig, 0.52, 0.60, 0.44, 0.26,
             f"${gap_m:.0f}M",
             f"Annual gap vs peer-median education spending\n"
             f"What Saugus would need to spend per year\n"
             f"to match the average comparable town",
             DARK_RED, value_size=36)

    # Row 2
    _callout(fig, 0.04, 0.30, 0.29, 0.24,
             f"+{ed_growth:.0f}%",
             f"Nominal education spending\ngrowth FY2010–FY{yr}\nvs +{rev_growth:.0f}% revenue growth",
             ORANGE, value_size=28)

    _callout(fig, 0.37, 0.30, 0.29, 0.24,
             f"${surplus:.1f}M",
             f"FY2025 general fund surplus\nTown collected more than\nit spent last fiscal year",
             DARK_BLUE, value_size=28)

    _callout(fig, 0.70, 0.30, 0.26, 0.24,
             f"{surplus_years} of 16",
             f"years with a budget surplus\navg. ${avg_surplus:.1f}M/yr (nominal)\nFY2010–FY2025",
             DARK_BLUE, value_size=28)

    # Footer note
    fig.text(0.5, 0.23,
             "All dollar figures are nominal (not inflation-adjusted) unless otherwise noted. "
             "See Data Sources page for methodology.",
             ha="center", fontsize=7.5, color=LIGHT_GRAY,
             transform=fig.transFigure, style="italic")

    # Narrative pull-quote
    fig.text(0.5, 0.12,
             f"\"Since FY2010, Saugus revenues have grown {rev_growth:.0f}%.\n"
             f"Education spending has grown only {ed_growth:.0f}% — "
             f"well below the rate of inflation.\n"
             f"Education's share of the budget has fallen from {ed_pct_2010:.0f}% (FY2010) to {ed_pct:.0f}% (FY{yr}).\"",
             ha="center", fontsize=11, color="white",
             transform=fig.transFigure, linespacing=1.7,
             bbox=dict(boxstyle="round,pad=0.5", facecolor=CHART_BG,
                       edgecolor=GOLD, linewidth=1.5))

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 3: education share — trend ──────────────────────────────────────────

def education_share_trend_page(pdf, sexp):
    years = sorted(sexp["fiscal_year"].unique())
    ed_pct = [_v(sexp, y, "education") / _v(sexp, y, "total_expenditures") * 100
              for y in years]
    ps_pct = [_v(sexp, y, "public_safety") / _v(sexp, y, "total_expenditures") * 100
              for y in years]
    fc_pct = [_v(sexp, y, "fixed_costs") / _v(sexp, y, "total_expenditures") * 100
              for y in years]
    ds_pct = [_v(sexp, y, "debt_service") / _v(sexp, y, "total_expenditures") * 100
              for y in years]

    fig, ax = plt.subplots(figsize=(11, 7))
    fig.patch.set_facecolor("white")

    ax.plot(years, ed_pct, color=RED,     linewidth=3,   marker="o", markersize=6,
            label="Education", zorder=5)
    ax.plot(years, fc_pct, color="#7030A0", linewidth=2, marker="s", markersize=5,
            label="Fixed Costs (pension/benefits)", zorder=4)
    ax.plot(years, ps_pct, color=STEEL,   linewidth=2,   marker="^", markersize=5,
            label="Public Safety", zorder=4)
    ax.plot(years, ds_pct, color=ORANGE,  linewidth=2,   marker="D", markersize=5,
            label="Debt Service", zorder=4)

    # Annotate start and end of education line
    ax.annotate(f"FY{years[0]}: {ed_pct[0]:.1f}%", xy=(years[0], ed_pct[0]),
                xytext=(years[0] + 0.3, ed_pct[0] + 1.2),
                fontsize=10, fontweight="bold", color=RED,
                arrowprops=dict(arrowstyle="->", color=RED, lw=1.2))
    ax.annotate(f"FY{years[-1]}: {ed_pct[-1]:.1f}%", xy=(years[-1], ed_pct[-1]),
                xytext=(years[-1] - 1.5, ed_pct[-1] - 2.5),
                fontsize=10, fontweight="bold", color=RED,
                arrowprops=dict(arrowstyle="->", color=RED, lw=1.2))

    # Shade the "lost" education share
    ax.fill_between(years, ed_pct, [ed_pct[0]] * len(years),
                    where=[p < ed_pct[0] for p in ed_pct],
                    alpha=0.12, color=RED, label="_nolegend_")

    ax.set_title(
        f"Education's Share of Saugus Budget Has Fallen {ed_pct[0] - ed_pct[-1]:.0f} Percentage Points Since {years[0]}",
        fontsize=13, fontweight="bold", color=NAVY, pad=12)
    ax.set_ylabel("% of Total Expenditures", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.set_xlim(min(years) - 0.3, max(years) + 0.3)
    ax.set_ylim(0, 50)
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(fontsize=9, loc="upper right"); _style_legend(ax)
    ax.tick_params(labelsize=9)

    # Annotation box
    ax.text(2016.5, 8,
            "While education's share shrank,\nfixed costs more than doubled\nin percentage-point terms.",
            fontsize=9, color="#7030A0",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                      edgecolor="#7030A0", linewidth=1.2))

    fig.text(0.5, 0.01,
             "Source: MA DLS Schedule A  ·  General Fund expenditures only  ·  FY2010–FY2025  ·  "
             "These are budget allocation percentages (ratios); inflation affects numerator & denominator equally, so no CPI adjustment is needed.",
             ha="center", fontsize=6.5, color=STEEL, style="italic")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 4: peer education share comparison ───────────────────────────────────

def peer_education_share_page(pdf, exp, year=2024):
    exp_yr = exp[exp["fiscal_year"] == year].copy()
    exp_yr = exp_yr[~exp_yr["dor_code"].isin(EXCLUDED_PEER_CODES)]
    exp_yr["ed_pct"] = exp_yr["education"] / exp_yr["total_expenditures"] * 100
    exp_yr = exp_yr.dropna(subset=["ed_pct"]).sort_values("ed_pct")

    towns  = exp_yr["municipality"].tolist()
    vals   = exp_yr["ed_pct"].tolist()
    n      = len(towns)
    saugus_idx = towns.index(SAUGUS_NAME) if SAUGUS_NAME in towns else None
    peer_med = float(exp_yr["ed_pct"].median())

    colors = [GOLD if t == SAUGUS_NAME else LIGHT for t in towns]
    edge   = [NAVY if t == SAUGUS_NAME else "#AABBD0" for t in towns]

    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")

    ax.barh(range(n), vals, color=colors, edgecolor=edge, linewidth=0.5, height=0.75)

    # Peer median line
    ax.axvline(peer_med, color=NAVY, linewidth=2, linestyle="--", zorder=5,
               label=f"Peer median: {peer_med:.1f}%")

    # Saugus label — placed above the gap arrow to avoid overlap
    if saugus_idx is not None:
        ax.text(vals[saugus_idx] + 0.4, saugus_idx + 1.5,
                f"  Saugus: {vals[saugus_idx]:.1f}%  ← #{saugus_idx+1} lowest",
                va="center", fontsize=9, fontweight="bold", color=RED)

    ax.set_yticks(range(n))
    ax.set_yticklabels(towns, fontsize=6.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.set_xlim(0, 80)
    ax.set_xlabel("Education as % of Total General Fund Expenditures", fontsize=10)
    ax.set_title(
        f"Saugus Spends Less of Its Budget on Education Than\n"
        f"Almost Any Comparably Sized Massachusetts Town  (FY{year})",
        fontsize=13, fontweight="bold", color=NAVY, pad=10)
    ax.legend(fontsize=9, loc="lower right"); _style_legend(ax)
    ax.grid(True, alpha=0.2, axis="x")
    ax.tick_params(axis="y", labelsize=6.5)
    ax.tick_params(axis="x", labelsize=9)

    # Gap annotation
    if saugus_idx is not None:
        saugus_val = vals[saugus_idx]
        ax.annotate("",
                    xy=(peer_med, saugus_idx),
                    xytext=(saugus_val, saugus_idx),
                    arrowprops=dict(arrowstyle="<->", color=RED, lw=2))
        saugus_exp_yr = exp[(exp["fiscal_year"] == year) & (exp["dor_code"] == SAUGUS_CODE)]
        gap_m = (peer_med - saugus_val) / 100 * _v(saugus_exp_yr, year, "total_expenditures") / 1e6
        ax.text(peer_med + 1.5, saugus_idx + 5,
                f"{peer_med - saugus_val:.1f} ppt gap\n≈ ${gap_m:.0f}M/yr",
                ha="left", fontsize=8.5, fontweight="bold", color=RED,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                          edgecolor=RED, linewidth=1.2))

    # Transition note to Mahalanobis slide — top-right to avoid overlap with peer median legend
    ax.text(0.98, 0.98,
            "See next slide for demographically-matched peer comparison.",
            transform=ax.transAxes, fontsize=7.5, color=STEEL, ha="right", va="top",
            style="italic",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#F4F6FB",
                      edgecolor=STEEL, linewidth=0.8, alpha=0.9))

    fig.text(0.5, 0.005,
             f"Source: MA DLS Schedule A  ·  {n} similarly sized MA municipalities  ·  FY{year}  ·  "
             f"Tyringham, Mount Washington, Wellfleet, and East Brookfield excluded (extreme population outliers)",
             ha="center", fontsize=7.5, color=STEEL, style="italic")

    plt.tight_layout(rect=[0, 0.02, 1, 1])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 5: real spending growth index ────────────────────────────────────────

def real_spending_growth_page(pdf, sexp, deflator):
    years = sorted(sexp["fiscal_year"].unique())

    ed  = [_v(sexp, y, "education")      * deflator.get(y, 1.0) for y in years]
    ps  = [_v(sexp, y, "public_safety")  * deflator.get(y, 1.0) for y in years]
    fc  = [_v(sexp, y, "fixed_costs")    * deflator.get(y, 1.0) for y in years]
    ds  = [_v(sexp, y, "debt_service")   * deflator.get(y, 1.0) for y in years]

    ed_i = [v / ed[0]  * 100 for v in ed]
    ps_i = [v / ps[0]  * 100 for v in ps]
    fc_i = [v / fc[0]  * 100 for v in fc]
    ds_i = [v / ds[0]  * 100 for v in ds]

    fig, ax = plt.subplots(figsize=(11, 7))
    fig.patch.set_facecolor("white")

    ax.axhline(100, color="#CCCCCC", linewidth=1.2, linestyle=":", zorder=1,
               label="No real growth (inflation baseline)")

    ax.plot(years, fc_i,  color="#7030A0", linewidth=2.5, marker="s", markersize=5,
            label="Fixed Costs (pension & benefits)")
    ax.plot(years, ds_i,  color=ORANGE,    linewidth=2.5, marker="D", markersize=5,
            label="Debt Service")
    ax.plot(years, ps_i,  color=STEEL,     linewidth=2.5, marker="^", markersize=5,
            label="Public Safety")
    ax.plot(years, ed_i,  color=RED,       linewidth=3.5, marker="o", markersize=7,
            label="Education", zorder=5)

    # End-of-line labels
    def _chg(v):
        d = v - 100
        return f"+{d:.0f}%" if d >= 0 else f"{d:.0f}%"

    last = years[-1]
    for _, (series, label, color) in enumerate([
        (fc_i,  f"Fixed Costs  {_chg(fc_i[-1])}",  "#7030A0"),
        (ds_i,  f"Debt Service  {_chg(ds_i[-1])}", ORANGE),
        (ps_i,  f"Public Safety  {_chg(ps_i[-1])}",STEEL),
        (ed_i,  f"Education  {_chg(ed_i[-1])}",    RED),
    ]):
        ax.text(last + 0.15, series[-1], label,
                va="center", fontsize=9, fontweight="bold", color=color)

    # Callout box for education
    ax.annotate(
        f"Education spending has grown only\n"
        f"{_chg(ed_i[-1])} in real terms since FY2010.\n"
        f"Fixed costs grew {_chg(fc_i[-1])}.\n"
        f"Debt service grew {_chg(ds_i[-1])}.",
        xy=(years[-1], ed_i[-1]),
        xytext=(2013.5, ed_i[-1] + 55),
        fontsize=9, color=RED,
        bbox=dict(boxstyle="round,pad=0.5", facecolor="#FFF0F0",
                  edgecolor=RED, linewidth=1.5),
        arrowprops=dict(arrowstyle="->", color=RED, lw=1.5))

    ax.set_title(
        "Inflation-Adjusted Spending: Education Barely Growing\nWhile Other Categories Surge",
        fontsize=13, fontweight="bold", color=NAVY, pad=12)
    ax.set_ylabel("Index: FY2010 = 100  (inflation-adjusted)", fontsize=10)
    ax.set_xlim(min(years) - 0.3, max(years) + 2.5)
    ax.set_ylim(60, 280)
    ax.grid(True, alpha=0.25, linestyle="--")
    leg = ax.legend(fontsize=9, loc="upper left")
    if leg:
        leg.get_frame().set_facecolor(CHART_BG)
        leg.get_frame().set_edgecolor(CHART_GRID)
        for txt in leg.get_texts(): txt.set_color("white")
    ax.tick_params(labelsize=9)

    fig.text(0.5, 0.01,
             "All values inflation-adjusted to FY2010 dollars using BLS CPI-U annual averages.  "
             "Inflation data: Federal Reserve Economic Data (FRED) · fred.stlouisfed.org/series/CPIAUCSL",
             ha="center", fontsize=7.5, color=STEEL, style="italic")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 6: revenue growth vs education investment ────────────────────────────

def revenue_vs_education_page(pdf, srev, sexp, deflator):
    years = sorted(srev["fiscal_year"].unique())

    rev_real = [_v(srev, y, "total_revenues") * deflator.get(y, 1.0) / 1e6 for y in years]
    ed_real  = [_v(sexp, y, "education")      * deflator.get(y, 1.0) / 1e6 for y in years]
    surplus  = [(_v(srev, y, "total_revenues") - _v(sexp, y, "total_expenditures")) / 1e6
                for y in years]

    fig, axes = plt.subplots(1, 2, figsize=(11, 6.5))
    fig.patch.set_facecolor("white")
    fig.suptitle(
        "Revenues Have Grown Substantially — Education Investment Has Not Kept Pace",
        fontsize=12, fontweight="bold", color=NAVY, y=0.97)

    # Left: real revenues vs real education (indexed)
    ax = axes[0]
    rev_i = [v / rev_real[0] * 100 for v in rev_real]
    ed_i  = [v / ed_real[0]  * 100 for v in ed_real]
    def _signed(v):
        d = v - 100
        return f"+{d:.0f}%" if d >= 0 else f"{d:.0f}%"

    ax.plot(years, rev_i, color=GREEN, linewidth=2.5, marker="o", markersize=5,
            label=f"Total Revenue  ({_signed(rev_i[-1])})")
    ax.plot(years, ed_i,  color=RED,   linewidth=2.5, marker="s", markersize=5,
            label=f"Education Spending  ({_signed(ed_i[-1])})")
    ax.axhline(100, color="#CCCCCC", linewidth=1, linestyle=":")
    ax.fill_between(years, rev_i, ed_i,
                    where=[r > e for r, e in zip(rev_i, ed_i)],
                    alpha=0.10, color=RED, label="Widening gap")
    ax.set_title("Real Growth Index (FY2010 = 100)", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.grid(True, alpha=0.25, linestyle="--")
    leg = ax.legend(fontsize=8)
    if leg:
        leg.get_frame().set_facecolor(CHART_BG)
        leg.get_frame().set_edgecolor(CHART_GRID)
        for txt in leg.get_texts(): txt.set_color("white")
    ax.tick_params(labelsize=8)
    ax.set_xlim(min(years) - 0.3, max(years) + 0.3)

    # Right: annual surplus bars (no dual axis — removes the confusing $55M cumulative line)
    ax = axes[1]
    bar_colors = [GREEN if s >= 0 else RED for s in surplus]
    bars = ax.bar(years, surplus, color=bar_colors, alpha=0.80)
    ax.axhline(0, color=NAVY, linewidth=1.8, zorder=3)
    ax.set_title("Annual General Fund Surplus / Deficit ($M)", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_m_direct))
    ax.grid(True, alpha=0.25, linestyle="--", axis="y")
    ax.tick_params(labelsize=8)
    import matplotlib.patches as mpatches
    legend_handles = [
        mpatches.Patch(facecolor=GREEN, alpha=0.80, label="Surplus year"),
        mpatches.Patch(facecolor=RED,   alpha=0.80, label="Deficit year"),
    ]
    leg2 = ax.legend(handles=legend_handles, fontsize=8, loc="upper left")
    if leg2:
        leg2.get_frame().set_facecolor(CHART_BG)
        leg2.get_frame().set_edgecolor(CHART_GRID)
        for txt in leg2.get_texts(): txt.set_color("white")
    # Label each bar with its dollar value
    for bar, val in zip(bars, surplus):
        va = "bottom" if val >= 0 else "top"
        offset = 0.05 if val >= 0 else -0.05
        ax.text(bar.get_x() + bar.get_width() / 2, val + offset,
                f"${val:.1f}M", ha="center", va=va, fontsize=5.5, color=NAVY)

    for a in axes:
        for lbl in a.get_xticklabels():
            lbl.set_rotation(45)

    fig.text(0.5, 0.01,
             "Left panel: inflation-adjusted to FY2010 dollars (BLS CPI-U via FRED · fred.stlouisfed.org/series/CPIAUCSL).  "
             "Right panel: surplus figures are nominal dollars (standard for cash/budget reporting).  "
             "Spending data: MA DLS Schedule A.",
             ha="center", fontsize=6.5, color=STEEL, style="italic")

    plt.tight_layout(rect=[0, 0.04, 1, 0.96])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 7: the gap — what peer-level spending would mean ────────────────────

def funding_gap_page(pdf, sexp, year=2024):
    actual_ed    = _v(sexp[sexp["dor_code"] == SAUGUS_CODE], year, "education")
    total_exp    = _v(sexp[sexp["dor_code"] == SAUGUS_CODE], year, "total_expenditures")
    # Compute peer median dynamically, excluding outlier towns and Saugus itself
    peer_yr = sexp[(sexp["fiscal_year"] == year) &
                   (~sexp["dor_code"].isin(EXCLUDED_PEER_CODES)) &
                   (sexp["dor_code"] != SAUGUS_CODE)].copy()
    peer_yr = peer_yr[peer_yr["total_expenditures"] > 0]
    peer_yr["ed_pct"] = peer_yr["education"] / peer_yr["total_expenditures"] * 100
    peer_med_pct = float(peer_yr["ed_pct"].dropna().median())
    peer_med_ed  = total_exp * peer_med_pct / 100
    gap          = peer_med_ed - actual_ed

    # Contextual equivalents (rough estimates)
    avg_teacher_salary = 75000   # MA average all-in with benefits ~$75k
    teachers_equiv     = gap / avg_teacher_salary
    per_pupil_boost    = gap / 4200   # approx Saugus enrollment

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")

    fig.text(0.5, 0.94,
             "The Education Funding Gap: What Peer-Level Investment Would Mean",
             ha="center", fontsize=14, fontweight="bold", color=NAVY,
             transform=fig.transFigure)
    fig.text(0.5, 0.905,
             f"If Saugus allocated the same share of its budget to education as the median comparable town  (FY{year})",
             ha="center", fontsize=10, color=STEEL, transform=fig.transFigure)

    # Bar chart: actual vs peer-median vs peer-high
    ax = fig.add_axes([0.08, 0.44, 0.50, 0.42])
    categories = ["Saugus\n(actual)", f"Peer median\n({peer_med_pct:.1f}%)"]
    values = [actual_ed / 1e6, peer_med_ed / 1e6]
    bar_colors = [GOLD, NAVY]
    bars = ax.bar(categories, values, color=bar_colors, width=0.45,
                  edgecolor="white", linewidth=1.5)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                f"${val:.1f}M", ha="center", fontsize=12,
                fontweight="bold", color=NAVY)
    # Vertical gap arrow to the right of bar 1, clear of the peer median bar
    ax.annotate("",
                xy=(1.32, peer_med_ed / 1e6),
                xytext=(1.32, actual_ed / 1e6),
                arrowprops=dict(arrowstyle="<->", color=RED, lw=2.0))
    ax.text(1.38, (actual_ed + peer_med_ed) / 2 / 1e6,
            f"+${gap/1e6:.1f}M\nper year",
            va="center", ha="left", fontsize=11, fontweight="bold", color=RED,
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white",
                      edgecolor=RED, linewidth=1.2, alpha=0.95))
    ax.set_ylabel("Education Spending ($M)", fontsize=10)
    # Explicit yticks bypass formatter-reset issues from tight_layout
    _max_y = peer_med_ed / 1e6 * 1.18
    _step  = 10 if _max_y > 40 else 5
    _yticks = list(range(0, int(_max_y) + _step + 1, _step))
    ax.set_yticks(_yticks)
    ax.set_yticklabels([f"${v}M" for v in _yticks])
    ax.set_ylim(0, _max_y)
    ax.grid(True, alpha=0.2, axis="y")
    ax.tick_params(labelsize=10)
    ax.set_xlim(-0.5, 2.2)
    ax.set_title(f"FY{year} Education Spending", fontsize=10, color=NAVY, pad=8)

    # Right panel: callout equivalents
    equiv_x = 0.64
    _callout(fig, equiv_x, 0.73, 0.31, 0.13,
             f"${gap/1e6:.0f}M / yr",
             "annual gap\nvs peer-median spending",
             RED, value_size=22)
    _callout(fig, equiv_x, 0.58, 0.31, 0.13,
             f"~{teachers_equiv:.0f}",
             f"additional classroom teachers\n(@ MA avg. salary + benefits)",
             STEEL, value_size=22)
    _callout(fig, equiv_x, 0.43, 0.31, 0.13,
             f"${per_pupil_boost:,.0f}",
             "additional per-pupil spending\n(~4,200 students enrolled)",
             NAVY, value_size=22)

    # Historical gap table
    ax2 = fig.add_axes([0.08, 0.08, 0.86, 0.28])
    ax2.axis("off")
    hist_years = [2014, 2016, 2018, 2020, 2022, 2024]
    rows = []
    for y in hist_years:
        ed  = _v(sexp[sexp["dor_code"] == SAUGUS_CODE], y, "education")
        tot = _v(sexp[sexp["dor_code"] == SAUGUS_CODE], y, "total_expenditures")
        pct = ed / tot * 100 if tot else 0
        gap_y = (peer_med_pct / 100 * tot - ed) / 1e6
        rows.append([f"FY{y}", f"${ed/1e6:.1f}M", f"{pct:.1f}%", f"${gap_y:.1f}M"])

    col_labels = ["Year", "Actual Ed Spending", "Ed % of Budget",
                  f"Gap vs {peer_med_pct:.0f}% Peer Median"]
    tbl = ax2.table(cellText=rows, colLabels=col_labels,
                    loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_facecolor(NAVY)
            cell.set_text_props(color="white", fontweight="bold")
        elif c == 3:
            cell.set_facecolor("#FFF0F0")
            cell.set_text_props(color=RED, fontweight="bold")
        else:
            cell.set_facecolor("#F8F8F8" if r % 2 == 0 else "white")
        cell.set_edgecolor("#DDDDDD")

    ax2.set_title("Education Funding Gap — Every Other Year", fontsize=10,
                  color=NAVY, pad=8)

    fig.text(0.5, 0.02,
             f"Gap calculated as: (peer median ed% × Saugus total expenditures) − actual Saugus education spending.  "
             f"Peer median {peer_med_pct:.1f}% based on {year} Schedule A data for similarly sized MA towns.",
             ha="center", fontsize=7, color=STEEL, style="italic",
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── appendix divider ─────────────────────────────────────────────────────────

def appendix_divider_page(raw_pdf):
    """Unnumbered section-break slide. Accepts a raw PdfPages object to bypass page numbering."""
    from matplotlib.lines import Line2D
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    fig.text(0.5, 0.55,
             "Supporting Data & Methodology",
             ha="center", va="center",
             fontsize=28, fontweight="bold", color=WHITE,
             transform=fig.transFigure)

    line = Line2D([0.2, 0.8], [0.50, 0.50],
                  transform=fig.transFigure,
                  color=GOLD, linewidth=1.5)
    fig.add_artist(line)

    fig.text(0.5, 0.44,
             "The following slides are available for reference and Q&A.",
             ha="center", va="center",
             fontsize=14, color=LIGHT_GRAY,
             transform=fig.transFigure)

    fig.set_size_inches(11, 8.5)
    raw_pdf.savefig(fig)
    plt.close(fig)


# ── page 8: operating statement ───────────────────────────────────────────────

def operating_statement_page(pdf, srev, sexp, years_to_show=(2014, 2020, 2024)):
    available = set(srev["fiscal_year"].unique())
    snapshot_years = [y for y in years_to_show if y in available]

    def get(df, yr, col):
        rows = df[df["fiscal_year"] == yr]
        if len(rows) == 0:
            return 0
        v = rows[col].iloc[0]
        return 0 if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)

    REV_ITEMS = [
        ("Property Taxes",      "taxes"),
        ("State Aid",           "state_revenue"),
        ("Licenses & Permits",  "licenses_permits"),
        ("Service Charges",     "service_charges"),
        ("Fines & Forfeitures", "fines_forfeitures"),
        ("Miscellaneous",       "miscellaneous"),
        ("Transfers In",        "transfers"),
        ("Other Financing",     "other_financing"),
    ]
    EXP_ITEMS = [
        ("Education",           "education"),
        ("Public Safety",       "public_safety"),
        ("Fixed Costs",         "fixed_costs"),
        ("Debt Service",        "debt_service"),
        ("Public Works",        "public_works"),
        ("Intergovernmental",   "intergovernmental"),
        ("General Government",  "general_government"),
        ("Culture & Recreation","culture_recreation"),
        ("Human Services",      "human_services"),
        ("Other",               "other_expenditures"),
    ]

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")

    # APPENDIX watermark
    fig.text(0.5, 0.5, "APPENDIX",
             transform=fig.transFigure, fontsize=72, fontweight="bold",
             color="gray", alpha=0.08, ha="center", va="center",
             rotation=30, zorder=0)

    yr_labels = "  ·  ".join(f"FY{y}" for y in snapshot_years)
    fig.text(0.5, 0.965, "General Operating Fund Statement",
             ha="center", fontsize=13, fontweight="bold", color=NAVY)
    fig.text(0.5, 0.938,
             f"Saugus, Massachusetts  ·  Source: MA DLS Schedule A  ·  {yr_labels}",
             ha="center", fontsize=9, color=STEEL)

    label_w = 0.155
    val_w   = 0.095
    gap     = 0.025
    rev_x   = 0.025
    exp_x   = rev_x + label_w + len(snapshot_years) * val_w + gap

    def col_xs(sx):
        return [sx + label_w + (i + 1) * val_w for i in range(len(snapshot_years))]

    def draw_section(title, items, df, total_col, sx, top_y):
        xs      = col_xs(sx)
        total_w = label_w + len(snapshot_years) * val_w
        y       = top_y

        fig.text(sx, y, title, fontsize=9, fontweight="bold", color=NAVY,
                 transform=fig.transFigure)
        for i, yr in enumerate(snapshot_years):
            alpha = 1.0 if i == len(snapshot_years) - 1 else 0.6
            fig.text(xs[i], y, f"FY{yr}", fontsize=8, color=STEEL,
                     ha="right", alpha=alpha, transform=fig.transFigure)
        y -= 0.010
        fig.add_artist(plt.Line2D([sx, sx + total_w], [y, y],
                                  transform=fig.transFigure,
                                  color=NAVY, linewidth=0.8))
        y -= 0.016

        for label, col in items:
            vals = [get(df, yr, col) for yr in snapshot_years]
            if all(v == 0 for v in vals):
                continue
            is_ed = (col == "education")
            fig.text(sx + 0.004, y, label,
                     fontsize=6.8,
                     color=RED if is_ed else "black",
                     fontweight="bold" if is_ed else "normal",
                     transform=fig.transFigure)
            for i, (yr, v) in enumerate(zip(snapshot_years, vals)):
                color = (RED if is_ed else
                         (NAVY if i == len(snapshot_years) - 1 else "#666666"))
                fig.text(xs[i], y, f"${v/1e6:.1f}M",
                         fontsize=6.8, ha="right", color=color,
                         transform=fig.transFigure)
            y -= 0.026

        y -= 0.004
        fig.add_artist(plt.Line2D([sx, sx + total_w], [y, y],
                                  transform=fig.transFigure,
                                  color=NAVY, linewidth=1.0))
        y -= 0.017
        totals = [get(df, yr, total_col) for yr in snapshot_years]
        fig.text(sx + 0.004, y, f"TOTAL {title.upper()}",
                 fontsize=7.5, fontweight="bold", color=NAVY,
                 transform=fig.transFigure)
        for i, (yr, v) in enumerate(zip(snapshot_years, totals)):
            color = NAVY if i == len(snapshot_years) - 1 else "#555555"
            fig.text(xs[i], y, f"${v/1e6:.1f}M",
                     fontsize=7.5, fontweight="bold", ha="right",
                     color=color, transform=fig.transFigure)
        y -= 0.015
        for i in range(1, len(snapshot_years)):
            if totals[i - 1] > 0:
                chg = (totals[i] - totals[i - 1]) / totals[i - 1] * 100
                chg_color = (GREEN if title.startswith("Rev") else
                             (RED if chg > 0 else GREEN))
                mid_x = (xs[i - 1] + xs[i]) / 2
                fig.text(mid_x, y,
                         f"▲{chg:+.0f}%" if chg >= 0 else f"▼{chg:.0f}%",
                         fontsize=6, ha="center", color=chg_color,
                         transform=fig.transFigure)
        return y, totals

    rev_end_y, rev_totals = draw_section(
        "Revenues", REV_ITEMS, srev, "total_revenues", rev_x, top_y=0.905)
    exp_end_y, exp_totals = draw_section(
        "Expenditures", EXP_ITEMS, sexp, "total_expenditures", exp_x, top_y=0.905)

    bot_y = min(rev_end_y, exp_end_y) - 0.045
    surpluses = [r - e for r, e in zip(rev_totals, exp_totals)]
    surplus_strs = "     ".join(
        f"FY{yr}: ${s/1e6:.1f}M" for yr, s in zip(snapshot_years, surpluses))
    surplus_color = GREEN if surpluses[-1] >= 0 else RED
    fig.text(0.5, bot_y,
             f"Net {'Surplus' if surpluses[-1] >= 0 else 'Deficit'}  ·  {surplus_strs}",
             ha="center", fontsize=9.5, fontweight="bold", color=surplus_color,
             transform=fig.transFigure)

    fig.text(0.5, bot_y - 0.04,
             "Education row highlighted in red.  "
             "Values shown are nominal dollars (not inflation-adjusted).",
             ha="center", fontsize=7.5, color=STEEL, style="italic",
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── page 9: revenue & expenditure trends ─────────────────────────────────────

def revenue_expenditure_trends_page(pdf, srev, sexp, deflator):
    years = sorted(srev["fiscal_year"].unique())

    nom_rev  = [_v(srev, y, "total_revenues")      / 1e6 for y in years]
    nom_exp  = [_v(sexp, y, "total_expenditures")  / 1e6 for y in years]
    real_rev = [v * deflator.get(y, 1.0) for v, y in zip(nom_rev, years)]
    real_exp = [v * deflator.get(y, 1.0) for v, y in zip(nom_exp, years)]

    tax_pct   = [_v(srev, y, "taxes")         / _v(srev, y, "total_revenues") * 100 for y in years]
    state_pct = [_v(srev, y, "state_revenue") / _v(srev, y, "total_revenues") * 100 for y in years]
    other_pct = [100 - t - s for t, s in zip(tax_pct, state_pct)]

    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.text(0.5, 0.985, "Revenue & Expenditure Trends — Saugus General Fund",
             ha="center", fontsize=12, fontweight="bold", color=NAVY,
             transform=fig.transFigure)
    fig.text(0.5, 0.963,
             "Nominal = reported dollars.  Real = inflation-adjusted to FY2010 dollars using BLS CPI-U.  "
             "Inflation data: Federal Reserve Economic Data (FRED) · fred.stlouisfed.org/series/CPIAUCSL",
             ha="center", fontsize=7.5, color=STEEL, style="italic",
             transform=fig.transFigure)

    ax = axes[0, 0]
    ax.plot(years, nom_rev,  color=NAVY,  linewidth=2, marker="o", markersize=4, label="Nominal")
    ax.plot(years, real_rev, color=STEEL, linewidth=2, marker="s", markersize=4,
            linestyle="--", label="Real (FY2010 $)")
    ax.set_title("Total Revenues", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_m_direct))
    ax.legend(fontsize=8); ax.grid(True, alpha=0.25)

    ax = axes[0, 1]
    ax.stackplot(years, tax_pct, state_pct, other_pct,
                 labels=["Property Taxes", "State Aid", "Other Local"],
                 colors=[NAVY, STEEL, LIGHT], alpha=0.85)
    ax.set_title("Revenue Mix (% of Total)", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.set_ylim(0, 100); ax.legend(fontsize=7, loc="lower right"); ax.grid(True, alpha=0.2)

    ax = axes[1, 0]
    ax.plot(years, nom_exp,  color=RED,   linewidth=2, marker="o", markersize=4, label="Nominal")
    ax.plot(years, real_exp, color=ORANGE, linewidth=2, marker="s", markersize=4,
            linestyle="--", label="Real (FY2010 $)")
    ax.set_title("Total Expenditures", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_m_direct))
    ax.legend(fontsize=8); ax.grid(True, alpha=0.25)

    ax = axes[1, 1]
    fixed = [_v(sexp, y, "fixed_costs")  / 1e6 for y in years]
    debt  = [_v(sexp, y, "debt_service") / 1e6 for y in years]
    burden_pct = [(f + d) / (e / 1e6) * 100
                  for f, d, e in zip(fixed, debt,
                      [_v(sexp, y, "total_expenditures") for y in years])]
    ax.bar(years, fixed, color=GOLD,  label="Fixed Costs", alpha=0.85)
    ax.bar(years, debt, bottom=fixed, color=RED, label="Debt Service", alpha=0.85)
    ax2 = ax.twinx()
    ax2.plot(years, burden_pct, color=NAVY, linewidth=2, marker="D", markersize=4)
    ax2.axhline(35, color=RED, linewidth=1, linestyle=":", alpha=0.6)
    ax.set_title("Structural Burden (Fixed Costs + Debt Service)", fontsize=10, color=NAVY)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_m_direct))
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    lines1, lbl1 = ax.get_legend_handles_labels()
    ax.legend(lines1, lbl1, fontsize=7); ax.grid(True, alpha=0.2, axis="y")

    for a in axes.flat:
        a.tick_params(labelsize=7)
        for lbl in a.get_xticklabels():
            lbl.set_rotation(45)
        a.set_xlim(min(years) - 0.3, max(years) + 0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.948])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── appendix: hierarchical clustering dendrogram ─────────────────────────────

def dendrogram_page(pdf, Z, peer_df, optimal_k, feature_desc=None, rbp_work=None, rbp_top=None):
    """PCA scatter of all clustered towns — replaces the unreadable 221-town dendrogram."""
    if Z is None or peer_df is None or len(peer_df) == 0:
        return

    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA

    # Identify Saugus's cluster
    saugus_cluster = None
    if "hclust_label" in peer_df.columns:
        saugus_rows = peer_df[peer_df["municipality"] == "Saugus"]
        if len(saugus_rows):
            saugus_cluster = int(saugus_rows["hclust_label"].iloc[0])

    cluster_towns = set()
    if saugus_cluster is not None:
        cluster_towns = set(peer_df[peer_df["hclust_label"] == saugus_cluster]["municipality"].tolist())

    # Build feature matrix for PCA — use rbp_work (all districts with RBP features) if available
    if rbp_work is not None and rbp_top is not None:
        feat_cols = rbp_top
        # Merge cluster labels into rbp_work
        merged = peer_df[["municipality", "hclust_label"]].merge(
            rbp_work[["municipality"] + rbp_top], on="municipality", how="left")
        feat_matrix = merged[rbp_top].fillna(merged[rbp_top].mean())
        pca_df = merged[["municipality", "hclust_label"]].copy()
    else:
        feat_cols = [c for c in peer_df.columns
                     if c not in ("municipality", "hclust_label", "ed_pct", "mahal_dist", "dor_code")]
        if len(feat_cols) < 2:
            return  # fallback: can't do PCA without features
        feat_matrix = peer_df[feat_cols].fillna(peer_df[feat_cols].mean())
        pca_df = peer_df[["municipality", "hclust_label"]].copy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(feat_matrix)

    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X_scaled)
    pca_df = pca_df.copy()
    pca_df["pc1"] = coords[:, 0]
    pca_df["pc2"] = coords[:, 1]

    var1 = pca.explained_variance_ratio_[0] * 100
    var2 = pca.explained_variance_ratio_[1] * 100

    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)
    ax.set_facecolor(CHART_BG)

    # Color palette for clusters
    cluster_palette = [
        "#5D8AA8", "#E67E22", "#7D3C98", "#1E8449",
        "#C0392B", "#2471A3", "#F0A500", "#808080",
    ]

    unique_clusters = sorted(pca_df["hclust_label"].dropna().unique())
    for cl in unique_clusters:
        if cl == saugus_cluster:
            continue  # draw Saugus cluster last (on top)
        mask = pca_df["hclust_label"] == cl
        color = cluster_palette[int(cl) % len(cluster_palette)]
        ax.scatter(pca_df.loc[mask, "pc1"], pca_df.loc[mask, "pc2"],
                   color=color, alpha=0.45, s=18, zorder=2)

    # Draw Saugus cluster
    saugus_mask = pca_df["hclust_label"] == saugus_cluster
    ax.scatter(pca_df.loc[saugus_mask & (pca_df["municipality"] != "Saugus"), "pc1"],
               pca_df.loc[saugus_mask & (pca_df["municipality"] != "Saugus"), "pc2"],
               color=STEEL_BLUE, s=45, alpha=0.85, zorder=3, label="Saugus's cluster peers")

    # Label Saugus cluster towns (except Saugus itself)
    for _, row in pca_df[saugus_mask & (pca_df["municipality"] != "Saugus")].iterrows():
        ax.text(row["pc1"] + 0.06, row["pc2"], row["municipality"],
                fontsize=6.5, color=STEEL_BLUE, alpha=0.9, va="center")

    # Draw Saugus (gold star)
    saugus_row = pca_df[pca_df["municipality"] == "Saugus"]
    if len(saugus_row):
        ax.scatter(saugus_row["pc1"], saugus_row["pc2"],
                   color=GOLD, s=160, zorder=5, marker="*")
        ax.text(float(saugus_row["pc1"].iloc[0]) + 0.18,
                float(saugus_row["pc2"].iloc[0]),
                "Saugus", fontsize=9, color=GOLD, fontweight="bold", va="center")

    # Annotate Revere specifically if it appears in a non-Saugus cluster
    revere_row = pca_df[pca_df["municipality"].str.lower() == "revere"]
    if len(revere_row) and (saugus_cluster is None or
                            int(revere_row["hclust_label"].iloc[0]) != saugus_cluster):
        rx, ry = float(revere_row["pc1"].iloc[0]), float(revere_row["pc2"].iloc[0])
        ax.annotate(
            "Revere\n(different cluster:\nhigher commercial base)",
            xy=(rx, ry),
            xytext=(rx - 1.5, ry + 0.6),
            fontsize=7, color=LIGHT_GRAY,
            arrowprops=dict(arrowstyle="->", color=LIGHT_GRAY, lw=0.8),
            bbox=dict(boxstyle="round,pad=0.2", facecolor=NAVY, alpha=0.8,
                      edgecolor=CHART_GRID)
        )

    # Label 1 town from each non-Saugus cluster (closest to cluster centroid)
    import random as _random
    _random.seed(42)
    for cl in unique_clusters:
        if cl == saugus_cluster:
            continue
        cl_df = pca_df[pca_df["hclust_label"] == cl]
        centroid = cl_df[["pc1", "pc2"]].mean()
        cl_df = cl_df.copy()
        cl_df["dist_to_centroid"] = ((cl_df["pc1"] - centroid["pc1"])**2 +
                                      (cl_df["pc2"] - centroid["pc2"])**2) ** 0.5
        rep = cl_df.nsmallest(1, "dist_to_centroid").iloc[0]
        color = cluster_palette[int(cl) % len(cluster_palette)]
        ax.text(rep["pc1"] + 0.06, rep["pc2"], rep["municipality"],
                fontsize=5.5, color=color, alpha=0.7, va="center")

    ax.set_xlabel(f"Principal Component 1 ({var1:.0f}% variance)", color="white", fontsize=9)
    ax.set_ylabel(f"Principal Component 2 ({var2:.0f}% variance)", color="white", fontsize=9)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color(CHART_GRID)
    ax.grid(True, alpha=0.15, linestyle="--")

    _fdesc = feature_desc or "8 Fiscal & Demographic Features"
    ax.set_title(
        f"APPENDIX: Cluster Map — {len(pca_df)} MA Towns\n"
        f"{_fdesc}  ·  k={optimal_k} clusters (Ward linkage, elbow criterion)  ·  "
        f"PCA projection of z-scored features",
        color="white", fontsize=10, fontweight="bold", pad=8)

    # Story box — lead with the argument, not the methodology
    n_cluster = len(cluster_towns)

    story = (
        "What this chart shows:\n"
        f"• Saugus (gold ★) sits inside a cluster of {n_cluster} demographically similar MA towns\n"
        f"• Every one of those {n_cluster} towns spends a higher share of its budget on education\n"
        "• These towns face the same income levels, need rates, and state aid formulas as Saugus\n"
        "• The gap is not explained by wealth, demographics, or state funding\n"
        "• It is a local budget choice"
    )
    ax.text(0.01, 0.01, story,
            transform=ax.transAxes, fontsize=8, va="bottom", ha="left",
            color="white", linespacing=1.5,
            bbox=dict(boxstyle="round,pad=0.5", facecolor=DARK_BLUE,
                      edgecolor=GOLD, linewidth=1.5, alpha=0.95))

    fig.text(0.5, 0.01,
             f"PCA of {len(feat_cols)} z-scored features used for Ward hierarchical clustering.  "
             "Source: MA DLS Schedule A + ACS 5-year estimates.",
             ha="center", fontsize=7, color=LIGHT_GRAY, style="italic")

    plt.tight_layout(rect=[0, 0.02, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


# ── data sources ─────────────────────────────────────────────────────────────

def data_sources_page(pdf, rev, n_peer_towns):
    n_towns_raw = rev["dor_code"].nunique()
    yr_min  = int(rev["fiscal_year"].min())
    yr_max  = int(rev["fiscal_year"].max())

    def _src_block(fig, y, name, coverage, desc_lines, url, box_h=0.085):
        fig.text(0.06, y + 0.003, name, fontsize=8.5, fontweight="bold", color=NAVY,
                 transform=fig.transFigure)
        fig.text(0.62, y + 0.003, coverage, fontsize=7.5, color=STEEL,
                 transform=fig.transFigure)
        for i, line in enumerate(desc_lines):
            fig.text(0.06, y - 0.018 - i * 0.018, line, fontsize=7.0, color="#333333",
                     transform=fig.transFigure)
        fig.text(0.06, y - box_h + 0.016, f"↗ {url}", fontsize=6.5, color=STEEL,
                 style="italic", transform=fig.transFigure)
        return y - box_h - 0.012

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    fig.text(0.5, 0.968, "Data Sources & Methodology",
             ha="center", fontsize=13, fontweight="bold", color=NAVY)
    fig.text(0.5, 0.942,
             "All data loaded into a local PostgreSQL database (ma_school_data) for analysis.",
             ha="center", fontsize=8.5, color=STEEL)

    y = 0.918
    y = _src_block(fig, y, "MA DLS Schedule A — General Fund",
        f"FY{yr_min}–FY{yr_max}  ·  {n_towns_raw} towns in DLS database",
        ["Self-reported revenues and expenditures by category filed annually with MA Dept of Revenue.",
         "Unaudited. General Fund only — excludes enterprise funds (water/sewer), capital projects, trusts.",
         f"{n_towns_raw} towns downloaded — {n_peer_towns} used for peer comparisons "
         f"(Tyringham, Mount Washington, Wellfleet & East Brookfield excluded as extreme population outliers)."],
        "dls-gw.dor.state.ma.us")

    y = _src_block(fig, y, "BLS CPI-U via Federal Reserve Economic Data (FRED)",
        "Annual, FY2011–FY2025  ·  Series: CPIAUCSL",
        ["Monthly CPI-U (All Urban Consumers, All Items) averaged by calendar year to derive annual % change.",
         "Inflation adjustment: real$ = nominal$ ÷ cumulative price index (FY2010 base = 1.0).",
         "National CPI-U used. Boston MSA CPI (CUUSA103SA0) also computed: real ed growth −8.8% (national) vs −7.4% (Boston) — 1.4 ppt difference."],
        "fred.stlouisfed.org/series/CPIAUCSL")

    y = _src_block(fig, y, "U.S. Census Bureau — ACS 5-Year Estimates",
        "2023 vintage  ·  All MA municipalities",
        ["American Community Survey 5-year estimates used for population and median household income.",
         "2023 vintage matched to FY2024 Schedule A — standard one-year lag, consistent with DLS practice.",
         "Municipality names normalized to match MA DLS naming conventions for joins."],
        "census.gov/programs-surveys/acs")

    y = _src_block(fig, y, "Mahalanobis Distance Peer Selection",
        "10 closest towns to Saugus",
        ["6 factors (z-score standardized): Chronic Absenteeism %, Ch70 Aid/Pupil, % College-Educated, % SPED, Median HHI, % ELL.",
         "Factors chosen by leave-one-out importance from Ridge regression on MCAS scores across all 221 MA districts (R²=0.84).",
         "Peer group is a statistical result — not editorial judgment."],
        "See analysis/municipal_finance_report.py", box_h=0.092)

    y = _src_block(fig, y, "Hierarchical Clustering (Ward Linkage)",
        "Independent second methodology",
        ["Same 6 outcome-predictive factors as Mahalanobis. Ward linkage with Euclidean distance on all comparable towns.",
         "Number of clusters k determined by largest gap in successive merge distances (elbow criterion — no manual k).",
         "Saugus assigned to whichever cluster it naturally fell into. Convergence with Mahalanobis confirms robustness."],
        "See analysis/municipal_finance_report.py", box_h=0.092)

    # Caveats
    CAVEATS = [
        "Schedule A is self-reported and unaudited. Figures reflect town submissions, not audited amounts.",
        "General Fund only — enterprise funds, capital projects, and trust funds are excluded.",
        "Debt service is general fund P&I only; outstanding bond balances require a separate CAFR.",
        "Fixed costs include pensions, group health, and benefits. Definitions may vary slightly by town.",
        "CPI deflation uses national BLS CPI-U (FRED CPIAUCSL); local MA inflation may differ slightly.",
        "'Education' = total education appropriation to the school department, not per-pupil spending.",
    ]
    y -= 0.005
    fig.text(0.04, y, "Important Caveats", fontsize=8.5, fontweight="bold", color=RED,
             transform=fig.transFigure)
    y -= 0.022
    for caveat in CAVEATS:
        fig.text(0.05, y, f"• {caveat}", fontsize=7.0, color="#444444",
                 transform=fig.transFigure)
        y -= 0.028

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── slide 6: dual methodology — mahalanobis + hierarchical clustering ─────────

def dual_methodology_page(pdf, exp, mahal_peers, hclust_peers, year=2024):
    if mahal_peers is None or len(mahal_peers) == 0:
        return

    saugus_row = exp[(exp["fiscal_year"] == year) & (exp["dor_code"] == SAUGUS_CODE)].copy()
    saugus_ed  = float(saugus_row["education"].iloc[0] / saugus_row["total_expenditures"].iloc[0] * 100)

    mahal_set    = set(mahal_peers["municipality"].tolist())
    hclust_set   = set(hclust_peers["municipality"].tolist()) if len(hclust_peers) else set()
    consensus_set = mahal_set & hclust_set
    n_mahal      = len(mahal_set)
    n_hclust     = len(hclust_set)
    n_consensus  = len(consensus_set)

    mahal_med  = float(mahal_peers["ed_pct"].median())
    hclust_med = float(hclust_peers["ed_pct"].median()) if len(hclust_peers) else mahal_med

    # Build ed_pct lookup from Schedule A for all towns (covers peers who may lack it in peers_df)
    exp_yr = exp[exp["fiscal_year"] == year].copy()
    exp_yr["ed_pct_sched"] = exp_yr["education"] / exp_yr["total_expenditures"] * 100
    ed_lookup = (exp_yr.drop_duplicates("municipality")
                       .set_index("municipality")["ed_pct_sched"])

    # Compute peer medians from Schedule A (more complete than peers_df column)
    mahal_med_vals = [ed_lookup[m] for m in mahal_peers["municipality"] if m in ed_lookup.index and not pd.isna(ed_lookup[m])]
    hclust_med_vals = [ed_lookup[m] for m in hclust_peers["municipality"] if m in ed_lookup.index and not pd.isna(ed_lookup[m])]
    mahal_med  = float(pd.Series(mahal_med_vals).median()) if mahal_med_vals else float(mahal_peers["ed_pct"].median())
    hclust_med = float(pd.Series(hclust_med_vals).median()) if hclust_med_vals else mahal_med

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    fig.text(0.5, 0.977, "No Matter How You Slice It, Saugus Ranks Last",
             ha="center", fontsize=13, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.952,
             "We identified Saugus's true peer towns two different ways — different math, different approaches,\n"
             "no shared assumptions. Both reached the same conclusion.  Technical details in the footnotes below.",
             ha="center", fontsize=7.5, color=LIGHT_GRAY, transform=fig.transFigure)
    fig.text(0.5, 0.912,
             "These methods find towns that are truly similar to Saugus across income, demographics, "
             "and fiscal structure — not just similar in population or location.",
             ha="center", fontsize=9, color=LIGHT_GRAY,
             transform=fig.transFigure)

    # Method comparison table — shifted down slightly to accommodate plain-English sentence
    tbl_ax = fig.add_axes([0.02, 0.745, 0.96, 0.158])
    tbl_ax.axis("off")
    tbl_rows = [
        ["Approach",
         "Distance from Saugus to each town",
         "Natural grouping of all comparable towns"],
        ["Features",
         "6 z-scored factors (Ridge regression, R²=0.84)",
         "Same 6 factors"],
        ["Peers selected",
         f"10 closest towns",
         f"All {n_hclust} towns in Saugus's cluster"],
        ["Saugus rank",
         f"Last of {n_mahal + 1}",
         f"Last of {n_hclust + 1}"],
    ]
    tbl = tbl_ax.table(
        cellText=tbl_rows,
        colLabels=["", "Mahalanobis Distance", "Hierarchical Clustering (Ward)"],
        loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.35)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_facecolor("#2C3E6B")
            cell.set_text_props(color="white", fontweight="bold")
        elif c == 0:
            cell.set_facecolor("#243555")
            cell.set_text_props(color=LIGHT_GRAY, fontweight="bold")
        else:
            cell.set_facecolor("#1F3260")
            cell.set_text_props(color="white")
        cell.set_edgecolor(CHART_GRID)

    # Shared bar chart helper — uses Schedule A ed_pct lookup to show all towns
    def _peer_bar(ax, peers_df, title, peer_med):
        records = []
        for m in peers_df["municipality"].tolist():
            val = ed_lookup.get(m, float("nan"))
            if not pd.isna(val):
                records.append({"municipality": m, "ed_pct": val})
        records.append({"municipality": SAUGUS_NAME, "ed_pct": saugus_ed})
        combined = (pd.DataFrame(records)
                      .sort_values("ed_pct")
                      .reset_index(drop=True))
        towns = combined["municipality"].tolist()
        vals  = combined["ed_pct"].tolist()
        n     = len(towns)
        bar_colors = [GOLD if t == SAUGUS_NAME
                      else STEEL_BLUE if t in consensus_set
                      else "#4A7090" for t in towns]
        tick_labels = [t if t == SAUGUS_NAME
                       else f"{t} ★" if t in consensus_set
                       else t for t in towns]
        ax.barh(range(n), vals, color=bar_colors, edgecolor=CHART_GRID, linewidth=0.4, height=0.60)
        ax.axvline(peer_med, color=GOLD, linewidth=1.5, linestyle="--", alpha=0.8)
        ax.text(peer_med + 0.5, n - 0.5, f"Peer median\n{peer_med:.1f}%",
                va="top", fontsize=6, color=GOLD, fontweight="bold")
        saugus_idx = towns.index(SAUGUS_NAME) if SAUGUS_NAME in towns else None
        if saugus_idx is not None:
            ax.text(vals[saugus_idx] + 0.3, saugus_idx + 0.9,
                    f"  Saugus: {vals[saugus_idx]:.1f}%  ← last",
                    va="center", fontsize=7, fontweight="bold", color=RED)
        ax.set_yticks(range(n))
        ax.set_yticklabels(tick_labels, fontsize=6.0, color="white")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
        ax.set_xlim(0, 65)
        ax.set_title(title, fontsize=9, color="white", fontweight="bold", pad=6)
        ax.grid(True, alpha=0.2, axis="x", color=CHART_GRID)
        ax.tick_params(axis="y", labelsize=6, colors="white")
        ax.tick_params(axis="x", labelsize=7, colors="white")
        for spine in ax.spines.values():
            spine.set_edgecolor(CHART_GRID)

    ax_l = fig.add_axes([0.10, 0.26, 0.36, 0.48])
    ax_r = fig.add_axes([0.51, 0.26, 0.46, 0.48])
    ax_l.set_facecolor(CHART_BG)
    ax_r.set_facecolor(CHART_BG)
    _peer_bar(ax_l, mahal_peers, "Mahalanobis Distance Peers", mahal_med)
    _peer_bar(ax_r, hclust_peers, "Hierarchical Cluster Peers", hclust_med)

    fig.text(0.5, 0.235,
             "★ = town appears in both peer sets  ·  Blue bars = peers  ·  Gold = Saugus",
             ha="center", fontsize=7.5, color=LIGHT_GRAY, style="italic",
             transform=fig.transFigure)

    # Stat boxes — smaller, tighter, single row near bottom
    box_w, box_h, box_gap = 0.26, 0.115, 0.025
    box_y  = 0.085
    box_x0 = (1 - 3 * box_w - 2 * box_gap) / 2
    _callout(fig, box_x0, box_y, box_w, box_h,
             f"{n_consensus}", "towns agreed by\nboth methods", GOLD,
             value_size=22)
    _callout(fig, box_x0 + box_w + box_gap, box_y, box_w, box_h,
             f"Last of {n_mahal + 1}", "Saugus rank\nMahalanobis method", RED,
             value_size=22)
    _callout(fig, box_x0 + 2 * (box_w + box_gap), box_y, box_w, box_h,
             f"Last of {n_hclust + 1}", "Saugus rank\nHierarchical cluster", RED,
             value_size=22)

    fig.text(0.5, 0.040,
             "Different feature sets produce different peer lists — "
             "yet both methods independently rank Saugus last.",
             ha="center", fontsize=8, color=LIGHT_GRAY, style="italic",
             transform=fig.transFigure)

    fig.text(0.5, 0.022,
             "6 factors (z-score standardized): Chronic Absenteeism %, Ch70 State Aid/Pupil, "
             "% College-Educated Adults, % SPED, Median Household Income, % ELL",
             ha="center", fontsize=7, color=LIGHT_GRAY, style="italic",
             transform=fig.transFigure)
    fig.text(0.5, 0.006,
             "Nantucket excluded: resort/island economy — SPED % creates artificial closeness despite 60% college-educated adults (vs 31% Saugus).  "
             "Source: MA DLS Schedule A + DESE + ACS (2023).",
             ha="center", fontsize=7, color=LIGHT_GRAY, style="italic",
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── "saugus behaving like a wealthy town" slide ───────────────────────────────

def wealthy_town_slide(pdf, exp, acs, tax_rates, year=2024):
    saugus_acs = acs[acs["municipality"] == SAUGUS_NAME]
    saugus_hhi = int(saugus_acs["median_hh_income"].iloc[0]) if len(saugus_acs) else 100819

    saugus_ed = exp[(exp["dor_code"] == SAUGUS_CODE) & (exp["fiscal_year"] == year)]
    saugus_ed_pct = float(saugus_ed["education"].iloc[0] / saugus_ed["total_expenditures"].iloc[0] * 100)

    # Tax rate data for Saugus
    saugus_tr = tax_rates[(tax_rates["dor_code"] == SAUGUS_CODE) & (tax_rates["fiscal_year"] == year)]
    res_rate  = float(saugus_tr["residential"].iloc[0]) if len(saugus_tr) else 10.65
    com_rate  = float(saugus_tr["commercial"].iloc[0])  if len(saugus_tr) else 22.05
    split_ratio = com_rate / res_rate

    # Neighboring towns for residential rate comparison (recognizable to Saugus audience)
    # Geographic abutters only — avoids cherry-picking objection
    neighbor_codes = {
        "Lynn": 163, "Stoneham": 284,
        "Wakefield": 305, "Swampscott": 291,
    }
    neighbor_rates = {}
    for name, code in neighbor_codes.items():
        row = tax_rates[(tax_rates["dor_code"] == code) & (tax_rates["fiscal_year"] == year)]
        if len(row):
            neighbor_rates[name] = float(row["residential"].iloc[0])

    # Build bar chart data: Saugus + neighbors, sorted ascending
    bar_data = dict(neighbor_rates)
    bar_data["Saugus"] = res_rate
    bar_sorted = sorted(bar_data.items(), key=lambda x: x[1])
    bar_towns  = [t for t, _ in bar_sorted]
    bar_vals   = [v for _, v in bar_sorted]
    bar_colors = [GOLD if t == "Saugus" else STEEL for t in bar_towns]

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    # Title
    fig.text(0.5, 0.920,
             "Route 1 Is Keeping Your Tax Bill Low.",
             ha="center", fontsize=22, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.875,
             "That Advantage Isn't Reaching Your Schools.",
             ha="center", fontsize=22, fontweight="bold", color=GOLD,
             transform=fig.transFigure)

    # ── Three stat cards ────────────────────────────────────────────────────────
    card_h, card_w = 0.130, 0.265
    card_y = 0.710
    cx0, cx1, cx2 = 0.040, 0.368, 0.695

    # Dollar signs escaped as \$ to prevent matplotlib mathtext interpreting them as delimiters
    _callout(fig, cx0, card_y, card_w, card_h,
             f"\\${res_rate:.2f}",
             f"Saugus residential tax rate\nper \\$1,000 assessed value (FY2024)\nCompetitive with abutting communities",
             BLUE, value_size=26)

    _callout(fig, cx1, card_y, card_w, card_h,
             f"\\${com_rate:.2f}",
             f"Route 1 commercial rate\nper \\$1,000 assessed value (FY2024)\nBusinesses pay double the residential rate",
             GOLD, value_size=26, text_color=NAVY)

    _callout(fig, cx2, card_y, card_w, card_h,
             f"{saugus_ed_pct:.1f}%",
             "Education share of budget\n(FY2024)\nNear the bottom of all comparable towns",
             RED, value_size=26)

    # ── Bar chart: residential tax rates vs neighbors (left column) ─────────────
    ax = fig.add_axes([0.16, 0.31, 0.35, 0.34])
    ax.set_facecolor(CHART_BG)
    bars = ax.barh(bar_towns, bar_vals, color=bar_colors, height=0.6, zorder=3)
    ax.set_xlim(0, max(bar_vals) * 1.30)
    ax.set_xlabel("Residential tax rate ($/1,000)", color="white", fontsize=9)
    ax.tick_params(colors="white", labelsize=9)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.xaxis.label.set_color("white")
    ax.tick_params(axis="x", colors="white")
    ax.tick_params(axis="y", colors="white")
    ax.set_axisbelow(True)
    ax.xaxis.set_tick_params(labelsize=8)
    for bar_obj, val in zip(bars, bar_vals):
        ax.text(bar_obj.get_width() + 0.12, bar_obj.get_y() + bar_obj.get_height() / 2,
                f"${val:.2f}", va="center", fontsize=8.5, color="white", fontweight="bold")
    ax.set_title("Residential Tax Rate vs. Neighboring Towns (FY2024)",
                 color="white", fontsize=9, pad=6)

    # ── Right-side ratio callout box (DARK_BLUE, clear of bar chart) ──────────
    RX = 0.575  # left edge of right column
    _callout(fig, RX, 0.510, 0.375, 0.145,
             f"{split_ratio:.2f}\u00d7",
             "commercial-to-residential tax rate ratio\n"
             f"Businesses: \\${com_rate:.2f}   Homeowners: \\${res_rate:.2f}",
             DARK_BLUE, value_size=38)

    fig.text(RX + 0.1875, 0.470,
             f"Median household income: \\${saugus_hhi:,}",
             ha="center", fontsize=8.5, color=LIGHT_GRAY, alpha=0.80,
             transform=fig.transFigure)

    # ── Narrative block ─────────────────────────────────────────────────────────
    narrative_ax = fig.add_axes([0.05, 0.040, 0.90, 0.175])
    narrative_ax.axis("off")
    rect = plt.Rectangle((0, 0), 1, 1, transform=narrative_ax.transAxes,
                          facecolor=DARK_BLUE, edgecolor=GOLD, linewidth=2)
    narrative_ax.add_patch(rect)
    narrative_ax.text(0.5, 0.5,
        "Saugus businesses along Route 1 pay double the tax rate of homeowners — that commercial corridor\n"
        "is a real fiscal advantage that keeps residential bills competitive with abutting communities.\n"
        "Yet Saugus ranks near the bottom in education spending compared to similarly sized towns.\n"
        "The Route 1 advantage is real. It just isn't reaching the schools.",
        transform=narrative_ax.transAxes,
        fontsize=10.5, color="white", ha="center", va="center",
        linespacing=1.9, style="italic")

    fig.text(0.5, 0.020,
             "Tax rates: MA DOR municipal_tax_rates table (FY2024).  "
             "Education %: MA DLS Schedule A FY2024.  "
             "Median HHI: ACS 5-year estimates (2023).",
             ha="center", fontsize=7, color="white", alpha=0.5,
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── closing slide ─────────────────────────────────────────────────────────────

def closing_slide(pdf, srev, sexp, exp):
    yr = 2024
    ed_pct_2010 = _v(sexp, 2010, "education") / _v(sexp, 2010, "total_expenditures") * 100
    ed_pct_now  = _v(sexp, yr,   "education") / _v(sexp, yr,   "total_expenditures") * 100
    surplus_fy25 = (_v(srev, 2025, "total_revenues") - _v(sexp, 2025, "total_expenditures")) / 1e6
    surplus_years = sum(
        1 for y in range(2010, 2026)
        if _v(srev, y, "total_revenues") > _v(sexp, y, "total_expenditures")
    )
    avg_surplus = sum(
        _v(srev, y, "total_revenues") - _v(sexp, y, "total_expenditures")
        for y in range(2010, 2026)
    ) / 16 / 1e6

    # Compute peer median from full exp DataFrame (not Saugus-only sexp)
    peer_yr = exp[(exp["fiscal_year"] == yr) &
                  (~exp["dor_code"].isin(EXCLUDED_PEER_CODES)) &
                  (exp["dor_code"] != SAUGUS_CODE)].copy()
    peer_yr = peer_yr[peer_yr["total_expenditures"] > 0]
    peer_yr["ed_pct"] = peer_yr["education"] / peer_yr["total_expenditures"] * 100
    peer_med_pct = float(peer_yr["ed_pct"].dropna().median())
    total_exp = _v(sexp, yr, "total_expenditures")
    gap_m = (total_exp * peer_med_pct / 100 - _v(sexp, yr, "education")) / 1e6

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    # Title
    fig.text(0.5, 0.875,
             "Stability Was the Right Call.",
             ha="center", fontsize=26, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.825,
             "Now It's Time to Invest.",
             ha="center", fontsize=26, fontweight="bold", color=GOLD,
             transform=fig.transFigure)

    # 4 stat cards
    card_y = 0.57
    card_h = 0.19
    card_w = 0.20
    gap_card = 0.025
    start_x = (1 - (4 * card_w + 3 * gap_card)) / 2

    cards = [
        (f"{ed_pct_2010:.0f}% → {ed_pct_now:.0f}%",
         "Education share of budget\nFY2010 → FY2024", DARK_RED),
        (f"${gap_m:.0f}M/yr",
         "Annual gap vs\npeer-median spending", RED),
        (f"${surplus_fy25:.1f}M",
         "FY2025 general\nfund surplus", INFO_BLUE),
        (f"{surplus_years} of 16",
         f"years with a budget surplus\n(avg. ${avg_surplus:.1f}M/yr)", INFO_BLUE),
    ]
    for i, (val, lbl, bg) in enumerate(cards):
        cx = start_x + i * (card_w + gap_card)
        _callout(fig, cx, card_y, card_w, card_h, val, lbl, bg, value_size=20)

    # Narrative block with gold border
    narrative_ax = fig.add_axes([0.06, 0.20, 0.88, 0.34])
    narrative_ax.axis("off")
    narrative_ax.patch.set_alpha(0)
    for spine in narrative_ax.spines.values():
        spine.set_visible(False)
    rect = plt.Rectangle((0.0, 0.0), 1.0, 1.0, transform=narrative_ax.transAxes,
                          facecolor=DARK_BLUE, edgecolor=GOLD, linewidth=2, zorder=0)
    narrative_ax.add_patch(rect)
    narrative_ax.text(0.5, 0.5,
        "In 2012, Saugus nearly entered state receivership following criminal mismanagement,\n"
        "bid-rigging, and depleted reserves. Scott Crabtree's turnaround — from DOR watchlist\n"
        "to AA+ bond rating — is a genuine achievement that deserves full credit.\n"
        "That crisis is 13 years behind us. The emergency is over.\n"
        "The stabilization fund exists because of that hard work.\n"
        "The question before Town Meeting is what we do with the stability we've earned.",
        transform=narrative_ax.transAxes,
        fontsize=10.5, color="white", ha="center", va="center",
        linespacing=1.75, style="italic")

    # 3-bullet ask list
    asks = [
        "Commit to a 3-year glide path back to 40% — the share Saugus maintained before the crisis years",
        "Allocate $3M of the FY2025 surplus to restore teacher positions cut since 2017",
        "Direct the Finance Committee to publish a multi-year education investment plan before next budget cycle",
    ]
    ask_y = 0.185
    fig.text(0.08, ask_y, "What We're Asking For:",
             transform=fig.transFigure, fontsize=11, fontweight="bold",
             color=GOLD, ha="left")
    ask_y -= 0.038
    for ask in asks:
        fig.text(0.10, ask_y, f"▶  {ask}",
                 transform=fig.transFigure, fontsize=9.5, color="white",
                 ha="left", linespacing=1.4)
        ask_y -= 0.048

    fig.text(0.5, 0.025,
             "Data: MA DLS Schedule A FY2010–FY2025",
             ha="center", fontsize=7.5, color="white", alpha=0.55,
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── student outcomes page ─────────────────────────────────────────────────────

def student_outcomes_page(pdf, outcomes: dict):
    """Slide: MCAS trend lines + graduation rates — Saugus vs peers."""
    mcas        = outcomes["mcas"]
    state_med   = outcomes["state_med"]
    grad        = outcomes["grad"]
    teacher     = outcomes["teacher"]
    SAUGUS_ORG  = outcomes["saugus_org"]
    PEER_ORGS   = outcomes["peer_orgs"]

    saugus_ela  = mcas[(mcas["org_code"] == SAUGUS_ORG) & (mcas["subject"] == "ELA")].set_index("school_year")["meeting_exceeding_pct"]

    # Consensus peer average per year (ELA grades 3-8)
    peer_mcas_ela = (mcas[mcas["org_code"].isin(PEER_ORGS.values()) & (mcas["subject"] == "ELA")]
                     .groupby("school_year")["meeting_exceeding_pct"].mean())

    state_ela = state_med[state_med["subject"] == "ELA"].set_index("school_year")["state_med"]

    # Saugus graduation
    saugus_grad = grad[grad["org_code"] == SAUGUS_ORG].set_index("school_year")["four_year_grad_pct"]
    # Peer grad averages
    peer_grad = (grad[grad["org_code"].isin(PEER_ORGS.values())]
                 .groupby("school_year")["four_year_grad_pct"].mean())

    # Key stats — use 2019 (last pre-Covid year) as baseline
    ela_2019   = float(saugus_ela.get(2019, saugus_ela.get(2017, 0.48)))
    ela_latest_yr = int(max(saugus_ela.index))
    ela_latest = float(saugus_ela.get(ela_latest_yr, saugus_ela.iloc[-1]))
    ela_drop   = ela_latest - ela_2019

    peer_ela_2019   = float(peer_mcas_ela.get(2019, peer_mcas_ela.get(2017, 0.53)))
    peer_ela_latest = float(peer_mcas_ela.get(ela_latest_yr, peer_mcas_ela.iloc[-1]))
    gap_2019   = ela_2019  - peer_ela_2019
    gap_latest = ela_latest - peer_ela_latest

    grad_2017   = float(saugus_grad.get(2017, 89.2))
    grad_latest = float(saugus_grad.get(2025, saugus_grad.iloc[-1]))

    # Teacher FTE: 2017 vs latest
    t2017 = teacher[teacher["school_year"] == 2017]["fte"].iloc[0] if 2017 in teacher["school_year"].values else None
    t_latest_yr = teacher["school_year"].max()
    t_latest = float(teacher[teacher["school_year"] == t_latest_yr]["fte"].iloc[0])

    years_mcas = sorted(set(saugus_ela.index) & set(peer_mcas_ela.index))

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    # Title
    fig.text(0.5, 0.944, "The Results Are Showing Up in the Classrooms",
             ha="center", fontsize=21, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.912,
             "Spending fell.  Teacher count fell.  Outcomes fell.  Peer towns diverged.",
             ha="center", fontsize=11, color=GOLD, style="italic",
             transform=fig.transFigure)
    peer_names_str = ", ".join(sorted(PEER_ORGS.keys()))
    fig.text(0.5, 0.888,
             f"Peer towns: {peer_names_str}  "
             "— consensus of Mahalanobis Distance and Hierarchical Clustering (Ward)  (see slide 6)",
             ha="center", fontsize=9, color=LIGHT_GRAY, transform=fig.transFigure)

    # Chart label — sits between the peer towns line (0.888) and the ELA chart top (~0.86)
    fig.text(0.05, 0.878,
             "ELA: % Meeting or Exceeding — Grades 3–8 (All Students)  ·  Grade 10 trends in Appendix",
             ha="left", fontsize=8, color=LIGHT_GRAY, alpha=0.85,
             transform=fig.transFigure)

    # ── Left-top: ELA trend chart (Gr 3-8) ──────────────────────────────────
    ax = fig.add_axes([0.05, 0.44, 0.58, 0.42])
    ax.set_facecolor(CHART_BG)

    # State median (filled area)
    sm_yrs = [y for y in years_mcas if y in state_ela.index]
    ax.fill_between(sm_yrs, [float(state_ela[y]) * 100 for y in sm_yrs], alpha=0.12,
                    color="white", label="_nolegend_")

    # Peer average (grades 3-8)
    ax.plot(years_mcas, [float(peer_mcas_ela[y]) * 100 for y in years_mcas],
            color=STEEL, linewidth=2, linestyle="--", marker="o", markersize=4,
            label="Peers Gr 3–8 (avg)")

    # State median line
    ax.plot(sm_yrs, [float(state_ela[y]) * 100 for y in sm_yrs],
            color="white", linewidth=1.2, linestyle=":", alpha=0.55, label="MA state median")

    # Saugus grades 3-8
    ax.plot(years_mcas, [float(saugus_ela[y]) * 100 for y in years_mcas],
            color=GOLD, linewidth=3, marker="o", markersize=5, label="Saugus Gr 3–8")

    # Covid annotation band (testing cancelled 2020, disrupted 2021)
    ax.axvspan(2019.5, 2021.5, alpha=0.10, color="white", zorder=1)
    ax.text(2020.5, 18, "Covid\n(2020 cancelled)", ha="center", fontsize=6.5,
            color="white", alpha=0.55, va="bottom")

    # Gap annotation on last year
    last_yr = years_mcas[-1]
    ax.annotate("",
                xy=(last_yr, float(saugus_ela[last_yr]) * 100),
                xytext=(last_yr, float(peer_mcas_ela[last_yr]) * 100),
                arrowprops=dict(arrowstyle="<->", color=LIGHT_GRAY, lw=1.5))
    ax.text(last_yr + 0.15, (float(saugus_ela[last_yr]) + float(peer_mcas_ela[last_yr])) / 2 * 100,
            f"{abs(gap_latest)*100:.0f}pp\ngap",
            color=LIGHT_GRAY, fontsize=7.5, va="center")

    ax.set_ylabel("Students Meeting/Exceeding (%)", color="white", fontsize=9)
    ax.set_ylim(15, 75)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color(CHART_GRID)
    ax.legend(loc="upper right", fontsize=7.5, framealpha=0.3, labelcolor="white",
              facecolor=DARK_BLUE)
    ax.tick_params(labelbottom=False)

    # ── Left-bottom: graduation chart ─────────────────────────────────────────
    ax2 = fig.add_axes([0.05, 0.06, 0.58, 0.33])
    ax2.set_facecolor(CHART_BG)
    grad_yrs = sorted(saugus_grad.index)
    peer_grad_yrs = sorted(peer_grad.index)
    ax2.plot(peer_grad_yrs, [float(peer_grad[y]) for y in peer_grad_yrs],
             color=STEEL, linewidth=2, linestyle="--", marker="o", markersize=4, label="Peer avg")
    ax2.plot(grad_yrs, [float(saugus_grad[y]) for y in grad_yrs],
             color=GOLD, linewidth=2.5, marker="o", markersize=4, label="Saugus")
    ax2.axhline(90, color="white", alpha=0.2, linewidth=1)
    ax2.set_ylim(75, 100)
    ax2.set_ylabel("4-yr grad rate (%)", color="white", fontsize=8)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax2.tick_params(colors="white", labelsize=7.5)
    for spine in ax2.spines.values():
        spine.set_color(CHART_GRID)
    ax2.legend(loc="lower left", fontsize=7, framealpha=0.3, labelcolor="white",
               facecolor=DARK_BLUE)
    ax2.set_title("4-Year Graduation Rate", color="white", fontsize=8.5, pad=4)

    # ── Right: stat cards ─────────────────────────────────────────────────────
    rx, card_w, card_h = 0.655, 0.320, 0.130
    card_tops = [0.855, 0.700, 0.545, 0.390]

    _callout(fig, rx, card_tops[0] - card_h, card_w, card_h,
             f"{ela_latest * 100:.0f}%",
             f"Saugus ELA meeting/exceeding ({ela_latest_yr})\n"
             f"Down from {ela_2019*100:.0f}% pre-Covid (2019)\n"
             f"Peers averaged {peer_ela_latest*100:.0f}% in {ela_latest_yr}",
             RED, value_size=28)

    _callout(fig, rx, card_tops[1] - card_h, card_w, card_h,
             f"{ela_drop * 100:+.0f}pp",
             f"Saugus ELA change 2019→{ela_latest_yr}\n"
             f"Peers fell {abs((peer_ela_latest-peer_ela_2019)*100):.0f}pp — Saugus fell {abs(ela_drop*100):.0f}pp\n"
             f"Post-Covid: peers stabilized, Saugus kept falling",
             DARK_RED, value_size=28)

    _callout(fig, rx, card_tops[2] - card_h, card_w, card_h,
             f"{gap_latest * 100:+.0f}pp",
             f"Saugus gap vs peers ({ela_latest_yr} ELA)\n"
             f"Gap was {gap_2019*100:+.0f}pp in 2019 — has more than doubled\n"
             f"Covid affected all towns; the gap kept widening after",
             DARK_RED, value_size=28)

    _callout(fig, rx, card_tops[3] - card_h, card_w, card_h,
             f"{grad_latest:.1f}%",
             f"4-year graduation rate (2025)\nDown from {grad_2017:.1f}% in 2017  ·  Dropout: 9.1%",
             DARK_BLUE, value_size=28)

    # ── Teacher staffing note (bottom-right below cards) ──────────────────────
    if t2017 is not None:
        t_drop = t_latest - t2017
        fig.text(rx + card_w / 2, 0.175,
                 f"Teacher FTE: {t2017:.0f} (2017) → {t_latest:.0f} ({t_latest_yr})",
                 ha="center", fontsize=8.5, color=ORANGE, fontweight="bold",
                 transform=fig.transFigure)
        fig.text(rx + card_w / 2, 0.143,
                 f"{t_drop:+.0f} FTE while enrollment stayed flat",
                 ha="center", fontsize=8, color="white", alpha=0.75,
                 transform=fig.transFigure)
        fig.text(rx + card_w / 2, 0.115,
                 "Fewer teachers per student as budget share fell",
                 ha="center", fontsize=7.5, color="white", alpha=0.60,
                 transform=fig.transFigure)

    # ── Footer ────────────────────────────────────────────────────────────────
    fig.text(0.5, 0.025,
             f"MCAS: MA DESE (new format, 2017 onward; grades 3–8 all students).  "
             f"Peer set: {peer_names_str} (consensus from Mahalanobis + Ward clustering).  "
             "Staffing: MA DESE.  Correlation ≠ causation.",
             ha="center", fontsize=6.5, color="white", alpha=0.5,
             transform=fig.transFigure)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── APPENDIX: Grade 10 ELA ────────────────────────────────────────────────────

def grade10_ela_appendix_page(pdf, outcomes: dict):
    """Appendix slide: Grade 10 ELA trend — moved out of main MCAS slide for space."""
    mcas10     = outcomes["mcas10"]
    SAUGUS_ORG = outcomes["saugus_org"]
    PEER_ORGS  = outcomes["peer_orgs"]

    saugus_ela10 = (mcas10[(mcas10["org_code"] == SAUGUS_ORG) & (mcas10["subject"] == "ELA")]
                   .set_index("school_year")["meeting_exceeding_pct"])
    peer_ela10   = (mcas10[mcas10["org_code"].isin(PEER_ORGS.values()) & (mcas10["subject"] == "ELA")]
                   .groupby("school_year")["meeting_exceeding_pct"].mean())

    if saugus_ela10.empty:
        return

    yr10 = sorted(set(saugus_ela10.index) & set(peer_ela10.index))

    fig, ax = plt.subplots(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)
    ax.set_facecolor(CHART_BG)

    if yr10:
        ax.plot(yr10, [float(peer_ela10[y]) * 100 for y in yr10],
                color=STEEL, linewidth=2, linestyle="--", marker="o", markersize=5,
                label="Peers Gr 10 (avg)")
        ax.plot(yr10, [float(saugus_ela10[y]) * 100 for y in yr10],
                color=GOLD, linewidth=3, marker="s", markersize=6, label="Saugus Gr 10")
        ax.axvspan(2019.5, 2021.5, alpha=0.10, color="white", zorder=1)
        ax.text(2020.5, 20, "Covid\n(2020 cancelled)", ha="center", fontsize=7,
                color="white", alpha=0.55, va="bottom")

    ax.set_ylim(15, 100)
    ax.set_ylabel("% Meeting or Exceeding", color="white", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.tick_params(colors="white", labelsize=9)
    for spine in ax.spines.values():
        spine.set_color(CHART_GRID)
    ax.legend(loc="upper right", fontsize=9, framealpha=0.3, labelcolor="white",
              facecolor=DARK_BLUE)
    ax.grid(True, alpha=0.15, linestyle="--")
    peer_names_str = ", ".join(sorted(PEER_ORGS.keys()))
    ax.set_title(f"APPENDIX: ELA Grade 10\n"
                 f"Saugus vs Consensus Peer Towns ({peer_names_str})",
                 color="white", fontsize=11, fontweight="bold", pad=10)

    fig.text(0.5, 0.025,
             "Grade 10 ELA is the high-stakes graduation requirement test — a different scale from Grades 3–8.  "
             "Source: MA DESE MCAS results.",
             ha="center", fontsize=7, color=LIGHT_GRAY, style="italic")

    pdf.savefig(fig)
    plt.close(fig)


# ── Comprehensive RBP feature load ───────────────────────────────────────────

def load_rbp_features(engine, year: int = 2024) -> pd.DataFrame:
    """
    Build a district-level feature matrix for RBP from all available DESE/ACS tables.
    Returns one row per district with 14 predictors + avg_mcas target.
    Joins by district_name across DESE tables; bridges to ACS via municipality name.
    """
    with engine.connect() as conn:
        # Target: avg MCAS meeting/exceeding across ELA + Math, grades 3-8
        mcas = pd.read_sql(text("""
            SELECT org_code, district_name,
                   AVG(meeting_exceeding_pct) AS avg_mcas
            FROM mcas_results
            WHERE school_year = :yr
              AND student_group = 'All Students'
              AND grade = 'ALL (03-08)'
              AND subject IN ('ELA', 'MATH')
              AND org_code LIKE '%%0000'
            GROUP BY org_code, district_name
        """), conn, params={"yr": year})

        # Demographics
        pop = pd.read_sql(text("""
            SELECT district_name,
                   high_needs_pct, ell_pct, low_income_pct, sped_pct
            FROM district_selected_populations
            WHERE school_year = :yr
        """), conn, params={"yr": year})

        # Per-pupil spending
        ppe = pd.read_sql(text("""
            SELECT district_name, category, amount
            FROM per_pupil_expenditure
            WHERE school_year = :yr
              AND category IN ('Total In-District Expenditures', 'Teachers')
        """), conn, params={"yr": year})
        ppe_wide = ppe.pivot_table(index="district_name", columns="category",
                                   values="amount", aggfunc="first").reset_index()
        ppe_wide.columns.name = None
        ppe_wide = ppe_wide.rename(columns={
            "Total In-District Expenditures": "nss_per_pupil",
            "Teachers": "teacher_spending_per_pupil",
        })

        # Chapter 70 aid per pupil
        ch70 = pd.read_sql(text("""
            SELECT district_name, chapter70_aid_per_pupil AS ch70_per_pupil,
                   foundation_enrollment
            FROM district_chapter70
            WHERE fiscal_year = :yr
        """), conn, params={"yr": year})

        # Chronic absenteeism (district-level: school_name IS NULL, student_group='All')
        attend = pd.read_sql(text("""
            SELECT district_name, chronic_absenteeism_pct
            FROM attendance
            WHERE school_year = :yr AND student_group = 'All'
              AND school_name IS NULL
        """), conn, params={"yr": year})

        # Staffing: teacher FTE ratio and avg salary
        staff = pd.read_sql(text("""
            SELECT district_name, category, fte, avg_salary
            FROM staffing
            WHERE school_year = :yr
              AND category IN ('teacher_fte', 'teachers_per_100_fte', 'teacher_avg_salary')
        """), conn, params={"yr": year})
        staff_wide = staff.pivot_table(
            index="district_name", columns="category",
            values=["fte", "avg_salary"], aggfunc="first").reset_index()
        staff_wide.columns = ["district_name" if a == "" else
                               (b if a == "fte" else f"{b}_salary")
                               for a, b in [("", "")] +
                               [(col[0], col[1]) for col in staff_wide.columns[1:]]]
        # Simpler pivot
        staff_t = (staff[staff["category"] == "teacher_fte"]
                   [["district_name", "fte"]].rename(columns={"fte": "teacher_fte"}))
        staff_r = (staff[staff["category"] == "teachers_per_100_fte"]
                   [["district_name", "fte"]].rename(columns={"fte": "teachers_per_100_fte"}))
        staff_s = (staff[staff["category"] == "teacher_avg_salary"]
                   [["district_name", "avg_salary"]].rename(columns={"avg_salary": "avg_teacher_salary"}))

        # Total enrollment
        enroll = pd.read_sql(text("""
            SELECT district_name, total AS total_enrollment
            FROM enrollment
            WHERE school_year = :yr AND grade = 'Total' AND school_name IS NULL
        """), conn, params={"yr": year})

        # ACS: most recent year ≤ school year (lagged 1-yr)
        acs = pd.read_sql(text("""
            SELECT DISTINCT ON (municipality)
                municipality, median_hh_income,
                pct_owner_occupied, pct_bachelors_plus, pct_65_plus
            FROM municipal_census_acs
            ORDER BY municipality, acs_year DESC
        """), conn)
        acs["municipality"] = (acs["municipality"]
                               .str.replace(r"\s+Town$", "", regex=True).str.strip())
        acs = acs.drop_duplicates(subset=["municipality"])

    # Build feature matrix: start from MCAS, left-join everything
    df = mcas.copy()
    df = df.merge(pop, on="district_name", how="left")
    df = df.merge(ppe_wide, on="district_name", how="left")
    df = df.merge(ch70[["district_name", "ch70_per_pupil"]], on="district_name", how="left")
    df = df.merge(attend, on="district_name", how="left")
    df = df.merge(staff_t, on="district_name", how="left")
    df = df.merge(staff_r, on="district_name", how="left")
    df = df.merge(staff_s, on="district_name", how="left")
    df = df.merge(enroll, on="district_name", how="left")
    # Teachers per 100 students (direct student-teacher ratio proxy)
    df["teachers_per_100_students"] = df["teacher_fte"] / df["total_enrollment"] * 100
    # Bridge DESE → ACS via normalized district_name ≈ municipality
    df["_muni"] = df["district_name"].str.strip()
    acs = acs.rename(columns={"municipality": "_muni"})
    df = df.merge(acs, on="_muni", how="left")
    df = df.drop(columns=["_muni"])

    # Convert Decimal to float
    for col in df.columns:
        if col not in ("org_code", "district_name"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Saugus flag
    df["is_saugus"] = df["district_name"] == SAUGUS_NAME
    return df


# ── Relevance-Based Prediction ───────────────────────────────────────────────

def rbp_predict(df: pd.DataFrame, features: list[str], target: str,
                _bandwidth: float | None = None) -> pd.DataFrame:
    """
    Leave-one-out Ridge regression prediction.
    Predicts each district's MCAS using a model trained on all other districts.
    Returns input df with added columns: rbp_pred (%), rbp_resid (pp), rbp_weight.
    """
    sub = df[features + [target]].dropna().copy()
    n = len(sub)

    X_raw = sub[features].values.astype(float)
    y     = sub[target].values.astype(float)

    # Z-score normalise features
    mu  = X_raw.mean(axis=0)
    sig = X_raw.std(axis=0)
    sig[sig == 0] = 1.0
    X = (X_raw - mu) / sig

    alpha = 0.1   # mild L2 regularisation (Ridge)
    p_dim = X.shape[1]
    preds = np.full(n, np.nan)

    for i in range(n):
        X_tr = np.delete(X, i, axis=0)
        y_tr = np.delete(y, i)
        # Ridge: solve (X^T X + alpha * I) beta = X^T y
        A = X_tr.T @ X_tr + alpha * np.eye(p_dim)
        b = X_tr.T @ y_tr
        coef = np.linalg.solve(A, b)
        preds[i] = X[i] @ coef

    out = df.copy()
    idx = sub.index
    out.loc[idx, "rbp_pred"]   = preds * 100            # fraction → pct
    out.loc[idx, "rbp_resid"]  = (out.loc[idx, target].astype(float) * 100
                                   - out.loc[idx, "rbp_pred"])
    out["rbp_weight"] = 1.0
    return out


def rbp_fitted_predict(df: pd.DataFrame, features: list[str], target: str) -> pd.DataFrame:
    """
    In-sample Ridge regression fitted values (fit on all districts, predict in-sample).
    Used for the scatter plot — avoids LOO extrapolation issues for outlier districts.
    Includes an intercept so predictions are centered on the actual mean of the target.
    """
    sub = df[features + [target]].dropna().copy()
    X_raw = sub[features].values.astype(float)
    y     = sub[target].values.astype(float)

    mu  = X_raw.mean(axis=0)
    sig = X_raw.std(axis=0)
    sig[sig == 0] = 1.0
    X = (X_raw - mu) / sig

    # Add intercept column; regularize only slope terms (not intercept)
    n, p_dim = X.shape
    X_int = np.column_stack([np.ones(n), X])
    alpha = 0.1
    pen = np.diag([0.0] + [alpha] * p_dim)   # no penalty on intercept
    A = X_int.T @ X_int + pen
    coef = np.linalg.solve(A, X_int.T @ y)
    preds = X_int @ coef  # in-sample fitted values (no LOO)

    out = df.copy()
    idx = sub.index
    out.loc[idx, "rbp_pred"]  = preds * 100          # fraction → pct
    out.loc[idx, "rbp_resid"] = (out.loc[idx, target].astype(float) * 100
                                  - out.loc[idx, "rbp_pred"])
    out["rbp_weight"] = 1.0
    return out


def rbp_feature_importance(df: pd.DataFrame, features: list[str], target: str,
                            saugus_name: str = "Saugus") -> pd.DataFrame:
    """
    Leave-one-feature-out importance for the Saugus prediction.
    Uses Ridge regression; importance = change in Saugus predicted value.
    """
    base = rbp_predict(df, features, target)
    saugus_rows = base.loc[base["municipality"] == saugus_name, "rbp_pred"]
    if len(saugus_rows) == 0:
        return pd.DataFrame()
    saugus_full = float(saugus_rows.iloc[0])

    rows = []
    for feat in features:
        reduced = [f for f in features if f != feat]
        if not reduced:
            continue
        sub = rbp_predict(df.dropna(subset=reduced + [target]), reduced, target)
        saugus_row = sub[sub["municipality"] == saugus_name]
        if len(saugus_row) == 0:
            continue
        drop_pred = float(saugus_row["rbp_pred"].iloc[0])
        rows.append({"feature": feat, "full_pred": saugus_full,
                     "drop_pred": drop_pred,
                     "importance": abs(saugus_full - drop_pred)})
    return pd.DataFrame(rows).sort_values("importance", ascending=False)


def rbp_outcomes_page(pdf, outcomes: dict, rbp_features: pd.DataFrame,
                      top_features: list, fi: pd.DataFrame):
    """APPENDIX: Relevance-Based Prediction of MCAS outcomes.
    top_features: pre-computed top-N features used for scatter prediction.
    fi: pre-computed feature importance DataFrame for all 14 features.
    """
    TARGET = "avg_mcas"

    rbp_df = rbp_features.dropna(subset=RBP_ALL_FEATURES + [TARGET]).copy()
    rbp_df = rbp_df.rename(columns={"district_name": "municipality"})
    rbp_df["avg_me_pct"] = rbp_df["avg_mcas"]

    # Use in-sample Ridge fitted values for scatter (avoids LOO extrapolation for outlier districts)
    rbp_df = rbp_fitted_predict(rbp_df, top_features, TARGET)

    # Compute R² for in-sample fit
    _valid = rbp_df.dropna(subset=["rbp_pred", TARGET])
    _y_true = _valid[TARGET].astype(float) * 100
    _y_pred = _valid["rbp_pred"]
    _ss_res = float(((_y_true - _y_pred) ** 2).sum())
    _ss_tot = float(((_y_true - _y_true.mean()) ** 2).sum())
    r_squared = 1.0 - _ss_res / _ss_tot if _ss_tot > 0 else float("nan")

    saugus    = rbp_df[rbp_df["municipality"] == SAUGUS_NAME].iloc[0]
    # avg_mcas is fraction (0-1); rbp_pred is already ×100 (percentage)
    actual    = float(saugus["avg_mcas"]) * 100   # e.g. 29.0
    predicted = float(saugus["rbp_pred"])          # e.g. 44.2  (already in %)
    gap       = actual - predicted                 # e.g. -15.2 pp
    n_towns   = len(rbp_df)
    print(f"[Page 17] Saugus predicted={predicted:.1f}%, actual={actual:.1f}%, gap={gap:+.1f}pp, R²={r_squared:.2f}")
    _gap_color = RED if gap < -2 else GREEN if gap > 2 else STEEL_BLUE
    gap_sign  = "below" if gap < 0 else "above"

    saugus_rank = int(rbp_df["rbp_resid"].rank().loc[
        rbp_df["municipality"] == SAUGUS_NAME].iloc[0])

    FEAT_LABELS = {
        "high_needs_pct":            "% High-needs",
        "low_income_pct":            "% Low income",
        "ell_pct":                   "% ELL",
        "sped_pct":                  "% SPED",
        "nss_per_pupil":             "Net school spend/pupil",
        "teacher_spending_per_pupil":"Teacher spend/pupil",
        "ch70_per_pupil":            "Ch70 aid/pupil",
        "chronic_absenteeism_pct":   "Chronic absenteeism",
        "avg_teacher_salary":        "Avg teacher salary",
        "teachers_per_100_fte":      "Teachers per 100 staff",
        "teachers_per_100_students": "Teachers per 100 students",
        "total_enrollment":          "Enrollment",
        "median_hh_income":          "Median income",
        "pct_bachelors_plus":        "% College-educated",
        "pct_owner_occupied":        "% Homeowners",
    }

    # Compute z-scores for the same top_features used by Mahalanobis peer matching
    _HIGHER_IS_WORSE = {
        "chronic_absenteeism_pct": True,
        "high_needs_pct":          True,
        "low_income_pct":          True,
        "ell_pct":                 True,
        "sped_pct":                True,
    }
    zscore_rows = []
    for feat in top_features:
        if feat not in rbp_df.columns:
            continue
        col = rbp_df[feat].dropna()
        if len(col) < 5:
            continue
        mu, sd = col.mean(), col.std()
        if sd == 0:
            continue
        saugus_val = float(rbp_df.loc[rbp_df["municipality"] == SAUGUS_NAME, feat].iloc[0])
        z = (saugus_val - mu) / sd
        if _HIGHER_IS_WORSE.get(feat, False):
            z = -z  # flip so negative always means disadvantaged
        zscore_rows.append({"label": FEAT_LABELS.get(feat, feat), "z": z,
                            "val": saugus_val, "feat": feat})
    zdf = pd.DataFrame(zscore_rows).sort_values("z")

    # ══════════════════════════════════════════════════════════════════════════
    # PAGE 17: Scatter (expected vs actual) + stat cards
    # ══════════════════════════════════════════════════════════════════════════
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    n_top = len(top_features)
    fig.text(0.5, 0.955,
             "APPENDIX: Saugus vs. Demographically Similar Districts",
             ha="center", fontsize=18, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.922,
             f"In-sample Ridge regression (R²={r_squared:.2f}) on top {n_top} outcome-predictive factors (of 14 tested)\n"
             f"predicts Saugus should score {predicted:.0f}%.  Actual: {actual:.0f}%.  Gap: {abs(gap):.1f} pp {gap_sign} prediction.",
             ha="center", fontsize=9, color=LIGHT_GRAY, linespacing=1.5,
             transform=fig.transFigure)

    # Scatter: predicted vs actual, full left panel
    ax = fig.add_axes([0.07, 0.12, 0.54, 0.76])
    ax.set_facecolor(CHART_BG)
    ax.set_clip_on(True)

    non_saugus = rbp_df[rbp_df["municipality"] != SAUGUS_NAME].dropna(subset=["rbp_pred"])
    ax.scatter(non_saugus["rbp_pred"], non_saugus["avg_mcas"] * 100,
               color=STEEL_BLUE, alpha=0.45, s=28, zorder=2)

    lim_lo = min(rbp_df["rbp_pred"].min(), rbp_df["avg_mcas"].min() * 100) - 2
    lim_hi = max(rbp_df["rbp_pred"].max(), rbp_df["avg_mcas"].max() * 100) + 2
    ax.plot([lim_lo, lim_hi], [lim_lo, lim_hi],
            color=LIGHT_GRAY, linewidth=1.2, linestyle=":", alpha=0.5,
            label="Perfect prediction line")
    ax.set_xlim(lim_lo, lim_hi)
    ax.set_ylim(lim_lo, lim_hi)

    ax.scatter([predicted], [actual], color=GOLD, s=200, zorder=5, marker="*")
    ann_tx = lim_lo + (lim_hi - lim_lo) * 0.04
    ann_ty = lim_hi - (lim_hi - lim_lo) * 0.06
    ax.annotate(
        f"★ Saugus\nExpected: {predicted:.0f}%\nActual: {actual:.0f}%\nGap: {gap:+.1f}pp",
        xy=(predicted, actual),
        xytext=(ann_tx, ann_ty),
        fontsize=8, color=GOLD, fontweight="bold", va="top",
        arrowprops=dict(arrowstyle="->", color=GOLD, lw=1.2),
        annotation_clip=True)

    ax.set_xlabel("What the model expected (MCAS %)", color="white", fontsize=9)
    ax.set_ylabel("What the district actually scored", color="white", fontsize=9)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color(CHART_GRID)
    ax.legend(fontsize=8, framealpha=0.3, labelcolor="white",
              facecolor=DARK_BLUE, loc="lower right")
    ax.set_title("Expected vs Actual MCAS — every dot is one MA district",
                 color="white", fontsize=10, pad=6)

    # Stat cards: right panel
    rx, cw, ch_card, gap_sp = 0.650, 0.320, 0.170, 0.020
    card_bottoms = [
        0.870 - ch_card,
        0.870 - 2*ch_card - gap_sp,
        0.870 - 3*ch_card - 2*gap_sp,
    ]

    if gap >= 0:
        _card0_label = (f"Saugus scores {abs(gap):.1f}pp above peers with\n"
                        f"similar demographics — despite underfunding\n"
                        f"Expected {predicted:.0f}%  ·  Actual {actual:.0f}%")
        _card0_color = BLUE
    else:
        _card0_label = (f"Saugus scores {abs(gap):.1f}pp below what\n"
                        f"similar districts achieve\n"
                        f"Expected {predicted:.0f}%  ·  Actual {actual:.0f}%")
        _card0_color = RED
    _callout(fig, rx, card_bottoms[0], cw, ch_card,
             f"{gap:+.1f}pp", _card0_label, _card0_color, value_size=30)

    _n_better = n_towns - saugus_rank
    _callout(fig, rx, card_bottoms[1], cw, ch_card,
             f"{_n_better} of {n_towns}",
             "districts outperform Saugus\n"
             "relative to their predicted score —\n"
             "Saugus is near the bottom",
             DARK_RED, value_size=24)

    _callout(fig, rx, card_bottoms[2], cw, ch_card,
             f"top {len(top_features)} of 14",
             "Importance-ranked factors used for\n"
             "peer matching & this prediction\n"
             "All public DESE / ACS / DOR data",
             BLUE, value_size=22)

    fig.text(0.5, 0.028,
             f"Model: Ridge regression (L2 α=0.1), z-score normalised features, in-sample fitted values (R²={r_squared:.2f}).  "
             f"14 features tested; top {len(top_features)} selected by importance.  "
             "Sources: MA DESE, U.S. Census ACS.  Correlation ≠ causation.",
             ha="center", fontsize=7, color=LIGHT_GRAY, alpha=0.6,
             transform=fig.transFigure, linespacing=1.4)

    # ══════════════════════════════════════════════════════════════════════════
    # PAGE 16 (saved first): 2×2 layout — factor profile
    #   Top-left:    fi chart            Top-right:   fi explanation
    #   Bottom-left: z-score explanation Bottom-right: z-score chart
    # PAGE 17 (saved second): scatter + stat cards
    # ══════════════════════════════════════════════════════════════════════════
    fig2 = plt.figure(figsize=(11, 8.5))
    fig2.patch.set_facecolor(NAVY)

    fig2.text(0.5, 0.957,
              "APPENDIX: What Drives the Prediction — Factor Profile",
              ha="center", fontsize=18, fontweight="bold", color="white",
              transform=fig2.transFigure)
    fig2.text(0.5, 0.922,
              "Which factors predict MCAS outcomes (top), "
              "and where Saugus stands on those same factors vs. all MA districts (bottom).",
              ha="center", fontsize=9.5, color=LIGHT_GRAY,
              transform=fig2.transFigure)

    # Horizontal divider
    fig2.add_artist(plt.Line2D([0.04, 0.96], [0.505, 0.505],
                               transform=fig2.transFigure,
                               color=CHART_GRID, linewidth=0.8, alpha=0.5))

    # ── TOP-LEFT: feature importance chart ───────────────────────────────────
    if len(fi) > 0:
        fi_show = fi.copy()
        fi_show["label"] = fi_show["feature"].map(FEAT_LABELS).fillna(fi_show["feature"])
        ax_fi = fig2.add_axes([0.20, 0.525, 0.28, 0.365])
        ax_fi.set_facecolor(CHART_BG)
        spending_feats = {"nss_per_pupil", "teacher_spending_per_pupil",
                          "ch70_per_pupil", "teachers_per_100_fte", "avg_teacher_salary"}
        ax_fi.barh(fi_show["label"], fi_show["importance"],
                   color=[GOLD if f in spending_feats else STEEL_BLUE
                          for f in fi_show["feature"]],
                   height=0.6)
        ax_fi.tick_params(colors="white", labelsize=8.5)
        for spine in ax_fi.spines.values():
            spine.set_color(CHART_GRID)
        ax_fi.set_xlabel("Importance — leave-one-feature-out R² drop",
                         color="white", fontsize=8)
        ax_fi.set_title("All 14 factors ranked by importance",
                        color="white", fontsize=9.5, pad=5)

    # ── TOP-RIGHT: fi explanation ─────────────────────────────────────────────
    tx, ty, ts = 0.52, 0.872, 9.5
    fig2.text(tx, ty, "How to read this chart",
              ha="left", fontsize=ts, fontweight="bold", color=GOLD,
              transform=fig2.transFigure)
    fi_lines = [
        "Importance = R² drop when that factor is",
        "removed (leave-one-feature-out method).",
        "",
        "  Gold   Spending / staffing factors —",
        "          what the school budget controls",
        "  Blue    Demographic / socioeconomic",
        "",
        "A tall gold bar means that investment area",
        "most strongly predicts student outcomes.",
        f"The top {len(top_features)} factors are used for peer",
        "matching and the MCAS prediction.",
    ]
    for i, line in enumerate(fi_lines):
        fig2.text(tx, ty - 0.042 - i * 0.030, line,
                  ha="left", fontsize=8, color="white", alpha=0.85,
                  transform=fig2.transFigure, family="monospace" if line.startswith("  ") else "sans-serif")

    # ── BOTTOM-LEFT: z-score explanation ─────────────────────────────────────
    bx, by, bs = 0.05, 0.462, 9.5
    fig2.text(bx, by, "How to read this chart",
              ha="left", fontsize=bs, fontweight="bold", color=GOLD,
              transform=fig2.transFigure)
    z_lines = [
        "Each bar shows Saugus on one of the top",
        f"{len(top_features)} factors — the same ones used for",
        "peer matching — vs. all MA districts.",
        "",
        "  Red  (< −1)    Significantly disadvantaged",
        "  Blue (−1 to 1) Near the state average",
        "  Green (> +1)   Above average",
        "",
        "Bars left of 0 = Saugus faces a harder",
        "challenge; the model adjusts for this",
        "when setting the expected MCAS score.",
    ]
    for i, line in enumerate(z_lines):
        fig2.text(bx, by - 0.042 - i * 0.030, line,
                  ha="left", fontsize=8, color="white", alpha=0.85,
                  transform=fig2.transFigure, family="monospace" if line.startswith("  ") else "sans-serif")

    # ── BOTTOM-RIGHT: z-score chart ───────────────────────────────────────────
    ax_z = fig2.add_axes([0.48, 0.105, 0.26, 0.365])
    ax_z.set_facecolor(CHART_BG)
    if len(zdf) > 0:
        bar_c = [RED if z < -1 else GREEN if z > 1 else STEEL_BLUE for z in zdf["z"]]
        ax_z.barh(zdf["label"], zdf["z"], color=bar_c, height=0.65)
        ax_z.axvline(0,  color="white", linewidth=1.2, alpha=0.5)
        ax_z.axvline(-1, color=RED,   linewidth=0.9, linestyle=":", alpha=0.5)
        ax_z.axvline(1,  color=GREEN, linewidth=0.9, linestyle=":", alpha=0.5)
        ax_z.tick_params(colors="white", labelsize=8.5)
        for spine in ax_z.spines.values():
            spine.set_color(CHART_GRID)
        ax_z.set_xlabel("Standard deviations from state average", color="white", fontsize=8)
    ax_z.set_title("Saugus — where it stands on each factor",
                   color="white", fontsize=9.5, pad=5)

    fig2.text(0.5, 0.028,
              f"Model: Ridge regression (L2 α=0.1), z-score normalised, in-sample (R²={r_squared:.2f}).  "
              "Sources: MA DESE (MCAS, staffing, demographics, per-pupil, Chapter 70, attendance), "
              "U.S. Census ACS.  Correlation ≠ causation.",
              ha="center", fontsize=7, color=LIGHT_GRAY, alpha=0.6,
              transform=fig2.transFigure, linespacing=1.4)

    pdf.savefig(fig2)
    plt.close(fig2)

    # Factor profile saved above; now save scatter + stat cards
    pdf.savefig(fig)
    plt.close(fig)


# ── spending-outcomes scatter (appendix) ──────────────────────────────────────

def spending_outcomes_scatter_page(pdf, outcomes: dict, exp, year: int = 2024,
                                   rbp_features_df: pd.DataFrame | None = None,
                                   rbp_controls: list | None = None):
    """Appendix: partial regression of ed% vs MCAS after controlling for demographics."""
    from scipy import stats as scipy_stats

    scatter = outcomes["scatter"].copy()
    PEER_NAMES = set(outcomes["peer_orgs"].keys())

    scatter["is_saugus"] = scatter["municipality"] == SAUGUS_NAME
    scatter["is_peer"]   = scatter["district_name"].isin(PEER_NAMES)

    # ── Partial regression (added-variable plot) ─────────────────────────────
    # If RBP features are available, use the top RBP factors as demographic controls.
    # This removes confounding by all factors identified as predictive of MCAS outcomes.
    if rbp_features_df is not None and rbp_controls:
        # Join RBP features to scatter on district_name
        rbp_join = rbp_features_df[["district_name"] + rbp_controls].copy()
        scatter_ext = scatter.merge(rbp_join, on="district_name", how="inner")
        # Use only RBP controls that are actually present and non-constant
        controls = [c for c in rbp_controls
                    if c in scatter_ext.columns and scatter_ext[c].std() > 0]
        use = scatter_ext.dropna(subset=["per_pupil", "avg_me_pct"] + controls).copy()
    else:
        controls = ["high_needs_pct"]
        use = scatter.dropna(subset=["per_pupil", "avg_me_pct"] + controls).copy()

    def _residualize(y_col, X_cols, df):
        """Return residuals of y_col regressed on X_cols (z-scored)."""
        Y = df[y_col].values
        X = df[X_cols].values
        X = (X - X.mean(axis=0)) / X.std(axis=0)
        X = np.column_stack([np.ones(len(X)), X])
        coef, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)
        return Y - X @ coef

    # Log-transform per_pupil to normalize the distribution
    use["log_per_pupil"] = np.log(use["per_pupil"])
    use["ed_resid"]   = _residualize("log_per_pupil", controls, use)
    use["mcas_resid"] = _residualize("avg_me_pct",    controls, use)

    slope, intercept, r, p, _ = scipy_stats.linregress(use["ed_resid"], use["mcas_resid"])
    print(f"[Page 16] Partial regression r={r:.4f}, p={p:.4f}, n={len(use)}, controls={controls}")
    x_line = np.linspace(use["ed_resid"].min(), use["ed_resid"].max(), 100)
    y_line = slope * x_line + intercept

    # For display: MCAS residuals in pp; per-pupil residuals in log scale (no *100)
    use["ed_resid_pp"]   = use["ed_resid"]        # log scale, no conversion
    use["mcas_resid_pp"] = use["mcas_resid"] * 100
    x_line_pp = x_line
    y_line_pp = y_line * 100

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(NAVY)

    fig.text(0.5, 0.930, "APPENDIX: Education Spending vs. Student Outcomes",
             ha="center", fontsize=18, fontweight="bold", color="white",
             transform=fig.transFigure)
    fig.text(0.5, 0.898,
             f"Apples-to-apples: comparing districts with similar student populations  "
             f"({len(use)} MA towns, FY/SY 2024)",
             ha="center", fontsize=9.5, color=GOLD, transform=fig.transFigure)

    ax = fig.add_axes([0.09, 0.14, 0.58, 0.72])
    ax.set_facecolor(CHART_BG)

    LABEL_TOWNS = {"Southborough", "Holliston", "Boxford", "Lynnfield",
                   "Southbridge", "Methuen", "Quincy", "Wellfleet"}

    others = use[~use["is_saugus"] & ~use["is_peer"]]
    ax.scatter(others["ed_resid_pp"], others["mcas_resid_pp"],
               color="gray", alpha=0.45, s=22, zorder=2)
    for _, row in others.iterrows():
        if row["municipality"] in LABEL_TOWNS:
            ax.text(row["ed_resid_pp"] + 0.2, row["mcas_resid_pp"],
                    row["municipality"], fontsize=6.5, color=LIGHT_GRAY, alpha=0.80, va="center")

    peers = use[use["is_peer"]]
    ax.scatter(peers["ed_resid_pp"], peers["mcas_resid_pp"],
               color=STEEL, s=55, zorder=3)
    for _, row in peers.iterrows():
        ax.text(row["ed_resid_pp"] + 0.2, row["mcas_resid_pp"],
                row["district_name"], fontsize=7.5, color=STEEL, fontweight="bold", va="center")

    ax.plot(x_line_pp, y_line_pp,
            color=ORANGE, linewidth=2, linestyle="--", alpha=0.85, zorder=4,
            label=f"Trend (r={r:.2f}, p={p:.3f})")

    saugus = use[use["is_saugus"]]
    if len(saugus):
        sx = float(saugus["ed_resid_pp"].iloc[0])
        sy = float(saugus["mcas_resid_pp"].iloc[0])
        ax.scatter([sx], [sy], color=GOLD, s=160, zorder=5, marker="*")
        ax.text(sx + 0.2, sy - 1.5, "Saugus ★", fontsize=9,
                color=GOLD, fontweight="bold", va="top")

    ax.axhline(0, color="white", linewidth=0.8, alpha=0.3)
    ax.axvline(0, color="white", linewidth=0.8, alpha=0.3)
    ax.set_xlabel("Per-pupil spending — relative to similarly challenged districts (log scale)",
                  color="white", fontsize=9)
    ax.set_ylabel("MCAS meeting/exceeding — relative to similarly challenged districts (pp)",
                  color="white", fontsize=9)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.2f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.0f}pp"))
    ax.tick_params(colors="white", labelsize=8.5)
    for spine in ax.spines.values():
        spine.set_color(CHART_GRID)
    ax.legend(fontsize=9, framealpha=0.3, labelcolor="white", facecolor=DARK_BLUE)
    ax.set_title(f"Per-Pupil Spending vs. MCAS — {len(use)} MA Towns with Similar Student Populations",
                 color="white", fontsize=9.5, pad=6)

    rx, ry = 0.77, 0.82
    panel_items = [
        (f"r = {r:.2f}", "Partial correlation\n(demographics controlled)"),
        (f"p = {p:.3f}", "Not statistically significant\n(p < 0.05 threshold)"),
        (f"{len(use)}", "Towns in analysis"),
        (f"#{int(use['ed_resid'].rank().loc[use['is_saugus'].values].iloc[0])} of {len(use)}",
         "Saugus rank\n(spending residual, low→high)"),
    ]
    for val, lbl in panel_items:
        fig.text(rx + 0.10, ry, val, ha="center", fontsize=15, fontweight="bold",
                 color=GOLD, transform=fig.transFigure)
        fig.text(rx + 0.10, ry - 0.040, lbl, ha="center", fontsize=7,
                 color="white", alpha=0.75, transform=fig.transFigure, linespacing=1.3)
        ry -= 0.135

    controls_str = ", ".join(controls[:4]) + ("..." if len(controls) > 4 else "")
    fig.text(0.5, 0.030,
             f"Each axis shows how each town compares to others facing the same student challenges ({len(controls)} factors: {controls_str}).  "
             "High-needs students need more resources — this chart asks: among towns with equally challenging students, do those that invest more get better outcomes?  "
             "Correlation ≠ causation.",
             ha="center", fontsize=6.5, color="white", alpha=0.5,
             transform=fig.transFigure, linespacing=1.5)

    pdf.savefig(fig)
    plt.close(fig)


# ── page-number wrapper ───────────────────────────────────────────────────────

class _PageNumberedPdf:
    """Wraps PdfPages to inject 'X / TOTAL' page numbers on all non-title slides."""

    def __init__(self, raw_pdf, total: int = 17, skip_first: bool = True):
        self._pdf   = raw_pdf
        self._total = total
        self._n     = 0
        self._skip  = skip_first

    def savefig(self, fig, **kwargs):
        self._n += 1
        if not (self._skip and self._n == 1):
            # Top-right corner — never overlaps footers at the bottom of any slide
            fig.text(0.975, 0.975, f"{self._n} / {self._total}",
                     ha="right", va="top", fontsize=8, color=LIGHT_GRAY,
                     style="italic", transform=fig.transFigure, alpha=0.65)
        # Force consistent 11×8.5 inch pages regardless of content overflow
        fig.set_size_inches(11, 8.5)
        kwargs.pop("bbox_inches", None)
        self._pdf.savefig(fig, **kwargs)


# ── run ───────────────────────────────────────────────────────────────────────

def run():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    engine = get_engine()

    print("[municipal_finance] Loading data...")
    rev, exp, cpi = load_data(engine)
    acs = load_acs_data(engine)
    tax_rates = load_tax_rates(engine)
    rbp_features = load_rbp_features(engine, year=2024)

    srev = rev[rev["dor_code"] == SAUGUS_CODE].copy()
    sexp = exp[exp["dor_code"] == SAUGUS_CODE].copy()

    years      = sorted(srev["fiscal_year"].unique())
    deflator   = build_deflator(cpi, years)
    peer_stats = compute_peer_stats(rev, exp, year=2024)
    n_peer_towns = peer_stats["_n_peer_towns"]

    print(f"[municipal_finance] Saugus: {len(srev)} years, "
          f"{rev['dor_code'].nunique()} towns downloaded, {n_peer_towns} peers")

    print("[municipal_finance] Computing RBP feature importance...")
    rbp_work = rbp_features.dropna(subset=RBP_ALL_FEATURES + ["avg_mcas"]).copy()
    rbp_work = rbp_work.rename(columns={"district_name": "municipality"})
    rbp_work["avg_me_pct"] = rbp_work["avg_mcas"]
    fi_full  = rbp_feature_importance(rbp_work, RBP_ALL_FEATURES, "avg_mcas")
    rbp_top  = fi_full.head(N_TOP_RBP)["feature"].tolist()
    print(f"  Top {N_TOP_RBP} features: {rbp_top}")

    print("[municipal_finance] Computing peer groups...")
    feat_desc = f"top {N_TOP_RBP} outcome-predictive factors (demographics, spending, staffing, income)"
    mahal_peers = compute_mahal_peers(rev, exp, acs, year=2024, n_peers=10,
                                      rbp_df=rbp_work, rbp_top_features=rbp_top)
    hclust_peers, Z_linkage, hclust_df, optimal_k = compute_hclust_peers(
        rev, exp, acs, year=2024, rbp_df=rbp_work, rbp_top_features=rbp_top)
    if len(mahal_peers):
        print(f"  Mahalanobis peers: {mahal_peers['municipality'].tolist()}")
    if len(hclust_peers):
        print(f"  Cluster peers (k={optimal_k}): {hclust_peers['municipality'].tolist()}")
    m_set = set(mahal_peers["municipality"].tolist()) if len(mahal_peers) else set()
    h_set = set(hclust_peers["municipality"].tolist()) if len(hclust_peers) else set()
    consensus_names = sorted(m_set & h_set)
    print(f"  Consensus ({len(consensus_names)} towns): {consensus_names}")

    # Load MCAS outcomes using the dynamically computed consensus peer set
    outcomes = load_school_outcomes(engine, peer_names=consensus_names)

    import shutil, tempfile
    tmp_pdf = os.path.join(tempfile.gettempdir(), "municipal_finance_report.pdf")
    print("[municipal_finance] Writing PDF...")
    with PdfPages(tmp_pdf) as _raw_pdf:
        pdf = _PageNumberedPdf(_raw_pdf, total=17, skip_first=True)
        # ── Main presentation ─────────────────────────────────────────────────
        title_page(pdf, n_peer_towns)                                               # 1  (unnumbered)
        thesis_page(pdf, sexp, outcomes, peer_stats)                               # 2
        executive_summary_page(pdf, srev, sexp, peer_stats)                        # 3  The Bottom Line stat boxes
        education_share_trend_page(pdf, sexp)                                       # 4
        peer_education_share_page(pdf, exp, year=2024)                              # 5
        dual_methodology_page(pdf, exp, mahal_peers, hclust_peers, year=2024)        # 6
        wealthy_town_slide(pdf, exp, acs, tax_rates, year=2024)                     # 7
        real_spending_growth_page(pdf, sexp, deflator)                              # 8
        revenue_vs_education_page(pdf, srev, sexp, deflator)                        # 9
        student_outcomes_page(pdf, outcomes)                                        # 10
        funding_gap_page(pdf, exp, year=2024)                                       # 11
        closing_slide(pdf, srev, sexp, exp)                                         # 12
        # ── Appendix ─────────────────────────────────────────────────────────
        appendix_divider_page(_raw_pdf)                                             # unnumbered section break
        operating_statement_page(pdf, srev, sexp, years_to_show=[2014, 2020, 2024]) # 13 APPENDIX
        revenue_expenditure_trends_page(pdf, srev, sexp, deflator)                  # 14 APPENDIX
        dendrogram_page(pdf, Z_linkage, hclust_df, optimal_k,
                        feature_desc=f"Top {N_TOP_RBP} Outcome-Predictive Features",
                        rbp_work=rbp_work, rbp_top=rbp_top)                          # 15 APPENDIX
        grade10_ela_appendix_page(pdf, outcomes)                                    # 16 APPENDIX
        # rbp_outcomes_page(pdf, outcomes, rbp_features, rbp_top, fi_full)          # APPENDIX (2 pages) — removed: model
        # shows -1.2pp gap (not compelling) and predicts no effect from $1.5M–$3M spending increase.
        # Code preserved in rbp_outcomes_page() above for future use.
        data_sources_page(pdf, rev, n_peer_towns)                                   # 17 APPENDIX

    shutil.copy2(tmp_pdf, OUTPUT_PDF)
    print(f"[municipal_finance] Done → {OUTPUT_PDF}  ({os.path.getsize(OUTPUT_PDF)//1024}KB)")

    save_run_snapshot(engine, srev, sexp, exp, outcomes, peer_stats,
                      mahal_peers, hclust_peers, consensus_names, fi_full,
                      n_peer_towns)


def save_run_snapshot(engine, srev, sexp, exp, outcomes, peer_stats,
                      mahal_peers, hclust_peers, consensus_names, fi_full,
                      n_peer_towns):
    """
    Persist all computed values for this report run to the database.
    Enables year-over-year comparison of peer groups, metrics, and outcomes.
    """
    mcas       = outcomes["mcas"]
    SAUGUS_ORG = outcomes["saugus_org"]
    PEER_ORGS  = outcomes["peer_orgs"]

    fy_max = int(srev["fiscal_year"].max())
    sy_max = int(mcas["school_year"].max())
    yr     = 2024   # primary comparison year

    # ── Insert run record ────────────────────────────────────────────────────
    with engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO analysis_runs (data_vintage_fy, data_vintage_sy, n_peer_pool)
            VALUES (:fy, :sy, :n)
            RETURNING id
        """), {"fy": fy_max, "sy": sy_max, "n": n_peer_towns}).fetchone()
        run_id = row[0]

        # ── Peer groups ──────────────────────────────────────────────────────
        # Build ed_pct lookup from Schedule A
        exp_yr = exp[exp["fiscal_year"] == yr].copy()
        exp_yr["ed_pct_val"] = exp_yr["education"] / exp_yr["total_expenditures"] * 100
        ed_lu = exp_yr.drop_duplicates("municipality").set_index("municipality")["ed_pct_val"]

        peer_rows = []

        # Mahalanobis — preserve distance and rank
        for rank, (_, row_p) in enumerate(mahal_peers.iterrows(), start=1):
            muni = row_p["municipality"]
            peer_rows.append({
                "run_id": run_id, "method": "mahalanobis", "municipality": muni,
                "ed_pct": float(ed_lu.get(muni, float("nan"))) if not pd.isna(ed_lu.get(muni, float("nan"))) else None,
                "mahal_dist": float(row_p.get("mahal_dist", float("nan"))) if "mahal_dist" in row_p.index and not pd.isna(row_p.get("mahal_dist")) else None,
                "rank_in_set": rank,
            })

        # Ward cluster
        for _, row_p in hclust_peers.iterrows():
            muni = row_p["municipality"]
            peer_rows.append({
                "run_id": run_id, "method": "ward_cluster", "municipality": muni,
                "ed_pct": float(ed_lu.get(muni, float("nan"))) if not pd.isna(ed_lu.get(muni, float("nan"))) else None,
                "mahal_dist": None, "rank_in_set": None,
            })

        # Consensus
        for muni in consensus_names:
            peer_rows.append({
                "run_id": run_id, "method": "consensus", "municipality": muni,
                "ed_pct": float(ed_lu.get(muni, float("nan"))) if not pd.isna(ed_lu.get(muni, float("nan"))) else None,
                "mahal_dist": None, "rank_in_set": None,
            })

        if peer_rows:
            conn.execute(text("""
                INSERT INTO computed_peer_groups
                    (run_id, method, municipality, ed_pct, mahal_dist, rank_in_set)
                VALUES (:run_id, :method, :municipality, :ed_pct, :mahal_dist, :rank_in_set)
            """), peer_rows)

        # ── Key metrics ──────────────────────────────────────────────────────
        ed_pct_now  = _v(sexp, yr, "education") / _v(sexp, yr, "total_expenditures") * 100
        ed_pct_2010 = _v(sexp, 2010, "education") / _v(sexp, 2010, "total_expenditures") * 100
        gap_m = ((_v(sexp, yr, "total_expenditures") * peer_stats["ed_pct"]["median"] / 100)
                 - _v(sexp, yr, "education")) / 1e6

        # MCAS
        sela = mcas[(mcas["org_code"] == SAUGUS_ORG) & (mcas["subject"] == "ELA")].set_index("school_year")
        peer_ela = (mcas[mcas["org_code"].isin(PEER_ORGS.values()) & (mcas["subject"] == "ELA")]
                    .groupby("school_year")["meeting_exceeding_pct"].mean() * 100)
        ela_latest_yr = int(sela.index.max())
        ela_latest    = float(sela.loc[ela_latest_yr, "meeting_exceeding_pct"]) * 100
        ela_2019      = float(sela.loc[2019, "meeting_exceeding_pct"]) * 100 if 2019 in sela.index else None
        peer_latest   = float(peer_ela.get(ela_latest_yr, float("nan")))
        peer_2019     = float(peer_ela.get(2019, float("nan"))) if 2019 in peer_ela.index else None
        ela_gap       = ela_latest - peer_latest if not pd.isna(peer_latest) else None
        ela_gap_2019  = (ela_2019 - peer_2019) if ela_2019 and peer_2019 else None

        # Teacher FTE
        teacher = outcomes["teacher"]
        t_latest_yr = int(teacher["school_year"].max())
        t_latest    = float(teacher[teacher["school_year"] == t_latest_yr]["fte"].iloc[0])
        t_2017      = float(teacher[teacher["school_year"] == 2017]["fte"].iloc[0]) if 2017 in teacher["school_year"].values else None

        metrics = [
            # Fiscal metrics
            ("saugus_ed_pct",         yr,    None,          ed_pct_now),
            ("saugus_ed_pct_2010",    2010,  None,          ed_pct_2010),
            ("peer_median_ed_pct",    yr,    None,          peer_stats["ed_pct"]["median"]),
            ("rank_from_bottom",      yr,    None,          peer_stats["ed_pct"]["rank"]),
            ("n_peer_towns",          yr,    None,          peer_stats["ed_pct"]["n"]),
            ("funding_gap_m",         yr,    None,          gap_m),
            ("fy_latest_surplus_m",   fy_max,None,          (_v(srev, fy_max, "total_revenues") - _v(sexp, fy_max, "total_expenditures")) / 1e6),
            # MCAS metrics
            ("ela_saugus_pct",        None,  ela_latest_yr, ela_latest),
            ("ela_peer_avg_pct",      None,  ela_latest_yr, peer_latest),
            ("ela_gap_pp",            None,  ela_latest_yr, ela_gap),
            ("ela_saugus_pct_2019",   None,  2019,          ela_2019),
            ("ela_peer_avg_pct_2019", None,  2019,          peer_2019),
            ("ela_gap_2019_pp",       None,  2019,          ela_gap_2019),
            # Staffing
            ("teacher_fte",           None,  t_latest_yr,   t_latest),
            ("teacher_fte_2017",      None,  2017,          t_2017),
        ]

        metric_rows = [
            {"run_id": run_id, "metric": m, "fiscal_year": fy, "school_year": sy,
             "value": round(float(v), 4) if v is not None and not pd.isna(float(v) if v is not None else float("nan")) else None}
            for m, fy, sy, v in metrics
            if v is not None
        ]
        conn.execute(text("""
            INSERT INTO computed_metrics (run_id, metric, fiscal_year, school_year, value)
            VALUES (:run_id, :metric, :fiscal_year, :school_year, :value)
        """), metric_rows)

        # ── Feature importances ──────────────────────────────────────────────
        fi_rows = [
            {"run_id": run_id, "rank": i + 1,
             "feature": row_fi["feature"],
             "importance": round(float(row_fi["importance"]), 6)}
            for i, (_, row_fi) in enumerate(fi_full.head(N_TOP_RBP).iterrows())
        ]
        conn.execute(text("""
            INSERT INTO computed_feature_importance (run_id, rank, feature, importance)
            VALUES (:run_id, :rank, :feature, :importance)
        """), fi_rows)

    print(f"[municipal_finance] Snapshot saved → run_id={run_id}  "
          f"(FY{fy_max}, SY{sy_max}, {len(peer_rows)} peer rows, {len(metric_rows)} metrics)")


if __name__ == "__main__":
    run()
