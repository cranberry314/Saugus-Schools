"""
Mahalanobis peer analysis through time + feature sensitivity for Saugus.

For each available school year:
  1. Computes full 7-feature Mahalanobis distance to every MA district
  2. Computes 7 leave-one-out distances (drop each feature in turn) to test
     how sensitive the peer rankings are to any single variable

Output: Reports/saugus_peers_timeseries.xlsx
  Sheet "Summary"       — top-5 peers per year + Saugus feature values
  Sheet "Peers_YYYY"    — full ranked list for each year (top 30)
  Sheet "Sensitivity"   — for each year × dropped feature:
                            overlap of top-10 vs full top-10
                            Spearman rank correlation

Run: python analysis/peers_timeseries.py [--district 02620000] [--top 30]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import warnings
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sqlalchemy import text
from config import get_engine
from analysis.peers import fetch_feature_matrix, FEATURE_COLS

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_DISTRICT = "02620000"   # Saugus
YEARS_TO_TRY     = list(range(2017, 2026))   # MCAS available from ~2017
TOP_N            = 30
OUTPUT_DIR       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
OUTPUT_FILE      = os.path.join(OUTPUT_DIR, "saugus_peers_timeseries.xlsx")

FEATURE_LABELS = {
    "total_enrollment": "Total Enrollment",
    "pct_low_income":   "% Low Income",
    "pct_ell":          "% ELL",
    "pct_sped":         "% SPED",
    "ppe_total":        "Per-Pupil Exp ($)",
    "ela_me_pct":       "ELA M+E %",
    "math_me_pct":      "Math M+E %",
}


# ── Core distance helpers ─────────────────────────────────────────────────────

def _mahal_distances(feat_filled: pd.DataFrame, base_vec: np.ndarray,
                     feature_cols: list[str]) -> pd.Series:
    """
    Compute Mahalanobis distances from base_vec to all rows of feat_filled
    using the specified feature columns.  Returns a Series indexed by org_code.
    """
    X = feat_filled[feature_cols].values
    cov = np.cov(X, rowvar=False)
    if cov.ndim == 0:
        cov = np.array([[float(cov)]])
    reg = 1e-6 * np.eye(cov.shape[0])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cov_inv = np.linalg.pinv(cov + reg)
    base = feat_filled[feature_cols].loc[
        feat_filled.index[feat_filled.index == feat_filled.index[0]]
    ].values[0] if False else np.array(
        [feat_filled[feature_cols].iloc[
            feat_filled.index.get_loc(feat_filled.index[0])
        ].values[0]] * len(feature_cols)
    )   # placeholder — overwritten below

    # Build distance for each row
    distances = {}
    for org_code, row in feat_filled[feature_cols].iterrows():
        diff = row.values - base_vec
        dist = float(np.sqrt(max(0.0, diff @ cov_inv @ diff)))
        distances[org_code] = dist
    return pd.Series(distances)


def compute_full_and_loo(df: pd.DataFrame, base_org_code: str,
                         top_n: int = TOP_N):
    """
    For a single year's feature matrix df, compute:
      - full_peers:  DataFrame of all districts ranked by 7-feature distance
      - loo_peers:   dict {dropped_feature -> DataFrame ranked by 6-feature distance}

    Returns (full_df, loo_dict, saugus_features) or None if insufficient data.
    """
    feat = df[FEATURE_COLS].copy()

    # Require at least 4 non-NaN features for the base district
    if base_org_code not in feat.index:
        return None
    base_avail = feat.loc[base_org_code].notna().sum()
    if base_avail < 4:
        print(f"  [skip] Saugus only has {base_avail} features — not enough")
        return None

    # Drop districts with fewer than 4 valid features
    feat = feat.dropna(thresh=4)
    if base_org_code not in feat.index:
        return None

    # Convert to float (None → NaN)
    feat = feat.astype(float)

    # Drop columns that are entirely NaN (e.g. MCAS in COVID year 2020)
    feat = feat.dropna(axis=1, how="all")
    if len(feat.columns) < 2:
        print(f"  [skip] fewer than 2 usable features after dropping all-NaN columns")
        return None

    # Re-check base district still has enough data with remaining columns
    if feat.loc[base_org_code].notna().sum() < 2:
        print(f"  [skip] base district has < 2 features in remaining columns")
        return None

    # Fill NaN with column median for covariance stability
    medians = feat.median()
    feat_filled = feat.copy()
    for col in feat_filled.columns:
        feat_filled[col] = feat_filled[col].fillna(medians[col])
    # If a column's median is also NaN (all-NaN after dropna shouldn't happen, but be safe)
    feat_filled = feat_filled.dropna(axis=1, how="all")

    # Limit FEATURE_COLS to what survived
    active_features = [c for c in FEATURE_COLS if c in feat_filled.columns]

    saugus_features = feat.loc[base_org_code].to_dict()

    def _rank(feature_cols):
        """Return ranked rows (excluding base) using given cols."""
        # Only use cols that exist in feat_filled
        use_cols = [c for c in feature_cols if c in feat_filled.columns]
        if len(use_cols) < 1:
            return pd.DataFrame(columns=["org_code", "mahal_dist", "rank"])
        X    = feat_filled[use_cols].values.astype(float)
        base = feat_filled.loc[base_org_code, use_cols].values.astype(float)
        cov  = np.cov(X, rowvar=False)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        reg     = 1e-6 * np.eye(cov.shape[0])
        cov_inv = np.linalg.pinv(cov + reg)
        rows = []
        for oc, row in feat_filled[feature_cols].iterrows():
            if oc == base_org_code:
                continue
            diff = row.values - base
            dist = float(np.sqrt(max(0.0, diff @ cov_inv @ diff)))
            rows.append({"org_code": oc, "mahal_dist": round(dist, 4)})
        result = (pd.DataFrame(rows)
                    .sort_values("mahal_dist")
                    .reset_index(drop=True))
        result["rank"] = result.index + 1
        return result

    # Full ranking using all active features
    full_ranked = _rank(active_features)

    # Merge in district name/town
    meta = df[["district_name", "town"]].reset_index()
    full_ranked = full_ranked.merge(meta, on="org_code", how="left")

    # Add feature values to full_ranked
    feat_vals = feat.reset_index()
    full_ranked = full_ranked.merge(feat_vals, on="org_code", how="left")

    available_feat_cols = [c for c in FEATURE_COLS if c in full_ranked.columns]
    full_ranked = full_ranked[
        ["rank", "org_code", "district_name", "town", "mahal_dist"] + available_feat_cols
    ]

    # Leave-one-out: drop each feature in turn (only features that are active)
    loo_dict = {}
    for drop_col in active_features:
        sub_cols = [c for c in active_features if c != drop_col]
        loo_ranked = _rank(sub_cols)
        loo_dict[drop_col] = loo_ranked

    return full_ranked, loo_dict, saugus_features


# ── Sensitivity summary ───────────────────────────────────────────────────────

def sensitivity_table(full_ranked: pd.DataFrame, loo_dict: dict,
                      year: int, top_k: int = 10) -> list[dict]:
    """
    For each dropped feature, compute:
      - overlap: how many of the full top-K appear in the LOO top-K
      - spearman: rank correlation between full distances and LOO distances
        for the full set of districts
    Returns a list of dicts, one per dropped feature.
    """
    full_top   = set(full_ranked.head(top_k)["org_code"])
    full_dists = full_ranked.set_index("org_code")["mahal_dist"]

    rows = []
    for feat, loo_df in loo_dict.items():
        loo_top    = set(loo_df.head(top_k)["org_code"])
        overlap    = len(full_top & loo_top)

        # Spearman on common districts
        common     = full_dists.index.intersection(loo_df.set_index("org_code").index)
        if len(common) > 3:
            loo_dists  = loo_df.set_index("org_code").loc[common, "mahal_dist"]
            rho, _     = spearmanr(full_dists.loc[common], loo_dists)
        else:
            rho = float("nan")

        rows.append({
            "year":             year,
            "feature_dropped":  feat,
            "feature_label":    FEATURE_LABELS.get(feat, feat),
            f"top{top_k}_overlap": overlap,
            f"top{top_k}_overlap_pct": round(100 * overlap / top_k, 1),
            "spearman_rho":     round(rho, 3) if not np.isnan(rho) else None,
        })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def run(district: str = DEFAULT_DISTRICT, top_n: int = TOP_N):
    engine  = get_engine()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary_rows   = []   # one row per (year, rank 1-5)
    year_dfs       = {}   # year -> full ranked DataFrame
    sensitivity_rows = [] # one row per (year, dropped_feature)
    saugus_by_year = []   # Saugus feature values per year

    for year in YEARS_TO_TRY:
        print(f"\n[timeseries] Year {year} ...")
        try:
            df = fetch_feature_matrix(engine, year)
        except Exception as e:
            print(f"  [skip] fetch_feature_matrix failed: {e}")
            continue

        result = compute_full_and_loo(df, district, top_n)
        if result is None:
            print(f"  [skip] insufficient data for {district}")
            continue

        full_ranked, loo_dict, saugus_feats = result
        print(f"  {len(full_ranked)} districts ranked; top-3: "
              + ", ".join(full_ranked.head(3)["district_name"].fillna("?").tolist()))

        year_dfs[year] = full_ranked

        # Summary: top-5 for this year
        for _, row in full_ranked.head(5).iterrows():
            summary_rows.append({
                "year":          year,
                "rank":          row["rank"],
                "district_name": row["district_name"],
                "town":          row["town"],
                "mahal_dist":    row["mahal_dist"],
            })

        # Saugus feature values this year
        saugus_by_year.append({"year": year, **{
            FEATURE_LABELS.get(k, k): round(v, 2) if v is not None else None
            for k, v in saugus_feats.items()
        }})

        # Sensitivity
        sens = sensitivity_table(full_ranked, loo_dict, year, top_k=10)
        sensitivity_rows.extend(sens)

    if not year_dfs:
        print("[timeseries] No years had sufficient data — nothing to export.")
        return

    # ── Write Excel ───────────────────────────────────────────────────────────
    print(f"\n[timeseries] Writing {OUTPUT_FILE} ...")

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        wb = writer.book

        # ── Formats ──────────────────────────────────────────────────────────
        hdr_fmt  = wb.add_format({"bold": True, "bg_color": "#2F5496",
                                   "font_color": "white", "border": 1})
        num_fmt  = wb.add_format({"num_format": "#,##0.00", "border": 1})
        int_fmt  = wb.add_format({"num_format": "#,##0",    "border": 1})
        pct_fmt  = wb.add_format({"num_format": "0.0",      "border": 1})
        cell_fmt = wb.add_format({"border": 1})
        year_fmt = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})

        def _write_df(sheet_name, df, col_widths=None):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            ws.set_row(0, 18, hdr_fmt)
            if col_widths:
                for i, w in enumerate(col_widths):
                    ws.set_column(i, i, w)

        # ── Sheet: Summary ────────────────────────────────────────────────────
        summary_df = pd.DataFrame(summary_rows)
        saugus_df  = pd.DataFrame(saugus_by_year)

        # Pivot summary to wide: one row per year, columns = rank 1-5
        pivot = summary_df.pivot_table(
            index="year", columns="rank",
            values="district_name", aggfunc="first"
        ).reset_index()
        pivot.columns = ["Year"] + [f"Peer #{c}" for c in pivot.columns[1:]]

        # Add Saugus features alongside
        merged = pivot.merge(saugus_df, left_on="Year", right_on="year", how="left")
        merged = merged.drop(columns=["year"], errors="ignore")

        _write_df("Summary", merged,
                  col_widths=[8, 28, 28, 28, 28, 28,
                               14, 12, 10, 10, 14, 10, 10])

        # ── Sheet: per year ───────────────────────────────────────────────────
        for year, df_yr in sorted(year_dfs.items()):
            sheet = f"Peers_{year}"
            rename = {c: FEATURE_LABELS.get(c, c) for c in FEATURE_COLS}
            df_out = df_yr.rename(columns=rename)
            _write_df(sheet, df_out,
                      col_widths=[6, 10, 28, 20, 10,
                                   12, 12, 10, 10, 14, 10, 10])

        # ── Sheet: Sensitivity ────────────────────────────────────────────────
        if sensitivity_rows:
            sens_df = pd.DataFrame(sensitivity_rows)
            # Pivot: year as rows, feature as columns, overlap_pct as values
            overlap_col = [c for c in sens_df.columns if "overlap_pct" in c][0]
            pivot_sens = sens_df.pivot_table(
                index="year", columns="feature_label",
                values=overlap_col, aggfunc="first"
            ).reset_index()
            pivot_sens.columns.name = None

            rho_pivot = sens_df.pivot_table(
                index="year", columns="feature_label",
                values="spearman_rho", aggfunc="first"
            ).reset_index()
            rho_pivot.columns.name = None

            # Write overlap sheet
            _write_df("Sensitivity_Overlap_%",
                      pivot_sens,
                      col_widths=[8] + [16] * (len(pivot_sens.columns) - 1))

            _write_df("Sensitivity_SpearmanRho",
                      rho_pivot,
                      col_widths=[8] + [16] * (len(rho_pivot.columns) - 1))

            # Also write the full detail table
            detail_cols = ["year", "feature_label", overlap_col, "spearman_rho"]
            _write_df("Sensitivity_Detail",
                      sens_df[detail_cols].rename(columns={
                          "year": "Year",
                          "feature_label": "Feature Dropped",
                          overlap_col: "Top-10 Overlap %",
                          "spearman_rho": "Spearman ρ",
                      }),
                      col_widths=[8, 22, 18, 14])

    print(f"[timeseries] Done → {OUTPUT_FILE}")
    print(f"[timeseries] Years computed: {sorted(year_dfs.keys())}")
    print(f"[timeseries] Sheets: Summary, "
          + ", ".join(f"Peers_{y}" for y in sorted(year_dfs))
          + ", Sensitivity_Overlap_%, Sensitivity_SpearmanRho, Sensitivity_Detail")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mahalanobis peer analysis through time for a MA district")
    parser.add_argument("--district", default=DEFAULT_DISTRICT,
                        help="Base district org code (default: Saugus 02620000)")
    parser.add_argument("--top", type=int, default=TOP_N,
                        help=f"Peers to include per year (default {TOP_N})")
    args = parser.parse_args()
    run(district=args.district, top_n=args.top)
