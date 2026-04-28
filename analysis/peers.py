"""
Peer district finder using Mahalanobis distance.

Computes similar MA districts to a target (default: Saugus) based on
key demographic and financial metrics. Results are cached in peer_districts table.

Metrics used (all for the same school_year):
  - Total enrollment
  - % Low income
  - % ELL
  - % SPED
  - Per-pupil expenditure (total in-district)
  - MCAS ELA meeting+exceeding % (All students, Grade 10 or most recent available)
  - MCAS Math meeting+exceeding %

Run: python analysis/peers.py [--district 00760000] [--year 2024] [--top 20]
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import numpy as np
import pandas as pd
from scipy.spatial.distance import mahalanobis
from sqlalchemy import text
from config import get_engine

# Default: Saugus MA
DEFAULT_DISTRICT = "02620000"
DEFAULT_YEAR     = 2025
TOP_N            = 20


def fetch_feature_matrix(engine, school_year: int) -> pd.DataFrame:
    """
    Builds a feature matrix from the database: one row per district, columns = metrics.
    Only includes rows with sufficient data (not all NaN).
    """
    sql = text("""
        WITH enroll AS (
            SELECT org_code, SUM(total) AS total_enrollment
            FROM enrollment
            WHERE school_year = :yr
              AND grade = 'Total'
              AND LENGTH(org_code) = 8
              AND org_code NOT LIKE '%0000'  -- exclude district-level aggregates if schools also present
            GROUP BY org_code
            UNION ALL
            SELECT org_code, SUM(total) AS total_enrollment
            FROM enrollment
            WHERE school_year = :yr
              AND grade = 'Total'
              AND org_code LIKE '%0000'
            GROUP BY org_code
        ),
        enroll_deduped AS (
            SELECT DISTINCT ON (org_code) org_code, total_enrollment
            FROM enroll
            ORDER BY org_code, total_enrollment DESC
        ),
        demo AS (
            SELECT
                org_code,
                MAX(CASE WHEN category ILIKE '%low_income%' OR category ILIKE '%economically%' THEN pct END) AS pct_low_income,
                MAX(CASE WHEN category ILIKE '%ell%' OR category ILIKE '%english%learner%' THEN pct END) AS pct_ell,
                MAX(CASE WHEN category ILIKE '%sped%' OR category ILIKE '%disabilit%' THEN pct END) AS pct_sped
            FROM demographics
            WHERE school_year = :yr
            GROUP BY org_code
        ),
        ppe AS (
            SELECT org_code,
                   MAX(CASE WHEN category ILIKE '%in-district%total%' OR category ILIKE '%total%in-district%' THEN amount END) AS ppe_total
            FROM per_pupil_expenditure
            WHERE school_year = :yr
            GROUP BY org_code
        ),
        mcas_ela AS (
            SELECT org_code, AVG(meeting_exceeding_pct) AS ela_me_pct
            FROM mcas_results
            WHERE school_year = :yr
              AND subject ILIKE 'ELA'
              AND student_group = 'All Students'
              AND grade IN ('10', 'ALL (03-08)')
            GROUP BY org_code
        ),
        mcas_math AS (
            SELECT org_code, AVG(meeting_exceeding_pct) AS math_me_pct
            FROM mcas_results
            WHERE school_year = :yr
              AND subject ILIKE 'MATH'
              AND student_group = 'All Students'
              AND grade IN ('10', 'ALL (03-08)')
            GROUP BY org_code
        )
        SELECT
            d.org_code,
            d.name AS district_name,
            d.town,
            e.total_enrollment,
            demo.pct_low_income,
            demo.pct_ell,
            demo.pct_sped,
            ppe.ppe_total,
            ela.ela_me_pct,
            math.math_me_pct
        FROM districts d
        LEFT JOIN enroll_deduped e    ON e.org_code    = d.org_code
        LEFT JOIN demo                ON demo.org_code = d.org_code
        LEFT JOIN ppe                 ON ppe.org_code  = d.org_code
        LEFT JOIN mcas_ela ela        ON ela.org_code  = d.org_code
        LEFT JOIN mcas_math math      ON math.org_code = d.org_code
        WHERE d.is_district = TRUE
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"yr": school_year})

    df = df.set_index("org_code")
    return df


FEATURE_COLS = [
    "total_enrollment",
    "pct_low_income",
    "pct_ell",
    "pct_sped",
    "ppe_total",
    "ela_me_pct",
    "math_me_pct",
]


def compute_peers(df: pd.DataFrame, base_org_code: str, top_n: int = TOP_N) -> pd.DataFrame:
    """
    Computes Mahalanobis distance from base_org_code to all other districts.
    Returns a DataFrame sorted by distance (ascending), with columns:
      peer_org_code, district_name, town, mahalanobis_dist, [feature columns]
    """
    if base_org_code not in df.index:
        raise ValueError(
            f"District {base_org_code} not found in feature matrix. "
            f"Run scrapers first, then check: SELECT * FROM districts WHERE org_code = '{base_org_code}'"
        )

    # Keep only rows with enough non-NaN values
    feat = df[FEATURE_COLS].copy()
    available = feat.notna().any(axis=0).sum()
    thresh = max(1, min(4, available))  # require at least 1, up to 4, features to be present
    print(f"[peers] Features with any data: {available}/7 — using thresh={thresh}")
    feat = feat.dropna(thresh=thresh)

    if base_org_code not in feat.index:
        missing = df.loc[base_org_code, FEATURE_COLS].notna().sum() if base_org_code in df.index else 0
        raise ValueError(
            f"District {base_org_code} has too few features ({missing}/{len(FEATURE_COLS)}) for Mahalanobis distance. "
            f"Check that MCAS, enrollment, demographics, and PPE data are loaded for year {school_year}."
        )

    # Fill remaining NaN with column median for the covariance calculation
    feat_filled = feat.fillna(feat.median(numeric_only=True))

    # Covariance matrix (with regularisation to avoid singularity)
    cov = feat_filled.cov().values
    reg = 1e-6 * np.eye(cov.shape[0])
    cov_inv = np.linalg.inv(cov + reg)

    base_vec = feat_filled.loc[base_org_code].values

    results = []
    for org_code, row in feat_filled.iterrows():
        if org_code == base_org_code:
            continue
        diff = row.values - base_vec
        dist = float(np.sqrt(diff @ cov_inv @ diff))
        results.append({
            "peer_org_code":    org_code,
            "district_name":    df.loc[org_code, "district_name"] if org_code in df.index else None,
            "town":             df.loc[org_code, "town"] if org_code in df.index else None,
            "mahalanobis_dist": round(dist, 6),
            **{col: feat.loc[org_code, col] if org_code in feat.index else None
               for col in FEATURE_COLS},
        })

    peers_df = pd.DataFrame(results).sort_values("mahalanobis_dist").head(top_n)
    peers_df["rank_order"] = range(1, len(peers_df) + 1)
    return peers_df


def save_peers(engine, base_org_code: str, school_year: int, peers_df: pd.DataFrame):
    """Caches peer results in the peer_districts table."""
    with engine.begin() as conn:
        # Clear old results for this base district + year
        conn.execute(text("""
            DELETE FROM peer_districts
            WHERE base_org_code = :base AND school_year = :yr
        """), {"base": base_org_code, "yr": school_year})

        for _, row in peers_df.iterrows():
            conn.execute(text("""
                INSERT INTO peer_districts
                    (school_year, base_org_code, peer_org_code, mahalanobis_dist, rank_order)
                VALUES (:yr, :base, :peer, :dist, :rank)
                ON CONFLICT (school_year, base_org_code, peer_org_code) DO UPDATE SET
                    mahalanobis_dist = EXCLUDED.mahalanobis_dist,
                    rank_order       = EXCLUDED.rank_order,
                    computed_at      = NOW()
            """), {
                "yr":   school_year,
                "base": base_org_code,
                "peer": row["peer_org_code"],
                "dist": row["mahalanobis_dist"],
                "rank": row["rank_order"],
            })
    print(f"[peers] Saved {len(peers_df)} peer districts for {base_org_code} (year={school_year})")


def run(district: str = DEFAULT_DISTRICT, year: int = DEFAULT_YEAR, top_n: int = TOP_N):
    engine = get_engine()
    print(f"[peers] Computing peers for {district}, year={year}, top_n={top_n}")

    df = fetch_feature_matrix(engine, year)
    print(f"[peers] Feature matrix: {len(df)} districts, {df[FEATURE_COLS].notna().sum().to_dict()}")

    # Show the target district's features
    if district in df.index:
        target = df.loc[district]
        print(f"\n[peers] Target district ({district} — {target.get('district_name', '?')}):")
        for col in FEATURE_COLS:
            print(f"  {col:30s} = {target.get(col)}")
    else:
        print(f"[peers] WARNING: {district} not found in feature matrix")

    peers_df = compute_peers(df, district, top_n)

    print(f"\n[peers] Top {top_n} similar districts:")
    print(peers_df[["rank_order", "district_name", "town", "mahalanobis_dist"] + FEATURE_COLS].to_string(index=False))

    save_peers(engine, district, year, peers_df)
    return peers_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute peer districts via Mahalanobis distance")
    parser.add_argument("--district", default=DEFAULT_DISTRICT, help="Base district org code")
    parser.add_argument("--year",     type=int, default=DEFAULT_YEAR, help="School year (ending year)")
    parser.add_argument("--top",      type=int, default=TOP_N, help="Number of peers to return")
    args = parser.parse_args()
    run(district=args.district, year=args.year, top_n=args.top)
