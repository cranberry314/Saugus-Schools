"""
peers.py — shared Mahalanobis peer-distance kernel.

One robust implementation, used by the peer-analysis scripts (peer_finder_basic,
peer_analysis_comprehensive, peer_analysis_timeseries) so the distance math lives
in one place instead of three subtly-different copies.

Method: Mahalanobis distance from a base row to every other row, using the
pseudo-inverse of the ridge-regularized feature covariance.  pinv + ridge keeps it
stable when the peer features are collinear (income / poverty / education move
together across MA towns); the clamp guards against sqrt of a tiny-negative
quadratic form (a floating-point artifact) producing NaN.  This is the robust
variant peer_analysis_comprehensive and peer_analysis_timeseries already used;
peer_finder_basic previously used a plain matrix inverse with no clamp and is now
unified onto this.

The caller owns feature selection and NaN handling: pass a fully-filled numeric
matrix (typically NaNs replaced by column medians) indexed by id.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

RIDGE = 1e-6  # added to the covariance diagonal before inversion


def mahalanobis_distances(feat_filled: pd.DataFrame, base_id,
                          feature_cols=None, *, exclude_self: bool = True) -> pd.Series:
    """Mahalanobis distance from ``base_id`` to every row of ``feat_filled``.

    Parameters
    ----------
    feat_filled : DataFrame indexed by id, numeric feature columns, NO NaNs
                  (the caller fills them — usually with the column median).
    base_id     : index label of the base row (e.g. Saugus's org_code).
    feature_cols: subset of columns to use (default: all columns).
    exclude_self: drop the base row from the result (default True).

    Returns
    -------
    A full-precision Series (index = id) sorted ascending by distance.  Callers
    round for display.  Empty Series if no usable columns or base missing.
    """
    cols = list(feature_cols) if feature_cols is not None else list(feat_filled.columns)
    cols = [c for c in cols if c in feat_filled.columns]
    if not cols or base_id not in feat_filled.index:
        return pd.Series(dtype=float)

    X = feat_filled[cols].to_numpy(dtype=float)
    cov = np.cov(X, rowvar=False)
    if cov.ndim == 0:                       # single feature → 1×1 covariance
        cov = np.array([[float(cov)]])
    cov_inv = np.linalg.pinv(cov + RIDGE * np.eye(cov.shape[0]))

    base = feat_filled.loc[base_id, cols].to_numpy(dtype=float)
    diff = X - base                                          # (n, k)
    d2 = np.einsum("ij,jk,ik->i", diff, cov_inv, diff)       # row-wise quadratic form
    dist = np.sqrt(np.clip(d2, 0.0, None))                   # clamp: no sqrt(<0) → NaN

    out = pd.Series(dist, index=feat_filled.index)
    if exclude_self:
        out = out.drop(index=base_id, errors="ignore")
    return out.sort_values()
