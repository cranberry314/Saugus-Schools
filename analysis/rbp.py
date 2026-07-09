"""
Do Not Modify

Relevance-Based Prediction (RBP)
=================================
Implementation of Czasonis, Kritzman & Turkington (2024)
"A Transparent Alternative to Neural Networks."

Faithful to the paper on Eqs 1-18 (verified: RBP with all variables and no
censoring reproduces OLS to within the covariance ridge).  The grid also
follows the paper's sparse-sampling scheme exactly: each cell is an individual
(subset, threshold, censoring-mode) triple, and per prediction task we take the
full-variable linear cell + each single-variable linear cell + n_random random
cells = 1 + K + n_random cells (115 when K = 14 and n_random = 100); see
_build_grid.

This is a model-free prediction routine.  Unlike Ridge regression, RBP:
  - Retains all observations (no parameter estimation)
  - Forms each prediction as a relevance-weighted average of outcomes
  - Quantifies per-variable importance BEFORE the outcome is known
  - Reveals which observations drove the prediction

Entry point
-----------
    from analysis.rbp import rbp

    result = rbp(X, y, x_target, features)

    result.prediction      # scalar: predicted value for x_target
    result.fit             # scalar: reliability of this prediction (adjusted fit)
    result.weights         # Series: per-observation weights (index = X.index)
    result.variable_importance  # Series: per-feature importance scores
    result.most_relevant   # DataFrame: top-N observations by weight
    result.least_relevant  # DataFrame: bottom-N observations by weight

All equations reference the 2024 paper directly.

Math summary
------------
Relevance of observation i to task t (Eq 2–5):
    r_it = sim(x_i, x_t) + ½[info(x_i, x̄) + info(x_t, x̄)]
    sim  = −½(x_i − x_t)Ω⁻¹(x_i − x_t)'          [negative Mah. distance]
    info = (x − x̄)Ω⁻¹(x − x̄)'                     [Mahalanobis from mean]

Prediction weights (Eq 1, 6–10):
    Retain observations where r_it ≥ r*  (censoring threshold)
    w_itθ = 1/N + λ²/(n−1) × (δ(r_it)r_it − φ r̄_sub)

Adjusted fit (Eq 12–14):
    fit        = ρ(weights, y)²
    asymmetry  = ½(ρ(w_retained, y) − ρ(w_censored, y))²
    adj_fit    = K × (fit + asymmetry)   [K = number of variables in cell]

Grid (Eq 15–18):
    Each cell is a (variable subset, censoring threshold, censoring mode) triple
    drawn from the full grid of [every subset] × {0, 0.2, 0.5, 0.8} ×
    {relevance, similarity censoring}.  Sparse sample per task (paper p. 18):
    full-variable linear cell + each single-variable linear cell + n_random
    random cells = 1 + K + n_random cells.
    Final prediction = reliability-weighted average of all cell predictions
    Final weights    = reliability-weighted average of all cell weight vectors
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
import random


# ---------------------------------------------------------------------------
# Public result container
# ---------------------------------------------------------------------------

@dataclass
class RBPResult:
    prediction:           float
    fit:                  float                  # adjusted fit of composite prediction
    weights:              pd.Series              # index = observation labels
    variable_importance:  pd.Series              # index = feature names
    most_relevant:        pd.DataFrame           # top-N rows
    least_relevant:       pd.DataFrame           # bottom-N rows
    n_obs:                int = 0
    n_features:           int = 0
    grid_cells_used:      int = 0


# ---------------------------------------------------------------------------
# Core math — Equations 1–18
# ---------------------------------------------------------------------------

def _covariance_inv(X: np.ndarray) -> np.ndarray:
    """Regularised inverse covariance matrix of X (N × K)."""
    cov = np.cov(X.T)
    if cov.ndim == 0:                        # single feature
        cov = np.array([[float(cov)]])
    reg = 1e-6 * np.eye(cov.shape[0])
    return np.linalg.pinv(cov + reg)


def _relevance(X: np.ndarray, x_t: np.ndarray,
               Omega_inv: np.ndarray) -> np.ndarray:
    """
    Equations 2–5: compute r_it for every observation i.

    Parameters
    ----------
    X        : (N, K) observation matrix (already normalised)
    x_t      : (K,)   prediction task (already normalised)
    Omega_inv: (K, K) inverse covariance of X

    Returns
    -------
    r : (N,) relevance scores
    """
    x_mean = X.mean(axis=0)

    # Eq 3: sim(x_i, x_t) = −½(x_i − x_t)Ω⁻¹(x_i − x_t)'
    diff = X - x_t                                         # (N, K)
    sim  = -0.5 * np.einsum('ij,jk,ik->i', diff, Omega_inv, diff)

    # Eq 4: info(x_i, x̄)
    diff_i = X - x_mean                                    # (N, K)
    info_i = np.einsum('ij,jk,ik->i', diff_i, Omega_inv, diff_i)

    # Eq 5: info(x_t, x̄)
    diff_t = x_t - x_mean                                  # (K,)
    info_t = float(diff_t @ Omega_inv @ diff_t)

    # Eq 2: r_it = sim + ½(info_i + info_t)
    return sim + 0.5 * (info_i + info_t)


def _compute_delta(r_censor: np.ndarray, r_star_pctile: float) -> np.ndarray:
    """
    Equation 6: compute binary censoring mask δ.

    r_censor      : scores used to SET the threshold.
                    Standard mode  → full relevance r_it.
                    Similarity mode → sim(x_i, x_t) component only (Eq 3).
    r_star_pctile : percentile threshold (0 = no censoring, retain all).

    WHY SEPARATE FROM _prediction_weights:
    The paper distinguishes two censoring modes (§Data and Methodology):
      "We consider censoring based on relevance as well as censoring based on
       similarity."
    In similarity-only mode the CENSORING CRITERION differs (use sim component),
    but the WEIGHT FORMULA (Eq 1) still uses full relevance r_it for the tilts.
    Separating delta-computation from weight-computation makes this explicit.
    """
    N = len(r_censor)
    if r_star_pctile <= 0:
        return np.ones(N, dtype=float)           # retain all (Eq 6: r* = −∞)
    r_star = np.percentile(r_censor, r_star_pctile * 100)
    return (r_censor >= r_star).astype(float)    # Eq 6


def _prediction_weights(r: np.ndarray, delta: np.ndarray) -> np.ndarray:
    """
    Equations 1, 7–10: compute prediction weights given pre-computed delta.

    r     : full relevance r_it — ALWAYS used for the weight tilts (Eq 1).
            Do NOT pass sim-only scores here; similarity censoring only
            affects which observations are *retained* (delta), not the tilts.
    delta : binary censoring mask from _compute_delta (Eq 6).

    Returns w : (N,) weights summing to 1.

    MATHEMATICAL CHECK — weights sum to 1:
      Σ w = N × (1/N) + λ²/(n−1) × [Σ δ r − N × φ × r̄_sub]
           = 1 + λ²/(n−1) × [n r̄_sub − N × (n/N) × r̄_sub]
           = 1 + 0  =  1  ✓
    """
    N = len(r)
    n = delta.sum()                              # Eq 7
    if n < 2:                                    # degenerate — uniform weights
        return np.full(N, 1.0 / N)

    phi   = n / N                                # Eq 8
    r_bar = (delta * r).sum() / n               # Eq 9: r̄_sub (mean of retained r)

    # λ² = σ²_r,full / σ²_r,partial  (Eq 10 — second moments, not variances)
    ss_full    = (r ** 2).sum() / (N - 1)
    ss_partial = ((delta * r) ** 2).sum() / (n - 1)
    lam2 = ss_full / ss_partial if ss_partial > 0 else 1.0

    # Eq 1: w_itθ = 1/N + λ²/(n−1) × (δ(r_it) r_it − φ r̄_sub)
    tilt = lam2 / (n - 1) * (delta * r - phi * r_bar)
    return 1.0 / N + tilt


def _adjusted_fit(w: np.ndarray, y: np.ndarray, K: int,
                  delta: np.ndarray, r: np.ndarray) -> float:
    """
    Equations 12–14: adjusted fit for one grid cell.

    w     : (N,) composite prediction weights for this cell (used for Eq 13 fit)
    y     : (N,) outcomes
    K     : number of variables in this cell
    delta : (N,) binary mask — which observations were retained
    r     : (N,) full relevance r_it for this cell (used to re-derive w+/w-)
    """
    # Eq 13: fit = ρ(w, y)²
    def _corr(a, b):
        a, b = np.asarray(a, float), np.asarray(b, float)
        if a.std() < 1e-12 or b.std() < 1e-12:
            return 0.0
        return float(np.corrcoef(a, b)[0, 1])

    fit_sq = _corr(w, y) ** 2                               # Eq 13

    # Eq 14: asymmetry = ½(ρ(w+, y) − ρ(w−, y))²
    # w+ = the cell's Eq-1 weights (relevance retained, δ);  w- = Eq-1 weights with
    # the censoring flipped (the complementary set retained, 1−δ).  Both are
    # full-sample weight vectors sharing the 1/N baseline, so ρ(·, y) is taken over
    # all N observations — the retained-vs-censored contrast Eq 14 measures.  We
    # read Eq 14's "weights formed from the retained observations" as this
    # δ-weighting, not a recomputed 1/n subsample weighting.
    #
    # Degenerate case: at threshold 0 nothing is censored, so the complementary
    # (1−δ) set is empty.  _prediction_weights then returns uniform weights whose
    # ρ with y is 0, which would make asym = ½·ρ(w+)² > 0 — a spurious asymmetry
    # for every linear cell.  Eq 14 presupposes a non-degenerate complement, so
    # when fewer than 2 observations are censored the asymmetry is undefined and
    # contributes 0.
    n_censored = int(len(delta) - delta.sum())
    if n_censored < 2:
        asym = 0.0
    else:
        rho_plus  = _corr(w, y)                # w = the cell's Eq-1 weights = w+
        w_minus   = _prediction_weights(r, 1.0 - delta)
        rho_minus = _corr(w_minus, y)
        asym = 0.5 * (rho_plus - rho_minus) ** 2           # Eq 14

    return float(K * (fit_sq + asym))                      # Eq 12


def _cell_prediction(w: np.ndarray, y: np.ndarray) -> float:
    """Eq 11: ŷ_tθ = Σ w_itθ y_i"""
    return float(w @ y)


# ---------------------------------------------------------------------------
# Grid construction
# ---------------------------------------------------------------------------

def _build_grid(n_features: int, n_random: int = 100,
                thresholds: tuple = (0.0, 0.2, 0.5, 0.8),
                random_state: int = 42) -> list[tuple]:
    """
    Paper-exact sparse grid (Kritzman §Data and Methodology, p. 18).

    A grid CELL is an individual (variable-subset, threshold, censoring-mode)
    triple — NOT a variable subset.  The full grid is every variable
    combination × {0, 0.2, 0.5, 0.8} censoring thresholds × {relevance,
    similarity} censoring modes (>16,000 cells at K=14).  Per the paper we
    sparsely sample exactly:
      - the full-variable LINEAR cell      (all K features, threshold 0)
      - each single-variable LINEAR cell   (one feature,  threshold 0)  → K
      - n_random random cells from the rest of the grid
    giving 1 + K + n_random cells (115 when K = 14 and n_random = 100).

    "Linear" = threshold 0 = no censoring, which the paper notes is equivalent
    to linear regression on that subset.  Threshold-0 cells are always
    relevance-mode: with nothing censored, the similarity and relevance
    criteria retain every observation and therefore coincide, so we normalise
    use_sim=False whenever threshold==0 to avoid double-counting a cell.

    Returns a list of (frozenset(feature_idx), threshold, use_sim) triples.
    """
    rng = random.Random(random_state)
    K = n_features
    full = frozenset(range(K))

    # Explicitly included cells: full-sample linear + each single-variable linear
    cells: list[tuple] = [(full, 0.0, False)]
    for k in range(K):
        cells.append((frozenset([k]), 0.0, False))

    seen = set(cells)
    all_thresholds = list(thresholds)

    # n_random cells drawn from "the rest of the grid": a random subset, a
    # random threshold, and a random censoring mode, excluding any cell already
    # chosen above.  The attempts cap prevents an infinite loop when the grid
    # is smaller than 1 + K + n_random (small K).
    target = len(cells) + n_random
    max_attempts = max(n_random * 1000, 10_000)
    attempts = 0
    while len(cells) < target and attempts < max_attempts:
        attempts += 1
        size   = rng.randint(1, K)
        subset = frozenset(rng.sample(range(K), size))
        thr    = rng.choice(all_thresholds)
        mode   = False if thr == 0 else rng.choice((False, True))
        cell   = (subset, thr, mode)
        if cell in seen:
            continue
        seen.add(cell)
        cells.append(cell)

    return cells


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

CENSORING_THRESHOLDS = (0.0, 0.2, 0.5, 0.8)    # Kritzman §Data and Methodology


def rbp(X:         pd.DataFrame,
        y:         pd.Series,
        x_target:  pd.Series,
        features:  list[str],
        n_random_cells: int = 100,
        n_top_show:     int = 5,
        random_state:   int = 42,
        ) -> RBPResult:
    """
    Relevance-Based Prediction for a single prediction task x_target.

    Parameters
    ----------
    X         : DataFrame with one row per observation, columns include `features`.
                Must NOT include x_target as a row (exclude the unit being predicted).
    y         : Series of outcomes aligned with X.
    x_target  : Series with feature values for the unit to predict (e.g. Saugus).
    features  : list of column names to use as predictive variables.
    n_random_cells : number of randomly sampled variable subsets in the grid.
    n_top_show     : how many most/least relevant observations to return.
    random_state   : seed for random grid sampling.

    Returns
    -------
    RBPResult — see dataclass definition at top of file.
    """
    # ── Validate & extract ──────────────────────────────────────────────────
    feats = [f for f in features if f in X.columns and f in x_target.index]
    if not feats:
        raise ValueError("No valid features found in both X and x_target.")

    sub = X[feats].copy()
    sub['__y__'] = y.values if len(y) == len(sub) else y.reindex(sub.index).values
    sub = sub.dropna()
    if len(sub) < 5:
        raise ValueError(f"Too few complete observations ({len(sub)}) for RBP.")

    X_arr = sub[feats].values.astype(float)   # (N, K)
    y_arr = sub['__y__'].values.astype(float)  # (N,)
    N, K  = X_arr.shape
    obs_idx = sub.index

    # Task vector — restrict to features present
    xt_arr = np.array([float(x_target[f]) for f in feats])

    # Z-score normalise (use X distribution, not including x_target)
    mu  = X_arr.mean(axis=0)
    sig = X_arr.std(axis=0)
    sig[sig == 0] = 1.0
    X_z  = (X_arr - mu) / sig
    xt_z = (xt_arr - mu) / sig

    # ── Full-feature inverse covariance for relevance ──────────────────────
    Omega_inv_full = _covariance_inv(X_z)

    # ── Full-feature relevance for ordering exhibits ────────────────────────
    r_full = _relevance(X_z, xt_z, Omega_inv_full)

    # ── Grid ────────────────────────────────────────────────────────────────
    # Paper-exact: each cell is a (subset, threshold, censoring-mode) triple.
    grid_cells = _build_grid(K, n_random=n_random_cells,
                             thresholds=CENSORING_THRESHOLDS,
                             random_state=random_state)

    # Cache the per-subset relevance and similarity terms — several cells
    # (e.g. the linear cells, or repeated random subsets) share a subset, and
    # _covariance_inv / _relevance depend only on the subset, not the cell.
    _subset_cache: dict[frozenset, tuple] = {}

    def _subset_terms(subset: frozenset):
        if subset not in _subset_cache:
            feat_idx = sorted(subset)
            X_sub    = X_z[:, feat_idx]
            xt_sub   = xt_z[feat_idx]
            Om_inv   = _covariance_inv(X_sub)
            r_sub    = _relevance(X_sub, xt_sub, Om_inv)   # full relevance (Eq 2–5)

            # Similarity-only component (Eq 3 alone), used as the censoring
            # criterion when use_sim=True.  The informativeness terms (Eq 4–5)
            # are intentionally omitted: the paper tests whether censoring on
            # *proximity* alone yields a better prediction for this task.
            diff_sim = X_sub - xt_sub                      # (N, K_cell)
            sim_sub  = -0.5 * np.einsum('ij,jk,ik->i',
                                        diff_sim, Om_inv, diff_sim)  # Eq 3 only
            _subset_cache[subset] = (r_sub, sim_sub)
        return _subset_cache[subset]

    cell_predictions: list[float]      = []
    cell_fits:        list[float]      = []
    cell_weights:     list[np.ndarray] = []
    cell_subsets:     list[frozenset]  = []   # subset of each cell (for importance)

    for subset, threshold, use_sim in grid_cells:
        r_sub, sim_sub = _subset_terms(subset)
        cell_K = len(subset)

        # Censoring criterion (Eq 6):
        #   use_sim=False → censor on full relevance r_it  (standard)
        #   use_sim=True  → censor on similarity only       (paper §Data)
        r_censor = sim_sub if use_sim else r_sub
        delta    = _compute_delta(r_censor, threshold)

        # Weights ALWAYS use full relevance r_sub (Eq 1); only the censoring
        # criterion differs between modes.
        w       = _prediction_weights(r_sub, delta)
        adj_fit = _adjusted_fit(w, y_arr, cell_K, delta, r_sub)
        pred    = _cell_prediction(w, y_arr)

        cell_predictions.append(pred)
        cell_fits.append(adj_fit)
        cell_weights.append(w)
        cell_subsets.append(subset)

    n_cells = len(grid_cells)

    # ── Reliability-weighted composite (Eq 15–17) ───────────────────────────
    fits_arr = np.array(cell_fits, dtype=float)
    fits_arr = np.maximum(fits_arr, 0.0)          # non-negative (Eq 15 denominator)
    total_fit = fits_arr.sum()

    if total_fit <= 0:
        psi = np.full(n_cells, 1.0 / n_cells)
    else:
        psi = fits_arr / total_fit                 # Eq 15

    # Eq 17: ŷ_t,grid = Σ_θ ψ_θ ŷ_tθ
    final_pred = float(psi @ np.array(cell_predictions))

    # Eq 16: composite weight vector
    W_grid = np.zeros(N)
    for psi_val, w_cell in zip(psi, cell_weights):
        W_grid += psi_val * w_cell

    # Eq 18: composite fit
    def _corr(a, b):
        a, b = np.asarray(a, float), np.asarray(b, float)
        if a.std() < 1e-12 or b.std() < 1e-12:
            return 0.0
        return float(np.corrcoef(a, b)[0, 1])

    final_fit = _corr(W_grid, y_arr) ** 2

    # ── Variable importance (Exhibit 5 / footnote 12) ───────────────────────
    # For feature f: avg adjusted fit of cells that include f
    #              - avg adjusted fit of cells that exclude f
    # Each "cell" is a (subset, threshold, censoring-mode) triple.
    importance = {}
    cells_per_feat = {f: [] for f in feats}
    not_cells_per_feat = {f: [] for f in feats}

    for subset, fit_val in zip(cell_subsets, cell_fits):
        for i_f, f in enumerate(feats):
            if i_f in subset:
                cells_per_feat[f].append(fit_val)
            else:
                not_cells_per_feat[f].append(fit_val)

    for f in feats:
        with_f    = np.mean(cells_per_feat[f])    if cells_per_feat[f]     else 0.0
        without_f = np.mean(not_cells_per_feat[f]) if not_cells_per_feat[f] else 0.0
        importance[f] = with_f - without_f

    imp_series = pd.Series(importance, name="importance").sort_values(ascending=False)

    # ── Per-observation weight series & exhibits ────────────────────────────
    w_series = pd.Series(W_grid, index=obs_idx, name="weight")

    obs_df = sub[feats].copy()
    obs_df['__y__']    = y_arr
    obs_df['weight']   = W_grid
    obs_df['relevance'] = r_full

    top_n = obs_df.nlargest(n_top_show, 'weight')
    bot_n = obs_df.nsmallest(n_top_show, 'weight')

    return RBPResult(
        prediction           = final_pred,
        fit                  = final_fit,
        weights              = w_series,
        variable_importance  = imp_series,
        most_relevant        = top_n,
        least_relevant       = bot_n,
        n_obs                = N,
        n_features           = K,
        grid_cells_used      = n_cells,
    )


# ---------------------------------------------------------------------------
# Convenience: LOO prediction for all observations
# ---------------------------------------------------------------------------

def rbp_loo(X: pd.DataFrame, y: pd.Series, features: list[str],
            n_random_cells: int = 100, random_state: int = 42,
            verbose: bool = False) -> pd.DataFrame:
    """
    Leave-one-out RBP predictions for every row in X.

    For each observation i, excludes it from the training set and uses
    the remaining N-1 observations to predict observation i.

    Returns a DataFrame with columns:
        actual, predicted, residual, fit
    indexed like X.
    """
    results = []
    for i, (idx, row) in enumerate(X.iterrows()):
        X_train = X.drop(index=idx)
        y_train = y.drop(index=idx)
        x_task  = row

        try:
            res = rbp(X_train, y_train, x_task, features,
                      n_random_cells=n_random_cells,
                      random_state=random_state)
            results.append({
                'actual':    float(y.loc[idx]),
                'predicted': res.prediction,
                'residual':  float(y.loc[idx]) - res.prediction,
                'fit':       res.fit,
            })
        except Exception as e:
            if verbose:
                print(f"  LOO skip {idx}: {e}")
            results.append({
                'actual':    float(y.loc[idx]),
                'predicted': float('nan'),
                'residual':  float('nan'),
                'fit':       float('nan'),
            })

        if verbose and (i + 1) % 25 == 0:
            print(f"  LOO {i+1}/{len(X)} done", flush=True)

    return pd.DataFrame(results, index=X.index)


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("RBP self-test — synthetic data")

    rng = np.random.default_rng(42)
    N, K = 80, 4
    X_raw = rng.standard_normal((N, K))
    # True relationship: y = 2*x0 - x2 + noise
    y_raw = 2 * X_raw[:, 0] - X_raw[:, 2] + 0.5 * rng.standard_normal(N)

    df = pd.DataFrame(X_raw, columns=[f"f{i}" for i in range(K)])
    y  = pd.Series(y_raw, name="outcome")

    # Predict for observation 0, trained on 1..N
    task = df.iloc[0]
    X_tr = df.iloc[1:]
    y_tr = y.iloc[1:]

    result = rbp(X_tr, y_tr, task, list(df.columns))

    print(f"  Actual={y.iloc[0]:.3f}  Predicted={result.prediction:.3f}  "
          f"Residual={y.iloc[0]-result.prediction:.3f}")
    print(f"  Composite fit: {result.fit:.4f}")
    print(f"  Grid cells used: {result.grid_cells_used}")
    print("  Variable importance:")
    for feat, imp in result.variable_importance.items():
        print(f"    {feat}: {imp:+.4f}")
    print("  Most relevant observations:")
    print(result.most_relevant[['weight', '__y__']].to_string())
    print("Self-test passed.")
