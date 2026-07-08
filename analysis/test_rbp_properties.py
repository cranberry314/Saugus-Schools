"""
test_rbp_properties.py
======================
Independent verification that analysis/rbp.py is a faithful implementation of
Czasonis, Kritzman & Turkington (2024), "A Transparent Alternative to Neural
Networks" (Eqs 1-18 and the paper's stated convergence properties).

Each test re-derives the expected quantity from first principles (numpy /
first-principles Mahalanobis / ordinary least squares) and compares it to what
rbp.py produces — so these are genuine cross-checks, not the implementation
grading its own homework.

Run (self-contained; exits non-zero if any check fails, so it doubles as a CI
gate — no pytest dependency required):
    source .venv/bin/activate
    python analysis/test_rbp_properties.py

Tolerances: rbp.py regularises the covariance inverse with a 1e-6 ridge
(_covariance_inv), so quantities that flow through Ω⁻¹ agree with an
un-regularised reference to ~1e-6, while pure-arithmetic identities agree to
~1e-9.  Tolerances below are set accordingly.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis.rbp import (
    _covariance_inv, _relevance, _compute_delta, _prediction_weights,
    _cell_prediction, _adjusted_fit, _build_grid, rbp, CENSORING_THRESHOLDS,
)

RIDGE_TOL = 1e-4   # quantities that pass through the 1e-6 covariance ridge
EXACT_TOL = 1e-9   # pure-arithmetic identities


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_data(seed=0, N=200, K=4, noise=0.7):
    """Linear DGP with standardised design, returned already z-scored (the
    space rbp.py works in)."""
    rng = np.random.default_rng(seed)
    X = rng.standard_normal((N, K))
    beta = rng.standard_normal(K)
    y = X @ beta + noise * rng.standard_normal(N)
    mu, sig = X.mean(0), X.std(0)
    sig[sig == 0] = 1.0
    Xz = (X - mu) / sig
    return Xz, y


def _ols(Xz, y):
    """Reference OLS (with intercept) on the z-scored design."""
    Xd = np.column_stack([np.ones(len(Xz)), Xz])
    coef, *_ = np.linalg.lstsq(Xd, y, rcond=None)
    yhat = Xd @ coef
    r2 = 1.0 - ((y - yhat) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    return coef, yhat, r2


# ---------------------------------------------------------------------------
# 1. Relevance decomposition — Eqs 2-5
# ---------------------------------------------------------------------------

def test_relevance_decomposition():
    """r_it = sim(x_i,x_t) + ½(info(x_i,x̄)+info(x_t,x̄));  sim = -½ d_M²;
    info = d_M² from the mean.  Verified against a first-principles Mahalanobis
    using the SAME Ω⁻¹ (so this isolates the algebra, not the ridge)."""
    Xz, _ = _make_data(seed=1, N=150, K=5)
    Om = _covariance_inv(Xz)
    xbar = Xz.mean(0)
    t = 7
    xt = Xz[t]

    r_code = _relevance(Xz, xt, Om)

    def d2(a, b):
        d = a - b
        return float(d @ Om @ d)

    sim_ref  = np.array([-0.5 * d2(Xz[i], xt) for i in range(len(Xz))])
    info_i   = np.array([d2(Xz[i], xbar) for i in range(len(Xz))])
    info_t   = d2(xt, xbar)
    r_ref    = sim_ref + 0.5 * (info_i + info_t)

    assert np.allclose(r_code, r_ref, atol=EXACT_TOL), \
        f"relevance mismatch: max|Δ|={np.max(np.abs(r_code-r_ref)):.2e}"

    # sim symmetry: sim(i,t) == sim(t,i)
    sim_it = -0.5 * d2(Xz[3], Xz[9])
    sim_ti = -0.5 * d2(Xz[9], Xz[3])
    assert abs(sim_it - sim_ti) < EXACT_TOL
    return "relevance = sim + ½(info_i+info_t); sim=-½·d_M²; symmetric"


# ---------------------------------------------------------------------------
# 2. Prediction weights — Eqs 1, 7-10
# ---------------------------------------------------------------------------

def test_weights_formula_and_sum():
    """Re-implement Eq 1 (with Eqs 7-10) from scratch and match
    _prediction_weights; confirm weights sum to 1 at every censoring threshold."""
    Xz, y = _make_data(seed=2, N=180, K=4)
    Om = _covariance_inv(Xz)
    xt = Xz[11]
    r = _relevance(Xz, xt, Om)
    N = len(r)

    for thr in CENSORING_THRESHOLDS:
        delta = _compute_delta(r, thr)
        w_code = _prediction_weights(r, delta)

        # Independent Eq 1
        n = delta.sum()
        if n < 2:
            w_ref = np.full(N, 1.0 / N)
        else:
            phi = n / N                                   # Eq 8
            r_bar = (delta * r).sum() / n                 # Eq 9
            ss_full = (r ** 2).sum() / (N - 1)            # Eq 10 num (2nd moment)
            ss_part = ((delta * r) ** 2).sum() / (n - 1)  # Eq 10 den
            lam2 = ss_full / ss_part if ss_part > 0 else 1.0
            w_ref = 1.0 / N + lam2 / (n - 1) * (delta * r - phi * r_bar)  # Eq 1

        assert np.allclose(w_code, w_ref, atol=EXACT_TOL), \
            f"weights mismatch at thr={thr}"
        assert abs(w_code.sum() - 1.0) < EXACT_TOL, \
            f"weights sum {w_code.sum():.3e} ≠ 1 at thr={thr}"
    return "Eq 1 weights reproduced; Σw=1 at thresholds " \
           + ",".join(map(str, CENSORING_THRESHOLDS))


# ---------------------------------------------------------------------------
# 3. Censoring — Eq 6
# ---------------------------------------------------------------------------

def test_censoring_fraction():
    """δ(r_it)=1 iff r_it ≥ r* where r* is the p-th percentile: the retained
    fraction is (1-p) to within one observation."""
    Xz, _ = _make_data(seed=4, N=300, K=3)
    Om = _covariance_inv(Xz)
    r = _relevance(Xz, Xz[0], Om)
    N = len(r)
    for p in (0.0, 0.2, 0.5, 0.8):
        delta = _compute_delta(r, p)
        frac = delta.sum() / N
        assert abs(frac - (1 - p)) <= 1.0 / N + 1e-12, \
            f"retained frac {frac:.3f} ≠ {1-p:.3f} at p={p}"
    # threshold 0 retains everyone
    assert _compute_delta(r, 0.0).sum() == N
    return "retained fraction = 1-p at every percentile threshold"


# ---------------------------------------------------------------------------
# 4. Convergence to OLS (held-out task) — paper p.15
# ---------------------------------------------------------------------------

def test_convergence_to_ols_out_of_sample():
    """RBP with all variables and no censoring, predicting a HELD-OUT point,
    equals the OLS prediction at that point (to the covariance ridge)."""
    Xz, y = _make_data(seed=5, N=160, K=5)
    coef, _, _ = _ols(Xz, y)
    rng = np.random.default_rng(123)
    for _ in range(20):
        xt = rng.standard_normal(Xz.shape[1])
        Om = _covariance_inv(Xz)
        r = _relevance(Xz, xt, Om)
        w = _prediction_weights(r, _compute_delta(r, 0.0))
        rbp_pred = _cell_prediction(w, y)
        ols_pred = float(np.array([1.0, *xt]) @ coef)
        assert abs(rbp_pred - ols_pred) < RIDGE_TOL, \
            f"RBP {rbp_pred:.6f} ≠ OLS {ols_pred:.6f}"
    return "RBP(all vars, no censoring) = OLS prediction on 20 held-out points"


# ---------------------------------------------------------------------------
# 5. In-sample RBP = OLS fitted values
# ---------------------------------------------------------------------------

def test_insample_equals_ols_fitted():
    """Predicting each in-sample point from the full sample (all variables, no
    censoring) reproduces the OLS fitted value."""
    Xz, y = _make_data(seed=6, N=140, K=4)
    _, yhat, _ = _ols(Xz, y)
    Om = _covariance_inv(Xz)
    preds = []
    for t in range(len(Xz)):
        r = _relevance(Xz, Xz[t], Om)
        w = _prediction_weights(r, _compute_delta(r, 0.0))
        preds.append(_cell_prediction(w, y))
    preds = np.array(preds)
    assert np.allclose(preds, yhat, atol=RIDGE_TOL), \
        f"max|RBP-ŷ_OLS|={np.max(np.abs(preds-yhat)):.2e}"
    return "in-sample RBP = OLS fitted values (all N points)"


# ---------------------------------------------------------------------------
# 6. Convergence of fit to R-squared — paper p.15
# ---------------------------------------------------------------------------

def test_fit_converges_to_r2():
    """Informativeness-weighted average of the ADJUSTED fit (Eq 12, K·ρ² for the
    full-variable linear cell) across all in-sample prediction tasks equals the
    full-sample OLS R².  (The RAW fit ρ² converges to R²/K — the adjusted fit's
    K factor is what recovers R², which is why Eq 12 multiplies by K.)"""
    for seed, N, K, nz in [(3, 200, 4, 0.7), (11, 150, 6, 1.0),
                           (42, 300, 3, 0.3), (99, 120, 8, 1.5)]:
        Xz, y = _make_data(seed=seed, N=N, K=K, noise=nz)
        _, _, r2 = _ols(Xz, y)
        Om = _covariance_inv(Xz)
        xbar = Xz.mean(0)
        num = den = 0.0
        for t in range(N):
            r = _relevance(Xz, Xz[t], Om)
            w = _prediction_weights(r, _compute_delta(r, 0.0))
            adj = K * np.corrcoef(w, y)[0, 1] ** 2          # Eq 12 (asym=0)
            info = float((Xz[t] - xbar) @ Om @ (Xz[t] - xbar))  # Eq 5
            num += info * adj
            den += info
        assert abs(num / den - r2) < RIDGE_TOL, \
            f"info-wavg adj_fit {num/den:.6f} ≠ R² {r2:.6f} (seed={seed})"
    return "info-weighted avg adjusted fit = OLS R² across 4 configs"


# ---------------------------------------------------------------------------
# 7. Adjusted fit is 0-asymmetry at threshold 0 (linear cell)
# ---------------------------------------------------------------------------

def test_linear_cell_no_asymmetry():
    """At threshold 0 nothing is censored, so Eq-14 asymmetry is undefined and
    must contribute 0: adjusted fit = K·ρ(w,y)² exactly."""
    Xz, y = _make_data(seed=8, N=160, K=4)
    Om = _covariance_inv(Xz)
    r = _relevance(Xz, Xz[0], Om)
    delta = _compute_delta(r, 0.0)
    w = _prediction_weights(r, delta)
    K = Xz.shape[1]
    adj = _adjusted_fit(w, y, K, delta, r)
    expected = K * np.corrcoef(w, y)[0, 1] ** 2
    assert abs(adj - expected) < EXACT_TOL, f"{adj} ≠ {expected}"
    return "linear cell adjusted fit = K·ρ² (no spurious asymmetry)"


# ---------------------------------------------------------------------------
# 8. Variable importance — footnote 12
# ---------------------------------------------------------------------------

def test_importance_footnote12():
    """rbp().variable_importance[f] = mean(adj_fit of grid cells that INCLUDE f)
    − mean(adj_fit of grid cells that EXCLUDE f).  Reconstructed independently
    from the same grid + the audited cell primitives."""
    Xz, y = _make_data(seed=10, N=150, K=4)
    feats = [f"x{i}" for i in range(Xz.shape[1])]
    X = pd.DataFrame(Xz, columns=feats)
    xt = pd.Series(np.random.default_rng(1).standard_normal(len(feats)), index=feats)

    res = rbp(X, pd.Series(y), xt, feats, n_random_cells=60, random_state=42)

    # Rebuild the identical grid and adj_fit per cell, then apply footnote 12.
    K = len(feats)
    mu, sig = Xz.mean(0), Xz.std(0); sig[sig == 0] = 1
    Xn = (Xz - mu) / sig
    xtn = (xt.values - mu) / sig
    cells = _build_grid(K, n_random=60, thresholds=CENSORING_THRESHOLDS, random_state=42)
    per_feat_in, per_feat_out = {f: [] for f in feats}, {f: [] for f in feats}
    for subset, thr, use_sim in cells:
        idx = sorted(subset)
        Xs, xts = Xn[:, idx], xtn[idx]
        Om = _covariance_inv(Xs)
        r = _relevance(Xs, xts, Om)
        if use_sim:
            diff = Xs - xts
            crit = -0.5 * np.einsum('ij,jk,ik->i', diff, Om, diff)
        else:
            crit = r
        delta = _compute_delta(crit, thr)
        w = _prediction_weights(r, delta)
        adj = _adjusted_fit(w, y, len(subset), delta, r)
        for j, f in enumerate(feats):
            (per_feat_in if j in subset else per_feat_out)[f].append(adj)
    imp_ref = {f: (np.mean(per_feat_in[f]) if per_feat_in[f] else 0.0)
                  - (np.mean(per_feat_out[f]) if per_feat_out[f] else 0.0)
               for f in feats}

    for f in feats:
        assert abs(float(res.variable_importance[f]) - imp_ref[f]) < EXACT_TOL, \
            f"importance[{f}]: {res.variable_importance[f]} ≠ {imp_ref[f]}"
    return "importance = mean(adj_fit|with) − mean(adj_fit|without) (footnote 12)"


# ---------------------------------------------------------------------------
# 9. Grid structure — paper p.18
# ---------------------------------------------------------------------------

def test_grid_structure():
    """Sparse grid = 1 full-variable linear cell + K single-variable linear
    cells + n_random others; all threshold-0 cells are relevance-mode."""
    K, n_rand = 6, 40
    cells = _build_grid(K, n_random=n_rand, random_state=0)
    assert len(cells) == 1 + K + n_rand, f"grid size {len(cells)} ≠ {1+K+n_rand}"
    full = frozenset(range(K))
    assert (full, 0.0, False) in cells, "missing full-variable linear cell"
    singles = [(frozenset([k]), 0.0, False) for k in range(K)]
    for s in singles:
        assert s in cells, f"missing single-variable linear cell {s}"
    for subset, thr, use_sim in cells:
        if thr == 0.0:
            assert use_sim is False, "threshold-0 cell must be relevance-mode"
    assert len(set(cells)) == len(cells), "duplicate cells in grid"
    return f"grid = 1 + K + n_random = {len(cells)} unique cells; thr-0 ⇒ relevance-mode"


# ---------------------------------------------------------------------------
# 10. Composite prediction identity — Eqs 16-17
# ---------------------------------------------------------------------------

def test_composite_prediction_identity():
    """The reported prediction (Eq 17, ŷ = Σψ_θ ŷ_θ) equals the composite weight
    vector (Eq 16, w_grid = Σψ_θ w_θ) dotted with y, and w_grid sums to 1."""
    Xz, y = _make_data(seed=15, N=150, K=4)
    feats = [f"x{i}" for i in range(4)]
    X = pd.DataFrame(Xz, columns=feats)
    ys = pd.Series(y, index=X.index)
    xt = pd.Series([0.2, -0.1, 0.4, 0.0], index=feats)
    res = rbp(X, ys, xt, feats, random_state=3)
    y_al = ys.reindex(res.weights.index).values
    assert abs(res.prediction - float(res.weights.values @ y_al)) < EXACT_TOL, \
        "prediction ≠ w_grid·y"
    assert abs(res.weights.sum() - 1.0) < 1e-6, \
        f"Σw_grid = {res.weights.sum():.3e} ≠ 1"
    return "prediction = w_grid·y (Eqs 16-17); Σw_grid = 1"


# ---------------------------------------------------------------------------
# 11. Determinism & single-feature edge case
# ---------------------------------------------------------------------------

def test_determinism_and_k1():
    Xz, y = _make_data(seed=13, N=120, K=3)
    feats = [f"x{i}" for i in range(3)]
    X = pd.DataFrame(Xz, columns=feats)
    xt = pd.Series([0.1, -0.2, 0.3], index=feats)
    a = rbp(X, pd.Series(y), xt, feats, random_state=7)
    b = rbp(X, pd.Series(y), xt, feats, random_state=7)
    assert abs(a.prediction - b.prediction) < EXACT_TOL, "non-deterministic prediction"
    assert np.allclose(a.weights.values, b.weights.values, atol=EXACT_TOL)

    # K=1 must run and stay finite
    c = rbp(X[["x0"]], pd.Series(y), xt[["x0"]], ["x0"])
    assert np.isfinite(c.prediction), "K=1 prediction not finite"
    return "same seed ⇒ identical output; K=1 runs and is finite"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

ALL_TESTS = [
    ("Eqs 2-5  relevance decomposition",      test_relevance_decomposition),
    ("Eqs 1,7-10 weights & Σw=1",             test_weights_formula_and_sum),
    ("Eq 6  censoring fraction",              test_censoring_fraction),
    ("p.15  convergence to OLS (held-out)",   test_convergence_to_ols_out_of_sample),
    ("       in-sample RBP = OLS fitted",     test_insample_equals_ols_fitted),
    ("p.15  fit → R² convergence",            test_fit_converges_to_r2),
    ("Eq 14 linear cell has no asymmetry",    test_linear_cell_no_asymmetry),
    ("fn.12 variable importance",             test_importance_footnote12),
    ("p.18  grid structure",                  test_grid_structure),
    ("Eqs 16-17 composite prediction",        test_composite_prediction_identity),
    ("       determinism & K=1 edge case",    test_determinism_and_k1),
]


def main() -> int:
    print("RBP fidelity suite — verifying rbp.py against Czasonis-Kritzman-"
          "Turkington (2024)\n" + "=" * 78)
    failed = 0
    for label, fn in ALL_TESTS:
        try:
            detail = fn()
            print(f"  PASS  {label:40s}  {detail}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {label:40s}  {e}")
    print("=" * 78)
    print(f"{len(ALL_TESTS)-failed}/{len(ALL_TESTS)} passed"
          + ("" if not failed else f"  — {failed} FAILED"))
    return 1 if failed else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
