"""
actionable_factors.py  —  Design B: "match on structure, test the factors"
========================================================================
RBP is tier-blind: it treats every feature column identically.  To answer
"among demographically similar towns, how much does each *actionable* factor
matter for the outcome," we impose the tier structure ourselves by choosing
which columns RBP sees:

  1. STRUCTURAL (Tier 3 — what a town IS): the matching basis.  A baseline RBP
     leave-one-out is run on these alone → baseline LOO r per outcome.
  2. For each ACTIONABLE factor (Tier 1/2 — what a town DOES), re-run RBP LOO on
     STRUCTURAL + that one factor.  Its *marginal* contribution is:
        lift        = LOO r(structural + factor) − LOO r(structural)
        importance  = the factor's Exhibit-5 importance in the augmented Saugus run
     i.e. how much the factor sharpens the prediction ON TOP OF the structural
     match — which is exactly "how important is this difference."

  3. The descriptive "difference" is Saugus's value vs. the median of its
     structurally-nearest peers (Mahalanobis on STRUCTURAL only), so the peer
     set is defined by what Saugus IS, not what it does.

Full N is preserved throughout — RBP's relevance weighting is the "similar
towns" mechanism, so we never subset the data for estimation (that would wreck
the covariance).  rbp.py is untouched.

Caveat this CANNOT escape: lift/importance are association-given-peers, not a
causal dose-response.  Read the output as a screened shortlist, not a promise
that moving a factor moves the outcome.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

sys.path.insert(0, str(Path(__file__).parent))
from saugus_factor_analysis import (          # noqa: E402
    load_features, get_engine, SAUGUS, _higher_is_better,
    _fmt_feature_val, _feat_meta,
    _PAGE_W, _PAGE_H, _BLUE, _GREEN, _RED, _GOLD, _GREY, _BL,
    _header, _footer, _save,
)
from rbp import rbp, rbp_loo            # noqa: E402
from sqlalchemy import text             # noqa: E402

# ── Tier 3: structural matching basis (what a community IS) ──────────────────
STRUCTURAL = [
    "low_income_pct", "median_hh_income", "equalized_income",
    "pct_bachelors_plus", "pct_owner_occupied", "ell_pct", "sped_pct",
    "total_enrollment", "crime_rate",
]

# ── Tier 1/2: actionable factors (what a town DOES — votable or managed) ──────
FACTORS = [
    "chronic_absenteeism_pct",    # Tier 2 — attendance policy
    "teachers_per_100_students",  # Tier 2 — staffing
    "avg_teacher_salary",         # Tier 2 — pay / retention
    "in_district_ppe",              # Tier 1 — above-foundation spending level
    "res_tax_rate",               # Tier 1 — tax rate / override
    "ed_budget_share",            # Tier 1 — Town Meeting allocation
    "debt_service_pct",           # Tier 1 (partial) — borrowing
    "fixed_costs_pct",            # partial — pensions/benefits/OPEB/health
    "public_safety_pct",          # Tier 1 — allocation
    "public_works_pct",           # Tier 1 — allocation
]

# ── Derived "effort / intensity / efficiency" factors, mined from the wider DB ─
# Mostly wealth-normalized or per-unit ratios: they ask "how hard is this town
# trying, given what it has," which a raw level can't capture.  (label, kind)
DERIVED_INFO: dict[str, tuple[str, str]] = {
    "spend_vs_foundation":         ("in-district PPE/pupil ÷ foundation budget/pupil",  "rate"),
    "spend_vs_required":       ("in-district PPE/pupil ÷ Ch70 required NSS/pupil",   "rate"),
    "nss_per_income":              ("in-district PPE/pupil ÷ income per capita",         "rate"),
    "nss_per_eqv":                 ("in-district PPE/pupil ÷ property wealth/capita",    "rate"),
    "teacher_density_per_homeval": ("Teachers/100 ÷ (home value/100k)",      "rate"),
    "teacher_pay_vs_income":       ("Teacher salary ÷ median HH income",     "rate"),
    "teacher_pay_vs_homeval":      ("Teacher salary ÷ home value",           "rate"),
    "teacher_pay_share":      ("Teacher $/pupil ÷ in-district PPE/pupil",           "rate"),
    "reserves_pct":                ("Reserves (free cash+stab) ÷ budget",    "pct"),
    "free_cash_pct":               ("Free cash ÷ operating budget",          "pct"),
    "health_ins_per_employee":     ("Health insurance ÷ employees",          "dollar"),
    "health_ins_per_capita":       ("Health insurance ÷ population",         "dollar"),
    "opeb_per_capita":             ("OPEB contribution ÷ population",        "dollar"),
    "tax_bill_proxy":              ("Res tax rate × home value (tax bill)",  "dollar"),
    "teachers_per_lowincome":      ("Teachers/100 ÷ low-income %",           "rate"),
    "fixed_costs_per_capita":      ("Fixed costs ÷ population",              "dollar"),
    "ed_exp_per_capita":           ("Education $ ÷ population",              "dollar"),
}


def _lev_label(lv: str) -> tuple[str, str]:
    return DERIVED_INFO[lv] if lv in DERIVED_INFO else _feat_meta(lv)


def build_derived(df: pd.DataFrame, engine) -> tuple[pd.DataFrame, list[str]]:
    """Join wider-DB columns (latest non-null per municipality) and compute the
    derived factors.  Returns (augmented_df, derived_names)."""
    d = df.copy()
    d["_k"] = d["district_name"].str.lower().str.strip()

    def latest(table, namecol, cols, yearcol="fiscal_year", notnull=None):
        nn = f"WHERE {notnull} IS NOT NULL" if notnull else ""
        sql = (f"SELECT DISTINCT ON (lower({namecol})) lower({namecol}) AS _k, "
               + ", ".join(cols) + f" FROM {table} {nn} "
               f"ORDER BY lower({namecol}), {yearcol} DESC")
        with engine.connect() as c:
            return pd.read_sql(text(sql), c)

    joins = [
        latest("municipal_income_eqv", "municipality",
               ["income_per_capita", "eqv_per_capita", "population AS muni_pop"]),
        latest("municipal_free_cash", "municipality",
               ["cert_free_cash", "operating_budget AS fc_budget"], notnull="cert_free_cash"),
        latest("municipal_stabilization", "municipality",
               ["total_stabilization_fund_balance AS stab_bal"], notnull="total_stabilization_fund_balance"),
        latest("municipal_health_insurance", "municipality",
               ["health_insurance_expenditure AS health_ins"], notnull="health_insurance_expenditure"),
        latest("municipal_trust_funds", "municipality", ["opeb_trust AS opeb"],
               notnull="opeb_trust"),  # amount_type filter below
        latest("municipal_personnel", "municipality",
               ["total_employees", "total_salaries_wages"], notnull="total_employees"),
        latest("district_chapter70", "district_name",
               ["required_nss_per_pupil AS req_nss_pp"], notnull="required_nss_per_pupil"),
        latest("municipal_expenditures", "municipality",
               ["education AS ed_exp", "fixed_costs AS fc_exp", "total_expenditures AS tot_exp"]),
    ]
    # OPEB must be the Revenues (contributions-in) side
    with engine.connect() as c:
        opeb = pd.read_sql(text(
            "SELECT DISTINCT ON (lower(municipality)) lower(municipality) AS _k, "
            "opeb_trust AS opeb FROM municipal_trust_funds WHERE amount_type='Revenues' "
            "ORDER BY lower(municipality), fiscal_year DESC"), c)
        zl = pd.read_sql(text(
            "SELECT DISTINCT ON (lower(region_name)) lower(region_name) AS _k, zhvi "
            "FROM municipal_zillow_housing ORDER BY lower(region_name), data_year DESC, data_month DESC"), c)
    joins[4] = opeb
    for t in joins + [zl]:
        d = d.merge(t, on="_k", how="left")

    def safe(a, b):
        b = b.replace(0, np.nan)
        return a / b

    d["spend_vs_foundation"]         = safe(d["in_district_ppe"], d["foundation_budget_pp"])
    d["spend_vs_required"]       = safe(d["in_district_ppe"], d["req_nss_pp"])
    d["nss_per_income"]              = safe(d["in_district_ppe"], d["income_per_capita"])
    d["nss_per_eqv"]                 = safe(d["in_district_ppe"], d["eqv_per_capita"])
    d["teacher_density_per_homeval"] = safe(d["teachers_per_100_students"], d["zhvi"] / 1e5)
    d["teacher_pay_vs_income"]       = safe(d["avg_teacher_salary"], d["median_hh_income"])
    d["teacher_pay_vs_homeval"]      = safe(d["avg_teacher_salary"], d["zhvi"])
    d["teacher_pay_share"]      = safe(d["teacher_spending_per_pupil"], d["in_district_ppe"])
    d["reserves_pct"]                = safe(d["cert_free_cash"].fillna(0) + d["stab_bal"].fillna(0), d["fc_budget"]) * 100
    d["free_cash_pct"]               = safe(d["cert_free_cash"], d["fc_budget"]) * 100
    d["health_ins_per_employee"]     = safe(d["health_ins"], d["total_employees"])
    d["health_ins_per_capita"]       = safe(d["health_ins"], d["muni_pop"])
    d["opeb_per_capita"]             = safe(d["opeb"], d["muni_pop"])
    d["tax_bill_proxy"]              = d["res_tax_rate"] * d["zhvi"] / 1000
    d["teachers_per_lowincome"]      = safe(d["teachers_per_100_students"], d["low_income_pct"])
    d["fixed_costs_per_capita"]      = safe(d["fc_exp"], d["muni_pop"])
    d["ed_exp_per_capita"]           = safe(d["ed_exp"], d["muni_pop"])

    derived = list(DERIVED_INFO.keys())
    sv = d[d["district_name"].str.lower().str.strip() == "saugus"]
    if len(sv):
        vals = {k: (round(float(sv[k].iloc[0]), 3) if pd.notna(sv[k].iloc[0]) else None)
                for k in derived}
        print("[factors] Saugus derived values:")
        for k, v in vals.items():
            print(f"          {k:30s} = {v}")
    cov = {k: int(d[k].notna().sum()) for k in derived}
    print(f"[factors] derived coverage (of {len(d)} towns):",
          {k: cov[k] for k in derived})
    return d, derived


# Academic outcomes are where "which factor helps?" is the live question.
OUTCOMES = [
    ("MCAS Grades 3–8",     "avg_mcas"),
    ("Dropout Rate",        "dropout_pct"),
    ("MCAS Grade 10 (ELA)", "mcas10_ela"),
]

N_RANDOM = 800     # screening density — fine for relative lifts (full prod = 3000)
SEED     = 42

OUT_DIR  = Path(__file__).resolve().parent.parent / "Reports"
OUT_CSV  = OUT_DIR / "actionable_factors.csv"
OUT_PDF  = OUT_DIR / "actionable_factors.pdf"


# ── Core RBP helpers (full N, Saugus excluded only by LOO itself) ───────────
def _design(df: pd.DataFrame, features: list[str], target: str):
    full = df[["district_name"] + features + [target]].dropna().copy()
    X = full.drop(columns=["district_name"]).copy()
    X.index = full["district_name"].values
    y = X.pop(target)
    return X, y


def _loo_r(df: pd.DataFrame, features: list[str], target: str, n_random: int) -> tuple[float, int]:
    X, y = _design(df, features, target)
    loo = rbp_loo(X, y, features, n_random_cells=n_random, random_state=SEED)
    loo = loo.dropna(subset=["actual", "predicted"])
    if len(loo) < 3:
        return float("nan"), len(loo)
    return float(np.corrcoef(loo["actual"], loo["predicted"])[0, 1]), len(loo)


def _factor_importance(df, features, target, factor, n_random) -> float:
    X, y = _design(df, features, target)
    if SAUGUS not in X.index:
        return float("nan")
    res = rbp(X.drop(index=SAUGUS), y.drop(index=SAUGUS), X.loc[SAUGUS],
              features, n_random_cells=n_random, random_state=SEED)
    return float(res.variable_importance.get(factor, float("nan")))


def structural_peers(df: pd.DataFrame, n: int = 25) -> list[str]:
    """The n towns nearest Saugus by Mahalanobis distance on STRUCTURAL features."""
    d2 = df.set_index("district_name")
    feats = [f for f in STRUCTURAL if f in d2.columns]
    data = d2[feats].dropna()
    if SAUGUS not in data.index:
        return []
    cov = data.cov().values
    cinv = np.linalg.pinv(cov + 1e-6 * np.eye(len(feats)))
    sv = data.loc[SAUGUS].values.astype(float)
    dists = {}
    for t in data.index:
        if t == SAUGUS:
            continue
        diff = data.loc[t].values.astype(float) - sv
        dists[t] = float(np.sqrt(diff @ cinv @ diff))
    return sorted(dists, key=dists.get)[:n]


# ── Parallel worker: one (outcome, factor-or-baseline) cell ──────────────────
def _run_task(args: tuple) -> dict:
    df, label, target, factor, n_random = args
    feats = list(STRUCTURAL) if factor is None else list(STRUCTURAL) + [factor]
    # never let the target sit in its own predictor set
    feats = [f for f in feats if f != target]
    r, n = _loo_r(df, feats, target, n_random)
    imp = (float("nan") if factor is None
           else _factor_importance(df, feats, target, factor, n_random))
    tag = "baseline(structural)" if factor is None else factor
    print(f"[factors] {label:20s} {tag:26s} LOO r={r:+.4f} (N={n})", flush=True)
    return {"label": label, "target": target,
            "factor": ("__baseline__" if factor is None else factor),
            "loo_r": r, "n": n, "importance": imp}


def main(parallel: bool = True):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("[factors] Loading features...")
    df, derived = build_derived(load_features(get_engine()), get_engine())
    factor_pool = FACTORS + derived
    peers = structural_peers(df, n=25)
    print(f"[factors] {len(df)} districts; {len(peers)} structural peers of Saugus")
    print(f"[factors] nearest peers: {', '.join(peers[:10])} ...")
    print(f"[factors] testing {len(factor_pool)} factors ({len(FACTORS)} raw + {len(derived)} derived)")

    # Build task list: per outcome, a baseline + one per available factor.
    # Skip factors with thin coverage (need enough towns for a stable LOO).
    tasks = []
    for label, target in OUTCOMES:
        avail = [lv for lv in factor_pool
                 if lv in df.columns and lv != target and df[lv].notna().sum() >= 120]
        tasks.append((df, label, target, None, N_RANDOM))          # baseline
        for lv in avail:
            tasks.append((df, label, target, lv, N_RANDOM))

    print(f"[factors] {len(tasks)} RBP-LOO passes (n_random={N_RANDOM})...")
    if parallel:
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=min(mp.cpu_count(), 4)) as pool:
            rows = pool.map(_run_task, tasks)
    else:
        rows = [_run_task(t) for t in tasks]

    res = pd.DataFrame(rows)

    # Marginal lift = augmented LOO r − that outcome's baseline LOO r.
    base = (res[res["factor"] == "__baseline__"]
            .set_index("target")["loo_r"].to_dict())
    res["baseline_loo_r"] = res["target"].map(base)
    res["lift"] = res["loo_r"] - res["baseline_loo_r"]

    # Descriptive gap vs structurally-nearest peers (median), per factor.
    d2 = df.set_index("district_name")
    def _saugus(c):  # noqa
        return float(d2.loc[SAUGUS, c]) if SAUGUS in d2.index and c in d2.columns else float("nan")
    def _peermed(c):  # noqa
        vals = d2.loc[[p for p in peers if p in d2.index], c].dropna() if c in d2.columns else pd.Series(dtype=float)
        return float(vals.median()) if len(vals) else float("nan")
    res["saugus_val"]  = res["factor"].map(lambda lv: _saugus(lv) if lv != "__baseline__" else float("nan"))
    res["peer_median"] = res["factor"].map(lambda lv: _peermed(lv) if lv != "__baseline__" else float("nan"))
    res["gap_peer_minus_saugus"] = res["peer_median"] - res["saugus_val"]

    res = res[["label", "target", "factor", "baseline_loo_r", "loo_r", "lift",
               "importance", "saugus_val", "peer_median",
               "gap_peer_minus_saugus", "n"]]
    res.to_csv(OUT_CSV, index=False)
    print(f"[factors] wrote {OUT_CSV}")

    _render_pdf(res, peers)
    print(f"[factors] wrote {OUT_PDF}")
    return res


# ── Reporting ───────────────────────────────────────────────────────────────
def _render_pdf(res: pd.DataFrame, peers: list[str]):
    factors_only = res[res["factor"] != "__baseline__"].copy()
    with PdfPages(str(OUT_PDF)) as pdf:
        # Title / method page
        fig, ax = plt.subplots(figsize=(_PAGE_W, _PAGE_H)); ax.axis("off")
        _header(fig, "Actionable Factors — Match on Structure, Test the Factors",
                "Design B: RBP relevance is anchored on Tier-3 structural features (the "
                "peer match); each Tier-1/2 factor is added on top and scored by its "
                "marginal LOO-r lift and Exhibit-5 importance.  Full N — no subsetting.")
        lines = [
            "Structural matching basis (what a town IS): " + ", ".join(STRUCTURAL),
            "",
            "For each outcome:  baseline = RBP LOO on structural features only.",
            "Then, per factor:  lift = LOO r(structural + factor) − LOO r(structural).",
            "importance = that factor's Exhibit-5 score in the augmented Saugus run.",
            "gap = (median of Saugus's 25 structurally-nearest peers) − Saugus.",
            "",
            "Read: high lift / high importance = the difference MATTERS for the outcome;",
            "a non-trivial gap = Saugus is an outlier on it.  Both ⇒ shortlist.",
            "",
            "CAVEAT: association given peers, NOT a causal dose-response.  This screens",
            "candidates; it does not promise that moving a factor moves the outcome.",
        ]
        for i, ln in enumerate(lines):
            ax.text(0.06, 0.78 - i * 0.045, ln, fontsize=9.5,
                    color=_BL if not ln.startswith("CAVEAT") else _RED,
                    transform=ax.transAxes, family="monospace" if "," in ln and i == 0 else None)
        ax.text(0.06, 0.10, "Structural peers: " + ", ".join(peers[:14]) + " …",
                fontsize=7.5, color=_GREY, style="italic", transform=ax.transAxes)
        _footer(fig, "RBP is tier-blind; the tier roles are imposed by which columns enter the "
                "match vs. the test.  rbp.py unchanged.")
        _save(pdf, fig)

        # One page per outcome: sorted marginal-lift bars + detail table.
        for label, target in [(l, t) for l, t in OUTCOMES]:
            sub = (factors_only[factors_only["label"] == label]
                   .sort_values("lift", ascending=True))
            if sub.empty:
                continue
            higher_better = _higher_is_better(target)
            fig, (axL, axR) = plt.subplots(
                1, 2, figsize=(_PAGE_W, _PAGE_H), gridspec_kw={"width_ratios": [1.1, 1.0]})
            base_r = float(sub["baseline_loo_r"].iloc[0])
            _header(fig, f"Actionable Factors: {label}",
                    f"Structural baseline LOO r = {base_r:+.3f}  ·  bars = marginal lift "
                    f"from adding each factor  ·  n_random={N_RANDOM}")

            # Left: marginal lift bars
            names = [_lev_label(lv)[0] for lv in sub["factor"]]
            lifts = sub["lift"].values
            colors = [_GREEN if v > 0 else _RED for v in lifts]
            y = range(len(names))
            axL.barh(list(y), lifts, color=colors, alpha=0.85)
            axL.set_yticks(list(y)); axL.set_yticklabels(names, fontsize=8)
            axL.axvline(0, color=_BL, lw=0.8)
            axL.set_xlabel("Marginal LOO-r lift over structural baseline")
            axL.set_title("Does the factor sharpen the prediction?", fontsize=9)
            axL.grid(axis="x", alpha=0.25)
            for yi, v in zip(y, lifts):
                axL.text(v + (0.001 if v >= 0 else -0.001), yi, f"{v:+.3f}",
                         va="center", ha="left" if v >= 0 else "right", fontsize=6.5,
                         color=_BL)

            # Right: detail table (lift, importance, Saugus vs peers, gap)
            axR.axis("off")
            tbl_rows = []
            for _, r in sub.sort_values("lift", ascending=False).iterrows():
                lv = r["factor"]
                _, kind = _lev_label(lv)
                gap = r["gap_peer_minus_saugus"]
                # interpret gap direction for the outcome-agnostic descriptive note
                tbl_rows.append([
                    _lev_label(lv)[0][:26],
                    f"{r['lift']:+.3f}",
                    f"{r['importance']:+.2f}" if not np.isnan(r['importance']) else "—",
                    _fmt_feature_val(r["saugus_val"], kind),
                    _fmt_feature_val(r["peer_median"], kind),
                    (f"{gap:+.1f}" if abs(gap) < 1000 else f"{gap:+,.0f}"),
                ])
            tbl = axR.table(
                cellText=tbl_rows,
                colLabels=["Factor", "Lift", "Imp", "Saugus", "Peer med", "Gap"],
                bbox=[0.0, 0.05, 1.0, 0.86], cellLoc="center")
            tbl.auto_set_font_size(False); tbl.set_fontsize(7.5)
            col_w = [0.34, 0.13, 0.10, 0.15, 0.15, 0.13]
            for (ri, ci), cell in tbl.get_celld().items():
                if ci < len(col_w):
                    cell.set_width(col_w[ci])
                if ci == 0:
                    cell.set_text_props(ha="left")
                if ri == 0:
                    cell.set_facecolor(_BLUE); cell.set_text_props(color="white")
                else:
                    cell.set_facecolor("#F5F5F5" if ri % 2 else "white")
                cell.set_edgecolor("#DDDDDD")
            axR.text(0.0, 0.95,
                     "Gap = peer median − Saugus (structurally-nearest 25).  "
                     f"Outcome is {'higher' if higher_better else 'lower'}-is-better.",
                     fontsize=7, color=_GREY, style="italic", transform=axR.transAxes)

            _footer(fig, "Lift/importance are association given the structural peer match — a "
                    "screened shortlist, not a causal effect.")
            _save(pdf, fig)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--serial", action="store_true", help="run sequentially")
    a = ap.parse_args()
    main(parallel=not a.serial)
