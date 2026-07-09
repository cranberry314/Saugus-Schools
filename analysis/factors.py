"""
factors.py — the factor LIBRARY
================================
Every factor the project knows about is defined here ONCE, as a named `Factor`
object you can reference directly, e.g. ``factors.spend_vs_required``.  A report or
screen then *selects* the subset it wants by listing those references, grouped by
tier — so the selection is explicit and readable, while each factor's definition
(tier, formula, units, provenance) lives in exactly one place and cannot drift.

    import analysis.factors as F

    TIER1 = [F.ed_budget_share, F.spend_vs_required, F.fixed_costs_pct]   # a report
    names = {f.name for f in TIER1}                                       # → column names

A `Factor` carries:
    name    — the DataFrame column / DB name
    tier    — 1 votable · 2 managed · 3 structural (what a town IS)
    kind    — "raw" (a DB column) or "derived" (a ratio; see `formula` + derive_factors)
    unit    — display/format kind: "pct" (0-100) · "pct100" (0-1 fraction) ·
              "dollar" · "count" · "rate" (small ratios)
    higher_is_better — True/False for actionable levers; None for structural
    label   — human-readable name for tables/exhibits
    formula — for derived factors, the exact ratio (the math lives in derive_factors)

To ADD a factor: define one `Factor(...)` below and (if derived) add its formula to
derive_factors.  To USE it in a report: reference it there by name.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# The Factor object
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Factor:
    name: str
    tier: int                       # 1 votable · 2 managed · 3 structural
    kind: str                       # "raw" | "derived"
    unit: str                       # pct | pct100 | dollar | count | rate
    higher_is_better: bool | None   # None for structural
    label: str
    formula: str | None = None      # for derived factors

    def __str__(self) -> str:       # so it reads/serialises as its column name
        return self.name


# ---------------------------------------------------------------------------
# The library — every factor defined once, referenceable as factors.<name>
# ---------------------------------------------------------------------------

# ── Tier 3 — structural: what a community IS (peer context; never ranked) ──────
low_income_pct        = Factor("low_income_pct",        3, "raw", "pct",    None, "% students from low-income families")
median_hh_income      = Factor("median_hh_income",      3, "raw", "dollar", None, "Median household income")
equalized_income      = Factor("equalized_income",      3, "raw", "dollar", None, "Equalized property value per capita")
pct_bachelors_plus    = Factor("pct_bachelors_plus",    3, "raw", "pct",    None, "Adults with a bachelor's degree+")
pct_owner_occupied    = Factor("pct_owner_occupied",    3, "raw", "pct",    None, "Owner-occupied housing units")
ell_pct               = Factor("ell_pct",               3, "raw", "pct",    None, "English language learners")
sped_pct              = Factor("sped_pct",              3, "raw", "pct",    None, "Special education students")
total_enrollment      = Factor("total_enrollment",      3, "raw", "count",  None, "Total district enrollment")
crime_rate            = Factor("crime_rate",            3, "raw", "count",  None, "Crime incidents per 100k residents")
health_ins_per_capita = Factor("health_ins_per_capita", 3, "derived", "dollar", None,
                               "Health insurance $ per resident",
                               formula="health_insurance_expenditure / municipal_population")

# ── Tier 1 — votable: what the town chooses to fund (Town Meeting / ballot) ────
ed_budget_share       = Factor("ed_budget_share",       1, "raw", "pct",    True,  "Education's share of the municipal budget")
fixed_costs_pct       = Factor("fixed_costs_pct",       1, "raw", "pct",    False, "Fixed costs (mostly health ins.)")
spend_vs_required     = Factor("spend_vs_required",     1, "derived", "rate", True,
                               "Spending vs Ch70 required minimum",
                               formula="in-district PPE / required NSS per pupil")

# ── Tier 2 — managed: day-to-day school operations (administration) ────────────
chronic_absenteeism_pct   = Factor("chronic_absenteeism_pct",   2, "raw", "pct",    False, "Students chronically absent (10%+)")
avg_teacher_salary        = Factor("avg_teacher_salary",        2, "raw", "dollar", True,  "Average teacher salary")
teachers_per_100_students = Factor("teachers_per_100_students", 2, "derived", "rate", True,
                                   "Teachers per 100 students",
                                   formula="teacher_fte / total_enrollment × 100")
instructional_share       = Factor("instructional_share",       2, "derived", "pct100", True,
                                   "Share of school $ reaching the classroom",
                                   formula="(teachers + other-teaching + materials + instructional-leadership) / in-district PPE")
teachers_per_lowincome    = Factor("teachers_per_lowincome",    2, "derived", "rate", True,
                                   "Teachers per low-income student",
                                   formula="teachers_per_100_students / low_income_pct")
teacher_pay_share         = Factor("teacher_pay_share",         2, "derived", "rate", True,
                                   "Teacher share of school spending",
                                   formula="teacher spending per pupil / in-district PPE")

# The one library factor deliberately NOT tiered into any curated report pool:
# strong raw but weak partial (mostly a wealth proxy, not a lever).  Kept because
# the statewide screen still tests it as a candidate.  Derivable in derive_factors.
nss_per_eqv = Factor("nss_per_eqv", 3, "derived", "rate", None,
                     "School spend vs. property wealth",
                     formula="in-district PPE / equalized valuation per capita")


# ---------------------------------------------------------------------------
# Registry + lookups (all derived from the Factor objects above)
# ---------------------------------------------------------------------------

# The curated library — every Factor defined above.  Add a factor to this list
# when you define it so the registry/lookups see it.
LIBRARY: list[Factor] = [
    low_income_pct, median_hh_income, equalized_income, pct_bachelors_plus,
    pct_owner_occupied, ell_pct, sped_pct, total_enrollment, crime_rate,
    health_ins_per_capita,
    ed_budget_share, fixed_costs_pct, spend_vs_required,
    chronic_absenteeism_pct, avg_teacher_salary, teachers_per_100_students,
    instructional_share, teachers_per_lowincome, teacher_pay_share,
    nss_per_eqv,
]

FACTOR_CATALOG: dict[str, Factor] = {f.name: f for f in LIBRARY}


def get(name: str) -> Factor | None:
    """The Factor object for a column name (None if not in the library)."""
    return FACTOR_CATALOG.get(name)


def tier_of(name: str) -> int | None:
    f = FACTOR_CATALOG.get(name)
    return f.tier if f else None


def is_structural(name: str) -> bool:
    return tier_of(name) == 3


def is_actionable(name: str) -> bool:
    return tier_of(name) in (1, 2)


def factors_in_tier(*tiers: int) -> list[Factor]:
    """Library Factor objects whose tier is in `tiers` (definition order)."""
    return [f for f in LIBRARY if f.tier in tiers]


def names(factors) -> set[str]:
    """Column-name set for a list of Factor objects (or names)."""
    return {f.name if isinstance(f, Factor) else f for f in factors}


DERIVED_FACTORS: set[str] = {f.name for f in LIBRARY if f.kind == "derived"}


# ---------------------------------------------------------------------------
# Derived-ratio formulas (the math for kind="derived" factors, in one place)
# ---------------------------------------------------------------------------

def _col(df: pd.DataFrame, *names_: str):
    """First present column among `names_` (handles report-vs-screen naming), or None."""
    for n in names_:
        if n in df.columns:
            return df[n]
    return None


def _safe_div(a, b):
    if a is None or b is None:
        return np.nan
    return a / b.replace(0, np.nan)


def derive_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add the derived-ratio factors to `df` (row-wise; never touches the DB).  Each
    factor is computed ONLY when its inputs are present, so a caller supplying just
    some inputs gets just those factors — an absent input never overwrites an
    existing column with NaN.  Column names are resolved by alias (see _col) so both
    the report's and the screen's naming work unchanged.
    """
    d = df.copy()

    ppe_indist = _col(d, "in_district_ppe", "ppe_in_district")
    ppe_teach  = _col(d, "teacher_spending_per_pupil", "ppe_teachers")
    ppe_instr  = _col(d, "ppe_instructional")
    teach_fte  = _col(d, "teacher_fte")
    enroll     = _col(d, "total_enrollment")
    low_inc    = _col(d, "low_income_pct")
    req_nss    = _col(d, "req_nss_pp", "required_nss_pp")
    eqv_pc     = _col(d, "eqv_per_capita")
    health     = _col(d, "health_ins", "health_insurance_expenditure", "health_ins_exp")
    muni_pop   = _col(d, "muni_pop", "municipal_population")

    def _set(name, a, b, scale=1.0):
        if a is not None and b is not None:
            d[name] = _safe_div(a, b) * scale

    # Staffing intensity (density first — per-need uses it)
    _set(teachers_per_100_students.name, teach_fte, enroll, scale=100.0)
    _set(teachers_per_lowincome.name, _col(d, teachers_per_100_students.name), low_inc)

    # Spending mix (shares of in-district PPE)
    _set(instructional_share.name, ppe_instr, ppe_indist)
    _set(teacher_pay_share.name,   ppe_teach, ppe_indist)

    # Spending effort
    _set(spend_vs_required.name, ppe_indist, req_nss)
    _set(nss_per_eqv.name,       ppe_indist, eqv_pc)

    # Municipal cost drag (structural)
    _set(health_ins_per_capita.name, health, muni_pop)

    return d
