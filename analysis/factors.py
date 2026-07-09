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
    curated: bool = True            # True = in a report pool (precise metadata);
                                    # False = exploratory screen candidate (best-effort)

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

# A wealth-proxy ratio kept for the screen to test, not curated into a report pool.
nss_per_eqv = Factor("nss_per_eqv", 3, "derived", "rate", None,
                     "School spend vs. property wealth", curated=False,
                     formula="in-district PPE / equalized valuation per capita")

CURATED: list[Factor] = [
    low_income_pct, median_hh_income, equalized_income, pct_bachelors_plus,
    pct_owner_occupied, ell_pct, sped_pct, total_enrollment, crime_rate,
    health_ins_per_capita,
    ed_budget_share, fixed_costs_pct, spend_vs_required,
    chronic_absenteeism_pct, avg_teacher_salary, teachers_per_100_students,
    instructional_share, teachers_per_lowincome, teacher_pay_share,
]


# ---------------------------------------------------------------------------
# Exploratory candidates — the wider pool the statewide screen tests but no
# report has curated.  Metadata (unit/label) is best-effort; formulas are taken
# verbatim from the screen (factor_selection_scratch.add_derived_ratios).  Some
# are the screen's differently-named twin of a curated factor (noted inline);
# they stay under the screen's name so its panel keeps working unchanged.
# Registered as module attributes below, so `factors.<name>` works for all.
# ---------------------------------------------------------------------------

def _f(name, tier, kind, unit, hib, label, formula=None):
    return Factor(name, tier, kind, unit, hib, label, formula=formula, curated=False)

EXPLORATORY: list[Factor] = [
    nss_per_eqv,
    # ── Tier 1 — votable / fiscal ─────────────────────────────────────────────
    _f("res_tax_rate",        1, "raw", "rate",   None, "Residential tax rate (per $1,000)"),
    _f("com_tax_rate",        1, "raw", "rate",   None, "Commercial tax rate (per $1,000)"),
    _f("free_cash_pct",       1, "raw", "pct",    True, "Free cash as % of budget"),
    _f("stabilization_pct",   1, "raw", "pct",    True, "Stabilization fund as % of budget"),
    _f("new_growth_pct_levy", 1, "raw", "pct",    True, "New growth as % of prior-year levy"),
    _f("ppe_in_district",     1, "raw", "dollar", True, "In-district spending per pupil"),
    _f("spend_per_eqv",       1, "derived", "rate", True, "In-district PPE vs property wealth (≈ nss_per_eqv)",
       "ppe_in_district / eqv_per_capita"),
    _f("debt_service_share",  1, "derived", "pct", False, "Debt service % of municipal budget",
       "muni_debt_service / muni_total_exp × 100"),
    _f("capital_school_share",1, "derived", "pct100", True, "Schools' share of capital spending",
       "capital_schools / capital_total"),
    _f("tax_pct_rev",         1, "derived", "pct", None, "Taxes as % of municipal revenue",
       "muni_taxes / muni_total_rev × 100"),
    _f("state_aid_pct",       1, "derived", "pct", None, "State aid as % of revenue",
       "muni_state_rev / muni_total_rev × 100"),
    # ── Tier 2 — managed: raw per-pupil spending lines ────────────────────────
    _f("attendance_rate_pct", 2, "raw", "pct",    True, "Attendance rate"),
    _f("teacher_avg_salary",  2, "raw", "dollar", True, "Average teacher salary (≈ avg_teacher_salary)"),
    _f("ppe_teachers",        2, "raw", "dollar", True, "Teacher spending per pupil"),
    _f("ppe_administration",  2, "raw", "dollar", None, "Administration spending per pupil"),
    _f("ppe_pupil_services",  2, "raw", "dollar", True, "Pupil-services spending per pupil"),
    _f("ppe_instr_leadership",2, "raw", "dollar", True, "Instructional leadership per pupil"),
    _f("ppe_prof_dev",        2, "raw", "dollar", True, "Professional development per pupil"),
    _f("ppe_guidance",        2, "raw", "dollar", True, "Guidance / counseling per pupil"),
    _f("ppe_materials",       2, "raw", "dollar", True, "Instructional materials per pupil"),
    _f("ch70_aid_per_pupil",  2, "raw", "dollar", None, "Chapter 70 aid per pupil"),
    # ── Tier 2 — managed: derived staffing / spending-mix ─────────────────────
    _f("student_teacher_ratio",   2, "derived", "rate", False, "Students per teacher",
       "total_enrollment / teacher_fte"),
    _f("paras_per_100_students",  2, "derived", "rate", True, "Paraprofessionals per 100 students",
       "para_fte / total_enrollment × 100"),
    _f("coaches_per_100_students",2, "derived", "rate", True, "Instructional coaches per 100 students",
       "instructional_coach_fte / total_enrollment × 100"),
    _f("sped_support_per_100",    2, "derived", "rate", True, "SPED support staff per 100 students",
       "sped_support_fte / total_enrollment × 100"),
    _f("salary_vs_income",        2, "derived", "rate", True, "Teacher salary vs local median income",
       "teacher_avg_salary / median_hh_income"),
    _f("admin_share",             2, "derived", "pct100", False, "Administration share of the school dollar",
       "ppe_administration / ppe_in_district"),
    _f("pupil_services_share",    2, "derived", "pct100", True, "Pupil-services share of the school dollar",
       "ppe_pupil_services / ppe_in_district"),
    _f("operations_share",        2, "derived", "pct100", None, "Operations share of the school dollar",
       "ppe_operations / ppe_in_district"),
    _f("benefits_share",          2, "derived", "pct100", None, "Benefits share of the school dollar",
       "ppe_insurance_retire / ppe_in_district"),
    _f("foundation_budget_pp",    2, "derived", "dollar", None, "Foundation budget per pupil",
       "foundation_budget / foundation_enrollment"),
    _f("fixed_costs_share",       2, "derived", "pct", False, "Fixed costs % of municipal budget (≈ fixed_costs_pct)",
       "muni_fixed_costs / muni_total_exp × 100"),
    _f("sped_support_per_100sped",2, "derived", "rate", True, "SPED specialists per 100 SPED students",
       "sped_support_fte / (sped_pct/100 × enrollment) × 100"),
    _f("paras_per_100_sped",      2, "derived", "rate", True, "Paras per 100 SPED students",
       "para_fte / (sped_pct/100 × enrollment) × 100"),
    _f("paras_per_100_highneed",  2, "derived", "rate", True, "Paras per 100 high-need students",
       "para_fte / (high_needs_pct/100 × enrollment) × 100"),
    _f("support_per_100_ell",     2, "derived", "rate", True, "Support staff per 100 ELL students",
       "instructional_support_fte / (ell_pct/100 × enrollment) × 100"),
    _f("para_teacher_ratio",      2, "derived", "rate", None, "Paraprofessionals per teacher",
       "para_fte / teacher_fte"),
    _f("adults_per_100_students", 2, "derived", "rate", True, "Instructional adults per 100 students",
       "(teacher + para + coach + sped-support + instr-support FTE) / enrollment × 100"),
    _f("para_share_of_staff",     2, "derived", "pct100", None, "Para share of teaching staff",
       "para_fte / (teacher_fte + para_fte)"),
    _f("coaches_per_100_teachers",2, "derived", "rate", True, "Coaches per 100 teachers",
       "instructional_coach_fte / teacher_fte × 100"),
    _f("pupil_svc_per_need",      2, "derived", "dollar", True, "Pupil-services $ per unit of high need",
       "ppe_pupil_services / (high_needs_pct/100)"),
    _f("pd_per_teacher",          2, "derived", "dollar", True, "PD spending per teacher",
       "ppe_prof_dev × enrollment / teacher_fte"),
    # ── Tier 3 — structural: raw demographics / wealth / health ───────────────
    _f("income_per_capita",   3, "raw", "dollar", None, "Income per capita"),
    _f("eqv_per_capita",      3, "raw", "dollar", None, "Equalized valuation per capita (≈ equalized_income)"),
    _f("poverty_pct",         3, "raw", "pct",    None, "Poverty rate"),
    _f("high_needs_pct",      3, "raw", "pct",    None, "High-needs student share"),
    _f("flne_pct",            3, "raw", "pct",    None, "First-language-not-English share"),
    _f("foundation_enrollment",3,"raw", "count",  None, "Foundation enrollment"),
    _f("total_population",    3, "raw", "count",  None, "Town population"),
    _f("muni_pop",            3, "raw", "count",  None, "Municipal population"),
    _f("pct_65_plus",         3, "raw", "pct",    None, "Residents aged 65+"),
    _f("pct_under18",         3, "raw", "pct",    None, "Residents under 18"),
    _f("median_age",          3, "raw", "count",  None, "Median age"),
    _f("acs_unemployment",    3, "raw", "pct",    None, "Unemployment rate (ACS)"),
    _f("pct_foreign_born",    3, "raw", "pct",    None, "Foreign-born share"),
    _f("pct_single_parent",   3, "raw", "pct",    None, "Single-parent household share"),
    _f("total_av",            3, "raw", "dollar", None, "Total assessed value"),
    _f("res_av",              3, "raw", "dollar", None, "Residential assessed value"),
    _f("home_value",          3, "raw", "dollar", None, "Zillow home value (ZHVI)"),
    _f("median_sale_price",   3, "raw", "dollar", None, "Median home sale price"),
    _f("gf_exp_per_capita",   3, "raw", "dollar", None, "General-fund spending per capita"),
    _f("new_growth_res_pct",  3, "raw", "pct",    None, "Residential share of new growth"),
    _f("pct_fair_poor_health",3, "raw", "pct",    None, "Adults in fair/poor health"),
    _f("avg_mentally_unhealthy_days", 3, "raw", "count", None, "Avg mentally-unhealthy days / month"),
    _f("pct_smokers",         3, "raw", "pct",    None, "Adult smokers"),
    _f("pct_obese",           3, "raw", "pct",    None, "Adult obesity"),
    _f("pct_children_poverty",3, "raw", "pct",    None, "Children in poverty"),
    _f("pct_children_single_parent", 3, "raw", "pct", None, "Children in single-parent homes"),
    _f("pct_uninsured",       3, "raw", "pct",    None, "Uninsured residents"),
    _f("county_unemployment", 3, "raw", "pct",    None, "County unemployment rate"),
    # ── Tier 3 — structural: derived municipal / tax-base ratios ──────────────
    _f("commercial_av_share", 3, "derived", "pct", None, "Commercial/industrial share of the tax base",
       "(commercial_av + industrial_av) / total_av × 100"),
    _f("violent_rate",        3, "derived", "count", None, "Violent crimes per 100k residents",
       "violent_crimes / crime_pop × 100000"),
    _f("muni_avg_salary",     3, "derived", "dollar", None, "Municipal employee average salary",
       "muni_salaries / muni_employees"),
    _f("public_safety_share", 3, "derived", "pct", None, "Public safety % of municipal budget",
       "muni_pub_safety / muni_total_exp × 100"),
    _f("public_works_share",  3, "derived", "pct", None, "Public works % of municipal budget",
       "muni_pub_works / muni_total_exp × 100"),
    _f("gen_gov_share",       3, "derived", "pct", None, "General government % of municipal budget",
       "muni_gen_gov / muni_total_exp × 100"),
    _f("intergov_pct",        3, "derived", "pct", None, "Intergovernmental revenue % of revenue",
       "muni_intergov / muni_total_rev × 100"),
]

# Register every exploratory factor as a module attribute → factors.<name> works.
for _factor in EXPLORATORY:
    globals().setdefault(_factor.name, _factor)


# ---------------------------------------------------------------------------
# Registry + lookups (all derived from the Factor objects above)
# ---------------------------------------------------------------------------

LIBRARY: list[Factor] = CURATED + EXPLORATORY

FACTOR_CATALOG: dict[str, Factor] = {f.name: f for f in LIBRARY}
CURATED_FACTORS: set[str] = {f.name for f in LIBRARY if f.curated}


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
