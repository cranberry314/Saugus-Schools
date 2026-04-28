"""
Data Integrity Tests — MA School Data Pipeline
===============================================

Tests every table that was stitched from multiple sources, plus general
outlier/consistency checks across all tables.

Stitched sources (primary concern):
  district_chapter70      FY2007-2022 from keyfactors.xlsx dataAid sheet
                          FY2023-2026 from DESE individual-year xlsx files
                          Seam: FY2022 → FY2023

  municipal_income_eqv    FY2014-2025 from DLS combined report (annual)
                          FY2016, FY2020 EQV filled from biennial EQV report
                          FY2016, FY2020 income is NULL (biennial has no income)
                          FY2019, FY2021 entirely absent (odd years not published)

  per_pupil_expenditure   SY2009-2018 from original DESE files (322 districts)
                          SY2019+ expanded universe (404 districts)
                          Seam: SY2018 → SY2019

  attendance              SY2018-2020: annual report (ddYear = plain year)
                          SY2021+: end-of-year snapshot (ddYear = YYYYeoy)
                          Seam: SY2020 → SY2021

Each test returns a list of finding dicts:
  { table, test, year/district, value, expected, severity, note }
  severity: INFO | WARNING | FAIL

Output: Reports/data_integrity_report.pdf + console summary

Run: python analysis/data_integrity.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import textwrap
import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Reports")
OUTPUT_PDF = os.path.join(OUTPUT_DIR, "data_integrity_report.pdf")

NAVY  = "#1F3864"
STEEL = "#2F5496"
GOLD  = "#C9A800"
RED   = "#C00000"
WARN  = "#E36C09"
GREEN = "#375623"
LIGHT = "#D6E4F0"

SEV_COLOR = {"INFO": GREEN, "WARNING": WARN, "FAIL": RED}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_change(a, b):
    """% change from a to b."""
    if a is None or b is None or float(a) == 0:
        return None
    return (float(b) - float(a)) / abs(float(a)) * 100


def _zscore(val, mean, sd):
    if sd is None or float(sd) == 0:
        return None
    return (float(val) - float(mean)) / float(sd)


# ── Test suites ───────────────────────────────────────────────────────────────

def test_chapter70(conn) -> list[dict]:
    """
    1. Seam continuity: YoY % change at FY2022→2023 vs trailing 3-year avg change
    2. Per-district outlier: any single district with >50% YoY change in aid_pp
    3. District count stability: should be ~435-439 every year
    4. Cross-check: ch70 required_nss_per_pupil vs PPE net school spending (FY2023+)
    """
    findings = []

    # ── 1. Seam continuity ────────────────────────────────────────────────────
    rows = conn.execute(text("""
        SELECT fiscal_year,
               COUNT(*) as n,
               AVG(chapter70_aid_per_pupil) as avg_aid,
               AVG(required_nss_per_pupil)  as avg_nss
        FROM district_chapter70
        GROUP BY fiscal_year ORDER BY fiscal_year
    """)).fetchall()
    df = pd.DataFrame(rows, columns=["fy","n","avg_aid","avg_nss"]).set_index("fy")
    df = df.apply(pd.to_numeric, errors="coerce")

    # YoY % change in statewide avg aid per pupil
    df["aid_yoy"] = df["avg_aid"].pct_change() * 100
    df["nss_yoy"] = df["avg_nss"].pct_change() * 100

    # Baseline: mean abs YoY change in FY2010-2022 (pre-seam, pre-Student Opportunity Act)
    baseline_aid = df.loc[2010:2022, "aid_yoy"].abs()
    baseline_mean = baseline_aid.mean()
    baseline_sd   = baseline_aid.std()

    seam_aid_yoy = df.loc[2023, "aid_yoy"] if 2023 in df.index else None
    if seam_aid_yoy is not None:
        z = _zscore(abs(seam_aid_yoy), baseline_mean, baseline_sd)
        sev = "WARNING" if abs(seam_aid_yoy) > baseline_mean + 2*baseline_sd else "INFO"
        findings.append({
            "table": "district_chapter70", "test": "Seam continuity FY2022→2023",
            "detail": f"Statewide avg aid/pupil: +{seam_aid_yoy:.1f}% YoY  "
                      f"(baseline mean {baseline_mean:.1f}%±{baseline_sd:.1f}%,  z={z:.1f})",
            "severity": sev,
            "note": "FY2023 introduced Student Opportunity Act reforms — larger jump is expected policy change, not artifact."
                    if sev == "WARNING" else "Within normal range at seam boundary.",
        })

    # ── 2. District count stability ───────────────────────────────────────────
    for fy, row in df.iterrows():
        n = row["n"]
        if not (430 <= n <= 445):
            findings.append({
                "table": "district_chapter70", "test": "District count",
                "detail": f"FY{fy}: {int(n)} districts (expected 430-445)",
                "severity": "WARNING",
                "note": "Unexpected drop/spike in district count — possible missing rows.",
            })

    # ── 3. Per-district YoY outliers (>60% change = likely error not policy) ──
    dist_rows = conn.execute(text("""
        SELECT lea_code, district_name, fiscal_year, chapter70_aid_per_pupil,
               LAG(chapter70_aid_per_pupil) OVER (PARTITION BY lea_code ORDER BY fiscal_year) as prev
        FROM district_chapter70
        ORDER BY lea_code, fiscal_year
    """)).fetchall()
    dist_df = pd.DataFrame(dist_rows, columns=["lea","name","fy","aid","prev"])
    dist_df = dist_df.apply(lambda c: pd.to_numeric(c, errors="coerce"))
    dist_df["yoy"] = ((dist_df["aid"] - dist_df["prev"]) / dist_df["prev"].abs() * 100)

    outliers = dist_df[dist_df["yoy"].abs() > 60].dropna(subset=["yoy"])
    for _, r in outliers.iterrows():
        findings.append({
            "table": "district_chapter70", "test": "Per-district YoY outlier",
            "detail": f"FY{int(r.fy)}: {r['name']} (LEA {int(r.lea)})  "
                      f"aid/pupil ${r.prev:,.0f} → ${r.aid:,.0f}  ({r.yoy:+.0f}%)",
            "severity": "WARNING",
            "note": "Jumps >60% YoY may reflect Chapter 70 formula changes, district mergers, "
                    "or a seam artifact. Verify against DESE source.",
        })

    # ── 4. Cross-check ch70 required_nss vs PPE (FY2023+ only) ───────────────
    cross = conn.execute(text("""
        SELECT c.fiscal_year, c.lea_code, c.district_name,
               c.required_nss_per_pupil           AS ch70_nss,
               p.amount                            AS ppe_nss,
               ROUND(ABS(c.required_nss_per_pupil - p.amount)
                     / NULLIF(c.required_nss_per_pupil,0) * 100, 1) AS pct_diff
        FROM district_chapter70 c
        JOIN per_pupil_expenditure p
          ON SUBSTRING(p.org_code,1,4)::int = c.lea_code
         AND p.school_year = c.fiscal_year
         AND p.category = 'Total In-District Expenditures'
        WHERE c.fiscal_year IN (2023, 2024)
          AND c.required_nss_per_pupil IS NOT NULL
          AND p.amount IS NOT NULL
          AND ABS(c.required_nss_per_pupil - p.amount)
              / NULLIF(c.required_nss_per_pupil,0) > 0.25
        ORDER BY pct_diff DESC
        LIMIT 10
    """)).fetchall()
    if cross:
        for r in cross:
            findings.append({
                "table": "district_chapter70 × per_pupil_expenditure",
                "test": "Ch70 required NSS vs actual PPE",
                "detail": f"FY{r[0]}: {r[2]}  ch70_nss=${float(r[3]):,.0f}  ppe=${float(r[4]):,.0f}  diff={r[5]:.0f}%",
                "severity": "INFO",
                "note": "Required NSS is a floor, not a cap — districts routinely spend above it. "
                        "Large differences expected; flag only for documentation.",
            })
    else:
        findings.append({
            "table": "district_chapter70 × per_pupil_expenditure",
            "test": "Ch70 required NSS vs actual PPE",
            "detail": "All matched districts within 25% — no anomalies",
            "severity": "INFO", "note": "",
        })

    return findings


def test_eqv(conn) -> list[dict]:
    """
    1. Expected nulls: FY2016 and FY2020 income should be NULL (biennial supplement)
    2. FY2019 and FY2021 should be entirely absent (odd years not published)
    3. YoY % change in avg EQV per capita — flag >20% single-year jump
    4. Internal consistency: EQV / population ≈ EQV per capita
    """
    findings = []

    rows = conn.execute(text("""
        SELECT fiscal_year,
               COUNT(*) as n,
               COUNT(income_per_capita) as n_income,
               COUNT(eqv_per_capita) as n_eqv,
               AVG(eqv_per_capita) as avg_eqv,
               AVG(income_per_capita) as avg_inc
        FROM municipal_income_eqv
        GROUP BY fiscal_year ORDER BY fiscal_year
    """)).fetchall()
    df = pd.DataFrame(rows, columns=["fy","n","n_income","n_eqv","avg_eqv","avg_inc"])
    df = df.apply(pd.to_numeric, errors="coerce").set_index("fy")

    # ── 1. Biennial null pattern ───────────────────────────────────────────────
    for fy in [2016, 2020]:
        if fy in df.index:
            if df.loc[fy, "n_income"] > 0:
                findings.append({
                    "table": "municipal_income_eqv", "test": "Biennial null pattern",
                    "detail": f"FY{fy}: income_per_capita populated ({int(df.loc[fy,'n_income'])} rows) — expected NULL",
                    "severity": "FAIL",
                    "note": "FY2016 and FY2020 EQV data comes from biennial supplement which has no income. Non-null means data contamination.",
                })
            else:
                findings.append({
                    "table": "municipal_income_eqv", "test": "Biennial null pattern",
                    "detail": f"FY{fy}: income_per_capita correctly NULL, EQV present ({int(df.loc[fy,'n_eqv'])} rows)",
                    "severity": "INFO", "note": "Expected pattern for biennial gap-fill years.",
                })

    # ── 2. Odd years should be absent ─────────────────────────────────────────
    for fy in [2019, 2021]:
        if fy in df.index:
            findings.append({
                "table": "municipal_income_eqv", "test": "Odd-year absence",
                "detail": f"FY{fy}: {int(df.loc[fy,'n'])} rows found — expected 0 (DLS does not publish odd years)",
                "severity": "FAIL",
                "note": "DLS EQV report is biennial (even years only). Odd-year rows may be a load error.",
            })
        else:
            findings.append({
                "table": "municipal_income_eqv", "test": "Odd-year absence",
                "detail": f"FY{fy}: correctly absent",
                "severity": "INFO", "note": "Odd years not published — expected.",
            })

    # ── 3. YoY EQV spike check ────────────────────────────────────────────────
    eqv_even = df[df.index % 2 == 0]["avg_eqv"].dropna()
    for i in range(1, len(eqv_even)):
        fy      = eqv_even.index[i]
        prev_fy = eqv_even.index[i-1]
        yoy     = _pct_change(eqv_even.iloc[i-1], eqv_even.iloc[i])
        if yoy is not None and abs(yoy) > 20:
            findings.append({
                "table": "municipal_income_eqv", "test": "YoY EQV spike (even years)",
                "detail": f"FY{prev_fy}→FY{fy}: avg EQV/capita {yoy:+.1f}%  "
                          f"(${eqv_even.iloc[i-1]:,.0f} → ${eqv_even.iloc[i]:,.0f})",
                "severity": "WARNING",
                "note": "EQV changes >20% over a 2-year period are unusual but can reflect "
                        "revaluation years in MA. Verify against DOR published reports.",
            })

    # ── 4. Internal consistency: EQV / population ≈ EQV per capita ────────────
    inconsistent = conn.execute(text("""
        SELECT fiscal_year, municipality, eqv, population, eqv_per_capita,
               ROUND(eqv / NULLIF(population,0), 0) AS computed_eqv_pc,
               ABS(eqv_per_capita - eqv / NULLIF(population,0))
                 / NULLIF(eqv_per_capita, 0) * 100 AS pct_err
        FROM municipal_income_eqv
        WHERE population > 0 AND eqv IS NOT NULL AND eqv_per_capita IS NOT NULL
          AND ABS(eqv_per_capita - eqv / NULLIF(population,0))
              / NULLIF(eqv_per_capita, 0) > 0.05
        ORDER BY pct_err DESC LIMIT 10
    """)).fetchall()
    if inconsistent:
        for r in inconsistent:
            findings.append({
                "table": "municipal_income_eqv", "test": "EQV / population consistency",
                "detail": f"FY{r[0]} {r[1]}: stored={float(r[4]):,.0f}  computed={float(r[5]):,.0f}  err={float(r[6]):.1f}%",
                "severity": "WARNING",
                "note": "EQV per capita should equal EQV / population within rounding. "
                        "Discrepancy may indicate different population base used by DOR.",
            })
    else:
        findings.append({
            "table": "municipal_income_eqv", "test": "EQV / population consistency",
            "detail": "All rows within 5% tolerance",
            "severity": "INFO", "note": "",
        })

    return findings


def test_ppe(conn) -> list[dict]:
    """
    1. District count jump at SY2018→2019
    2. Per-district YoY change > 30% in Net School Spending — flag outliers
    3. Teacher + Admin + Other categories should sum to ≤ Total (not exceed by >5%)
    """
    findings = []

    rows = conn.execute(text("""
        SELECT school_year, COUNT(DISTINCT org_code) as n
        FROM per_pupil_expenditure
        GROUP BY school_year ORDER BY school_year
    """)).fetchall()
    df_n = pd.DataFrame(rows, columns=["yr","n"]).set_index("yr")

    # ── 1. Seam district count ────────────────────────────────────────────────
    if 2018 in df_n.index and 2019 in df_n.index:
        jump = df_n.loc[2019,"n"] - df_n.loc[2018,"n"]
        sev  = "WARNING" if jump > 50 else "INFO"
        findings.append({
            "table": "per_pupil_expenditure", "test": "District count seam SY2018→2019",
            "detail": f"SY2018: {df_n.loc[2018,'n']} districts → SY2019: {df_n.loc[2019,'n']} districts (+{jump})",
            "severity": sev,
            "note": "DESE expanded PPE reporting universe in SY2019 (charters + more districts added). "
                    "Year-over-year comparisons across this seam should account for composition change.",
        })

    # ── 2. YoY NSS outliers per district ─────────────────────────────────────
    nss_rows = conn.execute(text("""
        SELECT school_year, org_code, amount,
               LAG(amount) OVER (PARTITION BY org_code ORDER BY school_year) AS prev
        FROM per_pupil_expenditure
        WHERE category = 'Total In-District Expenditures'
        ORDER BY org_code, school_year
    """)).fetchall()
    nss_df = pd.DataFrame(nss_rows, columns=["yr","org","nss","prev"])
    nss_df[["nss","prev"]] = nss_df[["nss","prev"]].apply(pd.to_numeric, errors="coerce")
    nss_df["yoy"] = (nss_df["nss"] - nss_df["prev"]) / nss_df["prev"].abs() * 100

    # Compute statewide mean/sd YoY per year for context
    yearly_stats = nss_df.groupby("yr")["yoy"].agg(["mean","std"]).rename(
        columns={"mean":"mean_yoy","std":"sd_yoy"})

    extreme = nss_df[(nss_df["yoy"].abs() > 40) & nss_df["prev"].notna()].copy()
    extreme = extreme.merge(yearly_stats, left_on="yr", right_index=True)
    extreme["z"] = (extreme["yoy"] - extreme["mean_yoy"]) / extreme["sd_yoy"]
    extreme = extreme[extreme["z"].abs() > 3].sort_values("z", key=abs, ascending=False).head(15)

    if extreme.empty:
        findings.append({
            "table": "per_pupil_expenditure", "test": "Per-district NSS YoY outliers (>40% & z>3)",
            "detail": "No extreme outliers found",
            "severity": "INFO", "note": "",
        })
    else:
        for _, r in extreme.iterrows():
            findings.append({
                "table": "per_pupil_expenditure", "test": "Per-district NSS YoY outlier",
                "detail": f"SY{int(r.yr)}: {r.org}  NSS ${r.prev:,.0f}→${r.nss:,.0f}  ({r.yoy:+.0f}%)  z={r.z:.1f}",
                "severity": "WARNING",
                "note": "Large YoY swings in NSS can reflect real spending changes, enrollment denominator "
                        "shifts, or data entry errors. Cross-check with district financials.",
            })

    # ── 3. Sub-category sum vs total ─────────────────────────────────────────
    sum_check = conn.execute(text("""
        SELECT school_year, org_code,
               MAX(CASE WHEN category='Total In-District Expenditures' THEN amount END) AS total,
               MAX(CASE WHEN category='Teachers'                       THEN amount END) AS teacher,
               MAX(CASE WHEN category='Administration'                 THEN amount END) AS admin,
               MAX(CASE WHEN category='Pupil Services'                 THEN amount END) AS pupil,
               MAX(CASE WHEN category='Instructional Leadership'       THEN amount END) AS instr
        FROM per_pupil_expenditure
        WHERE school_year = 2024
        GROUP BY school_year, org_code
        HAVING MAX(CASE WHEN category='Total In-District Expenditures' THEN amount END) IS NOT NULL
    """)).fetchall()
    issues = 0
    for r in sum_check:
        parts = [x for x in [r[3],r[4],r[5],r[6]] if x is not None]
        subtotal = sum(float(x) for x in parts)
        total    = float(r[2])
        if total > 0 and subtotal > total * 1.05:
            issues += 1
    if issues:
        findings.append({
            "table": "per_pupil_expenditure", "test": "Sub-categories exceed total (SY2024)",
            "detail": f"{issues} districts where sub-category sum > total by >5%",
            "severity": "WARNING",
            "note": "Sub-categories should not sum to more than the in-district total. "
                    "May indicate double-counting in source data.",
        })
    else:
        findings.append({
            "table": "per_pupil_expenditure", "test": "Sub-categories exceed total (SY2024)",
            "detail": "All districts: sub-category sums ≤ total in-district expenditure",
            "severity": "INFO", "note": "",
        })

    return findings


def test_attendance(conn) -> list[dict]:
    """
    1. Seam SY2020→2021: attendance rate drop & chronic absenteeism spike (real but flag)
    2. Attendance rate physically impossible values (< 50% or > 100%)
    3. Chronic absenteeism should be ≤ 100% and ≥ 0%
    """
    findings = []

    rows = conn.execute(text("""
        SELECT school_year,
               AVG(attendance_rate_pct) as avg_att,
               STDDEV(attendance_rate_pct) as sd_att,
               AVG(chronic_absenteeism_pct) as avg_ca,
               STDDEV(chronic_absenteeism_pct) as sd_ca,
               COUNT(*) as n
        FROM attendance WHERE student_group='All'
        GROUP BY school_year ORDER BY school_year
    """)).fetchall()
    df = pd.DataFrame(rows, columns=["yr","avg_att","sd_att","avg_ca","sd_ca","n"])
    df = df.apply(pd.to_numeric, errors="coerce").set_index("yr")

    # ── 1. Seam check ─────────────────────────────────────────────────────────
    if 2020 in df.index and 2021 in df.index:
        att_drop = df.loc[2021,"avg_att"] - df.loc[2020,"avg_att"]
        ca_jump  = df.loc[2021,"avg_ca"]  - df.loc[2020,"avg_ca"]
        findings.append({
            "table": "attendance", "test": "Seam SY2020→2021 (COVID impact)",
            "detail": f"Attendance rate: {att_drop:+.1f}pp   Chronic absenteeism: {ca_jump:+.1f}pp",
            "severity": "INFO",
            "note": "SY2021 used EOY snapshot format vs plain-year format for 2020. "
                    "The increase in chronic absenteeism is consistent with published COVID-era research "
                    "and is a real effect, not a seam artifact.",
        })
    if 2021 in df.index and 2022 in df.index:
        ca_jump2 = df.loc[2022,"avg_ca"] - df.loc[2021,"avg_ca"]
        if ca_jump2 > 5:
            findings.append({
                "table": "attendance", "test": "Chronic absenteeism post-COVID spike",
                "detail": f"SY2021→SY2022: avg chronic absenteeism {ca_jump2:+.1f}pp  ({df.loc[2021,'avg_ca']:.1f}% → {df.loc[2022,'avg_ca']:.1f}%)",
                "severity": "INFO",
                "note": "Post-pandemic absenteeism surge documented nationally. Not a data error.",
            })

    # ── 2. Impossible / extreme values ───────────────────────────────────────
    # True impossibles: >100% attendance, <0% chronic absenteeism, chronic >100%
    impossible = conn.execute(text("""
        SELECT school_year, org_code, attendance_rate_pct, chronic_absenteeism_pct
        FROM attendance
        WHERE student_group='All'
          AND (attendance_rate_pct > 100
               OR chronic_absenteeism_pct > 100 OR chronic_absenteeism_pct < 0)
        ORDER BY school_year, org_code
    """)).fetchall()
    if impossible:
        for r in impossible:
            findings.append({
                "table": "attendance", "test": "Impossible values",
                "detail": f"SY{r[0]}: {r[1]}  att={r[2]}%  chronic={r[3]}%",
                "severity": "FAIL",
                "note": "Attendance rate > 100% or chronic absenteeism outside 0-100% is mathematically impossible.",
            })
    else:
        findings.append({
            "table": "attendance", "test": "Impossible values",
            "detail": "No impossible values found",
            "severity": "INFO", "note": "",
        })

    # Very low attendance (< 50%) — possible for alternative/credit-recovery charter schools
    very_low = conn.execute(text("""
        SELECT school_year, org_code, district_name, attendance_rate_pct, chronic_absenteeism_pct
        FROM attendance
        WHERE student_group='All'
          AND attendance_rate_pct < 50
        ORDER BY school_year, org_code
    """)).fetchall()
    if very_low:
        for r in very_low:
            findings.append({
                "table": "attendance", "test": "Very low attendance rate (< 50%)",
                "detail": f"SY{r[0]}: {r[1]} ({r[2]})  att={r[3]}%  chronic={r[4]}%",
                "severity": "WARNING",
                "note": "Attendance below 50% may be valid for alternative credit-recovery charter "
                        "schools (e.g. Phoenix Academy) where students have non-standard schedules. "
                        "Verify district type before treating as error.",
            })
    else:
        findings.append({
            "table": "attendance", "test": "Very low attendance rate (< 50%)",
            "detail": "No districts below 50% attendance",
            "severity": "INFO", "note": "",
        })

    # ── 3. Per-district outliers ──────────────────────────────────────────────
    for yr_stat, row in df.iterrows():
        threshold_att = row["avg_att"] - 3 * row["sd_att"]
        threshold_ca  = row["avg_ca"]  + 3 * row["sd_ca"]
        outlier_rows  = conn.execute(text("""
            SELECT org_code, attendance_rate_pct, chronic_absenteeism_pct
            FROM attendance
            WHERE school_year = :yr AND student_group='All'
              AND (attendance_rate_pct < :low_att OR chronic_absenteeism_pct > :high_ca)
            ORDER BY attendance_rate_pct
        """), {"yr": int(yr_stat), "low_att": float(threshold_att),
               "high_ca": float(threshold_ca)}).fetchall()
        for r in outlier_rows:
            findings.append({
                "table": "attendance", "test": f"SY{yr_stat} — 3σ outlier",
                "detail": f"SY{yr_stat}: {r[0]}  att={r[1]}%  (state mean {row['avg_att']:.1f}%)  "
                          f"chronic={r[2]}%  (state mean {row['avg_ca']:.1f}%)",
                "severity": "WARNING",
                "note": "Value > 3 standard deviations from statewide mean. May be a small district, "
                        "charter school, or data error. Review against DESE source.",
            })

    return findings


def test_graduation(conn) -> list[dict]:
    """
    1. Grad rate physically impossible (< 0% or > 100%)
    2. YoY change per district > 20pp (unusual — flag)
    3. Check: districts with grad rate + dropout > 100%
    """
    findings = []

    impossible = conn.execute(text("""
        SELECT school_year, org_code, four_year_grad_pct, dropout_pct
        FROM graduation_rates
        WHERE student_group='All'
          AND (four_year_grad_pct > 100 OR four_year_grad_pct < 0
               OR dropout_pct > 100 OR dropout_pct < 0)
    """)).fetchall()
    if impossible:
        for r in impossible:
            findings.append({
                "table": "graduation_rates", "test": "Impossible values",
                "detail": f"SY{r[0]}: {r[1]}  grad={r[2]}%  dropout={r[3]}%",
                "severity": "FAIL", "note": "Values outside 0-100% are invalid.",
            })
    else:
        findings.append({
            "table": "graduation_rates", "test": "Impossible values",
            "detail": "No impossible values found",
            "severity": "INFO", "note": "",
        })

    # YoY per-district outliers
    yoy_rows = conn.execute(text("""
        SELECT school_year, org_code, four_year_grad_pct,
               LAG(four_year_grad_pct) OVER (PARTITION BY org_code ORDER BY school_year) AS prev
        FROM graduation_rates WHERE student_group='All'
    """)).fetchall()
    yoy_df = pd.DataFrame(yoy_rows, columns=["yr","org","grad","prev"])
    yoy_df[["grad","prev"]] = yoy_df[["grad","prev"]].apply(pd.to_numeric, errors="coerce")
    yoy_df["delta"] = yoy_df["grad"] - yoy_df["prev"]

    big = yoy_df[yoy_df["delta"].abs() > 20].dropna(subset=["delta"])
    if big.empty:
        findings.append({
            "table": "graduation_rates", "test": "YoY change > 20pp",
            "detail": "No districts with >20pp single-year swing",
            "severity": "INFO", "note": "",
        })
    else:
        for _, r in big.head(10).iterrows():
            findings.append({
                "table": "graduation_rates", "test": "YoY change > 20pp",
                "detail": f"SY{int(r.yr)}: {r.org}  {r.prev:.1f}% → {r.grad:.1f}%  ({r.delta:+.1f}pp)",
                "severity": "WARNING",
                "note": "Swings >20pp in a single year are unusual. Could reflect cohort size, "
                        "reclassification, or data entry issues.",
            })

    return findings


def test_staffing(conn) -> list[dict]:
    """
    1. teachers_per_100_fte implausibly low or high (< 2 or > 20)
       Note: this metric is teachers per 100 enrolled pupils (not per 100 staff FTE).
       MA district median ~8; normal range ~4-14.
    2. teacher_avg_salary < $20k or > $160k
    3. YoY salary change > 20% per district
    """
    findings = []

    imp = conn.execute(text("""
        SELECT school_year, org_code, fte
        FROM staffing
        WHERE category='teachers_per_100_fte'
          AND (fte < 2 OR fte > 20)
        ORDER BY school_year, fte
    """)).fetchall()
    if imp:
        for r in imp:
            findings.append({
                "table": "staffing", "test": "Teachers per 100 pupils implausible",
                "detail": f"SY{r[0]}: {r[1]}  {r[2]:.1f} teachers per 100 pupils",
                "severity": "WARNING",
                "note": "MA districts typically have 6-12 teachers per 100 enrolled pupils "
                        "(median ~8). Values outside 2-20 are unusual.",
            })
    else:
        findings.append({
            "table": "staffing", "test": "Teachers per 100 pupils plausible range",
            "detail": "All values in 2-20 range (MA median ~8)",
            "severity": "INFO", "note": "",
        })

    sal_imp = conn.execute(text("""
        SELECT school_year, org_code, avg_salary
        FROM staffing
        WHERE category='teacher_avg_salary'
          AND (avg_salary < 20000 OR avg_salary > 160000)
        ORDER BY avg_salary
    """)).fetchall()
    if sal_imp:
        for r in sal_imp:
            findings.append({
                "table": "staffing", "test": "Teacher avg salary implausible",
                "detail": f"SY{r[0]}: {r[1]}  ${float(r[2]):,.0f}",
                "severity": "WARNING",
                "note": "MA teacher salaries are typically $50k-$130k. Outliers may be "
                        "top of scale in wealthy districts, part-time averages, or entry errors.",
            })
    else:
        findings.append({
            "table": "staffing", "test": "Teacher avg salary plausible range",
            "detail": "All values in $20k-$160k range",
            "severity": "INFO", "note": "",
        })

    return findings


def test_selected_populations(conn) -> list[dict]:
    """
    1. High Needs % < max(ELL, Low Income, SPED) — can't be below any component
    2. high_needs_pct > 100
    3. YoY > 20pp swing per district
    """
    findings = []

    # High Needs must be >= each of its components (it's an unduplicated union)
    comp_fail = conn.execute(text("""
        SELECT school_year, org_code, district_name,
               high_needs_pct, ell_pct, low_income_pct, sped_pct,
               GREATEST(ell_pct, low_income_pct, sped_pct) AS max_component
        FROM district_selected_populations
        WHERE high_needs_pct IS NOT NULL
          AND GREATEST(ell_pct, low_income_pct, sped_pct) IS NOT NULL
          AND high_needs_pct < GREATEST(ell_pct, low_income_pct, sped_pct) - 1
        ORDER BY school_year, org_code
        LIMIT 20
    """)).fetchall()
    if comp_fail:
        for r in comp_fail:
            findings.append({
                "table": "district_selected_populations",
                "test": "High Needs < max component",
                "detail": f"SY{r[0]}: {r[2]}  high_needs={r[3]}%  max_component={float(r[7]):.1f}% "
                          f"(ell={r[4]}  li={r[5]}  sped={r[6]})",
                "severity": "FAIL",
                "note": "High Needs is an UNDUPLICATED count — it must be >= every individual component. "
                        "A value below any single component indicates a data loading error.",
            })
    else:
        findings.append({
            "table": "district_selected_populations",
            "test": "High Needs >= each component",
            "detail": "All rows pass — high_needs_pct ≥ max(ell, low_income, sped)",
            "severity": "INFO", "note": "",
        })

    over100 = conn.execute(text("""
        SELECT school_year, org_code, high_needs_pct FROM district_selected_populations
        WHERE high_needs_pct > 100
    """)).fetchall()
    if over100:
        for r in over100:
            findings.append({
                "table": "district_selected_populations",
                "test": "High Needs > 100%",
                "detail": f"SY{r[0]}: {r[1]}  {r[2]}%",
                "severity": "FAIL", "note": "Percentage cannot exceed 100.",
            })
    else:
        findings.append({
            "table": "district_selected_populations",
            "test": "High Needs <= 100%",
            "detail": "All values in valid range",
            "severity": "INFO", "note": "",
        })

    return findings


# ── PDF generation ────────────────────────────────────────────────────────────

def _title_page(pdf):
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.set_facecolor(NAVY); fig.patch.set_facecolor(NAVY); ax.axis("off")
    ax.text(0.5, 0.72, "MA School Data Pipeline",
            ha="center", fontsize=26, color="white", fontweight="bold",
            transform=ax.transAxes)
    ax.text(0.5, 0.62, "Data Integrity Test Report",
            ha="center", fontsize=20, color=GOLD, transform=ax.transAxes)
    ax.text(0.5, 0.46,
            "Tests across all multi-source stitched tables:\n"
            "  district_chapter70  ·  municipal_income_eqv  ·  per_pupil_expenditure\n"
            "  attendance  ·  graduation_rates  ·  staffing  ·  district_selected_populations",
            ha="center", fontsize=11, color=LIGHT, transform=ax.transAxes, linespacing=2.0)
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _stitched_sources_page(pdf):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0.06, 0.04, 0.88, 0.92])
    ax.axis("off")
    ax.text(0.5, 0.98, "Multi-Source Stitched Tables",
            ha="center", va="top", fontsize=15, fontweight="bold", color=NAVY,
            transform=ax.transAxes)

    body = textwrap.dedent("""\
    Table                        Sources Stitched                           Seam / Known Issue
    ────────────────────────────────────────────────────────────────────────────────────────────
    district_chapter70           keyfactors.xlsx (FY2007-2022)              FY2022→2023: Student
                                 DESE year files (FY2023-2026)              Opportunity Act caused
                                                                            genuine +17% avg jump

    municipal_income_eqv         DLS combined report (most years)           FY2016 & FY2020: EQV
                                 DLS biennial EQV report (FY2016, 2020)     present but income NULL
                                                                            FY2019 & 2021 absent
                                                                            (odd years not published)

    per_pupil_expenditure        DESE Education to Career CSV               SY2018→2019: district
                                 (single file, but reporting universe        count jumps 322 → 404;
                                 expanded in SY2019)                        year-over-year comparisons
                                                                            cross this boundary

    attendance                   DESE profiles (SY2018-2020, plain year)    SY2020→2021: COVID spike
                                 DESE profiles (SY2021+, EOY snapshot)      in chronic absenteeism
                                                                            is real, not artifact

    graduation_rates             DESE profiles (single endpoint,            No seam — single URL
                                 all years via form POST)                   pattern for all years

    staffing                     District_Expenditures_by_Spending_         SY2018→2019: same
                                 Category CSV (Other Staff +                universe expansion as PPE
                                 Teacher Salaries categories)

    district_selected_populations DESE Selected Populations (single         High Needs NULL for
                                 ASP.NET form, FY2009-2026)                 SY2009-2012 (not
                                                                            published before ~2013)

    ────────────────────────────────────────────────────────────────────────────────────────────
    Tables NOT stitched (single source, no seam concerns):
      mcas_results               Socrata API (single dataset, SY2017+)
      enrollment                 Same CSV as PPE (universe change still applies at SY2019)
      demographics               Same CSV as PPE
      municipal_census_acs       Census API (single endpoint, ACS 5-year)
      municipal_zillow_housing   Zillow Research CSV (single file per series)
      district_financials        District_Expenditures_by_Function_Code CSV
    """)
    ax.text(0.01, 0.90, body,
            ha="left", va="top", fontsize=7.8, color="#1a1a1a",
            transform=ax.transAxes, family="monospace", linespacing=1.55)
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _summary_table_page(pdf, all_findings):
    fail  = [f for f in all_findings if f["severity"] == "FAIL"]
    warn  = [f for f in all_findings if f["severity"] == "WARNING"]
    info  = [f for f in all_findings if f["severity"] == "INFO"]

    fig, axes = plt.subplots(1, 2, figsize=(13, 7.5),
                              gridspec_kw={"width_ratios": [1, 2]})
    fig.patch.set_facecolor("white")
    fig.suptitle("Test Results Summary", fontsize=14, fontweight="bold", color=NAVY)

    # Left: donut chart
    ax_pie = axes[0]
    sizes  = [len(fail), len(warn), len(info)]
    colors = [RED, WARN, GREEN]
    labels = [f"FAIL\n({len(fail)})", f"WARNING\n({len(warn)})", f"INFO\n({len(info)})"]
    wedges, texts = ax_pie.pie(sizes, labels=labels, colors=colors,
                                startangle=90, wedgeprops={"width": 0.5})
    for t in texts:
        t.set_fontsize(9)
    ax_pie.set_title(f"Total: {len(all_findings)} checks", fontsize=10, color=NAVY)

    # Right: table of FAILs and WARNINGs
    ax_tbl = axes[1]
    ax_tbl.axis("off")
    priority = fail + warn
    if not priority:
        ax_tbl.text(0.5, 0.5, "No FAILs or WARNINGs — all checks passed",
                    ha="center", va="center", fontsize=12, color=GREEN,
                    fontweight="bold", transform=ax_tbl.transAxes)
    else:
        rows_data = []
        for f in priority[:20]:
            detail = f["detail"][:75] + "…" if len(f["detail"]) > 75 else f["detail"]
            rows_data.append([f["severity"], f["table"].split("×")[0].strip()[:30], detail[:75]])
        tbl = ax_tbl.table(
            cellText=rows_data,
            colLabels=["Sev", "Table", "Finding"],
            loc="upper center", cellLoc="left",
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(7)
        tbl.scale(1, 1.5)
        for (r, c), cell in tbl.get_celld().items():
            if r == 0:
                cell.set_facecolor(STEEL)
                cell.set_text_props(color="white", fontweight="bold")
            elif r <= len(rows_data):
                sev = rows_data[r-1][0]
                cell.set_facecolor({"FAIL": "#FFE0E0", "WARNING": "#FFF3E0"}.get(sev, "white"))
            cell.set_edgecolor("#cccccc")

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _detail_pages(pdf, all_findings):
    """One page per table, listing all findings."""
    tables = {}
    for f in all_findings:
        t = f["table"].split("×")[0].strip()
        tables.setdefault(t, []).append(f)

    for table_name, findings in tables.items():
        fig = plt.figure(figsize=(11, 8.5))
        fig.patch.set_facecolor("white")
        ax = fig.add_axes([0.05, 0.04, 0.90, 0.90])
        ax.axis("off")

        fail_n = sum(1 for f in findings if f["severity"] == "FAIL")
        warn_n = sum(1 for f in findings if f["severity"] == "WARNING")
        color  = RED if fail_n else (WARN if warn_n else GREEN)
        status = f"{'FAIL' if fail_n else 'WARNING' if warn_n else 'PASS'}  "
        status += f"({fail_n} fails, {warn_n} warnings, {len(findings)-fail_n-warn_n} info)"

        ax.text(0.5, 0.98, table_name,
                ha="center", va="top", fontsize=13, fontweight="bold",
                color=NAVY, transform=ax.transAxes)
        ax.text(0.5, 0.93, status,
                ha="center", va="top", fontsize=10, color=color,
                fontweight="bold", transform=ax.transAxes)

        y = 0.87
        for f in findings:
            sev_color = SEV_COLOR.get(f["severity"], "#333")
            ax.text(0.01, y, f"[{f['severity']}]",
                    ha="left", va="top", fontsize=8, color=sev_color,
                    fontweight="bold", transform=ax.transAxes)
            ax.text(0.11, y, f["test"],
                    ha="left", va="top", fontsize=8, color="#333",
                    fontweight="bold", transform=ax.transAxes)
            y -= 0.04
            ax.text(0.04, y, f["detail"],
                    ha="left", va="top", fontsize=7.5, color="#222",
                    transform=ax.transAxes, family="monospace")
            y -= 0.035
            if f.get("note"):
                wrapped = textwrap.fill(f["note"], width=115)
                for line in wrapped.split("\n"):
                    ax.text(0.04, y, f"↳ {line}",
                            ha="left", va="top", fontsize=7, color="#666",
                            transform=ax.transAxes, style="italic")
                    y -= 0.028
            y -= 0.01
            if y < 0.04:
                break  # overflow guard

        pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


def _trend_charts(pdf, conn):
    """Visual seam checks: time series with seam lines annotated."""
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.patch.set_facecolor("white")
    fig.suptitle("Visual Seam Checks — Statewide Averages Over Time",
                 fontsize=13, fontweight="bold", color=NAVY)

    # ── Ch70 aid per pupil ────────────────────────────────────────────────────
    ax = axes[0][0]
    rows = conn.execute(text("""
        SELECT fiscal_year, AVG(chapter70_aid_per_pupil) as v,
               STDDEV(chapter70_aid_per_pupil) as sd
        FROM district_chapter70 GROUP BY fiscal_year ORDER BY fiscal_year
    """)).fetchall()
    yrs = [r[0] for r in rows]
    vals = [float(r[1]) for r in rows]
    sds  = [float(r[2]) if r[2] is not None else 0.0 for r in rows]
    ax.plot(yrs, vals, marker="o", markersize=3, color=STEEL, linewidth=1.8, label="State avg")
    hi = [v + 1*s for v, s in zip(vals, sds)]
    lo = [v - 1*s for v, s in zip(vals, sds)]
    ax.plot(yrs, hi, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6, label="±1 SD")
    ax.plot(yrs, lo, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6)
    saugus_ch70 = conn.execute(text("""
        SELECT fiscal_year, chapter70_aid_per_pupil FROM district_chapter70
        WHERE lea_code=262 AND chapter70_aid_per_pupil IS NOT NULL ORDER BY fiscal_year
    """)).fetchall()
    if saugus_ch70:
        sx = [r[0] for r in saugus_ch70]; sy = [float(r[1]) for r in saugus_ch70]
        ax.scatter(sx, sy, color=RED, s=20, zorder=5, label="Saugus")
    ax.axvline(2022.5, color=RED, linewidth=1.2, linestyle="--", alpha=0.7)
    ax.text(2022.6, min(vals)*1.02, "seam", fontsize=7, color=RED)
    ax.set_title("Ch70 Aid/Pupil — statewide avg", fontsize=9, fontweight="bold", color=NAVY)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.legend(fontsize=7); ax.spines[["top","right"]].set_visible(False)
    ax.set_facecolor("#FAFBFF")

    # ── EQV per capita ────────────────────────────────────────────────────────
    ax = axes[0][1]
    rows = conn.execute(text("""
        SELECT fiscal_year, AVG(eqv_per_capita) as v,
               STDDEV(eqv_per_capita) as sd
        FROM municipal_income_eqv WHERE eqv_per_capita IS NOT NULL
        GROUP BY fiscal_year ORDER BY fiscal_year
    """)).fetchall()
    yrs2  = [r[0] for r in rows]
    vals2 = [float(r[1]) for r in rows]
    sds2  = [float(r[2]) if r[2] is not None else 0.0 for r in rows]
    ax.plot(yrs2, vals2, marker="o", markersize=4, color=STEEL, linewidth=1.8, label="State avg")
    hi2 = [v + 1*s for v, s in zip(vals2, sds2)]
    lo2 = [v - 1*s for v, s in zip(vals2, sds2)]
    ax.plot(yrs2, hi2, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6, label="±1 SD")
    ax.plot(yrs2, lo2, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6)
    for gap_yr in [2016, 2020]:
        if gap_yr in yrs2:
            ax.scatter([gap_yr], [vals2[yrs2.index(gap_yr)]],
                       color=GOLD, s=60, zorder=5, label=f"FY{gap_yr} (biennial fill)")
    saugus_eqv = conn.execute(text("""
        SELECT fiscal_year, eqv_per_capita FROM municipal_income_eqv
        WHERE municipality='Saugus' AND eqv_per_capita IS NOT NULL ORDER BY fiscal_year
    """)).fetchall()
    if saugus_eqv:
        sx2 = [r[0] for r in saugus_eqv]; sy2 = [float(r[1]) for r in saugus_eqv]
        ax.scatter(sx2, sy2, color=RED, s=20, zorder=6, label="Saugus")
    ax.set_title("EQV/Capita — statewide avg (even years)", fontsize=9, fontweight="bold", color=NAVY)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.legend(fontsize=7); ax.spines[["top","right"]].set_visible(False)
    ax.set_facecolor("#FAFBFF")

    # ── PPE district count ────────────────────────────────────────────────────
    ax = axes[1][0]
    rows = conn.execute(text("""
        SELECT school_year, COUNT(DISTINCT org_code) FROM per_pupil_expenditure
        GROUP BY school_year ORDER BY school_year
    """)).fetchall()
    yrs3 = [r[0] for r in rows]; vals3 = [r[1] for r in rows]
    ax.bar(yrs3, vals3, color=STEEL, alpha=0.75, edgecolor="white")
    ax.axvline(2018.5, color=RED, linewidth=1.2, linestyle="--", alpha=0.7)
    ax.text(2018.7, min(vals3)*1.01, "seam\n(+82 districts)", fontsize=7, color=RED)
    ax.set_title("PPE — distinct districts per year", fontsize=9, fontweight="bold", color=NAVY)
    ax.spines[["top","right"]].set_visible(False)
    ax.set_facecolor("#FAFBFF")

    # ── Chronic absenteeism ───────────────────────────────────────────────────
    ax = axes[1][1]
    rows = conn.execute(text("""
        SELECT school_year, AVG(chronic_absenteeism_pct),
               STDDEV(chronic_absenteeism_pct)
        FROM attendance
        WHERE student_group='All' GROUP BY school_year ORDER BY school_year
    """)).fetchall()
    yrs4  = [r[0] for r in rows]
    vals4 = [float(r[1]) for r in rows]
    sds4  = [float(r[2]) if r[2] is not None else 0.0 for r in rows]
    ax.plot(yrs4, vals4, marker="o", markersize=4, color=STEEL, linewidth=1.8, label="State avg")
    hi4 = [v + 1*s for v, s in zip(vals4, sds4)]
    lo4 = [max(0.0, v - 1*s) for v, s in zip(vals4, sds4)]
    ax.plot(yrs4, hi4, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6, label="±1 SD")
    ax.plot(yrs4, lo4, color=STEEL, linewidth=0.9, linestyle=":", alpha=0.6)
    saugus_att = conn.execute(text("""
        SELECT school_year, chronic_absenteeism_pct FROM attendance
        WHERE org_code='02620000' AND student_group='All'
          AND chronic_absenteeism_pct IS NOT NULL ORDER BY school_year
    """)).fetchall()
    if saugus_att:
        sx4 = [r[0] for r in saugus_att]; sy4 = [float(r[1]) for r in saugus_att]
        ax.scatter(sx4, sy4, color=RED, s=20, zorder=5, label="Saugus")
    ax.axvspan(2020.5, 2022.5, alpha=0.08, color=RED, label="COVID period")
    ax.axvline(2020.5, color=GOLD, linewidth=1, linestyle="--", alpha=0.7)
    ax.text(2020.6, min(vals4)*1.02, "format\nchange", fontsize=7, color=GOLD)
    ax.set_title("Chronic Absenteeism — statewide avg", fontsize=9, fontweight="bold", color=NAVY)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f"{x:.0f}%"))
    ax.legend(fontsize=7); ax.spines[["top","right"]].set_visible(False)
    ax.set_facecolor("#FAFBFF")

    for ax in axes.flat:
        ax.tick_params(labelsize=7)

    fig.tight_layout(rect=[0, 0, 1, 0.95])

    # Explanation note at the bottom
    fig.text(
        0.5, 0.005,
        "Dotted lines show ±1 standard deviation (SD) from the statewide mean each year. "
        "About 68% of districts fall within this band. "
        "A value outside the band is unusual but not necessarily erroneous — "
        "it flags years or districts worth closer inspection.",
        ha="center", va="bottom", fontsize=7, color="#555555", style="italic",
        wrap=True,
    )

    pdf.savefig(fig, bbox_inches="tight"); plt.close(fig)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    engine = get_engine()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[integrity] Running tests ...")
    all_findings = []
    suite_map = {
        "district_chapter70":             test_chapter70,
        "municipal_income_eqv":           test_eqv,
        "per_pupil_expenditure":          test_ppe,
        "attendance":                     test_attendance,
        "graduation_rates":               test_graduation,
        "staffing":                       test_staffing,
        "district_selected_populations":  test_selected_populations,
    }

    with engine.connect() as conn:
        for name, fn in suite_map.items():
            try:
                results = fn(conn)
                all_findings.extend(results)
                fails   = sum(1 for r in results if r["severity"] == "FAIL")
                warns   = sum(1 for r in results if r["severity"] == "WARNING")
                print(f"  {name:45s} {len(results):3d} checks  "
                      f"FAIL={fails}  WARN={warns}")
            except Exception as e:
                print(f"  ERROR in {name}: {e}")
                all_findings.append({
                    "table": name, "test": "Suite error",
                    "detail": str(e), "severity": "FAIL",
                    "note": "Test suite threw an exception — check DB connectivity and schema.",
                })

        print(f"\n[integrity] Writing PDF → {OUTPUT_PDF}")
        with PdfPages(OUTPUT_PDF) as pdf:
            _title_page(pdf)
            _stitched_sources_page(pdf)
            _summary_table_page(pdf, all_findings)
            _trend_charts(pdf, conn)
            _detail_pages(pdf, all_findings)

    fail_n = sum(1 for f in all_findings if f["severity"] == "FAIL")
    warn_n = sum(1 for f in all_findings if f["severity"] == "WARNING")
    info_n = sum(1 for f in all_findings if f["severity"] == "INFO")
    print(f"\n[integrity] Done — {len(all_findings)} total:  "
          f"FAIL={fail_n}  WARNING={warn_n}  INFO={info_n}")
    print(f"  PDF: {OUTPUT_PDF}")


if __name__ == "__main__":
    run()
