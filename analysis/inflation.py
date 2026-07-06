"""
inflation.py — shared CPI deflation for turning nominal $ into real $.

The project deflates against two different price series (kept as-is here; which
one a given report uses is that report's own methodological choice):

  • national — inflation_cpi.cpi_pct_change (annual % change → cumulative level)
  • Boston   — cpi_boston_msa.cpi_index    (a ready-made price index)

Both reduce to a {year: price_level} Series.  ``deflator(level, base_year)`` turns
that into a {year: factor} map where ``nominal * factor = real (base_year) $``.
Because a deflator is a ratio of price levels, the choice of which year anchors
the cumulative level is irrelevant — only the base_year matters.
"""
from __future__ import annotations

import pandas as pd


def price_level_from_pct(cpi_df: pd.DataFrame, base_year: int,
                         year_col: str = "year",
                         pct_col: str = "cpi_pct_change") -> pd.Series:
    """Cumulative price level from an annual %-change table, anchored at base_year.

    ``cpi_pct_change[y]`` is the inflation from year y-1 to y, so it is applied to
    every year *after* base_year; years at or before base_year are the anchor and
    are set to 1.0.  ``base_year`` may sit one step before the first data row (a
    synthetic base — e.g. FY2010 when the CPI table starts at 2011).
    """
    d = cpi_df[[year_col, pct_col]].dropna().sort_values(year_col)
    level: dict[int, float] = {int(base_year): 1.0}
    prev = int(base_year)
    for yr, pct in zip(d[year_col], d[pct_col]):
        yr = int(yr)
        if yr <= base_year:
            continue
        level[yr] = level[prev] * (1 + float(pct) / 100.0)
        prev = yr
    return pd.Series(level).sort_index()


def price_level_from_index(cpi_df: pd.DataFrame, year_col: str,
                           idx_col: str) -> pd.Series:
    """Price level straight from a ready-made index column (e.g. Boston MSA)."""
    return cpi_df.set_index(year_col)[idx_col].astype(float)


def deflator(price_level: pd.Series, base_year: int) -> dict[int, float]:
    """{year: factor} with ``nominal * factor = real (base_year) dollars``.

    factor = price_level[base_year] / price_level[year].  Years missing from
    ``price_level`` are absent from the result (callers default them as needed).
    """
    base = float(price_level.loc[base_year])
    return {int(y): base / float(lv) for y, lv in price_level.items()}
