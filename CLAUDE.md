# CLAUDE.md

Guidance for AI assistants working in this repo. The [README](README.md) is the
full project documentation (setup, run commands, factor definitions, DB schema);
this file captures the conventions and non-obvious rules that reading the code
alone won't tell you. When they disagree, prefer this file for *how to work* and
the README for *what things are*.

## What this project is

A data pipeline + statistical analysis comparing Saugus, MA school funding and
academic outcomes against its statistically-similar MA peer towns, to understand
the spending/outcomes gap and *why* it exists. Data lands in a local PostgreSQL
warehouse (`ma_school_data`); analyses read from it and emit PDF reports to
`Reports/`.

## Environment (always)

- **Activate the venv first.** Prefix every Python command with
  `source .venv/bin/activate &&`. The project uses the local `.venv/`, not Docker.
- Requires a running local PostgreSQL (`ma_school_data` on `localhost`, config in
  [config.py](config.py)). Analyses will fail without it.
- The repo lives under `~/Documents` (iCloud-synced). Streamed writes to
  `Reports/` intermittently fail with `TimeoutError`/`ETIMEDOUT` (Errno 60).
  **Write large/streamed files (PDFs) to a local tempfile, then `shutil.move`
  into `Reports/` with retries** — see `build_pdf` in the analysis scripts for the
  pattern, and reuse it for any new large file writes.

## Architecture: reuse the shared modules

The pipeline was deliberately de-duplicated. Do **not** re-derive these inline —
import them:

- `analysis/factors.py` — the factor **library**: every factor defined once as a
  named `Factor` object (tier, formula, units), plus `derive_factors()`, which holds
  **all** derived-ratio math in one place. Both the flagship report and the statewide
  screen select from it. Add or re-tier a factor here, never inline.
- `db/queries.py` — centralized feature/data queries for the flagship.
- `analysis/inflation.py` — CPI deflation (shared, one implementation).

## The flagship (RBP) and its discipline

`analysis/saugus_factor_analysis.py` (built on `analysis/rbp.py`) is the
confirmatory Relevance-Based Prediction report — a faithful implementation of
Czasonis, Kritzman & Turkington (2024).

- **No in-model pruning, ever.** Faithful to Kritzman: every candidate factor is
  kept; near-zero-importance factors are diversified away by relevance weighting,
  not deleted. Importance is diagnostic only. Do not "optimize" by dropping
  low-importance factors here.
- **Factor selection happens elsewhere.** The statewide factor *screen*
  (`factor_selection_scratch.py`, gitignored) is the place selection/discarding is
  allowed (method-plural, intentionally NOT bound by Kritzman's no-pruning rule). It
  nominates the curated factors the RBP report then consumes without pruning. Keep
  these two roles separate.
- **Each report is self-contained.** The four `MODEL_*` definitions in the flagship
  each state their target and their explicit `factors` list (selected by reference
  from `analysis/factors.py`). That list *is* the candidate set — no shared pool, no
  exclusion bookkeeping. To change a report, edit its own list.
- **Tiers:** Tier-3 = structural traits (what a town *is*: income, poverty,
  enrollment) → peer context only. Tier-1/2 = actionable factors (what a town
  *does*: staffing, pay, budget shares) → the factors actually ranked. RBP itself is
  tier-blind; tiers only shape how we read/display the output.
- **The engine is verified.** `analysis/test_rbp_properties.py` checks `rbp.py`
  against the paper's equations + convergence results — run it after any RBP change.
  `rbp.py` is marked "Do Not Modify"; leave it unless explicitly asked.

## Terminology

- Use **"factor"**, not "lever." The codebase went through a full sweep to
  standardize on "factor" (files, README, reports). Don't reintroduce "lever."

## Reports: two audiences, one source

`analysis/saugus_synthesis.py` is the single source of truth for the narrative
report and emits both audiences (no sync risk):

- default → `saugus_full_analysis.pdf` — technical: full methodology, regression
  tables, peer-selection math.
- `--parent` → `saugus_community_brief.pdf` — community: story and conclusions,
  methodology pages omitted.

Rebuild PDFs from cache without recompute via `--regen-pdf` where supported.

### Report style rules (user preferences)

- **Show numbers, not bar charts.** Per-outcome factor pages are numeric,
  multi-method tables grouped by tier (Tier 1 → 2 → 3), each tier ranked
  best→worst, paginated with "(continued)" bands so nothing truncates.
- **Covid framing:** never headline the raw 2017→2025 MCAS drop (critics dismiss
  it as Covid). Baseline on **pre-Covid 2019** and use the **gap-widening**
  framing (e.g. "gap grew from 5pp in 2019 to 13pp in 2025") — peers dipped and
  recovered post-2022, Saugus didn't. Acknowledge Covid in chart annotations.

## Working style

- **Rigor over speed.** Multi-hour (even multi-day) compute runs are acceptable;
  pick the most defensible method and flag weaknesses proactively.
- **Push back.** Flag bad ideas and scope creep early with reasons — don't be a
  yes-man.

## Status

- The flagship (`rbp.py` + `factors.py` + `saugus_factor_analysis.py`) is being
  prepared to share with **David Turkington** (a co-author of the RBP paper), so the
  bar is a faithful implementation *and* clean, expert-readable code — not just a
  working report. The earlier budget/staffing and fixed-cost report pages (and their
  Ridge cross-check) have been removed to keep the flagship focused; recover them from
  git history if needed.
- Factors are fully centralized in `analysis/factors.py` (a library of 97 — 19
  curated + 78 exploratory candidates flagged `curated=False`); each report selects
  its own factors explicitly.
- Work in progress: within-district fixed-effects **event study**
  (`analysis/event_study.py`) — the causal-leaning temporal complement to the
  cross-sectional RBP model. Not yet published.
