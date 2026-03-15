---
phase: 02-transform
plan: "02"
subsystem: transform
tags: [tdd, date-conversion, pipeline-wiring, century-pivot]
dependency_graph:
  requires: [02-01]
  provides: [load_csv with date conversion, main() real pipeline]
  affects: [importer.py, transform.py]
tech_stack:
  added: [re, datetime.date]
  patterns: [century-pivot two-digit year, str.replace with callable on single column]
key_files:
  created: []
  modified:
    - transform.py
    - importer.py
    - tests/test_transform.py
    - tests/test_main.py
decisions:
  - Dynamic _PIVOT = date.today().year % 100 — pivot recalculates automatically each year without code changes
  - str.replace with callable (not sub) — pandas str.replace accepts re.Pattern + callable for per-match logic
  - csv_path from cfg[paths][csv_path] — config INI [paths] section added as simple mechanism; no sys.argv needed for basic runs
metrics:
  duration_minutes: 8
  completed_date: "2026-03-15"
  tasks_completed: 2
  files_modified: 4
---

# Phase 02 Plan 02: Date Conversion and Pipeline Wiring Summary

**One-liner:** dd-MMM-yy date conversion with dynamic century pivot wired into load_csv, and main() replaced hardcoded test rows with real load_csv -> upsert_batch pipeline.

## What Was Built

### TF-02 Date conversion (transform.py)

Added to `transform.py`:

- `_DATE_PAT` — compiled regex matching `dd-MMM-yy` patterns case-insensitively
- `_PIVOT` — dynamic two-digit year threshold (`date.today().year % 100`, currently 26)
- `_MONTH_MAP` — dict mapping 3-letter abbreviations to zero-padded month numbers
- `_convert_date_match(m)` — per-match conversion function: applies century pivot (yy > 26 → 1900s, yy <= 26 → 2000s), returns ISO `YYYY-MM-DD`
- `_convert_dates(df)` — applies `str.replace` with `_DATE_PAT` and `_convert_date_match` on the `CLEAR_TEXT_REPLY` column only, returns a copy
- `load_csv` now calls `_convert_dates(df)` after `_validate_columns`, before return

### Pipeline wiring (importer.py)

- Added `from transform import load_csv` import
- `main()` now reads `csv_path` from `cfg["paths"]["csv_path"]`
- Replaced three hardcoded test rows with `df = load_csv(csv_path, logger)`
- Converts DataFrame to `rows = df.to_dict(orient="records")` before passing to `upsert_batch`

## Decisions Made

| Decision | Rationale |
|---|---|
| Dynamic `_PIVOT` based on `date.today().year % 100` | Automatically recalculates each year — no code maintenance needed as years advance |
| `str.replace` with callable, not `re.sub` | pandas `Series.str.replace` supports `(pattern, callable, regex=True)` — cleaner than calling `.apply()` row-by-row |
| csv_path in `[paths]` INI section | Consistent with existing INI pattern; Tom can edit one file; no CLI argument complexity needed for Phase 1-2 |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated test_main_exits_0_on_success to work with new pipeline**
- **Found during:** GREEN phase execution
- **Issue:** The existing `test_main_exits_0_on_success` test did not patch `load_csv` and did not include a `[paths]` section in its config — main() now requires both. The test raised `KeyError: 'paths'` and exited 1 instead of 0.
- **Fix:** Added `load_csv` monkeypatch returning a stub DataFrame, and added `cfg["paths"]["csv_path"] = "dummy.csv"` to the patched config
- **Files modified:** `tests/test_main.py`
- **Commit:** ac9766f (included in GREEN commit)

## Test Coverage

| Test class/function | What it verifies |
|---|---|
| `TestDateConversion::test_date_standalone` | "18-MAR-52" -> "1952-03-18" |
| `TestDateConversion::test_date_embedded` | "PC2897 (20-FEB-02)" -> "PC2897 (2002-02-20)" |
| `TestDateConversion::test_century_pivot_old` | yy=52 > pivot(26) -> 1952 |
| `TestDateConversion::test_century_pivot_recent` | yy=02 < pivot(26) -> 2002 |
| `TestDateConversion::test_date_unparseable` | "99-XXX-00" left unchanged |
| `TestDateConversion::test_date_other_columns_untouched` | Only CLEAR_TEXT_REPLY converted |
| `TestDateConversion::test_no_dates_no_crash` | No date patterns -> no change |
| `test_main_calls_load_csv` | main() calls load_csv with csv_path from config |
| `test_main_passes_df_to_upsert` | DataFrame rows passed through to upsert_batch |

**Full suite result:** 30 passed, 5 integration skipped, 0 failed

## Commits

| Hash | Type | Description |
|---|---|---|
| 6a36837 | test | RED: add failing tests for date conversion and pipeline wiring |
| ac9766f | feat | GREEN: add date conversion and wire load_csv into main pipeline |

## Self-Check

Files created/modified:
- transform.py — modified (added _DATE_PAT, _PIVOT, _MONTH_MAP, _convert_date_match, _convert_dates; load_csv updated)
- importer.py — modified (from transform import load_csv; main() wired)
- tests/test_transform.py — modified (TestDateConversion added)
- tests/test_main.py — modified (pipeline wiring tests added; existing test updated)
