---
phase: 04-integration-and-scheduler
plan: "01"
subsystem: pipeline-wiring
tags: [integration, pipeline, tdd, bat, scheduler]
dependency_graph:
  requires: [03-01]
  provides: [full-etl-pipeline, task-scheduler-entry-point]
  affects: [importer.py, run.bat]
tech_stack:
  added: []
  patterns: [tdd-red-green, monkeypatch-module-attribute, extract-transform-load]
key_files:
  created:
    - tests/test_integration.py
    - run.bat
  modified:
    - importer.py
    - tests/test_main.py
    - tests/conftest.py
decisions:
  - extract_data return value flows into load_csv — cfg[paths][csv_path] removed from main() call path
  - SystemExit from extract_data propagates naturally (BaseException, not caught by except Exception)
  - run.bat uses C:\Python313\python.exe (from 'where python' on this machine; Tom must update for deployment)
metrics:
  duration_minutes: 2
  tasks_completed: 3
  files_modified: 5
  completed_date: "2026-03-15"
---

# Phase 04 Plan 01: Integration and Scheduler Summary

Full ETL pipeline wired from single entry point — extract_data -> load_csv -> upsert_batch with Task Scheduler wrapper using absolute paths and ERRORLEVEL propagation.

## What Was Built

`importer.py` now calls `extract_data(url, work_dir, logger)` as the first pipeline step before `load_csv()`. The csv_path returned by `extract_data` flows directly into `load_csv` — the config's `csv_path` key is no longer used in the main pipeline. `run.bat` provides a Task Scheduler entry point with `cd /d` working directory, absolute python.exe and script paths, and `exit /b %ERRORLEVEL%` to propagate exit codes.

## Tasks Completed

| # | Name | Commit | Files |
|---|------|--------|-------|
| 1 | Write failing tests (TDD RED) | 54229f5 | tests/test_integration.py, tests/conftest.py |
| 2 | Wire extract_data into main() (TDD GREEN) | 459522d | importer.py, tests/test_main.py |
| 3 | Create run.bat Task Scheduler wrapper | af38d83 | run.bat |

## Test Results

- Before: 41 unit tests passing
- After: 45 unit tests passing (+4 new integration-wiring tests)
- All 5 deselected tests remain deselected (live SQL Server integration tests)

## Decisions Made

1. **extract_data return value as csv_path** — `cfg["paths"]["csv_path"]` is no longer read in main(). The CSV path is now exclusively determined by what `extract_data` returns, ensuring the pipeline uses the freshly downloaded file path, not a stale config value.

2. **SystemExit propagation** — `extract_data` raises `SystemExit(1)` on error. Since `SystemExit` is a `BaseException` (not `Exception`), it is not caught by `except Exception` in main(), propagating correctly to the caller and to Task Scheduler via ERRORLEVEL.

3. **Python path in run.bat** — Used `C:\Python313\python.exe` (first result of `where python` on this dev machine). Setup instructions in comments tell Tom to run `where python` and update the path before deployment.

## Deviations from Plan

None - plan executed exactly as written.

## Key Links

- `importer.py` imports `extract_data` from `extract.py` via `from extract import extract_data`
- `main()` calls `extract_data(url, work_dir, logger)` where url and work_dir come from `cfg["paths"]`
- `run.bat` calls `importer.py` via absolute python.exe path with `exit /b %ERRORLEVEL%`

## Self-Check: PASSED

- FOUND: importer.py
- FOUND: run.bat
- FOUND: tests/test_integration.py
- FOUND: .planning/phases/04-integration-and-scheduler/04-01-SUMMARY.md
- FOUND commit 54229f5 (TDD RED tests)
- FOUND commit 459522d (pipeline wiring GREEN)
- FOUND commit af38d83 (run.bat)
