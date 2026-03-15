---
phase: 01-foundation-and-load
plan: "02"
subsystem: database
tags: [python, mssql-python, pyodbc, sql-server, upsert, UPDATE+INSERT, transactions, pytest, tdd]

# Dependency graph
requires:
  - phase: 01-foundation-and-load/01-01
    provides: "load_config(), setup_logger(), main() scaffold, test fixtures in conftest.py"
provides:
  - "get_connection(cfg) opens SQL Server connection via mssql-python (pyodbc fallback), autocommit=False"
  - "ensure_table(conn, table) creates V_CHARACTERISTICS_TESTING if absent — no PK (MRC is varchar(max))"
  - "upsert_batch(conn, table, rows, logger) UPDATE+INSERT per row; commits on success, rolls back on error"
  - "importer.py main() full pipeline: config -> logger -> connect -> ensure_table -> upsert -> exit 0/1"
  - "6 integration tests covering insert, idempotent rerun, update, no-MERGE, varchar(max), rollback"
affects: [01-03, all-subsequent-plans]

# Tech tracking
tech-stack:
  added: [mssql-python (primary DB driver), pyodbc (fallback DB driver)]
  patterns:
    - "UPDATE+INSERT upsert — UPDLOCK/SERIALIZABLE hints prevent phantom inserts under concurrency"
    - "Driver fallback — mssql-python primary, pyodbc if ImportError"
    - "Session-scoped db_config fixture skips all integration tests when config.ini absent"
    - "clean_table fixture deletes all rows before and after each integration test for isolation"
    - "pytest integration mark registered in pytest.ini for selective test runs"

key-files:
  created:
    - db.py
  modified:
    - importer.py
    - tests/conftest.py
    - tests/test_upsert.py
    - tests/test_main.py
    - pytest.ini

key-decisions:
  - "No PRIMARY KEY on V_CHARACTERISTICS_TESTING — MRC is varchar(max) which exceeds SQL Server 900-byte key limit; WHERE NIIN=? AND MRC=? handles row matching"
  - "never fast_executemany — documented risk of varchar(max) truncation with batch executemany"
  - "UPDATE+INSERT with UPDLOCK+SERIALIZABLE hints — prevents phantom rows under concurrent load"
  - "test_no_merge_statement is a static analysis test — does not require DB connection, always runs"
  - "Integration tests skip automatically when config.ini missing (session-scoped db_config fixture)"

patterns-established:
  - "Upsert pattern: cursor.execute(UPDATE WITH UPDLOCK); if rowcount==0: cursor.execute(INSERT)"
  - "Transaction pattern: conn.commit() after full batch success; conn.rollback() + re-raise on any error"
  - "DB fixture pattern: session-scoped config, function-scoped connection, clean_table for isolation"

requirements-completed: [LD-01, LD-02, LD-03, LD-04]

# Metrics
duration: 3min
completed: 2026-03-15
---

# Phase 01 Plan 02: Database Module and Upsert Summary

**UPDATE+INSERT upsert for SQL Server via mssql-python/pyodbc, with UPDLOCK/SERIALIZABLE hints, full transaction rollback, and 6 integration tests against V_CHARACTERISTICS_TESTING.**

## Performance

- **Duration:** ~3 min
- **Started:** 2026-03-15T16:37:01Z
- **Completed:** 2026-03-15T16:39:48Z
- **Tasks:** 2 of 3 (Task 3 is a human-verify checkpoint — pending user verification)
- **Files modified:** 6

## Accomplishments

- `get_connection()` opens a SQL Server connection from ConfigParser, using mssql-python with an automatic pyodbc fallback, and sets `autocommit=False` for explicit transaction control
- `ensure_table()` creates V_CHARACTERISTICS_TESTING on first run (no PK — MRC is varchar(max)), skips if already exists, commits the DDL immediately
- `upsert_batch()` uses UPDATE+INSERT with UPDLOCK/SERIALIZABLE hints per row: zero MERGE statements; commits full batch on success, rolls back entirely on any error
- `importer.py` main() now runs the complete pipeline end-to-end: load config, setup logger, connect, ensure table, upsert 3 test rows, log inserted/updated counts, exit 0
- 6 integration tests written (1 passes without DB — static MERGE check; 5 skip cleanly until config.ini is populated)

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: Failing integration tests** - `19f8c44` (test)
2. **Task 1 GREEN: db.py implementation** - `62c642c` (feat)
3. **Task 2: Wire db.py into importer.py** - `6f4a4d5` (feat)

_Note: TDD order — tests written first (RED commit), then implementation (GREEN commit)._

## Files Created/Modified

- `db.py` — Three exported functions: `get_connection`, `ensure_table`, `upsert_batch`
- `importer.py` — Updated `main()` with full DB pipeline; imports from db
- `tests/conftest.py` — Added `db_config`, `db_conn`, `clean_table` integration fixtures
- `tests/test_upsert.py` — Replaced 6 skipped stubs with real integration tests
- `tests/test_main.py` — Updated to mock `get_connection`, `ensure_table`, `upsert_batch`; added connection-failure test
- `pytest.ini` — Registered `integration` marker

## Decisions Made

- No PRIMARY KEY constraint on V_CHARACTERISTICS_TESTING: MRC is varchar(max), which SQL Server cannot index due to the 900-byte key size limit. Row matching is handled entirely by `WHERE NIIN = ? AND MRC = ?` in the UPDATE and the subsequent INSERT.
- `fast_executemany` not used anywhere: research in 01-RESEARCH.md identified varchar(max) truncation risk with batch executemany.
- Static MERGE check (`test_no_merge_statement`) does not depend on DB and always runs — gives instant feedback even without `config.ini`.
- Integration tests use a session-scoped `db_config` fixture that calls `pytest.skip()` if `config.ini` is absent, so the test suite runs cleanly on machines without SQL Server.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Tightened no-MERGE regex to exclude docstring/comment matches**

- **Found during:** Task 1 GREEN (running tests after db.py created)
- **Issue:** Initial regex `\bMERGE\s+(INTO\s+)?\w` matched the phrase "never MERGE" in db.py docstrings, causing false-positive test failure
- **Fix:** Rewrote pattern to match SQL DML form only: `MERGE [INTO] <identifier>` or `MERGE <word> USING`. Pure comment lines (starting with `#`) are skipped. Test re-run passed.
- **Files modified:** `tests/test_upsert.py`
- **Verification:** `test_no_merge_statement` PASSED; no false positives on docstring text
- **Committed in:** `62c642c` (Task 1 GREEN commit)

**2. [Rule 2 - Missing Critical] Added `integration` marker registration to pytest.ini**

- **Found during:** Task 1 GREEN (test run)
- **Issue:** `PytestUnknownMarkWarning` for unregistered `@pytest.mark.integration` — warnings indicate the mark is not recognized, which can cause unexpected deselect behavior
- **Fix:** Added `markers = integration: ...` to pytest.ini
- **Files modified:** `pytest.ini`
- **Verification:** Warnings gone, marker resolves correctly with `-m integration` and `-m "not integration"`
- **Committed in:** `62c642c` (Task 1 GREEN commit)

---

**Total deviations:** 2 auto-fixed (1 Rule 1 bug, 1 Rule 2 missing critical)
**Impact on plan:** Both fixes required for correctness. No scope creep.

## Issues Encountered

- mssql-python is the primary driver per LD decision; pyodbc fallback is implemented but both will be validated at the Task 3 checkpoint when Tom runs the pipeline against his actual SQL Server instance.

## User Setup Required

Tom must complete these steps before running Task 3 verification:

1. Copy `config.ini.example` to `config.ini` in the project root
2. Fill in real SQL Server credentials: `server`, `database`, `username`, `password`
3. Confirm `table = V_CHARACTERISTICS_TESTING` is correct
4. Ensure SQL Server is accessible from the machine running the script

## Next Phase Readiness

- Task 3 (human-verify checkpoint) is pending — Tom needs to run `python importer.py` twice and `pytest tests/ -v` against his real SQL Server instance to confirm end-to-end correctness
- Once Task 3 is approved, Phase 1 is complete and Phase 2 (CSV transform) can begin
- The upsert foundation is solid: all 5 DB-dependent tests will run as true integration tests once `config.ini` is populated

---
*Phase: 01-foundation-and-load*
*Completed: 2026-03-15 (Task 3 checkpoint pending human verification)*
