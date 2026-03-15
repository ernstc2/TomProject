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
  added: [mssql-python (primary DB driver), pyodbc (fallback DB driver with runtime driver detection)]
  patterns:
    - "UPDATE+INSERT upsert — UPDLOCK/SERIALIZABLE hints prevent phantom inserts under concurrency"
    - "Driver fallback — mssql-python primary, pyodbc if ImportError; driver auto-detected via pyodbc.drivers()"
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
  - "ODBC driver auto-detected at runtime via pyodbc.drivers() — prefer Driver 18, then 17, then generic SQL Server"

patterns-established:
  - "Upsert pattern: cursor.execute(UPDATE WITH UPDLOCK); if rowcount==0: cursor.execute(INSERT)"
  - "Transaction pattern: conn.commit() after full batch success; conn.rollback() + re-raise on any error"
  - "DB fixture pattern: session-scoped config, function-scoped connection, clean_table for isolation"

requirements-completed: [LD-01, LD-02, LD-03, LD-04]

# Metrics
duration: ~60min
completed: 2026-03-15
---

# Phase 01 Plan 02: Database Module and Upsert Summary

**UPDATE+INSERT upsert for SQL Server via pyodbc with auto-detected ODBC driver (18/17/generic), UPDLOCK/SERIALIZABLE hints, full transaction rollback, and 16 passing tests (6 integration against real SQL Server).**

## Performance

- **Duration:** ~60 min
- **Started:** 2026-03-15T16:37:01Z
- **Completed:** 2026-03-15
- **Tasks:** 3 of 3 (including human-verify: all 5 verification steps passed)
- **Files modified:** 6

## Accomplishments

- `get_connection()` opens a SQL Server connection from ConfigParser, using mssql-python primary with automatic pyodbc fallback; pyodbc probes installed ODBC drivers at runtime (Driver 18 -> 17 -> generic), so no hardcoded driver name
- `ensure_table()` creates V_CHARACTERISTICS_TESTING on first run (no PK — MRC is varchar(max)), skips if already exists, commits the DDL immediately
- `upsert_batch()` uses UPDATE+INSERT with UPDLOCK/SERIALIZABLE hints per row: zero MERGE statements; commits full batch on success, rolls back entirely on any error
- `importer.py` main() runs the complete pipeline end-to-end: load config, setup logger, connect, ensure table, upsert 3 test rows, log inserted/updated counts, exit 0
- 16 tests passing: 6 integration tests confirmed against real SQL Server (insert, idempotent, update, no-MERGE, 5000-char varchar(max), batch rollback) + 10 unit tests
- End-to-end verified by user: first run inserted=3 updated=0; second run inserted=0 updated=3; 3 rows visible in SSMS

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: Failing integration tests** - `19f8c44` (test)
2. **Task 1 GREEN: db.py implementation** - `62c642c` (feat)
3. **Task 2: Wire db.py into importer.py** - `6f4a4d5` (feat)
4. **Deviation fix: ODBC driver auto-detection** - `6734b91` (fix)

**Plan metadata:** `57eba9b` (docs: complete db module and upsert plan — pre-checkpoint)

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

**3. [Rule 1 - Bug] ODBC driver version hardcoded to Driver 17 — machine had Driver 18 only**

- **Found during:** Task 3 (human-verify — user ran pipeline and got connection failure)
- **Issue:** `get_connection()` pyodbc fallback path had "ODBC Driver 17 for SQL Server" hardcoded. User's machine has Driver 18 installed, not Driver 17, so the connection string was invalid.
- **Fix:** Replaced hardcoded string with a runtime probe: `pyodbc.drivers()` scanned for Driver 18 first, then Driver 17, then generic "SQL Server". Raises `RuntimeError` with the full driver list if none found.
- **Files modified:** `db.py`
- **Verification:** User ran `python importer.py` after fix — connected successfully, inserted=3 updated=0
- **Committed in:** `6734b91` (separate fix commit after checkpoint approval)

---

**Total deviations:** 3 auto-fixed (2 Rule 1 bugs, 1 Rule 2 missing critical)
**Impact on plan:** All fixes required for correctness. No scope creep.

## Issues Encountered

None beyond the ODBC driver deviation documented above. mssql-python import failed on user's machine (ImportError), confirming the pyodbc fallback path is the active path in this environment.

## User Setup Required

config.ini must exist with real SQL Server credentials (copy from config.ini.example, fill in server/database/username/password). config.ini is gitignored — credentials are never committed.

## Next Phase Readiness

- End-to-end pipeline is verified on a real SQL Server instance with real credentials
- `upsert_batch(conn, table, rows, logger)` is ready to receive rows from Phase 2's CSV parser
- Phase 2 can focus entirely on CSV parsing and transformation — the load layer is stable and proven

---
*Phase: 01-foundation-and-load*
*Completed: 2026-03-15*
