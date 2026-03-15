---
phase: 02-transform
plan: 01
subsystem: testing
tags: [pandas, csv, transform, delimiter-detection, column-validation]

requires:
  - phase: 01-foundation-and-load
    provides: conftest.py fixtures (tmp_path, caplog) and importer/db module interfaces

provides:
  - transform.py with load_csv() exported
  - _detect_delimiter() using csv.Sniffer with comma/pipe/tab/semicolon candidates
  - _validate_columns() logging each missing column before SystemExit(1)
  - tests/test_transform.py — 10 unit tests covering TF-01, TF-03, TF-04

affects:
  - 02-transform (subsequent plans wire load_csv into main pipeline)
  - 03-extract (transform output feeds into upsert batch)

tech-stack:
  added: [pandas (read_csv with dtype=str)]
  patterns: [TDD red-green, csv.Sniffer fallback pattern, dtype=str for NIIN safety]

key-files:
  created:
    - transform.py
    - tests/test_transform.py
  modified: []

key-decisions:
  - "dtype=str on pd.read_csv — never allow pandas to infer numeric type for NIIN column"
  - "csv.Sniffer with explicit delimiters= candidates (not sep=None) — avoids pandas ambiguous parser"
  - "Fallback delimiter is comma — matches most RFC 4180 files if sniff fails"
  - "keep_default_na=False — prevents pandas from silently converting empty strings to NaN"
  - "All missing columns logged before single SystemExit(1) — operator can fix all at once"

patterns-established:
  - "TDD flow: write failing test -> commit RED -> implement -> all green -> commit GREEN"
  - "Class-level CSV strings built with string concatenation, not triple-quoted literals — avoids Python parsing leading-zero integers"
  - "load_csv accepts optional logger= param — falls back to module logger for production, caller-supplied logger for test isolation"

requirements-completed: [TF-01, TF-03, TF-04]

duration: 3min
completed: 2026-03-15
---

# Phase 2 Plan 1: Core CSV Transform Summary

**pandas load_csv() with csv.Sniffer delimiter detection, dtype=str NIIN preservation, and column validation exit(1) — 10 unit tests, all green**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-15T22:02:25Z
- **Completed:** 2026-03-15T22:05:06Z
- **Tasks:** 2 (RED + GREEN TDD phases)
- **Files modified:** 2

## Accomplishments

- TF-01: RFC 4180 quote stripping works automatically via pandas; embedded `""` resolves to `"` correctly
- TF-03: Delimiter auto-detected via csv.Sniffer (comma and pipe tested), with clean fallback to comma on sniff failure
- TF-04: Column validation logs every missing required column at ERROR before raising SystemExit(1)
- NIIN leading zeros preserved end-to-end: `"000000042"` stays `"000000042"` as string dtype

## Task Commits

Each task was committed atomically:

1. **Task 1: RED — failing tests** - `19d40c8` (test)
2. **Task 2: GREEN — implement transform.py** - `af0c2b2` (feat)

**Plan metadata:** (docs commit — see below)

_Note: TDD tasks have two commits (test -> feat). No REFACTOR pass needed._

## Files Created/Modified

- `transform.py` - Core CSV loader: _detect_delimiter(), _validate_columns(), load_csv() exported
- `tests/test_transform.py` - 199 lines, 10 unit tests across 3 test classes (delimiter, quotes, validation)

## Decisions Made

- `dtype=str` always — NIIN values contain leading zeros that pandas would silently drop as integers
- `csv.Sniffer` with explicit `delimiters=",|\t;"` — plan requirement; never uses `sep=None` (parser ambiguity risk)
- `keep_default_na=False` — prevents pandas converting empty cells to NaN, which would break upsert string comparisons
- Fallback delimiter is comma — sensible default for RFC 4180 files

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed SyntaxError in test file from leading-zero integer literals**
- **Found during:** Task 1 (RED phase)
- **Issue:** Triple-quoted CSV strings containing NIIN values like `000000042` caused Python `SyntaxError: leading zeros in decimal integer literals are not permitted` during test collection
- **Fix:** Rewrote all class-level CSV constants using string concatenation (`"header\n" "row\n"`) instead of triple-quoted blocks; values are strings, not parsed as Python integers
- **Files modified:** tests/test_transform.py
- **Verification:** Test collection succeeds; all 10 tests collected and run
- **Committed in:** 19d40c8 (Task 1 commit, RED phase)

**2. [Rule 1 - Bug] Fixed em-dash unicode character in test docstring**
- **Found during:** Task 1 (RED phase)
- **Issue:** Em-dash character (U+2014) in a test docstring caused `SyntaxError: invalid character` on Windows with Python 3.13
- **Fix:** Replaced em-dash with ASCII hyphen in docstring
- **Files modified:** tests/test_transform.py
- **Verification:** No syntax errors reported by pytest collection
- **Committed in:** 19d40c8 (Task 1 commit, RED phase)

---

**Total deviations:** 2 auto-fixed (both Rule 1 - Bug, both in RED phase test file)
**Impact on plan:** Both fixes were purely syntactic issues in the test file. No scope change, no logic change.

## Issues Encountered

Both issues were syntax problems in the test file discovered during the RED phase. The implementation phase (GREEN) ran without issues.

## Next Phase Readiness

- `load_csv()` is exported and ready to be imported by the next transform plan
- Date conversion (TF-02) and pipeline wiring can proceed
- No blockers

---
*Phase: 02-transform*
*Completed: 2026-03-15*
