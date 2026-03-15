---
phase: 03-extract
plan: 01
subsystem: extract
tags: [requests, zipfile, http-download, zip-validation, user-agent, streaming]

# Dependency graph
requires:
  - phase: 02-transform
    provides: "transform.py SystemExit(1) and logger=None patterns that extract.py replicates"
provides:
  - "extract_data(url, work_dir, logger=None) -- downloads zip, validates, extracts CSV, returns absolute path"
  - "_download_file() with browser User-Agent header and 64KB streaming (DL-01)"
  - "_validate_zip() guards against HTML error pages via zipfile.is_zipfile() (DL-02)"
  - "_find_csv_member() case-insensitive CSV location inside zip (DL-03)"
  - "config.ini.example [paths] section with download_url and work_dir keys"
affects: [04-wire, importer.py]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "requests.get with stream=True and iter_content(chunk_size=65536) for large file downloads"
    - "zipfile.is_zipfile() as authoritative zip guard before extraction"
    - "n.upper().endswith('.CSV') for case-insensitive zip member matching"
    - "Browser User-Agent header in HEADERS constant to bypass DLA 403 blocks"

key-files:
  created:
    - extract.py
    - tests/test_extract.py
  modified:
    - config.ini.example

key-decisions:
  - "Browser User-Agent header stored as HEADERS module constant -- DLA CDN returns 403 to default Python User-Agent"
  - "zipfile.is_zipfile() used as DL-02 guard rather than content-type header check -- magic number is authoritative; content-type can lie"
  - "download_url stored in config.ini [paths] section -- not hardcoded, so Tom can update URL without code changes if DLA reorganizes site"
  - "Streams in 64KB chunks rather than response.content in-memory -- V_CHARACTERISTICS.CSV is ~40MB compressed, memory spike avoided"
  - "Always overwrites zip on disk rather than skipping if file exists -- prevents stale partial downloads from previous interrupted runs"

patterns-established:
  - "Pattern: extract module follows transform.py convention of logger=None with fallback to module-level _log"
  - "Pattern: all error exits use raise SystemExit(1) with log.error() before exit for operator diagnostics"

requirements-completed: [DL-01, DL-02, DL-03]

# Metrics
duration: 2min
completed: 2026-03-15
---

# Phase 3 Plan 01: Extract Module Summary

**extract_data() using requests streaming with browser User-Agent, zipfile.is_zipfile() HTML guard, and case-insensitive CSV member extraction returning absolute path**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-15T22:28:41Z
- **Completed:** 2026-03-15T22:30:16Z
- **Tasks:** 2 (RED + GREEN TDD cycle)
- **Files modified:** 3

## Accomplishments

- extract_data() orchestrates download, validation, and extraction as the "E" in the ETL pipeline
- Browser User-Agent header prevents DLA CDN 403 blocks on all HTTP requests (DL-01)
- zipfile.is_zipfile() guards against HTML error pages being silently treated as valid zips (DL-02)
- Case-insensitive CSV member lookup handles both V_CHARACTERISTICS.CSV and data.csv naming variants (DL-03)
- 11 unit tests cover all error paths with mocked HTTP -- no network required
- config.ini.example updated with [paths] section documenting download_url and work_dir

## Task Commits

Each task was committed atomically:

1. **Task 1: RED - Write failing tests for extract module** - `9d22e3f` (test)
2. **Task 2: GREEN - Implement extract.py and update config.ini.example** - `cb353c2` (feat)

_Note: TDD tasks have two commits (test RED -> feat GREEN)_

## Files Created/Modified

- `extract.py` - extract_data() orchestrator with _download_file(), _validate_zip(), _find_csv_member()
- `tests/test_extract.py` - 11 unit tests covering DL-01, DL-02, DL-03 with mocked HTTP
- `config.ini.example` - Added [paths] section with download_url, work_dir, csv_path keys

## Decisions Made

- Browser User-Agent header stored as HEADERS module constant -- DLA CDN returns 403 to default Python User-Agent strings (confirmed during research)
- zipfile.is_zipfile() used as the authoritative DL-02 guard rather than checking content-type -- magic number check cannot be spoofed by a misconfigured CDN returning HTML with 200 status
- download_url stored in config.ini [paths] section rather than hardcoded -- DLA has reorganized URLs before; Tom can update without touching Python code
- Streams response in 64KB chunks -- V_CHARACTERISTICS.CSV is ~40MB compressed, avoiding full in-memory load
- Always overwrites characteristics.zip on disk -- prevents stale partial downloads from a previous interrupted run being mistaken for a valid zip

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None. All 11 tests passed on first implementation run. Full suite (41 tests) remains green.

## User Setup Required

Tom must manually visit the DLA FLIS Electronic Reading Room page in a browser, copy the direct download URL for characteristics.zip, and update download_url in config.ini before the extract step can run against the live site. This is documented via the inline comment in config.ini.example.

The actual download URL is intentionally not hardcoded -- see decision above.

## Next Phase Readiness

- extract_data() is ready to be called from importer.py as the first step of the monthly ETL run
- Function signature: extract_data(url, work_dir, logger=None) -> str (absolute CSV path)
- Wiring extract_data() into main() in importer.py is the Phase 4 task
- Blocker remains: Tom must confirm the actual DLA download URL on his machine before a live run will succeed

---
*Phase: 03-extract*
*Completed: 2026-03-15*
