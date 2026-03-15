# Roadmap: PubLog Characteristics Importer

## Overview

Build a monthly ETL pipeline in four phases ordered by risk. Prove the database connection and upsert logic first (the riskiest unknowns), then add the transformation layer with isolated unit-testable logic, then implement the DLA download (the most uncertain external dependency), then wire everything together into a Task Scheduler job. Each phase delivers a tested, independently verifiable component before the next phase begins.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundation and Load** - Project scaffold, config/logging infrastructure, and verified upsert against the testing table (completed 2026-03-15)
- [ ] **Phase 2: Transform** - Comma-delimited CSV parsing, quote stripping, and schema validation producing a clean DataFrame
- [ ] **Phase 3: Extract** - HTTPS download from DLA with User-Agent header, zip validation, and CSV extraction
- [ ] **Phase 4: Integration and Scheduler** - Wire all stages through main(), create run.bat wrapper, configure and verify Task Scheduler

## Phase Details

### Phase 1: Foundation and Load
**Goal**: The project environment exists, credentials and config are externalized, logging works, and the upsert function correctly inserts, updates, and skips rows in the testing table
**Depends on**: Nothing (first phase)
**Requirements**: OP-01, OP-02, OP-03, OP-05, LD-01, LD-02, LD-03, LD-04
**Success Criteria** (what must be TRUE):
  1. Running the script with a hand-crafted test DataFrame writes the correct rows to V_CHARACTERISTICS_TESTING and the log file shows inserted/updated/skipped counts
  2. Re-running the same data produces zero inserts and zero updates (idempotent upsert using confirmed merge key)
  3. Running with changed rows produces the correct number of updates and the old values are gone from the testing table
  4. A simulated failure mid-upsert rolls back the transaction and leaves the testing table unchanged
  5. All connection details (server, database, username, password, table name) are read from a config file and absent from the source code
**Plans:** 2/2 plans complete

Plans:
- [ ] 01-01-PLAN.md — Project scaffold: config loading, rotating logger, main entry point, test infrastructure
- [ ] 01-02-PLAN.md — Database connection, UPDATE+INSERT upsert, integration tests, end-to-end verification

### Phase 2: Transform
**Goal**: A function accepts a raw comma-delimited CSV file path and returns a clean, validated DataFrame with quotes stripped and correct columns present — verified with sample data including edge cases
**Depends on**: Phase 1
**Requirements**: TF-01, TF-02, TF-03
**Success Criteria** (what must be TRUE):
  1. A comma-delimited CSV containing quoted fields produces a DataFrame where all quotes are removed from every field
  2. The parser correctly handles comma delimiters, double-quoted fields, and the header row
  3. A CSV missing one of the required columns (NIIN, REQUIREMENTS_STATEMENT, MRC, CLEAR_TEXT_REPLY) causes the script to exit with code 1 and log the missing column name
**Plans**: TBD

### Phase 3: Extract
**Goal**: A function downloads the characteristics.zip from the live DLA FLIS site, validates it is a real zip, and returns the path to the extracted CSV — verified against the actual DLA URL on the deployment machine
**Depends on**: Phase 2
**Requirements**: DL-01, DL-02, DL-03
**Success Criteria** (what must be TRUE):
  1. Running the extract function against the live DLA URL downloads a file and extracts the V_CHARACTERISTIS CSV to the work directory without a 403 error
  2. Pointing the function at a URL that returns an HTML error page causes the script to exit with code 1 rather than attempting to unzip HTML
  3. The extracted CSV file exists on disk and the function returns its absolute path
**Plans**: TBD

### Phase 4: Integration and Scheduler
**Goal**: All three stages run end-to-end from a single entry point, the run.bat wrapper launches the correct Python executable, and Windows Task Scheduler completes the job unattended with a non-empty log file and correct exit code
**Depends on**: Phase 3
**Requirements**: OP-04
**Success Criteria** (what must be TRUE):
  1. Running run.bat from a terminal executes the full pipeline (download → transform → upsert) and the log file contains a run-summary line with row counts
  2. The Task Scheduler job completes with Last Run Result 0x0 and the log file shows the run completed after the scheduled trigger fired
  3. Running the pipeline a second time against the same DLA data produces zero inserts and zero updates (end-to-end idempotency confirmed)
  4. A deliberate failure (bad credentials) causes Task Scheduler to record a non-zero exit code and the log file contains the error
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation and Load | 2/2 | Complete   | 2026-03-15 |
| 2. Transform | 0/TBD | Not started | - |
| 3. Extract | 0/TBD | Not started | - |
| 4. Integration and Scheduler | 0/TBD | Not started | - |
