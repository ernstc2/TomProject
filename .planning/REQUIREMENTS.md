# Requirements: PubLog Characteristics Importer

**Defined:** 2026-03-13
**Core Value:** Reliably download, transform, and upsert the characteristics data into SQL Server each month with zero manual steps.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Download

- [ ] **DL-01**: Script downloads characteristics.zip from DLA FLIS site with browser User-Agent header
- [ ] **DL-02**: Script validates downloaded file is a valid zip before extraction
- [ ] **DL-03**: Script extracts the characteristics CSV from the zip to a working directory

### Transform

- [x] **TF-01**: Script removes quotes from all CSV fields
- [ ] **TF-02**: Script converts 2-digit year dates (dd-MMM-yy) to proper SQL date format with correct century pivot
- [x] **TF-03**: Script detects and validates the CSV delimiter (pipe vs comma)
- [x] **TF-04**: Script validates expected columns (NIIN, REQUIREMENTS_STATEMENT, MRC, CLEAR_TEXT_REPLY) exist before proceeding

### Load

- [x] **LD-01**: Script upserts data into SQL Server keyed on NIIN (insert new, update changed, skip unchanged)
- [x] **LD-02**: Script uses UPDATE+INSERT pattern (not MERGE) for upsert
- [x] **LD-03**: Script handles varchar(max) columns without silent truncation
- [x] **LD-04**: Script wraps upsert in a transaction and rolls back on failure

### Operations

- [x] **OP-01**: Script logs all actions to a rotating log file
- [x] **OP-02**: Script exits with code 0 on success, 1 on failure for Task Scheduler
- [x] **OP-03**: Script reads SQL connection details from a config file (not hardcoded)
- [ ] **OP-04**: A `.bat` wrapper with absolute paths runs the script via Task Scheduler
- [x] **OP-05**: Script uses a testing table (V_CHARACTERISTICS_TESTING) during development

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Reliability

- **RE-01**: Retry downloads with exponential backoff on transient failures
- **RE-02**: Checksum/size comparison to skip unchanged files

### Observability

- **OB-01**: Audit table tracking rows inserted/updated/skipped per run
- **OB-02**: Email notification on pipeline failure

### Developer Experience

- **DX-01**: Dry-run mode (show what would change without writing to database)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Other PubLog files (V_MOE_RULE, P_CAGE, etc.) | Only characteristics is needed |
| Website changes | This project only handles the data import side |
| Production table writes | Development uses V_CHARACTERISTICS_TESTING only |
| Orchestration framework (Airflow, Prefect) | Over-engineered for a single monthly script |
| Docker containerization | Runs directly on Windows with Task Scheduler |
| Real-time/CDC streaming | Monthly batch is the correct cadence |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DL-01 | Phase 3 | Pending |
| DL-02 | Phase 3 | Pending |
| DL-03 | Phase 3 | Pending |
| TF-01 | Phase 2 | Complete |
| TF-02 | Phase 2 | Pending |
| TF-03 | Phase 2 | Complete |
| TF-04 | Phase 2 | Complete |
| LD-01 | Phase 1 | Complete |
| LD-02 | Phase 1 | Complete |
| LD-03 | Phase 1 | Complete |
| LD-04 | Phase 1 | Complete |
| OP-01 | Phase 1 | Complete |
| OP-02 | Phase 1 | Complete |
| OP-03 | Phase 1 | Complete |
| OP-04 | Phase 4 | Pending |
| OP-05 | Phase 1 | Complete |

**Coverage:**
- v1 requirements: 16 total
- Mapped to phases: 16
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-13*
*Last updated: 2026-03-13 after roadmap creation*
