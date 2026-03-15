---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: planning
stopped_at: Completed 03-extract 03-01-PLAN.md
last_updated: "2026-03-15T22:31:15.296Z"
last_activity: 2026-03-13 — Roadmap created
progress:
  total_phases: 4
  completed_phases: 3
  total_plans: 5
  completed_plans: 5
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-13)

**Core value:** Reliably download, transform, and upsert the characteristics data into SQL Server each month with zero manual steps.
**Current focus:** Phase 1 — Foundation and Load

## Current Position

Phase: 1 of 4 (Foundation and Load)
Plan: 0 of TBD in current phase
Status: Ready to plan
Last activity: 2026-03-13 — Roadmap created

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**
- Total plans completed: 0
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| - | - | - | - |

**Recent Trend:**
- Last 5 plans: —
- Trend: —

*Updated after each plan completion*
| Phase 01-foundation-and-load P01 | 7 | 2 tasks | 10 files |
| Phase 01-foundation-and-load P02 | 3 | 2 tasks | 6 files |
| Phase 01-foundation-and-load P02 | 60 | 3 tasks | 6 files |
| Phase 02-transform P01 | 201 | 2 tasks | 2 files |
| Phase 02-transform P02 | 8 | 2 tasks | 4 files |
| Phase 03-extract P01 | 2 | 2 tasks | 3 files |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Python for automation (pending confirmation)
- Upsert via UPDATE+INSERT, not MERGE — avoids documented SQL Server MERGE race condition bugs
- NIIN as merge key (pending confirmation)
- Use mssql-python 1.4.0 as primary driver; fall back to pyodbc 5.3.0 if compatibility issues arise
- [Phase 01-foundation-and-load]: INI format chosen for config (simplest for Tom to maintain)
- [Phase 01-foundation-and-load]: setup_logger() accepts logger_name param for test isolation on Windows
- [Phase 01-foundation-and-load]: config.ini gitignored; config.ini.example committed — credentials never in version control
- [Phase 01-foundation-and-load]: No PRIMARY KEY on V_CHARACTERISTICS_TESTING — MRC is varchar(max), row matching done by WHERE NIIN=? AND MRC=?
- [Phase 01-foundation-and-load]: UPDATE+INSERT with UPDLOCK+SERIALIZABLE hints — prevents phantom rows, never uses MERGE (LD-02)
- [Phase 01-foundation-and-load]: ODBC driver auto-detected via pyodbc.drivers() — prefer Driver 18, then 17, then generic SQL Server — no hardcoding
- [Phase 02-transform]: dtype=str on pd.read_csv — never allow pandas to infer numeric type for NIIN column
- [Phase 02-transform]: csv.Sniffer with explicit delimiters candidates — avoids sep=None parser ambiguity
- [Phase 02-transform]: keep_default_na=False — prevents empty cells converting to NaN in upsert string comparisons
- [Phase 02-transform]: Dynamic _PIVOT = date.today().year % 100 — pivot recalculates automatically each year without code changes
- [Phase 02-transform]: csv_path from cfg[paths][csv_path] — config INI [paths] section; consistent with existing INI pattern
- [Phase 03-extract]: Browser User-Agent header stored as HEADERS constant -- DLA CDN returns 403 to default Python User-Agent
- [Phase 03-extract]: download_url in config.ini [paths] -- not hardcoded so Tom can update URL without code changes
- [Phase 03-extract]: zipfile.is_zipfile() as DL-02 guard -- magic number is authoritative vs content-type header

### Pending Todos

None yet.

### Blockers/Concerns

- [Phase 3]: DLA download URL structure is LOW confidence — must manually inspect the DLA FLIS Electronic Reading Room page before implementing extract_data(). Verify: direct GET URL, whether auth/cookies are required, exact filename inside zip.
- [Phase 3]: SSL certificate behavior on the actual deployment Windows machine is unverified.
- [Phase 1]: mssql-python 1.4.0 compatibility with the specific on-prem SQL Server version is unconfirmed. Test during Phase 1; fall back to pyodbc immediately if issues arise.
- [Phase 2]: Actual CSV delimiter (pipe vs comma) must be confirmed by inspecting a real DLA file before finalizing transform logic.

## Session Continuity

Last session: 2026-03-15T22:31:15.294Z
Stopped at: Completed 03-extract 03-01-PLAN.md
Resume file: None
