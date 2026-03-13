# PubLog Characteristics Importer

## What This Is

An automated pipeline that downloads the V_CHARACTERISTICS data file from the DLA FLIS Electronic Reading Room, transforms it (removes quotes, converts date formats), and upserts it into a SQL Server database. Runs monthly via Windows Task Scheduler to keep the database current without manual intervention.

## Core Value

Reliably download, transform, and upsert the characteristics data into SQL Server each month with zero manual steps.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Automatically download characteristics.zip from DLA FLIS site monthly
- [ ] Extract the V_CHARACTERISTIS CSV from the zip
- [ ] Remove quotes from CSV fields
- [ ] Convert 2-digit year dates (dd-MMM-yy → yyyy-MMM-dd format)
- [ ] Upsert into SQL Server: insert new rows, update changed rows, skip unchanged rows (keyed on NIIN)
- [ ] Use a testing table (V_CHARACTERISTICS_TESTING) during development
- [ ] Run on a monthly schedule via Windows Task Scheduler
- [ ] Log what happened each run (rows inserted, updated, skipped, errors)

### Out of Scope

- Other PubLog files (V_MOE_RULE, P_CAGE, V_FLIS_IDENTIFICATION, V_FLIS_MANAGEMENT, V_CAGE_STATUS_AND_TYPE) — only characteristics is needed
- Website changes — this only handles the data import side
- Production table writes — development uses V_CHARACTERISTICS_TESTING only

## Context

- Data source: https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/
- The characteristics.zip file is updated monthly by DLA
- Uncle's existing process uses EmEditor for manual text transformations and a SQL script for import
- Target table schema: `dbo.V_CHARACTERISTICS` with columns: NIIN (varchar(50)), REQUIREMENTS_STATEMENT (varchar(max)), MRC (varchar(max)), CLEAR_TEXT_REPLY (varchar(max))
- Testing table: `dbo.V_CHARACTERISTICS_TESTING` — mirrors production schema
- SQL Server connection via SQL authentication (username/password)
- Practice SQL Server instance available for development

## Constraints

- **Platform**: Windows — must work with Windows Task Scheduler
- **Database**: SQL Server with SQL authentication
- **Data source**: DLA website may change structure; downloads are zip files containing pipe-delimited or comma-delimited CSVs
- **No AI attribution**: No traces of AI tooling in code, commits, or documentation

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Python for automation | Best CSV/HTTP/SQL libraries, easy to maintain, cross-platform scripting | — Pending |
| Upsert instead of truncate+reload | Preserves unchanged data, more efficient, safer | — Pending |
| NIIN as merge key | Primary identifier for characteristics records | — Pending |

---
*Last updated: 2026-03-13 after initialization*
