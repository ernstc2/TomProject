# PubLog Characteristics Importer

## What This Is

An automated pipeline that downloads the V_CHARACTERISTICS data file from the DLA FLIS Electronic Reading Room, transforms it (removes quotes from the pipe-delimited CSV), and upserts it into a SQL Server database. Runs monthly via Windows Task Scheduler to keep the database current without manual intervention.

## Core Value

Reliably download, transform, and upsert the characteristics data into SQL Server each month with zero manual steps.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Automatically download characteristics.zip from DLA FLIS site monthly
- [ ] Extract the V_CHARACTERISTIS CSV from the zip
- [ ] Parse the pipe-delimited CSV and remove quotes from fields
- [ ] Upsert into SQL Server: insert new rows, update changed rows, skip unchanged rows (keyed on NIIN)
- [ ] Use a testing table (V_CHARACTERISTICS_TESTING) during development
- [ ] Run on a monthly schedule via Windows Task Scheduler
- [ ] Log what happened each run (rows inserted, updated, skipped, errors)

### Out of Scope

- Other PubLog files (V_MOE_RULE, P_CAGE, V_FLIS_IDENTIFICATION, V_FLIS_MANAGEMENT, V_CAGE_STATUS_AND_TYPE) — characteristics first, others later
- Date conversion — V_CHARACTERISTICS has no date fields (only V_Management and V_MOE_Rule do)
- Website changes — this only handles the data import side
- Production table writes — development uses V_CHARACTERISTICS_TESTING only

## Context

- Data source: https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/
- The characteristics.zip file is updated monthly by DLA
- Uncle's existing process uses EmEditor for manual text transformations and a SQL script for import
- Target table schema: `dbo.V_CHARACTERISTICS` with columns: NIIN (varchar(50)), REQUIREMENTS_STATEMENT (varchar(max)), MRC (varchar(max)), CLEAR_TEXT_REPLY (varchar(max))
- Testing table: `dbo.V_CHARACTERISTICS_TESTING` — does not exist yet, must be created in Phase 1
- Tom's current import pattern: rename old table to X_ prefix backup, create empty clone, BULK INSERT from .TXT file on local disk
- Source file: comma-delimited CSV, all fields double-quoted, has header row
- Column order in CSV: NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY
- ~39 million rows (~2.9 GB uncompressed)
- NIIN is NOT unique — multiple rows per NIIN (different MRC values). Merge key TBD (likely NIIN + MRC)
- Tom's SQL script references pipe-delimited .TXT because EmEditor converted CSV→TXT and changed delimiter; we work with the raw CSV directly
- SQL Server connection via SQL authentication (username/password)
- Practice SQL Server instance available for development

## Constraints

- **Platform**: Windows — must work with Windows Task Scheduler
- **Database**: SQL Server with SQL authentication
- **Data source**: DLA website; downloads are zip files containing comma-delimited, double-quoted CSVs
- **Production database**: DN_Live
- **No AI attribution**: No traces of AI tooling in code, commits, or documentation

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Python for automation | Best CSV/HTTP/SQL libraries, easy to maintain. Python confirmed available on Tom's machine. | Confirmed |
| Upsert instead of drop+reload | Avoids downtime — Tom's current drop method blocks user access until bulk insert completes. Upsert keeps table available. | Confirmed by Tom |
| Merge key TBD | NIIN is not unique (multiple MRC values per NIIN). Need to confirm composite key with Tom. | Pending — ask Tom |

---
*Last updated: 2026-03-14 after Tom's email feedback*
