# PubLog Importer v2.0

Automates the monthly table refresh from DLA FLIS PubLog data.
Downloads zip files from the DLA FLIS Electronic Reading Room, extracts CSVs,
and loads data into SQL Server using a load-swap strategy that preserves
the previous data as a backup.

## Tables

The pipeline processes four tables:

| Table | Zip File | Rows (approx) |
|-------|----------|----------------|
| V_CHARACTERISTICS | Characteristics.zip | ~39 million |
| V_CAGE_STATUS_AND_TYPE | CAGE.zip | ~2 million |
| V_MANAGEMENT | MANAGEMENT.zip | ~10 million |
| V_MOE_RULE | MOE_RULE.zip | ~15 million |

## Setup

### 1. Install Python

Download and install Python 3.10+ from https://www.python.org/downloads/

During install, check "Add Python to PATH".

After install, open a command prompt and verify:
```
python --version
```

### 2. Install dependencies

Open a command prompt in the project folder and run:
```
pip install -r requirements.txt
```

### 3. Configure config.ini

Create a file called `config.ini` in the project folder (it is not included for security reasons). Use `config.ini.example` as a template:

```
copy config.ini.example config.ini
```

Then open `config.ini` and replace the placeholder values:
- `YOUR_SERVER_NAME` — your SQL Server address
- `your_sql_login` — your SQL Server username
- `your_password` — your SQL Server password

The `target_table` setting in each table section controls which table gets written to.

### 4. Update run.bat

Open `run.bat` in a text editor. Update the Python path on the line that looks like:
```
"C:\Python313\python.exe" "%~dp0importer.py"
```

To find your Python path, run this in a command prompt:
```
where python
```

Then replace the path in run.bat with your result.

## Running the pipeline

### Option 1: Double-click

Double-click `run.bat` in File Explorer. A command window will open showing progress and close when done.

### Option 2: Command prompt

```
cd C:\path\to\project
.\run.bat
```

This runs all four tables in sequence. Check the exit code after:
```
echo %ERRORLEVEL%
```
- 0 = success
- 1 = error (check logs/publog_importer.log for details)

### Option 3: Run a single table

To run just one table instead of all four:
```
python importer.py --table V_MANAGEMENT
```

Valid table names: `V_CHARACTERISTICS`, `V_CAGE_STATUS_AND_TYPE`, `V_MANAGEMENT`, `V_MOE_RULE`

### Option 4: Task Scheduler (automated monthly runs)

Open a command prompt as Administrator and run:
```
schtasks /create /tn "PubLog Importer" /tr "C:\path\to\project\run.bat" /sc monthly /d 1 /st 06:00 /ru "DOMAIN\YourUser" /f
```

This runs the import on the 1st of every month at 6:00 AM.

## How it works

For each table, the pipeline:

1. Scrapes the DLA FLIS reading room page to find the download link
2. Downloads the zip file (e.g. CAGE.zip)
3. Extracts the CSV from inside the zip
4. Reads the CSV, validates columns, and converts date formats
5. Creates a new staging table (`TABLE_NEW`), bulk inserts all rows
6. Renames the current table to `TABLE_PRIOR` (backup)
7. Renames `TABLE_NEW` to the target table name

If anything goes wrong during load, the original table is untouched.

## Runtime

A full run of all four tables takes approximately 3-4 hours depending on network and server speed. Individual tables vary — V_CHARACTERISTICS is the longest at roughly 2 hours.

## Logs

Logs are written to `logs/publog_importer.log` with automatic rotation (10 MB max, 5 backups).

## Data safety

Each run preserves the previous table as `TABLE_PRIOR` (e.g. `V_MANAGEMENT_PRIOR`). If anything goes wrong, the previous data is still available.

## Project files

**Required for the pipeline:**
| File | Purpose |
|------|---------|
| `importer.py` | Main entry point — orchestrates the full pipeline |
| `db.py` | Database connection, table creation, and load-swap |
| `extract.py` | Downloads and extracts zip files from DLA |
| `transform.py` | Reads and validates CSV data |
| `config.ini` | Your server credentials and table settings (you create this) |
| `config.ini.example` | Template for config.ini |
| `run.bat` | Launcher script for manual or scheduled runs |
| `requirements.txt` | Python package dependencies |

**Not required (development only):**
| File | Purpose |
|------|---------|
| `tests/` | Automated tests — safe to ignore |
| `pytest.ini` | Test configuration — safe to ignore |
