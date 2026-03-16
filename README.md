# PubLog Characteristics Importer

Automates the monthly V_CHARACTERISTICS table refresh from the DLA FLIS PubLog data.
Replaces the manual process of downloading Characteristics.zip, extracting, and running BULK INSERT scripts.

## What it does

1. Downloads Characteristics.zip from the DLA FLIS Electronic Reading Room
2. Extracts the CSV from the zip
3. Loads all ~39 million rows into V_CHARACTERISTICS
4. Preserves the previous data in V_CHARACTERISTICS_PRIOR as a backup
5. Renames columns to match the existing table schema

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

Create a file called `config.ini` in the project folder (it is not included for security reasons). Use this template:

```ini
[database]
server = YOUR_SERVER
database = DN_Live
username = YOUR_USERNAME
password = YOUR_PASSWORD
table = V_CHARACTERISTICS
encrypt = yes
trust_server_certificate = yes

[logging]
log_dir = logs
max_bytes = 10485760
backup_count = 5

[paths]
download_url = https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/
work_dir = work
```

Replace `YOUR_SERVER`, `YOUR_USERNAME`, and `YOUR_PASSWORD` with your SQL Server credentials.

### 4. Update run.bat

Open `run.bat` in a text editor. Update line 15 with your Python path.

To find your Python path, run this in a command prompt:
```
where python
```

Then replace the path in run.bat:
```
"C:\YOUR\PATH\TO\python.exe" "%~dp0importer.py"
```

## Running the pipeline

### Option 1: Double-click

Double-click `run.bat` in File Explorer. A command window will open showing progress and close when done.

### Option 2: Command prompt

```
cd C:\path\to\project
.\run.bat
```

Check the exit code after:
```
echo %ERRORLEVEL%
```
- 0 = success
- 1 = error (check logs/publog_importer.log for details)

### Option 3: Task Scheduler (automated monthly runs)

Open a command prompt as Administrator and run:
```
schtasks /create /tn "PubLog Importer" /tr "C:\path\to\project\run.bat" /sc monthly /d 1 /st 06:00 /ru "DOMAIN\YourUser" /f
```

This runs the import on the 1st of every month at 6:00 AM.

## Runtime

A full run takes approximately 2 hours depending on network and server speed.

## Logs

Logs are written to `logs/publog_importer.log` with automatic rotation (10 MB max, 5 backups).

## Data safety

Each run preserves the previous table as `V_CHARACTERISTICS_PRIOR`. If anything goes wrong, the previous data is still available. The pipeline will not drop the prior backup unless the current table has data to replace it.

## Project files

**Required for the pipeline:**
| File | Purpose |
|------|---------|
| `importer.py` | Main entry point -- orchestrates the full pipeline |
| `db.py` | Database connection, table loading, and column swap |
| `extract.py` | Downloads and extracts the zip from DLA |
| `transform.py` | Reads and validates the CSV data |
| `config.ini` | Your server credentials and settings (you create this) |
| `run.bat` | Launcher script for manual or scheduled runs |
| `requirements.txt` | Python package dependencies |

**Not required (development only):**
| File | Purpose |
|------|---------|
| `tests/` | Automated tests used during development -- safe to ignore |
| `pytest.ini` | Test configuration -- safe to ignore |
