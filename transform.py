"""Core CSV transform module for PubLog importer.

Provides load_csv() which detects the delimiter, parses the file with all
columns as strings (preserving NIIN leading zeros), validates that the four
required columns are present, and returns a pandas DataFrame.
"""

import csv
import logging

import pandas as pd

# Columns that must be present in the CSV for the upsert pipeline to work.
REQUIRED_COLUMNS = {"NIIN", "REQUIREMENTS_STATEMENT", "MRC", "CLEAR_TEXT_REPLY"}

_log = logging.getLogger(__name__)


def _detect_delimiter(path, sample_bytes=8192):
    """Sniff the delimiter from the first *sample_bytes* of *path*.

    Checks comma, pipe, tab, and semicolon.  Falls back to comma if
    csv.Sniffer cannot determine the delimiter.

    Args:
        path: Path-like object pointing to the CSV file.
        sample_bytes: Number of bytes to read for sniffing (default 8 KB).

    Returns:
        A single-character delimiter string (e.g. ',' or '|').
    """
    try:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            sample = fh.read(sample_bytes)
        dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
        return dialect.delimiter
    except (csv.Error, UnicodeDecodeError):
        _log.debug("csv.Sniffer could not detect delimiter for %s; falling back to ','", path)
        return ","


def _validate_columns(df, logger=None):
    """Raise SystemExit(1) if any required columns are absent from *df*.

    All missing column names are logged at ERROR level before exiting so the
    operator can fix the source file in one pass.

    Args:
        df: pandas DataFrame whose columns are to be checked.
        logger: Optional stdlib logger.  Falls back to the module logger.

    Raises:
        SystemExit(1): When one or more required columns are missing.
    """
    log = logger if logger is not None else _log
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        for col in sorted(missing):
            log.error("Required column missing from CSV: %s", col)
        raise SystemExit(1)


def load_csv(path, logger=None):
    """Load a DLA characteristics CSV file and return a validated DataFrame.

    All columns are read as strings (dtype=str) so that NIIN leading zeros
    are never silently converted to integers.

    Args:
        path: Path-like object or str pointing to the CSV file.
        logger: Optional stdlib logger for info/error messages.

    Returns:
        A pandas DataFrame with at least the four required columns, all values
        as strings.

    Raises:
        FileNotFoundError: If *path* does not exist.
        SystemExit(1): If required columns are absent after loading.
    """
    log = logger if logger is not None else _log

    # Explicit existence check gives a clean error before pandas tries to open.
    import os
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")

    delimiter = _detect_delimiter(path)
    log.debug("Detected delimiter %r for %s", delimiter, path)

    df = pd.read_csv(
        path,
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
    )

    _validate_columns(df, logger=log)

    log.info("Loaded %d rows from %s", len(df), path)
    return df
