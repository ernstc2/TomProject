"""Core CSV transform module for PubLog importer.

Provides load_csv() which detects the delimiter, parses the file with all
columns as strings (preserving NIIN leading zeros), validates that the four
required columns are present, and returns a pandas DataFrame.
"""

import csv
import logging
import re
from datetime import date

import pandas as pd

# Columns that must be present in the CSV for the upsert pipeline to work.
REQUIRED_COLUMNS = {"NIIN", "REQUIREMENTS_STATEMENT", "MRC", "CLEAR_TEXT_REPLY"}

_log = logging.getLogger(__name__)

# Regex for dd-MMM-yy date patterns (case-insensitive month abbreviations).
_DATE_PAT = re.compile(
    r"\b(\d{1,2})-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-(\d{2})\b",
    re.IGNORECASE,
)

# Regex for dd-MMM-yyyy date patterns (4-digit year, no century pivot).
_DATE_PAT_4Y = re.compile(
    r"\b(\d{1,2})-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-(\d{4})\b",
    re.IGNORECASE,
)

# Dynamic pivot: two-digit years above this threshold belong to the previous century.
_PIVOT = date.today().year % 100

# Map 3-letter month abbreviations to zero-padded month numbers.
_MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _convert_date_match(m):
    """Convert a regex match of dd-MMM-yy to YYYY-MM-DD.

    Century pivot: if two-digit year > _PIVOT, the date is in the previous
    century (1900s); otherwise it is in the current century (2000s).

    Args:
        m: A re.Match object with groups (day, month_abbr, year_2digit).

    Returns:
        ISO-format date string 'YYYY-MM-DD'.
    """
    day = m.group(1).zfill(2)
    month = _MONTH_MAP[m.group(2).upper()]
    yy = int(m.group(3))
    year = (1900 + yy) if yy > _PIVOT else (2000 + yy)
    return f"{year}-{month}-{day}"


def _convert_date_match_4y(m):
    """Convert a regex match of dd-MMM-yyyy to YYYY-MM-DD (no century pivot).

    Args:
        m: A re.Match object with groups (day, month_abbr, year_4digit).

    Returns:
        ISO-format date string 'YYYY-MM-DD'.
    """
    day = m.group(1).zfill(2)
    month = _MONTH_MAP[m.group(2).upper()]
    year = m.group(3)
    return f"{year}-{month}-{day}"


def _convert_dates(df):
    """Replace dd-MMM-yy patterns in the CLEAR_TEXT_REPLY column with YYYY-MM-DD.

    Only the CLEAR_TEXT_REPLY column is modified; all other columns are
    untouched.  Returns a copy of the DataFrame.

    Args:
        df: pandas DataFrame that has already been validated for required columns.

    Returns:
        A new DataFrame with date patterns in CLEAR_TEXT_REPLY converted.
    """
    df = df.copy()
    df["CLEAR_TEXT_REPLY"] = df["CLEAR_TEXT_REPLY"].str.replace(
        _DATE_PAT, _convert_date_match, regex=True
    )
    return df


def _apply_date_conversion(df, date_columns, date_format):
    """Apply date format conversion to specified columns.

    Dispatches to the appropriate converter based on date_format:
    - "dd-MMM-yy"   -> 2-digit year with century pivot
    - "dd-MMM-yyyy" -> 4-digit year without pivot

    Args:
        df: pandas DataFrame to transform.
        date_columns: List of column names to apply conversion to.
        date_format: Format string "dd-MMM-yy" or "dd-MMM-yyyy".

    Returns:
        A new DataFrame with converted date columns.
    """
    df = df.copy()
    if date_format == "dd-MMM-yy":
        pat, converter = _DATE_PAT, _convert_date_match
    elif date_format == "dd-MMM-yyyy":
        pat, converter = _DATE_PAT_4Y, _convert_date_match_4y
    else:
        return df
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].str.replace(pat, converter, regex=True)
    return df


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


def _validate_columns(df, required=None, logger=None):
    """Raise SystemExit(1) if any required columns are absent from *df*.

    All missing column names are logged at ERROR level before exiting so the
    operator can fix the source file in one pass.

    Args:
        df: pandas DataFrame whose columns are to be checked.
        required: Optional list of required column names.  Falls back to the
            REQUIRED_COLUMNS constant when None (backwards compatibility).
        logger: Optional stdlib logger.  Falls back to the module logger.

    Raises:
        SystemExit(1): When one or more required columns are missing.
    """
    log = logger if logger is not None else _log
    req = REQUIRED_COLUMNS if required is None else set(required)
    missing = req - set(df.columns)
    if missing:
        for col in sorted(missing):
            log.error("Required column missing from CSV: %s", col)
        raise SystemExit(1)


def _normalize_numeric(value):
    """Strip leading zeros from a numeric string, preserving decimals.

    '000003228.01' -> '3228.01', '000000046.20' -> '46.20', '0' -> '0'.
    Non-numeric or empty values are returned unchanged.
    """
    if not value or not value.strip():
        return value
    stripped = value.lstrip("0") or "0"
    if stripped.startswith("."):
        stripped = "0" + stripped
    return stripped


def load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
    """Load a DLA characteristics CSV file and return a validated DataFrame.

    All columns are read as strings (dtype=str) so that NIIN leading zeros
    are never silently converted to integers.

    Args:
        path: Path-like object or str pointing to the CSV file.
        logger: Optional stdlib logger for info/error messages.
        required_columns: Optional list of required column names.  When None,
            falls back to the REQUIRED_COLUMNS constant (backwards compat).
            When provided, output is also subset to exactly these columns in
            this order.
        date_columns: Optional list of column names to apply date conversion
            to.  When None or empty, no date conversion is performed (unless
            required_columns is also None, which uses the legacy _convert_dates
            path).
        date_format: Format string controlling which converter to use:
            "dd-MMM-yy"   -> 2-digit year with century pivot
            "dd-MMM-yyyy" -> 4-digit year without pivot
        numeric_columns: Optional list of column names to strip leading zeros
            from (e.g. UNIT_PRICE '000003228.01' -> '3228.01').

    Returns:
        A pandas DataFrame with validated and optionally subset columns, all
        values as strings.

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

    _validate_columns(df, required=required_columns, logger=log)

    # Date conversion: legacy path when no required_columns (backwards compat),
    # or config-driven path when required_columns is provided.
    if required_columns is None:
        df = _convert_dates(df)
    elif date_columns and date_format:
        df = _apply_date_conversion(df, date_columns, date_format)

    # Strip leading zeros from numeric columns (e.g. UNIT_PRICE).
    if numeric_columns:
        df = df.copy()
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].map(_normalize_numeric)

    # Subset output to only the required columns (in specified order).
    if required_columns:
        df = df[required_columns]

    log.info("Loaded %d rows from %s", len(df), path)
    return df
