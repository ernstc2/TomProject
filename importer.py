"""PubLog Importer — main entry point.

Loads configuration from config.ini, sets up a rotating file logger,
and orchestrates the import process across all configured table sections.
"""

import argparse
import configparser
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from db import get_connection, ensure_table, upsert_batch, upsert_bulk, load_swap, swap_mrc_columns
from extract import extract_data
from transform import load_csv


def load_config(path="config.ini"):
    """Read an INI configuration file and return a ConfigParser object.

    Args:
        path: Path to the INI file. Defaults to "config.ini".

    Returns:
        A ConfigParser populated with values from the file.

    Raises:
        FileNotFoundError: If the file does not exist or cannot be read.
    """
    cfg = configparser.ConfigParser()
    read_files = cfg.read(path, encoding="utf-8-sig")
    if not read_files:
        raise FileNotFoundError(
            f"Configuration file not found or empty: {path}"
        )
    return cfg


RESERVED_SECTIONS = {"DEFAULT", "database", "logging", "paths"}


def get_table_sections(cfg):
    """Return section names that represent table definitions.

    Excludes reserved INI sections (database, logging, paths, DEFAULT).

    Args:
        cfg: A ConfigParser object.

    Returns:
        List of section name strings that are table definitions.
    """
    return [s for s in cfg.sections() if s not in RESERVED_SECTIONS]


def parse_list(value):
    """Parse comma-separated config string into a list of stripped strings.

    Args:
        value: A comma-separated string (e.g. "NIIN,MRC,CLEAR_TEXT_REPLY") or None.

    Returns:
        List of non-empty stripped strings. Returns [] for None or empty input.
    """
    if not value or not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args():
    """Parse command-line arguments.

    Returns:
        argparse.Namespace with attribute:
            table (str or None): Table section name to process, or None to run all.
    """
    parser = argparse.ArgumentParser(description="PubLog importer")
    parser.add_argument(
        "--table",
        metavar="NAME",
        default=None,
        help="Run only this table section (e.g. V_MANAGEMENT). Omit to run all.",
    )
    return parser.parse_args()


class IssueCollector(logging.Handler):
    """A logging handler that captures WARNING and ERROR messages for end-of-run summary."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.warnings = []
        self.errors = []

    def emit(self, record):
        msg = self.format(record) if self.formatter else record.getMessage()
        if record.levelno >= logging.ERROR:
            self.errors.append(msg)
        else:
            self.warnings.append(msg)

    def reset(self):
        self.warnings.clear()
        self.errors.clear()

    @property
    def has_issues(self):
        return bool(self.warnings or self.errors)


# Module-level so main() can inspect issues after the pipeline runs.
issue_collector = IssueCollector()


def setup_logger(log_dir, max_bytes=10_485_760, backup_count=5, logger_name="publog_importer"):
    """Create and configure a logger with rotating file and console handlers.

    Args:
        log_dir: Directory where the log file will be written.
        max_bytes: Maximum size of each log file before rotation (default 10 MB).
        backup_count: Number of backup log files to retain (default 5).
        logger_name: Name for the logger instance (default "publog_importer").

    Returns:
        A configured logging.Logger instance at INFO level.
    """
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "publog_importer.log")
    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.addHandler(issue_collector)

    return logger


def run_table(cfg, section, conn, logger):
    """Run the full extract-transform-load pipeline for one table section.

    Args:
        cfg: A ConfigParser object containing all config sections.
        section: The INI section name for this table (e.g. "V_MANAGEMENT").
        conn: An open database connection.
        logger: A configured logger instance.

    Raises:
        Any exception from extract_data, load_csv, or load_swap propagates up
        so the caller can handle failure isolation.
    """
    table_cfg = cfg[section]
    url = table_cfg["download_url"]
    csv_name = table_cfg["csv_name"]
    zip_name = table_cfg.get("zip_name", "Characteristics.zip")
    target = table_cfg["target_table"]
    columns = parse_list(table_cfg.get("columns", ""))
    date_cols = parse_list(table_cfg.get("date_columns", ""))
    date_fmt = table_cfg.get("date_format", "").strip()
    numeric_cols = parse_list(table_cfg.get("numeric_columns", ""))
    index_cols = parse_list(table_cfg.get("index_columns", ""))
    col_size = int(table_cfg.get("column_size", "500"))
    work_dir = cfg["paths"]["work_dir"]

    logger.info("Downloading %s", url)
    csv_path = extract_data(url, work_dir, logger, zip_name=zip_name, csv_name=csv_name)

    logger.info("Loading CSV: %s", csv_path)
    df = load_csv(
        csv_path,
        logger=logger,
        required_columns=columns or None,
        date_columns=date_cols or None,
        date_format=date_fmt or None,
        numeric_columns=numeric_cols or None,
    )

    rows = df.to_dict(orient="records")
    actual_columns = list(df.columns)
    result = load_swap(conn, target, rows, logger, columns=actual_columns,
                       index_columns=index_cols or None,
                       column_size=col_size)
    logger.info("Table %s complete: %d rows loaded", section, result["loaded"])


def main():
    """Top-level orchestrator.

    Reads all table sections from config, optionally filters to a single table
    via --table NAME, runs each table through run_table(), and exits with code 0
    if all succeed or code 1 if any fail (failure-isolated: one table failing
    does not abort the remaining tables).
    """
    conn = None
    logger = None
    try:
        args = parse_args()
        cfg = load_config()
        log_dir = cfg["logging"]["log_dir"]
        max_bytes = int(cfg["logging"].get("max_bytes", 10_485_760))
        backup_count = int(cfg["logging"].get("backup_count", 5))

        issue_collector.reset()  # Reset from any previous run
        logger = setup_logger(log_dir, max_bytes=max_bytes, backup_count=backup_count)
        logger.info("PubLog Importer started")

        conn = get_connection(cfg)
        server = cfg["database"]["server"]
        database = cfg["database"]["database"]
        logger.info("Connected to %s/%s", server, database)

        table_sections = get_table_sections(cfg)
        logger.info("Found %d table(s): %s", len(table_sections), table_sections)

        if args.table is not None:
            if args.table not in table_sections:
                logger.error(
                    "Unknown table: %s. Configured tables: %s",
                    args.table,
                    table_sections,
                )
                sys.exit(1)
            table_sections = [args.table]

        failed_tables = []

        for section in table_sections:
            logger.info("=" * 60)
            logger.info("Processing table: %s", section)
            logger.info("=" * 60)
            try:
                run_table(cfg, section, conn, logger)
            except Exception as exc:
                logger.error("Table %s failed: %s", section, exc)
                failed_tables.append(section)

        # Print a clear end-of-run summary
        print("")
        print("=" * 60)

        if failed_tables or issue_collector.has_issues:
            if issue_collector.errors:
                print(f"  ERRORS ({len(issue_collector.errors)}):")
                for msg in issue_collector.errors:
                    print(f"    - {msg}")

            if issue_collector.warnings:
                print(f"  WARNINGS ({len(issue_collector.warnings)}):")
                for msg in issue_collector.warnings:
                    print(f"    - {msg}")

            if failed_tables:
                print(f"  FAILED TABLES: {', '.join(failed_tables)}")

            print("=" * 60)
            sys.exit(1)
        else:
            logger.info("Pipeline complete. All tables succeeded — no errors or warnings.")
            print("  Pipeline complete. All tables succeeded — no errors or warnings.")
            print("=" * 60)
            sys.exit(0)

    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        if logger:
            logger.error("Fatal error: %s", exc, exc_info=True)
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
