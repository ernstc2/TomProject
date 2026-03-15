"""PubLog Importer — main entry point.

Loads configuration from config.ini, sets up a rotating file logger,
and orchestrates the import process.
"""

import configparser
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from db import get_connection, ensure_table, upsert_batch
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
    read_files = cfg.read(path)
    if not read_files:
        raise FileNotFoundError(
            f"Configuration file not found or empty: {path}"
        )
    return cfg


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

    return logger


def main():
    """Top-level orchestrator.

    Loads config, sets up logging, reads the CSV via load_csv() (which applies
    date conversion), connects to SQL Server, ensures the target table exists,
    upserts all rows, and logs results.
    Exits with code 0 on success, code 1 on any error.
    """
    conn = None
    try:
        cfg = load_config()
        log_dir = cfg["logging"]["log_dir"]
        max_bytes = int(cfg["logging"].get("max_bytes", 10_485_760))
        backup_count = int(cfg["logging"].get("backup_count", 5))

        logger = setup_logger(log_dir, max_bytes=max_bytes, backup_count=backup_count)
        logger.info("PubLog Importer started")

        # Connect to SQL Server
        conn = get_connection(cfg)
        server = cfg["database"]["server"]
        database = cfg["database"]["database"]
        logger.info("Connected to %s/%s", server, database)

        # Ensure the target table exists
        table = cfg["database"]["table"]
        ensure_table(conn, table)
        logger.info("Table %s ensured", table)

        # Download and extract the CSV
        url = cfg["paths"]["download_url"]
        work_dir = cfg["paths"]["work_dir"]
        logger.info("Downloading from %s", url)
        csv_path = extract_data(url, work_dir, logger)
        logger.info("CSV ready at %s", csv_path)

        # Load the CSV and convert dates
        df = load_csv(csv_path, logger)
        logger.info("Loaded %d rows from %s", len(df), csv_path)

        # Convert DataFrame rows to list of dicts for upsert
        rows = df.to_dict(orient="records")

        result = upsert_batch(conn, table, rows, logger)
        logger.info(
            "Upsert complete: inserted=%d updated=%d",
            result["inserted"],
            result["updated"],
        )

        logger.info("Run complete.")
        sys.exit(0)

    except Exception as exc:  # noqa: BLE001
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
