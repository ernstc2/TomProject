"""Database module — connection, table creation, and upsert logic.

Provides:
    get_connection(cfg)        -- Open a SQL Server connection via mssql-python (pyodbc fallback)
    ensure_table(conn, table)  -- Create V_CHARACTERISTICS_TESTING if it doesn't exist
    upsert_batch(conn, table, rows, logger) -- UPDATE+INSERT each row; never MERGE
"""

import logging

_logger = logging.getLogger(__name__)


def get_connection(cfg):
    """Open a SQL Server connection from a ConfigParser object.

    Reads connection parameters from cfg["database"]:
        server, database, username, password, encrypt, trust_server_certificate

    Uses mssql-python as the primary driver; falls back to pyodbc if unavailable.
    Sets autocommit=False so callers control transactions explicitly.

    Args:
        cfg: A ConfigParser object with a [database] section.

    Returns:
        An open database connection with autocommit disabled.

    Raises:
        Exception: If neither driver can be imported or the connection fails.
    """
    db = cfg["database"]
    server = db["server"]
    database = db["database"]
    username = db["username"]
    password = db["password"]
    encrypt = db.get("encrypt", "yes")
    trust_cert = db.get("trust_server_certificate", "yes")

    conn = None

    # Primary: mssql-python
    try:
        from mssql_python import connect as mssql_connect  # type: ignore
        conn_str = (
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_cert};"
        )
        conn = mssql_connect(conn_str)
        _logger.info("Connected via mssql-python to %s/%s", server, database)
    except ImportError:
        _logger.warning("mssql-python not available — falling back to pyodbc")

    # Fallback: pyodbc
    if conn is None:
        import pyodbc  # type: ignore

        # Pick the best available ODBC driver
        available = pyodbc.drivers()
        driver = None
        for candidate in [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ]:
            if candidate in available:
                driver = candidate
                break
        if driver is None:
            raise RuntimeError(
                f"No SQL Server ODBC driver found. Available: {available}"
            )

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_cert};"
        )
        conn = pyodbc.connect(conn_str)
        _logger.info("Connected via pyodbc to %s/%s", server, database)

    conn.autocommit = False
    return conn


def ensure_table(conn, table):
    """Create the target table if it does not already exist.

    The table schema matches production exactly:
        NIIN                   varchar(50)   NOT NULL
        MRC                    varchar(max)  NOT NULL
        REQUIREMENTS_STATEMENT varchar(max)  NULL
        CLEAR_TEXT_REPLY       varchar(max)  NULL

    No PRIMARY KEY constraint is created because MRC is varchar(max), which
    exceeds SQL Server's 900-byte index key limit.

    Args:
        conn:  An open database connection.
        table: The table name to check/create (e.g. "V_CHARACTERISTICS_TESTING").
    """
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
        (table,),
    )
    exists = cursor.fetchone() is not None

    if exists:
        _logger.info("Table %s already exists — skipping creation", table)
    else:
        cursor.execute(
            f"""
            CREATE TABLE {table} (
                NIIN                    varchar(50)   NOT NULL,
                MRC                     varchar(max)  NOT NULL,
                REQUIREMENTS_STATEMENT  varchar(max)  NULL,
                CLEAR_TEXT_REPLY        varchar(max)  NULL
            )
            """
        )
        conn.commit()
        _logger.info("Table %s created", table)


def upsert_batch(conn, table, rows, logger=None):
    """Upsert a list of row dicts into the target table using UPDATE+INSERT.

    For each row:
      1. Attempt UPDATE WHERE NIIN=? AND MRC=? (with UPDLOCK, SERIALIZABLE hints)
      2. If rowcount == 0, INSERT the row
      3. After all rows commit; on any error rollback and re-raise

    Never uses SQL MERGE (locked decision LD-02: avoids SQL Server MERGE race bugs).
    Never uses fast_executemany (risk of varchar(max) truncation, per research).

    Args:
        conn:   An open database connection with autocommit=False.
        table:  Target table name (e.g. "V_CHARACTERISTICS_TESTING").
        rows:   List of dicts with keys NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY.
        logger: Optional logger; falls back to module-level logger if None.

    Returns:
        dict: {"inserted": N, "updated": N}

    Raises:
        Exception: Any database error, after rolling back the transaction.
    """
    log = logger if logger is not None else _logger

    inserted = 0
    updated = 0

    cursor = conn.cursor()

    try:
        for row in rows:
            niin = row["NIIN"]
            mrc = row["MRC"]
            req_stmt = row["REQUIREMENTS_STATEMENT"]
            clear_reply = row["CLEAR_TEXT_REPLY"]

            # Attempt UPDATE first
            cursor.execute(
                f"""
                UPDATE {table} WITH (UPDLOCK, SERIALIZABLE)
                SET REQUIREMENTS_STATEMENT = ?,
                    CLEAR_TEXT_REPLY       = ?
                WHERE NIIN = ?
                  AND MRC  = ?
                """,
                (req_stmt, clear_reply, niin, mrc),
            )

            if cursor.rowcount >= 1:
                updated += 1
            else:
                # Row does not exist — INSERT
                cursor.execute(
                    f"""
                    INSERT INTO {table}
                        (NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY)
                    VALUES
                        (?, ?, ?, ?)
                    """,
                    (niin, mrc, req_stmt, clear_reply),
                )
                inserted += 1

        conn.commit()
        log.info(
            "Upsert complete: inserted=%d updated=%d table=%s",
            inserted,
            updated,
            table,
        )
        return {"inserted": inserted, "updated": updated}

    except Exception as exc:
        conn.rollback()
        log.error("Upsert failed, transaction rolled back: %s", exc)
        raise
