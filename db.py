"""Database module — connection, table creation, and upsert logic.

Provides:
    get_connection(cfg)        -- Open a SQL Server connection via mssql-python (pyodbc fallback)
    ensure_table(conn, table)  -- Create V_CHARACTERISTICS_TESTING if it doesn't exist
    upsert_batch(conn, table, rows, logger) -- UPDATE+INSERT each row; never MERGE
    upsert_bulk(conn, table, rows, logger)  -- Staging table bulk upsert for large datasets
    load_swap(conn, table, rows, logger)    -- Full table replacement via bulk load + rename
    swap_mrc_columns(conn, table, logger)   -- Swap MRC/REQUIREMENTS_STATEMENT column names
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
            f"Connection Timeout=60;"
        )
        conn = pyodbc.connect(conn_str, timeout=60)
        conn.timeout = 900  # 15-minute command timeout
        _logger.info("Connected via pyodbc to %s/%s", server, database)

    conn.autocommit = False
    return conn


def ensure_table(conn, table):
    """Create the target table if it does not already exist.

    The table schema matches production exactly:
        NIIN                   varchar(50)   NOT NULL
        MRC                    varchar(150)  NOT NULL
        REQUIREMENTS_STATEMENT varchar(150)  NULL
        CLEAR_TEXT_REPLY       varchar(150)  NULL

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
                MRC                     varchar(150)  NOT NULL,
                REQUIREMENTS_STATEMENT  varchar(150)  NULL,
                CLEAR_TEXT_REPLY        varchar(150)  NULL
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


_BULK_CHUNK = 10_000


def upsert_bulk(conn, table, rows, logger=None):
    """Bulk upsert via staging table — designed for millions of rows.

    Strategy:
      1. Create a staging table with the same schema + index on NIIN.
      2. Bulk-load all rows into staging using fast_executemany in chunks.
      3. UPDATE target rows that exist but have changed data.
      4. INSERT rows that exist in staging but not in target.
      5. Drop the staging table.

    Never uses SQL MERGE (locked decision LD-02).
    Uses fast_executemany with explicit setinputsizes to avoid varchar(max) truncation.

    Args:
        conn:   An open database connection with autocommit=False.
        table:  Target table name (e.g. "V_CHARACTERISTICS_TESTING").
        rows:   List of dicts with keys NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY.
        logger: Optional logger; falls back to module-level logger if None.

    Returns:
        dict: {"inserted": N, "updated": N}

    Raises:
        Exception: Any database error, after rolling back and cleaning up staging.
    """
    log = logger if logger is not None else _logger
    staging = f"{table}_STAGING"
    cursor = conn.cursor()

    try:
        # 1. Create staging table
        cursor.execute(f"DROP TABLE IF EXISTS {staging}")
        cursor.execute(
            f"""
            CREATE TABLE {staging} (
                NIIN                    varchar(50)   NOT NULL,
                MRC                     varchar(max)  NOT NULL,
                REQUIREMENTS_STATEMENT  varchar(max)  NULL,
                CLEAR_TEXT_REPLY        varchar(max)  NULL
            )
            """
        )
        cursor.execute(f"CREATE INDEX IX_{staging}_NIIN ON {staging} (NIIN)")
        conn.commit()
        log.info("Staging table %s created", staging)

        # 2. Bulk-load into staging using fast_executemany
        cursor.fast_executemany = True
        # Explicit column sizes prevent varchar(max) truncation bug in pyodbc.
        # (sql_type, size, precision) — size=0 means max/unlimited.
        import pyodbc as _pyodbc
        cursor.setinputsizes(
            [(_pyodbc.SQL_VARCHAR, 50, 0),   # NIIN: varchar(50)
             (_pyodbc.SQL_VARCHAR, 0, 0),    # MRC: varchar(max)
             (_pyodbc.SQL_VARCHAR, 0, 0),    # REQUIREMENTS_STATEMENT
             (_pyodbc.SQL_VARCHAR, 0, 0)]    # CLEAR_TEXT_REPLY
        )

        insert_sql = (
            f"INSERT INTO {staging} "
            f"(NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY) "
            f"VALUES (?, ?, ?, ?)"
        )

        total = len(rows)
        for i in range(0, total, _BULK_CHUNK):
            chunk = rows[i : i + _BULK_CHUNK]
            params = [
                (r["NIIN"], r["MRC"], r["REQUIREMENTS_STATEMENT"], r["CLEAR_TEXT_REPLY"])
                for r in chunk
            ]
            cursor.executemany(insert_sql, params)
            if (i // _BULK_CHUNK) % 100 == 0:
                loaded = min(i + _BULK_CHUNK, total)
                pct = loaded / total * 100
                log.info("Bulk load progress: %d / %d rows (%.1f%%)", loaded, total, pct)

        conn.commit()
        log.info("Bulk load complete: %d rows in staging (100.0%%)", total)

        # Create index on target NIIN if not exists (speeds up the JOIN)
        cursor.execute(
            f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE object_id = OBJECT_ID('{table}')
                  AND name = 'IX_{table}_NIIN'
            )
            CREATE INDEX IX_{table}_NIIN ON {table} (NIIN)
            """
        )
        conn.commit()

        # 3. UPDATE existing rows where data has changed
        cursor.execute(
            f"""
            UPDATE t
            SET t.REQUIREMENTS_STATEMENT = s.REQUIREMENTS_STATEMENT,
                t.CLEAR_TEXT_REPLY       = s.CLEAR_TEXT_REPLY
            FROM {table} t
            INNER JOIN {staging} s
                ON t.NIIN = s.NIIN
               AND t.MRC  = s.MRC
            WHERE ISNULL(t.REQUIREMENTS_STATEMENT, '') != ISNULL(s.REQUIREMENTS_STATEMENT, '')
               OR ISNULL(t.CLEAR_TEXT_REPLY, '')       != ISNULL(s.CLEAR_TEXT_REPLY, '')
            """
        )
        updated = cursor.rowcount
        conn.commit()
        log.info("Updated %d changed rows", updated)

        # 4. INSERT rows not already in target
        cursor.execute(
            f"""
            INSERT INTO {table}
                (NIIN, MRC, REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY)
            SELECT s.NIIN, s.MRC, s.REQUIREMENTS_STATEMENT, s.CLEAR_TEXT_REPLY
            FROM {staging} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {table} t
                WHERE t.NIIN = s.NIIN
                  AND t.MRC  = s.MRC
            )
            """
        )
        inserted = cursor.rowcount
        conn.commit()
        log.info("Inserted %d new rows", inserted)

        # 5. Drop staging table
        cursor.execute(f"DROP TABLE {staging}")
        conn.commit()

        log.info(
            "Bulk upsert complete: inserted=%d updated=%d table=%s",
            inserted, updated, table,
        )
        return {"inserted": inserted, "updated": updated}

    except Exception as exc:
        conn.rollback()
        # Clean up staging table on failure
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")
            conn.commit()
        except Exception:
            pass
        log.error("Bulk upsert failed, transaction rolled back: %s", exc)
        raise


def load_swap(conn, table, rows=None, logger=None, columns=None, index_columns=None,
              column_size=150, chunks=None):
    """Full table replacement via bulk load + rename, preserving prior data.

    Strategy:
      1. Bulk-load all rows into a new table (e.g. V_CHARACTERISTICS_NEW).
      2. Only drop _PRIOR if the current table has data (safety check).
      3. Rename the current table to _PRIOR.
      4. Rename the new table to the real name.
      5. Create non-clustered indexes on specified columns.

    The _PRIOR table is kept as a backup of the previous dataset.
    The rename is a metadata-only operation, so users lose access for
    milliseconds instead of the entire load duration.

    Accepts data in two forms (supply one):
      - rows:   List of dicts (legacy, loads all data into memory).
      - chunks: Iterable of (DataFrame, columns) tuples from stream_csv.
                Only one chunk is in memory at a time.

    Args:
        conn:    An open database connection with autocommit=False.
        table:   Target table name (e.g. "V_CHARACTERISTICS").
        rows:    List of dicts. Keys must include all names in columns.
        logger:  Optional logger; falls back to module-level logger if None.
        columns: Ordered list of column names to include. If None, inferred
                 from the first row's dict keys. Drives CREATE TABLE and INSERT.
        index_columns: List of column names to create non-clustered indexes on
                       after the swap completes. If None, no indexes are created.
        column_size: varchar size for columns in the new table (default 500).
        chunks:  Iterable of (DataFrame, columns_list) tuples for streaming mode.

    Returns:
        dict: {"loaded": N}

    Raises:
        Exception: Any database error, after cleaning up temp tables.
    """
    log = logger if logger is not None else _logger

    # Resolve columns for legacy mode; streaming mode gets them from chunks
    if chunks is None:
        if columns is None:
            columns = list(rows[0].keys()) if rows else []

    new_table = f"{table}_NEW"
    prior_table = f"{table}_PRIOR"
    cursor = conn.cursor()

    try:
        # Clean up leftover _NEW from any previous failed run
        cursor.execute(f"DROP TABLE IF EXISTS {new_table}")
        conn.commit()

        if chunks is not None:
            # --- Streaming mode: process one chunk at a time ---
            total = 0
            table_created = False

            for chunk_df, chunk_columns, total_expected in chunks:
                if not table_created:
                    columns = chunk_columns
                    # Create the new table
                    index_set = set(index_columns or [])
                    default_type = "varchar(max)" if column_size == 0 else f"varchar({column_size})"
                    def _col_type(col):
                        if column_size == 0 and col in index_set:
                            return "varchar(900)"
                        return default_type
                    col_defs = ",\n                ".join(
                        f"[{col}] {_col_type(col)} NULL" for col in columns
                    )
                    cursor.execute(f"""
                        CREATE TABLE {new_table} (
                            {col_defs}
                        )
                    """)
                    conn.commit()
                    log.info("Created %s for bulk load", new_table)

                    # Configure driver for bulk inserts
                    if 'pyodbc' in type(conn).__module__:
                        cursor.fast_executemany = True
                        import pyodbc as _pyodbc
                        cursor.setinputsizes(
                            [(_pyodbc.SQL_VARCHAR, 0, 0)] * len(columns)
                        )

                    col_list = ", ".join(f"[{col}]" for col in columns)
                    placeholders = ", ".join("?" * len(columns))
                    insert_sql = f"INSERT INTO {new_table} ({col_list}) VALUES ({placeholders})"
                    table_created = True

                # Insert this chunk in sub-batches
                chunk_rows = chunk_df.values.tolist()
                for i in range(0, len(chunk_rows), _BULK_CHUNK):
                    batch = chunk_rows[i : i + _BULK_CHUNK]
                    cursor.executemany(insert_sql, batch)

                total += len(chunk_rows)
                conn.commit()
                if total_expected > 0:
                    pct = total / total_expected * 100
                    log.info("Bulk load progress: %d / %d rows (%.1f%%)", total, total_expected, pct)
                else:
                    log.info("Bulk load progress: %d rows loaded so far", total)

            if not table_created:
                raise ValueError("No data chunks received — nothing to load")

            log.info("Bulk load complete: %d rows in %s", total, new_table)

        else:
            # --- Legacy mode: all rows in memory ---
            # 1. Create the new table with dynamic columns
            index_set = set(index_columns or [])
            default_type = "varchar(max)" if column_size == 0 else f"varchar({column_size})"
            def _col_type(col):
                if column_size == 0 and col in index_set:
                    return "varchar(900)"
                return default_type
            col_defs = ",\n                ".join(f"[{col}] {_col_type(col)} NULL" for col in columns)
            cursor.execute(
                f"""
                CREATE TABLE {new_table} (
                    {col_defs}
                )
                """
            )
            conn.commit()
            log.info("Created %s for bulk load", new_table)

            # 2. Bulk-load all rows into the new table
            if 'pyodbc' in type(conn).__module__:
                cursor.fast_executemany = True
                import pyodbc as _pyodbc
                cursor.setinputsizes(
                    [(_pyodbc.SQL_VARCHAR, 0, 0)] * len(columns)
                )

            col_list = ", ".join(f"[{col}]" for col in columns)
            placeholders = ", ".join("?" * len(columns))
            insert_sql = f"INSERT INTO {new_table} ({col_list}) VALUES ({placeholders})"

            total = len(rows)
            for i in range(0, total, _BULK_CHUNK):
                chunk = rows[i : i + _BULK_CHUNK]
                params = [tuple(r[col] for col in columns) for r in chunk]
                cursor.executemany(insert_sql, params)
                if (i // _BULK_CHUNK) % 100 == 0:
                    loaded = min(i + _BULK_CHUNK, total)
                    pct = loaded / total * 100
                    log.info("Bulk load progress: %d / %d rows (%.1f%%)", loaded, total, pct)

            conn.commit()
            log.info("Bulk load complete: %d rows in %s (100.0%%)", total, new_table)

        # 3. Convert empty strings to NULL
        for col in columns:
            cursor.execute(f"UPDATE {new_table} SET [{col}] = NULL WHERE [{col}] = ''")
        conn.commit()
        log.info("Converted empty strings to NULL in %s", new_table)

        # 4. Swap tables (all in one transaction so failure is atomic)
        cursor.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
            (table,),
        )
        current_exists = cursor.fetchone() is not None

        if current_exists:
            # Quick existence check — no need to count every row
            cursor.execute(f"SELECT TOP 1 1 FROM {table}")
            has_data = cursor.fetchone() is not None

            if has_data:
                # Safe to replace _PRIOR — current table has data
                cursor.execute(f"DROP TABLE IF EXISTS {prior_table}")
                log.info("Dropped old %s", prior_table)
            else:
                # Current table is empty — keep _PRIOR as safety net
                log.warning(
                    "%s is empty — keeping %s as safety net",
                    table, prior_table,
                )

            cursor.execute(f"EXEC sp_rename '{table}', '{prior_table}'")
            log.info("Renamed %s -> %s", table, prior_table)

        cursor.execute(f"EXEC sp_rename '{new_table}', '{table}'")
        conn.commit()
        log.info("Table swap complete: %s is now live", table)

        # 5. Create non-clustered indexes on specified columns
        for idx_col in (index_columns or []):
            idx_name = f"IX_{table}_{idx_col}"
            cursor.execute(
                f"CREATE NONCLUSTERED INDEX [{idx_name}] ON [{table}] ([{idx_col}])"
            )
            conn.commit()
            log.info("Created index %s on %s", idx_name, table)

        log.info("Load-swap complete: %d rows loaded into %s", total, table)
        return {"loaded": total}

    except Exception as exc:
        conn.rollback()
        # Clean up on failure — try to restore original if swap partially completed
        try:
            cursor.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                (table,),
            )
            target_exists = cursor.fetchone() is not None
            if not target_exists:
                cursor.execute(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                    (prior_table,),
                )
                if cursor.fetchone() is not None:
                    cursor.execute(f"EXEC sp_rename '{prior_table}', '{table}'")
                    conn.commit()
                    log.info("Restored %s from %s after failure", table, prior_table)

            cursor.execute(f"DROP TABLE IF EXISTS {new_table}")
            conn.commit()
        except Exception:
            pass
        log.error("Load-swap failed: %s", exc)
        raise


def swap_mrc_columns(conn, table, logger=None):
    """Swap the MRC and REQUIREMENTS_STATEMENT column names.

    Tom's original table has these two columns named in the opposite order
    due to positional BULK INSERT mapping. This renames the columns after
    load so downstream queries and reports continue to work unchanged.

    Uses a three-step sp_rename through a temp name since SQL Server
    cannot rename two columns simultaneously.

    Args:
        conn:   An open database connection with autocommit=False.
        table:  Target table name (e.g. "V_CHARACTERISTICS_TESTING").
        logger: Optional logger; falls back to module-level logger if None.
    """
    log = logger if logger is not None else _logger
    cursor = conn.cursor()

    try:
        cursor.execute(f"EXEC sp_rename '{table}.MRC', 'MRC_TEMP', 'COLUMN'")
        cursor.execute(f"EXEC sp_rename '{table}.REQUIREMENTS_STATEMENT', 'MRC', 'COLUMN'")
        cursor.execute(f"EXEC sp_rename '{table}.MRC_TEMP', 'REQUIREMENTS_STATEMENT', 'COLUMN'")
        conn.commit()
        log.info("Swapped MRC <-> REQUIREMENTS_STATEMENT columns on %s", table)
    except Exception as exc:
        conn.rollback()
        log.error("Column swap failed on %s: %s", table, exc)
        raise
