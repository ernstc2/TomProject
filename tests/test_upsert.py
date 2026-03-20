"""Integration tests for db.py upsert logic — Plan 02.

These tests require a live SQL Server connection via config.ini.
Run with: pytest tests/test_upsert.py -v -m integration

Unit tests for dynamic load_swap columns do NOT require a live DB.
"""
import os
import unittest.mock
import pytest

from db import get_connection, ensure_table, upsert_batch, load_swap, swap_mrc_columns


TABLE = "V_CHARACTERISTICS_TESTING"


@pytest.mark.integration
def test_insert_new_row(db_conn, clean_table, db_config):
    """Inserting a new row results in 1 inserted, 0 updated; row exists in table."""
    conn = db_conn
    rows = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "Test requirement",
            "CLEAR_TEXT_REPLY": "Test reply",
        }
    ]
    result = upsert_batch(conn, TABLE, rows, logger=None)
    assert result["inserted"] == 1
    assert result["updated"] == 0

    # Confirm the row exists
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT NIIN, MRC, REQUIREMENTS_STATEMENT FROM {TABLE} WHERE NIIN = ? AND MRC = ?",
        ("TEST001", "A"),
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == "TEST001"
    assert row[1] == "A"
    assert row[2] == "Test requirement"


@pytest.mark.integration
def test_idempotent_rerun(db_conn, clean_table, db_config):
    """Re-inserting the same row results in 0 inserted, 1 updated; data unchanged."""
    conn = db_conn
    rows = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "Test requirement",
            "CLEAR_TEXT_REPLY": "Test reply",
        }
    ]
    # First insert
    result1 = upsert_batch(conn, TABLE, rows, logger=None)
    assert result1["inserted"] == 1
    assert result1["updated"] == 0

    # Second insert (same data)
    result2 = upsert_batch(conn, TABLE, rows, logger=None)
    assert result2["inserted"] == 0
    assert result2["updated"] == 1

    # Data is unchanged
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT REQUIREMENTS_STATEMENT FROM {TABLE} WHERE NIIN = ? AND MRC = ?",
        ("TEST001", "A"),
    )
    row = cursor.fetchone()
    assert row[0] == "Test requirement"


@pytest.mark.integration
def test_update_changed_row(db_conn, clean_table, db_config):
    """Upserting changed data updates the existing row with new values."""
    conn = db_conn
    rows_old = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "old value",
            "CLEAR_TEXT_REPLY": "old reply",
        }
    ]
    rows_new = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "new value",
            "CLEAR_TEXT_REPLY": "new reply",
        }
    ]

    upsert_batch(conn, TABLE, rows_old, logger=None)
    result = upsert_batch(conn, TABLE, rows_new, logger=None)

    assert result["updated"] == 1
    assert result["inserted"] == 0

    cursor = conn.cursor()
    cursor.execute(
        f"SELECT REQUIREMENTS_STATEMENT, CLEAR_TEXT_REPLY FROM {TABLE} WHERE NIIN = ? AND MRC = ?",
        ("TEST001", "A"),
    )
    row = cursor.fetchone()
    assert row[0] == "new value"
    assert row[1] == "new reply"


def test_no_merge_statement():
    """db.py must not contain a SQL MERGE statement (outside comments/strings)."""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db.py")
    with open(db_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    import re
    # Pattern: MERGE followed by a table reference — i.e. MERGE INTO <word> or MERGE <word>
    # This is the SQL statement form. We skip lines that are pure comments (# ...) or
    # lines where MERGE only appears inside a string fragment like "never MERGE" or "not MERGE".
    # We look specifically for the SQL DML pattern: MERGE [INTO] <identifier>
    sql_merge_pattern = re.compile(r"\bMERGE\s+(?:INTO\s+)\w+|\bMERGE\s+\w+\s+USING\b", re.IGNORECASE)
    violations = []
    for lineno, line in enumerate(lines, start=1):
        stripped = line.strip()
        # Skip pure comment lines
        if stripped.startswith("#"):
            continue
        if sql_merge_pattern.search(line):
            violations.append(f"  Line {lineno}: {line.rstrip()}")

    assert not violations, (
        "db.py contains a SQL MERGE statement — use UPDATE+INSERT pattern instead (LD-02):\n"
        + "\n".join(violations)
    )


@pytest.mark.integration
def test_long_string_not_truncated(db_conn, clean_table, db_config):
    """A 5000-character REQUIREMENTS_STATEMENT round-trips without truncation (LD-03)."""
    conn = db_conn
    long_value = "X" * 5000
    rows = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": long_value,
            "CLEAR_TEXT_REPLY": "short reply",
        }
    ]
    result = upsert_batch(conn, TABLE, rows, logger=None)
    assert result["inserted"] == 1

    cursor = conn.cursor()
    cursor.execute(
        f"SELECT REQUIREMENTS_STATEMENT FROM {TABLE} WHERE NIIN = ? AND MRC = ?",
        ("TEST001", "A"),
    )
    row = cursor.fetchone()
    assert row is not None
    assert len(row[0]) == 5000, (
        f"varchar(max) content was truncated: expected 5000 chars, got {len(row[0])}"
    )


@pytest.mark.integration
def test_rollback_on_failure(db_conn, clean_table, db_config):
    """A failure mid-batch rolls back the entire batch; table has 0 rows."""
    conn = db_conn
    # First 2 rows are valid; 3rd has None for MRC (NOT NULL) to force a DB error
    rows = [
        {
            "NIIN": "TEST001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "req 1",
            "CLEAR_TEXT_REPLY": "reply 1",
        },
        {
            "NIIN": "TEST002",
            "MRC": "B",
            "REQUIREMENTS_STATEMENT": "req 2",
            "CLEAR_TEXT_REPLY": "reply 2",
        },
        {
            "NIIN": "TEST003",
            "MRC": None,  # NOT NULL column — should cause DB error
            "REQUIREMENTS_STATEMENT": "req 3",
            "CLEAR_TEXT_REPLY": "reply 3",
        },
    ]

    with pytest.raises(Exception):
        upsert_batch(conn, TABLE, rows, logger=None)

    # After rollback, the table must be empty
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
    count = cursor.fetchone()[0]
    assert count == 0, (
        f"Rollback failed: expected 0 rows after failure, found {count}"
    )


@pytest.mark.integration
def test_swap_mrc_columns(db_conn, clean_table, db_config):
    """swap_mrc_columns renames MRC <-> REQUIREMENTS_STATEMENT on the live table."""
    conn = db_conn

    # Load a row so the table exists with the standard schema
    rows = [
        {
            "NIIN": "TEST001",
            "MRC": "mrc_value",
            "REQUIREMENTS_STATEMENT": "req_value",
            "CLEAR_TEXT_REPLY": "reply",
        }
    ]
    load_swap(conn, TABLE, rows, logger=None)

    # Swap columns
    swap_mrc_columns(conn, TABLE, logger=None)

    # After swap: what was the MRC column is now named REQUIREMENTS_STATEMENT
    # and vice versa. Query by the NEW column names.
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT MRC, REQUIREMENTS_STATEMENT FROM {TABLE} WHERE NIIN = 'TEST001'"
    )
    row = cursor.fetchone()
    assert row is not None
    # The data that was in MRC ("mrc_value") should now be under REQUIREMENTS_STATEMENT
    assert row[0] == "req_value", (
        f"Expected MRC column to contain 'req_value' after swap, got '{row[0]}'"
    )
    assert row[1] == "mrc_value", (
        f"Expected REQUIREMENTS_STATEMENT column to contain 'mrc_value' after swap, got '{row[1]}'"
    )

    # Swap back so clean_table teardown can still find the columns
    swap_mrc_columns(conn, TABLE, logger=None)


# ---------------------------------------------------------------------------
# Unit tests for dynamic columns in load_swap (LDM-03)
# ---------------------------------------------------------------------------

class _FakeCursor:
    """Mock cursor that captures executed SQL and params for inspection."""

    def __init__(self):
        self.executed = []       # list of (sql, params) tuples
        self.fast_executemany = False
        self._fetchone_result = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params_list):
        self.executed.append((sql, params_list))

    def fetchone(self):
        return self._fetchone_result

    def setinputsizes(self, sizes):
        pass


class _FakeConn:
    """Mock connection that returns a _FakeCursor and tracks commit/rollback."""

    def __init__(self, table_exists=False):
        self._cursor = _FakeCursor()
        self._table_exists = table_exists
        self.committed = 0
        self.rolled_back = 0

    def cursor(self):
        # Return same cursor so we can inspect all calls
        return self._cursor

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


def _get_executed_sqls(conn):
    """Return list of SQL strings from all cursor.execute() calls."""
    return [sql for sql, _params in conn._cursor.executed]


def test_load_swap_dynamic_columns_creates_table():
    """load_swap with explicit columns builds CREATE TABLE with those columns."""
    conn = _FakeConn(table_exists=False)
    # Patch fetchone to return None (table does not exist) for INFORMATION_SCHEMA check
    conn._cursor._fetchone_result = None

    rows = [{"COL_A": "val1", "COL_B": "val2", "COL_C": "val3"}]
    columns = ["COL_A", "COL_B", "COL_C"]

    load_swap(conn, "TEST_TABLE", rows, logger=None, columns=columns)

    sqls = _get_executed_sqls(conn)
    create_sql = next((s for s in sqls if "CREATE TABLE" in s and "_NEW" in s), None)
    assert create_sql is not None, "No CREATE TABLE found for _NEW table"
    assert "[COL_A]" in create_sql
    assert "[COL_B]" in create_sql
    assert "[COL_C]" in create_sql
    # Must NOT contain hardcoded V_CHARACTERISTICS columns
    assert "NIIN" not in create_sql
    assert "MRC" not in create_sql
    assert "REQUIREMENTS_STATEMENT" not in create_sql


def test_load_swap_dynamic_columns_inserts_correct_values():
    """load_swap with columns extracts row values in column order for INSERT."""
    conn = _FakeConn(table_exists=False)
    conn._cursor._fetchone_result = None

    rows = [{"COL_A": "val1", "COL_B": "val2"}]
    columns = ["COL_A", "COL_B"]

    load_swap(conn, "TEST_TABLE", rows, logger=None, columns=columns)

    # Find the executemany call
    executemany_calls = [
        (sql, params) for sql, params in conn._cursor.executed
        if isinstance(params, list)
    ]
    assert len(executemany_calls) >= 1, "No executemany call found"
    insert_sql, params_list = executemany_calls[0]
    assert "INSERT INTO" in insert_sql
    assert "[COL_A]" in insert_sql
    assert "[COL_B]" in insert_sql
    # Params should be a list of tuples with values in column order
    assert params_list == [("val1", "val2")]


def test_load_swap_columns_none_infers_from_rows():
    """load_swap with columns=None infers column names from first row keys."""
    conn = _FakeConn(table_exists=False)
    conn._cursor._fetchone_result = None

    rows = [{"ALPHA": "a", "BETA": "b"}]

    # columns=None — should infer from rows[0].keys()
    load_swap(conn, "TEST_TABLE", rows, logger=None, columns=None)

    sqls = _get_executed_sqls(conn)
    create_sql = next((s for s in sqls if "CREATE TABLE" in s and "_NEW" in s), None)
    assert create_sql is not None
    assert "[ALPHA]" in create_sql
    assert "[BETA]" in create_sql


def test_load_swap_backwards_compatible():
    """load_swap called without columns param still works with V_CHARACTERISTICS rows."""
    conn = _FakeConn(table_exists=False)
    conn._cursor._fetchone_result = None

    rows = [
        {
            "NIIN": "001",
            "MRC": "A",
            "REQUIREMENTS_STATEMENT": "Req",
            "CLEAR_TEXT_REPLY": "Reply",
        }
    ]

    # Call without columns keyword (old call pattern)
    result = load_swap(conn, "V_CHARACTERISTICS_TESTING", rows, logger=None)

    assert result == {"loaded": 1}
    sqls = _get_executed_sqls(conn)
    create_sql = next((s for s in sqls if "CREATE TABLE" in s and "_NEW" in s), None)
    assert create_sql is not None
    # Should have all 4 columns inferred from row keys
    assert "[NIIN]" in create_sql
    assert "[MRC]" in create_sql
