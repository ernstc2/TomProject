"""Integration tests for db.py upsert logic — Plan 02.

These tests require a live SQL Server connection via config.ini.
Run with: pytest tests/test_upsert.py -v -m integration
"""
import os
import pytest

from db import get_connection, ensure_table, upsert_batch


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
