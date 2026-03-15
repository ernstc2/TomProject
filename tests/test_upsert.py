"""Stub tests for Plan 02 — upsert logic (requires DB connection)."""
import pytest


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_insert_new_row():
    pass


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_idempotent_rerun():
    pass


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_update_changed_row():
    pass


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_no_merge_statement():
    pass


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_long_string_not_truncated():
    pass


@pytest.mark.skip(reason="Plan 02 — DB connection required")
def test_rollback_on_failure():
    pass
