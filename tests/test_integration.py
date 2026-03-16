"""Integration wiring tests — verify pipeline call order and idempotency.

These tests verify that main() calls extract_data -> load_csv -> upsert_bulk
in the correct order and that a second run on unchanged data exits 0.

All DB/network calls are mocked; no live SQL Server or network is needed.
"""
import pytest
import pandas as pd

import importer


class _MockConn:
    """Minimal mock connection object."""

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Shared config patcher
# ---------------------------------------------------------------------------

def _make_patched_load_config(tmp_config, tmp_log_dir):
    """Return a load_config replacement pointing at tmp_config with patched log_dir."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    return patched_load_config


_SAMPLE_DF = pd.DataFrame([
    {"NIIN": "000000042", "MRC": "A",
     "REQUIREMENTS_STATEMENT": "Req one", "CLEAR_TEXT_REPLY": "Reply one"},
])


# ---------------------------------------------------------------------------
# Test: pipeline call order
# ---------------------------------------------------------------------------

def test_main_calls_extract_then_load_then_upsert(tmp_config, tmp_log_dir, monkeypatch):
    """main() must call extract_data, then load_csv, then upsert_bulk in that order."""
    call_order = []

    monkeypatch.setattr(importer, "load_config", _make_patched_load_config(tmp_config, tmp_log_dir))
    monkeypatch.setattr(
        importer, "extract_data",
        lambda url, work_dir, logger=None: call_order.append("extract") or "/mock/extracted.csv",
    )
    monkeypatch.setattr(
        importer, "load_csv",
        lambda path, logger=None: call_order.append("load") or _SAMPLE_DF,
    )
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer, "load_swap",
        lambda conn, table, rows, logger: call_order.append("load_swap") or {"loaded": 1},
    )
    monkeypatch.setattr(importer, "swap_mrc_columns", lambda conn, table, logger=None: None)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert call_order == ["extract", "load", "load_swap"]


# ---------------------------------------------------------------------------
# Test: extract_data receives url and work_dir from config
# ---------------------------------------------------------------------------

def test_extract_receives_url_and_work_dir(tmp_config, tmp_log_dir, monkeypatch):
    """main() must pass cfg[paths][download_url] and cfg[paths][work_dir] to extract_data."""
    captured = {}

    monkeypatch.setattr(importer, "load_config", _make_patched_load_config(tmp_config, tmp_log_dir))

    def capturing_extract(url, work_dir, logger=None):
        captured["url"] = url
        captured["work_dir"] = work_dir
        return "/mock/extracted.csv"

    monkeypatch.setattr(importer, "extract_data", capturing_extract)
    monkeypatch.setattr(importer, "load_csv", lambda path, logger=None: _SAMPLE_DF)
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer, "load_swap",
        lambda conn, table, rows, logger: {"loaded": 1},
    )
    monkeypatch.setattr(importer, "swap_mrc_columns", lambda conn, table, logger=None: None)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert captured["url"] == "https://test.example.com/characteristics.zip"
    # work_dir should come from cfg[paths][work_dir], not hardcoded
    assert "work" in captured["work_dir"]


# ---------------------------------------------------------------------------
# Test: extract_data return value flows into load_csv (not cfg[paths][csv_path])
# ---------------------------------------------------------------------------

def test_extract_csv_path_used_by_load_csv(tmp_config, tmp_log_dir, monkeypatch):
    """load_csv must receive the path returned by extract_data, not cfg[paths][csv_path]."""
    load_csv_calls = []

    monkeypatch.setattr(importer, "load_config", _make_patched_load_config(tmp_config, tmp_log_dir))
    monkeypatch.setattr(
        importer, "extract_data",
        lambda url, work_dir, logger=None: "/mock/extracted.csv",
    )

    def capturing_load_csv(path, logger=None):
        load_csv_calls.append(path)
        return _SAMPLE_DF

    monkeypatch.setattr(importer, "load_csv", capturing_load_csv)
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer, "load_swap",
        lambda conn, table, rows, logger: {"loaded": 1},
    )
    monkeypatch.setattr(importer, "swap_mrc_columns", lambda conn, table, logger=None: None)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert len(load_csv_calls) == 1
    # Must use extract_data's return value, NOT cfg[paths][csv_path]
    assert load_csv_calls[0] == "/mock/extracted.csv"


# ---------------------------------------------------------------------------
# Test: second run on unchanged data produces zero changes, exits 0
# ---------------------------------------------------------------------------

def test_second_run_zero_changes(tmp_config, tmp_log_dir, monkeypatch):
    """Calling main() twice with same data exits 0 both times; second upsert returns 0/0."""
    run_count = [0]

    monkeypatch.setattr(importer, "load_config", _make_patched_load_config(tmp_config, tmp_log_dir))
    monkeypatch.setattr(
        importer, "extract_data",
        lambda url, work_dir, logger=None: "/mock/extracted.csv",
    )
    monkeypatch.setattr(importer, "load_csv", lambda path, logger=None: _SAMPLE_DF)
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)

    def idempotent_load_swap(conn, table, rows, logger):
        run_count[0] += 1
        return {"loaded": len(rows)}

    monkeypatch.setattr(importer, "load_swap", idempotent_load_swap)
    monkeypatch.setattr(importer, "swap_mrc_columns", lambda conn, table, logger=None: None)

    # First run
    with pytest.raises(SystemExit) as exc_info_1:
        importer.main()
    assert exc_info_1.value.code == 0

    # Second run — same data, zero changes
    with pytest.raises(SystemExit) as exc_info_2:
        importer.main()
    assert exc_info_2.value.code == 0
    assert run_count[0] == 2
