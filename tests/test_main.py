"""Tests for main() entry point exit codes — OP-02.

DB functions are mocked so these are pure unit tests (no live SQL Server needed).
"""
import pytest

import importer


class _MockConn:
    """Minimal mock connection object."""

    def close(self):
        pass


def test_main_exits_0_on_success(tmp_config, tmp_log_dir, monkeypatch):
    """main() exits with code 0 when config is valid and DB calls succeed."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        # Override log_dir to use our tmp dir
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    mock_conn = _MockConn()

    monkeypatch.setattr(importer, "load_config", patched_load_config)
    monkeypatch.setattr(importer, "get_connection", lambda cfg: mock_conn)
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer,
        "upsert_batch",
        lambda conn, table, rows, logger: {"inserted": 3, "updated": 0},
    )

    with pytest.raises(SystemExit) as exc_info:
        importer.main()
    assert exc_info.value.code == 0


def test_main_exits_1_on_missing_config(monkeypatch):
    """main() exits with code 1 when config.ini does not exist."""
    monkeypatch.setattr(
        importer,
        "load_config",
        lambda path="config.ini": (_ for _ in ()).throw(
            FileNotFoundError("config.ini not found")
        ),
    )
    with pytest.raises(SystemExit) as exc_info:
        importer.main()
    assert exc_info.value.code == 1


def test_main_exits_1_on_connection_failure(tmp_config, tmp_log_dir, monkeypatch):
    """main() exits with code 1 when DB connection raises an exception."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    monkeypatch.setattr(importer, "load_config", patched_load_config)
    monkeypatch.setattr(
        importer,
        "get_connection",
        lambda cfg: (_ for _ in ()).throw(
            ConnectionError("Cannot connect to SQL Server")
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        importer.main()
    assert exc_info.value.code == 1
