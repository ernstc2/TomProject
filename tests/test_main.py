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
    import pandas as pd

    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        # Override log_dir to use our tmp dir
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        # Provide csv_path now required by main()
        if "paths" not in cfg:
            cfg["paths"] = {}
        cfg["paths"]["csv_path"] = "dummy.csv"
        return cfg

    mock_conn = _MockConn()

    monkeypatch.setattr(importer, "load_config", patched_load_config)
    monkeypatch.setattr(importer, "extract_data", lambda url, work_dir, logger=None: "dummy.csv")
    monkeypatch.setattr(
        importer,
        "load_csv",
        lambda path, logger=None: pd.DataFrame([
            {"NIIN": "T01", "MRC": "A",
             "REQUIREMENTS_STATEMENT": "Req", "CLEAR_TEXT_REPLY": "Reply"},
        ]),
    )
    monkeypatch.setattr(importer, "get_connection", lambda cfg: mock_conn)
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer,
        "upsert_bulk",
        lambda conn, table, rows, logger: {"inserted": 1, "updated": 0},
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


# ---------------------------------------------------------------------------
# Pipeline wiring tests (02-02): main() must call load_csv and upsert_bulk
# ---------------------------------------------------------------------------

import textwrap


def _make_csv(tmp_path, content, filename="test.csv"):
    """Write *content* to tmp_path/filename and return the path."""
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def _patch_config_with_csv(tmp_config, tmp_log_dir, csv_path):
    """Return a load_config replacement that points to csv_path via config."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        import configparser
        cfg = real_load_config(str(tmp_config))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        if "paths" not in cfg:
            cfg["paths"] = {}
        cfg["paths"]["csv_path"] = str(csv_path)
        return cfg

    return patched_load_config


_SAMPLE_CSV = (
    "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
    "000000042,A,Req one,Reply one\n"
    "000000043,B,Req two,Reply two\n"
)


def test_main_calls_load_csv(tmp_path, tmp_config, tmp_log_dir, monkeypatch):
    """main() must call transform.load_csv with the path returned by extract_data."""
    load_csv_calls = []

    import pandas as pd

    def fake_load_csv(path, logger=None):
        load_csv_calls.append(path)
        return pd.DataFrame([
            {"NIIN": "000000042", "MRC": "A",
             "REQUIREMENTS_STATEMENT": "Req one", "CLEAR_TEXT_REPLY": "Reply one"},
        ])

    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    monkeypatch.setattr(importer, "load_config", patched_load_config)
    monkeypatch.setattr(importer, "extract_data", lambda url, work_dir, logger=None: "/mock/extract.csv")
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)
    monkeypatch.setattr(
        importer, "upsert_bulk",
        lambda conn, table, rows, logger: {"inserted": 1, "updated": 0},
    )

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert len(load_csv_calls) == 1
    assert load_csv_calls[0] == "/mock/extract.csv"


def test_main_passes_df_to_upsert(tmp_path, tmp_config, tmp_log_dir, monkeypatch):
    """main() must convert the DataFrame to rows and pass them to upsert_bulk."""
    import pandas as pd

    expected_rows = [
        {"NIIN": "000000042", "MRC": "A",
         "REQUIREMENTS_STATEMENT": "Req one", "CLEAR_TEXT_REPLY": "Reply one"},
        {"NIIN": "000000043", "MRC": "B",
         "REQUIREMENTS_STATEMENT": "Req two", "CLEAR_TEXT_REPLY": "Reply two"},
    ]
    captured = {}

    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    monkeypatch.setattr(importer, "load_config", patched_load_config)
    monkeypatch.setattr(importer, "extract_data", lambda url, work_dir, logger=None: "/mock/extract.csv")
    monkeypatch.setattr(
        importer, "load_csv",
        lambda path, logger=None: pd.DataFrame(expected_rows),
    )
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "ensure_table", lambda conn, table: None)

    def capturing_upsert(conn, table, rows, logger):
        captured["rows"] = rows
        return {"inserted": len(rows), "updated": 0}

    monkeypatch.setattr(importer, "upsert_bulk", capturing_upsert)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert "rows" in captured
    assert captured["rows"] == expected_rows
