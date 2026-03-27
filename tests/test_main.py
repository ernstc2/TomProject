"""Tests for main() entry point exit codes — OP-02.

DB functions are mocked so these are pure unit tests (no live SQL Server needed).
"""
import logging
import pytest

import importer


class _MockConn:
    """Minimal mock connection object."""

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir):
    """Return a load_config replacement pointing at the multitable fixture config."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config_multitable))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    return patched_load_config


def _make_singletable_patched_config(tmp_config, tmp_log_dir):
    """Return a load_config replacement pointing at the singletable fixture config."""
    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    return patched_load_config


# ---------------------------------------------------------------------------
# parse_args tests (PIP-01, PIP-02)
# ---------------------------------------------------------------------------

def test_parse_args_no_args(monkeypatch):
    """parse_args() with no --table flag returns args.table == None."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    args = importer.parse_args()
    assert args.table is None


def test_parse_args_with_table(monkeypatch):
    """parse_args() with --table V_MANAGEMENT returns args.table == 'V_MANAGEMENT'."""
    monkeypatch.setattr("sys.argv", ["importer.py", "--table", "V_MANAGEMENT"])
    args = importer.parse_args()
    assert args.table == "V_MANAGEMENT"


# ---------------------------------------------------------------------------
# Multi-table main() loop tests (PIP-01 through PIP-04)
# ---------------------------------------------------------------------------

def test_main_runs_all_tables(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """When no --table flag is given, main() calls run_table for all table sections."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    processed = []

    def fake_run_table(cfg, section, conn, logger):
        processed.append(section)

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", fake_run_table)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    # All 4 table sections should have been processed
    assert "V_CHARACTERISTICS" in processed
    assert "V_MANAGEMENT" in processed
    assert "V_CAGE_STATUS_AND_TYPE" in processed
    assert "V_MOE_RULE" in processed
    assert len(processed) == 4


def test_main_runs_only_specified_table(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """When --table V_MANAGEMENT is given, main() calls run_table only for V_MANAGEMENT."""
    monkeypatch.setattr("sys.argv", ["importer.py", "--table", "V_MANAGEMENT"])
    processed = []

    def fake_run_table(cfg, section, conn, logger):
        processed.append(section)

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", fake_run_table)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert processed == ["V_MANAGEMENT"]


def test_main_unknown_table_exits_1(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """When --table NONEXISTENT is given, main() exits with code 1."""
    monkeypatch.setattr("sys.argv", ["importer.py", "--table", "NONEXISTENT"])

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 1


def test_main_failure_isolation(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """A failure in one table does not abort others; main() exits with code 1."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    processed = []

    def fake_run_table(cfg, section, conn, logger):
        processed.append(section)
        if section == "V_MANAGEMENT":
            raise RuntimeError("Simulated failure")

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", fake_run_table)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 1
    # All tables should have been attempted (failure isolation)
    assert "V_CHARACTERISTICS" in processed
    assert "V_MANAGEMENT" in processed
    assert "V_CAGE_STATUS_AND_TYPE" in processed
    assert "V_MOE_RULE" in processed


def test_main_all_succeed_exits_0(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """When all tables succeed, main() exits with code 0."""
    monkeypatch.setattr("sys.argv", ["importer.py"])

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", lambda cfg, section, conn, logger: None)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0


def test_main_logs_table_name(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """Log output contains 'Processing table: V_CHARACTERISTICS' when that section runs."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    log_messages = []

    class CapturingHandler(logging.Handler):
        def emit(self, record):
            log_messages.append(self.format(record))

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", lambda cfg, section, conn, logger: None)

    # Capture log output by patching setup_logger to add a capturing handler
    real_setup_logger = importer.setup_logger

    def capturing_setup_logger(*args, **kwargs):
        logger = real_setup_logger(*args, **kwargs)
        handler = CapturingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        return logger

    monkeypatch.setattr(importer, "setup_logger", capturing_setup_logger)

    with pytest.raises(SystemExit):
        importer.main()

    assert any("Processing table: V_CHARACTERISTICS" in msg for msg in log_messages)


# ---------------------------------------------------------------------------
# Legacy single-table tests — updated for multi-table main()
# ---------------------------------------------------------------------------

def test_main_exits_0_on_success(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """main() exits with code 0 when config is valid and all run_table calls succeed."""
    monkeypatch.setattr("sys.argv", ["importer.py"])

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", lambda cfg, section, conn, logger: None)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()
    assert exc_info.value.code == 0


def test_main_exits_1_on_missing_config(monkeypatch):
    """main() exits with code 1 when config.ini does not exist."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
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


def test_main_exits_1_on_connection_failure(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """main() exits with code 1 when DB connection raises an exception."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
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
# Pipeline wiring tests — main() delegates to run_table
# ---------------------------------------------------------------------------

import textwrap


def _make_csv(tmp_path, content, filename="test.csv"):
    """Write *content* to tmp_path/filename and return the path."""
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def test_main_calls_run_table_for_each_section(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """main() delegates per-table work to run_table, not inline logic."""
    monkeypatch.setattr("sys.argv", ["importer.py"])
    run_table_calls = []

    def tracking_run_table(cfg, section, conn, logger):
        run_table_calls.append(section)

    monkeypatch.setattr(importer, "load_config", _make_multitable_patched_config(tmp_config_multitable, tmp_log_dir))
    monkeypatch.setattr(importer, "get_connection", lambda cfg: _MockConn())
    monkeypatch.setattr(importer, "run_table", tracking_run_table)

    with pytest.raises(SystemExit) as exc_info:
        importer.main()

    assert exc_info.value.code == 0
    assert len(run_table_calls) == 4


# ---------------------------------------------------------------------------
# run_table config wiring tests (07-02)
# ---------------------------------------------------------------------------

def test_run_table_passes_config_to_load_csv(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes config-driven columns, date_columns, date_format to load_csv."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        captured["required_columns"] = required_columns
        captured["date_columns"] = date_columns
        captured["date_format"] = date_format
        return pd.DataFrame({"NIIN": ["001"], "IMC": ["X"], "DT_ASGND": ["2002-02-20"], "MOE_RL": ["R1"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))
    conn = _MockConn()

    importer.run_table(cfg, "V_MOE_RULE", conn, logger)

    assert captured["required_columns"] == ["NIIN", "IMC", "AMC", "PICA_LOA", "SICA_LOA", "AAC", "AMSC", "NIMSC", "IMCA", "SICA", "MOE_CD", "PICA", "DSOR", "DT_ASGND", "AUTH_COLLAB", "FMR_MOE_RL", "MOE_RL", "SUPP_COLLAB"]
    assert captured["date_columns"] == ["DT_ASGND"]
    assert captured["date_format"] == "dd-MMM-yy"


def test_run_table_empty_date_config(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes None for date_columns/date_format when config values are empty."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        captured["date_columns"] = date_columns
        captured["date_format"] = date_format
        return pd.DataFrame({"CAGE_CODE": ["C1"], "STATUS": ["A"], "TYPE": ["T"], "ASSOC_CODE": ["AC"], "DESIGNATOR": ["X"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))

    importer.run_table(cfg, "V_CAGE_STATUS_AND_TYPE", _MockConn(), logger)

    assert captured["date_columns"] is None
    assert captured["date_format"] is None


def test_run_table_v_characteristics_backwards_compat(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes V_CHARACTERISTICS config to load_csv correctly."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        captured["required_columns"] = required_columns
        captured["date_columns"] = date_columns
        captured["date_format"] = date_format
        return pd.DataFrame({"NIIN": ["001"], "MRC": ["A"], "REQUIREMENTS_STATEMENT": ["R"], "CLEAR_TEXT_REPLY": ["C"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))

    importer.run_table(cfg, "V_CHARACTERISTICS", _MockConn(), logger)

    assert captured["required_columns"] == ["NIIN", "MRC", "REQUIREMENTS_STATEMENT", "CLEAR_TEXT_REPLY"]
    assert captured["date_columns"] == ["CLEAR_TEXT_REPLY"]
    assert captured["date_format"] == "dd-MMM-yy"


# ---------------------------------------------------------------------------
# run_table zip_name wiring tests (08-02)
# ---------------------------------------------------------------------------

def test_run_table_passes_zip_name_to_extract_data(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes zip_name and csv_name from config to extract_data (V_MOE_RULE)."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        captured["zip_name"] = zip_name
        captured["csv_name"] = csv_name
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        return pd.DataFrame({"NIIN": ["001"], "IMC": ["X"], "DT_ASGND": ["2002-02-20"], "MOE_RL": ["R1"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))

    importer.run_table(cfg, "V_MOE_RULE", _MockConn(), logger)

    assert captured["zip_name"] == "MOE_RULE.zip"
    assert captured["csv_name"] == "V_MOE_RULE.CSV"


def test_run_table_passes_zip_name_cage(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes zip_name and csv_name from config to extract_data (V_CAGE_STATUS_AND_TYPE)."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        captured["zip_name"] = zip_name
        captured["csv_name"] = csv_name
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        return pd.DataFrame({"CAGE_CODE": ["C1"], "STATUS": ["A"], "TYPE": ["T"], "ASSOC_CODE": ["AC"], "DESIGNATOR": ["X"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))

    importer.run_table(cfg, "V_CAGE_STATUS_AND_TYPE", _MockConn(), logger)

    assert captured["zip_name"] == "CAGE.zip"
    assert captured["csv_name"] == "V_CAGE_STATUS_AND_TYPE.CSV"


def test_run_table_passes_zip_name_management(tmp_config_multitable, tmp_log_dir, monkeypatch):
    """run_table passes zip_name and csv_name from config to extract_data (V_MANAGEMENT)."""
    import pandas as pd
    captured = {}

    def fake_extract_data(url, work_dir, logger, zip_name="Characteristics.zip", csv_name=None):
        captured["zip_name"] = zip_name
        captured["csv_name"] = csv_name
        return "fake.csv"

    def fake_load_csv(path, logger=None, required_columns=None, date_columns=None, date_format=None, numeric_columns=None):
        return pd.DataFrame({"NIIN": ["001"], "EFFECTIVE_DATE": ["2002-02-20"], "MGMT_CTL": ["D"]})

    def fake_load_swap(conn, target, rows, logger, columns=None):
        return {"loaded": 1}

    monkeypatch.setattr(importer, "extract_data", fake_extract_data)
    monkeypatch.setattr(importer, "load_csv", fake_load_csv)
    monkeypatch.setattr(importer, "load_swap", fake_load_swap)

    cfg = importer.load_config(str(tmp_config_multitable))
    cfg["logging"]["log_dir"] = str(tmp_log_dir)
    logger = importer.setup_logger(str(tmp_log_dir))

    importer.run_table(cfg, "V_MANAGEMENT", _MockConn(), logger)

    assert captured["zip_name"] == "MANAGEMENT.zip"
    assert captured["csv_name"] == "V_FLIS_MANAGEMENT.CSV"
