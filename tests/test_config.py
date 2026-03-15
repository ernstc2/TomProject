"""Tests for config loading — OP-03 and OP-05."""
import pytest

from importer import load_config


def test_load_config_reads_ini(tmp_config):
    """load_config returns a ConfigParser with all expected database keys."""
    cfg = load_config(str(tmp_config))
    assert "database" in cfg
    for key in ("server", "database", "username", "password", "table"):
        assert key in cfg["database"], f"Missing key: {key}"


def test_no_hardcoded_credentials():
    """importer.py must not contain hardcoded credentials or literal server names."""
    with open("importer.py", "r", encoding="utf-8") as f:
        source = f.read()
    # Must not contain the live database name as a bare string value
    assert '= "DN_Live"' not in source
    assert "= 'DN_Live'" not in source
    # Must not contain a hardcoded password value
    assert '= "your_password"' not in source
    assert "= 'your_password'" not in source
    # Must not contain a hardcoded server name value
    assert '= "YOUR_SERVER_NAME"' not in source
    assert "= 'YOUR_SERVER_NAME'" not in source


def test_table_name_from_config(tmp_config):
    """Config table name is externalized and readable."""
    cfg = load_config(str(tmp_config))
    assert cfg["database"]["table"] == "V_CHARACTERISTICS_TESTING"


def test_load_config_missing_file():
    """load_config raises FileNotFoundError for a nonexistent path."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent_12345.ini")
