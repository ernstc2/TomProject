"""Tests for config loading — OP-03 and OP-05, plus CFG-01 through CFG-04."""
import pytest

from importer import load_config, get_table_sections, parse_list


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


# ---------------------------------------------------------------------------
# get_table_sections tests (CFG-01)
# ---------------------------------------------------------------------------

def test_get_table_sections_returns_only_table_names(tmp_config_multitable):
    """get_table_sections returns only table section names, not reserved sections."""
    cfg = load_config(str(tmp_config_multitable))
    sections = get_table_sections(cfg)
    assert "V_CHARACTERISTICS" in sections
    assert "V_MANAGEMENT" in sections
    assert "database" not in sections
    assert "logging" not in sections
    assert "paths" not in sections


def test_get_table_sections_empty_config(tmp_config):
    """get_table_sections returns [] when config has only reserved sections."""
    cfg = load_config(str(tmp_config))
    sections = get_table_sections(cfg)
    assert sections == []


# ---------------------------------------------------------------------------
# parse_list tests
# ---------------------------------------------------------------------------

def test_parse_list_comma_separated():
    """parse_list splits a comma-separated string into a list."""
    result = parse_list("NIIN,MRC,REQUIREMENTS_STATEMENT")
    assert result == ["NIIN", "MRC", "REQUIREMENTS_STATEMENT"]


def test_parse_list_with_spaces():
    """parse_list strips whitespace from each item."""
    result = parse_list("NIIN , MRC , CLEAR_TEXT_REPLY")
    assert result == ["NIIN", "MRC", "CLEAR_TEXT_REPLY"]


def test_parse_list_empty():
    """parse_list returns [] for an empty string."""
    assert parse_list("") == []


def test_parse_list_none():
    """parse_list returns [] for None input."""
    assert parse_list(None) == []


# ---------------------------------------------------------------------------
# Config section key tests (CFG-01 through CFG-04)
# ---------------------------------------------------------------------------

def test_config_v_characteristics_has_required_keys(tmp_config_multitable):
    """V_CHARACTERISTICS section has all required keys."""
    cfg = load_config(str(tmp_config_multitable))
    section = cfg["V_CHARACTERISTICS"]
    for key in ("download_url", "csv_name", "target_table", "columns",
                "key_columns", "date_columns", "date_format", "drop_columns"):
        assert key in section, f"Missing key in V_CHARACTERISTICS: {key}"


def test_config_v_management_has_required_keys(tmp_config_multitable):
    """V_MANAGEMENT section has all required keys and correct csv_name."""
    cfg = load_config(str(tmp_config_multitable))
    section = cfg["V_MANAGEMENT"]
    for key in ("download_url", "csv_name", "target_table", "columns",
                "key_columns", "date_columns", "date_format", "drop_columns"):
        assert key in section, f"Missing key in V_MANAGEMENT: {key}"
    assert section["csv_name"] == "V_FLIS_MANAGEMENT.CSV"


def test_config_v_cage_drop_columns(tmp_config_multitable):
    """V_CAGE_STATUS_AND_TYPE drop_columns includes PARENT_CAGE."""
    cfg = load_config(str(tmp_config_multitable))
    section = cfg["V_CAGE_STATUS_AND_TYPE"]
    drop_cols = parse_list(section["drop_columns"])
    assert "PARENT_CAGE" in drop_cols


def test_config_v_management_drop_columns(tmp_config_multitable):
    """V_MANAGEMENT drop_columns includes ROW_OBS_DT."""
    cfg = load_config(str(tmp_config_multitable))
    section = cfg["V_MANAGEMENT"]
    drop_cols = parse_list(section["drop_columns"])
    assert "ROW_OBS_DT" in drop_cols


def test_config_v_moe_rule_date_format(tmp_config_multitable):
    """V_MOE_RULE date_format is dd-MMM-yy."""
    cfg = load_config(str(tmp_config_multitable))
    assert cfg["V_MOE_RULE"]["date_format"] == "dd-MMM-yy"


def test_config_v_management_date_format(tmp_config_multitable):
    """V_MANAGEMENT date_format is dd-MMM-yyyy."""
    cfg = load_config(str(tmp_config_multitable))
    assert cfg["V_MANAGEMENT"]["date_format"] == "dd-MMM-yyyy"


def test_production_target_tables_have_no_testing_suffix():
    """CUT-01: All target_table values in config.ini.example must be production names (no _TESTING suffix)."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read("config.ini.example")
    table_sections = [s for s in cfg.sections()
                      if s not in ("database", "logging", "paths")]
    assert len(table_sections) >= 4, f"Expected at least 4 table sections, got {table_sections}"
    for section in table_sections:
        target = cfg[section]["target_table"]
        assert "_TESTING" not in target, (
            f"[{section}] target_table = {target} still has _TESTING suffix"
        )
