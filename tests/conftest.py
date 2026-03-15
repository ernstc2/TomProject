"""Shared pytest fixtures for PubLog importer tests."""
import configparser
import pytest


@pytest.fixture
def tmp_config(tmp_path):
    """Create a temporary config.ini with valid test values. Returns the path."""
    config = configparser.ConfigParser()
    config["database"] = {
        "server": "TEST_SERVER",
        "database": "TEST_DB",
        "username": "test_user",
        "password": "test_pass",
        "table": "V_CHARACTERISTICS_TESTING",
        "encrypt": "yes",
        "trust_server_certificate": "yes",
    }
    config["logging"] = {
        "log_dir": str(tmp_path / "logs"),
        "max_bytes": "10485760",
        "backup_count": "5",
    }
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as f:
        config.write(f)
    return config_path


@pytest.fixture
def tmp_log_dir(tmp_path):
    """Return a log directory path inside tmp_path (does not create it)."""
    return tmp_path / "logs"
