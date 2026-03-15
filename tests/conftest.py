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
    config["paths"] = {
        "download_url": "https://test.example.com/characteristics.zip",
        "work_dir": str(tmp_path / "work"),
        "csv_path": str(tmp_path / "work" / "V_CHARACTERISTICS.CSV"),
    }
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as f:
        config.write(f)
    return config_path


@pytest.fixture
def tmp_log_dir(tmp_path):
    """Return a log directory path inside tmp_path (does not create it)."""
    return tmp_path / "logs"


# ---------------------------------------------------------------------------
# Integration fixtures — require a real config.ini with SQL Server credentials
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def db_config():
    """Load config.ini from project root. Skip all integration tests if missing."""
    import os
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "config.ini"
    )
    if not os.path.exists(config_path):
        pytest.skip("config.ini required for integration tests — copy config.ini.example and fill in credentials")

    cfg = configparser.ConfigParser()
    read = cfg.read(config_path)
    if not read:
        pytest.skip("config.ini could not be read — check file permissions")
    return cfg


@pytest.fixture(scope="function")
def db_conn(db_config):
    """Open a DB connection for one test, close it on teardown."""
    from db import get_connection
    conn = get_connection(db_config)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def clean_table(db_conn, db_config):
    """Delete all rows from the testing table before and after each test."""
    table = db_config["database"]["table"]
    cursor = db_conn.cursor()
    cursor.execute(f"DELETE FROM {table}")
    db_conn.commit()
    yield
    cursor.execute(f"DELETE FROM {table}")
    db_conn.commit()
