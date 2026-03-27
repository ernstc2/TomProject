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


@pytest.fixture
def tmp_config_multitable(tmp_path):
    """Create a temporary config.ini with database, logging, paths, and two table sections.

    Includes [V_CHARACTERISTICS], [V_CAGE_STATUS_AND_TYPE], [V_MANAGEMENT], and
    [V_MOE_RULE] sections with all required keys. Returns the path to the config file.
    """
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
        "download_url": "https://test.example.com/",
        "work_dir": str(tmp_path / "work"),
    }
    config["V_CHARACTERISTICS"] = {
        "download_url": "https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/",
        "zip_name": "Characteristics.zip",
        "csv_name": "V_CHARACTERISTICS.CSV",
        "target_table": "V_CHARACTERISTICS",
        "columns": "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY",
        "key_columns": "NIIN,MRC",
        "date_columns": "CLEAR_TEXT_REPLY",
        "date_format": "dd-MMM-yy",
        "drop_columns": "",
    }
    config["V_CAGE_STATUS_AND_TYPE"] = {
        "download_url": "https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/",
        "zip_name": "CAGE.zip",
        "csv_name": "V_CAGE_STATUS_AND_TYPE.CSV",
        "target_table": "V_CAGE_STATUS_AND_TYPE_TESTING",
        "columns": "CAGE_CODE,DESIGNATOR,AFFIL_CODE,ADP,TYPE_OF_BUSINESS,WOMAN_OWNED,BUS_SIZE,PRIMARY_BUSINESS,TYPE,STATUS,CNGRSL_DSTRCT,CAO,ASSOC_CODE,RPLM_CODE,FAX,PHONE",
        "key_columns": "CAGE_CODE",
        "date_columns": "",
        "date_format": "",
        "drop_columns": "PARENT_CAGE",
    }
    config["V_MANAGEMENT"] = {
        "download_url": "https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/",
        "zip_name": "MANAGEMENT.zip",
        "csv_name": "V_FLIS_MANAGEMENT.CSV",
        "target_table": "V_MANAGEMENT_TESTING",
        "columns": "NIIN,REP_REC_CODE,REP_NET_PR,MOE,USC,SOSM,AAC,QUP,CIIC,SLC,UI_CONV_FAC,UI,SOS,EFFECTIVE_DATE,MGMT_CTL,UNIT_PRICE",
        "key_columns": "NIIN",
        "date_columns": "EFFECTIVE_DATE",
        "date_format": "dd-MMM-yyyy",
        "drop_columns": "ROW_OBS_DT",
    }
    config["V_MOE_RULE"] = {
        "download_url": "https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/",
        "zip_name": "MOE_RULE.zip",
        "csv_name": "V_MOE_RULE.CSV",
        "target_table": "V_MOE_RULE_TESTING",
        "columns": "NIIN,IMC,AMC,PICA_LOA,SICA_LOA,AAC,AMSC,NIMSC,IMCA,SICA,MOE_CD,PICA,DSOR,DT_ASGND,AUTH_COLLAB,FMR_MOE_RL,MOE_RL,SUPP_COLLAB",
        "key_columns": "NIIN",
        "date_columns": "DT_ASGND",
        "date_format": "dd-MMM-yy",
        "drop_columns": "ROW_OBS_DT",
    }
    config_path = tmp_path / "config.ini"
    with open(config_path, "w") as f:
        config.write(f)
    return config_path


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
