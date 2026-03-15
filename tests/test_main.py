"""Tests for main() entry point exit codes — OP-02."""
import pytest

import importer


def test_main_exits_0_on_success(tmp_config, tmp_log_dir, monkeypatch):
    """main() exits with code 0 when config is valid."""
    # Point load_config at our tmp config and setup_logger at tmp log dir
    import configparser

    real_load_config = importer.load_config

    def patched_load_config(path="config.ini"):
        cfg = real_load_config(str(tmp_config))
        # Override log_dir to use our tmp dir
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["log_dir"] = str(tmp_log_dir)
        return cfg

    monkeypatch.setattr(importer, "load_config", patched_load_config)

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
