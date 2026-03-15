"""Tests for logging setup — OP-01."""
import logging
from logging.handlers import RotatingFileHandler
import pytest

from importer import setup_logger


def test_setup_logger_creates_log_file(tmp_log_dir):
    """setup_logger creates the log directory and the log file inside it."""
    logger_name = f"publog_importer_{tmp_log_dir.name}"
    logger = setup_logger(str(tmp_log_dir), logger_name=logger_name)
    try:
        log_file = tmp_log_dir / "publog_importer.log"
        assert tmp_log_dir.exists(), "Log directory was not created"
        assert log_file.exists(), "Log file was not created"
    finally:
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)


def test_log_file_has_rotating_handler(tmp_log_dir):
    """Logger returned by setup_logger has at least one RotatingFileHandler."""
    logger_name = f"publog_importer_rot_{tmp_log_dir.name}"
    logger = setup_logger(str(tmp_log_dir), logger_name=logger_name)
    try:
        rotating_handlers = [
            h for h in logger.handlers if isinstance(h, RotatingFileHandler)
        ]
        assert len(rotating_handlers) >= 1, "No RotatingFileHandler found on logger"
    finally:
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)


def test_log_message_written(tmp_log_dir):
    """Logger writes messages to the log file."""
    logger_name = f"publog_importer_msg_{tmp_log_dir.name}"
    logger = setup_logger(str(tmp_log_dir), logger_name=logger_name)
    try:
        logger.info("test message from pytest")
        for h in logger.handlers:
            h.flush()
        log_file = tmp_log_dir / "publog_importer.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "test message from pytest" in contents
    finally:
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)
