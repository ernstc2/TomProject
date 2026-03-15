"""Unit tests for transform.py — CSV loading, delimiter detection, quote handling,
NIIN leading-zero preservation, and column validation.

Covers requirements: TF-01 (quote handling), TF-03 (delimiter detection), TF-04 (column validation).

These tests are written FIRST (RED phase) and must fail until transform.py exists.
"""
import logging
import sys
import textwrap

import pytest

from transform import load_csv  # noqa: E402  (module does not exist yet)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_csv(tmp_path, content, filename="test.csv"):
    """Write *content* to tmp_path/filename and return the path."""
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


def _make_logger(caplog, level=logging.WARNING):
    """Return a stdlib logger that feeds into pytest's caplog fixture."""
    logger = logging.getLogger("test_transform")
    logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# TF-03  Delimiter detection
# ---------------------------------------------------------------------------

class TestDelimiterDetection:
    """load_csv() must auto-detect the delimiter and always return the 4 required columns."""

    COMMA_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        '000000042,A,"Requirement one","Reply one"\n'
        '000000043,B,"Requirement two","Reply two"\n'
    )

    PIPE_CSV = (
        "NIIN|MRC|REQUIREMENTS_STATEMENT|CLEAR_TEXT_REPLY\n"
        "000000042|A|Requirement one|Reply one\n"
        "000000043|B|Requirement two|Reply two\n"
    )

    # A header that looks like it could confuse the Sniffer (no delimiter ambiguity)
    SNIFFER_FAIL_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        "000000042,A,Req,Reply\n"
    )

    def test_comma_delimiter(self, tmp_path):
        """Comma-separated CSV returns a 4-column DataFrame."""
        path = _make_csv(tmp_path, self.COMMA_CSV)
        df = load_csv(path)
        assert list(df.columns) == [
            "NIIN", "MRC", "REQUIREMENTS_STATEMENT", "CLEAR_TEXT_REPLY"
        ]
        assert len(df) == 2

    def test_pipe_delimiter(self, tmp_path):
        """Pipe-separated CSV returns a 4-column DataFrame."""
        path = _make_csv(tmp_path, self.PIPE_CSV)
        df = load_csv(path)
        assert list(df.columns) == [
            "NIIN", "MRC", "REQUIREMENTS_STATEMENT", "CLEAR_TEXT_REPLY"
        ]
        assert len(df) == 2

    def test_sniffer_fallback(self, tmp_path, monkeypatch):
        """When csv.Sniffer raises an exception, delimiter falls back to comma."""
        import csv

        original_sniff = csv.Sniffer.sniff

        def always_fail(self, sample, delimiters=None):
            raise csv.Error("Sniffer could not determine delimiter")

        monkeypatch.setattr(csv.Sniffer, "sniff", always_fail)

        path = _make_csv(tmp_path, self.SNIFFER_FAIL_CSV)
        df = load_csv(path)
        # Should still load correctly via comma fallback
        assert "NIIN" in df.columns
        assert len(df) == 1


# ---------------------------------------------------------------------------
# TF-01  Quote handling
# ---------------------------------------------------------------------------

class TestQuoteHandling:
    """load_csv() must strip outer RFC 4180 double-quotes and resolve escaped quotes."""

    QUOTED_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        '"000000042","A","Requirement one","Reply one"\n'
        '"000000043","B","Requirement two","Reply two"\n'
    )

    EMBEDDED_QUOTE_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        '000000042,A,"He said ""hello""","ok"\n'
    )

    NIIN_ZEROS_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        '"000000042",A,Req,Reply\n'
        '"001",B,Req2,Reply2\n'
    )

    def test_quotes_stripped(self, tmp_path):
        """Outer double-quotes are removed from field values."""
        path = _make_csv(tmp_path, self.QUOTED_CSV)
        df = load_csv(path)
        # Values must NOT contain surrounding quote characters
        assert df.loc[0, "NIIN"] == "000000042"
        assert df.loc[0, "MRC"] == "A"
        assert df.loc[0, "REQUIREMENTS_STATEMENT"] == "Requirement one"

    def test_embedded_quotes(self, tmp_path):
        """RFC 4180 double-double-quote escape resolves to a single quote character."""
        path = _make_csv(tmp_path, self.EMBEDDED_QUOTE_CSV)
        df = load_csv(path)
        assert df.loc[0, "REQUIREMENTS_STATEMENT"] == 'He said "hello"'

    def test_niin_leading_zeros(self, tmp_path):
        """NIIN values are preserved as strings - leading zeros are NOT dropped."""
        path = _make_csv(tmp_path, self.NIIN_ZEROS_CSV)
        df = load_csv(path)
        assert df.loc[0, "NIIN"] == "000000042"
        assert df.loc[1, "NIIN"] == "001"
        # Confirm the dtype is object (string), not numeric
        assert df["NIIN"].dtype == object


# ---------------------------------------------------------------------------
# TF-04  Column validation
# ---------------------------------------------------------------------------

class TestColumnValidation:
    """load_csv() must exit(1) if required columns are missing."""

    ALL_COLUMNS_CSV = (
        "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        "000000042,A,Req,Reply\n"
    )

    MISSING_NIIN_CSV = (
        "MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
        "A,Req,Reply\n"
    )

    MISSING_TWO_CSV = (
        "NIIN,REQUIREMENTS_STATEMENT\n"
        "000000042,Req\n"
    )

    def test_all_columns_present(self, tmp_path):
        """CSV with all 4 required columns loads without raising an exception."""
        path = _make_csv(tmp_path, self.ALL_COLUMNS_CSV)
        df = load_csv(path)
        assert df is not None
        assert len(df) == 1

    def test_missing_column_exits(self, tmp_path, caplog):
        """CSV missing NIIN column causes SystemExit(1) and logs the missing name."""
        path = _make_csv(tmp_path, self.MISSING_NIIN_CSV)
        logger = logging.getLogger("test_transform_exit")
        with caplog.at_level(logging.ERROR, logger="test_transform_exit"):
            with pytest.raises(SystemExit) as exc_info:
                load_csv(path, logger=logger)
        assert exc_info.value.code == 1
        assert "NIIN" in caplog.text

    def test_missing_multiple_columns(self, tmp_path, caplog):
        """CSV missing MRC and CLEAR_TEXT_REPLY causes SystemExit(1) with both names logged."""
        path = _make_csv(tmp_path, self.MISSING_TWO_CSV)
        logger = logging.getLogger("test_transform_multi_exit")
        with caplog.at_level(logging.ERROR, logger="test_transform_multi_exit"):
            with pytest.raises(SystemExit) as exc_info:
                load_csv(path, logger=logger)
        assert exc_info.value.code == 1
        assert "MRC" in caplog.text
        assert "CLEAR_TEXT_REPLY" in caplog.text

    def test_file_not_found(self, tmp_path):
        """Nonexistent file path raises FileNotFoundError (not SystemExit)."""
        missing_path = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            load_csv(missing_path)
