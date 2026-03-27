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


# ---------------------------------------------------------------------------
# TF-02  Date conversion (dd-MMM-yy -> YYYY-MM-DD, century pivot)
# ---------------------------------------------------------------------------

class TestDateConversion:
    """load_csv() must convert dd-MMM-yy dates in CLEAR_TEXT_REPLY to YYYY-MM-DD."""

    def _csv(self, reply_value, niin="000000042", mrc="A", req="Requirement"):
        """Build a single-row CSV string with the given CLEAR_TEXT_REPLY value."""
        return (
            "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
            f"{niin},{mrc},{req},{reply_value}\n"
        )

    def test_date_standalone(self, tmp_path):
        """A standalone dd-MMM-yy date is converted to YYYY-MM-DD."""
        path = _make_csv(tmp_path, self._csv("18-MAR-52"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"] == "1952-03-18"

    def test_date_embedded(self, tmp_path):
        """A date embedded in text is replaced in-place (surrounding text preserved)."""
        path = _make_csv(tmp_path, self._csv("PC2897 (20-FEB-02)"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"] == "PC2897 (2002-02-20)"

    def test_century_pivot_old(self, tmp_path):
        """yy=52 (> current 2-digit year 26) maps to 1952, not 2052."""
        path = _make_csv(tmp_path, self._csv("18-MAR-52"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"].startswith("1952-")

    def test_century_pivot_recent(self, tmp_path):
        """yy=02 (< current 2-digit year 26) maps to 2002."""
        path = _make_csv(tmp_path, self._csv("20-FEB-02"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"].startswith("2002-")

    def test_date_unparseable(self, tmp_path):
        """A date-like pattern with an invalid month abbreviation is left unchanged."""
        path = _make_csv(tmp_path, self._csv("99-XXX-00"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"] == "99-XXX-00"

    def test_date_other_columns_untouched(self, tmp_path):
        """Date patterns in NIIN, MRC, REQUIREMENTS_STATEMENT columns are NOT converted."""
        content = (
            "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
            "18-MAR-52,18-MAR-52,18-MAR-52,18-MAR-52\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(path)
        # CLEAR_TEXT_REPLY is converted
        assert df.loc[0, "CLEAR_TEXT_REPLY"] == "1952-03-18"
        # Other columns are NOT converted
        assert df.loc[0, "NIIN"] == "18-MAR-52"
        assert df.loc[0, "MRC"] == "18-MAR-52"
        assert df.loc[0, "REQUIREMENTS_STATEMENT"] == "18-MAR-52"

    def test_no_dates_no_crash(self, tmp_path):
        """CLEAR_TEXT_REPLY with no date patterns is returned unchanged."""
        path = _make_csv(tmp_path, self._csv("No dates here at all"))
        df = load_csv(path)
        assert df.loc[0, "CLEAR_TEXT_REPLY"] == "No dates here at all"


# ---------------------------------------------------------------------------
# TRN-01  Generalized column validation
# ---------------------------------------------------------------------------

class TestGeneralizedColumnValidation:
    """load_csv() must validate arbitrary required_columns from config, not hardcoded."""

    def test_custom_required_columns_valid(self, tmp_path):
        """CSV with columns matching required_columns returns a DataFrame with those columns."""
        content = (
            "NIIN,MOE_RULE,DT_ASGND,SOS\n"
            "000000042,RULE1,20-FEB-02,S1\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(path, required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"])
        assert list(df.columns) == ["NIIN", "MOE_RULE", "DT_ASGND", "SOS"]
        assert len(df) == 1

    def test_custom_required_columns_missing_warns_and_continues(self, tmp_path, caplog):
        """CSV missing a required_columns entry logs a warning and continues with available columns."""
        content = (
            "NIIN,MOE_RULE\n"
            "000000042,RULE1\n"
        )
        path = _make_csv(tmp_path, content)
        logger = logging.getLogger("test_custom_missing")
        with caplog.at_level(logging.WARNING, logger="test_custom_missing"):
            df = load_csv(path, logger=logger, required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"])
        assert "DT_ASGND" in caplog.text
        assert "SOS" in caplog.text
        # Only available columns should be in the result
        assert list(df.columns) == ["NIIN", "MOE_RULE"]
        assert len(df) == 1

    def test_default_required_columns_unchanged(self, tmp_path):
        """load_csv with no required_columns kwarg still validates the default 4 columns."""
        content = (
            "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n"
            "000000042,A,Requirement,Reply\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(path)
        assert "NIIN" in df.columns
        assert "MRC" in df.columns
        assert "REQUIREMENTS_STATEMENT" in df.columns
        assert "CLEAR_TEXT_REPLY" in df.columns


# ---------------------------------------------------------------------------
# TRN-02  MOE_RULE date conversion (dd-MMM-yy, 2-digit year with century pivot)
# ---------------------------------------------------------------------------

class TestMoeRuleDateConversion:
    """load_csv() must convert dd-MMM-yy dates on any specified date_columns."""

    def _csv(self, date_value):
        return (
            "NIIN,MOE_RULE,DT_ASGND,SOS\n"
            f"000000042,RULE1,{date_value},S1\n"
        )

    def test_2digit_date_on_custom_column(self, tmp_path):
        """20-FEB-02 in DT_ASGND is converted to 2002-02-20."""
        path = _make_csv(tmp_path, self._csv("20-FEB-02"))
        df = load_csv(
            path,
            required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"],
            date_columns=["DT_ASGND"],
            date_format="dd-MMM-yy",
        )
        assert df.loc[0, "DT_ASGND"] == "2002-02-20"

    def test_2digit_century_pivot_on_custom_column(self, tmp_path):
        """18-MAR-52 in DT_ASGND is converted to 1952-03-18 (century pivot)."""
        path = _make_csv(tmp_path, self._csv("18-MAR-52"))
        df = load_csv(
            path,
            required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"],
            date_columns=["DT_ASGND"],
            date_format="dd-MMM-yy",
        )
        assert df.loc[0, "DT_ASGND"] == "1952-03-18"


# ---------------------------------------------------------------------------
# TRN-03  Management date conversion (dd-MMM-yyyy, 4-digit year, no pivot)
# ---------------------------------------------------------------------------

class TestManagementDateConversion:
    """load_csv() must convert dd-MMM-yyyy dates on EFFECTIVE_DATE without century pivot."""

    def _csv(self, date_value):
        return (
            "NIIN,EFFECTIVE_DATE,DEMIL_CODE\n"
            f"000000042,{date_value},D1\n"
        )

    def test_4digit_date_conversion(self, tmp_path):
        """20-JAN-2024 in EFFECTIVE_DATE is converted to 2024-01-20."""
        path = _make_csv(tmp_path, self._csv("20-JAN-2024"))
        df = load_csv(
            path,
            required_columns=["NIIN", "EFFECTIVE_DATE", "DEMIL_CODE"],
            date_columns=["EFFECTIVE_DATE"],
            date_format="dd-MMM-yyyy",
        )
        assert df.loc[0, "EFFECTIVE_DATE"] == "2024-01-20"

    def test_4digit_date_no_pivot(self, tmp_path):
        """15-MAR-1987 in EFFECTIVE_DATE is converted to 1987-03-15 (no century pivot)."""
        path = _make_csv(tmp_path, self._csv("15-MAR-1987"))
        df = load_csv(
            path,
            required_columns=["NIIN", "EFFECTIVE_DATE", "DEMIL_CODE"],
            date_columns=["EFFECTIVE_DATE"],
            date_format="dd-MMM-yyyy",
        )
        assert df.loc[0, "EFFECTIVE_DATE"] == "1987-03-15"


# ---------------------------------------------------------------------------
# TRN-04  Column drop / subsetting
# ---------------------------------------------------------------------------

class TestColumnDrop:
    """load_csv() must subset output to only required_columns, dropping extra columns."""

    def test_extra_columns_dropped(self, tmp_path):
        """CSV with extra column ROW_OBS_DT is absent from output when not in required_columns."""
        content = (
            "NIIN,MOE_RULE,DT_ASGND,SOS,ROW_OBS_DT\n"
            "000000042,RULE1,20-FEB-02,S1,01-JAN-2020\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(path, required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"])
        assert "ROW_OBS_DT" not in df.columns

    def test_column_order_matches_required(self, tmp_path):
        """Output column order matches the order specified in required_columns."""
        content = (
            "SOS,NIIN,MOE_RULE,DT_ASGND\n"
            "S1,000000042,RULE1,20-FEB-02\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(path, required_columns=["NIIN", "MOE_RULE", "DT_ASGND", "SOS"])
        assert list(df.columns) == ["NIIN", "MOE_RULE", "DT_ASGND", "SOS"]


# ---------------------------------------------------------------------------
# TRN-01/TRN-04  CAGE no date conversion
# ---------------------------------------------------------------------------

class TestCageNoDateConversion:
    """load_csv() with empty date_columns must perform no date conversion."""

    def test_no_date_columns_no_conversion(self, tmp_path):
        """date_columns=[] performs no conversion; PARENT_CAGE is absent from output."""
        content = (
            "CAGE_CODE,STATUS,TYPE,ASSOC_CODE,DESIGNATOR,PARENT_CAGE\n"
            "12345,20-FEB-02,T,11111,X,99999\n"
        )
        path = _make_csv(tmp_path, content)
        df = load_csv(
            path,
            required_columns=["CAGE_CODE", "STATUS", "TYPE", "ASSOC_CODE", "DESIGNATOR"],
            date_columns=[],
            date_format="",
        )
        # STATUS must NOT be date-converted
        assert df.loc[0, "STATUS"] == "20-FEB-02"
        # PARENT_CAGE must be dropped (not in required_columns)
        assert "PARENT_CAGE" not in df.columns
