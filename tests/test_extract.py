"""Unit tests for extract.py -- download, validate, and extract characteristics zip.

Covers requirements: DL-01 (Chrome TLS download), DL-02 (zip validation),
DL-03 (CSV extraction with absolute path).
"""
import io
import logging
import os
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from extract import (
    _download_file,
    _find_csv_member,
    _resolve_download_url,
    _validate_zip,
    extract_data,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(content, status_code=200):
    """Return a MagicMock simulating a curl_cffi response with streaming support."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.iter_content.return_value = [content]
    resp.raise_for_status.return_value = None
    return resp


def _create_zip(path, members):
    """Create a zip file at path with {name: content} members."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)


# Mock target: curl_cffi.requests.get as used by extract.py
_CFFI_GET = "extract.cffi_requests.get"


# ---------------------------------------------------------------------------
# URL resolution tests (scrape reading room page)
# ---------------------------------------------------------------------------

READING_ROOM_HTML = """
<html><body>
<table>
<tr><td>CAGE</td><td><a href="/files/CAGE.zip">CAGE.zip</a></td></tr>
<tr><td>Characteristics</td><td><a href="/files/Characteristics.zip">Characteristics.zip</a></td></tr>
<tr><td>History</td><td><a href="/files/History.zip">History.zip</a></td></tr>
<tr><td>Management</td><td><a href="/files/MANAGEMENT.zip">MANAGEMENT.zip</a></td></tr>
<tr><td>MOE Rule</td><td><a href="/files/MOE_RULE.zip">MOE_RULE.zip</a></td></tr>
</table>
</body></html>
"""


class TestResolveDownloadUrl:
    """_resolve_download_url() must scrape the reading room page for the zip link."""

    def test_finds_characteristics_zip_link(self):
        """Finds the Characteristics.zip link and returns an absolute URL."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/reading-room/", logging.getLogger("test")
            )

        assert result.endswith("/files/Characteristics.zip")

    def test_case_insensitive_match(self):
        """Matches 'characteristics' regardless of case in text or href."""
        html = '<html><body><a href="/dl/CHARACTERISTICS.ZIP">Download</a></body></html>'
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/page/", logging.getLogger("test")
            )

        assert result.endswith("/dl/CHARACTERISTICS.ZIP")

    def test_no_matching_link_exits_1(self):
        """Exits with code 1 when no Characteristics.zip link is found."""
        html = "<html><body><a href='/other.zip'>Other.zip</a></body></html>"
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            with pytest.raises(SystemExit) as exc_info:
                _resolve_download_url(
                    "https://www.dla.mil/page/", logging.getLogger("test")
                )
        assert exc_info.value.code == 1

    def test_http_error_exits_1(self):
        """Exits with code 1 on HTTP error fetching the page."""
        from curl_cffi.requests.exceptions import RequestException

        with patch(_CFFI_GET, side_effect=RequestException("500")):
            with pytest.raises(SystemExit) as exc_info:
                _resolve_download_url(
                    "https://www.dla.mil/page/", logging.getLogger("test")
                )
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# DL-01  Download tests
# ---------------------------------------------------------------------------

class TestDownload:
    """_download_file() must use Chrome TLS impersonation and handle HTTP errors."""

    def test_download_uses_chrome_impersonation(self, tmp_path):
        """_download_file() passes impersonate='chrome' to curl_cffi."""
        dest = str(tmp_path / "test.zip")
        mock_resp = _mock_response(b"fake zip content")

        with patch(_CFFI_GET, return_value=mock_resp) as mock_get:
            _download_file("http://example.com/test.zip", dest, logging.getLogger("test"))

        call_kwargs = mock_get.call_args
        assert call_kwargs[1].get("impersonate") == "chrome"

    def test_download_writes_content_to_file(self, tmp_path):
        """_download_file() writes the response body to the destination file."""
        dest = str(tmp_path / "test.zip")
        mock_resp = _mock_response(b"fake zip content")

        with patch(_CFFI_GET, return_value=mock_resp):
            _download_file("http://example.com/test.zip", dest, logging.getLogger("test"))

        with open(dest, "rb") as fh:
            assert fh.read() == b"fake zip content"

    def test_download_http_error_exits_1(self, tmp_path):
        """_download_file() raises SystemExit(1) on HTTP error."""
        from curl_cffi.requests.exceptions import RequestException

        dest = str(tmp_path / "test.zip")
        with patch(_CFFI_GET, side_effect=RequestException("403 Forbidden")):
            with pytest.raises(SystemExit) as exc_info:
                _download_file(
                    "http://example.com/test.zip", dest, logging.getLogger("test")
                )
        assert exc_info.value.code == 1

    def test_download_network_error_exits_1(self, tmp_path):
        """_download_file() raises SystemExit(1) on network error."""
        from curl_cffi.requests.exceptions import RequestException

        dest = str(tmp_path / "test.zip")
        with patch(_CFFI_GET, side_effect=RequestException("Network unreachable")):
            with pytest.raises(SystemExit) as exc_info:
                _download_file(
                    "http://example.com/test.zip", dest, logging.getLogger("test")
                )
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# DL-02  Zip validation tests
# ---------------------------------------------------------------------------

class TestValidateZip:
    """_validate_zip() must reject HTML files and accept real zips."""

    def test_html_file_exits_1(self, tmp_path):
        """An HTML file saved as .zip causes SystemExit(1)."""
        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(b"<!DOCTYPE html><html>error</html>")

        with pytest.raises(SystemExit) as exc_info:
            _validate_zip(str(zip_path), logging.getLogger("test"))
        assert exc_info.value.code == 1

    def test_valid_zip_passes(self, tmp_path):
        """A real zip file passes validation without raising an exception."""
        zip_path = str(tmp_path / "test.zip")
        _create_zip(zip_path, {"readme.txt": "hello"})

        # Should not raise
        _validate_zip(zip_path, logging.getLogger("test"))

    def test_empty_file_exits_1(self, tmp_path):
        """An empty file saved as .zip causes SystemExit(1)."""
        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(b"")

        with pytest.raises(SystemExit) as exc_info:
            _validate_zip(str(zip_path), logging.getLogger("test"))
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# DL-03  CSV extraction tests
# ---------------------------------------------------------------------------

class TestExtractCSV:
    """_find_csv_member() and full extraction must handle CSV location and naming."""

    def test_extracts_csv_returns_absolute_path(self, tmp_path):
        """extract_data() returns an absolute path ending with the CSV filename."""
        zip_path = str(tmp_path / "characteristics.zip")
        _create_zip(zip_path, {"V_CHARACTERISTICS.CSV": "NIIN,MRC\n001,A\n"})

        mock_resp = _mock_response(open(zip_path, "rb").read())

        with patch(_CFFI_GET, return_value=mock_resp):
            result = extract_data(
                "http://example.com/characteristics.zip",
                str(tmp_path / "work"),
            )

        assert os.path.isabs(result)
        assert result.endswith("V_CHARACTERISTICS.CSV")

    def test_zip_without_csv_exits_1(self, tmp_path):
        """_find_csv_member() raises SystemExit(1) when the zip has no CSV member."""
        zip_path = str(tmp_path / "no_csv.zip")
        _create_zip(zip_path, {"readme.txt": "no csv here"})

        with zipfile.ZipFile(zip_path, "r") as zf:
            with pytest.raises(SystemExit) as exc_info:
                _find_csv_member(zf, logging.getLogger("test"))
        assert exc_info.value.code == 1

    def test_csv_name_case_insensitive(self, tmp_path):
        """_find_csv_member() finds a lowercase .csv member as well as uppercase .CSV."""
        zip_path = str(tmp_path / "lower.zip")
        _create_zip(zip_path, {"data.csv": "col1,col2\n"})

        with zipfile.ZipFile(zip_path, "r") as zf:
            member = _find_csv_member(zf, logging.getLogger("test"))
        assert member == "data.csv"


# ---------------------------------------------------------------------------
# Full orchestrator tests
# ---------------------------------------------------------------------------

class TestExtractData:
    """extract_data() must orchestrate download, validation, and extraction."""

    def test_extract_data_end_to_end(self, tmp_path):
        """extract_data() returns a path to an existing file with the expected CSV content."""
        csv_content = "NIIN,MRC,REQUIREMENTS_STATEMENT,CLEAR_TEXT_REPLY\n000000042,A,Req,Reply\n"

        # Build a real zip in memory that contains V_CHARACTERISTICS.CSV
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("V_CHARACTERISTICS.CSV", csv_content)
        zip_bytes = buf.getvalue()

        mock_resp = _mock_response(zip_bytes)
        work_dir = str(tmp_path / "work")

        with patch(_CFFI_GET, return_value=mock_resp):
            result = extract_data(
                "http://example.com/characteristics.zip",
                work_dir,
            )

        assert os.path.isabs(result)
        assert os.path.exists(result)
        with open(result, "r", encoding="utf-8") as fh:
            contents = fh.read()
        assert "NIIN" in contents
        assert "000000042" in contents


# ---------------------------------------------------------------------------
# zip_name parameter tests for _resolve_download_url
# ---------------------------------------------------------------------------

class TestResolveDownloadUrlZipName:
    """_resolve_download_url() must accept zip_name and find the matching link."""

    def test_finds_cage_zip(self):
        """_resolve_download_url with zip_name='CAGE.zip' returns URL ending /files/CAGE.zip."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/reading-room/",
                logging.getLogger("test"),
                zip_name="CAGE.zip",
            )

        assert result.endswith("/files/CAGE.zip")

    def test_finds_management_zip(self):
        """_resolve_download_url with zip_name='MANAGEMENT.zip' returns URL ending /files/MANAGEMENT.zip."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/reading-room/",
                logging.getLogger("test"),
                zip_name="MANAGEMENT.zip",
            )

        assert result.endswith("/files/MANAGEMENT.zip")

    def test_finds_moe_rule_zip(self):
        """_resolve_download_url with zip_name='MOE_RULE.zip' returns URL ending /files/MOE_RULE.zip."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/reading-room/",
                logging.getLogger("test"),
                zip_name="MOE_RULE.zip",
            )

        assert result.endswith("/files/MOE_RULE.zip")

    def test_no_matching_zip_exits_1(self):
        """_resolve_download_url with zip_name='NONEXISTENT.zip' raises SystemExit(1)."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            with pytest.raises(SystemExit) as exc_info:
                _resolve_download_url(
                    "https://www.dla.mil/reading-room/",
                    logging.getLogger("test"),
                    zip_name="NONEXISTENT.zip",
                )
        assert exc_info.value.code == 1

    def test_case_insensitive_zip_name(self):
        """_resolve_download_url with zip_name='cage.zip' (lowercase) finds /files/CAGE.zip."""
        mock_resp = MagicMock()
        mock_resp.text = READING_ROOM_HTML
        mock_resp.raise_for_status.return_value = None

        with patch(_CFFI_GET, return_value=mock_resp):
            result = _resolve_download_url(
                "https://www.dla.mil/reading-room/",
                logging.getLogger("test"),
                zip_name="cage.zip",
            )

        assert result.endswith("/files/CAGE.zip")


# ---------------------------------------------------------------------------
# csv_name parameter tests for _find_csv_member
# ---------------------------------------------------------------------------

class TestFindCsvMemberByName:
    """_find_csv_member() must accept csv_name and select the matching member."""

    def test_selects_named_member(self, tmp_path):
        """_find_csv_member with csv_name='V_FLIS_MANAGEMENT.CSV' selects that member."""
        zip_path = str(tmp_path / "management.zip")
        _create_zip(zip_path, {
            "V_FLIS_MANAGEMENT.CSV": "col1,col2\n",
            "OTHER.CSV": "other data\n",
        })

        with zipfile.ZipFile(zip_path, "r") as zf:
            member = _find_csv_member(zf, logging.getLogger("test"), csv_name="V_FLIS_MANAGEMENT.CSV")

        assert member == "V_FLIS_MANAGEMENT.CSV"

    def test_csv_name_case_insensitive(self, tmp_path):
        """_find_csv_member with csv_name='v_flis_management.csv' finds 'V_FLIS_MANAGEMENT.CSV'."""
        zip_path = str(tmp_path / "management.zip")
        _create_zip(zip_path, {
            "V_FLIS_MANAGEMENT.CSV": "col1,col2\n",
        })

        with zipfile.ZipFile(zip_path, "r") as zf:
            member = _find_csv_member(zf, logging.getLogger("test"), csv_name="v_flis_management.csv")

        assert member == "V_FLIS_MANAGEMENT.CSV"

    def test_csv_name_not_found_falls_back(self, tmp_path):
        """_find_csv_member with csv_name='MISSING.CSV' falls back to first CSV member."""
        zip_path = str(tmp_path / "test.zip")
        _create_zip(zip_path, {
            "FIRST.CSV": "data\n",
        })

        with zipfile.ZipFile(zip_path, "r") as zf:
            member = _find_csv_member(zf, logging.getLogger("test"), csv_name="MISSING.CSV")

        assert member == "FIRST.CSV"

    def test_no_csv_name_uses_first(self, tmp_path):
        """_find_csv_member with csv_name=None uses first CSV member (backwards compat)."""
        zip_path = str(tmp_path / "test.zip")
        _create_zip(zip_path, {
            "FIRST.CSV": "data\n",
            "SECOND.CSV": "more data\n",
        })

        with zipfile.ZipFile(zip_path, "r") as zf:
            member = _find_csv_member(zf, logging.getLogger("test"), csv_name=None)

        assert member == "FIRST.CSV"


# ---------------------------------------------------------------------------
# zip_name and csv_name parameter tests for extract_data
# ---------------------------------------------------------------------------

class TestExtractDataZipName:
    """extract_data() must accept zip_name and csv_name and route downloads accordingly."""

    def test_saves_zip_as_zip_name(self, tmp_path):
        """extract_data with zip_name='CAGE.zip' saves file as 'CAGE.zip' in work_dir."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("V_CAGE_STATUS_AND_TYPE.CSV", "CAGE_CODE,NAME\n12345,ACME\n")
        zip_bytes = buf.getvalue()

        mock_resp = _mock_response(zip_bytes)
        work_dir = str(tmp_path / "work")

        with patch(_CFFI_GET, return_value=mock_resp):
            result = extract_data(
                "http://example.com/CAGE.zip",
                work_dir,
                zip_name="CAGE.zip",
            )

        assert os.path.exists(os.path.join(work_dir, "CAGE.zip"))
        assert not os.path.exists(os.path.join(work_dir, "characteristics.zip"))

    def test_passes_csv_name_to_find_member(self, tmp_path):
        """extract_data with csv_name='V_FLIS_MANAGEMENT.CSV' extracts that specific member."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("V_FLIS_MANAGEMENT.CSV", "NIIN,DATE\n001,01-JAN-2024\n")
        zip_bytes = buf.getvalue()

        mock_resp = _mock_response(zip_bytes)
        work_dir = str(tmp_path / "work")

        with patch(_CFFI_GET, return_value=mock_resp):
            result = extract_data(
                "http://example.com/MANAGEMENT.zip",
                work_dir,
                zip_name="MANAGEMENT.zip",
                csv_name="V_FLIS_MANAGEMENT.CSV",
            )

        assert result.endswith("V_FLIS_MANAGEMENT.CSV")
        assert os.path.exists(result)

    def test_default_zip_name_backwards_compat(self, tmp_path):
        """extract_data with no zip_name argument still works for Characteristics."""
        csv_content = "NIIN,MRC\n001,A\n"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("V_CHARACTERISTICS.CSV", csv_content)
        zip_bytes = buf.getvalue()

        mock_resp = _mock_response(zip_bytes)
        work_dir = str(tmp_path / "work")

        with patch(_CFFI_GET, return_value=mock_resp):
            result = extract_data(
                "http://example.com/Characteristics.zip",
                work_dir,
            )

        assert os.path.exists(os.path.join(work_dir, "Characteristics.zip"))
        assert result.endswith("V_CHARACTERISTICS.CSV")
