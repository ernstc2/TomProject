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
