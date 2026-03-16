"""Extract module -- download characteristics.zip, validate, and extract CSV.

Uses curl_cffi to impersonate Chrome's TLS fingerprint, which is required
to bypass Akamai bot detection on the DLA website.
"""

import logging
import os
import zipfile
from urllib.parse import urljoin

import lxml.html
from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException

_log = logging.getLogger(__name__)


def _resolve_download_url(page_url, logger, timeout=30):
    """Scrape the DLA reading room page and return the Characteristics.zip link.

    Fetches *page_url* using Chrome TLS impersonation, parses the HTML,
    and finds an <a> tag whose text or href contains 'characteristics'
    and ends with '.zip'.

    Args:
        page_url: URL of the DLA FLIS Electronic Reading Room page.
        logger: stdlib logger for error messages.
        timeout: HTTP request timeout in seconds (default 30).

    Returns:
        Absolute URL to the Characteristics.zip download.

    Raises:
        SystemExit(1): On HTTP/network error or if no matching link is found.
    """
    try:
        resp = cffi_requests.get(page_url, impersonate="chrome", timeout=timeout)
        resp.raise_for_status()
    except RequestException as exc:
        logger.error("Error fetching reading room page: %s", exc)
        raise SystemExit(1) from exc

    doc = lxml.html.fromstring(resp.text)
    for link in doc.iterlinks():
        element, _attr, href, _pos = link
        text = (element.text_content() or "").strip().lower()
        href_lower = href.lower()
        if "characteristics" in (text + href_lower) and href_lower.endswith(".zip"):
            download_url = urljoin(page_url, href)
            logger.info("Resolved download URL: %s", download_url)
            return download_url

    logger.error(
        "No Characteristics.zip link found on page: %s", page_url
    )
    raise SystemExit(1)


def _download_file(url, dest_path, logger, timeout=600):
    """Download *url* to *dest_path* using Chrome TLS impersonation.

    Streams response in 64 KB chunks to avoid holding the full file in memory
    (Characteristics.zip is ~450 MB).

    Args:
        url: Direct download URL for the zip file.
        dest_path: Local file path to write the downloaded bytes into.
        logger: stdlib logger for error messages.
        timeout: HTTP request timeout in seconds (default 600 for large file).

    Raises:
        SystemExit(1): On HTTP error (4xx/5xx) or network error.
    """
    try:
        resp = cffi_requests.get(
            url, impersonate="chrome", stream=True, timeout=timeout
        )
        resp.raise_for_status()
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    fh.write(chunk)
    except RequestException as exc:
        logger.error("Error downloading zip: %s", exc)
        raise SystemExit(1) from exc


def _validate_zip(path, logger):
    """Raise SystemExit(1) if *path* is not a valid zip archive.

    Logs the first 200 bytes of the file when validation fails so the operator
    can diagnose whether an HTML error page was returned instead of the zip.

    Args:
        path: Local file path to validate.
        logger: stdlib logger for error messages.

    Raises:
        SystemExit(1): If the file is not a valid zip archive.
    """
    if not zipfile.is_zipfile(path):
        try:
            with open(path, "rb") as fh:
                preview = fh.read(200)
            logger.error("Downloaded file is not a valid zip. Preview: %r", preview)
        except OSError:
            logger.error("Downloaded file is not a valid zip and cannot be read.")
        raise SystemExit(1)


def _find_csv_member(zf, logger):
    """Return the name of the first CSV member in *zf*.

    Matching is case-insensitive so both 'data.csv' and 'V_CHARACTERISTICS.CSV'
    are found correctly.

    Args:
        zf: An open zipfile.ZipFile instance.
        logger: stdlib logger for error and warning messages.

    Returns:
        The member name string (e.g. 'V_CHARACTERISTICS.CSV').

    Raises:
        SystemExit(1): If no CSV member is found in the zip.
    """
    csv_members = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
    if not csv_members:
        logger.error("No CSV file found inside zip. Contents: %s", zf.namelist())
        raise SystemExit(1)
    if len(csv_members) > 1:
        logger.warning("Multiple CSV files in zip; using first: %s", csv_members)
    return csv_members[0]


def extract_data(url, work_dir, logger=None):
    """Download characteristics.zip, validate, extract CSV, return absolute path.

    Orchestrates the full extract pipeline:
      1. Create work_dir if it does not exist.
      2. If *url* is not a direct .zip link, scrape the page to find it.
      3. Download the zip using Chrome TLS impersonation (DL-01).
      4. Validate the downloaded file is a real zip, not an HTML error page (DL-02).
      5. Extract the CSV member from the zip (DL-03).
      6. Return the absolute path to the extracted CSV.

    Args:
        url: DLA reading room page URL or direct download URL for the zip.
        work_dir: Directory to write the zip and extracted CSV into.
        logger: Optional stdlib logger.  Falls back to module logger if None.

    Returns:
        Absolute path (str) to the extracted CSV file.

    Raises:
        SystemExit(1): On HTTP error, invalid zip, or missing CSV inside zip.
    """
    log = logger or _log
    os.makedirs(work_dir, exist_ok=True)

    if not url.lower().endswith(".zip"):
        log.info("URL is not a direct zip link, scraping page: %s", url)
        url = _resolve_download_url(url, log)

    zip_path = os.path.join(work_dir, "characteristics.zip")
    log.info("Downloading %s -> %s", url, zip_path)
    _download_file(url, zip_path, log)

    log.info("Validating zip: %s", zip_path)
    _validate_zip(zip_path, log)

    with zipfile.ZipFile(zip_path, "r") as zf:
        member = _find_csv_member(zf, log)
        zf.extract(member, work_dir)

    csv_path = os.path.abspath(os.path.join(work_dir, member))
    log.info("Extracted CSV: %s", csv_path)
    return csv_path
