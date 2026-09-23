"""Tests that remfile identifies its requests: a User-Agent header normally,
and a source=remfile query parameter for archive hosts under pyodide.

Uses a local HTTP server that records requests, so no network access is
needed.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile
from remfile import request_watermark
from remfile.request_watermark import (
    USER_AGENT,
    add_request_watermark,
    is_presigned_url,
    is_watermark_host,
    request_headers,
    request_url,
)

DATA = bytes(i % 256 for i in range(4096))

DANDI_BLOB = "https://dandiarchive.s3.amazonaws.com/blobs/d86/055/d8605573"


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://example.org/blob", "https://example.org/blob?source=remfile"),
        (
            "https://example.org/blob?versionId=abc",
            "https://example.org/blob?versionId=abc&source=remfile",
        ),
        (
            "https://example.org/blob?a=1#frag",
            "https://example.org/blob?a=1&source=remfile#frag",
        ),
        ("https://example.org/blob?", "https://example.org/blob?source=remfile"),
        # already watermarked: idempotent
        (
            "https://example.org/blob?source=remfile",
            "https://example.org/blob?source=remfile",
        ),
        # an existing source parameter is left alone
        ("https://example.org/blob?source=other", "https://example.org/blob?source=other"),
        # presigned urls (SigV4 and SigV2): the signature covers the query string
        (
            "https://bucket.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc",
            "https://bucket.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc",
        ),
        (
            "https://bucket.s3.amazonaws.com/k?AWSAccessKeyId=a&Expires=1&Signature=abc",
            "https://bucket.s3.amazonaws.com/k?AWSAccessKeyId=a&Expires=1&Signature=abc",
        ),
        # not http(s)
        ("file:///tmp/blob", "file:///tmp/blob"),
    ],
)
def test_add_request_watermark(url, expected):
    assert add_request_watermark(url) == expected
    assert add_request_watermark(add_request_watermark(url)) == expected


def test_is_presigned_url():
    assert is_presigned_url("https://b/k?x-amz-signature=abc")
    assert not is_presigned_url("https://b/k?X-Amz-Algorithm=x")
    assert not is_presigned_url("https://b/k")


@pytest.mark.parametrize(
    "url",
    [
        DANDI_BLOB,
        "https://dandiarchive.s3.us-east-2.amazonaws.com/blobs/abc",
        "https://s3.amazonaws.com/dandiarchive/blobs/abc",
        "https://dandiarchive-embargo.s3.amazonaws.com/blobs/abc",
        "https://api.dandiarchive.org/api/assets/abc/download/",
        "https://api-dandi.emberarchive.org/api/assets/abc/download/",
        "https://ember-open-data.s3.amazonaws.com/blobs/abc",
        "https://s3.us-east-2.amazonaws.com/ember-open-data/blobs/abc",
        "https://openneuro.org.s3.amazonaws.com/ds000001/sub-01/anat/x.nii.gz",
        "https://s3.amazonaws.com/openneuro.org/ds000001/x.nii.gz",
        "https://openneuro.org/crn/datasets/ds000001/files/x",
    ],
)
def test_is_watermark_host(url):
    assert is_watermark_host(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/blob",
        "https://notdandiarchive.org/blob",
        "https://dandiarchive.org.evil.com/blob",
        "https://otherbucket.s3.amazonaws.com/dandiarchive/blob",
        "https://s3.amazonaws.com/otherbucket/blob",
        "http://127.0.0.1:8000/blob",
        "not a url",
    ],
)
def test_is_not_watermark_host(url):
    assert not is_watermark_host(url)


def test_outside_pyodide_uses_user_agent_and_leaves_url_alone():
    assert request_url(DANDI_BLOB) == DANDI_BLOB
    assert request_headers() == {"User-Agent": USER_AGENT}
    assert USER_AGENT.startswith("remfile/")


def test_pyodide_watermarks_archive_urls_only(monkeypatch):
    monkeypatch.setattr(request_watermark, "_in_pyodide", lambda: True)
    assert request_url(DANDI_BLOB) == DANDI_BLOB + "?source=remfile"
    assert request_url("https://example.org/blob") == "https://example.org/blob"
    presigned = DANDI_BLOB + "?X-Amz-Signature=abc"
    assert request_url(presigned) == presigned
    # the browser refuses to let a page set the User-Agent
    assert request_headers() == {}


class _Handler(BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):  # noqa: N802
        _Handler.requests.append((self.path, self.headers.get("User-Agent")))
        rng = self.headers.get("Range")
        if not rng:
            self.send_response(200)
            self.send_header("Content-Length", str(len(DATA)))
            self.end_headers()
            self.wfile.write(DATA)
            return
        start_s, end_s = rng.split("=")[1].split("-")
        start, end = int(start_s), min(int(end_s), len(DATA) - 1)
        body = DATA[start:end + 1]
        self.send_response(206)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep the test output quiet
        pass


@pytest.fixture
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    _Handler.requests = []
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


@pytest.mark.parametrize("use_session", [True, False])
def test_requests_carry_user_agent(server, use_session):
    f = remfile.File(server, _min_chunk_size=1024, _use_session=use_session)
    f.seek(100)
    assert f.read(2000) == DATA[100:2100]
    assert len(_Handler.requests) >= 2  # size probe + at least one range request
    assert all(r == ("/data.bin", USER_AGENT) for r in _Handler.requests)
