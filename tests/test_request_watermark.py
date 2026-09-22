"""Tests that remfile tags its requests with a source=remfile query parameter.

Uses a local HTTP server that records request paths, so no network access is
needed.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile
from remfile.request_watermark import add_request_watermark, is_presigned_url

DATA = bytes(i % 256 for i in range(4096))


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


class _Handler(BaseHTTPRequestHandler):
    paths = []

    def do_GET(self):  # noqa: N802
        _Handler.paths.append(self.path)
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
    _Handler.paths = []
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def test_requests_carry_watermark(server):
    f = remfile.File(server, _min_chunk_size=1024)
    f.seek(100)
    assert f.read(2000) == DATA[100:2100]
    assert len(_Handler.paths) >= 2  # size probe + at least one range request
    assert all(p == "/data.bin?source=remfile" for p in _Handler.paths)


def test_disk_cache_key_is_unwatermarked(server, tmp_path):
    disk_cache = remfile.DiskCache(str(tmp_path))
    f = remfile.File(server, disk_cache=disk_cache, _min_chunk_size=1024)
    f.read(10)
    key = f"{server}|1024|0"
    assert disk_cache.get(key) == DATA[:1024]
