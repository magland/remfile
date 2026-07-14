"""Tests that HTTP error responses are never mistaken for file data.

These use a local HTTP server, so they need no network access.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

DATA = bytes(range(256)) * 64  # 16 KB of known bytes
ERROR_BODY = b"<?xml version='1.0'?><Error><Code>InternalError</Code></Error>"


class _Handler(BaseHTTPRequestHandler):
    # Set by each test to control how the server misbehaves.
    mode = "ok"

    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")

        # A GET with no Range header is remfile's file-size probe. Always answer
        # it normally so that the tests exercise the read path, not the open path.
        if not rng:
            self._respond(200, DATA)
            return

        if _Handler.mode == "error_500":
            self._respond(500, ERROR_BODY)
            return

        if _Handler.mode == "error_403":
            self._respond(403, ERROR_BODY)
            return

        if _Handler.mode == "ignore_range":
            # A server with no byte-range support returns the whole file.
            self._respond(200, DATA)
            return

        start, end = _parse_range(rng, len(DATA))
        body = DATA[start:end + 1]
        self.send_response(206)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
        self.end_headers()
        self.wfile.write(body)

    def _respond(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep the test output quiet
        pass


def _parse_range(header, size):
    spec = header.split("=")[1]
    start_s, end_s = spec.split("-")
    start = int(start_s)
    end = int(end_s) if end_s else size - 1
    return start, min(end, size - 1)


@pytest.fixture
def server(monkeypatch):
    # Keep the tests quick: the default 8 retries with exponential backoff
    # would spend ~25 seconds per failing request.
    monkeypatch.setattr(remfile.RemFile, "_num_request_retries", 1)

    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    _Handler.mode = "ok"
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def _open(url):
    # Small chunks and no retries-worth of waiting: these tests only care that
    # the failure is detected, not how long the retries take.
    return remfile.File(url, _min_chunk_size=1024)


def test_http_500_is_not_returned_as_data(server):
    """An error body must never be handed back as file content."""
    f = _open(server)
    _Handler.mode = "error_500"

    f.seek(0)
    with pytest.raises(Exception) as excinfo:
        f.read(100)

    # The point of the test: whatever happens, we must not receive the error
    # document as if it were the file's bytes.
    assert b"InternalError" not in str(excinfo.value).encode()
    assert "500" in str(excinfo.value)


def test_http_403_is_not_returned_as_data(server):
    """A 403 (e.g. an expired presigned URL) must not be read as data."""
    f = _open(server)
    _Handler.mode = "error_403"

    f.seek(0)
    with pytest.raises(Exception) as excinfo:
        f.read(100)

    assert "403" in str(excinfo.value)


def test_server_ignoring_range_is_detected(server):
    """A server that returns the whole file for a range request must be caught."""
    f = _open(server)
    _Handler.mode = "ignore_range"

    # Read from a non-zero offset: the whole-file response would otherwise be
    # sliced down to the right length but contain the wrong bytes.
    f.seek(4096)
    with pytest.raises(Exception) as excinfo:
        f.read(100)

    assert "Unexpected number of bytes" in str(excinfo.value)


def test_normal_reads_still_work(server):
    """The happy path is unaffected by the new checks."""
    f = _open(server)

    f.seek(0)
    assert f.read(100) == DATA[:100]

    f.seek(4096)
    assert f.read(256) == DATA[4096:4096 + 256]
