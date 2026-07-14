"""Tests that permanent HTTP errors fail immediately instead of backing off.

Uses a local HTTP server, so no network access is needed.
"""
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

DATA = bytes(range(256)) * 64  # 16 KB of known bytes


class _Handler(BaseHTTPRequestHandler):
    # Status to return for range requests. None means serve the data normally.
    error_status = None
    request_count = 0

    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")
        if not rng:
            # remfile's file-size probe -- always answer this normally so the
            # tests exercise the read path.
            self.send_response(200)
            self.send_header("Content-Length", str(len(DATA)))
            self.end_headers()
            self.wfile.write(DATA)
            return

        _Handler.request_count += 1

        if _Handler.error_status is not None:
            self.send_response(_Handler.error_status)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        spec = rng.split("=")[1]
        start_s, end_s = spec.split("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(DATA) - 1
        end = min(end, len(DATA) - 1)
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
    _Handler.error_status = None
    _Handler.request_count = 0
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


@pytest.mark.parametrize("status", [404, 403, 400])
def test_permanent_errors_fail_immediately(server, status):
    """A 4xx must be reported at once, not after ~25s of exponential backoff."""
    f = remfile.File(server, _min_chunk_size=1024)
    _Handler.error_status = status
    _Handler.request_count = 0

    started = time.time()
    f.seek(0)
    with pytest.raises(Exception) as excinfo:
        f.read(100)
    elapsed = time.time() - started

    assert str(status) in str(excinfo.value)
    # One attempt, no retries: the default 8 retries would take ~25 seconds.
    assert _Handler.request_count == 1
    assert elapsed < 2


def test_throttling_is_still_retried(server):
    """429 is a throttle, not a permanent error -- backing off is correct."""
    f = remfile.File(server, _min_chunk_size=1024)
    _Handler.error_status = 429
    _Handler.request_count = 0

    f.seek(0)
    with pytest.raises(Exception):
        # Keep the test quick; we only care that it retried at all.
        remfile.RemFile._num_request_retries = 2
        try:
            f.read(100)
        finally:
            remfile.RemFile._num_request_retries = 8

    assert _Handler.request_count > 1


def test_transient_errors_are_still_retried(server):
    """A 500 is transient: it must still be retried, and succeed on recovery."""
    f = remfile.File(server, _min_chunk_size=1024)
    _Handler.error_status = 500
    _Handler.request_count = 0

    # Recover after a couple of failures.
    def recover():
        time.sleep(0.25)
        _Handler.error_status = None

    threading.Thread(target=recover, daemon=True).start()

    f.seek(0)
    assert f.read(100) == DATA[:100]
    assert _Handler.request_count > 1
