"""Tests that a transient failure while opening a file is retried.

Uses a local HTTP server, so no network access is needed.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

DATA = bytes(range(256)) * 64  # 16 KB of known bytes


class _Handler(BaseHTTPRequestHandler):
    # Number of upcoming requests that should fail with a 500.
    fail_next = 0
    request_count = 0

    def do_GET(self):  # noqa: N802
        _Handler.request_count += 1

        if _Handler.fail_next > 0:
            _Handler.fail_next -= 1
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        rng = self.headers.get("Range")
        if not rng:
            # remfile's file-size probe.
            self.send_response(200)
            self.send_header("Content-Length", str(len(DATA)))
            self.end_headers()
            self.wfile.write(DATA)
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
    _Handler.fail_next = 0
    _Handler.request_count = 0
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def test_transient_failure_while_opening_is_retried(server):
    """A 500 on the file-length request must not fail the open."""
    _Handler.fail_next = 2

    f = remfile.File(server, _min_chunk_size=1024)

    assert f.length == len(DATA)
    assert _Handler.request_count > 2  # it retried past the failures

    f.seek(0)
    assert f.read(100) == DATA[:100]


def test_open_still_raises_when_the_server_stays_down(server, monkeypatch):
    """Retries are bounded: a persistently failing server still raises."""
    monkeypatch.setattr(remfile.RemFile, "_num_request_retries", 1)
    _Handler.fail_next = 1000  # never recovers

    with pytest.raises(Exception) as excinfo:
        remfile.File(server, _min_chunk_size=1024)

    assert "500" in str(excinfo.value)


def test_open_succeeds_normally(server):
    """The happy path is unaffected."""
    f = remfile.File(server, _min_chunk_size=1024)
    assert f.length == len(DATA)
    assert _Handler.request_count == 1  # no retries needed
