"""Tests of the public size and max_threads arguments, and their deprecated
underscore aliases.

These use a local HTTP server, so they need no network access.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

DATA = bytes(range(256)) * 64  # 16 KB of known bytes


class _Handler(BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")
        _Handler.requests.append(rng)
        if rng:
            start_s, end_s = rng.split("=")[1].split("-")
            body, status = DATA[int(start_s):int(end_s) + 1], 206
        else:
            body, status = DATA, 200
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
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


def test_size_skips_the_size_request(server):
    f = remfile.File(server, size=len(DATA))
    assert f.length == len(DATA)
    assert f.read(10) == DATA[:10]
    # Only the range request, no request for the size
    assert len(_Handler.requests) == 1 and _Handler.requests[0] is not None


def test_max_threads(server):
    assert remfile.File(server, size=len(DATA))._max_threads == 1
    assert remfile.File(server, size=len(DATA), max_threads=3)._max_threads == 3


def test_deprecated_aliases(server):
    with pytest.warns(DeprecationWarning, match="_size"):
        f = remfile.File(server, _size=len(DATA))
    assert f.length == len(DATA)
    with pytest.warns(DeprecationWarning, match="_max_threads"):
        f = remfile.File(server, size=len(DATA), _max_threads=3)
    assert f._max_threads == 3


def test_both_spellings_rejected(server):
    with pytest.raises(TypeError):
        remfile.File(server, size=1, _size=1)
    with pytest.raises(TypeError):
        remfile.File(server, size=1, max_threads=2, _max_threads=2)
