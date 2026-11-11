"""Tests that requests time out, so that a stalled connection cannot hang
forever, and that a file can be used as a context manager.

These use a local HTTP server, so they need no network access.
"""
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

import remfile

DATA = bytes(range(256)) * 64  # 16 KB of known bytes
STALL = 2  # seconds a stalled response waits; far longer than the test timeout
TIMEOUT = 0.2


class _Handler(BaseHTTPRequestHandler):
    # Number of upcoming requests that stall before responding.
    stall_next = 0

    def do_GET(self):  # noqa: N802
        if _Handler.stall_next > 0:
            _Handler.stall_next -= 1
            time.sleep(STALL)
        rng = self.headers.get("Range")
        if not rng:
            body, status = DATA, 200
        else:
            start_s, end_s = rng.split("=")[1].split("-")
            body, status = DATA[int(start_s):int(end_s) + 1], 206
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client timed out and went away

    def log_message(self, *args):  # keep the test output quiet
        pass


@pytest.fixture
def server(monkeypatch):
    # Keep the tests quick: fail after one retry.
    monkeypatch.setattr(remfile.RemFile, "_num_request_retries", 1)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    _Handler.stall_next = 0
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def test_stalled_read_times_out(server):
    f = remfile.File(server, timeout=TIMEOUT)
    _Handler.stall_next = 2  # the request and its one retry
    t0 = time.monotonic()
    with pytest.raises(requests.exceptions.Timeout):
        f.read(10)
    assert time.monotonic() - t0 < STALL


def test_stalled_read_is_retried(server):
    f = remfile.File(server, timeout=TIMEOUT)
    _Handler.stall_next = 1  # only the first attempt stalls
    assert f.read(10) == DATA[:10]


def test_stalled_open_times_out(server):
    _Handler.stall_next = 2  # the file-size request and its one retry
    t0 = time.monotonic()
    with pytest.raises(requests.exceptions.Timeout):
        remfile.File(server, timeout=TIMEOUT)
    assert time.monotonic() - t0 < STALL


def test_default_timeout(server, monkeypatch):
    timeouts = []
    get = requests.Session.get

    def recording_get(self, *args, **kwargs):
        timeouts.append(kwargs.get("timeout"))
        return get(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", recording_get)
    f = remfile.File(server)
    assert f.read(10) == DATA[:10]
    assert timeouts == [remfile.RemFile.default_timeout]


def test_context_manager_closes_session(server, monkeypatch):
    closed = []
    monkeypatch.setattr(requests.Session, "close", lambda self: closed.append(self))
    with remfile.File(server) as f:
        assert f.read(10) == DATA[:10]
        assert f.session not in closed
    assert f.session in closed
