"""Tests that remfile.File behaves as a standard binary file object.

These use a local HTTP server, so they need no network access.
"""
import io
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

DATA = b"".join(b"line %d\n" % i for i in range(3000))  # ~26 KB, several chunks


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")
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
def url():
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def _open(url):
    # Small chunks, so that reads span several of them
    return remfile.File(url, _min_chunk_size=1024)


def test_is_a_raw_binary_file(url):
    f = _open(url)
    assert isinstance(f, io.RawIOBase)
    assert f.readable() and f.seekable() and not f.writable()


@pytest.mark.parametrize("size", [None, -1])
def test_read_to_end(url, size):
    f = _open(url)
    f.seek(100)
    assert f.read(size) == DATA[100:]
    assert f.tell() == len(DATA)
    assert f.read(size) == b""


def test_short_read_at_end(url):
    f = _open(url)
    assert f.seek(-5, io.SEEK_END) == len(DATA) - 5
    assert f.read(10) == DATA[-5:]
    assert f.tell() == len(DATA)
    assert f.read(10) == b""
    f.seek(len(DATA) + 10)
    assert f.read(1) == b""


def test_readinto(url):
    f = _open(url)
    f.seek(2000)
    buf = bytearray(3000)
    assert f.readinto(buf) == 3000
    assert bytes(buf) == DATA[2000:5000]


def test_buffered_and_text_wrappers(url):
    with io.TextIOWrapper(io.BufferedReader(_open(url)), encoding="ascii") as f:
        assert f.readline() == "line 0\n"
        assert list(f)[-1] == "line 2999\n"


def test_close(url):
    with _open(url) as f:
        assert not f.closed
        f.read(10)
    assert f.closed
