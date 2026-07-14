"""Tests that the _chunk_increment_factor argument actually takes effect.

Uses a local HTTP server that records the requested byte ranges, so no network
access is needed.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

CHUNK = 1024
NUM_CHUNKS = 256
DATA = bytes(i % 256 for i in range(CHUNK * NUM_CHUNKS))  # 256 KB


class _Handler(BaseHTTPRequestHandler):
    ranges = []

    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")
        if not rng:
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

        _Handler.ranges.append((start, end))

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
    _Handler.ranges = []
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def _sequential_read_spans(url, increment_factor):
    """Read chunk by chunk and return the size of each range that was fetched."""
    _Handler.ranges = []
    f = remfile.File(
        url,
        _min_chunk_size=CHUNK,
        _chunk_increment_factor=increment_factor,
    )
    # Read one chunk at a time, sequentially, so the read never itself asks for
    # more than one chunk -- the request sizes then reflect read-ahead alone.
    for i in range(20):
        f.seek(i * CHUNK)
        assert f.read(CHUNK) == DATA[i * CHUNK:(i + 1) * CHUNK]
    return [end - start + 1 for start, end in _Handler.ranges]


def test_larger_increment_factor_reads_further_ahead(server):
    """A bigger factor must grow the read-ahead window faster.

    The argument is accepted and documented, so it should do something. It used
    to be stored and then ignored, with 1.7 hardcoded in _load_chunk.
    """
    slow = _sequential_read_spans(server, 1.5)
    fast = _sequential_read_spans(server, 3.0)

    # Reading further ahead per request means fewer requests for the same data.
    assert len(fast) < len(slow), (
        f"factor 3.0 issued {len(fast)} requests, factor 1.5 issued {len(slow)}; "
        "the increment factor appears to have no effect"
    )
    # And the largest single fetch should be bigger.
    assert max(fast) > max(slow)


def test_data_is_correct_for_various_factors(server):
    """Whatever the factor, the bytes returned must be right."""
    for factor in (1.2, 1.7, 2.5, 4.0):
        f = remfile.File(
            server, _min_chunk_size=CHUNK, _chunk_increment_factor=factor
        )
        for offset in (0, 3000, 100000, len(DATA) - 500):
            f.seek(offset)
            assert f.read(500) == DATA[offset:offset + 500], f"factor={factor}"
