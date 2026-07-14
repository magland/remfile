"""Tests that a large read is fetched without ramping up over many round trips.

Uses a local HTTP server that counts requests, so no network access is needed.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import remfile

CHUNK = 1024
NUM_CHUNKS = 64
DATA = bytes((i // CHUNK) % 256 for i in range(CHUNK * NUM_CHUNKS))  # 64 KB


class _Handler(BaseHTTPRequestHandler):
    request_count = 0
    ranges = []

    def do_GET(self):  # noqa: N802
        rng = self.headers.get("Range")
        if not rng:
            # remfile's file-size probe -- not counted as a data request.
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

        _Handler.request_count += 1
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
    _Handler.request_count = 0
    _Handler.ranges = []
    yield f"http://127.0.0.1:{httpd.server_port}/data.bin"
    httpd.shutdown()
    httpd.server_close()


def test_large_read_does_not_ramp_up_over_many_requests(server):
    """A read spanning many chunks should cost one request, not a geometric ramp.

    With a 1 KB chunk size, reading all 64 KB spans 64 chunks. Growing the
    request geometrically by 1.7x would need ~9 sequential round trips to work
    through that span; fetching what the read actually asks for needs one.
    """
    f = remfile.File(server, _min_chunk_size=CHUNK)

    f.seek(0)
    data = f.read(len(DATA))

    assert data == DATA
    assert _Handler.request_count == 1, (
        f"expected a single range request, got {_Handler.request_count}: "
        f"{_Handler.ranges}"
    )


def test_partial_large_read_is_one_request(server):
    """The same holds for a large read that starts partway into the file."""
    f = remfile.File(server, _min_chunk_size=CHUNK)

    offset = 10 * CHUNK
    size = 40 * CHUNK
    f.seek(offset)
    data = f.read(size)

    assert data == DATA[offset:offset + size]
    assert _Handler.request_count == 1, (
        f"expected a single range request, got {_Handler.request_count}: "
        f"{_Handler.ranges}"
    )


def test_cached_chunks_are_not_refetched(server):
    """Re-reading the same region must not issue any new requests."""
    f = remfile.File(server, _min_chunk_size=CHUNK)

    f.seek(0)
    first = f.read(20 * CHUNK)

    _Handler.request_count = 0
    f.seek(0)
    second = f.read(20 * CHUNK)

    assert second == first
    assert _Handler.request_count == 0


def test_small_reads_are_still_correct(server):
    """Small and non-sequential reads keep working."""
    f = remfile.File(server, _min_chunk_size=CHUNK)

    for offset in (0, 5000, 100, 60000, 32768):
        f.seek(offset)
        assert f.read(50) == DATA[offset:offset + 50]
