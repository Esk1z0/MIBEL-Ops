"""HTTP client limits: capped Retry-After and a total download deadline (no worker held for long)."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from urllib3.response import HTTPResponse

from mibel_ingestor.http import CHUNK_BYTES, MAX_RETRY_AFTER_S, FetchError, HttpClient, _build_retry
from mibel_ingestor.model import Request
from mibel_ingestor.settings import HttpSettings


CHUNK = b"x" * CHUNK_BYTES  # one read chunk per write, so the deadline is checked between writes
DRIP_CHUNKS = 40
DRIP_PAUSE_S = 0.05


class DripHandler(BaseHTTPRequestHandler):
    """Serves a body slowly: each chunk arrives before the read timeout, but the whole body takes long."""

    def do_GET(self) -> None:
        """Send ``DRIP_CHUNKS`` chunks with a pause between them."""
        self.send_response(200)
        self.send_header("Content-Length", str(len(CHUNK) * DRIP_CHUNKS))
        self.end_headers()
        for _ in range(DRIP_CHUNKS):
            self.wfile.write(CHUNK)
            self.wfile.flush()
            time.sleep(DRIP_PAUSE_S)

    def log_message(self, format: str, *args: object) -> None:
        """Keep the test output quiet."""


@pytest.fixture
def drip_server() -> Iterator[str]:
    """Run a local dripping HTTP server and return its URL."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), DripHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/"
    server.shutdown()
    server.server_close()


def http_settings(timeout: float) -> HttpSettings:
    """Small limits for tests."""
    return HttpSettings(timeout=timeout, retries=0, backoff=0, max_response_bytes=10_000_000, user_agent="test")


# ##################################################################################################
# #######################################     RETRY-AFTER     ######################################
# ##################################################################################################


def test_retry_after_is_capped() -> None:
    retry = _build_retry(http_settings(timeout=30))
    hour_long = HTTPResponse(body=b"", headers={"Retry-After": "3600"}, status=429)
    short = HTTPResponse(body=b"", headers={"Retry-After": "5"}, status=429)
    assert retry.get_retry_after(hour_long) == MAX_RETRY_AFTER_S
    assert retry.get_retry_after(short) == 5


def test_capped_policy_survives_urllib3_copies() -> None:
    # urllib3 builds a new Retry object on every attempt; the cap must travel with it
    retry = _build_retry(http_settings(timeout=30)).new(total=1)
    assert retry.get_retry_after(HTTPResponse(body=b"", headers={"Retry-After": "999"})) == MAX_RETRY_AFTER_S


# ##################################################################################################
# #####################################     DOWNLOAD DEADLINE     ##################################
# ##################################################################################################


def test_slow_body_hits_the_download_deadline(drip_server: str) -> None:
    # timeout 0.2 s per read, deadline 4 x 0.2 = 0.8 s; the body needs 40 x 0.05 = 2 s
    client = HttpClient(http_settings(timeout=0.2))
    started = time.monotonic()
    with pytest.raises(FetchError, match="too long"):
        client.get(Request(drip_server))
    assert time.monotonic() - started < 1.8


def test_fast_body_is_read_completely(drip_server: str) -> None:
    client = HttpClient(http_settings(timeout=2))
    assert len(client.get(Request(drip_server))) == len(CHUNK) * DRIP_CHUNKS
