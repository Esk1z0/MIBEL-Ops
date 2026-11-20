"""HTTP adapter: one session per source, bounded retries, polite rate limit, size and time limits.

Errors never include the URL or parameters of a request marked ``secret`` (AGENTS.md §3).
Downloaded payloads are untrusted data: they are only parsed, never executed.

A download never holds a worker for long: ``Retry-After`` waits are capped, and reading the body has
a total deadline on top of the per-read ``timeout``.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .model import NotAvailable, Request
from .settings import HttpSettings


if TYPE_CHECKING:
    # typing only: BaseHTTPResponse does not exist in urllib3 1.26, which requests still accepts
    from urllib3.response import BaseHTTPResponse


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
CHUNK_BYTES = 64 * 1024
# Datasets of one source may run in parallel on the worker pool; a pool of 4 avoids
# "connection pool is full" warnings without keeping many idle sockets on the Pi.
POOL_SIZE = 4
HIDDEN_URL = "<hidden: request carries a secret>"
# urllib3 honours Retry-After up to 6 h, which would block a worker for that long. Longer waits are
# cut to this; once the bounded retries are spent, the scheduler's failure backoff takes over.
MAX_RETRY_AFTER_S = 60.0
# `timeout` bounds each socket read, not the whole body: reading may take at most this many timeouts
DOWNLOAD_DEADLINE_FACTOR = 4


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class FetchError(RuntimeError):
    """The download failed after its bounded retries (network error, HTTP error, oversized or slow body)."""


class _BoundedRetry(Retry):
    """urllib3 retry policy whose ``Retry-After`` waits are capped at ``MAX_RETRY_AFTER_S``."""

    def get_retry_after(self, response: BaseHTTPResponse) -> float | None:
        """Return the server's requested wait, capped."""
        seconds = super().get_retry_after(response)
        if seconds is None:
            return None
        return min(seconds, MAX_RETRY_AFTER_S)


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


class HttpClient:
    """HTTP GET client for one source.

    ``min_interval`` (seconds) spaces consecutive requests to the same source, to respect the usage
    limits of public APIs. Retries (with backoff and ``Retry-After``) are bounded per call by
    ``HttpSettings.retries``; the scheduler decides when to try again after a failed run.
    """

    def __init__(self, http_settings: HttpSettings, *, min_interval: float = 0.0) -> None:
        self.http_settings = http_settings
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_request = 0.0
        self.session = requests.Session()
        self.session.headers["User-Agent"] = http_settings.user_agent
        adapter = HTTPAdapter(max_retries=_build_retry(http_settings), pool_maxsize=POOL_SIZE)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, request: Request) -> bytes:
        """Download a request and return its body.

        Raises:
            NotAvailable: HTTP 404, which public sources use for "not published yet".
            FetchError: any other failure; the message hides the URL of secret requests.
        """
        self._wait_for_turn()
        where = HIDDEN_URL if request.secret else request.url
        deadline = time.monotonic() + self.http_settings.timeout * DOWNLOAD_DEADLINE_FACTOR
        try:
            with self.session.get(
                request.url,
                params=request.params,
                headers=request.headers,
                timeout=self.http_settings.timeout,
                stream=True,
            ) as response:
                if response.status_code == 404:
                    raise NotAvailable(f"HTTP 404 for {where}")
                if response.status_code >= 400:
                    raise FetchError(f"HTTP {response.status_code} for {where}")
                return self._read_limited(response, where, deadline)
        except requests.RequestException as exc:
            # `from None`: the original exception text may contain the full URL with its token
            raise FetchError(f"{type(exc).__name__} for {where}") from None

    def close(self) -> None:
        """Close the pooled connections."""
        self.session.close()

    # --- private ---
    def _wait_for_turn(self) -> None:
        """Sleep until ``min_interval`` has passed since the previous request of this source."""
        if self.min_interval <= 0:
            return
        with self._lock:
            wait = self._last_request + self.min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def _read_limited(self, response: requests.Response, where: str, deadline: float) -> bytes:
        """Read the streamed body, refusing responses above ``max_response_bytes`` or past ``deadline``.

        The deadline is checked between chunks, so a server that drips data stays bounded to the
        deadline plus the time of one chunk.
        """
        chunks = []
        size = 0
        for chunk in response.iter_content(CHUNK_BYTES):
            size += len(chunk)
            if size > self.http_settings.max_response_bytes:
                raise FetchError(f"response larger than {self.http_settings.max_response_bytes} bytes for {where}")
            if time.monotonic() > deadline:
                raise FetchError(f"download took too long for {where}")
            chunks.append(chunk)
        return b"".join(chunks)


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _build_retry(http_settings: HttpSettings) -> Retry:
    """Return the urllib3 retry policy: GET only, transient status codes, honour a capped ``Retry-After``."""
    return _BoundedRetry(
        total=http_settings.retries,
        backoff_factor=http_settings.backoff,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
