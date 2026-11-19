"""HTTP adapter: one session per source, bounded retries, polite rate limit and a size limit.

Errors never include the URL or parameters of a request marked ``secret`` (AGENTS.md §3).
Downloaded payloads are untrusted data: they are only parsed, never executed.
"""

from __future__ import annotations

import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .model import NotAvailable, Request
from .settings import HttpSettings

# --------------------------------------------------------------------------- constants
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
CHUNK_BYTES = 64 * 1024
# Datasets of one source may run in parallel on the worker pool; a pool of 4 avoids
# "connection pool is full" warnings without keeping many idle sockets on the Pi.
POOL_SIZE = 4
HIDDEN_URL = "<hidden: request carries a secret>"


# --------------------------------------------------------------------------- types
class FetchError(RuntimeError):
    """The download failed after its bounded retries (network error, HTTP error or oversized body)."""


# --------------------------------------------------------------------------- public API
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
                return self._read_limited(response, where)
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

    def _read_limited(self, response: requests.Response, where: str) -> bytes:
        """Read the streamed body, refusing responses above ``max_response_bytes``."""
        chunks = []
        size = 0
        for chunk in response.iter_content(CHUNK_BYTES):
            size += len(chunk)
            if size > self.http_settings.max_response_bytes:
                raise FetchError(f"response larger than {self.http_settings.max_response_bytes} bytes for {where}")
            chunks.append(chunk)
        return b"".join(chunks)


# --------------------------------------------------------------------------- private helpers
def _build_retry(http_settings: HttpSettings) -> Retry:
    """Return the urllib3 retry policy: GET only, transient status codes, honour ``Retry-After``."""
    return Retry(
        total=http_settings.retries,
        backoff_factor=http_settings.backoff,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
