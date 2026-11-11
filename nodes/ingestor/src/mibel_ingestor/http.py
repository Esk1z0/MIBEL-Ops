"""HTTP adapter: one session per source, bounded retries, polite rate limit, size limit.

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


class FetchError(RuntimeError):
    pass


class HttpClient:
    def __init__(self, cfg: HttpSettings, *, min_interval: float = 0.0) -> None:
        self.cfg = cfg
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last = 0.0
        self.session = requests.Session()
        self.session.headers["User-Agent"] = cfg.user_agent
        retry = Retry(total=cfg.retries, backoff_factor=cfg.backoff, status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=("GET",), respect_retry_after_header=True, raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=4)   # datasets of a source may run in parallel
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _throttle(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            wait = self._last + self.min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    def get(self, req: Request) -> bytes:
        self._throttle()
        where = "<hidden: request carries a secret>" if req.secret else req.url
        try:
            with self.session.get(req.url, params=req.params, headers=req.headers,
                                  timeout=self.cfg.timeout, stream=True) as resp:
                if resp.status_code == 404:
                    raise NotAvailable(f"HTTP 404 for {where}")
                if resp.status_code >= 400:
                    raise FetchError(f"HTTP {resp.status_code} for {where}")
                chunks, size = [], 0
                for chunk in resp.iter_content(64 * 1024):
                    size += len(chunk)
                    if size > self.cfg.max_response_bytes:
                        raise FetchError(f"response larger than {self.cfg.max_response_bytes} bytes for {where}")
                    chunks.append(chunk)
                return b"".join(chunks)
        except requests.RequestException as exc:
            raise FetchError(f"{type(exc).__name__} for {where}") from None

    def close(self) -> None:
        self.session.close()
