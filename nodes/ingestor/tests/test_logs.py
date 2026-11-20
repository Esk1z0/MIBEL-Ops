"""Logging: tokens in request URLs never reach the log lines (AGENTS.md §3)."""

from __future__ import annotations

import logging
import socket
from collections.abc import Iterator

import pytest

from mibel_ingestor.http import FetchError, HttpClient
from mibel_ingestor.logs import configure_logging, redact_url_queries
from mibel_ingestor.model import Request
from mibel_ingestor.settings import HttpSettings


FAKE_TOKEN = "FAKE-TOKEN-123"


@pytest.fixture
def json_logging(capsys: pytest.CaptureFixture[str]) -> Iterator[None]:
    """Install the node's logging for one test and restore the previous root handlers afterwards.

    Depends on ``capsys`` so the handler is created after stderr is captured and writes into it.
    """
    root = logging.getLogger()
    previous_handlers = root.handlers[:]
    previous_level = root.level
    configure_logging("INFO")
    yield
    root.handlers[:] = previous_handlers
    root.setLevel(previous_level)


def closed_local_port() -> int:
    """Return a local TCP port with nothing listening on it (connections are refused at once)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


# ##################################################################################################
# #######################################     REDACTION     ########################################
# ##################################################################################################


def test_redact_url_queries() -> None:
    text = f"broken: /api?documentType=A11&securityToken={FAKE_TOKEN} and https://x.invalid/a?b=c'"
    assert redact_url_queries(text) == "broken: /api?<redacted> and https://x.invalid/a?<redacted>'"


def test_urllib3_retry_warning_hides_the_token(json_logging: None, capsys: pytest.CaptureFixture[str]) -> None:
    # the exact call urllib3 makes when it retries a broken connection
    logging.getLogger("urllib3.connectionpool").warning(
        "Retrying (%r) after connection broken by '%r': %s",
        "Retry(total=2)",
        "NewConnectionError()",
        f"/api?securityToken={FAKE_TOKEN}",
    )
    output = capsys.readouterr().err
    assert "Retrying" in output
    assert FAKE_TOKEN not in output


def test_refused_connection_never_logs_the_token(json_logging: None, capsys: pytest.CaptureFixture[str]) -> None:
    http_settings = HttpSettings(timeout=2, retries=1, backoff=0, max_response_bytes=1000, user_agent="test")
    client = HttpClient(http_settings)
    request = Request(
        f"http://127.0.0.1:{closed_local_port()}/api",
        params={"securityToken": FAKE_TOKEN},
        secret=True,
    )
    with pytest.raises(FetchError) as caught:
        client.get(request)
    output = capsys.readouterr().err
    assert "Retrying" in output
    assert FAKE_TOKEN not in output
    assert FAKE_TOKEN not in str(caught.value)
