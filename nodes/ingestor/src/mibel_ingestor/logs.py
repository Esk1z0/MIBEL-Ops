"""JSON log lines on stdout (collected by journald under systemd).

Never log URLs or headers of secret requests (AGENTS.md §3). The HTTP libraries do log request paths:
urllib3 writes the full path with its query string at WARNING when it retries a broken connection, and
ENTSO-E sends its token as a query parameter. So every record of those libraries has its query strings
removed before it is written, and urllib3's debug lines are not emitted at all.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


# Attributes every LogRecord has; anything else was passed through `extra=` and is logged as a field
_STANDARD_ATTRIBUTES = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Loggers whose messages may contain request URLs (and therefore tokens in query strings)
HTTP_LOGGERS = ("urllib3", "requests")
REDACTED_QUERY = "?<redacted>"
_URL_QUERY = re.compile(r"\?[^\s'\"]*")


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


class JsonFormatter(logging.Formatter):
    """Format each record as one JSON object: time, level, logger, message and the ``extra`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        """Return the record as a JSON line."""
        document: dict[str, Any] = {
            "ts": time.strftime(TIME_FORMAT, time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name, value in vars(record).items():
            if name not in _STANDARD_ATTRIBUTES:
                document[name] = value
        if record.exc_info and record.exc_info[0] is not None:
            document["error"] = f"{record.exc_info[0].__name__}: {record.exc_info[1]}"
        return json.dumps(document, ensure_ascii=False, default=str)


class RedactUrlQueries(logging.Filter):
    """Remove query strings from the messages of the HTTP libraries, which may carry tokens."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Rewrite the message of an HTTP library record without query strings; never drop a record."""
        if record.name.startswith(HTTP_LOGGERS):
            record.msg = redact_url_queries(record.getMessage())
            record.args = ()
        return True


def redact_url_queries(text: str) -> str:
    """Replace every URL query string in ``text`` (``/api?securityToken=x``) by ``?<redacted>``."""
    return _URL_QUERY.sub(REDACTED_QUERY, text)


def configure_logging(level: str) -> None:
    """Send every log record to stdout as JSON at the given level, with HTTP query strings redacted."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    # a handler filter sees every record it writes, including those propagated from library loggers
    handler.addFilter(RedactUrlQueries())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    logging.getLogger("urllib3").setLevel(logging.WARNING)
