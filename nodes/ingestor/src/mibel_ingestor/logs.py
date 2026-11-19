"""JSON log lines on stdout (collected by journald under systemd).

Never log URLs or headers of secret requests (AGENTS.md §3); urllib3's debug lines would include full
URLs, so that logger is capped at WARNING.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

# --------------------------------------------------------------------------- constants
# Attributes every LogRecord has; anything else was passed through `extra=` and is logged as a field
_STANDARD_ATTRIBUTES = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


# --------------------------------------------------------------------------- public API
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


def configure_logging(level: str) -> None:
    """Send every log record to stdout as JSON at the given level."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    logging.getLogger("urllib3").setLevel(logging.WARNING)
