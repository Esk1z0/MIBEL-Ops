"""JSON log lines on stdout (journald under systemd). Never log URLs or headers of secret requests."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

_STANDARD = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname, "logger": record.name, "msg": record.getMessage(),
        }
        doc.update({k: v for k, v in vars(record).items() if k not in _STANDARD})
        if record.exc_info and record.exc_info[0] is not None:
            doc["error"] = f"{record.exc_info[0].__name__}: {record.exc_info[1]}"
        return json.dumps(doc, ensure_ascii=False, default=str)


def setup(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    logging.getLogger("urllib3").setLevel(logging.WARNING)     # urllib3 debug lines include full URLs
