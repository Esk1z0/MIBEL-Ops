"""Sink port and adapters: where delivered messages go.

A sink receives outgoing messages and reports which ones were delivered. The deliverer deletes those
from the outbox and retries the rest with backoff, forever, while they are inside ``pending_ttl``.

- ``kafka``: production (``pro``). ``confluent-kafka`` is imported only when this sink is selected.
- ``jsonl``: one JSON line per message in the state directory (``test`` mode while Kafka is not up).
- ``stdout``: a human-readable summary per message (``dev``).
- ``null``: discards everything (benchmarks).
"""

from __future__ import annotations

import json
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from .model import OutgoingMessage
from .settings import SinkSettings, SinkType


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


DEFAULT_FLUSH_TIMEOUT_S = 30.0
DEFAULT_MESSAGE_TIMEOUT_MS = 30000
DEFAULT_LINGER_MS = 50
ROTATED_SUFFIX = ".1"


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


class Sink(Protocol):
    """Port of every output adapter."""

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Deliver messages and return, in the same order, whether each one was delivered."""
        ...

    def close(self) -> None:
        """Flush and release resources."""
        ...


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


class NullSink:
    """Discards every message and reports it as delivered (benchmarks)."""

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Report every message as delivered."""
        return [True] * len(messages)

    def close(self) -> None:
        """Nothing to release."""


class StdoutSink:
    """Prints one summary line per message (development)."""

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Print ``[topic] key · contract · N rows`` for each message."""
        for message in messages:
            document = json.loads(message.payload)
            row_count = len(document["rows"]) if "rows" in document else document.get("count", 0)
            print(f"[{message.topic}] {message.key} · {document.get('contract')} · {row_count} rows",
                  file=sys.stdout, flush=True)
        return [True] * len(messages)

    def close(self) -> None:
        """Nothing to release."""


class JsonlSink:
    """Appends ``{"topic", "key", "message"}`` lines to a file; rotates once to ``.1`` above ``max_bytes``."""

    def __init__(self, path: Path, max_bytes: int) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Append the messages as JSON lines."""
        with self._lock:
            self._rotate_if_full()
            with self.path.open("ab") as file:
                for message in messages:
                    file.write(_to_json_line(message))
        return [True] * len(messages)

    def close(self) -> None:
        """Nothing to release (the file is opened per call)."""

    def _rotate_if_full(self) -> None:
        """Move the file to ``<name>.1`` (replacing the previous one) once it exceeds ``max_bytes``."""
        if self.path.exists() and self.path.stat().st_size > self.max_bytes:
            self.path.replace(self.path.with_suffix(self.path.suffix + ROTATED_SUFFIX))


class KafkaSink:
    """Idempotent Kafka producer; a message counts as delivered only when the broker acknowledges it."""

    def __init__(self, sink_settings: SinkSettings, client_id: str) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise RuntimeError("kafka sink needs the 'kafka' extra: pip install 'mibel-ingestor[kafka]'") from None
        kafka_options = sink_settings.kafka
        self.flush_timeout = float(kafka_options.get("flush_timeout_s", DEFAULT_FLUSH_TIMEOUT_S))
        self.producer = Producer({
            "bootstrap.servers": sink_settings.bootstrap,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": kafka_options.get("acks", "all"),
            "compression.type": kafka_options.get("compression", "lz4"),
            "linger.ms": int(kafka_options.get("linger_ms", DEFAULT_LINGER_MS)),
            "message.timeout.ms": int(kafka_options.get("message_timeout_ms", DEFAULT_MESSAGE_TIMEOUT_MS)),
        })

    def send(self, messages: list[OutgoingMessage]) -> list[bool]:
        """Produce the messages and wait for the broker's acknowledgements."""
        delivered = [False] * len(messages)
        for index, message in enumerate(messages):
            try:
                self.producer.produce(
                    message.topic,
                    value=message.payload,
                    key=message.key.encode(),
                    on_delivery=_delivery_callback(delivered, index),
                )
            except BufferError:
                # the local producer queue is full: the rest stays pending for the next round
                self.producer.poll(1)
                break
            self.producer.poll(0)
        self.producer.flush(self.flush_timeout)
        return delivered

    def close(self) -> None:
        """Flush messages still in the producer queue."""
        self.producer.flush(self.flush_timeout)


def build_sink(sink_settings: SinkSettings, client_id: str) -> Sink:
    """Return the sink adapter selected in the settings."""
    if sink_settings.type is SinkType.KAFKA:
        return KafkaSink(sink_settings, client_id)
    if sink_settings.type is SinkType.JSONL:
        return JsonlSink(sink_settings.path, sink_settings.max_bytes)
    if sink_settings.type is SinkType.STDOUT:
        return StdoutSink()
    return NullSink()


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _to_json_line(message: OutgoingMessage) -> bytes:
    """Wrap a message payload (already JSON) with its topic and key, as one line."""
    topic = json.dumps(message.topic).encode()
    key = json.dumps(message.key).encode()
    return b'{"topic":%s,"key":%s,"message":%s}\n' % (topic, key, message.payload)


def _delivery_callback(delivered: list[bool], index: int) -> Callable[[Any, Any], None]:
    """Return a Kafka delivery callback that marks message ``index`` as delivered on success."""
    def on_delivery(error: Any, _message: Any) -> None:
        delivered[index] = error is None
    return on_delivery
