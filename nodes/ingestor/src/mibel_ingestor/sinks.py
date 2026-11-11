"""Sink port and adapters. A sink receives (topic, key, payload) messages and reports which ones were
delivered; the deliverer deletes those from the outbox and retries the rest with backoff, forever,
while they are inside ``pending_ttl``.

- ``kafka``:  production (pro). ``confluent-kafka`` is imported only when this sink is selected.
- ``jsonl``:  one JSON line per message in the state directory (test mode while Kafka is not up).
- ``stdout``: human-readable summary per message (dev).
- ``null``:   discards everything (benchmarks).
"""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from typing import Any, Protocol

from .settings import SinkSettings


class Sink(Protocol):
    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]: ...
    def close(self) -> None: ...


class NullSink:
    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]:
        return [True] * len(messages)

    def close(self) -> None:
        pass


class StdoutSink:
    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]:
        for topic, key, payload in messages:
            doc = json.loads(payload)
            rows = len(doc.get("rows", [])) if "rows" in doc else doc.get("count", 0)
            print(f"[{topic}] {key} · {doc.get('contract')} · {rows} rows", file=sys.stdout, flush=True)
        return [True] * len(messages)

    def close(self) -> None:
        pass


class JsonlSink:
    """Appends ``{"topic", "key", "message"}`` lines; rotates once to ``.1`` above ``max_bytes``."""

    def __init__(self, path: Path, max_bytes: int) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]:
        with self._lock:
            if self.path.exists() and self.path.stat().st_size > self.max_bytes:
                self.path.replace(self.path.with_suffix(self.path.suffix + ".1"))
            with self.path.open("ab") as fh:
                for topic, key, payload in messages:
                    fh.write(b'{"topic":%s,"key":%s,"message":%s}\n'
                             % (json.dumps(topic).encode(), json.dumps(key).encode(), payload))
        return [True] * len(messages)

    def close(self) -> None:
        pass


class KafkaSink:
    def __init__(self, cfg: SinkSettings, client_id: str) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise RuntimeError("kafka sink needs the 'kafka' extra: pip install 'mibel-ingestor[kafka]'") from None
        conf: dict[str, Any] = {
            "bootstrap.servers": cfg.bootstrap, "client.id": client_id,
            "enable.idempotence": True, "acks": cfg.kafka.get("acks", "all"),
            "compression.type": cfg.kafka.get("compression", "lz4"),
            "linger.ms": int(cfg.kafka.get("linger_ms", 50)),
            "message.timeout.ms": int(cfg.kafka.get("message_timeout_ms", 30000)),
        }
        self.timeout = float(cfg.kafka.get("flush_timeout_s", 30))
        self.producer = Producer(conf)

    def send(self, messages: list[tuple[str, str, bytes]]) -> list[bool]:
        results = [False] * len(messages)

        def callback(index: int) -> Any:
            def done(err: Any, _msg: Any) -> None:
                results[index] = err is None
            return done

        for i, (topic, key, payload) in enumerate(messages):
            try:
                self.producer.produce(topic, value=payload, key=key.encode(), on_delivery=callback(i))
            except BufferError:
                self.producer.poll(1)
                break                                    # the rest stays pending for the next flush
            self.producer.poll(0)
        self.producer.flush(self.timeout)
        return results

    def close(self) -> None:
        self.producer.flush(self.timeout)


def build(cfg: SinkSettings, client_id: str) -> Sink:
    if cfg.type == "kafka":
        return KafkaSink(cfg, client_id)
    if cfg.type == "jsonl":
        return JsonlSink(cfg.path, cfg.max_bytes)
    if cfg.type == "stdout":
        return StdoutSink()
    if cfg.type == "null":
        return NullSink()
    raise ValueError(f"unknown sink type {cfg.type!r} (kafka | jsonl | stdout | null)")
