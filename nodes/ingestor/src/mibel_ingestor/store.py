"""Local SQLite state (one file, WAL mode, one connection guarded by a lock).

Tables:
- ``outbox``: curated batches waiting for delivery. Delivered rows are deleted at once; undelivered
  rows live at most ``pending_ttl`` and the file is kept under ``max_bytes`` (oldest dropped first).
  Only the manager keeps data for real (ADR-05): sources can be backfilled if a gap appears.
- ``seen``: last value sent per (source, variable, node, event_time) — change detection, so a value
  polled every 5 min is sent once and only revisions or new forecast vintages travel again.
- ``quarantine``: rejected observations with their reason, kept for ``quarantine_ttl``.
- ``capacity``: installed capacity per technology (DG-E11).
- ``backfill_done``: completed (source, dataset, day) chunks, so a backfill can resume.
No raw payloads are stored.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .model import UTC, Observation, iso

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS outbox(
    id INTEGER PRIMARY KEY, topic TEXT NOT NULL, key TEXT NOT NULL, payload BLOB NOT NULL,
    created REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_try REAL NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS outbox_next ON outbox(next_try);
CREATE TABLE IF NOT EXISTS seen(
    source TEXT, variable TEXT, node TEXT, event_time TEXT, value REAL, seen_at REAL,
    PRIMARY KEY(source, variable, node, event_time)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS quarantine(
    id INTEGER PRIMARY KEY, created REAL, source TEXT, dataset TEXT, rule TEXT,
    variable TEXT, node TEXT, event_time TEXT, value REAL);
CREATE TABLE IF NOT EXISTS capacity(technology TEXT PRIMARY KEY, mw REAL, updated REAL);
CREATE TABLE IF NOT EXISTS backfill_done(
    source TEXT, dataset TEXT, day TEXT, PRIMARY KEY(source, dataset, day)) WITHOUT ROWID;
"""


@dataclass(frozen=True)
class Pending:
    id: int
    topic: str
    key: str
    payload: bytes
    attempts: int


class Store:
    def __init__(self, path: Path | str) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # -- outbox -----------------------------------------------------------------------------
    def enqueue(self, messages: Iterable[tuple[str, str, bytes]]) -> int:
        now = time.time()
        rows = [(t, k, p, now) for t, k, p in messages]
        with self._lock:
            self._db.executemany("INSERT INTO outbox(topic, key, payload, created) VALUES (?,?,?,?)", rows)
        return len(rows)

    def due(self, limit: int) -> list[Pending]:
        with self._lock:
            cur = self._db.execute("SELECT id, topic, key, payload, attempts FROM outbox WHERE next_try <= ? "
                                   "ORDER BY id LIMIT ?", (time.time(), limit))
            return [Pending(*row) for row in cur.fetchall()]

    def delivered(self, ids: list[int]) -> None:
        if ids:
            with self._lock:
                self._db.executemany("DELETE FROM outbox WHERE id = ?", [(i,) for i in ids])

    def failed(self, items: list[Pending], base: float, cap: float) -> None:
        now = time.time()
        rows = [(min(cap, base * 2 ** p.attempts) + now, p.id) for p in items]
        with self._lock:
            self._db.executemany("UPDATE outbox SET attempts = attempts + 1, next_try = ? WHERE id = ?", rows)

    def pending(self) -> int:
        with self._lock:
            return int(self._db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    # -- change detection ---------------------------------------------------------------------
    def changed(self, source: str, observations: list[Observation]) -> list[Observation]:
        """Observations whose value differs from the last one sent (or never sent)."""
        if not observations:
            return []
        lo = iso(min(o.event_time for o in observations))
        hi = iso(max(o.event_time for o in observations))
        variables = sorted({o.variable for o in observations})
        marks = ",".join("?" * len(variables))
        with self._lock:
            cur = self._db.execute(
                f"SELECT variable, node, event_time, value FROM seen WHERE source = ? AND variable IN ({marks}) "
                "AND event_time BETWEEN ? AND ?", (source, *variables, lo, hi))
            last = {(v, n, t): val for v, n, t, val in cur.fetchall()}
        return [o for o in observations if last.get((o.variable, o.node, iso(o.event_time))) != o.value]

    def remember(self, source: str, observations: list[Observation]) -> None:
        now = time.time()
        rows = [(source, o.variable, o.node, iso(o.event_time), o.value, now) for o in observations]
        with self._lock:
            self._db.executemany("INSERT OR REPLACE INTO seen VALUES (?,?,?,?,?,?)", rows)

    # -- quarantine / capacity / backfill -----------------------------------------------------
    def quarantine(self, source: str, dataset: str, rejected: list[tuple[Observation, str]]) -> None:
        now = time.time()
        rows = [(now, source, dataset, rule, o.variable, o.node, iso(o.event_time), o.value) for o, rule in rejected]
        with self._lock:
            self._db.executemany("INSERT INTO quarantine(created, source, dataset, rule, variable, node, "
                                 "event_time, value) VALUES (?,?,?,?,?,?,?,?)", rows)

    def set_capacity(self, values: dict[str, float]) -> None:
        now = time.time()
        with self._lock:
            self._db.executemany("INSERT OR REPLACE INTO capacity VALUES (?,?,?)",
                                 [(k, v, now) for k, v in values.items()])

    def capacities(self) -> dict[str, float]:
        with self._lock:
            return {k: float(v) for k, v in self._db.execute("SELECT technology, mw FROM capacity")}

    def backfill_is_done(self, source: str, dataset: str, day: str) -> bool:
        with self._lock:
            return self._db.execute("SELECT 1 FROM backfill_done WHERE source=? AND dataset=? AND day=?",
                                    (source, dataset, day)).fetchone() is not None

    def backfill_mark(self, source: str, dataset: str, day: str) -> None:
        with self._lock:
            self._db.execute("INSERT OR IGNORE INTO backfill_done VALUES (?,?,?)", (source, dataset, day))

    # -- housekeeping -------------------------------------------------------------------------
    def purge(self, *, pending_ttl: float, seen_ttl: float, quarantine_ttl: float, max_bytes: int) -> int:
        """Applies the life limits of every table. Returns how many undelivered messages were dropped."""
        now = time.time()
        with self._lock:
            dropped = self._db.execute("DELETE FROM outbox WHERE created < ?", (now - pending_ttl,)).rowcount
            # by event_time: a value inside a live window is never forgotten while it can still be polled
            self._db.execute("DELETE FROM seen WHERE event_time < ?",
                             (iso(datetime.fromtimestamp(now - seen_ttl, UTC)),))
            self._db.execute("DELETE FROM quarantine WHERE created < ?", (now - quarantine_ttl,))
            size = self._size()
            while size > max_bytes:
                n = self._db.execute("DELETE FROM outbox WHERE id IN (SELECT id FROM outbox ORDER BY id LIMIT 100)"
                                     ).rowcount
                if n == 0:
                    break
                dropped += n
                size = self._size()
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return dropped

    def _size(self) -> int:
        """Bytes used by live pages (free pages excluded, so deletes are reflected without VACUUM)."""
        page = self._db.execute("PRAGMA page_size").fetchone()[0]
        count = self._db.execute("PRAGMA page_count").fetchone()[0]
        free = self._db.execute("PRAGMA freelist_count").fetchone()[0]
        return int(page * (count - free))
