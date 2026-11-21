"""Local SQLite state of the node: one file, WAL mode, one connection guarded by a lock.

Tables:

- ``outbox``: curated batches waiting for delivery. Delivered rows are deleted at once; undelivered
  rows live at most ``pending_ttl`` and the file is kept under ``max_bytes``, oldest dropped first
  (NFR-03: 30 days / 1 GB). Only the manager keeps data for real (ADR-05); gaps can be backfilled.
- ``seen``: last value sent per (source, variable, node, event_time), for change detection: a value
  polled every 5 min is sent once, and it travels again only when its value changes (a revision, or a
  new forecast vintage with a different value; a vintage that repeats the value is not resent).
- ``quarantine``: rejected observations with their rule, kept for ``quarantine_ttl``.
- ``capacity``: installed capacity per technology, for DG-E11.
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
from types import TracebackType
from typing import NamedTuple

from .model import UTC, Observation, OutgoingMessage, Rejection, to_iso


# ##################################################################################################
# #######################################     CONSTANTS     ########################################
# ##################################################################################################


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
IN_MEMORY = ":memory:"
# Messages dropped per step when the file is above its size limit
SIZE_PURGE_STEP = 100


# ##################################################################################################
# #########################################     TYPES     ##########################################
# ##################################################################################################


@dataclass(frozen=True)
class PendingMessage:
    """An outbox row due for delivery."""

    id: int
    message: OutgoingMessage
    attempts: int


class SeenKey(NamedTuple):
    """Identity of a value for change detection.

    A NamedTuple rather than a frozen dataclass: it is built and hashed once per observation on every
    poll, and tuple hashing is several times cheaper (measured with benchmarks/bench.py).
    """

    variable: str
    node: str
    event_time: str


# ##################################################################################################
# #######################################     PUBLIC API     #######################################
# ##################################################################################################


class Store:
    """Thread-safe access to the node's SQLite file (or an in-memory database for tests)."""

    def __init__(self, path: Path | str) -> None:
        if str(path) != IN_MEMORY:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(SCHEMA)

    def close(self) -> None:
        """Close the database connection (closing twice is harmless)."""
        with self._lock:
            self._db.close()

    def __enter__(self) -> Store:
        """Use the store in a ``with`` block, which closes it at the end."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection: Python 3.13+ warns (ResourceWarning) about SQLite connections left open."""
        self.close()

    # --- outbox ---
    def enqueue(self, messages: Iterable[OutgoingMessage]) -> int:
        """Store messages for delivery and return how many were stored."""
        now = time.time()
        rows = [(message.topic, message.key, message.payload, now) for message in messages]
        with self._lock:
            self._db.executemany("INSERT INTO outbox(topic, key, payload, created) VALUES (?,?,?,?)", rows)
        return len(rows)

    def fetch_due(self, limit: int) -> list[PendingMessage]:
        """Return up to ``limit`` messages whose next attempt is due, oldest first."""
        with self._lock:
            cursor = self._db.execute(
                "SELECT id, topic, key, payload, attempts FROM outbox WHERE next_try <= ? ORDER BY id LIMIT ?",
                (time.time(), limit),
            )
            rows = cursor.fetchall()
        return [_to_pending(row) for row in rows]

    def mark_delivered(self, ids: list[int]) -> None:
        """Delete delivered messages: the node keeps nothing once the manager has it."""
        if not ids:
            return
        with self._lock:
            self._db.executemany("DELETE FROM outbox WHERE id = ?", [(message_id,) for message_id in ids])

    def mark_failed(self, pending: list[PendingMessage], first_delay: float, max_delay: float) -> None:
        """Schedule failed messages again with exponential backoff (``first_delay × 2^attempts``, capped)."""
        now = time.time()
        rows = []
        for item in pending:
            delay = min(max_delay, first_delay * 2**item.attempts)
            rows.append((now + delay, item.id))
        with self._lock:
            self._db.executemany("UPDATE outbox SET attempts = attempts + 1, next_try = ? WHERE id = ?", rows)

    def count_pending(self) -> int:
        """Return how many messages are waiting for delivery."""
        with self._lock:
            return int(self._db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    # --- change detection ---
    def filter_changed(self, source: str, observations: list[Observation]) -> list[Observation]:
        """Return the observations whose value differs from the last one sent (or was never sent)."""
        if not observations:
            return []
        last_sent = self._last_sent_values(source, observations)
        changed = []
        for observation in observations:
            key = SeenKey(observation.variable, observation.node, to_iso(observation.event_time))
            if last_sent.get(key) != observation.value:
                changed.append(observation)
        return changed

    def remember_sent(self, source: str, observations: list[Observation]) -> None:
        """Record the values just sent, for the next change detection."""
        now = time.time()
        rows = []
        for observation in observations:
            rows.append((source, observation.variable, observation.node, to_iso(observation.event_time),
                         observation.value, now))
        with self._lock:
            self._db.executemany("INSERT OR REPLACE INTO seen VALUES (?,?,?,?,?,?)", rows)

    # --- quarantine, capacity and backfill progress ---
    def quarantine(self, source: str, dataset: str, rejections: list[Rejection]) -> None:
        """Keep rejected observations locally for inspection (they also go to the DLQ)."""
        now = time.time()
        rows = []
        for rejection in rejections:
            observation = rejection.observation
            rows.append((now, source, dataset, rejection.rule, observation.variable, observation.node,
                         to_iso(observation.event_time), observation.value))
        with self._lock:
            self._db.executemany(
                "INSERT INTO quarantine(created, source, dataset, rule, variable, node, event_time, value) "
                "VALUES (?,?,?,?,?,?,?,?)",
                rows,
            )

    def set_capacities(self, megawatts_by_technology: dict[str, float]) -> None:
        """Store the latest installed capacity per technology (DG-E11)."""
        now = time.time()
        rows = [(technology, megawatts, now) for technology, megawatts in megawatts_by_technology.items()]
        with self._lock:
            self._db.executemany("INSERT OR REPLACE INTO capacity VALUES (?,?,?)", rows)

    def load_capacities(self) -> dict[str, float]:
        """Return the installed capacity per technology, in MW."""
        with self._lock:
            rows = self._db.execute("SELECT technology, mw FROM capacity").fetchall()
        return {technology: float(megawatts) for technology, megawatts in rows}

    def is_backfill_done(self, source: str, dataset: str, day: str) -> bool:
        """Whether a backfill day was already completed."""
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM backfill_done WHERE source=? AND dataset=? AND day=?",
                (source, dataset, day),
            ).fetchone()
        return row is not None

    def mark_backfill_done(self, source: str, dataset: str, day: str) -> None:
        """Record a completed backfill day, so a resumed backfill skips it."""
        with self._lock:
            self._db.execute("INSERT OR IGNORE INTO backfill_done VALUES (?,?,?)", (source, dataset, day))

    # --- housekeeping ---
    def purge(self, *, pending_ttl: float, seen_ttl: float, quarantine_ttl: float, max_bytes: int) -> int:
        """Apply the life limits of every table and return how many undelivered messages were dropped."""
        now = time.time()
        with self._lock:
            dropped = self._db.execute("DELETE FROM outbox WHERE created < ?", (now - pending_ttl,)).rowcount
            # `seen` is purged by event_time, not by seen_at: a value that can still be polled inside a
            # live window must never be forgotten, or it would be sent again as if it were new
            oldest_event = to_iso(datetime.fromtimestamp(now - seen_ttl, UTC))
            self._db.execute("DELETE FROM seen WHERE event_time < ?", (oldest_event,))
            self._db.execute("DELETE FROM quarantine WHERE created < ?", (now - quarantine_ttl,))
            dropped += self._drop_oldest_until_below(max_bytes)
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return dropped

    # --- private ---
    def _last_sent_values(self, source: str, observations: list[Observation]) -> dict[SeenKey, float]:
        """Return the last value sent for the variables and time span of ``observations``."""
        first_event = to_iso(min(observation.event_time for observation in observations))
        last_event = to_iso(max(observation.event_time for observation in observations))
        variables = sorted({observation.variable for observation in observations})
        placeholders = ",".join("?" * len(variables))
        with self._lock:
            cursor = self._db.execute(
                f"SELECT variable, node, event_time, value FROM seen WHERE source = ? "
                f"AND variable IN ({placeholders}) AND event_time BETWEEN ? AND ?",
                (source, *variables, first_event, last_event),
            )
            rows = cursor.fetchall()
        last_sent = {}
        for variable, node, event_time, value in rows:
            last_sent[SeenKey(variable, node, event_time)] = value
        return last_sent

    def _drop_oldest_until_below(self, max_bytes: int) -> int:
        """Drop the oldest outbox rows until the file is under ``max_bytes``; the caller holds the lock."""
        dropped = 0
        while self._used_bytes() > max_bytes:
            removed = self._db.execute(
                "DELETE FROM outbox WHERE id IN (SELECT id FROM outbox ORDER BY id LIMIT ?)",
                (SIZE_PURGE_STEP,),
            ).rowcount
            if removed == 0:
                break
            dropped += removed
        return dropped

    def _used_bytes(self) -> int:
        """Return the bytes used by live pages (free pages excluded, so deletes count without VACUUM)."""
        page_size = self._db.execute("PRAGMA page_size").fetchone()[0]
        page_count = self._db.execute("PRAGMA page_count").fetchone()[0]
        free_pages = self._db.execute("PRAGMA freelist_count").fetchone()[0]
        return int(page_size * (page_count - free_pages))


# ##################################################################################################
# ####################################     PRIVATE HELPERS     #####################################
# ##################################################################################################


def _to_pending(row: tuple[int, str, str, bytes, int]) -> PendingMessage:
    """Build a pending message from an outbox row ``(id, topic, key, payload, attempts)``."""
    message_id, topic, key, payload, attempts = row
    return PendingMessage(id=message_id, message=OutgoingMessage(topic, key, payload), attempts=attempts)
