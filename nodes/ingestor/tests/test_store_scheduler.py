"""SQLite store (change detection, outbox, life limits, backfill progress) and slot computation."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from mibel_ingestor.model import UTC, Observation, OutgoingMessage
from mibel_ingestor.scheduler import find_next_slot
from mibel_ingestor.store import Store

from conftest import MADRID

BASE_TIME = datetime(2026, 11, 11, 10, tzinfo=UTC)


def demand(minutes: int, value: float) -> Observation:
    """A demand observation ``minutes`` after the base time."""
    return Observation("demand_real", "ES", BASE_TIME + timedelta(minutes=minutes), value, "MW")


def message(payload: bytes) -> OutgoingMessage:
    """An outgoing message with a fixed topic and key."""
    return OutgoingMessage("topic", "key", payload)


def local(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    """A Madrid local datetime."""
    return datetime(year, month, day, hour, minute, tzinfo=MADRID)


# --- store ---
def test_change_detection() -> None:
    store = Store(":memory:")
    first = [demand(0, 1), demand(15, 2)]
    assert store.filter_changed("esios", first) == first
    store.remember_sent("esios", first)
    assert store.filter_changed("esios", [demand(0, 1), demand(15, 3), demand(30, 4)]) == [demand(15, 3), demand(30, 4)]
    assert store.filter_changed("other", first) == first  # memory is per source


def test_outbox_lifecycle_and_backoff() -> None:
    store = Store(":memory:")
    store.enqueue([message(b"1"), message(b"2")])
    delivered, failed = store.fetch_due(10)
    store.mark_delivered([delivered.id])
    store.mark_failed([failed], first_delay=60, max_delay=300)
    assert store.count_pending() == 1
    assert store.fetch_due(10) == []  # retried later, never forgotten


def test_purge_applies_life_limits() -> None:
    store = Store(":memory:")
    old_value = Observation("v", "ES", datetime(2020, 1, 1, tzinfo=UTC), 1, "MW")
    store.enqueue([message(b"x" * 1000)])
    store.remember_sent("esios", [old_value])
    time.sleep(0.01)
    dropped = store.purge(pending_ttl=0.001, seen_ttl=86400, quarantine_ttl=1, max_bytes=10**9)
    assert dropped == 1
    assert store.count_pending() == 0
    assert store.filter_changed("esios", [old_value])  # forgotten: older than seen_ttl


def test_purge_keeps_file_under_size_limit() -> None:
    store = Store(":memory:")
    store.enqueue([message(b"x" * 10_000) for _ in range(300)])
    dropped = store.purge(pending_ttl=3600, seen_ttl=86400, quarantine_ttl=60, max_bytes=1_000_000)
    assert dropped > 0
    assert store.count_pending() < 300


def test_backfill_progress() -> None:
    store = Store(":memory:")
    assert not store.is_backfill_done("omie", "day_ahead", "2026-01-01")
    store.mark_backfill_done("omie", "day_ahead", "2026-01-01")
    assert store.is_backfill_done("omie", "day_ahead", "2026-01-01")


# --- scheduler ---
def test_next_slot_alignment() -> None:
    assert find_next_slot(local(2026, 11, 11, 10, 3), 300, 60, MADRID) == local(2026, 11, 11, 10, 6)
    assert find_next_slot(local(2026, 11, 11, 10, 6), 300, 60, MADRID) == local(2026, 11, 11, 10, 11)
    assert find_next_slot(local(2026, 11, 11, 23, 59), 3600, 420, MADRID) == local(2026, 11, 12, 0, 7)


def test_next_slot_six_hours_and_dst() -> None:
    assert find_next_slot(local(2026, 11, 11, 7, 0), 6 * 3600, 1800, MADRID) == local(2026, 11, 11, 12, 30)
    # 25 Oct 2026: clocks go back; a daily 13:00 slot stays at 13:00 local time
    assert find_next_slot(local(2026, 10, 25, 1, 0), 86400, 13 * 3600, MADRID) == local(2026, 10, 25, 13, 0)
