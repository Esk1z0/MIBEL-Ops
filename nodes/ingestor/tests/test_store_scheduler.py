from __future__ import annotations

import time
from datetime import datetime, timedelta

from mibel_ingestor.model import UTC, Observation
from mibel_ingestor.scheduler import next_slot
from mibel_ingestor.store import Store

from conftest import TZ


def obs(minutes: int, value: float) -> Observation:
    return Observation("demand_real", "ES", datetime(2026, 11, 11, 10, tzinfo=UTC) + timedelta(minutes=minutes),
                       value, "MW")


# --------------------------------------------------------------------------------- store
def test_change_detection() -> None:
    s = Store(":memory:")
    first = [obs(0, 1), obs(15, 2)]
    assert s.changed("esios", first) == first
    s.remember("esios", first)
    assert s.changed("esios", [obs(0, 1), obs(15, 3), obs(30, 4)]) == [obs(15, 3), obs(30, 4)]
    assert s.changed("other", first) == first                      # memory is per source


def test_outbox_lifecycle_and_backoff() -> None:
    s = Store(":memory:")
    s.enqueue([("t", "k", b"1"), ("t", "k", b"2")])
    a, b = s.due(10)
    s.delivered([a.id])
    s.failed([b], base=60, cap=300)
    assert s.pending() == 1 and s.due(10) == []                    # retried later, never forgotten


def test_purge_applies_short_life(tmp_path: object) -> None:
    s = Store(":memory:")
    s.enqueue([("t", "k", b"x" * 1000)])
    s.remember("esios", [Observation("v", "ES", datetime(2020, 1, 1, tzinfo=UTC), 1, "MW")])
    time.sleep(0.01)
    dropped = s.purge(pending_ttl=0.001, seen_ttl=86400, quarantine_ttl=1, max_bytes=10**9)
    assert dropped == 1 and s.pending() == 0
    assert s.changed("esios", [Observation("v", "ES", datetime(2020, 1, 1, tzinfo=UTC), 1, "MW")])


def test_purge_keeps_file_under_size_limit() -> None:
    s = Store(":memory:")
    s.enqueue([("t", "k", b"x" * 10_000) for _ in range(300)])
    dropped = s.purge(pending_ttl=3600, seen_ttl=86400, quarantine_ttl=60, max_bytes=1_000_000)
    assert dropped > 0 and s.pending() < 300


def test_backfill_progress() -> None:
    s = Store(":memory:")
    assert not s.backfill_is_done("omie", "day_ahead", "2026-01-01")
    s.backfill_mark("omie", "day_ahead", "2026-01-01")
    assert s.backfill_is_done("omie", "day_ahead", "2026-01-01")


# ----------------------------------------------------------------------------- scheduler
def local(*args: int) -> datetime:
    return datetime(*args, tzinfo=TZ)


def test_next_slot_alignment() -> None:
    assert next_slot(local(2026, 11, 11, 10, 3), 300, 60, TZ) == local(2026, 11, 11, 10, 6)
    assert next_slot(local(2026, 11, 11, 10, 6), 300, 60, TZ) == local(2026, 11, 11, 10, 11)
    assert next_slot(local(2026, 11, 11, 23, 59), 3600, 420, TZ) == local(2026, 11, 12, 0, 7)


def test_next_slot_six_hours_and_dst() -> None:
    assert next_slot(local(2026, 11, 11, 7, 0), 6 * 3600, 1800, TZ) == local(2026, 11, 11, 12, 30)
    # 25 Oct 2026: clocks go back; a daily 13:00 slot stays at 13:00 local time
    assert next_slot(local(2026, 10, 25, 1, 0), 86400, 13 * 3600, TZ) == local(2026, 10, 25, 13, 0)
