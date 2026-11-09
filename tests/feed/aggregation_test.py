"""Forming data must never overwrite the authoritative closed-minute journal."""

from decimal import Decimal

import pytest

from core.market.models import MINUTE, DataError, MinuteBar
from engine.feed.aggregation import Rollups
from engine.feed.store import Store


def bar(i, *, volume="2", price="10"):
    value = Decimal(price)
    return MinuteBar("ETHUSDT", i * MINUTE, value, value, value, value, Decimal(volume))


def test_partial_is_ephemeral_and_final_rollup_is_idempotent(tmp_path):
    source = Store(tmp_path / "minutes.sqlite", ("ETHUSDT",), 0)
    updates = []
    rollups = Rollups(source, tmp_path / "rollups.sqlite", ["1m", "5m"], updates.append)
    for i in range(4):
        source.put([bar(i)], "rest", (i + 1) * MINUTE)
        rollups.update(bar(i), (i + 1) * MINUTE, is_closed=True)
    partial = bar(4, volume="1", price="11")
    result = rollups.update(partial, 4 * MINUTE + 1000)
    assert all(not value.is_closed and not value.allow_new_risk for value in result)
    assert result[1].volume == "9"
    assert len(list(source.read("ETHUSDT", 4 * MINUTE, 5 * MINUTE))) == 0
    assert rollups.db.execute("SELECT count(*) FROM rollups WHERE interval='5m'").fetchone()[0] == 0
    source.put([bar(4, price="11")], "websocket", 5 * MINUTE)
    final = rollups.update(bar(4, price="11"), 5 * MINUTE, is_closed=True, allow_new_risk=True)[1]
    assert final.is_closed and final.is_complete and final.allow_new_risk
    assert final.volume == "10" and final.close == "11"
    repeated = rollups.update(bar(4, price="11"), 5 * MINUTE, is_closed=True)[1]
    assert repeated.volume == "10" and repeated.revision > final.revision
    # A stale partial cannot reopen an already closed candle.
    assert rollups.update(partial, 5 * MINUTE) is None
    rollups.close()
    restored = Rollups(source, tmp_path / "rollups.sqlite", ["5m"])
    restored.recover(5 * MINUTE)
    assert restored.latest[("ETHUSDT", "5m")].volume == "10"
    assert not restored.latest[("ETHUSDT", "5m")].allow_new_risk
    restored.close()
    source.close()


def test_time_close_is_distinct_from_complete_coverage_and_backfill_repairs(tmp_path):
    source = Store(tmp_path / "minutes.sqlite", ("ETHUSDT",), 0)
    rollups = Rollups(source, tmp_path / "rollups.sqlite", ["5m"])
    source.put([bar(0), bar(4)], "rest", 5 * MINUTE)
    value = rollups.update(bar(4), 5 * MINUTE, is_closed=True, allow_new_risk=True)[0]
    assert value.is_closed and not value.is_complete and not value.allow_new_risk
    source.put([bar(i) for i in (1, 2, 3)], "rest", 5 * MINUTE)
    corrected = rollups.update(bar(3), 5 * MINUTE, is_closed=True)[0]
    assert corrected.is_complete and corrected.volume == "10"
    assert corrected.revision > value.revision and not corrected.allow_new_risk
    assert rollups.update(bar(10), 5 * MINUTE) is None
    with pytest.raises(DataError, match="future"):
        rollups.update(bar(10), 5 * MINUTE, is_closed=True)
    rollups.close()
    source.close()


def test_auto_rollup_reuses_complete_five_minutes_and_merges_live_tail_once(tmp_path):
    source = Store(tmp_path / "minutes.sqlite", ("ETHUSDT",), 0)
    path = tmp_path / "rollups.sqlite"
    rollups = Rollups(source, path, ["1m", "5m", "15m"])
    for i in range(13):
        source.put([bar(i)], "rest", (i + 1) * MINUTE)
        rollups.update(bar(i), (i + 1) * MINUTE, is_closed=True)
    live = rollups.update(bar(13, volume="1"), 13 * MINUTE + 1000)[-1]
    assert set(live.source_levels) == {"5m", "1m", "live_trades"}
    assert live.volume == "27" and not live.is_closed and not live.allow_new_risk
    repeated = rollups.update(bar(13, volume="1"), 13 * MINUTE + 2000)[-1]
    assert repeated.volume == "27"
    for i in (13, 14):
        source.put([bar(i)], "websocket", (i + 1) * MINUTE)
        final = rollups.update(bar(i), (i + 1) * MINUTE, is_closed=True)[-1]
    assert final.source_levels == ("5m",)
    assert final.is_complete and final.is_closed and final.volume == "30"
    rollups.close()
    recovered = Rollups(source, path, ["15m"])
    recovered.recover(15 * MINUTE)
    value = recovered.latest[("ETHUSDT", "15m")]
    assert value.volume == "30" and value.source_levels == ("5m",)
    assert not value.allow_new_risk
    recovered.close()
    source.close()


def test_auto_rollup_falls_back_when_lower_rollup_is_incomplete(tmp_path):
    source = Store(tmp_path / "minutes.sqlite", ("ETHUSDT",), 0)
    rollups = Rollups(source, tmp_path / "rollups.sqlite", ["5m", "15m"])
    source.put([bar(i) for i in range(15) if i != 2], "rest", 15 * MINUTE)
    value = rollups.update(bar(4), 15 * MINUTE, is_closed=True)[0]
    assert not value.is_complete
    aggregate = rollups.update(bar(14), 15 * MINUTE, is_closed=True)[-1]
    assert aggregate.volume == "28" and not aggregate.is_complete
    assert "1m" in aggregate.source_levels
    source.put([bar(2)], "rest", 15 * MINUTE)
    repaired = rollups.update(bar(2), 15 * MINUTE, is_closed=True)[-1]
    assert repaired.is_complete and repaired.volume == "30"
    assert not repaired.allow_new_risk
    rollups.close()
    source.close()
