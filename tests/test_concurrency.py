"""
Concurrency and OCC tests — the Phase 1 gate.

Tests 1-10 use InMemoryEventStore (logic validation).
Test 11 (double-decision) and Test 12 (retry) are the critical gate tests.
"""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    FraudScreeningCompleted,
    OptimisticConcurrencyError,
    StreamArchivedError,
    StreamNotFoundError,
)


# -------------------------------------------------------------------------
# Basic append tests
# -------------------------------------------------------------------------


class TestAppendNewStream:
    async def test_creates_stream_returns_version(self, store, sample_events):
        """expected_version=-1 creates stream, returns version matching event count."""
        version = await store.append("loan-001", sample_events[:1], expected_version=-1)
        assert version == 1

    async def test_multiple_events_returns_correct_version(self, store, sample_events):
        """Appending N events returns version N."""
        version = await store.append("loan-001", sample_events, expected_version=-1)
        assert version == 3


class TestAppendExistingStream:
    async def test_sequential_appends_increment_version(self, store, sample_events):
        """Sequential appends correctly increment the version."""
        v1 = await store.append("loan-001", sample_events[:1], expected_version=-1)
        assert v1 == 1

        v2 = await store.append("loan-001", sample_events[1:2], expected_version=1)
        assert v2 == 2

        v3 = await store.append("loan-001", sample_events[2:3], expected_version=2)
        assert v3 == 3


# -------------------------------------------------------------------------
# OCC tests
# -------------------------------------------------------------------------


class TestOCCWrongVersion:
    async def test_wrong_version_raises(self, store, sample_events):
        """expected_version mismatch raises OptimisticConcurrencyError."""
        await store.append("loan-001", sample_events[:1], expected_version=-1)

        with pytest.raises(OptimisticConcurrencyError) as exc_info:
            await store.append("loan-001", sample_events[1:2], expected_version=5)

        assert exc_info.value.stream_id == "loan-001"
        assert exc_info.value.expected_version == 5
        assert exc_info.value.actual_version == 1


class TestOCCNonexistentStream:
    async def test_nonexistent_stream_wrong_version_raises(self, store, sample_events):
        """expected_version != -1 on missing stream → OCC(expected=N, actual=0)."""
        with pytest.raises(OptimisticConcurrencyError) as exc_info:
            await store.append("loan-999", sample_events[:1], expected_version=3)

        assert exc_info.value.expected_version == 3
        assert exc_info.value.actual_version == 0


class TestAppendAfterArchive:
    async def test_append_to_archived_stream_raises(self, store, sample_events):
        """Append to archived stream raises StreamArchivedError."""
        await store.append("loan-001", sample_events[:1], expected_version=-1)
        await store.archive_stream("loan-001")

        with pytest.raises(StreamArchivedError):
            await store.append("loan-001", sample_events[1:2], expected_version=1)


# -------------------------------------------------------------------------
# Load tests
# -------------------------------------------------------------------------


class TestLoadStreamOrdering:
    async def test_events_in_stream_position_order(self, store, sample_events):
        """Events returned in stream_position order."""
        await store.append("loan-001", sample_events, expected_version=-1)
        events = await store.load_stream("loan-001")

        positions = [e.stream_position for e in events]
        assert positions == [1, 2, 3]

    async def test_event_types_preserved(self, store, sample_events):
        """Event types are correctly stored and retrieved."""
        await store.append("loan-001", sample_events, expected_version=-1)
        events = await store.load_stream("loan-001")

        types = [e.event_type for e in events]
        assert types == [
            "ApplicationSubmitted",
            "CreditAnalysisRequested",
            "CreditAnalysisCompleted",
        ]


class TestLoadStreamRange:
    async def test_from_position_filtering(self, store, sample_events):
        """from_position filters events correctly."""
        await store.append("loan-001", sample_events, expected_version=-1)
        events = await store.load_stream("loan-001", from_position=1)

        positions = [e.stream_position for e in events]
        assert positions == [2, 3]

    async def test_to_position_filtering(self, store, sample_events):
        """to_position filters events correctly."""
        await store.append("loan-001", sample_events, expected_version=-1)
        events = await store.load_stream("loan-001", from_position=0, to_position=2)

        positions = [e.stream_position for e in events]
        assert positions == [1, 2]


class TestLoadAllGlobalOrdering:
    async def test_cross_stream_global_position_order(self, store, sample_events):
        """Events across streams ordered by global_position."""
        await store.append("loan-001", sample_events[:1], expected_version=-1)
        await store.append("loan-002", sample_events[:1], expected_version=-1)
        await store.append("loan-001", sample_events[1:2], expected_version=1)

        events = []
        async for e in await store.load_all():
            events.append(e)

        assert len(events) == 3

        # Global positions must be monotonically increasing
        gpos = [e.global_position for e in events]
        assert gpos == sorted(gpos)
        assert len(set(gpos)) == 3  # all unique


class TestLoadAllTypeFilter:
    async def test_event_types_filter(self, store, sample_events):
        """event_types parameter filters correctly."""
        await store.append("loan-001", sample_events, expected_version=-1)

        events = []
        async for e in await store.load_all(event_types=["ApplicationSubmitted"]):
            events.append(e)

        assert len(events) == 1
        assert events[0].event_type == "ApplicationSubmitted"


# -------------------------------------------------------------------------
# Stream version & metadata tests
# -------------------------------------------------------------------------


class TestStreamVersion:
    async def test_returns_correct_version(self, store, sample_events):
        await store.append("loan-001", sample_events, expected_version=-1)
        assert await store.stream_version("loan-001") == 3

    async def test_returns_zero_for_nonexistent(self, store):
        assert await store.stream_version("loan-999") == 0


# -------------------------------------------------------------------------
# THE DOUBLE-DECISION TEST — Phase 1 gate
# -------------------------------------------------------------------------


class TestConcurrentDoubleAppend:
    async def test_exactly_one_succeeds(self, store):
        """
        Two agents both read version 3, both try to append.
        Exactly one must win. The other must get OptimisticConcurrencyError.
        Total events in stream = 4, not 5.
        """
        # Setup: create stream with 3 events
        for i in range(3):
            await store.append(
                "loan-001",
                [ApplicationSubmitted(payload={"application_id": "APP-001", "seq": i})],
                expected_version=i - 1 if i == 0 else i,
            )

        # Verify setup
        assert await store.stream_version("loan-001") == 3

        # Two concurrent appends at expected_version=3
        results = {"success": 0, "occ_error": 0}

        async def try_append(agent_id: str):
            try:
                event = CreditAnalysisCompleted(
                    payload={
                        "application_id": "APP-001",
                        "agent_id": agent_id,
                        "confidence_score": 0.85,
                    }
                )
                await store.append("loan-001", [event], expected_version=3)
                results["success"] += 1
            except OptimisticConcurrencyError:
                results["occ_error"] += 1

        # Spawn both tasks
        await asyncio.gather(
            try_append("credit-agent-01"),
            try_append("credit-agent-02"),
        )

        # Assertions
        assert results["success"] == 1, "Exactly one must succeed"
        assert results["occ_error"] == 1, "Exactly one must fail with OCC"
        assert await store.stream_version("loan-001") == 4, "Stream must be at version 4"

        # Verify the winner's event is at position 4
        events = await store.load_stream("loan-001")
        assert len(events) == 4
        assert events[-1].stream_position == 4


class TestOCCRetryAfterReload:
    async def test_retry_succeeds_after_reload(self, store):
        """
        Task A fails OCC → reloads stream → retries at new version → succeeds.
        Proves the real-world retry workflow.
        """
        # Setup: stream at version 2
        for i in range(2):
            await store.append(
                "loan-001",
                [ApplicationSubmitted(payload={"application_id": "APP-001"})],
                expected_version=i - 1 if i == 0 else i,
            )

        # Agent B appends at version 2 (succeeds)
        await store.append(
            "loan-001",
            [FraudScreeningCompleted(payload={"application_id": "APP-001", "fraud_score": 0.1})],
            expected_version=2,
        )

        # Agent A tries at stale version 2 → fails
        with pytest.raises(OptimisticConcurrencyError) as exc_info:
            await store.append(
                "loan-001",
                [CreditAnalysisCompleted(payload={"application_id": "APP-001"})],
                expected_version=2,
            )
        assert exc_info.value.actual_version == 3

        # Agent A reloads stream, gets new version
        new_version = await store.stream_version("loan-001")
        assert new_version == 3

        # Agent A retries at correct version → succeeds
        final_version = await store.append(
            "loan-001",
            [CreditAnalysisCompleted(payload={"application_id": "APP-001"})],
            expected_version=3,
        )
        assert final_version == 4

        # Stream has all 4 events
        events = await store.load_stream("loan-001")
        assert len(events) == 4
