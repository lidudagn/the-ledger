"""
The Ledger — Phase 4: Audit Chain Tests

Covers:
  1. Basic chain valid (5 events → chain_valid=True)
  2. Chain continuation (second check links to first)
  3. Canonical hash determinism (key order doesn't matter)
  4. Full-event hash includes event_type & position
  5. Tamper detection (corrupted payload → tamper_detected=True)
  6. Empty stream (no events → valid with 0 verified)
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from in_memory_store import InMemoryEventStore
from models.events import BaseEvent
from integrity.audit_chain import (
    canonical_event_hash,
    compute_chain_hash,
    run_integrity_check,
    verify_chain_integrity,
    GENESIS_HASH,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store():
    return InMemoryEventStore()


async def _seed_loan_events(store, app_id: str = "LOAN-001", count: int = 5) -> int:
    """Seed N events into loan-{app_id} stream."""
    version = -1
    for i in range(count):
        event = BaseEvent(
            event_type="ApplicationSubmitted" if i == 0 else "CreditAnalysisCompleted",
            event_version=1,
            payload={
                "application_id": app_id,
                "sequence": i + 1,
                "data": f"event-{i+1}-data",
            },
        )
        version = await store.append(
            f"loan-{app_id}", [event], expected_version=version if version > 0 else -1 if i == 0 else version
        )
    return version


# =============================================================================
# 1. Basic Chain Valid
# =============================================================================


class TestBasicChainValid:

    @pytest.mark.asyncio
    async def test_chain_valid_five_events(self, store):
        """5 events → events_verified=5, chain_valid=True."""
        await _seed_loan_events(store, "LOAN-001", count=5)

        result = await run_integrity_check(store, "loan", "LOAN-001")

        assert result.events_verified == 5
        assert result.chain_valid is True
        assert result.tamper_detected is False
        assert result.previous_hash == GENESIS_HASH
        assert len(result.integrity_hash) == 64  # SHA-256 hex digest
        assert len(result.event_hashes) == 5

    @pytest.mark.asyncio
    async def test_chain_empty_stream(self, store):
        """No events in stream → valid with 0 verified."""
        # Don't seed any events — just create the stream entry
        result = await run_integrity_check(store, "loan", "LOAN-EMPTY")

        assert result.events_verified == 0
        assert result.chain_valid is True
        assert result.tamper_detected is False


# =============================================================================
# 2. Chain Continuation
# =============================================================================


class TestChainContinuation:

    @pytest.mark.asyncio
    async def test_second_check_links_to_first(self, store):
        """Two checks → second.previous_hash == first.integrity_hash."""
        await _seed_loan_events(store, "LOAN-002", count=3)

        first = await run_integrity_check(store, "loan", "LOAN-002")
        assert first.events_verified == 3
        assert first.previous_hash == GENESIS_HASH

        # Add more events
        version = await store.stream_version("loan-LOAN-002")
        event = BaseEvent(
            event_type="ComplianceCheckCompleted",
            event_version=1,
            payload={"application_id": "LOAN-002", "overall_verdict": "CLEAR"},
        )
        await store.append("loan-LOAN-002", [event], expected_version=version)

        # Second check
        second = await run_integrity_check(store, "loan", "LOAN-002")

        assert second.previous_hash == first.integrity_hash
        assert second.events_verified == 1  # only new events
        assert second.chain_valid is True
        assert second.integrity_hash != first.integrity_hash


# =============================================================================
# 3. Canonical Hash Determinism
# =============================================================================


class TestCanonicalHash:

    def test_key_order_independent(self):
        """Same payload with different key order → same hash."""
        h1 = canonical_event_hash({
            "event_type": "Test",
            "event_version": 1,
            "payload": {"b": 2, "a": 1},
            "recorded_at": "2026-01-01T00:00:00+00:00",
            "stream_position": 1,
        })
        h2 = canonical_event_hash({
            "event_type": "Test",
            "event_version": 1,
            "payload": {"a": 1, "b": 2},
            "recorded_at": "2026-01-01T00:00:00+00:00",
            "stream_position": 1,
        })
        assert h1 == h2

    def test_different_type_different_hash(self):
        """Changing event_type alone → different hash."""
        base = {
            "event_version": 1,
            "payload": {"data": True},
            "recorded_at": "2026-01-01T00:00:00+00:00",
            "stream_position": 1,
        }
        h1 = canonical_event_hash({**base, "event_type": "TypeA"})
        h2 = canonical_event_hash({**base, "event_type": "TypeB"})
        assert h1 != h2

    def test_different_position_different_hash(self):
        """Same payload at different positions → different hash."""
        base = {
            "event_type": "Test",
            "event_version": 1,
            "payload": {"data": True},
            "recorded_at": "2026-01-01T00:00:00+00:00",
        }
        h1 = canonical_event_hash({**base, "stream_position": 1})
        h2 = canonical_event_hash({**base, "stream_position": 2})
        assert h1 != h2


# =============================================================================
# 4. Tamper Detection
# =============================================================================


class TestTamperDetection:

    @pytest.mark.asyncio
    async def test_tamper_payload_detected(self, store):
        """Corrupt payload between checks → tamper_detected=True."""
        await _seed_loan_events(store, "LOAN-003", count=3)

        # First check — establishes baseline
        first = await run_integrity_check(store, "loan", "LOAN-003")
        assert first.chain_valid is True

        # Tamper: modify an event's payload directly in the store
        stream = store._streams["loan-LOAN-003"]
        original = stream[1]  # second event
        tampered = original.model_copy(update={
            "payload": {**original.payload, "tampered": True},
        })
        stream[1] = tampered

        # Full verification should detect tampering
        verify_result = await verify_chain_integrity(store, "loan", "LOAN-003")
        assert verify_result.tamper_detected is True
        assert verify_result.chain_valid is False

    @pytest.mark.asyncio
    async def test_tamper_event_type_detected(self, store):
        """Change event_type between checks → tamper_detected=True."""
        await _seed_loan_events(store, "LOAN-004", count=3)

        first = await run_integrity_check(store, "loan", "LOAN-004")
        assert first.chain_valid is True

        # Tamper: change event_type
        stream = store._streams["loan-LOAN-004"]
        original = stream[0]
        tampered = original.model_copy(update={"event_type": "FakeEventType"})
        stream[0] = tampered

        verify_result = await verify_chain_integrity(store, "loan", "LOAN-004")
        assert verify_result.tamper_detected is True
        assert verify_result.chain_valid is False


# =============================================================================
# 5. Chain Hash Computation
# =============================================================================


class TestChainHashComputation:

    def test_genesis_chain(self):
        """Chain from genesis with known hashes."""
        event_hashes = ["aaa", "bbb", "ccc"]
        result = compute_chain_hash(GENESIS_HASH, event_hashes)
        assert len(result) == 64  # SHA-256
        # Same input → same output (deterministic)
        assert result == compute_chain_hash(GENESIS_HASH, event_hashes)

    def test_different_previous_hash_different_result(self):
        """Different previous_hash → different chain hash."""
        h1 = compute_chain_hash("hash_a", ["hash1", "hash2"])
        h2 = compute_chain_hash("hash_b", ["hash1", "hash2"])
        assert h1 != h2
