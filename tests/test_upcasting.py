"""
The Ledger — Phase 4: Upcasting Tests

Covers:
  1. Auto-upcast CreditAnalysisCompleted v1→v2 on load
  2. Auto-upcast DecisionGenerated v1→v2 on load
  3. Chain upcasting (v1→v2→v3)
  4. No upcaster → pass-through unchanged
  5. Loop guard (infinite chain → UpcastingError)
  6. Malformed payload handling
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from in_memory_store import InMemoryEventStore
from models.events import BaseEvent, StoredEvent
from upcasting.registry import UpcasterRegistry, UpcastingError, MAX_UPCAST_STEPS, LATEST_VERSIONS
from upcasters import registry as global_registry


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store():
    return InMemoryEventStore()


@pytest.fixture
def registry():
    """Fresh registry (not the global one) for isolated tests."""
    return UpcasterRegistry()


# =============================================================================
# 1. Auto-Upcast CreditAnalysisCompleted v1 → v2
# =============================================================================


class TestCreditAnalysisUpcast:
    """Verify CreditAnalysisCompleted v1 is transparently upcasted to v2."""

    @pytest.mark.asyncio
    async def test_auto_upcast_credit_v1_to_v2(self, store):
        """Store v1 CreditAnalysisCompleted, load via global registry → v2."""
        v1_event = BaseEvent(
            event_type="CreditAnalysisCompleted",
            event_version=1,
            payload={
                "application_id": "LOAN-001",
                "agent_id": "credit-agent-01",
                "risk_tier": "MEDIUM",
                "analysis_duration_ms": 1200,
            },
        )
        await store.append("loan-LOAN-001", [v1_event], expected_version=-1)

        # Load and upcast
        events = await store.load_stream("loan-LOAN-001")
        raw = events[0]

        # Apply upcaster
        upcasted = global_registry.upcast(raw)

        assert upcasted.event_version == 2
        assert upcasted.payload["model_version"] == "legacy-pre-2026"
        assert upcasted.payload["confidence_score"] is None
        assert upcasted.payload["regulatory_basis"] == "2025-Q4-GAAP"
        # Original fields preserved
        assert upcasted.payload["application_id"] == "LOAN-001"
        assert upcasted.payload["risk_tier"] == "MEDIUM"

    @pytest.mark.asyncio
    async def test_v2_event_not_upcasted(self, store):
        """v2 CreditAnalysisCompleted should pass through unchanged."""
        v2_event = BaseEvent(
            event_type="CreditAnalysisCompleted",
            event_version=2,
            payload={
                "application_id": "LOAN-002",
                "model_version": "v2.3",
                "confidence_score": 0.85,
            },
        )
        await store.append("loan-LOAN-002", [v2_event], expected_version=-1)

        events = await store.load_stream("loan-LOAN-002")
        upcasted = global_registry.upcast(events[0])

        # Version should be unchanged — already at ceiling
        assert upcasted.event_version == 2
        assert upcasted.payload["model_version"] == "v2.3"
        assert upcasted.payload["confidence_score"] == 0.85


# =============================================================================
# 2. Auto-Upcast DecisionGenerated v1 → v2
# =============================================================================


class TestDecisionGeneratedUpcast:
    """Verify DecisionGenerated v1 is transparently upcasted to v2."""

    @pytest.mark.asyncio
    async def test_auto_upcast_decision_v1_to_v2(self, store):
        """Store v1 DecisionGenerated, upcast → v2 with model_versions."""
        v1_event = BaseEvent(
            event_type="DecisionGenerated",
            event_version=1,
            payload={
                "application_id": "LOAN-001",
                "recommendation": "APPROVE",
                "contributing_agent_sessions": ["sess-001", "sess-002"],
            },
        )
        await store.append("loan-LOAN-001", [v1_event], expected_version=-1)

        events = await store.load_stream("loan-LOAN-001")
        upcasted = global_registry.upcast(events[0])

        assert upcasted.event_version == 2
        assert upcasted.payload["model_versions"] == {
            "sess-001": "legacy-pre-2026",
            "sess-002": "legacy-pre-2026",
        }
        assert upcasted.payload["recommendation"] == "APPROVE"

    @pytest.mark.asyncio
    async def test_decision_v1_no_sessions(self, store):
        """v1 DecisionGenerated without sessions → empty model_versions."""
        v1_event = BaseEvent(
            event_type="DecisionGenerated",
            event_version=1,
            payload={
                "application_id": "LOAN-002",
                "recommendation": "DECLINE",
            },
        )
        await store.append("loan-LOAN-002", [v1_event], expected_version=-1)

        events = await store.load_stream("loan-LOAN-002")
        upcasted = global_registry.upcast(events[0])

        assert upcasted.event_version == 2
        assert upcasted.payload["model_versions"] == {}


# =============================================================================
# 3. Chain Upcasting & Safety
# =============================================================================


class TestUpcastingSafety:
    """Chain upcasting, loop guard, and edge cases."""

    @pytest.mark.asyncio
    async def test_chain_upcast_v1_v2_v3(self, registry):
        """Register v1→v2→v3 chain, verify full chain applies."""
        @registry.register("TestEvent", from_version=1)
        def v1_to_v2(payload: dict) -> dict:
            return {**payload, "v2_field": "added"}

        @registry.register("TestEvent", from_version=2)
        def v2_to_v3(payload: dict) -> dict:
            return {**payload, "v3_field": "added"}

        # Temporarily override LATEST_VERSIONS for this test
        from upcasting import registry as upcasting_module
        old_latest = upcasting_module.LATEST_VERSIONS.copy()
        upcasting_module.LATEST_VERSIONS["TestEvent"] = 3

        try:
            v1 = StoredEvent(
                event_id="00000000-0000-0000-0000-000000000001",
                stream_id="test-1",
                stream_position=1,
                global_position=1,
                event_type="TestEvent",
                event_version=1,
                payload={"original": True},
                metadata={},
                recorded_at="2026-01-01T00:00:00+00:00",
            )

            result = registry.upcast(v1)
            assert result.event_version == 3
            assert result.payload["original"] is True
            assert result.payload["v2_field"] == "added"
            assert result.payload["v3_field"] == "added"
        finally:
            upcasting_module.LATEST_VERSIONS = old_latest

    @pytest.mark.asyncio
    async def test_no_upcaster_passes_through(self, store):
        """Events without registered upcasters pass through unchanged."""
        event = BaseEvent(
            event_type="ApplicationSubmitted",
            event_version=1,
            payload={"application_id": "LOAN-003"},
        )
        await store.append("loan-LOAN-003", [event], expected_version=-1)

        events = await store.load_stream("loan-LOAN-003")
        upcasted = global_registry.upcast(events[0])

        assert upcasted.event_version == 1
        assert upcasted.payload["application_id"] == "LOAN-003"

    @pytest.mark.asyncio
    async def test_loop_guard_raises_upcasting_error(self, registry):
        """Circular registration → UpcastingError after MAX_UPCAST_STEPS."""
        # Register a cycle: each version maps to next, forever
        from upcasting import registry as upcasting_module
        old_latest = upcasting_module.LATEST_VERSIONS.copy()
        # Remove ceiling to allow infinite loop attempt
        upcasting_module.LATEST_VERSIONS.pop("CyclicEvent", None)

        try:
            for v in range(1, MAX_UPCAST_STEPS + 5):
                @registry.register("CyclicEvent", from_version=v)
                def cyclic_upcast(payload: dict, _v=v) -> dict:
                    return {**payload, f"v{_v+1}": True}

            v1 = StoredEvent(
                event_id="00000000-0000-0000-0000-000000000002",
                stream_id="test-2",
                stream_position=1,
                global_position=1,
                event_type="CyclicEvent",
                event_version=1,
                payload={"start": True},
                metadata={},
                recorded_at="2026-01-01T00:00:00+00:00",
            )

            with pytest.raises(UpcastingError, match="Infinite upcast chain"):
                registry.upcast(v1)
        finally:
            upcasting_module.LATEST_VERSIONS = old_latest

    @pytest.mark.asyncio
    async def test_malformed_payload_handled(self, store):
        """Upcaster receives empty payload → still returns dict without crash."""
        v1_event = BaseEvent(
            event_type="CreditAnalysisCompleted",
            event_version=1,
            payload={},  # Empty/malformed
        )
        await store.append("loan-LOAN-004", [v1_event], expected_version=-1)

        events = await store.load_stream("loan-LOAN-004")
        upcasted = global_registry.upcast(events[0])

        # Should still upcast without crash
        assert upcasted.event_version == 2
        assert upcasted.payload["model_version"] == "legacy-pre-2026"
        assert upcasted.payload["confidence_score"] is None
