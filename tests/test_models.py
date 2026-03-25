"""Tests for Pydantic models and EVENT_REGISTRY."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from models.events import (
    EVENT_REGISTRY,
    ApplicationSubmitted,
    BaseEvent,
    CreditAnalysisCompleted,
    DomainError,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamMetadata,
    StreamNotFoundError,
)


class TestBaseEvent:
    def test_create_with_defaults(self):
        event = BaseEvent(event_type="TestEvent")
        assert event.event_type == "TestEvent"
        assert event.event_version == 1
        assert event.payload == {}
        assert event.metadata == {}

    def test_create_with_payload(self):
        event = ApplicationSubmitted(
            payload={"application_id": "APP-001", "requested_amount_usd": 500000.0}
        )
        assert event.event_type == "ApplicationSubmitted"
        assert event.payload["application_id"] == "APP-001"

    def test_model_dump_roundtrip(self):
        event = CreditAnalysisCompleted(
            payload={
                "application_id": "APP-001",
                "confidence_score": 0.85,
                "risk_tier": "MEDIUM",
            }
        )
        data = event.model_dump(mode="json")
        restored = CreditAnalysisCompleted(**data)
        assert restored.event_type == event.event_type
        assert restored.payload == event.payload


class TestEventRegistry:
    def test_registry_has_45_events(self):
        assert len(EVENT_REGISTRY) == 45

    def test_all_registered_types_are_base_event(self):
        for event_type, cls in EVENT_REGISTRY.items():
            assert issubclass(cls, BaseEvent), f"{event_type} is not a BaseEvent"

    def test_duplicate_registration_raises(self):
        """Attempting to register same event_type twice raises ValueError."""
        from models.events import register

        class DuplicateTest(BaseEvent):
            event_type: str = "ApplicationSubmitted"

        try:
            register(DuplicateTest)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Duplicate" in str(e)


class TestErrors:
    def test_occ_error_fields(self):
        err = OptimisticConcurrencyError("loan-001", expected_version=3, actual_version=5)
        assert err.stream_id == "loan-001"
        assert err.expected_version == 3
        assert err.actual_version == 5
        assert isinstance(err, Exception)
        assert not isinstance(err, DomainError)  # OCC is infrastructure, not domain
        assert err.suggested_action == "reload_stream_and_retry"

    def test_stream_not_found(self):
        err = StreamNotFoundError("loan-999")
        assert err.stream_id == "loan-999"
        assert isinstance(err, DomainError)
