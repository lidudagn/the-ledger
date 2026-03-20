"""Shared test fixtures."""

import sys
from pathlib import Path

import pytest

# Add src to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    BaseEvent,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    FraudScreeningCompleted,
)


@pytest.fixture
def store():
    """Fresh InMemoryEventStore per test."""
    return InMemoryEventStore()


@pytest.fixture
def sample_events() -> list[BaseEvent]:
    """Three sample events for testing."""
    return [
        ApplicationSubmitted(
            payload={
                "application_id": "APP-001",
                "applicant_id": "ACME-001",
                "requested_amount_usd": 500000.0,
                "loan_purpose": "expansion",
                "submission_channel": "api",
                "submitted_at": "2026-01-15T10:00:00Z",
            }
        ),
        CreditAnalysisRequested(
            payload={
                "application_id": "APP-001",
                "assigned_agent_id": "credit-agent-01",
                "requested_at": "2026-01-15T10:01:00Z",
                "priority": "high",
            }
        ),
        CreditAnalysisCompleted(
            payload={
                "application_id": "APP-001",
                "agent_id": "credit-agent-01",
                "session_id": "sess-001",
                "model_version": "v2.3",
                "confidence_score": 0.85,
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": 450000.0,
                "analysis_duration_ms": 1200,
                "input_data_hash": "abc123",
            }
        ),
    ]


@pytest.fixture
def make_event():
    """Factory to create simple test events."""

    def _make(event_type: str = "ApplicationSubmitted", **payload_overrides):
        return ApplicationSubmitted(
            payload={"application_id": "test", **payload_overrides}
        )

    return _make
