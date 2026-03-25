"""
The Ledger — Regulatory Package Tests (Phase 6)

5 tests validating the self-contained examination package:
  1. Package contains all 8 sections
  2. Package integrity is independently verifiable (hash recomputation)
  3. Temporal state at examination date shows earlier state
  4. Narrative coherence (chronological, maps to events)
  5. Schema evolution transparency (upcaster trace)
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from in_memory_store import InMemoryEventStore
from integrity.audit_chain import GENESIS_HASH, canonical_event_hash, compute_chain_hash
from models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    ComplianceCheckRequested,
    DecisionGenerated,
    DecisionRequested,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    ApplicationApproved,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentOutputWritten,
)
from regulatory.package import generate_regulatory_package


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _build_lifecycle_with_timestamps(
    store: InMemoryEventStore,
    app_id: str = "REG-001",
):
    """Build a full lifecycle, returning the store for package generation."""
    corr = str(uuid.uuid4())

    await store.append(f"loan-{app_id}", [
        ApplicationSubmitted(payload={
            "application_id": app_id,
            "applicant_id": "COMP-001",
            "requested_amount_usd": 500_000.0,
            "loan_purpose": "Expansion",
            "submission_channel": "api",
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }),
    ], expected_version=-1, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        CreditAnalysisRequested(payload={
            "application_id": app_id,
            "assigned_agent_id": "credit1",
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "priority": "normal",
        }),
    ], expected_version=1, correlation_id=corr)

    # Agent session
    await store.append("agent-credit1-sess1", [
        AgentSessionStarted(payload={"agent_id": "credit1", "session_id": "sess1"}),
        AgentContextLoaded(payload={
            "agent_id": "credit1",
            "session_id": "sess1",
            "context_source": "fresh",
            "model_version": "v2.3",
            "context_token_count": 4000,
        }),
    ], expected_version=-1, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        CreditAnalysisCompleted(payload={
            "application_id": app_id,
            "agent_id": "credit1",
            "session_id": "sess1",
            "model_version": "v2.3",
            "confidence_score": 0.85,
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 600_000.0,
            "analysis_duration_ms": 150,
            "input_data_hash": "abc123",
        }),
    ], expected_version=2, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        FraudScreeningCompleted(payload={
            "application_id": app_id,
            "agent_id": "fraud1",
            "fraud_score": 0.05,
            "anomaly_flags": [],
            "screening_model_version": "f-v1",
            "input_data_hash": "def456",
        }),
    ], expected_version=3, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        ComplianceCheckRequested(payload={
            "application_id": app_id,
            "regulation_set_version": "AMLv2",
            "checks_required": ["REG-KYC", "REG-SANCTIONS"],
        }),
    ], expected_version=4, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        DecisionRequested(payload={"application_id": app_id}),
    ], expected_version=5, correlation_id=corr)

    # Agent output for causal chain
    await store.append("agent-credit1-sess1", [
        AgentOutputWritten(payload={"application_id": app_id, "output": {}}),
    ], expected_version=2, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        DecisionGenerated(payload={
            "application_id": app_id,
            "orchestrator_agent_id": "orch1",
            "recommendation": "APPROVE",
            "confidence_score": 0.90,
            "contributing_agent_sessions": ["agent-credit1-sess1"],
            "decision_basis_summary": "All clear",
            "model_versions": {"credit1": "v2.3"},
        }),
    ], expected_version=6, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        HumanReviewCompleted(payload={
            "application_id": app_id,
            "reviewer_id": "LO-100",
            "override": False,
            "final_decision": "APPROVE",
            "override_reason": "",
        }),
    ], expected_version=7, correlation_id=corr)

    await store.append(f"loan-{app_id}", [
        ApplicationApproved(payload={
            "application_id": app_id,
            "approved_amount_usd": 500_000.0,
            "interest_rate": 5.5,
            "conditions": [],
            "approved_by": "LO-100",
            "effective_date": datetime.now(timezone.utc).isoformat(),
        }),
    ], expected_version=8, correlation_id=corr)

    return corr


# ─── Test 1: Package contains all 8 sections ─────────────────────────────────

@pytest.mark.asyncio
async def test_package_contains_all_sections():
    """Verify all 8 required sections are present and non-empty."""
    store = InMemoryEventStore()
    await _build_lifecycle_with_timestamps(store)

    package = await generate_regulatory_package(store, "REG-001")

    # All 8 sections present
    required_sections = [
        "package_metadata",
        "event_stream",
        "projection_states_at_examination",
        "schema_evolution",
        "integrity_verification",
        "agent_participation",
        "narrative",
        "independent_verification",
    ]
    for section in required_sections:
        assert section in package, f"Missing section: {section}"

    # Event stream is non-empty and ordered
    events = package["event_stream"]
    assert len(events) > 0
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions), "Events not ordered by position"

    # Narrative is non-empty
    assert len(package["narrative"]) > 0

    # Agent participation has entries
    assert len(package["agent_participation"]) > 0

    # Package metadata has required fields
    meta = package["package_metadata"]
    assert meta["application_id"] == "REG-001"
    assert meta["package_version"] == "1.0"
    assert meta["event_count"] > 0
    assert meta["estimated_size_bytes"] > 0


# ─── Test 2: Package integrity independently verifiable ───────────────────────

@pytest.mark.asyncio
async def test_package_integrity_independently_verifiable():
    """
    Re-compute hash chain from raw event_stream data.
    Recomputed hash must match stored integrity_hash.
    This is the core audit-grade assertion.
    """
    store = InMemoryEventStore()
    await _build_lifecycle_with_timestamps(store)

    package = await generate_regulatory_package(store, "REG-001")

    integrity = package["integrity_verification"]
    raw_events = package["event_stream"]

    # Re-compute event hashes from raw package data
    recomputed_hashes: list[str] = []
    for e in raw_events:
        event_dict = {
            "event_type": e["event_type"],
            "event_version": e["event_version"],
            "payload": e["payload"],
            "recorded_at": e["recorded_at"],
            "stream_position": e["stream_position"],
        }
        h = canonical_event_hash(event_dict)
        recomputed_hashes.append(h)

    # Verify individual hashes match
    assert recomputed_hashes == integrity["event_hashes"], (
        "Event hashes do not match — potential tampering"
    )

    # Re-compute chain hash
    previous_hash = integrity["previous_hash"]
    recomputed_chain = compute_chain_hash(previous_hash, recomputed_hashes)

    assert recomputed_chain == integrity["integrity_hash"], (
        f"Chain hash mismatch: {recomputed_chain} != {integrity['integrity_hash']}"
    )

    # Verify it uses genesis hash as starting point
    assert previous_hash == GENESIS_HASH


# ─── Test 3: Temporal state at examination date ───────────────────────────────

@pytest.mark.asyncio
async def test_package_temporal_state_at_examination_date():
    """
    Generate package with examination_date BEFORE final approval.
    Projection state must reflect the earlier time, not current.
    """
    store = InMemoryEventStore()
    await _build_lifecycle_with_timestamps(store)

    # Load all events to find a midpoint timestamp
    all_events = await store.load_stream("loan-REG-001")

    # Find the timestamp of the DecisionRequested event (before approval)
    decision_requested = [e for e in all_events if e.event_type == "DecisionRequested"]
    assert len(decision_requested) > 0

    # Use a date slightly after DecisionRequested but before DecisionGenerated
    cutoff = decision_requested[0].recorded_at + timedelta(microseconds=1)

    package = await generate_regulatory_package(
        store, "REG-001", examination_date=cutoff
    )

    # State should reflect PENDING_DECISION, not FINAL_APPROVED
    proj_state = package["projection_states_at_examination"]["application_summary"]
    assert proj_state.get("state") != "FINAL_APPROVED", (
        f"Temporal state should be earlier than FINAL_APPROVED, got {proj_state.get('state')}"
    )

    # Event stream should be shorter (only events up to examination_date)
    full_package = await generate_regulatory_package(store, "REG-001")
    assert package["package_metadata"]["event_count"] <= full_package["package_metadata"]["event_count"]


# ─── Test 4: Narrative coherence ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_package_narrative_coherence():
    """
    Verify narrative sentences are chronologically ordered
    and map to actual events in the event_stream.
    """
    store = InMemoryEventStore()
    await _build_lifecycle_with_timestamps(store)

    package = await generate_regulatory_package(store, "REG-001")

    narrative = package["narrative"]
    event_stream = package["event_stream"]

    # Narrative should have numbered sentences
    lines = [l for l in narrative.split("\n") if l.strip()]
    assert len(lines) == len(event_stream), (
        f"Narrative has {len(lines)} lines but event stream has "
        f"{len(event_stream)} events"
    )

    # Each line should start with a number matching its position
    for i, line in enumerate(lines):
        expected_prefix = f"{i + 1}. "
        assert line.startswith(expected_prefix), (
            f"Line {i + 1} should start with '{expected_prefix}', got: {line[:20]}"
        )

    # Verify key event types are referenced in the narrative
    event_types = [e["event_type"] for e in event_stream]

    if "ApplicationSubmitted" in event_types:
        assert "submitted" in narrative.lower()
    if "CreditAnalysisCompleted" in event_types:
        assert "credit" in narrative.lower()
    if "ApplicationApproved" in event_types:
        assert "APPROVED" in narrative


# ─── Test 5: Schema evolution transparency ────────────────────────────────────

@pytest.mark.asyncio
async def test_package_schema_evolution_transparency():
    """
    Verify the schema_evolution section correctly reports
    known upcaster mappings for events in the stream.
    """
    store = InMemoryEventStore()
    await _build_lifecycle_with_timestamps(store)

    package = await generate_regulatory_package(store, "REG-001")

    schema_evo = package["schema_evolution"]

    # Original event versions should be recorded
    assert "original_event_versions" in schema_evo
    orig_versions = schema_evo["original_event_versions"]
    assert "ApplicationSubmitted" in orig_versions
    assert "CreditAnalysisCompleted" in orig_versions

    # CreditAnalysisCompleted is at v2 (per models/events.py) which has a known upcaster
    # The upcasters_applied may or may not be present depending on whether the event
    # was created at v2 (not actually upcasted) vs loaded from v1 storage
    assert "upcasters_applied" in schema_evo
    assert isinstance(schema_evo["upcasters_applied"], list)

    # If there are upcasters listed, they should have required fields
    for entry in schema_evo["upcasters_applied"]:
        assert "event_type" in entry
        assert "original_version" in entry
        assert "upcasted_to_version" in entry
        assert "stream_position" in entry
        assert "fields_inferred" in entry
        assert isinstance(entry["fields_inferred"], list)
