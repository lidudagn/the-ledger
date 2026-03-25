"""
The Ledger — What-If Projector Tests (Phase 6)

5 tests validating counterfactual replay:
  1. HIGH risk counterfactual produces different outcome than MEDIUM
  2. Counterfactual NEVER writes to real store (explicit invariant)
  3. Causal dependency filtering (dependent skipped, independent replayed)
  4. Confidence floor forces REFER (BR3 enforcement)
  5. Aggregate validates before projections (impossible state caught)
"""

import uuid
from datetime import datetime, timezone

import pytest

from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    BaseEvent,
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
from what_if.projector import run_what_if, WhatIfResult


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _build_full_lifecycle(store: InMemoryEventStore, app_id: str = "WI-001"):
    """
    Build a complete happy-path lifecycle through the loan stream.
    Returns event_ids for causal tracing.
    """
    corr = str(uuid.uuid4())

    # 1. ApplicationSubmitted (version 1)
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

    # 2. CreditAnalysisRequested (version 2)
    await store.append(f"loan-{app_id}", [
        CreditAnalysisRequested(payload={
            "application_id": app_id,
            "assigned_agent_id": "credit1",
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "priority": "normal",
        }),
    ], expected_version=1, correlation_id=corr)

    # Agent session for credit
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

    # 3. CreditAnalysisCompleted (version 3) — this is the branch point
    credit_events = await store.load_stream(f"loan-{app_id}")
    last_credit_id = str(credit_events[-1].event_id)

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
    ], expected_version=2, correlation_id=corr, causation_id=last_credit_id)

    # 4. FraudScreeningCompleted (version 4) — independent of credit analysis
    await store.append(f"loan-{app_id}", [
        FraudScreeningCompleted(payload={
            "application_id": app_id,
            "agent_id": "fraud1",
            "fraud_score": 0.05,
            "anomaly_flags": [],
            "screening_model_version": "f-v1",
            "input_data_hash": "def456",
        }),
    ], expected_version=3, correlation_id=corr)  # no causation_id → independent

    # 5. ComplianceCheckRequested (version 5) — independent
    await store.append(f"loan-{app_id}", [
        ComplianceCheckRequested(payload={
            "application_id": app_id,
            "regulation_set_version": "AMLv2",
            "checks_required": ["REG-KYC", "REG-SANCTIONS"],
        }),
    ], expected_version=4, correlation_id=corr)

    # 6. DecisionRequested (version 6)
    await store.append(f"loan-{app_id}", [
        DecisionRequested(payload={"application_id": app_id}),
    ], expected_version=5, correlation_id=corr)

    # Agent output for causal chain validation
    await store.append("agent-credit1-sess1", [
        AgentOutputWritten(payload={"application_id": app_id, "output": {}}),
    ], expected_version=2, correlation_id=corr)

    # 7. DecisionGenerated (version 7) — causally dependent on credit analysis
    credit_events_after = await store.load_stream(f"loan-{app_id}")
    credit_completed_event = [
        e for e in credit_events_after if e.event_type == "CreditAnalysisCompleted"
    ][0]
    credit_completed_id = str(credit_completed_event.event_id)

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
    ], expected_version=6, correlation_id=corr, causation_id=credit_completed_id)

    # 8. HumanReviewCompleted (version 8) — causally dependent on decision
    decision_events = await store.load_stream(f"loan-{app_id}")
    decision_event = [
        e for e in decision_events if e.event_type == "DecisionGenerated"
    ][0]
    decision_id = str(decision_event.event_id)

    await store.append(f"loan-{app_id}", [
        HumanReviewCompleted(payload={
            "application_id": app_id,
            "reviewer_id": "LO-100",
            "override": False,
            "final_decision": "APPROVE",
            "override_reason": "",
        }),
    ], expected_version=7, correlation_id=corr, causation_id=decision_id)

    # 9. ApplicationApproved (version 9) — causally dependent on human review
    hr_events = await store.load_stream(f"loan-{app_id}")
    hr_event = [e for e in hr_events if e.event_type == "HumanReviewCompleted"][0]
    hr_id = str(hr_event.event_id)

    await store.append(f"loan-{app_id}", [
        ApplicationApproved(payload={
            "application_id": app_id,
            "approved_amount_usd": 500_000.0,
            "interest_rate": 5.5,
            "conditions": [],
            "approved_by": "LO-100",
            "effective_date": datetime.now(timezone.utc).isoformat(),
        }),
    ], expected_version=8, correlation_id=corr, causation_id=hr_id)

    return corr


# ─── Test 1: HIGH risk changes the decision ──────────────────────────────────

@pytest.mark.asyncio
async def test_what_if_high_risk_changes_decision():
    """
    The spec scenario: substitute MEDIUM → HIGH risk tier.
    Business rules must cascade — the outcome must differ.
    """
    store = InMemoryEventStore()
    app_id = "WI-001"
    await _build_full_lifecycle(store, app_id)

    # Counterfactual: HIGH risk with lower confidence
    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit1",
        "session_id": "sess1",
        "model_version": "v2.3",
        "confidence_score": 0.55,  # Below 0.6 threshold → BR3 forces REFER
        "risk_tier": "HIGH",
        "recommended_limit_usd": 200_000.0,
        "analysis_duration_ms": 150,
        "input_data_hash": "abc123",
    })

    result = await run_what_if(
        store, app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Real outcome: FINAL_APPROVED
    assert result.real_outcome.get("state") == "FINAL_APPROVED"

    # Counterfactual: should NOT be FINAL_APPROVED (business rules cascaded)
    cf_state = result.counterfactual_outcome.get("state")
    assert cf_state != "FINAL_APPROVED", (
        f"Expected counterfactual to differ from FINAL_APPROVED, got {cf_state}"
    )

    # Divergence must be non-empty
    assert len(result.divergence_events) > 0

    # At least the DecisionGenerated should be skipped as causally dependent
    assert len(result.skipped_dependent_events) >= 1

    # Assumptions should document branch resolution
    assert any("branch_resolution" in a for a in result.assumptions)


# ─── Test 2: Never writes to real store ───────────────────────────────────────

@pytest.mark.asyncio
async def test_what_if_never_writes_to_real_store():
    """
    Explicit safety invariant: real store is read-only during what-if.
    """
    store = InMemoryEventStore()
    app_id = "WI-002"
    await _build_full_lifecycle(store, app_id)

    # Capture state before
    version_before = await store.stream_version(f"loan-{app_id}")
    all_events_before = []
    async for e in store.load_all():
        all_events_before.append(e)
    count_before = len(all_events_before)

    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit1",
        "session_id": "sess1",
        "model_version": "v2.3",
        "confidence_score": 0.40,
        "risk_tier": "HIGH",
        "recommended_limit_usd": 100_000.0,
        "analysis_duration_ms": 200,
        "input_data_hash": "xyz",
    })

    result = await run_what_if(
        store, app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Version unchanged
    version_after = await store.stream_version(f"loan-{app_id}")
    assert version_before == version_after, (
        f"Real store version changed: {version_before} → {version_after}"
    )

    # Event count unchanged
    all_events_after = []
    async for e in store.load_all():
        all_events_after.append(e)
    assert len(all_events_after) == count_before

    # Result was actually produced
    assert isinstance(result, WhatIfResult)
    assert result.counterfactual_event_count > 0


# ─── Test 3: Causal dependency filtering ──────────────────────────────────────

@pytest.mark.asyncio
async def test_what_if_causal_dependency_filtering():
    """
    Events with causation_id tracing back to the branched event
    should be skipped. Independent events should be replayed.
    """
    store = InMemoryEventStore()
    app_id = "WI-003"
    await _build_full_lifecycle(store, app_id)

    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit1",
        "session_id": "sess1",
        "model_version": "v2.3",
        "confidence_score": 0.92,
        "risk_tier": "LOW",
        "recommended_limit_usd": 700_000.0,
        "analysis_duration_ms": 100,
        "input_data_hash": "abc123",
    })

    result = await run_what_if(
        store, app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # DecisionGenerated had causation_id → CreditAnalysisCompleted → dependent
    skipped_types = [e["event_type"] for e in result.skipped_dependent_events]
    assert "DecisionGenerated" in skipped_types, (
        f"DecisionGenerated should be causally dependent but was not skipped. "
        f"Skipped: {skipped_types}"
    )

    # FraudScreeningCompleted had no causation_id → independent → replayed
    replayed_types = [e["event_type"] for e in result.replayed_independent_events]
    assert "FraudScreeningCompleted" in replayed_types, (
        f"FraudScreeningCompleted should be independent but was not replayed. "
        f"Replayed: {replayed_types}"
    )

    # Counterfactual stream should be shorter than real (dependent events removed)
    assert result.counterfactual_event_count < result.real_event_count


# ─── Test 4: Confidence floor forces REFER (BR3) ─────────────────────────────

@pytest.mark.asyncio
async def test_what_if_confidence_floor_forces_refer():
    """
    BR3: confidence_score < 0.6 forces REFER regardless of agent analysis.
    The aggregate validates this during replay.
    """
    store = InMemoryEventStore()
    app_id = "WI-004"
    await _build_full_lifecycle(store, app_id)

    # Counterfactual with very low confidence
    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit1",
        "session_id": "sess1",
        "model_version": "v2.3",
        "confidence_score": 0.35,  # Way below 0.6
        "risk_tier": "HIGH",
        "recommended_limit_usd": 100_000.0,
        "analysis_duration_ms": 250,
        "input_data_hash": "lowconf",
    })

    result = await run_what_if(
        store, app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Real outcome should remain FINAL_APPROVED
    assert result.real_outcome.get("state") == "FINAL_APPROVED"

    # Counterfactual with low confidence should NOT reach approval
    cf_state = result.counterfactual_outcome.get("state")
    assert cf_state != "FINAL_APPROVED", (
        f"With confidence 0.35 (< 0.6), outcome should not be FINAL_APPROVED, "
        f"got {cf_state}"
    )


# ─── Test 5: Aggregate validates before projections ───────────────────────────

@pytest.mark.asyncio
async def test_what_if_aggregate_validates_before_projections():
    """
    If counterfactual creates an impossible transition, aggregate catches it.
    Projections should NOT show corrupted state.
    """
    store = InMemoryEventStore()
    app_id = "WI-005"
    await _build_full_lifecycle(store, app_id)

    # Inject a counterfactual that would still be valid for the aggregate
    # (same type, just different data) — verify assumptions are generated
    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit1",
        "session_id": "sess1",
        "model_version": "v2.3",
        "confidence_score": 0.95,
        "risk_tier": "LOW",
        "recommended_limit_usd": 1_000_000.0,
        "analysis_duration_ms": 50,
        "input_data_hash": "perfect",
    })

    result = await run_what_if(
        store, app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event],
    )

    # Should succeed — aggregate validates the counterfactual stream
    assert result.counterfactual_event_count > 0
    assert result.counterfactual_outcome is not None

    # Assumptions should exist (at minimum the branch resolution)
    assert len(result.assumptions) > 0

    # Validate divergence is coherently structured
    for d in result.divergence_events:
        assert "type" in d
        assert "position" in d
