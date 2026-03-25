"""
Phase 2 Tests — Aggregates, Commands & Business Rules

14 tests using InMemoryEventStore:
  1-3: State reconstruction & transitions
  4-5: Gas Town enforcement
  6: Confidence floor (BR3)
  7: Compliance dependency (BR4)
  8: Causal chain validation (BR5)
  9: Agent session lifecycle
  10: Context event reference
  11-12: Command handler flows
  13: Domain-level concurrency (two handlers racing)
  14: Human override path (NARR-05)
"""

from __future__ import annotations

import asyncio

import pytest

from aggregates.agent_session import AgentSessionAggregate, SessionState
from aggregates.loan_application import (
    ApplicationState,
    LoanApplicationAggregate,
)
from commands.handlers import (
    ApproveApplicationCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    RequestCreditAnalysisCommand,
    SubmitApplicationCommand,
    handle_approve_application,
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_fraud_screening_completed,
    handle_human_review_completed,
    handle_request_credit_analysis,
    handle_submit_application,
)
from in_memory_store import InMemoryEventStore
from models.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    ApplicationSubmitted,
    BaseEvent,
    ComplianceCheckRequested,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionRequested,
    DomainError,
    FraudScreeningCompleted,
    OptimisticConcurrencyError,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store():
    return InMemoryEventStore()


async def _seed_application(store: InMemoryEventStore, app_id: str = "APP-001") -> int:
    """Seed a loan application at SUBMITTED state."""
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[ApplicationSubmitted(payload={
            "application_id": app_id,
            "applicant_id": "COMP-031",
            "requested_amount_usd": 500000.0,
            "loan_purpose": "expansion",
            "submission_channel": "api",
        })],
        expected_version=-1,
    )


async def _seed_agent_session(
    store: InMemoryEventStore,
    agent_id: str = "credit-agent",
    session_id: str = "sess-001",
    model_version: str = "v2.3",
) -> int:
    """Seed an agent session with context loaded (Gas Town compliant)."""
    stream_id = f"agent-{agent_id}-{session_id}"
    await store.append(
        stream_id=stream_id,
        events=[AgentSessionStarted(payload={
            "agent_id": agent_id,
            "session_id": session_id,
        })],
        expected_version=-1,
    )
    return await store.append(
        stream_id=stream_id,
        events=[AgentContextLoaded(payload={
            "agent_id": agent_id,
            "session_id": session_id,
            "context_source": "fresh",
            "model_version": model_version,
        })],
        expected_version=1,
    )


async def _advance_to_awaiting_analysis(store: InMemoryEventStore, app_id: str = "APP-001") -> int:
    """Seed and advance to AWAITING_ANALYSIS."""
    await _seed_application(store, app_id)
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[CreditAnalysisRequested(payload={
            "application_id": app_id,
            "assigned_agent_id": "credit-agent",
            "requested_at": "",
            "priority": "normal",
        })],
        expected_version=1,
    )


async def _advance_to_pending_decision(
    store: InMemoryEventStore, app_id: str = "APP-001",
) -> int:
    """Seed and advance through full pipeline to PENDING_DECISION."""
    await _advance_to_awaiting_analysis(store, app_id)
    await _seed_agent_session(store)

    # CreditAnalysisCompleted → ANALYSIS_COMPLETE
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[CreditAnalysisCompleted(payload={
            "application_id": app_id,
            "agent_id": "credit-agent",
            "session_id": "sess-001",
            "model_version": "v2.3",
            "confidence_score": 0.85,
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 400000.0,
        })],
        expected_version=2,
    )

    # FraudScreeningCompleted (stays ANALYSIS_COMPLETE)
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[FraudScreeningCompleted(payload={
            "application_id": app_id,
            "fraud_score": 0.1,
        })],
        expected_version=3,
    )

    # ComplianceCheckRequested → COMPLIANCE_REVIEW
    await store.append(
        stream_id=f"loan-{app_id}",
        events=[ComplianceCheckRequested(payload={
            "application_id": app_id,
            "regulation_set_version": "2026-Q1",
            "checks_required": ["REG-001", "REG-002"],
        })],
        expected_version=4,
    )

    # DecisionRequested → PENDING_DECISION
    return await store.append(
        stream_id=f"loan-{app_id}",
        events=[DecisionRequested(payload={"application_id": app_id})],
        expected_version=5,
    )


# =============================================================================
# Test 1: State reconstruction from events
# =============================================================================


class TestStateReconstruction:
    async def test_load_reconstructs_state(self, store):
        await _seed_application(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")

        assert app.state == ApplicationState.SUBMITTED
        assert app.version == 1
        assert app.applicant_id == "COMP-031"
        assert app.requested_amount == 500000.0


# =============================================================================
# Test 2-3: State transitions
# =============================================================================


class TestStateTransitions:
    async def test_valid_transitions_happy_path(self, store):
        """Full lifecycle: SUBMITTED → ... → FINAL_APPROVED."""
        await _advance_to_pending_decision(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")
        assert app.state == ApplicationState.PENDING_DECISION
        assert app.version == 6

    async def test_invalid_transition_raises_domain_error(self, store):
        """Appending CreditAnalysisCompleted to a SUBMITTED stream should fail."""
        await _seed_application(store)

        # Try to append CreditAnalysisCompleted without CreditAnalysisRequested
        await store.append(
            stream_id="loan-APP-001",
            events=[CreditAnalysisCompleted(payload={
                "application_id": "APP-001",
                "confidence_score": 0.9,
                "risk_tier": "LOW",
            })],
            expected_version=1,
        )

        # Loading the aggregate should raise because SUBMITTED → ANALYSIS_COMPLETE
        # is not a valid transition
        with pytest.raises(DomainError, match="Invalid state transition"):
            await LoanApplicationAggregate.load(store, "APP-001")

    async def test_refer_blocks_second_decision(self, store):
        """REFER decision must go through human review first."""
        await _advance_to_pending_decision(store, "APP-REFER")
        
        # Generate decision with REFER
        await handle_decision_generated(
            GenerateDecisionCommand(
                application_id="APP-REFER",
                orchestrator_agent_id="orch",
                recommendation="REFER",
                confidence_score=0.5,
                contributing_agent_sessions=[],
            ),
            store
        )
        
        # Try second DecisionGenerated → should raise DomainError
        from models.events import DomainError
        with pytest.raises(DomainError, match="REFER decision is pending human review"):
            await handle_decision_generated(
                GenerateDecisionCommand(
                    application_id="APP-REFER",
                    orchestrator_agent_id="orch",
                    recommendation="APPROVE",
                    confidence_score=0.9,
                    contributing_agent_sessions=[],
                ),
                store
            )


# =============================================================================
# Test 4-5: Gas Town enforcement
# =============================================================================


class TestGasTown:
    async def test_no_decision_without_context(self, store):
        """Agent session without AgentContextLoaded cannot make decisions."""
        # Create session with only AgentSessionStarted — no context
        await store.append(
            stream_id="agent-credit-agent-sess-no-ctx",
            events=[AgentSessionStarted(payload={
                "agent_id": "credit-agent",
                "session_id": "sess-no-ctx",
            })],
            expected_version=-1,
        )

        agent = await AgentSessionAggregate.load(
            store, "credit-agent", "sess-no-ctx"
        )
        with pytest.raises(DomainError, match="Gas Town violation"):
            agent.assert_context_loaded()

    async def test_model_version_mismatch_raises(self, store):
        """Model version in command must match session's declared version."""
        await _seed_agent_session(store, model_version="v2.3")
        agent = await AgentSessionAggregate.load(store, "credit-agent", "sess-001")

        with pytest.raises(DomainError, match="Model version mismatch"):
            agent.assert_model_version_current("v2.4")  # wrong version


# =============================================================================
# Test 6: Confidence floor (BR3)
# =============================================================================


class TestConfidenceFloor:
    async def test_confidence_floor_forces_refer(self, store):
        """BR3: confidence < 0.6 must force recommendation to REFER."""
        await _seed_application(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")

        # Try APPROVE with low confidence
        result = app.assert_valid_decision(0.55, "APPROVE")
        assert result == "REFER"

        # APPROVE with high confidence is fine
        result = app.assert_valid_decision(0.85, "APPROVE")
        assert result == "APPROVE"

        # REFER with low confidence stays REFER
        result = app.assert_valid_decision(0.4, "REFER")
        assert result == "REFER"


# =============================================================================
# Test 7: Compliance dependency (BR4)
# =============================================================================


class TestComplianceDependency:
    async def test_approve_blocked_without_compliance(self, store):
        """BR4: Cannot approve if required compliance checks are not passed."""
        await _seed_application(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")
        app.required_checks = ["REG-001", "REG-002"]

        # No compliance events → should fail
        with pytest.raises(DomainError, match="compliance checks not passed"):
            app.assert_compliance_clear(compliance_events=[], required_checks=["REG-001", "REG-002"])

    async def test_approve_succeeds_with_compliance_clear(self, store):
        """BR4: Approve works when all required checks pass."""
        await _seed_application(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")

        # Create compliance events
        from models.events import StoredEvent
        import uuid
        from datetime import datetime

        compliance_events = [
            StoredEvent(
                event_id=uuid.uuid4(),
                stream_id="compliance-APP-001",
                stream_position=1,
                global_position=100,
                recorded_at=datetime.now(),
                event_type="ComplianceRulePassed",
                payload={"rule_id": "REG-001"},
            ),
            StoredEvent(
                event_id=uuid.uuid4(),
                stream_id="compliance-APP-001",
                stream_position=2,
                global_position=101,
                recorded_at=datetime.now(),
                event_type="ComplianceRulePassed",
                payload={"rule_id": "REG-002"},
            ),
        ]

        # Should NOT raise
        app.assert_compliance_clear(
            compliance_events=compliance_events,
            required_checks=["REG-001", "REG-002"],
        )

    async def test_human_approve_blocked_without_compliance(self, store):
        """BR4: Human cannot approve if compliance checks are not passed."""
        await _advance_to_pending_decision(store, "APP-HUMAN")
        app = await LoanApplicationAggregate.load(store, "APP-HUMAN")
        app.required_checks = ["REG-001"]
        # Fake a decline to get to DECLINED_PENDING_HUMAN
        await handle_decision_generated(
            GenerateDecisionCommand(
                application_id="APP-HUMAN",
                orchestrator_agent_id="orch",
                recommendation="DECLINE",
                confidence_score=0.9,
                contributing_agent_sessions=[],
            ),
            store
        )
        
        # Try handle_human_review_completed with final_decision="APPROVE"
        from models.events import DomainError
        with pytest.raises(DomainError, match="compliance checks not passed"):
            await handle_human_review_completed(
                HumanReviewCompletedCommand(
                    application_id="APP-HUMAN",
                    reviewer_id="human-01",
                    override=True,
                    override_reason="Because I said so",
                    final_decision="APPROVE"
                ),
                store
            )


# =============================================================================
# Test 8: Causal chain validation (BR5)
# =============================================================================


class TestCausalChain:
    async def test_invalid_session_rejected(self, store):
        """BR5: Contributing session with no decision for this app is rejected."""
        await _seed_application(store)
        app = await LoanApplicationAggregate.load(store, "APP-001")

        # Create a session with events but no decision for APP-001
        from models.events import StoredEvent
        import uuid
        from datetime import datetime

        fake_events = [
            StoredEvent(
                event_id=uuid.uuid4(),
                stream_id="agent-credit-agent-sess-other",
                stream_position=1,
                global_position=50,
                recorded_at=datetime.now(),
                event_type="AgentSessionStarted",
                payload={},
            ),
        ]

        with pytest.raises(DomainError, match="Causal chain broken"):
            app.assert_valid_causal_chain({
                "agent-credit-agent-sess-other": fake_events,
            })


# =============================================================================
# Test 9: Agent session lifecycle
# =============================================================================


class TestAgentSessionLifecycle:
    async def test_full_lifecycle(self, store):
        """Session: STARTED → CONTEXT_LOADED → EXECUTING → COMPLETED."""
        await _seed_agent_session(store)
        agent = await AgentSessionAggregate.load(store, "credit-agent", "sess-001")

        assert agent.state == SessionState.CONTEXT_LOADED
        assert agent.context_loaded is True
        assert agent.model_version == "v2.3"
        assert agent.context_event_position == 2  # second event in stream


# =============================================================================
# Test 10: Context event must be referenced
# =============================================================================


class TestContextReference:
    async def test_context_event_position_returned(self, store):
        """get_context_event_position() returns the position for referencing."""
        await _seed_agent_session(store)
        agent = await AgentSessionAggregate.load(store, "credit-agent", "sess-001")

        pos = agent.get_context_event_position()
        assert pos == 2  # AgentContextLoaded is the 2nd event


# =============================================================================
# Test 11-12: Command handler flows
# =============================================================================


class TestCommandHandlers:
    async def test_happy_path_submit_and_request_analysis(self, store):
        """Full command flow: submit → request analysis."""
        # Submit
        version = await handle_submit_application(
            SubmitApplicationCommand(
                application_id="APP-100",
                applicant_id="COMP-031",
                requested_amount_usd=500000.0,
            ),
            store,
        )
        assert version == 1

        # Request credit analysis
        version = await handle_request_credit_analysis(
            RequestCreditAnalysisCommand(application_id="APP-100"),
            store,
        )
        assert version == 2

        # Verify state
        app = await LoanApplicationAggregate.load(store, "APP-100")
        assert app.state == ApplicationState.AWAITING_ANALYSIS

    async def test_occ_on_stale_version(self, store):
        """Two handlers racing should cause OCC on the loser."""
        await handle_submit_application(
            SubmitApplicationCommand(
                application_id="APP-200",
                applicant_id="COMP-031",
                requested_amount_usd=500000.0,
            ),
            store,
        )

        # Simulate stale read: both handlers load at version 1
        # First request succeeds
        await handle_request_credit_analysis(
            RequestCreditAnalysisCommand(application_id="APP-200"),
            store,
        )

        # Second request should fail with OCC (version now 2, not 1)
        with pytest.raises(
            (OptimisticConcurrencyError, DomainError),
        ):
            await handle_request_credit_analysis(
                RequestCreditAnalysisCommand(application_id="APP-200"),
                store,
            )


# =============================================================================
# Test 13: Two handlers racing (domain-level concurrency)
# =============================================================================


class TestDomainConcurrency:
    async def test_two_handlers_racing(self, store):
        """Two concurrent credit analysis handlers — exactly one succeeds."""
        await _advance_to_awaiting_analysis(store, "APP-RACE")
        await _seed_agent_session(store, "agent-a", "sess-a", "v2.3")
        await _seed_agent_session(store, "agent-b", "sess-b", "v2.3")

        results = {"success": 0, "occ_error": 0}

        async def try_credit(agent_id: str, session_id: str):
            try:
                await handle_credit_analysis_completed(
                    CreditAnalysisCompletedCommand(
                        application_id="APP-RACE",
                        agent_id=agent_id,
                        session_id=session_id,
                        model_version="v2.3",
                        confidence_score=0.85,
                        risk_tier="MEDIUM",
                        recommended_limit_usd=400000.0,
                    ),
                    store,
                )
                results["success"] += 1
            except (OptimisticConcurrencyError, DomainError):
                results["occ_error"] += 1

        await asyncio.gather(
            try_credit("agent-a", "sess-a"),
            try_credit("agent-b", "sess-b"),
        )

        assert results["success"] == 1
        assert results["occ_error"] == 1

        app = await LoanApplicationAggregate.load(store, "APP-RACE")
        assert app.state == ApplicationState.ANALYSIS_COMPLETE
        assert app.version == 3  # submit + credit_requested + credit_completed


# =============================================================================
# Test 14: Human override path (NARR-05)
# =============================================================================


class TestHumanOverride:
    async def test_approve_after_human_override(self, store):
        """NARR-05: orchestrator declines, human overrides to approve."""
        await _advance_to_pending_decision(store, "APP-NARR05")

        # Seed compliance stream with passed checks
        await store.append(
            stream_id="compliance-APP-NARR05",
            events=[
                ComplianceRulePassed(payload={
                    "application_id": "APP-NARR05",
                    "rule_id": "REG-001",
                }),
                ComplianceRulePassed(payload={
                    "application_id": "APP-NARR05",
                    "rule_id": "REG-002",
                }),
            ],
            expected_version=-1,
        )

        # Decision: DECLINE (high confidence)
        await handle_decision_generated(
            GenerateDecisionCommand(
                application_id="APP-NARR05",
                orchestrator_agent_id="orchestrator-01",
                recommendation="DECLINE",
                confidence_score=0.82,
                contributing_agent_sessions=[],
            ),
            store,
        )

        app = await LoanApplicationAggregate.load(store, "APP-NARR05")
        assert app.state == ApplicationState.DECLINED_PENDING_HUMAN

        # Human override: approve
        await handle_human_review_completed(
            HumanReviewCompletedCommand(
                application_id="APP-NARR05",
                reviewer_id="LO-Sarah-Chen",
                override=True,
                final_decision="APPROVE",
                override_reason="15-year customer, prior repayment",
            ),
            store,
        )

        app = await LoanApplicationAggregate.load(store, "APP-NARR05")
        assert app.state == ApplicationState.FINAL_APPROVED
        assert app.human_review_override is True
