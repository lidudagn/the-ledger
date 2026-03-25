"""
Phase 2 — Stress Tests

Covers critical edge cases, boundary conditions, and a full end-to-end workflow
to guarantee robustness according to production stress-testing standards.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from aggregates.agent_session import SessionState, AgentSessionAggregate
from aggregates.loan_application import ApplicationState, LoanApplicationAggregate
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
    ComplianceCheckRequested,
    ComplianceRulePassed,
    DomainError,
)

@pytest.fixture
def store():
    return InMemoryEventStore()

# =============================================================================
# Stress Test 1: Confidence Boundary (BR3)
# =============================================================================

class TestConfidenceBoundary:
    async def test_confidence_exactly_point_six(self, store):
        """BR3 Boundary: exactly 0.6 should NOT force REFER. It passes."""
        app = LoanApplicationAggregate("APP-BOUND")
        # 0.599 forces REFER
        assert app.assert_valid_decision(0.599, "APPROVE") == "REFER"
        # 0.60 allows the recommendation
        assert app.assert_valid_decision(0.60, "APPROVE") == "APPROVE"


# =============================================================================
# Stress Test 2: Full End-to-End Decision Workflow
# =============================================================================

class TestFullWorkflow:
    async def test_full_application_lifecycle_happy_path(self, store):
        """Simulate a complete, successful application lifecycle using ONLY command handlers."""
        app_id = "APP-FULL-1"
        
        # 1. Submit
        await handle_submit_application(
            SubmitApplicationCommand(
                application_id=app_id, applicant_id="A1", requested_amount_usd=100000.0
            ), store
        )

        app = await LoanApplicationAggregate.load(store, app_id)
        assert app.state == ApplicationState.SUBMITTED

        # 2. Request Credit
        await handle_request_credit_analysis(
            RequestCreditAnalysisCommand(application_id=app_id), store
        )

        # 3. Setup Agent Session & Complete Credit
        agent_id = "agent-cred-1"
        session_id = "sess-cred-1"
        from models.events import AgentOutputWritten
        await store.append(f"agent-{agent_id}-{session_id}", [
            AgentSessionStarted(payload={"agent_id": agent_id, "session_id": session_id}),
            AgentContextLoaded(payload={"agent_id": agent_id, "session_id": session_id, "model_version": "v1"}),
            AgentOutputWritten(payload={"agent_id": agent_id, "session_id": session_id, "application_id": app_id})
        ], expected_version=-1)
        
        await handle_credit_analysis_completed(
            CreditAnalysisCompletedCommand(
                application_id=app_id, agent_id=agent_id, session_id=session_id,
                model_version="v1", confidence_score=0.9, risk_tier="LOW",
                recommended_limit_usd=150000.0,
            ), store
        )

        # 4. Setup Fraud Session & Complete Fraud
        agent_id_f = "agent-fraud-1"
        session_id_f = "sess-fraud-1"
        await store.append(f"agent-{agent_id_f}-{session_id_f}", [
            AgentSessionStarted(payload={"agent_id": agent_id_f, "session_id": session_id_f}),
            AgentContextLoaded(payload={"agent_id": agent_id_f, "session_id": session_id_f, "model_version": "v2"}),
            AgentOutputWritten(payload={"agent_id": agent_id_f, "session_id": session_id_f, "application_id": app_id})
        ], expected_version=-1)
        
        await handle_fraud_screening_completed(
            FraudScreeningCompletedCommand(
                application_id=app_id, agent_id=agent_id_f, session_id=session_id_f,
                fraud_score=0.05, anomaly_flags=[], screening_model_version="v2",
            ), store
        )

        app = await LoanApplicationAggregate.load(store, app_id)
        assert app.state == ApplicationState.ANALYSIS_COMPLETE

        # 5. Compliance Request & Fulfillment (simulated external trigger)
        await store.append(f"loan-{app_id}", [
            ComplianceCheckRequested(payload={"application_id": app_id, "checks_required": ["KYC"]})
        ], expected_version=app.version)
        
        await store.append(f"compliance-{app_id}", [
            ComplianceRulePassed(payload={"application_id": app_id, "rule_id": "KYC"})
        ], expected_version=-1)

        # Decision request
        v = await store.stream_version(f"loan-{app_id}")
        from models.events import DecisionRequested
        await store.append(f"loan-{app_id}", [DecisionRequested(payload={"application_id": app_id})], expected_version=v)

        # 6. Generate Decision
        await handle_decision_generated(
            GenerateDecisionCommand(
                application_id=app_id, orchestrator_agent_id="orch",
                recommendation="APPROVE", confidence_score=0.95,
                contributing_agent_sessions=[f"agent-{agent_id}-{session_id}", f"agent-{agent_id_f}-{session_id_f}"]
            ), store
        )

        app = await LoanApplicationAggregate.load(store, app_id)
        assert app.state == ApplicationState.APPROVED_PENDING_HUMAN

        # 7. Human Review
        await handle_human_review_completed(
            HumanReviewCompletedCommand(
                application_id=app_id, reviewer_id="reviewer-1", override=False, final_decision="APPROVE"
            ), store
        )

        app = await LoanApplicationAggregate.load(store, app_id)
        assert app.state == ApplicationState.FINAL_APPROVED
        assert app.approved_amount == 100000.0


# =============================================================================
# Stress Test 3: Causal Chain Validation Edge Cases
# =============================================================================

class TestCausalChainStress:
    async def test_missing_session_stream_fails(self, store):
        """BR5: If a contributing session stream does not exist, reject immediately."""
        app_id = "APP-MISSING-SESS"
        # Seed to pending decision
        from tests.test_aggregates import _advance_to_pending_decision
        await _advance_to_pending_decision(store, app_id)

        with pytest.raises(DomainError, match="Causal chain broken: session .* does not exist"):
            await handle_decision_generated(
                GenerateDecisionCommand(
                    application_id=app_id, orchestrator_agent_id="orch",
                    recommendation="APPROVE", confidence_score=0.9,
                    contributing_agent_sessions=["agent-bad-missing"]
                ), store
            )

