"""
The Ledger — Command Handlers

Every command handler follows the pattern:
  1. Reconstruct aggregate state from event history (I/O)
  2. Validate business rules (pure — assertions on aggregate)
  3. Build new events (pure)
  4. Append atomically with OCC (I/O)

All I/O lives here. Aggregates are pure domain logic.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from aggregates.agent_session import AgentSessionAggregate
from aggregates.loan_application import (
    ApplicationState,
    LoanApplicationAggregate,
)
from aggregates.compliance_record import ComplianceRecordAggregate
from models.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    DecisionRequested,
    DomainError,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    HumanReviewRequested,
)


# =============================================================================
# Command Models (Pydantic)
# =============================================================================


class SubmitApplicationCommand(BaseModel):
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str = ""
    submission_channel: str = "api"
    correlation_id: str | None = None


class RequestCreditAnalysisCommand(BaseModel):
    application_id: str
    assigned_agent_id: str = ""
    priority: str = "normal"
    correlation_id: str | None = None
    causation_id: str | None = None


class CreditAnalysisCompletedCommand(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: float
    analysis_duration_ms: int = 0
    input_data_hash: str = ""
    data_quality_caveats: list[str] = Field(default_factory=list)
    correlation_id: str | None = None
    causation_id: str | None = None


class FraudScreeningCompletedCommand(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    fraud_score: float
    anomaly_flags: list[str] = Field(default_factory=list)
    screening_model_version: str = ""
    input_data_hash: str = ""
    correlation_id: str | None = None
    causation_id: str | None = None


class GenerateDecisionCommand(BaseModel):
    application_id: str
    orchestrator_agent_id: str
    recommendation: str  # APPROVE, DECLINE, or REFER
    confidence_score: float
    contributing_agent_sessions: list[str] = Field(default_factory=list)
    decision_basis_summary: str = ""
    model_versions: dict[str, str] = Field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None


class ApproveApplicationCommand(BaseModel):
    application_id: str
    approved_amount_usd: float
    interest_rate: float = 0.0
    conditions: list[str] = Field(default_factory=list)
    approved_by: str = "auto"
    correlation_id: str | None = None
    causation_id: str | None = None


class HumanReviewCompletedCommand(BaseModel):
    application_id: str
    reviewer_id: str
    override: bool = False
    final_decision: str = ""
    override_reason: str = ""
    correlation_id: str | None = None
    causation_id: str | None = None


class StartAgentSessionCommand(BaseModel):
    agent_id: str
    session_id: str
    context_source: str = "fresh"
    model_version: str = ""
    event_replay_from_position: int = 0
    context_token_count: int = 0
    correlation_id: str | None = None


class RequestComplianceCheckCommand(BaseModel):
    application_id: str
    regulation_set_version: str = ""
    checks_required: list[str] = Field(default_factory=list)
    correlation_id: str | None = None
    causation_id: str | None = None


class RequestDecisionCommand(BaseModel):
    application_id: str
    correlation_id: str | None = None
    causation_id: str | None = None


# =============================================================================
# Command Handlers
# =============================================================================


async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: Any,
) -> int:
    """Submit a new loan application. Creates the loan stream."""
    # Duplicate application_id check
    existing_version = await store.stream_version(f"loan-{cmd.application_id}")
    if existing_version > 0:
        raise DomainError(
            f"Application '{cmd.application_id}' already exists "
            f"(stream at version {existing_version})"
        )

    events = [
        ApplicationSubmitted(payload={
            "application_id": cmd.application_id,
            "applicant_id": cmd.applicant_id,
            "requested_amount_usd": cmd.requested_amount_usd,
            "loan_purpose": cmd.loan_purpose,
            "submission_channel": cmd.submission_channel,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        })
    ]
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=-1,  # new stream
        correlation_id=cmd.correlation_id,
    )


async def handle_request_credit_analysis(
    cmd: RequestCreditAnalysisCommand,
    store: Any,
) -> int:
    """Request credit analysis — transitions SUBMITTED → AWAITING_ANALYSIS."""
    # 1. Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate BR1: state must be SUBMITTED
    app.assert_state(ApplicationState.SUBMITTED)

    # 3. Build event
    events = [
        CreditAnalysisRequested(payload={
            "application_id": cmd.application_id,
            "assigned_agent_id": cmd.assigned_agent_id,
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "priority": cmd.priority,
        })
    ]

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: Any,
) -> int:
    """
    Record completed credit analysis on the loan stream.
    Validates: BR1 (state), BR2 (no duplicate), Gas Town, model version.
    """
    # 1. Load BOTH aggregates (I/O)
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(
        store, cmd.agent_id, cmd.session_id
    )

    # 2. Validate (pure — each aggregate checks its own rules)
    app.assert_awaiting_credit_analysis()           # BR1
    app.assert_no_duplicate_credit_analysis()       # BR2
    agent.assert_context_loaded()                   # Gas Town
    agent.assert_model_version_current(cmd.model_version)  # Model version

    # Get context event position for causal reference
    context_pos = agent.get_context_event_position()

    # 3. Build event
    events = [
        CreditAnalysisCompleted(payload={
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "model_version": cmd.model_version,
            "confidence_score": cmd.confidence_score,
            "risk_tier": cmd.risk_tier,
            "recommended_limit_usd": cmd.recommended_limit_usd,
            "analysis_duration_ms": cmd.analysis_duration_ms,
            "input_data_hash": cmd.input_data_hash,
            "data_quality_caveats": cmd.data_quality_caveats,
            "context_event_position": context_pos,
        })
    ]

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: Any,
) -> int:
    """Record completed fraud screening on the loan stream."""
    # 1. Load aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(
        store, cmd.agent_id, cmd.session_id
    )

    # 2. Validate
    app.assert_state(ApplicationState.ANALYSIS_COMPLETE)  # BR1
    agent.assert_context_loaded()                          # Gas Town

    # 3. Build event
    events = [
        FraudScreeningCompleted(payload={
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "fraud_score": cmd.fraud_score,
            "anomaly_flags": cmd.anomaly_flags,
            "screening_model_version": cmd.screening_model_version,
            "input_data_hash": cmd.input_data_hash,
        })
    ]

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_decision_generated(
    cmd: GenerateDecisionCommand,
    store: Any,
) -> int:
    """
    Record orchestrator decision.
    Validates: BR1 (state), BR3 (confidence floor), BR5 (causal chain).
    """
    # 1. Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate BR1: state
    app.assert_state(ApplicationState.PENDING_DECISION)

    # BR: REFER protection — cannot generate another decision if REFER pending
    app.assert_no_pending_refer()

    # BR3: confidence floor — may override recommendation
    final_recommendation = app.assert_valid_decision(
        cmd.confidence_score, cmd.recommendation
    )

    # BR5: causal chain — load each contributing session (I/O in handler)
    if cmd.contributing_agent_sessions:
        session_events_map: dict[str, list[Any]] = {}
        for session_stream_id in cmd.contributing_agent_sessions:
            session_events = await store.load_stream(session_stream_id)
            if not session_events:
                raise DomainError(
                    f"Causal chain broken: session '{session_stream_id}' "
                    "does not exist"
                )
            session_events_map[session_stream_id] = session_events
        app.assert_valid_causal_chain(session_events_map)

    # 3. Build event
    events = [
        DecisionGenerated(payload={
            "application_id": cmd.application_id,
            "orchestrator_agent_id": cmd.orchestrator_agent_id,
            "recommendation": final_recommendation,
            "confidence_score": cmd.confidence_score,
            "contributing_agent_sessions": cmd.contributing_agent_sessions,
            "decision_basis_summary": cmd.decision_basis_summary,
            "model_versions": cmd.model_versions,
        })
    ]

    # Also emit HumanReviewRequested if REFER
    if final_recommendation == "REFER":
        events.append(HumanReviewRequested(payload={
            "application_id": cmd.application_id,
            "reason": "Confidence below threshold or agent recommendation",
        }))

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_approve_application(
    cmd: ApproveApplicationCommand,
    store: Any,
) -> int:
    """
    Approve the application.
    Validates: BR1 (state), BR4 (compliance must be clear).
    """
    # 1. Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate BR1: state
    app.assert_state(
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.PENDING_DECISION,  # auto-approve path
    )

    # BR4: cross-aggregate read — load compliance stream (I/O in handler)
    compliance_events = await store.load_stream(
        f"compliance-{cmd.application_id}"
    )

    app.assert_compliance_clear(compliance_events)

    # 3. Build event
    events = [
        ApplicationApproved(payload={
            "application_id": cmd.application_id,
            "approved_amount_usd": cmd.approved_amount_usd,
            "interest_rate": cmd.interest_rate,
            "conditions": cmd.conditions,
            "approved_by": cmd.approved_by,
            "effective_date": datetime.now(timezone.utc).isoformat(),
        })
    ]

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store: Any,
) -> int:
    """Record human review outcome."""
    # 1. Load aggregate
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate BR1: state must be pending human review
    app.assert_state(
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.PENDING_DECISION,  # REFER path
    )

    # BR: override_reason required when override=True (moved from aggregate replay)
    if cmd.override and not cmd.override_reason:
        raise DomainError(
            "override_reason is required when override=True"
        )

    # BR4: Compliance check before approval (cross-aggregate read)
    if cmd.final_decision == "APPROVE":
        compliance_events = await store.load_stream(
            f"compliance-{cmd.application_id}"
        )
        app.assert_compliance_clear(compliance_events)

    # 3. Build events
    events = [
        HumanReviewCompleted(payload={
            "application_id": cmd.application_id,
            "reviewer_id": cmd.reviewer_id,
            "override": cmd.override,
            "final_decision": cmd.final_decision,
            "override_reason": cmd.override_reason,
        })
    ]

    # Append final decision event based on human's verdict
    if cmd.final_decision == "APPROVE":
        events.append(ApplicationApproved(payload={
            "application_id": cmd.application_id,
            "approved_amount_usd": app.requested_amount,  # derived from aggregate state
            "interest_rate": 0.0,
            "conditions": [],
            "approved_by": cmd.reviewer_id,
            "effective_date": datetime.now(timezone.utc).isoformat(),
        }))
    elif cmd.final_decision == "DECLINE":
        events.append(ApplicationDeclined(payload={
            "application_id": cmd.application_id,
            "decline_reasons": [cmd.override_reason] if cmd.override_reason else [],
            "declined_by": cmd.reviewer_id,
            "adverse_action_notice_required": True,
        }))

    # 4. Append with explicit OCC
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: Any,
) -> int:
    """
    Start a new agent session with context loaded (Gas Town pattern).
    Creates the agent stream with AgentSessionStarted + AgentContextLoaded.
    This is a precondition for any agent decision tools.
    """
    stream_id = f"agent-{cmd.agent_id}-{cmd.session_id}"
    events = [
        AgentSessionStarted(payload={
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
        }),
        AgentContextLoaded(payload={
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "context_source": cmd.context_source,
            "event_replay_from_position": cmd.event_replay_from_position,
            "context_token_count": cmd.context_token_count,
            "model_version": cmd.model_version,
        }),
    ]
    return await store.append(
        stream_id=stream_id,
        events=events,
        expected_version=-1,  # new stream
        correlation_id=cmd.correlation_id,
    )


async def handle_compliance_check_requested(
    cmd: RequestComplianceCheckCommand,
    store: Any,
) -> int:
    """Request compliance checks — transitions ANALYSIS_COMPLETE → COMPLIANCE_REVIEW."""
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    app.assert_state(ApplicationState.ANALYSIS_COMPLETE)

    events = [
        ComplianceCheckRequested(payload={
            "application_id": cmd.application_id,
            "regulation_set_version": cmd.regulation_set_version,
            "checks_required": cmd.checks_required,
        })
    ]
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_decision_requested(
    cmd: RequestDecisionCommand,
    store: Any,
) -> int:
    """Request decision — transitions COMPLIANCE_REVIEW → PENDING_DECISION."""
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    app.assert_state(ApplicationState.COMPLIANCE_REVIEW)

    events = [
        DecisionRequested(payload={"application_id": cmd.application_id})
    ]
    return await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


# =============================================================================
# Compliance Rule Handlers
# =============================================================================


class RecordComplianceRulePassedCommand(BaseModel):
    application_id: str
    rule_id: str
    rule_version: str = ""
    evaluation_timestamp: str = ""
    evidence_hash: str = ""
    correlation_id: str | None = None
    causation_id: str | None = None


class RecordComplianceRuleFailedCommand(BaseModel):
    application_id: str
    rule_id: str
    rule_version: str = ""
    failure_reason: str = ""
    remediation_required: bool = False
    correlation_id: str | None = None
    causation_id: str | None = None


async def handle_compliance_rule_passed(
    cmd: RecordComplianceRulePassedCommand,
    store: Any,
) -> int:
    """Record a passed compliance rule on the compliance stream."""
    agg = await ComplianceRecordAggregate.load(store, cmd.application_id)
    agg.assert_not_completed()
    agg.assert_rule_not_evaluated(cmd.rule_id)

    events = [
        ComplianceRulePassed(payload={
            "application_id": cmd.application_id,
            "rule_id": cmd.rule_id,
            "rule_version": cmd.rule_version,
            "evaluation_timestamp": cmd.evaluation_timestamp or datetime.now(timezone.utc).isoformat(),
            "evidence_hash": cmd.evidence_hash,
        })
    ]
    expected = -1 if agg.version == 0 else agg.version
    return await store.append(
        stream_id=agg.stream_id,
        events=events,
        expected_version=expected,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_compliance_rule_failed(
    cmd: RecordComplianceRuleFailedCommand,
    store: Any,
) -> int:
    """Record a failed compliance rule on the compliance stream."""
    agg = await ComplianceRecordAggregate.load(store, cmd.application_id)
    agg.assert_not_completed()
    agg.assert_rule_not_evaluated(cmd.rule_id)

    events = [
        ComplianceRuleFailed(payload={
            "application_id": cmd.application_id,
            "rule_id": cmd.rule_id,
            "rule_version": cmd.rule_version,
            "failure_reason": cmd.failure_reason,
            "remediation_required": cmd.remediation_required,
        })
    ]
    expected = -1 if agg.version == 0 else agg.version
    return await store.append(
        stream_id=agg.stream_id,
        events=events,
        expected_version=expected,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
