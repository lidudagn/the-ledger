"""
The Ledger — Canonical Event Schema & Domain Models

This file is the single source of truth for all event types in the system.
Every agent, every test, every projection imports from here.

Contains:
- BaseEvent, StoredEvent, StreamMetadata (core models)
- 45 canonical event types (pure data contracts — no domain logic)
- Error types (OptimisticConcurrencyError, DomainError, etc.)
- EVENT_REGISTRY with duplicate-safe registration
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# =============================================================================
# EVENT REGISTRY
# =============================================================================

EVENT_REGISTRY: dict[str, type["BaseEvent"]] = {}


def register(event_cls: type["BaseEvent"]) -> type["BaseEvent"]:
    """Register an event class in the global registry. Raises on duplicates."""
    event_type = event_cls.model_fields["event_type"].default
    if event_type in EVENT_REGISTRY:
        raise ValueError(f"Duplicate event_type registration: {event_type}")
    EVENT_REGISTRY[event_type] = event_cls
    return event_cls


# =============================================================================
# CORE MODELS
# =============================================================================


class BaseEvent(BaseModel):
    """
    Base class for all domain events. Pure data contract.

    Contract C4: metadata must contain at minimum:
      - correlation_id (str | None): groups related events across streams
      - causation_id (str | None): the event that directly caused this one
    EventStore.append() constructs metadata — callers do not populate it directly.
    """

    event_type: str
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredEvent(BaseEvent):
    """
    An event as persisted in the events table.
    Returned by load_stream() and load_all().
    """

    event_id: uuid.UUID
    stream_id: str
    stream_position: int
    global_position: int
    recorded_at: datetime


class StreamMetadata(BaseModel):
    """Metadata for an event stream from the event_streams table."""

    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# ERROR TYPES
# =============================================================================


class DomainError(Exception):
    """Base error for domain logic violations (Phase 2+)."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class OptimisticConcurrencyError(DomainError):
    """
    Raised when append() detects a version mismatch.
    The caller must reload the stream and retry.
    """

    def __init__(
        self, stream_id: str, expected_version: int, actual_version: int
    ) -> None:
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Concurrency conflict on stream '{stream_id}': "
            f"expected version {expected_version}, actual {actual_version}"
        )


class StreamNotFoundError(DomainError):
    """Raised when loading a stream that does not exist."""

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        super().__init__(f"Stream not found: '{stream_id}'")


class StreamArchivedError(DomainError):
    """Raised when attempting to append to an archived stream."""

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        super().__init__(f"Stream is archived: '{stream_id}'")


# =============================================================================
# LOAN APPLICATION EVENTS (loan-{id}) — 13 events
# =============================================================================


@register
class ApplicationSubmitted(BaseEvent):
    """A new loan application has been submitted."""

    event_type: str = "ApplicationSubmitted"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "applicant_id": "",
        "requested_amount_usd": 0.0,
        "loan_purpose": "",
        "submission_channel": "",
        "submitted_at": "",
    })


@register
class DocumentUploadRequested(BaseEvent):
    """A document upload has been requested for an application."""

    event_type: str = "DocumentUploadRequested"
    event_version: int = 1


@register
class DocumentUploaded(BaseEvent):
    """A document has been successfully uploaded."""

    event_type: str = "DocumentUploaded"
    event_version: int = 1


@register
class DocumentUploadFailed(BaseEvent):
    """A document upload has failed (reasoned addition)."""

    event_type: str = "DocumentUploadFailed"
    event_version: int = 1


@register
class CreditAnalysisRequested(BaseEvent):
    """Credit analysis has been requested for an application."""

    event_type: str = "CreditAnalysisRequested"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "assigned_agent_id": "",
        "requested_at": "",
        "priority": "",
    })


@register
class FraudScreeningRequested(BaseEvent):
    """Fraud screening has been requested."""

    event_type: str = "FraudScreeningRequested"
    event_version: int = 1


@register
class ComplianceCheckRequested(BaseEvent):
    """Compliance check has been requested for an application."""

    event_type: str = "ComplianceCheckRequested"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "regulation_set_version": "",
        "checks_required": [],
    })


@register
class DecisionRequested(BaseEvent):
    """A decision has been requested from the orchestrator."""

    event_type: str = "DecisionRequested"
    event_version: int = 1


@register
class DecisionGenerated(BaseEvent):
    """The orchestrator has generated a decision recommendation."""

    event_type: str = "DecisionGenerated"
    event_version: int = 2
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "orchestrator_agent_id": "",
        "recommendation": "",
        "confidence_score": 0.0,
        "contributing_agent_sessions": [],
        "decision_basis_summary": "",
        "model_versions": {},
    })


@register
class HumanReviewRequested(BaseEvent):
    """A human review has been requested."""

    event_type: str = "HumanReviewRequested"
    event_version: int = 1


@register
class HumanReviewCompleted(BaseEvent):
    """A human reviewer has completed their review."""

    event_type: str = "HumanReviewCompleted"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "reviewer_id": "",
        "override": False,
        "final_decision": "",
        "override_reason": "",
    })


@register
class ApplicationApproved(BaseEvent):
    """The loan application has been approved."""

    event_type: str = "ApplicationApproved"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "approved_amount_usd": 0.0,
        "interest_rate": 0.0,
        "conditions": [],
        "approved_by": "",
        "effective_date": "",
    })


@register
class ApplicationDeclined(BaseEvent):
    """The loan application has been declined."""

    event_type: str = "ApplicationDeclined"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "decline_reasons": [],
        "declined_by": "",
        "adverse_action_notice_required": False,
    })


# =============================================================================
# DOCUMENT PACKAGE EVENTS (docpkg-{id}) — 8 events
# =============================================================================


@register
class PackageCreated(BaseEvent):
    """A new document package has been created."""

    event_type: str = "PackageCreated"
    event_version: int = 1


@register
class DocumentAdded(BaseEvent):
    """A document has been added to a package."""

    event_type: str = "DocumentAdded"
    event_version: int = 1


@register
class DocumentFormatValidated(BaseEvent):
    """A document's format has been validated."""

    event_type: str = "DocumentFormatValidated"
    event_version: int = 1


@register
class ExtractionStarted(BaseEvent):
    """Data extraction has started on a document."""

    event_type: str = "ExtractionStarted"
    event_version: int = 1


@register
class ExtractionCompleted(BaseEvent):
    """Data extraction has completed successfully."""

    event_type: str = "ExtractionCompleted"
    event_version: int = 1


@register
class ExtractionFailed(BaseEvent):
    """Data extraction has failed (reasoned addition — rubric references this)."""

    event_type: str = "ExtractionFailed"
    event_version: int = 1


@register
class QualityAssessmentCompleted(BaseEvent):
    """Quality assessment of extracted data has completed."""

    event_type: str = "QualityAssessmentCompleted"
    event_version: int = 1


@register
class PackageReadyForAnalysis(BaseEvent):
    """The document package is ready for analysis."""

    event_type: str = "PackageReadyForAnalysis"
    event_version: int = 1


# =============================================================================
# AGENT SESSION EVENTS (agent-{type}-{session_id}) — 10 events
# =============================================================================


@register
class AgentSessionStarted(BaseEvent):
    """An AI agent session has started."""

    event_type: str = "AgentSessionStarted"
    event_version: int = 1


@register
class AgentContextLoaded(BaseEvent):
    """Agent has loaded its context (Gas Town pattern — required before decisions)."""

    event_type: str = "AgentContextLoaded"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "agent_id": "",
        "session_id": "",
        "context_source": "",
        "event_replay_from_position": 0,
        "context_token_count": 0,
        "model_version": "",
    })


@register
class AgentInputValidated(BaseEvent):
    """Agent input has been validated successfully."""

    event_type: str = "AgentInputValidated"
    event_version: int = 1


@register
class AgentInputValidationFailed(BaseEvent):
    """Agent input validation has failed."""

    event_type: str = "AgentInputValidationFailed"
    event_version: int = 1


@register
class AgentNodeExecuted(BaseEvent):
    """An agent graph node has been executed."""

    event_type: str = "AgentNodeExecuted"
    event_version: int = 1


@register
class AgentToolCalled(BaseEvent):
    """An agent has called a tool."""

    event_type: str = "AgentToolCalled"
    event_version: int = 1


@register
class AgentOutputWritten(BaseEvent):
    """Agent has written its output."""

    event_type: str = "AgentOutputWritten"
    event_version: int = 1


@register
class AgentSessionCompleted(BaseEvent):
    """Agent session has completed successfully."""

    event_type: str = "AgentSessionCompleted"
    event_version: int = 1


@register
class AgentSessionFailed(BaseEvent):
    """Agent session has failed."""

    event_type: str = "AgentSessionFailed"
    event_version: int = 1


@register
class AgentSessionRecovered(BaseEvent):
    """Agent session has recovered from a failure."""

    event_type: str = "AgentSessionRecovered"
    event_version: int = 1


# =============================================================================
# CREDIT RECORD EVENTS (credit-{id}) — 5 events
# =============================================================================


@register
class CreditRecordOpened(BaseEvent):
    """A credit record has been opened for analysis."""

    event_type: str = "CreditRecordOpened"
    event_version: int = 1


@register
class HistoricalProfileConsumed(BaseEvent):
    """Historical credit profile data has been consumed."""

    event_type: str = "HistoricalProfileConsumed"
    event_version: int = 1


@register
class ExtractedFactsConsumed(BaseEvent):
    """Extracted financial facts have been consumed by the analysis."""

    event_type: str = "ExtractedFactsConsumed"
    event_version: int = 1


@register
class CreditAnalysisCompleted(BaseEvent):
    """Credit analysis has been completed."""

    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "agent_id": "",
        "session_id": "",
        "model_version": "",
        "confidence_score": 0.0,
        "risk_tier": "",
        "recommended_limit_usd": 0.0,
        "analysis_duration_ms": 0,
        "input_data_hash": "",
    })


@register
class CreditAnalysisDeferred(BaseEvent):
    """Credit analysis has been deferred (insufficient data or pending review)."""

    event_type: str = "CreditAnalysisDeferred"
    event_version: int = 1


# =============================================================================
# FRAUD SCREENING EVENTS (fraud-{id}) — 3 events
# =============================================================================


@register
class FraudScreeningInitiated(BaseEvent):
    """Fraud screening has been initiated."""

    event_type: str = "FraudScreeningInitiated"
    event_version: int = 1


@register
class FraudAnomalyDetected(BaseEvent):
    """A fraud anomaly has been detected during screening."""

    event_type: str = "FraudAnomalyDetected"
    event_version: int = 1


@register
class FraudScreeningCompleted(BaseEvent):
    """Fraud screening has been completed."""

    event_type: str = "FraudScreeningCompleted"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "agent_id": "",
        "fraud_score": 0.0,
        "anomaly_flags": [],
        "screening_model_version": "",
        "input_data_hash": "",
    })


# =============================================================================
# COMPLIANCE RECORD EVENTS (compliance-{id}) — 5 events
# =============================================================================


@register
class ComplianceCheckInitiated(BaseEvent):
    """A compliance check has been initiated."""

    event_type: str = "ComplianceCheckInitiated"
    event_version: int = 1


@register
class ComplianceRulePassed(BaseEvent):
    """A compliance rule has passed."""

    event_type: str = "ComplianceRulePassed"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "rule_id": "",
        "rule_version": "",
        "evaluation_timestamp": "",
        "evidence_hash": "",
    })


@register
class ComplianceRuleFailed(BaseEvent):
    """A compliance rule has failed."""

    event_type: str = "ComplianceRuleFailed"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "application_id": "",
        "rule_id": "",
        "rule_version": "",
        "failure_reason": "",
        "remediation_required": False,
    })


@register
class ComplianceRuleNoted(BaseEvent):
    """A compliance rule has been noted (informational, not pass/fail)."""

    event_type: str = "ComplianceRuleNoted"
    event_version: int = 1


@register
class ComplianceCheckCompleted(BaseEvent):
    """All compliance checks for an application have completed."""

    event_type: str = "ComplianceCheckCompleted"
    event_version: int = 1


# =============================================================================
# AUDIT LEDGER EVENTS (audit-{entity_type}-{entity_id}) — 1 event
# =============================================================================


@register
class AuditIntegrityCheckRun(BaseEvent):
    """An integrity check has been run on an audit chain."""

    event_type: str = "AuditIntegrityCheckRun"
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=lambda: {
        "entity_id": "",
        "check_timestamp": "",
        "events_verified_count": 0,
        "integrity_hash": "",
        "previous_hash": "",
    })
