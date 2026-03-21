"""
The Ledger — LoanApplicationAggregate

Reconstructs LoanApplication state by replaying the loan-{application_id} event stream.
Enforces the application state machine and 5 business rules.

Business Rules (pure — no I/O):
  BR1: Valid state transitions only (DomainError on out-of-order)
  BR2: No duplicate CreditAnalysisCompleted (unless HumanReviewOverride)
  BR3: Confidence < 0.6 → force recommendation = "REFER"
  BR4: Cannot approve without all compliance checks passed
  BR5: Causal chain — contributing sessions must reference valid, ordered sessions

Stream Routing Note:
  The Phase 2 spec handler code appends CreditAnalysisCompleted to loan-{id}
  so that this aggregate can transition AWAITING_ANALYSIS → ANALYSIS_COMPLETE.
  The Event Catalogue lists it under AgentSession — this is a documented spec
  contradiction. We follow the handler code for Phase 2 state machine needs.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from models.events import DomainError, StoredEvent

if TYPE_CHECKING:
    from event_store import EventStore
    from in_memory_store import InMemoryEventStore


# =============================================================================
# Application State Machine
# =============================================================================


class ApplicationState(str, Enum):
    """Exact states from the spec state machine."""

    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


# Valid transitions: from_state -> set of allowed to_states
VALID_TRANSITIONS: dict[ApplicationState | None, set[ApplicationState]] = {
    None: {ApplicationState.SUBMITTED},
    ApplicationState.SUBMITTED: {ApplicationState.AWAITING_ANALYSIS},
    ApplicationState.AWAITING_ANALYSIS: {ApplicationState.ANALYSIS_COMPLETE},
    ApplicationState.ANALYSIS_COMPLETE: {ApplicationState.COMPLIANCE_REVIEW},
    ApplicationState.COMPLIANCE_REVIEW: {ApplicationState.PENDING_DECISION},
    ApplicationState.PENDING_DECISION: {
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.FINAL_APPROVED,   # auto-approve (no human review needed)
        ApplicationState.FINAL_DECLINED,   # auto-decline
    },
    ApplicationState.APPROVED_PENDING_HUMAN: {
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,   # human override can decline
    },
    ApplicationState.DECLINED_PENDING_HUMAN: {
        ApplicationState.FINAL_DECLINED,
        ApplicationState.FINAL_APPROVED,   # human override can approve (NARR-05)
    },
    # Terminal states — no further transitions
    ApplicationState.FINAL_APPROVED: set(),
    ApplicationState.FINAL_DECLINED: set(),
}


# =============================================================================
# LoanApplicationAggregate
# =============================================================================


class LoanApplicationAggregate:
    """
    Reconstructs loan application state from events.

    Pure domain logic — no I/O. The load() classmethod is the only method
    that touches the store; all assert_* methods receive pre-loaded data.
    """

    def __init__(self, application_id: str) -> None:
        self.application_id = application_id
        self.version: int = 0
        self.state: ApplicationState | None = None

        # Core data
        self.applicant_id: str | None = None
        self.requested_amount: float = 0.0

        # Analysis tracking
        self.credit_analysis_completed: bool = False
        self.credit_session_id: str | None = None
        self.credit_confidence: float | None = None
        self.risk_tier: str | None = None
        self.fraud_score: float | None = None
        self.fraud_session_id: str | None = None

        # Compliance tracking
        self.required_checks: list[str] = []

        # Decision tracking
        self.recommendation: str | None = None
        self.decision_confidence: float | None = None
        self.contributing_sessions: list[str] = []
        self.human_review_override: bool = False

        # Final outcome
        self.approved_amount: float | None = None
        self.decline_reasons: list[str] = []
        self.documents_uploaded: int = 0

    @classmethod
    async def load(
        cls,
        store: "EventStore | InMemoryEventStore",
        application_id: str,
    ) -> "LoanApplicationAggregate":
        """Replay loan-{application_id} stream to reconstruct state."""
        from models.events import StreamNotFoundError

        agg = cls(application_id=application_id)
        try:
            events = await store.load_stream(f"loan-{application_id}")
        except StreamNotFoundError:
            # Stream doesn't exist yet — return empty aggregate at version 0
            return agg

        for event in events:
            agg._apply(event)
        return agg

    # -------------------------------------------------------------------------
    # Event replay
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        """Route event to the correct handler by event_type."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _validate_transition(self, new_state: ApplicationState) -> None:
        """BR1: Raise DomainError if transition is invalid."""
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if new_state not in allowed:
            raise DomainError(
                f"Invalid state transition: {self.state} → {new_state}. "
                f"Allowed: {allowed}"
            )

    # --- Loan lifecycle event handlers ---

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.SUBMITTED)
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload.get("applicant_id")
        self.requested_amount = event.payload.get("requested_amount_usd", 0.0)

    def _on_DocumentUploadRequested(self, event: StoredEvent) -> None:
        # Stays SUBMITTED — sub-state tracking only
        pass

    def _on_DocumentUploaded(self, event: StoredEvent) -> None:
        # Stays SUBMITTED — increment upload counter
        self.documents_uploaded += 1

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.AWAITING_ANALYSIS)
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        # Stream routing note: appended to loan-{id} per Phase 2 handler code
        self._validate_transition(ApplicationState.ANALYSIS_COMPLETE)
        self.state = ApplicationState.ANALYSIS_COMPLETE
        self.credit_analysis_completed = True
        self.credit_session_id = event.payload.get("session_id")
        self.credit_confidence = event.payload.get("confidence_score")
        self.risk_tier = event.payload.get("risk_tier")

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        # Stays ANALYSIS_COMPLETE — fraud is a parallel analysis step
        self.fraud_score = event.payload.get("fraud_score")
        self.fraud_session_id = event.payload.get("session_id")

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.COMPLIANCE_REVIEW)
        self.state = ApplicationState.COMPLIANCE_REVIEW
        self.required_checks = event.payload.get("checks_required", [])

    def _on_DecisionRequested(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.PENDING_DECISION)
        self.state = ApplicationState.PENDING_DECISION

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        recommendation = event.payload.get("recommendation", "")
        self.recommendation = recommendation
        self.decision_confidence = event.payload.get("confidence_score")
        self.contributing_sessions = event.payload.get(
            "contributing_agent_sessions", []
        )

        # DecisionGenerated determines the pending direction
        if recommendation == "APPROVE":
            self._validate_transition(ApplicationState.APPROVED_PENDING_HUMAN)
            self.state = ApplicationState.APPROVED_PENDING_HUMAN
        elif recommendation == "DECLINE":
            self._validate_transition(ApplicationState.DECLINED_PENDING_HUMAN)
            self.state = ApplicationState.DECLINED_PENDING_HUMAN
        # REFER stays in PENDING_DECISION — needs human review first

    def _on_HumanReviewRequested(self, event: StoredEvent) -> None:
        # Stays in current pending state — signals review was initiated
        pass

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        override = event.payload.get("override", False)
        if override:
            self.human_review_override = True

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.FINAL_APPROVED)
        self.state = ApplicationState.FINAL_APPROVED
        self.approved_amount = event.payload.get("approved_amount_usd")

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self._validate_transition(ApplicationState.FINAL_DECLINED)
        self.state = ApplicationState.FINAL_DECLINED
        self.decline_reasons = event.payload.get("decline_reasons", [])

    # -------------------------------------------------------------------------
    # Business rule assertions (pure — no I/O)
    # -------------------------------------------------------------------------

    def assert_state(self, *expected_states: ApplicationState) -> None:
        """Assert aggregate is in one of the expected states."""
        if self.state not in expected_states:
            raise DomainError(
                f"Expected state {expected_states}, got {self.state}"
            )

    def assert_awaiting_credit_analysis(self) -> None:
        """Assert application is waiting for credit analysis."""
        self.assert_state(ApplicationState.AWAITING_ANALYSIS)

    def assert_no_duplicate_credit_analysis(self) -> None:
        """BR2: No duplicate CreditAnalysisCompleted unless HumanReviewOverride."""
        if self.credit_analysis_completed and not self.human_review_override:
            raise DomainError(
                "CreditAnalysisCompleted already exists for this application. "
                "Duplicate analysis blocked unless superseded by HumanReviewOverride."
            )

    def assert_valid_decision(
        self, confidence: float, recommendation: str
    ) -> str:
        """
        BR3: Confidence < 0.6 → force REFER.
        Returns the (possibly overridden) recommendation.
        """
        if confidence < 0.6 and recommendation != "REFER":
            return "REFER"
        return recommendation

    def assert_compliance_clear(
        self,
        compliance_events: list[StoredEvent],
        required_checks: list[str] | None = None,
    ) -> None:
        """
        BR4: Cannot approve without all required compliance checks passed.
        Takes pre-loaded compliance stream events (loaded by the handler).
        """
        checks = required_checks or self.required_checks
        if not checks:
            # No required checks defined — pass
            return

        passed_rules: set[str] = set()
        failed_rules: set[str] = set()
        for event in compliance_events:
            if event.event_type == "ComplianceRulePassed":
                passed_rules.add(event.payload.get("rule_id", ""))
            elif event.event_type == "ComplianceRuleFailed":
                failed_rules.add(event.payload.get("rule_id", ""))

        missing = set(checks) - passed_rules
        if missing:
            raise DomainError(
                f"Cannot approve: compliance checks not passed: {missing}"
            )
        if failed_rules:
            raise DomainError(
                f"Cannot approve: compliance checks failed: {failed_rules}"
            )

    def assert_valid_causal_chain(
        self,
        session_events_map: dict[str, list[StoredEvent]],
    ) -> None:
        """
        BR5: Every contributing session must have a decision event for
        this application_id, and sessions must exist.
        Takes a dict of {session_stream_id: [events]} loaded by the handler.
        """
        for session_id, events in session_events_map.items():
            if not events:
                raise DomainError(
                    f"Causal chain broken: session '{session_id}' has no events"
                )

            # Check session contains a decision-type event for this application
            has_decision = False
            for event in events:
                payload = event.payload
                if event.event_type in (
                    "CreditAnalysisCompleted",
                    "FraudScreeningCompleted",
                    "ComplianceCheckCompleted",
                ):
                    if payload.get("application_id") == self.application_id:
                        has_decision = True
                        break

            if not has_decision:
                raise DomainError(
                    f"Causal chain broken: session '{session_id}' has no "
                    f"decision event for application '{self.application_id}'"
                )
