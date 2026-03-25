"""
The Ledger — ApplicationSummary Projection

Read-optimised view of every loan application's current state.
One row per application_id, updated via UPSERT on each relevant event.

SLO: < 500ms lag in normal operation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from projections.base import InMemoryProjectionStore, Projection


TABLE = "application_summary"


class ApplicationSummaryProjection(Projection):
    """
    Projection 1: Application Summary.

    Subscribes to 15 loan-lifecycle event types and maintains a single
    denormalised row per application with current state.
    """

    def __init__(self, store: InMemoryProjectionStore | None = None) -> None:
        self._store = store or InMemoryProjectionStore()
        self._checkpoint: int = -1

    @property
    def name(self) -> str:
        return "application_summary"

    @property
    def subscribed_events(self) -> set[str]:
        return {
            "ApplicationSubmitted",
            "DocumentUploadRequested",
            "DocumentUploaded",
            "CreditAnalysisRequested",
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "ComplianceCheckRequested",
            "ComplianceCheckCompleted",
            "DecisionRequested",
            "DecisionGenerated",
            "HumanReviewRequested",
            "HumanReviewCompleted",
            "ApplicationApproved",
            "ApplicationDeclined",
            "AgentSessionCompleted",
        }

    async def handle(self, event: dict) -> None:
        """Process a single event dict. Idempotent via UPSERT."""
        payload = event.get("payload", {})
        app_id = payload.get("application_id", "")
        event_type = event.get("event_type", "")

        if not app_id:
            # Some events (AgentSessionCompleted) may not have application_id directly
            # Skip those that don't—they update via session tracking
            if event_type == "AgentSessionCompleted":
                await self._handle_session_completed(event)
                return
            return

        # Base update: always set last_event_type and last_event_at
        recorded_at = event.get("recorded_at")
        if isinstance(recorded_at, str):
            recorded_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        
        update: dict[str, Any] = {
            "application_id": app_id,
            "last_event_type": event_type,
            "last_event_at": recorded_at,
        }

        handler = getattr(self, f"_handle_{event_type}", None)
        if handler:
            import asyncio
            if asyncio.iscoroutinefunction(handler):
                await handler(payload, update)
            else:
                handler(payload, update)

        await self._store.upsert(TABLE, app_id, update)

    def _handle_ApplicationSubmitted(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "SUBMITTED"
        update["applicant_id"] = payload.get("applicant_id")
        update["requested_amount_usd"] = payload.get("requested_amount_usd")
        
        submitted_at = payload.get("submitted_at")
        if submitted_at:
            from datetime import datetime
            try:
                # Handle possible trailing Z or +00:00
                if isinstance(submitted_at, str):
                    ts = submitted_at.replace("Z", "+00:00")
                    update["submitted_at"] = datetime.fromisoformat(ts)
                else: # already datetime
                    update["submitted_at"] = submitted_at
            except (ValueError, TypeError):
                update["submitted_at"] = None

    def _handle_DocumentUploadRequested(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "DOCUMENTS_REQUESTED"

    def _handle_DocumentUploaded(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "DOCUMENTS_UPLOADED"

    def _handle_CreditAnalysisRequested(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "AWAITING_ANALYSIS"

    def _handle_CreditAnalysisCompleted(
        self, payload: dict, update: dict
    ) -> None:
        update["risk_tier"] = payload.get("risk_tier") or payload.get("decision", {}).get("risk_tier")
        update["state"] = "ANALYSIS_COMPLETE"

    def _handle_FraudScreeningCompleted(
        self, payload: dict, update: dict
    ) -> None:
        update["fraud_score"] = payload.get("fraud_score")

    def _handle_ComplianceCheckRequested(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "COMPLIANCE_REVIEW"
        update["compliance_status"] = "PENDING"

    def _handle_ComplianceCheckCompleted(
        self, payload: dict, update: dict
    ) -> None:
        update["compliance_status"] = payload.get("overall_verdict", "CLEAR")

    def _handle_DecisionRequested(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "PENDING_DECISION"

    def _handle_DecisionGenerated(
        self, payload: dict, update: dict
    ) -> None:
        recommendation = payload.get("recommendation", "")
        update["decision"] = recommendation
        if recommendation == "APPROVE":
            update["state"] = "APPROVED_PENDING_HUMAN"
        elif recommendation == "DECLINE":
            update["state"] = "DECLINED_PENDING_HUMAN"
        # REFER stays PENDING_DECISION

    def _handle_HumanReviewRequested(
        self, payload: dict, update: dict
    ) -> None:
        pass  # No state change — just records the request

    def _handle_HumanReviewCompleted(
        self, payload: dict, update: dict
    ) -> None:
        update["human_reviewer_id"] = payload.get("reviewer_id")

    def _handle_ApplicationApproved(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "FINAL_APPROVED"
        update["approved_amount_usd"] = payload.get("approved_amount_usd")
        update["final_decision_at"] = datetime.now(timezone.utc)

    def _handle_ApplicationDeclined(
        self, payload: dict, update: dict
    ) -> None:
        update["state"] = "FINAL_DECLINED"
        update["final_decision_at"] = datetime.now(timezone.utc)

    async def _handle_session_completed(self, event: dict) -> None:
        """Track completed agent sessions for applications."""
        # AgentSessionCompleted may not have application_id in payload
        # In a full system, we'd correlate via session stream.
        # For now, we track via the session stream metadata.
        pass

    async def rebuild_from_scratch(self) -> None:
        """Truncate and reset to zero."""
        await self._store.truncate(TABLE)
        self._checkpoint = -1

    # -- Query methods --

    def get_summary(self, application_id: str) -> dict[str, Any] | None:
        """Get the current summary for an application."""
        return self._store.get(TABLE, application_id)

    def get_all_summaries(self) -> list[dict[str, Any]]:
        """Get all application summaries."""
        return self._store.get_all(TABLE)

    def get_by_state(self, state: str) -> list[dict[str, Any]]:
        """Get all applications in a given state."""
        return self._store.query(TABLE, lambda r: r.get("state") == state)

    def count(self) -> int:
        """Total number of applications tracked."""
        return self._store.count(TABLE)
