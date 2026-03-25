"""
The Ledger — AgentPerformanceLedger Projection

Aggregated performance metrics per AI agent model version.
Answers: "Has agent v2.3 been making systematically different decisions than v2.2?"

SLO: < 500ms lag.
"""

from __future__ import annotations

from typing import Any
from inspect import iscoroutinefunction

from projections.base import InMemoryProjectionStore, Projection


TABLE = "agent_performance"


class AgentPerformanceLedgerProjection(Projection):
    """
    Projection 2: Agent Performance Ledger.

    Subscribes to 4 event types. Maintains running averages and counts
    per (agent_id, model_version) pair.
    """

    def __init__(self, store: InMemoryProjectionStore | None = None) -> None:
        self._store = store or InMemoryProjectionStore()
        self._checkpoint: int = -1

    @property
    def name(self) -> str:
        return "agent_performance_ledger"

    @property
    def subscribed_events(self) -> set[str]:
        return {
            "CreditAnalysisCompleted",
            "FraudScreeningCompleted",
            "DecisionGenerated",
            "HumanReviewCompleted",
        }

    async def handle(self, event: dict) -> None:
        """Process a single event dict. Idempotent via UPSERT with running averages."""
        event_type = event.get("event_type", "")
        handler = getattr(self, f"_handle_{event_type}", None)
        if handler:
            if iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)

    async def _handle_CreditAnalysisCompleted(self, event: dict) -> None:
        payload = event.get("payload", {})
        agent_id = payload.get("agent_id", "unknown")
        # In CreditAnalysisCompleted the payload has model_version directly
        model_version = payload.get("model_version", "unknown")
        key = (agent_id, model_version)

        existing = self._store.get(TABLE, key) or self._default_row(
            agent_id, model_version
        )

        old_count = existing["analyses_completed"]
        new_count = old_count + 1

        confidence = payload.get("decision", {}).get("confidence") or payload.get("confidence_score")
        if confidence is not None:
            old_avg = float(existing.get("avg_confidence_score") or 0.0)
            existing["avg_confidence_score"] = (
                (old_avg * old_count) + confidence
            ) / new_count

        duration = payload.get("analysis_duration_ms")
        if duration is not None:
            old_avg_dur = float(existing.get("avg_duration_ms") or 0.0)
            existing["avg_duration_ms"] = (
                (old_avg_dur * old_count) + duration
            ) / new_count

        existing["analyses_completed"] = new_count
        existing["last_seen_at"] = event.get("recorded_at")
        if not existing.get("first_seen_at"):
            existing["first_seen_at"] = event.get("recorded_at")

        await self._store.upsert(TABLE, key, existing)

    async def _handle_FraudScreeningCompleted(self, event: dict) -> None:
        payload = event.get("payload", {})
        agent_id = payload.get("agent_id", "unknown")
        model_version = payload.get("screening_model_version", "unknown")
        key = (agent_id, model_version)

        existing = self._store.get(TABLE, key) or self._default_row(
            agent_id, model_version
        )

        existing["analyses_completed"] = existing["analyses_completed"] + 1
        existing["last_seen_at"] = event.get("recorded_at")
        if not existing.get("first_seen_at"):
            existing["first_seen_at"] = event.get("recorded_at")

        await self._store.upsert(TABLE, key, existing)

    async def _handle_DecisionGenerated(self, event: dict) -> None:
        payload = event.get("payload", {})
        agent_id = payload.get("orchestrator_agent_id", "unknown")
        # Use first model version from model_versions dict, or "unknown"
        model_versions = payload.get("model_versions", {})
        model_version = next(iter(model_versions.values()), "unknown") if model_versions else "unknown"
        key = (agent_id, model_version)

        existing = self._store.get(TABLE, key) or self._default_row(
            agent_id, model_version
        )

        existing["decisions_generated"] = existing["decisions_generated"] + 1
        recommendation = payload.get("recommendation", "")
        if recommendation == "APPROVE":
            existing["approve_count"] = existing.get("approve_count", 0) + 1
        elif recommendation == "DECLINE":
            existing["decline_count"] = existing.get("decline_count", 0) + 1
        elif recommendation == "REFER":
            existing["refer_count"] = existing.get("refer_count", 0) + 1

        existing["last_seen_at"] = event.get("recorded_at")
        if not existing.get("first_seen_at"):
            existing["first_seen_at"] = event.get("recorded_at")

        await self._store.upsert(TABLE, key, existing)

    async def _handle_HumanReviewCompleted(self, event: dict) -> None:
        payload = event.get("payload", {})
        if payload.get("override"):
            # Attribute override to a generic "human_review" agent
            key = ("human_review", "manual")
            existing = self._store.get(TABLE, key) or self._default_row(
                "human_review", "manual"
            )
            existing["human_override_count"] = (
                existing.get("human_override_count", 0) + 1
            )
            existing["last_seen_at"] = event.get("recorded_at")
            if not existing.get("first_seen_at"):
                existing["first_seen_at"] = event.get("recorded_at")
            await self._store.upsert(TABLE, key, existing)

    def _default_row(self, agent_id: str, model_version: str) -> dict[str, Any]:
        return {
            "agent_id": agent_id,
            "model_version": model_version,
            "analyses_completed": 0,
            "decisions_generated": 0,
            "avg_confidence_score": None,
            "avg_duration_ms": None,
            "approve_count": 0,
            "decline_count": 0,
            "refer_count": 0,
            "human_override_count": 0,
            "first_seen_at": None,
            "last_seen_at": None,
        }

    async def rebuild_from_scratch(self) -> None:
        """Truncate and reset."""
        await self._store.truncate(TABLE)
        self._checkpoint = -1

    # -- Query methods --

    def get_performance(
        self, agent_id: str, model_version: str
    ) -> dict[str, Any] | None:
        """Get metrics for a specific agent + version combo."""
        return self._store.get(TABLE, (agent_id, model_version))

    def get_all_performance(self) -> list[dict[str, Any]]:
        """Get all agent performance rows."""
        return self._store.get_all(TABLE)
