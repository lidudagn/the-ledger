"""
The Ledger — ComplianceAuditView Projection

Regulatory read model for compliance examination. Stores every compliance
event and supports temporal queries ("What was the compliance state at time T?").

Unlike other projections, this one is append-only (not UPSERT) and uses
snapshots triggered on ComplianceCheckCompleted for efficient temporal queries.

SLO: < 2000ms lag (regulatory — allowed higher lag, must be complete).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from projections.base import InMemoryProjectionStore, Projection


EVENTS_TABLE = "compliance_audit_events"
SNAPSHOTS_TABLE = "compliance_snapshots"


class ComplianceAuditViewProjection(Projection):
    """
    Projection 3: Compliance Audit View.

    Append-only event log per application + temporal query support.
    Snapshots created on ComplianceCheckCompleted for efficient historical lookups.
    """

    def __init__(self, store: InMemoryProjectionStore | None = None) -> None:
        self._store = store or InMemoryProjectionStore()
        self._checkpoint: int = -1

    @property
    def name(self) -> str:
        return "compliance_audit_view"

    @property
    def subscribed_events(self) -> set[str]:
        return {
            "ComplianceCheckInitiated",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted",
        }

    async def handle(self, event: dict) -> None:
        """
        Process a compliance event. Idempotent via INSERT ON CONFLICT DO NOTHING
        keyed on global_position.
        """
        payload = event.get("payload", {})
        app_id = payload.get("application_id", "")
        event_type = event.get("event_type", "")
        global_position = event.get("global_position", 0)
        recorded_at = event.get("recorded_at")

        # Build the audit event row
        row = {
            "application_id": app_id,
            "event_type": event_type,
            "rule_id": payload.get("rule_id"),
            "rule_version": payload.get("rule_version"),
            "failure_reason": payload.get("failure_reason"),
            "is_hard_block": payload.get("is_hard_block", False),
            "regulation_version": payload.get("regulation_set_version"),
            "evidence_hash": payload.get("evidence_hash"),
            "global_position": global_position,
            "recorded_at": recorded_at,
            "payload": payload,
        }

        # Determine verdict
        if event_type == "ComplianceRulePassed":
            row["verdict"] = "PASSED"
        elif event_type == "ComplianceRuleFailed":
            row["verdict"] = "FAILED"
        elif event_type == "ComplianceRuleNoted":
            row["verdict"] = "NOTED"
        elif event_type == "ComplianceCheckInitiated":
            row["verdict"] = "INITIATED"
        elif event_type == "ComplianceCheckCompleted":
            row["verdict"] = "COMPLETED"

        # Insert (idempotent: skip if global_position already exists)
        await self._store.insert_if_absent(
            EVENTS_TABLE, global_position, row
        )

        # On ComplianceCheckCompleted → create snapshot
        if event_type == "ComplianceCheckCompleted":
            await self._create_snapshot(app_id, event)

    async def _create_snapshot(
        self, application_id: str, completed_event: dict
    ) -> None:
        """Create a point-in-time compliance snapshot."""
        global_position = completed_event.get("global_position", 0)
        
        # Gather all compliance events for this application up to this point
        all_events = self._store.query(
            EVENTS_TABLE,
            lambda r: (
                r.get("application_id") == application_id
                and r.get("global_position", 0)
                <= global_position
            ),
        )

        # Compute snapshot state
        rules_evaluated = []
        has_hard_block = False
        payload = completed_event.get("payload", {})
        overall_verdict = payload.get(
            "overall_verdict", "CLEAR"
        )

        for evt in all_events:
            if evt.get("rule_id"):
                rules_evaluated.append(
                    {
                        "rule_id": evt["rule_id"],
                        "verdict": evt.get("verdict"),
                        "rule_version": evt.get("rule_version"),
                        "failure_reason": evt.get("failure_reason"),
                        "is_hard_block": evt.get("is_hard_block", False),
                    }
                )
            if evt.get("is_hard_block") and evt.get("verdict") == "FAILED":
                has_hard_block = True

        recorded_at = completed_event.get("recorded_at")
        snapshot_at = (
            recorded_at.isoformat()
            if recorded_at and hasattr(recorded_at, "isoformat")
            else (recorded_at if isinstance(recorded_at, str) else datetime.now(timezone.utc).isoformat())
        )

        snapshot = {
            "application_id": application_id,
            "snapshot_at": snapshot_at,
            "global_position": global_position,
            "overall_verdict": overall_verdict,
            "rules_evaluated": rules_evaluated,
            "has_hard_block": has_hard_block,
        }

        snapshot_key = (application_id, snapshot_at)
        await self._store.upsert(SNAPSHOTS_TABLE, snapshot_key, snapshot)

    # -------------------------------------------------------------------------
    # Temporal Queries
    # -------------------------------------------------------------------------

    def get_current_compliance(
        self, application_id: str
    ) -> dict[str, Any] | None:
        """
        Full compliance record for an application: all checks, verdicts,
        and regulation versions.
        """
        events = self._store.query(
            EVENTS_TABLE,
            lambda r: r.get("application_id") == application_id,
        )

        if not events:
            return None

        # Sort by global_position
        events.sort(key=lambda e: e.get("global_position", 0))

        # Build current state
        rules: list[dict[str, Any]] = []
        has_hard_block = False
        overall_verdict = "PENDING"

        for evt in events:
            if evt.get("rule_id"):
                rules.append(
                    {
                        "rule_id": evt["rule_id"],
                        "verdict": evt.get("verdict"),
                        "rule_version": evt.get("rule_version"),
                        "failure_reason": evt.get("failure_reason"),
                        "is_hard_block": evt.get("is_hard_block", False),
                    }
                )
            if evt.get("is_hard_block") and evt.get("verdict") == "FAILED":
                has_hard_block = True
            if evt.get("event_type") == "ComplianceCheckCompleted":
                overall_verdict = evt.get("payload", {}).get(
                    "overall_verdict", "CLEAR"
                )

        return {
            "application_id": application_id,
            "overall_verdict": overall_verdict,
            "has_hard_block": has_hard_block,
            "rules": rules,
            "total_events": len(events),
        }

    def get_compliance_at(
        self,
        application_id: str,
        as_of: datetime,
        up_to_position: int | None = None,
    ) -> dict[str, Any] | None:
        """
        Compliance state as it existed at a specific moment in time.

        Algorithm:
        1. Check for nearest snapshot BEFORE as_of (and <= up_to_position if given)
        2. If snapshot exists at or before as_of, return that snapshot state
        3. If no snapshot, load ALL events with recorded_at <= as_of
        4. Reconstruct compliance state from events

        Args:
            application_id: The loan application ID.
            as_of: Point-in-time to query compliance state.
            up_to_position: Optional global_position upper bound for precision.
        """
        as_of_iso = as_of.isoformat() if as_of else None

        # Find best snapshot
        def snapshot_filter(r: dict) -> bool:
            if r.get("application_id") != application_id:
                return False
            if (r.get("snapshot_at") or "") > (as_of_iso or ""):
                return False
            if up_to_position is not None:
                if r.get("global_position", 0) > up_to_position:
                    return False
            return True

        snapshots = self._store.query(SNAPSHOTS_TABLE, snapshot_filter)

        if snapshots:
            # Use the latest snapshot at or before as_of by global_position
            snapshots.sort(key=lambda s: s.get("global_position", 0))
            best_snapshot = snapshots[-1]

            return {
                "application_id": application_id,
                "overall_verdict": best_snapshot.get("overall_verdict"),
                "has_hard_block": best_snapshot.get("has_hard_block", False),
                "rules": best_snapshot.get("rules_evaluated", []),
                "snapshot_at": best_snapshot.get("snapshot_at"),
                "global_position": best_snapshot.get("global_position"),
                "source": "snapshot",
            }

        # No snapshot — reconstruct from raw events
        def event_filter(r: dict) -> bool:
            if r.get("application_id") != application_id:
                return False
            
            recorded_at = r.get("recorded_at")
            recorded_at_str = recorded_at.isoformat() if hasattr(recorded_at, "isoformat") else str(recorded_at or "")
            
            if recorded_at_str > (as_of_iso or ""):
                return False
            if up_to_position is not None:
                if r.get("global_position", 0) > up_to_position:
                    return False
            return True

        events = self._store.query(EVENTS_TABLE, event_filter)

        if not events:
            return None

        events.sort(key=lambda e: e.get("global_position", 0))

        rules: list[dict[str, Any]] = []
        has_hard_block = False
        overall_verdict = "PENDING"

        for evt in events:
            if evt.get("rule_id"):
                rules.append(
                    {
                        "rule_id": evt["rule_id"],
                        "verdict": evt.get("verdict"),
                        "rule_version": evt.get("rule_version"),
                        "failure_reason": evt.get("failure_reason"),
                        "is_hard_block": evt.get("is_hard_block", False),
                    }
                )
            if evt.get("is_hard_block") and evt.get("verdict") == "FAILED":
                has_hard_block = True
            if evt.get("event_type") == "ComplianceCheckCompleted":
                overall_verdict = evt.get("payload", {}).get(
                    "overall_verdict", "CLEAR"
                )

        return {
            "application_id": application_id,
            "overall_verdict": overall_verdict,
            "has_hard_block": has_hard_block,
            "rules": rules,
            "total_events": len(events),
            "source": "event_replay",
        }

    def get_projection_lag(self) -> int:
        """Return current projection lag in event count."""
        # In production, this queries the DB. For in-memory, use checkpoint.
        return 0  # Daemon tracks the real lag

    async def rebuild_from_scratch(self) -> None:
        """Truncate both tables and reset checkpoint."""
        await self._store.truncate(EVENTS_TABLE)
        await self._store.truncate(SNAPSHOTS_TABLE)
        self._checkpoint = -1
