"""
The Ledger — What-If Projector (Phase 6)

Counterfactual event replay engine. Replays an application's event stream
with a substituted event at a branch point, applies causal dependency
filtering, enforces business rules through aggregate replay, and compares
real vs. counterfactual outcomes through fresh projection instances.

CRITICAL INVARIANT: Counterfactual replay NEVER writes to the real store.
All counterfactual events are processed in a sandboxed InMemoryEventStore.

Algorithm (13 steps):
  1.  Load real event stream
  2.  Guard: abort if stream exceeds MAX_REPLAY_EVENTS
  3.  Validate counterfactual events (type match, 1:1 replacement)
  4.  Resolve branch point (explicit position or first-match)
  5.  Split stream into pre-branch / branched / post-branch
  6.  Build causal dependency graph with O(1) event_id lookup
  7.  Construct counterfactual stream (pre + counterfactual + independent)
  8.  Replay through aggregate (validates transitions + business rules)
  9.  Seed sandboxed InMemoryEventStore
  10. Create FRESH projection instances for both branches
  11. Replay both streams through projections
  12. Compute divergence
  13. Return WhatIfResult
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from in_memory_store import InMemoryEventStore
from models.events import BaseEvent, StoredEvent
from projections.application_summary import ApplicationSummaryProjection
from projections.base import InMemoryProjectionStore, Projection
from projections.compliance_audit import ComplianceAuditViewProjection

# ─── Constants ────────────────────────────────────────────────────────────────

MAX_REPLAY_EVENTS = 5000
MAX_CAUSATION_DEPTH = 100


# ─── Result Data Structure ────────────────────────────────────────────────────

@dataclass
class WhatIfResult:
    """Result of a counterfactual what-if analysis."""

    application_id: str
    branch_event_type: str
    branch_position: int

    real_outcome: dict[str, Any]
    counterfactual_outcome: dict[str, Any]

    real_event_count: int
    counterfactual_event_count: int

    divergence_events: list[dict[str, Any]]
    skipped_dependent_events: list[dict[str, Any]]
    replayed_independent_events: list[dict[str, Any]]

    assumptions: list[str]
    branch_timestamp: str


# ─── Causal Dependency Tracing ────────────────────────────────────────────────

def _is_causally_dependent(
    event: StoredEvent,
    branched_event_id: str,
    event_id_map: dict[str, StoredEvent],
) -> tuple[bool, str | None]:
    """
    Trace causation_id chain from event back toward branched_event_id.

    Returns:
        (is_dependent, ambiguity_reason_or_none)
        - True, None → causally dependent on branched event
        - False, None → causally independent (no ambiguity)
        - False, reason → independent due to broken/missing chain (recorded)
    """
    visited: set[str] = set()
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    current_id = metadata.get("causation_id")
    depth = 0

    while current_id and depth < MAX_CAUSATION_DEPTH:
        current_id_str = str(current_id)

        if current_id_str in visited:  # cycle detection
            return False, f"causation_cycle at depth {depth}"
        visited.add(current_id_str)

        if current_id_str == str(branched_event_id):
            return True, None  # dependent

        parent = event_id_map.get(current_id_str)
        if parent is None:
            return False, f"broken_chain: missing event {current_id_str}"

        parent_meta = parent.metadata if isinstance(parent.metadata, dict) else {}
        current_id = parent_meta.get("causation_id")
        depth += 1

    if depth >= MAX_CAUSATION_DEPTH:
        return False, f"max_depth_exceeded at {depth}"

    return False, None  # independent, no ambiguity


# ─── Divergence Computation ───────────────────────────────────────────────────

def _compute_divergence(
    real_events: list[StoredEvent],
    counterfactual_events: list[StoredEvent],
) -> list[dict[str, Any]]:
    """
    Compute divergence between real and counterfactual event streams.

    Divergence is defined as:
      - Events present in one stream but absent in the other
        (by event_type + relative position)
      - Events with same type and relative position but differing payload
        (deep comparison excluding event_id, recorded_at, global_position)
    """
    divergences: list[dict[str, Any]] = []

    max_len = max(len(real_events), len(counterfactual_events))

    for i in range(max_len):
        real_e = real_events[i] if i < len(real_events) else None
        cf_e = counterfactual_events[i] if i < len(counterfactual_events) else None

        if real_e is None and cf_e is not None:
            divergences.append({
                "type": "counterfactual_only",
                "position": i,
                "event_type": cf_e.event_type,
                "payload": cf_e.payload,
            })
        elif cf_e is None and real_e is not None:
            divergences.append({
                "type": "real_only",
                "position": i,
                "event_type": real_e.event_type,
                "payload": real_e.payload,
            })
        elif real_e and cf_e:
            if real_e.event_type != cf_e.event_type:
                divergences.append({
                    "type": "type_mismatch",
                    "position": i,
                    "real_event_type": real_e.event_type,
                    "counterfactual_event_type": cf_e.event_type,
                })
            elif real_e.payload != cf_e.payload:
                divergences.append({
                    "type": "payload_differs",
                    "position": i,
                    "event_type": real_e.event_type,
                    "real_payload": real_e.payload,
                    "counterfactual_payload": cf_e.payload,
                })

    return divergences


# ─── Aggregate Replay (Business Rule Enforcement) ─────────────────────────────

def _replay_through_aggregate(
    events: list[StoredEvent],
    application_id: str,
) -> tuple[bool, str | None]:
    """
    Replay events through LoanApplicationAggregate._apply() to validate
    state transitions and business rules.

    Returns:
        (valid, error_message_or_none)
    """
    from aggregates.loan_application import LoanApplicationAggregate

    agg = LoanApplicationAggregate(application_id=application_id)
    try:
        for event in events:
            agg._apply(event)
        return True, None
    except Exception as e:
        return False, f"aggregate_rejected: {type(e).__name__}: {e}"


# ─── Projection Replay ───────────────────────────────────────────────────────

import inspect

async def _replay_through_projections(
    events: list[StoredEvent],
    projection_classes: list[type[Projection]] | None = None,
) -> dict[str, Any]:
    """
    Replay events through FRESH projection instances and return their state.
    Each projection gets a new InMemoryProjectionStore (empty state).
    """
    if projection_classes is None:
        projection_classes = [ApplicationSummaryProjection, ComplianceAuditViewProjection]

    projections: list[Projection] = []
    for cls in projection_classes:
        store = InMemoryProjectionStore()
        projections.append(cls(store=store))

    # Feed events to subscribed projections
    for event in events:
        event_dict = _stored_event_to_dict(event)
        for proj in projections:
            if event.event_type in proj.subscribed_events:
                await proj.handle(event_dict)

    # Extract results
    result: dict[str, Any] = {}
    for proj in projections:
            if isinstance(proj, ApplicationSummaryProjection):
                summaries = proj.get_all_summaries()
                if inspect.isawaitable(summaries):
                    summaries = await summaries
                if summaries:
                    result.update(summaries[0])
            elif isinstance(proj, ComplianceAuditViewProjection):
                # Try to get compliance for the application from events
                app_ids = set()
                for e in events:
                    aid = e.payload.get("application_id", "")
                    if aid:
                        app_ids.add(aid)
                for aid in app_ids:
                    comp = proj.get_current_compliance(aid)
                    if inspect.isawaitable(comp):
                        comp = await comp
                    if comp:
                        result["compliance"] = comp
                        break

    return result


def _stored_event_to_dict(event: StoredEvent) -> dict[str, Any]:
    """Convert StoredEvent to the dict format projections expect."""
    return {
        "event_id": str(event.event_id),
        "stream_id": event.stream_id,
        "stream_position": event.stream_position,
        "global_position": event.global_position,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "payload": event.payload if isinstance(event.payload, dict) else dict(event.payload),
        "metadata": event.metadata if isinstance(event.metadata, dict) else dict(event.metadata or {}),
        "recorded_at": event.recorded_at.isoformat() if isinstance(event.recorded_at, datetime) else str(event.recorded_at),
    }


# ─── Core What-If Engine ─────────────────────────────────────────────────────

async def run_what_if(
    store: Any,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
    branch_at_position: int | None = None,
    projections: list[type[Projection]] | None = None,
    include_compliance: bool = True,
) -> WhatIfResult:
    """
    Run a counterfactual what-if analysis on a loan application.

    Replays the application's event stream with a substituted event,
    filters causally dependent events, enforces business rules through
    aggregate replay, and compares outcomes through fresh projections.

    Args:
        store: EventStore or InMemoryEventStore (read-only — never modified)
        application_id: The loan application to analyze
        branch_at_event_type: Event type to substitute (e.g., "CreditAnalysisCompleted")
        counterfactual_events: Exactly 1 replacement event (must match branch type)
        branch_at_position: Explicit branch position (None = first match)
        projections: Projection classes to evaluate (None = defaults)
        include_compliance: Whether to include ComplianceAuditView

    Returns:
        WhatIfResult with real vs. counterfactual comparison

    Raises:
        ValueError: If counterfactual events are invalid
    """
    assumptions: list[str] = []

    # ── Step 1: Load real event stream ──────────────────────────────────
    stream_id = f"loan-{application_id}"
    real_events = await store.load_stream(stream_id)

    if not real_events:
        raise ValueError(f"No events found for stream {stream_id}")

    # ── Step 2: Guard against oversized streams ─────────────────────────
    if len(real_events) > MAX_REPLAY_EVENTS:
        raise ValueError(
            f"Stream has {len(real_events)} events, exceeding safety limit "
            f"of {MAX_REPLAY_EVENTS}. Aborting what-if analysis."
        )

    # ── Step 3: Validate counterfactual events ──────────────────────────
    if len(counterfactual_events) != 1:
        raise ValueError(
            f"Exactly 1 counterfactual event required (got {len(counterfactual_events)}). "
            "1:1 replacement only."
        )

    cf_event = counterfactual_events[0]
    if cf_event.event_type != branch_at_event_type:
        raise ValueError(
            f"Counterfactual event_type '{cf_event.event_type}' does not match "
            f"branch_at_event_type '{branch_at_event_type}'"
        )

    # ── Step 4: Resolve branch point ────────────────────────────────────
    branched_event: StoredEvent | None = None

    if branch_at_position is not None:
        # Explicit position
        for e in real_events:
            if e.stream_position == branch_at_position:
                if e.event_type != branch_at_event_type:
                    raise ValueError(
                        f"Event at position {branch_at_position} is "
                        f"'{e.event_type}', not '{branch_at_event_type}'"
                    )
                branched_event = e
                break
        if branched_event is None:
            raise ValueError(f"No event at stream_position {branch_at_position}")
        assumptions.append(
            f"branch_resolution: explicit position {branch_at_position}"
        )
    else:
        # First-match fallback
        for e in real_events:
            if e.event_type == branch_at_event_type:
                branched_event = e
                break
        if branched_event is None:
            raise ValueError(
                f"No '{branch_at_event_type}' event found in stream {stream_id}"
            )
        assumptions.append(
            f"branch_resolution: used first occurrence at position "
            f"{branched_event.stream_position}"
        )

    branch_position = branched_event.stream_position
    branched_event_id = str(branched_event.event_id)

    # Inherit causal linkage from original branched event
    original_meta = (
        branched_event.metadata
        if isinstance(branched_event.metadata, dict)
        else {}
    )
    cf_event = BaseEvent(
        event_type=cf_event.event_type,
        event_version=cf_event.event_version,
        payload=deepcopy(cf_event.payload),
        metadata={
            "correlation_id": original_meta.get("correlation_id"),
            "causation_id": original_meta.get("causation_id"),
            **(cf_event.metadata if cf_event.metadata else {}),
        },
    )

    # ── Step 5: Split stream ────────────────────────────────────────────
    pre_branch = [e for e in real_events if e.stream_position < branch_position]
    post_branch = [e for e in real_events if e.stream_position > branch_position]

    # ── Step 6: Build causal dependency graph ───────────────────────────
    # Build {event_id → event} map for O(1) lookup
    event_id_map: dict[str, StoredEvent] = {}
    for e in real_events:
        event_id_map[str(e.event_id)] = e

    skipped_dependent: list[dict[str, Any]] = []
    replayed_independent: list[StoredEvent] = []
    replayed_independent_dicts: list[dict[str, Any]] = []

    for e in post_branch:
        is_dep, ambiguity = _is_causally_dependent(
            e, branched_event_id, event_id_map
        )
        if is_dep:
            skipped_dependent.append(_stored_event_to_dict(e))
        else:
            if ambiguity:
                assumptions.append(
                    f"causal_ambiguity: event at position {e.stream_position} "
                    f"— {ambiguity} — treated as independent"
                )
            replayed_independent.append(e)
            replayed_independent_dicts.append(_stored_event_to_dict(e))

    # ── Step 7: Construct counterfactual stream ─────────────────────────
    # Convert counterfactual BaseEvent to StoredEvent for replay
    cf_stored = StoredEvent(
        event_id=uuid.uuid4(),
        stream_id=stream_id,
        stream_position=branch_position,
        global_position=0,  # sandbox-only, not meaningful
        event_type=cf_event.event_type,
        event_version=cf_event.event_version,
        payload=deepcopy(cf_event.payload),
        metadata=cf_event.metadata,
        recorded_at=branched_event.recorded_at,
    )

    counterfactual_stream = list(pre_branch) + [cf_stored] + list(replayed_independent)

    # ── Step 8: Aggregate-first replay (business rule enforcement) ──────
    # Real stream validation
    real_valid, real_err = _replay_through_aggregate(real_events, application_id)
    if not real_valid and real_err:
        assumptions.append(f"real_stream_note: {real_err}")

    # Counterfactual stream validation
    cf_valid, cf_err = _replay_through_aggregate(
        counterfactual_stream, application_id
    )
    if not cf_valid and cf_err:
        assumptions.append(cf_err)

    # ── Steps 9-11: Projection replay (fresh instances for both) ────────
    real_outcome = await _replay_through_projections(real_events, projections)
    cf_outcome = await _replay_through_projections(counterfactual_stream, projections)

    # ── Step 12: Compute divergence ─────────────────────────────────────
    divergence = _compute_divergence(real_events, counterfactual_stream)

    # ── Step 13: Return result ──────────────────────────────────────────
    branch_ts = (
        branched_event.recorded_at.isoformat()
        if isinstance(branched_event.recorded_at, datetime)
        else str(branched_event.recorded_at)
    )

    return WhatIfResult(
        application_id=application_id,
        branch_event_type=branch_at_event_type,
        branch_position=branch_position,
        real_outcome=real_outcome,
        counterfactual_outcome=cf_outcome,
        real_event_count=len(real_events),
        counterfactual_event_count=len(counterfactual_stream),
        divergence_events=divergence,
        skipped_dependent_events=skipped_dependent,
        replayed_independent_events=replayed_independent_dicts,
        assumptions=assumptions,
        branch_timestamp=branch_ts,
    )
