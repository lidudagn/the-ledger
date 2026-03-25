"""
The Ledger — Cryptographic Audit Chain (Phase 4)

SHA-256 hash chain for tamper detection and regulatory audit.
Each integrity check hashes events since the last check and links
to the previous check's hash, forming an append-only chain.

Design:
  - Canonical serialization: json.dumps(sort_keys=True, separators=(",",":"))
  - Full-event hashing: includes event_type, event_version, payload,
    recorded_at, AND stream_position (position-dependent)
  - Chain linking: each AuditIntegrityCheckRun stores previous_hash
  - Incremental: only hashes events since last check run
  - Tamper detection: re-hash events and compare against stored hashes

Usage:
    from integrity.audit_chain import run_integrity_check, verify_chain_integrity
    result = await run_integrity_check(store, "loan", "LOAN-001")
    assert result.chain_valid
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

GENESIS_HASH = "GENESIS"


@dataclass
class IntegrityCheckResult:
    """Result of an integrity check run."""
    entity_type: str
    entity_id: str
    events_verified: int
    integrity_hash: str          # new chain head hash
    previous_hash: str           # previous chain head (GENESIS for first)
    chain_valid: bool
    tamper_detected: bool
    checked_event_range: tuple[int, int]   # (from_position, to_position)
    event_hashes: list[str] = field(default_factory=list)


def canonical_event_hash(event: dict) -> str:
    """Position-dependent, identity-aware event hash using SHA-256.

    Canonical form includes:
      - event_id: guarantees uniqueness (two identical payloads → different hashes)
      - event_type: ensures type changes are detected
      - event_version: ensures version tampering is detected
      - payload: the domain data (sorted keys, no whitespace)
      - recorded_at: ensures timestamp manipulation is detected
      - stream_position: makes each hash position-dependent
        (swapping adjacent events produces different hashes)

    This is an EVENT IDENTITY HASH, not a content hash:
      - Prevents replay/duplicate injection ambiguity
      - Each physical event produces a globally unique hash
    """
    hashable = {
        "event_id": str(event.get("event_id", "")),
        "event_type": event["event_type"],
        "event_version": event["event_version"],
        "payload": event["payload"],
        "recorded_at": str(event["recorded_at"]),
        "stream_position": event["stream_position"],
    }
    canonical = json.dumps(hashable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def compute_chain_hash(previous_hash: str, event_hashes: list[str]) -> str:
    """Compute chain hash from previous hash and individual event hashes.

    chain_hash = sha256(previous_hash + h1 + h2 + ... + hN)
    """
    combined = previous_hash + "".join(event_hashes)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


async def run_integrity_check(
    store: Any,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """Run an integrity check on an entity's event stream.

    Flow:
    1. Load the entity's primary stream (e.g., loan-{id})
    2. Load the audit stream (audit-{entity_type}-{entity_id})
    3. Find last AuditIntegrityCheckRun → get previous_hash + last_verified_position
    4. Load events since last_verified_position
    5. Hash each event using canonical_event_hash()
    6. Compute chain hash: sha256(previous_hash + h1 + h2 + ...)
    7. Append AuditIntegrityCheckRun with new hash
    8. Return result
    """
    from models.events import BaseEvent

    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    from aggregates.audit_ledger import AuditLedgerAggregate
    ledger = await AuditLedgerAggregate.load(store, entity_type, entity_id)
    previous_hash = ledger.previous_hash
    last_verified_position = ledger.last_verified_position

    # Load primary events since last verification
    all_events = await store.load_stream(primary_stream)
    new_events = [
        e for e in all_events
        if e.stream_position > last_verified_position
    ]

    # Hash each new event
    event_hashes = []
    for e in new_events:
        event_dict = {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "event_version": e.event_version,
            "payload": e.payload if isinstance(e.payload, dict) else dict(e.payload),
            "recorded_at": str(e.recorded_at),
            "stream_position": e.stream_position,
        }
        h = canonical_event_hash(event_dict)
        event_hashes.append(h)

    # Compute chain hash
    if event_hashes:
        integrity_hash = compute_chain_hash(previous_hash, event_hashes)
        from_pos = new_events[0].stream_position
        to_pos = new_events[-1].stream_position
    else:
        integrity_hash = previous_hash
        from_pos = last_verified_position
        to_pos = last_verified_position

    if event_hashes:
        ledger.assert_contiguous_chain(from_pos, previous_hash)

    # Append AuditIntegrityCheckRun event to audit stream
    audit_event = BaseEvent(
        event_type="AuditIntegrityCheckRun",
        event_version=1,
        payload={
            "entity_type": entity_type,
            "entity_id": entity_id,
            "events_verified": len(new_events),
            "integrity_hash": integrity_hash,
            "previous_hash": previous_hash,
            "last_verified_position": to_pos,
            "event_hashes": event_hashes,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    expected = ledger.version if ledger.version > 0 else -1
    await store.append(audit_stream, [audit_event], expected)

    return IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified=len(new_events),
        integrity_hash=integrity_hash,
        previous_hash=previous_hash,
        chain_valid=True,
        tamper_detected=False,
        checked_event_range=(from_pos, to_pos),
        event_hashes=event_hashes,
    )


async def verify_chain_integrity(
    store: Any,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """Full verification: re-hash ALL events from genesis and compare
    against stored integrity hashes at each checkpoint.

    Used for tamper detection — if any stored hash doesn't match
    the recomputed hash, tamper_detected = True.
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    all_events = await store.load_stream(primary_stream)
    audit_events = await store.load_stream(audit_stream)

    # Rebuild hash chain from scratch
    chain_hash = GENESIS_HASH
    event_idx = 0
    tamper_detected = False

    for ae in audit_events:
        if ae.event_type != "AuditIntegrityCheckRun":
            continue

        stored_hash = ae.payload.get("integrity_hash")
        last_verified = ae.payload.get("last_verified_position", 0)
        stored_previous = ae.payload.get("previous_hash")

        # Verify previous_hash matches our running chain
        if stored_previous != chain_hash:
            tamper_detected = True
            break

        # Re-hash events in this segment
        segment_hashes = []
        while event_idx < len(all_events) and all_events[event_idx].stream_position <= last_verified:
            e = all_events[event_idx]
            event_dict = {
                "event_id": str(e.event_id),
                "event_type": e.event_type,
                "event_version": e.event_version,
                "payload": e.payload if isinstance(e.payload, dict) else dict(e.payload),
                "recorded_at": str(e.recorded_at),
                "stream_position": e.stream_position,
            }
            segment_hashes.append(canonical_event_hash(event_dict))
            event_idx += 1

        # Compute chain hash for this segment
        if segment_hashes:
            recomputed = compute_chain_hash(chain_hash, segment_hashes)
        else:
            recomputed = chain_hash

        if recomputed != stored_hash:
            tamper_detected = True
            break

        chain_hash = recomputed

    total_verified = len(all_events)
    last_pos = all_events[-1].stream_position if all_events else 0

    return IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified=total_verified,
        integrity_hash=chain_hash,
        previous_hash=GENESIS_HASH,
        chain_valid=not tamper_detected,
        tamper_detected=tamper_detected,
        checked_event_range=(1 if all_events else 0, last_pos),
    )
