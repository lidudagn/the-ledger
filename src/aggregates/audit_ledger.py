from __future__ import annotations
from typing import Any

from models.events import DomainError, StoredEvent


class AuditLedgerAggregate:
    """
    Tracks and enforces the integrity hash chain invariants for any entity.
    """
    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.stream_id = f"audit-{entity_type}-{entity_id}"
        self.version = 0
        
        self.last_verified_position = 0
        self.previous_hash = "GENESIS"
        self.check_runs = 0

    @classmethod
    async def load(cls, store: Any, entity_type: str, entity_id: str) -> AuditLedgerAggregate:
        agg = cls(entity_type, entity_id)
        events = await store.load_stream(agg.stream_id)
        for e in events:
            agg._apply(e)
            agg.version = e.stream_position
        return agg

    def _apply(self, event: StoredEvent) -> None:
        if event.event_type == "AuditIntegrityCheckRun":
            self.last_verified_position = event.payload.get("last_verified_position", self.last_verified_position)
            self.previous_hash = event.payload.get("integrity_hash", self.previous_hash)
            self.check_runs += 1

    def assert_contiguous_chain(self, from_position: int, submitted_previous_hash: str) -> None:
        if self.check_runs > 0:
            if submitted_previous_hash != self.previous_hash:
                raise DomainError("Chain hash mismatch: attempted to branch audit chain")
            if from_position <= self.last_verified_position and from_position != 0:
                raise DomainError(f"Cannot verify from position {from_position}; already verified up to {self.last_verified_position}")
