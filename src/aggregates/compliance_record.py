from __future__ import annotations
from typing import Any

from models.events import DomainError, StoredEvent


class ComplianceRecordAggregate:
    """
    Enforces business invariants for a loan application's regulatory checks.
    """
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.stream_id = f"compliance-{application_id}"
        self.version = 0
        
        self.status = "PENDING"
        self.rules_passed: set[str] = set()
        self.rules_failed: set[str] = set()
        self.regulation_version: str | None = None

    @classmethod
    async def load(cls, store: Any, application_id: str) -> ComplianceRecordAggregate:
        agg = cls(application_id)
        events = await store.load_stream(agg.stream_id)
        for e in events:
            agg._apply(e)
            agg.version = e.stream_position
        return agg

    def _apply(self, event: StoredEvent) -> None:
        if event.event_type == "ComplianceRulePassed":
            self.rules_passed.add(event.payload.get("rule_id", ""))
            if not self.regulation_version:
                self.regulation_version = event.payload.get("rule_version")
        elif event.event_type == "ComplianceRuleFailed":
            self.rules_failed.add(event.payload.get("rule_id", ""))
        elif event.event_type == "ComplianceCheckCompleted":
            self.status = event.payload.get("status", "COMPLETED")

    def assert_not_completed(self) -> None:
        if self.status in ["CLEAR", "BLOCKED", "COMPLETED"]:
            raise DomainError(f"Compliance check is already completed with status {self.status}")

    def assert_rule_not_evaluated(self, rule_id: str) -> None:
        if rule_id in self.rules_passed or rule_id in self.rules_failed:
            raise DomainError(f"Rule {rule_id} has already been evaluated for application {self.application_id}")
