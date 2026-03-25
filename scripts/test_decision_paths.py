"""
Phase 2 — DecisionGenerated Integration Test

Tests all 3 recommendation paths on separate applications:
  L1: APPROVE → APPROVED_PENDING_HUMAN → FINAL_APPROVED
  L2: DECLINE → DECLINED_PENDING_HUMAN → FINAL_DECLINED
  L3: REFER  → stays PENDING_DECISION (confidence < 0.6 forces REFER via BR3)
"""

from __future__ import annotations

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from aggregates.loan_application import (
    ApplicationState,
    LoanApplicationAggregate,
)
from commands.handlers import (
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    handle_decision_generated,
    handle_human_review_completed,
)
from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionRequested,
    FraudScreeningCompleted,
)


async def advance_to_pending_decision(store: InMemoryEventStore, app_id: str) -> None:
    """Advance an application through the full pipeline to PENDING_DECISION."""
    v = await store.append(f"loan-{app_id}", [ApplicationSubmitted(payload={
        "application_id": app_id, "applicant_id": "A1",
        "requested_amount_usd": 500000.0,
    })], expected_version=-1)

    v = await store.append(f"loan-{app_id}", [CreditAnalysisRequested(payload={
        "application_id": app_id,
    })], expected_version=v)

    v = await store.append(f"loan-{app_id}", [CreditAnalysisCompleted(payload={
        "application_id": app_id, "confidence_score": 0.85, "risk_tier": "MEDIUM",
    })], expected_version=v)

    v = await store.append(f"loan-{app_id}", [FraudScreeningCompleted(payload={
        "application_id": app_id, "fraud_score": 0.1,
    })], expected_version=v)

    v = await store.append(f"loan-{app_id}", [ComplianceCheckRequested(payload={
        "application_id": app_id, "checks_required": ["REG-001"],
    })], expected_version=v)

    await store.append(f"loan-{app_id}", [DecisionRequested(payload={
        "application_id": app_id,
    })], expected_version=v)

    # Seed compliance stream
    await store.append("compliance-" + app_id, [ComplianceRulePassed(payload={
        "application_id": app_id, "rule_id": "REG-001",
    })], expected_version=-1)


async def main():
    store = InMemoryEventStore()

    print("=" * 60)
    print("Phase 2 — DecisionGenerated Integration Test")
    print("=" * 60)

    # =========================================================================
    # TEST 1: APPROVE → APPROVED_PENDING_HUMAN → FINAL_APPROVED
    # =========================================================================
    print("\n--- L1: APPROVE path ---")
    await advance_to_pending_decision(store, "L1")

    await handle_decision_generated(GenerateDecisionCommand(
        application_id="L1",
        orchestrator_agent_id="orch-01",
        recommendation="APPROVE",
        confidence_score=0.9,
    ), store)

    app = await LoanApplicationAggregate.load(store, "L1")
    print(f"  After DecisionGenerated(APPROVE, 0.9): state = {app.state}")
    assert app.state == ApplicationState.APPROVED_PENDING_HUMAN, f"Expected APPROVED_PENDING_HUMAN, got {app.state}"

    # Human approves
    await handle_human_review_completed(HumanReviewCompletedCommand(
        application_id="L1",
        reviewer_id="LO-Sarah-Chen",
        override=False,
        final_decision="APPROVE",
    ), store)

    app = await LoanApplicationAggregate.load(store, "L1")
    print(f"  After HumanReviewCompleted(APPROVE):   state = {app.state}")
    assert app.state == ApplicationState.FINAL_APPROVED

    # =========================================================================
    # TEST 2: DECLINE → DECLINED_PENDING_HUMAN → FINAL_DECLINED
    # =========================================================================
    print("\n--- L2: DECLINE path ---")
    await advance_to_pending_decision(store, "L2")

    await handle_decision_generated(GenerateDecisionCommand(
        application_id="L2",
        orchestrator_agent_id="orch-01",
        recommendation="DECLINE",
        confidence_score=0.8,
    ), store)

    app = await LoanApplicationAggregate.load(store, "L2")
    print(f"  After DecisionGenerated(DECLINE, 0.8):  state = {app.state}")
    assert app.state == ApplicationState.DECLINED_PENDING_HUMAN

    # Human confirms decline
    await handle_human_review_completed(HumanReviewCompletedCommand(
        application_id="L2",
        reviewer_id="LO-Sarah-Chen",
        override=False,
        final_decision="DECLINE",
    ), store)

    app = await LoanApplicationAggregate.load(store, "L2")
    print(f"  After HumanReviewCompleted(DECLINE):   state = {app.state}")
    assert app.state == ApplicationState.FINAL_DECLINED

    # =========================================================================
    # TEST 3: REFER → stays PENDING_DECISION (BR3: confidence < 0.6)
    # =========================================================================
    print("\n--- L3: REFER path (BR3 confidence floor) ---")
    await advance_to_pending_decision(store, "L3")

    await handle_decision_generated(GenerateDecisionCommand(
        application_id="L3",
        orchestrator_agent_id="orch-01",
        recommendation="APPROVE",  # Agent says APPROVE but...
        confidence_score=0.5,       # ...BR3 forces REFER (< 0.6)
    ), store)

    app = await LoanApplicationAggregate.load(store, "L3")
    print(f"  After DecisionGenerated(APPROVE→REFER, 0.5): state = {app.state}")
    print(f"  BR3 enforced: original=APPROVE, forced=REFER")
    # REFER stays in PENDING_DECISION (no transition)
    assert app.state == ApplicationState.PENDING_DECISION

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("✅ All 3 DecisionGenerated paths verified:")
    print("   L1: APPROVE → APPROVED_PENDING_HUMAN → FINAL_APPROVED")
    print("   L2: DECLINE → DECLINED_PENDING_HUMAN → FINAL_DECLINED")
    print("   L3: REFER   → PENDING_DECISION (BR3 confidence floor)")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
