#!/usr/bin/env python3
"""
The Ledger — Phase 6: What-If Projections and Regulatory Time Travel Demo

This script demonstrates the two core capabilities of Phase 6:
1. Counterfactual Analysis (What-If)
   - Replays a loan with a substituted credit analysis event.
   - Shows isolation (real store unchanged).
   - Shows business rule cascading (e.g., confidence drop forces REFER).
   - Computes causal dependency skipping and divergence.
   
2. Regulatory Examination Package
   - Generates a self-contained, verifiable JSON artifact.
   - Includes schema evolution, temporal projections, and narrative.
   - Demonstrates independent integrity verification.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
import sys
import os

from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    ComplianceCheckRequested,
    DecisionGenerated,
    DecisionRequested,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    ApplicationApproved,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentOutputWritten,
)
from what_if.projector import run_what_if
from regulatory.package import generate_regulatory_package
from integrity.audit_chain import canonical_event_hash, compute_chain_hash, GENESIS_HASH


# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


async def build_demo_scenario(store: InMemoryEventStore) -> str:
    """Builds a realistic 9-event loan application history for the demo."""
    app_id = f"DEMO-{uuid.uuid4().hex[:6].upper()}"
    corr_id = str(uuid.uuid4())
    stream_id = f"loan-{app_id}"
    
    logger.info(f"\\n--- Building Base Scenario for Application {app_id} ---")
    
    sys.stdout.write("1. ApplicationSubmitted... ")
    sys.stdout.flush()
    await store.append(stream_id, [
        ApplicationSubmitted(payload={
            "application_id": app_id,
            "applicant_id": "CUST-999",
            "requested_amount_usd": 1_200_000.0,
            "loan_purpose": "Commercial Real Estate",
            "submission_channel": "broker",
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }),
    ], expected_version=-1, correlation_id=corr_id)
    print("Done")

    sys.stdout.write("2. CreditAnalysisRequested... ")
    sys.stdout.flush()
    await store.append(stream_id, [
        CreditAnalysisRequested(payload={
            "application_id": app_id,
            "assigned_agent_id": "credit-alpha",
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "priority": "high",
        }),
    ], expected_version=1, correlation_id=corr_id)
    print("Done")

    # Agent Context (Needed for agent participation trace)
    await store.append("agent-credit-alpha-session001", [
        AgentSessionStarted(payload={"agent_id": "credit-alpha", "session_id": "session001"}),
        AgentContextLoaded(payload={
            "agent_id": "credit-alpha",
            "session_id": "session001",
            "context_source": "rdbms",
            "model_version": "gpt-4-turbo-2024-04-09",
            "context_token_count": 8500,
        }),
    ], expected_version=-1, correlation_id=corr_id)

    sys.stdout.write("3. CreditAnalysisCompleted (MEDIUM Risk, 0.88 Config)... ")
    sys.stdout.flush()
    events = await store.load_stream(stream_id)
    credit_req_id = str(events[-1].event_id)
    
    await store.append(stream_id, [
        CreditAnalysisCompleted(payload={
            "application_id": app_id,
            "agent_id": "credit-alpha",
            "session_id": "session001",
            "model_version": "gpt-4-turbo-2024-04-09",
            "confidence_score": 0.88,
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 1_000_000.0,
            "analysis_duration_ms": 3400,
            "input_data_hash": "hash_xyz_789",
        }),
    ], expected_version=2, correlation_id=corr_id, causation_id=credit_req_id)
    print("Done")

    sys.stdout.write("4. FraudScreeningCompleted... ")
    sys.stdout.flush()
    await store.append(stream_id, [
        FraudScreeningCompleted(payload={
            "application_id": app_id,
            "agent_id": "fraud-bot",
            "fraud_score": 0.02,
            "anomaly_flags": [],
            "screening_model_version": "f-ensemble-v2",
            "input_data_hash": "hash_abc_123",
        }),
    ], expected_version=3, correlation_id=corr_id)
    print("Done")

    sys.stdout.write("5. ComplianceCheckRequested... ")
    sys.stdout.flush()
    await store.append(stream_id, [
        ComplianceCheckRequested(payload={
            "application_id": app_id,
            "regulation_set_version": "US-COMMERCIAL-v2",
            "checks_required": ["KYC", "AML", "SANCTIONS"],
        }),
    ], expected_version=4, correlation_id=corr_id)
    print("Done")

    sys.stdout.write("6. DecisionRequested... ")
    sys.stdout.flush()
    await store.append(stream_id, [
        DecisionRequested(payload={"application_id": app_id}),
    ], expected_version=5, correlation_id=corr_id)
    print("Done")
    
    # Agent Output Written 
    await store.append("agent-credit-alpha-session001", [
        AgentOutputWritten(payload={"application_id": app_id, "output": {}}),
    ], expected_version=2, correlation_id=corr_id)

    sys.stdout.write("7. DecisionGenerated (APPROVE)... ")
    sys.stdout.flush()
    events = await store.load_stream(stream_id)
    credit_comp_id = str([e for e in events if e.event_type == "CreditAnalysisCompleted"][0].event_id)
    
    await store.append(stream_id, [
        DecisionGenerated(payload={
            "application_id": app_id,
            "orchestrator_agent_id": "chief-justice",
            "recommendation": "APPROVE",
            "confidence_score": 0.91,
            "contributing_agent_sessions": ["agent-credit-alpha-session001"],
            "decision_basis_summary": "Strong financials, low fraud risk, medium credit risk.",
            "model_versions": {"chief-justice": "claude-3-opus"},
        }),
    ], expected_version=6, correlation_id=corr_id, causation_id=credit_comp_id)
    print("Done")

    sys.stdout.write("8. HumanReviewCompleted (Auto-Approval)... ")
    sys.stdout.flush()
    events = await store.load_stream(stream_id)
    decision_id = str([e for e in events if e.event_type == "DecisionGenerated"][0].event_id)
    
    await store.append(stream_id, [
        HumanReviewCompleted(payload={
            "application_id": app_id,
            "reviewer_id": "AUTO-SYSTEM",
            "override": False,
            "final_decision": "APPROVE",
            "override_reason": "",
        }),
    ], expected_version=7, correlation_id=corr_id, causation_id=decision_id)
    print("Done")

    sys.stdout.write("9. ApplicationApproved... ")
    sys.stdout.flush()
    events = await store.load_stream(stream_id)
    hr_id = str([e for e in events if e.event_type == "HumanReviewCompleted"][0].event_id)
    
    await store.append(stream_id, [
        ApplicationApproved(payload={
            "application_id": app_id,
            "approved_amount_usd": 1_000_000.0,
            "interest_rate": 6.25,
            "conditions": ["Requires quarterly financials"],
            "approved_by": "SYSTEM",
            "effective_date": datetime.now(timezone.utc).isoformat(),
        }),
    ], expected_version=8, correlation_id=corr_id, causation_id=hr_id)
    print("Done")

    return app_id

async def run_demo():
    store = InMemoryEventStore()
    app_id = await build_demo_scenario(store)
    
    logger.info(f"\\n{'='*70}\\nPART 1: COUNTERFACTUAL WHAT-IF REPLAY\\n{'='*70}")
    logger.info("SCENARIO: What if the credit agent had hallucinated or detected HIGH risk")
    logger.info("with a low confidence score (0.40) instead of MEDIUM risk?")
    logger.info("Question: Would the system still approve the loan?\\n")
    
    # Counterfactual Event
    cf_event = CreditAnalysisCompleted(payload={
        "application_id": app_id,
        "agent_id": "credit-alpha",
        "session_id": "session001",
        "model_version": "gpt-4-turbo-2024-04-09",
        "confidence_score": 0.40,  # Below 0.60 floor
        "risk_tier": "HIGH",      # Worse risk tier
        "recommended_limit_usd": 250_000.0,
        "analysis_duration_ms": 3400,
        "input_data_hash": "hash_xyz_789",
    })
    
    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf_event]
    )
    
    logger.info(">>> Assumptions & Safety Invariants Enforced:")
    for a in result.assumptions:
        logger.info(f"  - {a}")
        
    logger.info("\\n>>> Causal Filter Action:")
    logger.info(f"  Total real events: {result.real_event_count}")
    logger.info(f"  Dependent events skipped: {len(result.skipped_dependent_events)} "
                f"({', '.join(e['event_type'] for e in result.skipped_dependent_events)})")
    logger.info(f"  Independent events replayed: {len(result.replayed_independent_events)} "
                f"({', '.join(e['event_type'] for e in result.replayed_independent_events)})")
    
    logger.info("\\n>>> Projection Outcome Comparison:")
    logger.info(f"  Real State:           {result.real_outcome.get('state')}")
    logger.info(f"  Counterfactual State: {result.counterfactual_outcome.get('state')} ⚠️")
    
    if result.real_outcome.get('state') != result.counterfactual_outcome.get('state'):
        logger.info("  ↳ Business rules successfully blocked approval due to confidence floor (BR3).")

    # Verify sandboxing
    real_stream = await store.load_stream(f"loan-{app_id}")
    real_types = [e.event_type for e in real_stream]
    assert "CreditAnalysisCompleted" in real_types
    # Get the actual payload risk tier of the ONLY CreditAnalysisCompleted event in the real store
    real_event_payload = [e.payload for e in real_stream if e.event_type == "CreditAnalysisCompleted"][0]
    logger.info(f"\\n>>> Store Sandbox Integrity Verified: Real store still has risk_tier={real_event_payload.get('risk_tier')}")

    
    logger.info(f"\\n{'='*70}\\nPART 2: REGULATORY EXAMINATION PACKAGE\\n{'='*70}")
    logger.info("Generate self-contained audit package for external regulators...\\n")
    
    package = await generate_regulatory_package(store, app_id)
    
    logger.info(">>> Package Metadata generated:")
    for k, v in package["package_metadata"].items():
        if k != "warnings":
            logger.info(f"  {k}: {v}")
            
    logger.info("\\n>>> Extracted Human-Readable Narrative:")
    for line in package["narrative"].split("\\n"):
        logger.info(f"  {line}")
        
    logger.info("\\n>>> Schema Evolution Transparency:")
    upcasters = package["schema_evolution"].get("upcasters_applied", [])
    if upcasters:
        for u in upcasters:
            logger.info(f"  Event '{u['event_type']}' upcasted v{u['original_version']} -> v{u['upcasted_to_version']}")
    else:
        logger.info("  No schema upcasting applied for these events.")
        
    logger.info("\\n>>> Agent Participation Traced:")
    for agent in package["agent_participation"]:
        logger.info(f"  Agent {agent.get('agent_id')} (Session: {agent.get('session_id')})")
        logger.info(f"    - Model: {agent.get('model_version')}")
        logger.info(f"    - Output Confidence: {agent.get('confidence_score')}")
        
    logger.info("\\n>>> Simulating Independent Cryptographic Verification:")
    integrity = package["integrity_verification"]
    raw_events = package["event_stream"]
    
    logger.info(f"  System reported hash:      {integrity['integrity_hash'][:24]}...")
    
    # Simulate a regulator completely disconnected from our DB, writing their own script
    regulator_hashes = []
    for e in raw_events:
        h_data = {
            "event_type": e["event_type"],
            "event_version": e["event_version"],
            "payload": e["payload"],
            "recorded_at": e["recorded_at"],
            "stream_position": e["stream_position"],
        }
        regulator_hashes.append(canonical_event_hash(h_data))
        
    indep_hash = compute_chain_hash(GENESIS_HASH, regulator_hashes)
    logger.info(f"  Regulator recomputed hash: {indep_hash[:24]}...")
    
    if indep_hash == integrity['integrity_hash']:
        logger.info("  ↳ VERIFIED: 100% Match! The audit log is mathematically proven to be untampered.")
    else:
        logger.error("  ↳ FAILED: Hash mismatch!")


if __name__ == "__main__":
    asyncio.run(run_demo())
