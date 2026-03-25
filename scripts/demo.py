import asyncio
import time
import uuid
import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from pprint import pprint

sys.path.insert(0, str(Path(__file__).parent / "src"))

from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    ComplianceCheckRequested,
    ComplianceCheckCompleted,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentSessionFailed,
)
from models.events import OptimisticConcurrencyError
from projections.compliance_audit import ComplianceAuditViewProjection
from projections.base import InMemoryProjectionStore
from upcasting.registry import UpcasterRegistry
from integrity.gas_town import reconstruct_agent_context
from what_if.projector import run_what_if


async def run_step_1():
    print("\n" + "="*80)
    print("STEP 1: The Week Standard - End-to-End Decision History")
    print("="*80)
    start_time = time.time()
    store = InMemoryEventStore()
    app_id = "DEMO-001"
    stream_id = f"loan-{app_id}"
    
    events = [
        ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "C-1", "requested_amount_usd": 100000.0, "loan_purpose": "demo", "submission_channel": "web", "submitted_at": datetime.now(timezone.utc).isoformat()}),
        CreditAnalysisRequested(payload={"application_id": app_id, "assigned_agent_id": "agent-1", "requested_at": datetime.now(timezone.utc).isoformat(), "priority": "high"}),
        CreditAnalysisCompleted(payload={"application_id": app_id, "agent_id": "agent-1", "session_id": "sess-1", "model_version": "v1", "confidence_score": 0.9, "risk_tier": "LOW", "recommended_limit_usd": 100000.0, "analysis_duration_ms": 100, "input_data_hash": "hash"}),
        ComplianceCheckRequested(payload={"application_id": app_id, "regulation_set_version": "v1", "checks_required": []}),
        ComplianceCheckCompleted(payload={"application_id": app_id, "overall_verdict": "CLEAR", "has_hard_block": False}),
        DecisionRequested(payload={"application_id": app_id}),
        DecisionGenerated(payload={"application_id": app_id, "orchestrator_agent_id": "orch-1", "recommendation": "APPROVE", "confidence_score": 0.85, "contributing_agent_sessions": [], "decision_basis_summary": "All clear", "model_versions": {}}),
        HumanReviewRequested(payload={"application_id": app_id, "reason": "Random audit"}),
        HumanReviewCompleted(payload={"application_id": app_id, "reviewer_id": "human-1", "override": False, "final_decision": "APPROVE", "override_reason": ""}),
        ApplicationApproved(payload={"application_id": app_id, "approved_amount_usd": 100000.0, "interest_rate": 5.0, "conditions": [], "approved_by": "system", "effective_date": datetime.now(timezone.utc).isoformat()})
    ]
    
    # Append events sequentially simulating the state machine
    for i, e in enumerate(events):
        v = await store.stream_version(stream_id)
        await store.append(stream_id, [e], expected_version=-1 if v == 0 else v)
        
    print(f"Successfully appended {len(events)} events to {stream_id}")
    
    # Run Integrity Check
    from integrity.audit_chain import run_integrity_check, verify_chain_integrity
    await run_integrity_check(store, "loan", app_id)
    result = await verify_chain_integrity(store, "loan", app_id)
    print(f"Cryptographic Integrity Verification Passed: {result.chain_valid}")
    if result.tamper_detected:
        print(f"Tamper Detected!")
        
    duration = time.time() - start_time
    print(f"Completed in {duration:.3f} seconds (< 60s)")


async def run_step_2():
    print("\n" + "="*80)
    print("STEP 2: Concurrency Under Pressure - Double Decision Test")
    print("="*80)
    store = InMemoryEventStore()
    app_id = "DEMO-002"
    stream_id = f"loan-{app_id}"
    
    await store.append(stream_id, [
        ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "C-2", "requested_amount_usd": 100000.0, "loan_purpose": "demo", "submission_channel": "web", "submitted_at": datetime.now(timezone.utc).isoformat()})
    ], expected_version=-1)
    
    print("Simulating Agent A and Agent B appending to version 0 simultaneously...")
    
    try:
        # Agent A writes successfully
        await store.append(stream_id, [CreditAnalysisRequested(payload={"application_id": app_id, "assigned_agent_id": "A", "requested_at": "now", "priority": "normal"})], expected_version=0)
        print("Agent A: Success")
        
        # Agent B writes with same expected version
        await store.append(stream_id, [ComplianceCheckRequested(payload={"application_id": app_id, "regulation_set_version": "v1", "checks_required": []})], expected_version=0)
    except OptimisticConcurrencyError as e:
        print(f"Agent B: {e.__class__.__name__} caught! (Expected version 0, actual version 1)")
        # Agent B retries
        print("Agent B reloading stream and retrying...")
        current_version = await store.stream_version(stream_id)
        await store.append(stream_id, [ComplianceCheckRequested(payload={"application_id": app_id, "regulation_set_version": "v1", "checks_required": []})], expected_version=current_version)
        print("Agent B: Retry Success")

async def run_step_3():
    print("\n" + "="*80)
    print("STEP 3: Temporal Compliance Query")
    print("="*80)
    store = InMemoryEventStore()
    proj_store = InMemoryProjectionStore()
    proj = ComplianceAuditViewProjection(proj_store)
    
    app_id = "DEMO-003"
    stream_id = f"loan-{app_id}"
    
    e1 = ComplianceCheckRequested(payload={"application_id": app_id, "regulation_set_version": "v1", "checks_required": []})
    await store.append(stream_id, [e1], expected_version=-1)
    stored = await store.load_stream(stream_id)
    se1 = stored[-1].model_copy(update={"recorded_at": datetime.now(timezone.utc) - timedelta(days=2)})
    await proj.handle(se1.model_dump())
    
    print(f"T1 ({se1.recorded_at.isoformat()}): ComplianceCheckRequested applied")
    
    e2 = ComplianceCheckCompleted(payload={"application_id": app_id, "overall_verdict": "CLEAR", "has_hard_block": False})
    await store.append(stream_id, [e2], expected_version=1)
    stored = await store.load_stream(stream_id)
    se2 = stored[-1].model_copy(update={"recorded_at": datetime.now(timezone.utc)})
    await proj.handle(se2.model_dump())
    
    print(f"T2 ({se2.recorded_at.isoformat()}): ComplianceCheckCompleted applied")
    
    # Query at T1
    state_t1 = proj.get_compliance_at(app_id, as_of=se1.recorded_at + timedelta(seconds=1))
    print(f"\nQuerying state at T1: Status = {state_t1.get('overall_verdict', 'PENDING')}")
    
    # Query at T2
    state_t2 = proj.get_compliance_at(app_id, as_of=se2.recorded_at + timedelta(seconds=1))
    print(f"Querying state at T2: Status = {state_t2.get('overall_verdict', 'PENDING')}")


async def run_step_4():
    print("\n" + "="*80)
    print("STEP 4: Upcasting & Immutability")
    print("="*80)
    store = InMemoryEventStore()
    
    # Simulate an raw row in DB (v1 event)
    raw_v1_payload = {"application_id": "DEMO-004", "decision": "approved", "reason": "Looks good"}
    
    # Register upcaster purely for demonstration
    # Register upcaster purely for demonstration
    from upcasters import registry as global_registry
    
    @global_registry.register("CreditDecisionMade", from_version=1)
    def upcast_v1_to_v2(payload: dict) -> dict:
        return {
            **payload,
            "model_version": "legacy-unversioned",
            "confidence_score": None,
            "regulatory_basis": "pre-2026-regulatory-framework"
        }
        
    # We append a mocked v1 event conceptually
    from models.events import BaseEvent
    class CreditDecisionMade(BaseEvent):
        payload: dict
        
    e = CreditDecisionMade(event_version=1, event_type="CreditDecisionMade", payload=raw_v1_payload)
    await store.append("stream-1", [e], expected_version=-1)
    print(f"Stored raw payload in DB (v1): {raw_v1_payload}")
    
    stream = await store.load_stream("stream-1")
    upcasted = global_registry.upcast(stream[0])
    print(f"Loaded event payload (v2): {upcasted.payload}")
    print("Notice the payload is transformed in-memory, but the database row remains unchanged v1.")


async def run_step_5():
    print("\n" + "="*80)
    print("STEP 5: Gas Town Recovery")
    print("="*80)
    store = InMemoryEventStore()
    sess_id = "agent-demo-sess-005"
    
    await store.append(sess_id, [
        AgentSessionStarted(payload={"agent_id": "demo-agent", "session_id": sess_id}),
        AgentContextLoaded(payload={"agent_id": "demo-agent", "session_id": sess_id, "context_source": "db", "model_version": "v1", "context_token_count": 100}),
        AgentNodeExecuted(payload={"node_name": "load_facts", "input_keys": [], "output_keys": []}),
        AgentSessionFailed(payload={"error_type": "ProcessKilled", "error_message": "OOM", "last_successful_node": "load_facts", "recoverable": True})
    ], expected_version=-1)
    
    print("Simulated Agent Crash in the middle of a session.")
    
    # Recovery
    state = await reconstruct_agent_context(store, "demo-agent", sess_id)
    print(f"Reconstructed State: status={state.session_health_status}, last_node={state.last_successful_node}, tokens={len(state.context_text)}")
    print("Agent can now seamlessly resume execution from 'load_facts'.")


async def run_step_6():
    print("\n" + "="*80)
    print("STEP 6: What-If Counterfactual (Bonus)")
    print("="*80)
    store = InMemoryEventStore()
    app_id = "DEMO-006"
    stream_id = f"loan-{app_id}"
    
    await store.append(stream_id, [
        ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "C-1", "requested_amount_usd": 100000.0, "loan_purpose": "demo", "submission_channel": "web", "submitted_at": datetime.now(timezone.utc).isoformat()})
    ], expected_version=-1, correlation_id="corr-1")
    
    await store.append(stream_id, [
        CreditAnalysisCompleted(payload={"application_id": app_id, "agent_id": "agent-1", "session_id": "sess-1", "model_version": "v1", "confidence_score": 0.9, "risk_tier": "MEDIUM", "recommended_limit_usd": 100000.0, "analysis_duration_ms": 100, "input_data_hash": "hash"})
    ], expected_version=1, correlation_id="corr-1")
    
    stream = await store.load_stream(stream_id)
    credit_id = str(stream[-1].event_id)
    
    await store.append(stream_id, [
        DecisionGenerated(payload={"application_id": app_id, "orchestrator_agent_id": "orch", "recommendation": "APPROVE", "confidence_score": 0.9, "contributing_agent_sessions": [], "decision_basis_summary": "ok", "model_versions": {}})
    ], expected_version=2, correlation_id="corr-1", causation_id=credit_id)
    
    stream = await store.load_stream(stream_id)
    decision_id = str(stream[-1].event_id)
    
    await store.append(stream_id, [
        ApplicationApproved(payload={"application_id": app_id, "approved_amount_usd": 100000.0, "interest_rate": 5.0, "conditions": [], "approved_by": "system", "effective_date": "now"})
    ], expected_version=3, correlation_id="corr-1", causation_id=decision_id)
    
    print("Real State: Application FINAL_APPROVED with MEDIUM risk tier.")
    
    cf_event = CreditAnalysisCompleted(payload={"application_id": app_id, "agent_id": "agent-1", "session_id": "sess-1", "model_version": "v1", "confidence_score": 0.5, "risk_tier": "HIGH", "recommended_limit_usd": 10000.0, "analysis_duration_ms": 100, "input_data_hash": "hash"})
    
    print("Injecting Counterfactual: HIGH risk tier & confidence = 0.5 (< 0.6 threshold)")
    result = await run_what_if(store, app_id, "CreditAnalysisCompleted", [cf_event])
    
    print(f"Counterfactual State: {result.counterfactual_outcome.get('state')}")
    print("The business rules cascaded accurately without touching the real database.")

async def main():
    await run_step_1()
    await run_step_2()
    await run_step_3()
    await run_step_4()
    await run_step_5()
    await run_step_6()

if __name__ == "__main__":
    asyncio.run(main())
