import pytest
import asyncio
import os
import json
import asyncpg
from typing import AsyncGenerator
from collections import defaultdict
from dotenv import load_dotenv

from models.events import (
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    FraudScreeningRequested,
    FraudScreeningCompleted,
    ComplianceCheckRequested,
    ComplianceCheckCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    AgentSessionStarted,
    AgentSessionFailed,
    AgentSessionRecovered,
    AgentNodeExecuted,
    ExtractionCompleted,
    QualityAssessmentCompleted
)
from agents.credit_analysis import CreditAnalysisAgent
from agents.fraud_detection import FraudDetectionAgent
from agents.compliance import ComplianceAgent
from agents.decision_orchestrator import DecisionOrchestratorAgent

from in_memory_store import InMemoryEventStore
from registry.client import ApplicantRegistryClient


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="DATABASE_URL not set — skipping real DB tests",
)

@pytest.fixture
async def real_registry() -> AsyncGenerator[ApplicantRegistryClient, None]:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    client = ApplicantRegistryClient(pool)
    yield client
    await pool.close()


@pytest.fixture
async def memory_store():
    store = InMemoryEventStore()
    seed_path = os.path.join(os.path.dirname(__file__), "..", "seed_events.jsonl")
    events_by_stream = defaultdict(list)
    import models.events
    
    with open(seed_path, "r") as f:
        for line in f:
            data = json.loads(line)
            if data["stream_id"].startswith("agent-"):
                continue
            stream_id = data.pop("stream_id")
            stream_pos = data.pop("stream_position", None)
            data.pop("global_position", None)
            event_type = data.pop("event_type")
            try:
                EventClass = getattr(models.events, event_type)
                if event_type in ("FraudScreeningRequested", "ComplianceCheckRequested", "DecisionRequested"):
                    continue
                event_obj = EventClass(event_type=event_type, **data)
                events_by_stream[stream_id].append(event_obj)
            except AttributeError:
                pass
                
    for stream_id, ev_list in events_by_stream.items():
        await store.append(stream_id, ev_list, expected_version=-1)
    return store


@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision(memory_store, real_registry):
    """
    NARR-01: Two CreditAnalysisAgent instances are started simultaneously on COMP-031 
    after documents are processed. Both read the credit stream at version 0.
    """
    app_id = "APEX-NARR-01"
    await memory_store.append(
        f"loan-{app_id}",
        [
            ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "COMP-031", "requested_amount_usd": 100000, "loan_purpose": "x", "submission_channel": "x", "submitted_at": "now"}),
            CreditAnalysisRequested(payload={"application_id": app_id, "assigned_agent_id": "none", "requested_at": "now", "priority": "normal"})
        ],
        expected_version=-1
    )
    
    # 2. Run two agents concurrently using real LLMs
    agent_a = CreditAnalysisAgent(memory_store, real_registry)
    agent_b = CreditAnalysisAgent(memory_store, real_registry)

    # They will both execute the actual LLM calls and then race for OCC validation
    await asyncio.gather(
        agent_a.process_application(app_id),
        agent_b.process_application(app_id),
        return_exceptions=True
    )

    loan_stream = await memory_store.load_stream(f"loan-{app_id}")
    credit_completed = [e.event_type for e in loan_stream if e.event_type == "CreditAnalysisCompleted"]
    assert len(credit_completed) >= 1


@pytest.mark.asyncio
async def test_narr02_missing_ebitda(memory_store, real_registry):
    """
    NARR-02: Document Extraction Failure (Missing EBITDA)
    COMP-044 has the missing EBITDA.
    """
    app_id = "APEX-NARR-02"
    
    await memory_store.append(
        f"docpkg-{app_id}",
        [
            ExtractionCompleted(payload={
                "application_id": app_id,
                "document_id": "doc1",
                "document_type": "INCOME_STATEMENT",
                "extracted_text": "",
                "facts": {"ebitda": None, "total_revenue": 1000},
                "field_confidence": {"ebitda": 0.0},
                "extraction_notes": ["ebitda"]
            }),
            QualityAssessmentCompleted(payload={
                "application_id": app_id,
                "overall_confidence": 0.5,
                "is_coherent": True,
                "anomalies": [],
                "critical_missing_fields": ["ebitda"],
                "reextraction_recommended": False,
                "auditor_notes": "missing ebitda"
            })
        ],
        expected_version=-1
    )
    
    await memory_store.append(
        f"loan-{app_id}",
        [
            ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "COMP-044", "requested_amount_usd": 100000, "loan_purpose": "x", "submission_channel": "x", "submitted_at": "now"}),
            CreditAnalysisRequested(payload={"application_id": app_id, "priority": "normal"})
        ],
        expected_version=-1
    )

    # Uses real LLM model!
    agent = CreditAnalysisAgent(memory_store, real_registry)
    await agent.process_application(app_id)
        
    loan_events = await memory_store.load_stream(f"loan-{app_id}")
    credit_completed_events = [e for e in loan_events if e.event_type == "CreditAnalysisCompleted"]
    assert len(credit_completed_events)
    credit_completed = credit_completed_events[-1]
    
    assert len(credit_completed.payload["data_quality_caveats"]) > 0


@pytest.mark.asyncio
async def test_narr03_crash_recovery(memory_store, real_registry):
    """
    NARR-03: FraudDetectionAgent crashes mid-flight and recovers.
    """
    app_id = "APEX-NARR-03"
    await memory_store.append(
        f"loan-{app_id}",
        [
            ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "COMP-057", "requested_amount_usd": 100000, "loan_purpose": "x", "submission_channel": "x", "submitted_at": "now"}),
            CreditAnalysisRequested(payload={"application_id": app_id, "priority": "normal"}),
            CreditAnalysisCompleted(payload={"application_id": app_id, "agent_id": "a", "session_id": "s", "model_version": "v", "confidence_score": 1, "risk_tier": "LOW", "recommended_limit_usd": 1, "input_data_hash": "a"}),
            FraudScreeningRequested(payload={"application_id": app_id})
        ],
        expected_version=-1
    )
    
    agent = FraudDetectionAgent(memory_store, real_registry)
    session_id = "sess-crash-123"
    await memory_store.append(
        f"agent-fraud-{session_id}",
        [
            AgentSessionStarted(payload={"agent_id": "fraud-agent", "session_id": session_id}),
            AgentNodeExecuted(payload={"node_name": "load_facts", "input_keys": [], "output_keys": []}),
            AgentSessionFailed(payload={"error_type": "SimulatedCrash", "error_message": "Boom", "last_successful_node": "load_facts", "recoverable": True})
        ],
        expected_version=-1
    )

    # Uses real LLMs
    await agent.process_application(app_id)

    loan_events = await memory_store.load_stream(f"loan-{app_id}")
    fraud_completed = [e for e in loan_events if e.event_type == "FraudScreeningCompleted"]
    assert len(fraud_completed) > 0
    assert fraud_completed[-1].event_type == "FraudScreeningCompleted"


@pytest.mark.asyncio
async def test_narr04_compliance_hard_block(memory_store, real_registry):
    """
    NARR-04: Montana company Hard Block (REG-003)
    """
    app_id = "APEX-NARR-04"
    
    # 1. Fetch Montana applicant from actual registry database
    mt_company = None
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT company_id FROM applicant_registry.companies WHERE jurisdiction = 'MT' LIMIT 1")
        if record:
            mt_company = record["company_id"]
    await pool.close()
    
    if not mt_company:
        pytest.skip("No Montana company found in database")

    await memory_store.append(
        f"loan-{app_id}",
        [
            ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": mt_company, "requested_amount_usd": 100000, "loan_purpose": "x", "submission_channel": "x", "submitted_at": "now"}),
            ComplianceCheckRequested(payload={"application_id": app_id, "rules_to_evaluate": []})
        ],
        expected_version=-1
    )

    agent = ComplianceAgent(memory_store, real_registry)
    await agent.process_application(app_id)

    comp_events = await memory_store.load_stream(f"compliance-{app_id}")
    failed_events = [e for e in comp_events if e.event_type == "ComplianceRuleFailed"]
    
    assert len(failed_events) >= 1
    # Check if REG-002 (Montana block) failed
    mt_failure = [e for e in failed_events if e.payload.get("rule_id") == "REG-002"]
    assert len(mt_failure) == 1
    
    done_events = [e for e in comp_events if e.event_type == "ComplianceCheckCompleted"]
    assert len(done_events) == 1
    assert done_events[0].payload["status"] == "BLOCKED"
    
    loan_events = await memory_store.load_stream(f"loan-{app_id}")
    assert "DecisionGenerated" not in [e.event_type for e in loan_events]


@pytest.mark.asyncio
async def test_narr05_human_override(memory_store):
    """
    NARR-05: Human overrides DECLINE to APPROVE
    """
    app_id = "APEX-NARR-05"
    
    await memory_store.append(
        f"loan-{app_id}",
        [
            ApplicationSubmitted(payload={"application_id": app_id, "applicant_id": "COMP-068", "requested_amount_usd": 100000, "loan_purpose": "x", "submission_channel": "x", "submitted_at": "now"}),
            CreditAnalysisRequested(payload={"application_id": app_id, "priority": "normal"}),
            CreditAnalysisCompleted(payload={"application_id": app_id, "agent_id": "a", "session_id": "s", "model_version": "v", "confidence_score": 1, "risk_tier": "LOW", "recommended_limit_usd": 1, "input_data_hash": "a"}),
            ComplianceCheckRequested(payload={"application_id": app_id, "rules_to_evaluate": []}),
            ComplianceCheckCompleted(payload={"application_id": app_id, "overall_verdict": "CLEAR", "has_hard_block": False}),
            DecisionRequested(payload={"application_id": app_id}),
            DecisionGenerated(payload={
                "application_id": app_id, "orchestrator_agent_id": "orch1", "recommendation": "DECLINE", 
                "confidence_score": 0.8, "contributing_agent_sessions": [], "decision_basis_summary": "High leverage", "model_versions": {}
            }),
            HumanReviewRequested(payload={
                "application_id": app_id, "reason": "Referral policy triggered"
            })
        ],
        expected_version=-1
    )
    
    from commands.handlers import handle_human_review_completed, HumanReviewCompletedCommand
    cmd = HumanReviewCompletedCommand(
        application_id=app_id, reviewer_id="LO-Sarah-Chen", override=True, final_decision="APPROVE", override_reason="Has collateral"
    )
    await handle_human_review_completed(cmd, memory_store)
    
    loan_events = await memory_store.load_stream(f"loan-{app_id}")
    override = [e for e in loan_events if e.event_type == "HumanReviewCompleted"][0]
    approved = [e for e in loan_events if e.event_type == "ApplicationApproved"][0]
    
    assert override.payload["override"] is True
    assert override.payload["reviewer_id"] == "LO-Sarah-Chen"
    assert "approved_amount_usd" in approved.payload
