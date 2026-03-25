import pytest

from event_store import EventStore
from in_memory_store import InMemoryEventStore
from agents.decision_orchestrator import DecisionOrchestratorAgent
from models.events import (
    ApplicationSubmitted,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    FraudScreeningCompleted,
    DecisionGenerated,
)
from aggregates.loan_application import ApplicationState

@pytest.fixture
def store() -> EventStore:
    return InMemoryEventStore()

from unittest.mock import patch
from langchain_core.runnables import RunnableLambda
from agents.decision_orchestrator import DecisionOutput

@pytest.mark.asyncio
@patch("agents.decision_orchestrator.ChatOpenAI.with_structured_output")
async def test_decision_orchestrator_agent_end_to_end(mock_with_structured_output, store):
    async def mock_invoke(*args, **kwargs):
        return DecisionOutput(
            recommendation="APPROVE",
            confidence=0.77
        )
    mock_with_structured_output.return_value = RunnableLambda(mock_invoke)

    # 1. Setup loan aggregate and dependency streams
    app_id = "APP-004"
    loan_stream = f"loan-{app_id}"
    
    from models.events import (
        ApplicationSubmitted,
        CreditAnalysisRequested,
        CreditAnalysisCompleted,
        FraudScreeningCompleted,
        ComplianceCheckRequested,
        DecisionRequested,
    )
    
    await store.append(
        stream_id=loan_stream,
        events=[
            ApplicationSubmitted(payload={
                "application_id": app_id,
                "applicant_id": "COMP-999",
                "requested_amount_usd": 100000.0,
                "loan_purpose": "Test",
                "submission_channel": "api"
            }),
            CreditAnalysisRequested(payload={"application_id": app_id, "priority": "normal"}),
            CreditAnalysisCompleted(payload={
                "application_id": app_id,
                "agent_id": "cre1",
                "session_id": "sess-cre1",
                "model_version": "v1",
                "confidence_score": 0.9,
                "risk_tier": "LOW",
                "recommended_limit_usd": 120000,
                "analysis_duration_ms": 100,
                "input_data_hash": "x"
            }),
            FraudScreeningCompleted(payload={
                "application_id": app_id,
                "agent_id": "fra1",
                "session_id": "sess-fra1",
                "fraud_score": 0.1,
                "screening_model_version": "v1",
                "anomaly_flags": [],
                "input_data_hash": "x"
            }),
            ComplianceCheckRequested(payload={"application_id": app_id, "priority": "normal"}),
            DecisionRequested(payload={"application_id": app_id, "priority": "normal"})
        ],
        expected_version=-1
    )
    
    # Establish arbitrary session streams to satisfy handler's causal link
    await store.append(
        stream_id="agent-cre1-sess-cre1",
        events=[
            AgentSessionStarted(payload={"session_id": "sess-cre1"}),
            AgentOutputWritten(payload={"application_id": app_id, "session_id": "sess-cre1"}),
            AgentSessionCompleted(payload={"session_id": "sess-cre1"})
        ],
        expected_version=-1
    )
    await store.append(
        stream_id="agent-fra1-sess-fra1",
        events=[
            AgentSessionStarted(payload={"session_id": "sess-fra1"}),
            AgentOutputWritten(payload={"application_id": app_id, "session_id": "sess-fra1"}),
            AgentSessionCompleted(payload={"session_id": "sess-fra1"})
        ],
        expected_version=-1
    )
    
    # 2. Run Agent
    agent = DecisionOrchestratorAgent(store)
    final_state = await agent.process_application(app_id)
    
    # 3. Assert State
    assert final_state["application_id"] == app_id
    assert final_state["total_nodes_executed"] == 5
    assert final_state["total_llm_calls"] == 1
    assert final_state["final_decision"]["recommendation"] == "APPROVE"
    
    # 4. Verify Output on the loan stream
    loan_events = await store.load_stream(loan_stream)
    assert loan_events[-1].event_type == "DecisionGenerated"
    assert loan_events[-1].payload["recommendation"] == "APPROVE"
    assert "agent-cre1-sess-cre1" in loan_events[-1].payload["contributing_agent_sessions"]
    assert "agent-fra1-sess-fra1" in loan_events[-1].payload["contributing_agent_sessions"]
    
    # 5. Verify the Agent Session trace
    sess_id = final_state["session_id"]
    agent_stream = f"agent-orchestrator-1-{sess_id}"
    
    audit_events = await store.load_stream(agent_stream)
    event_types = [e.event_type for e in audit_events]
    
    assert event_types.count("AgentNodeExecuted") == 5
    assert event_types[-2] == "AgentOutputWritten"
    assert event_types[-1] == "AgentSessionCompleted"
