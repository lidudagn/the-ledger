import pytest

from event_store import EventStore
from in_memory_store import InMemoryEventStore
from agents.fraud_detection import FraudDetectionAgent
from models.events import (
    ApplicationSubmitted,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
    FraudScreeningCompleted,
)
from aggregates.loan_application import ApplicationState
from registry.client import ApplicantRegistryClient


class MockRegistryClient:
    async def get_company(self, company_id: str):
        class MockCompany:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
        return MockCompany(company_id=company_id, company_name="Test Corp", risk_tier="LOW")


@pytest.fixture
def store() -> EventStore:
    return InMemoryEventStore()


@pytest.fixture
def mock_registry() -> ApplicantRegistryClient:
    return MockRegistryClient()  # type: ignore


from unittest.mock import patch
from langchain_core.runnables import RunnableLambda
from agents.fraud_detection import FraudAssessmentOutput

@pytest.mark.asyncio
@patch("agents.fraud_detection.ChatOpenAI.with_structured_output")
async def test_fraud_agent_end_to_end(mock_with_structured_output, store, mock_registry):
    async def mock_invoke(*args, **kwargs):
        return FraudAssessmentOutput(
            fraud_score=0.18,
            anomaly_flags=[]
        )
    mock_with_structured_output.return_value = RunnableLambda(mock_invoke)

    # 1. Setup loan aggregate
    app_id = "APP-003"
    loan_stream = f"loan-{app_id}"
    
    # We must seed a loan that is in the ANALYSIS_COMPLETE state
    # because the handle_fraud_screening_completed requires 
    # ApplicationState.ANALYSIS_COMPLETE (BR1)
    
    from models.events import CreditAnalysisCompleted, CreditAnalysisRequested
    
    await store.append(
        stream_id=loan_stream,
        events=[
            ApplicationSubmitted(payload={
                "application_id": app_id,
                "applicant_id": "COMP-123",
                "requested_amount_usd": 100000.0,
                "loan_purpose": "Test",
                "submission_channel": "api"
            }),
            CreditAnalysisRequested(payload={
                "application_id": app_id,
                "assigned_agent_id": "test",
                "priority": "normal"
            }),
            CreditAnalysisCompleted(payload={
                "application_id": app_id,
                "agent_id": "test",
                "session_id": "test",
                "model_version": "v1",
                "confidence_score": 0.9,
                "risk_tier": "LOW",
                "recommended_limit_usd": 120000,
                "analysis_duration_ms": 100
            })
        ],
        expected_version=-1
    )
    
    # 2. Run Agent
    agent = FraudDetectionAgent(store, mock_registry)
    final_state = await agent.process_application(app_id)
    
    # 3. Assert State
    assert final_state["application_id"] == app_id
    assert final_state["applicant_id"] == "COMP-123"
    assert final_state["total_nodes_executed"] == 5
    assert final_state["total_llm_calls"] == 1
    
    # 4. Verify Output on the loan stream
    loan_events = await store.load_stream(loan_stream)
    assert loan_events[-1].event_type == "FraudScreeningCompleted"
    assert loan_events[-1].payload["fraud_score"] == 0.18
    
    # 5. Verify the Agent Session trace
    sess_id = final_state["session_id"]
    agent_stream = f"agent-fraud-agent-1-{sess_id}"
    
    audit_events = await store.load_stream(agent_stream)
    event_types = [e.event_type for e in audit_events]
    
    # Gas Town checks
    assert event_types[0] == "AgentSessionStarted"
    assert event_types[1] == "AgentContextLoaded"
    assert event_types[2] == "AgentInputValidated"
    
    # Nodes
    assert event_types.count("AgentNodeExecuted") == 5
    
    # Outputs
    assert event_types[-2] == "AgentOutputWritten"
    assert event_types[-1] == "AgentSessionCompleted"
    
    # Verify the AgentOutputWritten tracked the cross-aggregate write correctly
    output_event = next(e for e in audit_events if e.event_type == "AgentOutputWritten")
    assert output_event.payload["events_written"][0]["stream_id"] == loan_stream
    assert output_event.payload["events_written"][0]["event_type"] == "FraudScreeningCompleted"
    assert output_event.payload["events_written"][0]["stream_position"] > 0
