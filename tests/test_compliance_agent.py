import pytest

from event_store import EventStore
from in_memory_store import InMemoryEventStore
from agents.compliance import ComplianceAgent
from models.events import (
    ApplicationSubmitted,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
    ComplianceCheckCompleted,
)
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
from agents.compliance import ComplianceRuleOutput

@pytest.mark.asyncio
@patch("agents.compliance.ChatOpenAI.with_structured_output")
async def test_compliance_agent_end_to_end(mock_with_structured_output, store, mock_registry):
    async def mock_invoke(*args, **kwargs):
        reg_id = kwargs.get("reg_id")
        return ComplianceRuleOutput(status="PASS", reasoning=f"Test reasoning for {reg_id}")
    mock_with_structured_output.return_value = RunnableLambda(mock_invoke)
    # Setup loan aggregate
    app_id = "APP-002"
    loan_stream = f"loan-{app_id}"
    
    await store.append(
        stream_id=loan_stream,
        events=[
            ApplicationSubmitted(payload={
                "application_id": app_id,
                "applicant_id": "COMP-999",
                "requested_amount_usd": 100000.0,
                "loan_purpose": "Test",
                "submission_channel": "api"
            })
        ],
        expected_version=-1
    )
    
    # Run Agent
    agent = ComplianceAgent(store, mock_registry)
    final_state = await agent.process_application(app_id)
    
    # Assert State
    assert final_state["application_id"] == app_id
    assert final_state["applicant_id"] == "COMP-999"
    assert final_state["total_nodes_executed"] == 6
    assert final_state["pass_count"] == 6
    assert final_state["fail_count"] == 0
    assert final_state["overall_status"] == "CLEAR"
    
    # Determine the stream names
    comp_stream = f"compliance-{app_id}"
    sess_id = final_state["session_id"]
    agent_stream = f"agent-compliance-agent-1-{sess_id}"
    
    # Verify ComplianceCheckCompleted is in the compliance stream
    comp_events = await store.load_stream(comp_stream)
    assert len(comp_events) == 7
    assert comp_events[-1].event_type == "ComplianceCheckCompleted"
    assert comp_events[-1].payload["status"] == "CLEAR"
    
    # Verify the AgentSession trace
    audit_events = await store.load_stream(agent_stream)
    event_types = [e.event_type for e in audit_events]
    
    # Gas Town checks
    assert event_types[0] == "AgentSessionStarted"
    assert event_types[1] == "AgentContextLoaded"
    assert event_types[2] == "AgentInputValidated"
    
    # Nodes
    assert event_types.count("AgentNodeExecuted") == 6
    
    # Outputs
    assert event_types[-2] == "AgentOutputWritten"
    assert event_types[-1] == "AgentSessionCompleted"
    
    # Verify the AgentOutputWritten tracked the cross-aggregate write correctly
    output_event = next(e for e in audit_events if e.event_type == "AgentOutputWritten")
    assert output_event.payload["events_written"][0]["stream_id"] == comp_stream
    assert output_event.payload["events_written"][0]["event_type"] == "ComplianceCheckCompleted"
    assert output_event.payload["events_written"][0]["stream_position"] > 0
