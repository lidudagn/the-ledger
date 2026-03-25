import pytest

from event_store import EventStore
from in_memory_store import InMemoryEventStore
from agents.document_processing import DocumentProcessingAgent
from models.events import (
    PackageCreated,
    DocumentUploaded,
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentOutputWritten,
    AgentSessionCompleted,
    PackageReadyForAnalysis,
)


@pytest.fixture
def store() -> EventStore:
    return InMemoryEventStore()


from unittest.mock import patch
from langchain_core.runnables import RunnableLambda
from agents.document_processing import QualityAssessmentOutput

@pytest.mark.asyncio
@patch("agents.document_processing.ChatOpenAI.with_structured_output")
async def test_document_processing_agent_end_to_end(mock_with_structured_output, store):
    async def mock_invoke(*args, **kwargs):
        return QualityAssessmentOutput(quality_assessment="PASS")
    mock_with_structured_output.return_value = RunnableLambda(mock_invoke)

    # 1. Setup docpkg aggregate with some documents
    app_id = "TEST-001"
    docpkg_stream = f"docpkg-{app_id}"
    
    await store.append(
        stream_id=docpkg_stream,
        events=[
            PackageCreated(payload={"package_id": app_id}),
            DocumentUploaded(payload={"document_id": "doc-1", "document_type": "income_statement"}),
            DocumentUploaded(payload={"document_id": "doc-2", "document_type": "balance_sheet"}),
        ],
        expected_version=-1
    )
    
    # 2. Run Agent
    agent = DocumentProcessingAgent(store)
    final_state = await agent.process_application(app_id)
    
    # 3. Verify LangGraph state output
    assert final_state["application_id"] == app_id
    assert final_state["total_nodes_executed"] == 3
    assert final_state["total_llm_calls"] == 1
    assert final_state["quality_assessment"] == "PASS"
    assert len(final_state["documents"]) == 2
    
    # 4. Verify Output on the docpkg aggregate
    docpkg_events = await store.load_stream(docpkg_stream)
    assert docpkg_events[-1].event_type == "PackageReadyForAnalysis"
    assert docpkg_events[-1].payload["package_id"] == app_id
    
    # 5. Verify the full Audit Trail in the agent session stream
    session_id = final_state["session_id"]
    agent_stream = f"agent-document_processing-{session_id}"
    audit_events = await store.load_stream(agent_stream)
    
    event_types = [e.event_type for e in audit_events]
    
    # Gas Town pattern requirement
    assert event_types[0] == "AgentSessionStarted"
    assert event_types[1] == "AgentContextLoaded"
    
    # Pipeline execution tracking
    assert "AgentInputValidated" in event_types
    assert event_types.count("AgentNodeExecuted") == 3
    assert event_types.count("AgentToolCalled") == 2
    
    # Output and termination
    assert event_types[-2] == "AgentOutputWritten"
    assert event_types[-1] == "AgentSessionCompleted"
    
    # Verify AgentOutputWritten captures the cross-aggregate write position
    output_event = next(e for e in audit_events if e.event_type == "AgentOutputWritten")
    assert output_event.payload["events_written"][0]["stream_id"] == docpkg_stream
    assert output_event.payload["events_written"][0]["event_type"] == "PackageReadyForAnalysis"
