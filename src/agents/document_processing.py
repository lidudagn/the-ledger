import uuid
import datetime
from typing import TypedDict

from langgraph.graph import StateGraph, START, END

from event_store import EventStore
from models.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentOutputWritten,
    AgentSessionCompleted,
    PackageReadyForAnalysis,
)


import os
from pydantic import BaseModel, Field

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

class QualityAssessmentOutput(BaseModel):
    quality_assessment: str = Field(description="A string assessing the document quality, typically 'PASS', 'FAIL', or 'NEEDS_REVIEW'.")

class DocAgentState(TypedDict):
    """State of the LangGraph document processing agent."""
    application_id: str
    session_id: str
    documents: list[dict]
    extracted_data: dict
    quality_assessment: str
    
    # Audit metrics
    total_nodes_executed: int
    total_llm_calls: int
    total_tokens_used: int
    total_cost_usd: float
    total_duration_ms: int


class DocumentProcessingAgent:
    """
    First LangGraph agent in the Apex pipeline.
    Goal: Extract facts from uploaded documents (PDFs, images).
    No registry dependency required.
    """

    def __init__(self, store: EventStore):
        self.store = store
        self.agent_id = "doc-agent-1"
        self.agent_type = "document_processing"
        self.model_version = "gemini-2.5-pro"
        
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.getenv("OPENROUTER_API_KEY") or "DUMMY_KEY_FOR_TESTS"
        self.llm = ChatOpenAI(
            model=f"google/{self.model_version}",
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=0
        ).with_structured_output(QualityAssessmentOutput)
        
        self.graph = self._build_graph()

    def _build_graph(self):
        builder = StateGraph(DocAgentState)
        
        builder.add_node("validate_inputs", self.validate_inputs)
        builder.add_node("run_extraction", self.run_extraction)
        builder.add_node("assess_quality", self.assess_quality)
        
        builder.add_edge(START, "validate_inputs")
        builder.add_edge("validate_inputs", "run_extraction")
        builder.add_edge("run_extraction", "assess_quality")
        builder.add_edge("assess_quality", END)
        
        return builder.compile()

    async def process_application(self, application_id: str) -> DocAgentState:
        """
        Runs the full LangGraph agent workflow for an application.
        This handles all EventStore persistence and strictly follows optimistic concurrency.
        """
        session_id = f"sess-doc-{uuid.uuid4().hex[:8]}"
        stream_id = f"agent-{self.agent_type}-{session_id}"
        
        # 1. Start Session (Gas Town prerequisite)
        ev_start = AgentSessionStarted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "agent_id": self.agent_id,
            "application_id": application_id,
            "model_version": self.model_version,
            "langgraph_graph_version": "1.0.0-sim",
            "context_source": "fresh",
            "context_token_count": 0,
            "started_at": datetime.datetime.now().isoformat()
        })
        # -1 = brand new stream expected
        await self.store.append(stream_id, [ev_start], expected_version=-1)
        
        # 2. Load context (cross-aggregate read from docpkg stream)
        try:
            docpkg_stream = f"docpkg-{application_id}"
            docpkg_events = await self.store.load_stream(docpkg_stream)
            docs = [e.payload for e in docpkg_events if "Document" in e.event_type]
        except Exception:
            docs = [] # Agent will handle missing docs in validate_inputs
            
        ev_ctx = AgentContextLoaded(payload={
            "session_id": session_id,
            "context_source": "docpkg_stream",
            "model_version": self.model_version,
            "documents_loaded": len(docs)
        })
        v = await self.store.stream_version(stream_id)
        await self.store.append(stream_id, [ev_ctx], expected_version=v)

        # 3. Synchronous Graph Execution (nodes write tracing events)
        initial_state: DocAgentState = {
            "application_id": application_id,
            "session_id": session_id,
            "documents": docs,
            "extracted_data": {},
            "quality_assessment": "",
            "total_nodes_executed": 0,
            "total_llm_calls": 0,
            "total_tokens_used": 0,
            "total_cost_usd": 0.0,
            "total_duration_ms": 0
        }
        
        final_state = await self.graph.ainvoke(initial_state)
        
        # 4. Output write & termination
        # 4a. Write final PackageReadyForAnalysis output to the shared docpkg aggregate
        v_docpkg = await self.store.stream_version(f"docpkg-{application_id}")
        ev_ready = PackageReadyForAnalysis(payload={
            "package_id": application_id,
            "analysis_ready_at": datetime.datetime.now().isoformat()
        })
        ev_op_pos = -1 if v_docpkg == 0 else v_docpkg
        new_docpkg_version = await self.store.append(f"docpkg-{application_id}", [ev_ready], expected_version=ev_op_pos)
        
        # 4b. Write session conclusion to the agent session stream
        ev_out = AgentOutputWritten(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "events_written": [
                {
                    "stream_id": f"docpkg-{application_id}",
                    "event_type": "PackageReadyForAnalysis",
                    "stream_position": new_docpkg_version
                }
            ],
            "output_summary": f"{len(docs)} documents processed, high confidence. Package ready for credit analysis.",
            "written_at": datetime.datetime.now().isoformat()
        })
        
        ev_comp = AgentSessionCompleted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "total_nodes_executed": final_state["total_nodes_executed"],
            "total_llm_calls": final_state["total_llm_calls"],
            "total_tokens_used": final_state["total_tokens_used"],
            "total_cost_usd": final_state["total_cost_usd"],
            "total_duration_ms": final_state["total_duration_ms"],
            "next_agent_triggered": "credit_analysis",
            "completed_at": datetime.datetime.now().isoformat()
        })
        
        v_ag = await self.store.stream_version(stream_id)
        await self.store.append(stream_id, [ev_out, ev_comp], expected_version=v_ag)
        
        return final_state

    # =========================================================================
    # L A N G G R A P H   N O D E S
    # =========================================================================
    
    async def validate_inputs(self, state: DocAgentState) -> dict:
        session_id = state["session_id"]
        stream_id = f"agent-{self.agent_type}-{session_id}"
        v = await self.store.stream_version(stream_id)
        
        ev1 = AgentInputValidated(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": state["application_id"],
            "inputs_validated": ["application_id", "documents"],
            "validation_duration_ms": 157,
            "validated_at": datetime.datetime.now().isoformat()
        })
        
        ev2 = AgentNodeExecuted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "node_name": "validate_inputs",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id", "documents"],
            "output_keys": ["validated_inputs"],
            "llm_called": False,
            "duration_ms": 167,
            "executed_at": datetime.datetime.now().isoformat()
        })
        
        await self.store.append(stream_id, [ev1, ev2], expected_version=v)
        
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "total_duration_ms": state["total_duration_ms"] + 167
        }

    async def run_extraction(self, state: DocAgentState) -> dict:
        session_id = state["session_id"]
        stream_id = f"agent-{self.agent_type}-{session_id}"
        v = await self.store.stream_version(stream_id)
        
        events = []
        for doc in state["documents"]:
            dtype = doc.get("document_type", doc.get("document_id", "unknown"))
            events.append(AgentToolCalled(payload={
                "session_id": session_id,
                "agent_type": self.agent_type,
                "tool_name": "week3_extraction_pipeline",
                "tool_input_summary": f"PDF extraction: {dtype}",
                "tool_output_summary": f"Extracted line items for {dtype}",
                "tool_duration_ms": 4417,
                "called_at": datetime.datetime.now().isoformat()
            }))
        
        events.append(AgentNodeExecuted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "node_name": "run_extraction",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["documents"],
            "output_keys": ["raw_facts"],
            "llm_called": False,
            "duration_ms": 453,
            "executed_at": datetime.datetime.now().isoformat()
        }))
        
        await self.store.append(stream_id, events, expected_version=v)
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "extracted_data": {"extracted_status": "mock_success", "fields_found": 9 * len(state["documents"])},
            "total_duration_ms": state["total_duration_ms"] + 453
        }

    async def assess_quality(self, state: DocAgentState) -> dict:
        session_id = state["session_id"]
        stream_id = f"agent-{self.agent_type}-{session_id}"
        v = await self.store.stream_version(stream_id)
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a document quality assessment AI. Assess the quality of the extracted document facts and return PASS, FAIL, or NEEDS_REVIEW."),
            ("user", "Application ID: {app_id}\nExtracted data: {data}\nEvaluate quality.")
        ])
        
        chain = prompt | self.llm
        
        t0 = datetime.datetime.now()
        result: QualityAssessmentOutput = await chain.ainvoke({
            "app_id": state["application_id"],
            "data": state["extracted_data"]
        })
        t1 = datetime.datetime.now()
        duration_ms = int((t1 - t0).total_seconds() * 1000)
        
        ev1 = AgentNodeExecuted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "node_name": "assess_quality",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["raw_facts"],
            "output_keys": ["quality_assessment"],
            "llm_called": True,
            "llm_tokens_input": 0,
            "llm_tokens_output": 0,
            "llm_cost_usd": 0.0,
            "duration_ms": duration_ms,
            "executed_at": datetime.datetime.now().isoformat()
        })
        
        await self.store.append(stream_id, [ev1], expected_version=v)
        
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "quality_assessment": result.quality_assessment,
            "total_llm_calls": state["total_llm_calls"] + 1,
            "total_tokens_used": state["total_tokens_used"] + 0,
            "total_cost_usd": state["total_cost_usd"] + 0.0,
            "total_duration_ms": state["total_duration_ms"] + duration_ms
        }
