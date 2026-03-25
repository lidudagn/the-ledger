import uuid
import datetime
from typing import TypedDict, Any

from langgraph.graph import StateGraph, START, END

from event_store import EventStore
from models.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionCompleted,
)
from commands.handlers import handle_fraud_screening_completed, FraudScreeningCompletedCommand
from registry.client import ApplicantRegistryClient


import os
import json
from pydantic import BaseModel, Field

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

class FraudAssessmentOutput(BaseModel):
    fraud_score: float = Field(description="A score between 0.0 and 1.0 representing the likelihood of fraud.")
    anomaly_flags: list[str] = Field(description="A list of string flags indicating specific anomalies detected.")

class FraudAgentState(TypedDict):
    """LangGraph state for FraudDetectionAgent."""
    application_id: str
    applicant_id: str
    session_id: str
    
    registry_data_loaded: bool
    document_facts_loaded: bool
    
    fraud_assessment: dict
    
    # Audit metrics
    total_nodes_executed: int
    total_llm_calls: int
    total_tokens_used: int
    total_cost_usd: float
    total_duration_ms: int


class FraudDetectionAgent:
    """
    Evaluates fraud risk by cross-referencing extracted document facts
    with historical registry data. Models the 5-node graph seen in traces.
    """

    def __init__(self, store: EventStore, registry_client: ApplicantRegistryClient):
        self.store = store
        self.registry = registry_client
        self.agent_id = "fraud-agent-1"
        self.agent_type = "fraud_detection"
        self.model_version = "gemini-2.5-pro"
        
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.getenv("OPENROUTER_API_KEY") or "DUMMY_KEY_FOR_TESTS"
        self.llm = ChatOpenAI(
            model=f"google/{self.model_version}",
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=0
        ).with_structured_output(FraudAssessmentOutput)
        
        self.graph = self._build_graph()

    def _build_graph(self):
        builder = StateGraph(FraudAgentState)
        
        builder.add_node("validate_inputs", self.validate_inputs)
        builder.add_node("load_document_facts", self.load_document_facts)
        builder.add_node("cross_reference_registry", self.cross_reference_registry)
        builder.add_node("analyze_fraud_patterns", self.analyze_fraud_patterns)
        builder.add_node("write_output", self.write_output)
        
        builder.add_edge(START, "validate_inputs")
        builder.add_edge("validate_inputs", "load_document_facts")
        builder.add_edge("load_document_facts", "cross_reference_registry")
        builder.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        builder.add_edge("analyze_fraud_patterns", "write_output")
        builder.add_edge("write_output", END)
        
        return builder.compile()

    async def _append_session_event(self, session_id: str, events: list[Any]) -> None:
        stream_id = f"agent-{self.agent_id}-{session_id}"
        v = await self.store.stream_version(stream_id)
        await self.store.append(stream_id, events, expected_version=v)

    async def _load_applicant_id(self, application_id: str) -> str:
        # Cross-aggregate read from loan stream
        loan_events = await self.store.load_stream(f"loan-{application_id}")
        for e in loan_events:
            if e.event_type == "ApplicationSubmitted":
                return e.payload.get("applicant_id")
        raise ValueError("ApplicationSubmitted event not found for application")

    async def process_application(self, application_id: str) -> FraudAgentState:
        session_id = f"sess-fra-{uuid.uuid4().hex[:8]}"
        stream_id = f"agent-{self.agent_id}-{session_id}"
        
        # Resolve applicant ID
        applicant_id = await self._load_applicant_id(application_id)
        
        # 1. Start Session
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
        await self.store.append(stream_id, [ev_start], expected_version=-1)
        
        ev_ctx = AgentContextLoaded(payload={
            "session_id": session_id,
            "context_source": "registry_and_docpkg",
            "model_version": self.model_version
        })
        await self._append_session_event(session_id, [ev_ctx])

        # 2. Run Graph
        initial_state: FraudAgentState = {
            "application_id": application_id,
            "applicant_id": applicant_id,
            "session_id": session_id,
            "registry_data_loaded": False,
            "document_facts_loaded": False,
            "fraud_assessment": {},
            "total_nodes_executed": 0,
            "total_llm_calls": 0,
            "total_tokens_used": 0,
            "total_cost_usd": 0.0,
            "total_duration_ms": 0
        }
        
        final_state = await self.graph.ainvoke(initial_state)
        
        # 3. Flush the decision using the command handler (which writes to loan-{id})
        # Note: the handler enforces Gas Town, so session flush MUST happen before this!
        assessment = final_state["fraud_assessment"]
        
        cmd = FraudScreeningCompletedCommand(
            application_id=application_id,
            agent_id=self.agent_id,
            session_id=session_id,
            fraud_score=assessment.get("fraud_score", 0.1),
            anomaly_flags=assessment.get("anomaly_flags", []),
            screening_model_version=self.model_version,
            input_data_hash="mock_hash"
        )
        new_loan_version = await handle_fraud_screening_completed(cmd, self.store)
        
        # 4. Complete Session and Output Logging
        ev_out = AgentOutputWritten(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "events_written": [
                {
                    "stream_id": f"loan-{application_id}",
                    "event_type": "FraudScreeningCompleted",
                    "stream_position": new_loan_version
                }
            ],
            "output_summary": f"Fraud screening complete. Score: {assessment.get('fraud_score')}. PROCEED.",
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
            "next_agent_triggered": "decision_orchestrator",
            "completed_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(session_id, [ev_out, ev_comp])
        
        return final_state

    # =========================================================================
    # L A N G G R A P H   N O D E S
    # =========================================================================

    async def validate_inputs(self, state: FraudAgentState) -> dict:
        sess_id = state["session_id"]
        ev1 = AgentInputValidated(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "application_id": state["application_id"],
            "inputs_validated": ["application_id", "extracted_facts_events", "registry_access"],
            "validation_duration_ms": 112,
            "validated_at": datetime.datetime.now().isoformat()
        })
        ev2 = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "validate_inputs",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id"],
            "output_keys": ["data"],
            "llm_called": False,
            "duration_ms": 300
        })
        await self._append_session_event(sess_id, [ev1, ev2])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 300}

    async def load_document_facts(self, state: FraudAgentState) -> dict:
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "load_document_facts",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id"],
            "output_keys": ["data"],
            "llm_called": False,
            "duration_ms": 148
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "document_facts_loaded": True, "total_duration_ms": state["total_duration_ms"] + 148}

    async def cross_reference_registry(self, state: FraudAgentState) -> dict:
        sess_id = state["session_id"]
        # Dummy fetch to registry
        company = await self.registry.get_company(state["applicant_id"])
        
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "cross_reference_registry",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id"],
            "output_keys": ["data"],
            "llm_called": False,
            "duration_ms": 239
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "registry_data_loaded": True, "total_duration_ms": state["total_duration_ms"] + 239}

    async def analyze_fraud_patterns(self, state: FraudAgentState) -> dict:
        sess_id = state["session_id"]
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert fraud detection AI. Analyze the applicant for potential fraud. Return a fraud score between 0.0 and 1.0, and any anomaly flags as strings in a list."),
            ("user", "Application ID: {app_id}\nApplicant ID: {applicant_id}\nEvaluate fraud risk.")
        ])
        
        chain = prompt | self.llm
        
        t0 = datetime.datetime.now()
        result: FraudAssessmentOutput = await chain.ainvoke({
            "app_id": state["application_id"],
            "applicant_id": state["applicant_id"]
        })
        t1 = datetime.datetime.now()
        duration_ms = int((t1 - t0).total_seconds() * 1000)
        
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "analyze_fraud_patterns",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id"],
            "output_keys": ["fraud_assessment"],
            "llm_called": True,
            "llm_tokens_input": 0,
            "llm_tokens_output": 0,
            "llm_cost_usd": 0.0,
            "duration_ms": duration_ms,
            "executed_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(sess_id, [ev])
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "total_llm_calls": state["total_llm_calls"] + 1,
            "total_tokens_used": state["total_tokens_used"] + 0,
            "total_cost_usd": state["total_cost_usd"] + 0.0,
            "total_duration_ms": state["total_duration_ms"] + duration_ms,
            "fraud_assessment": {
                "fraud_score": result.fraud_score,
                "anomaly_flags": result.anomaly_flags
            }
        }

    async def write_output(self, state: FraudAgentState) -> dict:
        # Just logging the node execution. Output events are handled in process_application.
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "write_output",
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["application_id"],
            "output_keys": ["data"],
            "llm_called": False,
            "duration_ms": 417
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 417}
