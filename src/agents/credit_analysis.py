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
    AgentToolCalled,
    AgentOutputWritten,
    AgentSessionCompleted,
    StreamNotFoundError,
)
from commands.handlers import handle_credit_analysis_completed, CreditAnalysisCompletedCommand
from registry.client import ApplicantRegistryClient, CompanyProfile

import os
from pydantic import BaseModel, Field

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

class CreditDecisionOutput(BaseModel):
    risk_tier: str = Field(description="The risk tier, e.g. 'LOW', 'MEDIUM', 'HIGH'")
    confidence: float = Field(description="A confidence score between 0.0 and 1.0")
    recommended_limit: float = Field(description="The recommended credit limit in USD")
    data_quality_caveats: list[str] = Field(default_factory=list, description="List of missing fields or quality issues detected")

class CreditAgentState(TypedDict):
    """LangGraph state for CreditAnalysisAgent."""
    application_id: str
    applicant_id: str
    session_id: str
    
    historical_financials: list[dict]
    compliance_flags: list[dict]
    loan_history: list[dict]
    
    current_year_facts: dict
    quality_flags: list[str]
    
    credit_decision: dict
    policy_violations: list[str]
    
    # Audit metrics
    total_nodes_executed: int
    total_llm_calls: int
    total_tokens_used: int
    total_cost_usd: float
    total_duration_ms: int


class CreditAnalysisAgent:
    """
    Evaluates credit risk based on historical financials (from registry)
    and current year extracted facts.
    """

    def __init__(self, store: EventStore, registry_client: ApplicantRegistryClient):
        self.store = store
        self.registry = registry_client
        self.agent_id = "credit-agent-1"
        self.agent_type = "credit_analysis"
        self.model_version = "gemini-2.5-pro"
        
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.getenv("OPENROUTER_API_KEY") or "DUMMY_KEY_FOR_TESTS"
        self.llm = ChatOpenAI(
            model=f"google/{self.model_version}",
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=0
        ).with_structured_output(CreditDecisionOutput)
        
        self.graph = self._build_graph()

    def _build_graph(self):
        builder = StateGraph(CreditAgentState)
        
        builder.add_node("validate_inputs", self.validate_inputs)
        builder.add_node("open_credit_record", self.open_credit_record)
        builder.add_node("load_applicant_registry", self.load_applicant_registry)
        builder.add_node("load_extracted_facts", self.load_extracted_facts)
        builder.add_node("analyze_credit_risk", self.analyze_credit_risk)
        builder.add_node("apply_policy_constraints", self.apply_policy_constraints)
        builder.add_node("write_output", self.write_output)
        
        builder.add_edge(START, "validate_inputs")
        builder.add_edge("validate_inputs", "open_credit_record")
        builder.add_edge("open_credit_record", "load_applicant_registry")
        builder.add_edge("load_applicant_registry", "load_extracted_facts")
        builder.add_edge("load_extracted_facts", "analyze_credit_risk")
        builder.add_edge("analyze_credit_risk", "apply_policy_constraints")
        builder.add_edge("apply_policy_constraints", "write_output")
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

    async def process_application(self, application_id: str) -> CreditAgentState:
        session_id = f"sess-cre-{uuid.uuid4().hex[:8]}"
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
        initial_state: CreditAgentState = {
            "application_id": application_id,
            "applicant_id": applicant_id,
            "session_id": session_id,
            "historical_financials": [],
            "compliance_flags": [],
            "loan_history": [],
            "current_year_facts": {},
            "quality_flags": [],
            "credit_decision": {},
            "policy_violations": [],
            "total_nodes_executed": 0,
            "total_llm_calls": 0,
            "total_tokens_used": 0,
            "total_cost_usd": 0.0,
            "total_duration_ms": 0
        }
        
        final_state = await self.graph.ainvoke(initial_state)
        
        # 3. Call the command handler to write the decision to loan-{id} stream
        # This is safe because AgentContextLoaded was written in step 1.
        decision = final_state["credit_decision"]
        cmd = CreditAnalysisCompletedCommand(
            application_id=application_id,
            agent_id=self.agent_id,
            session_id=session_id,
            model_version=self.model_version,
            confidence_score=decision.get("confidence", 0.85),
            risk_tier=decision.get("risk_tier", "MEDIUM"),
            recommended_limit_usd=decision.get("recommended_limit", 500000.0),
            analysis_duration_ms=final_state["total_duration_ms"],
            input_data_hash="mock_hash",
            data_quality_caveats=decision.get("data_quality_caveats", [])
        )
        new_loan_version = await handle_credit_analysis_completed(cmd, self.store)
        
        # 4. Complete Session and Record Output Written
        # Update the final state to be formatted by write_output which creates AgentOutputWritten
        # Wait, write_output node runs BEFORE this point in LangGraph!
        # So we cannot get the version inside the node. We will just fix the write_output node to NOT write the output event, 
        # and instead write it here. Or we modify the event returned by the node.
        # Let's just create the AgentOutputWritten and AgentSessionCompleted here manually.
        
        
        ev_out = AgentOutputWritten(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "events_written": [
                {
                    "stream_id": f"loan-{application_id}",
                    "event_type": "CreditAnalysisCompleted",
                    "stream_position": new_loan_version
                }
            ],
            "output_summary": f"Risk: {decision.get('risk_tier')}. Limit: {decision.get('recommended_limit')}. Confidence: {decision.get('confidence')}.",
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
            "next_agent_triggered": "fraud_detection",
            "completed_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(session_id, [ev_out, ev_comp])

        return final_state

    # =========================================================================
    # L A N G G R A P H   N O D E S
    # =========================================================================

    async def validate_inputs(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "validate_inputs",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 100
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 100}

    async def open_credit_record(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "open_credit_record",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 50
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 50}

    async def load_applicant_registry(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        
        # Use Registry Client to load historical data
        hist_fin = await self.registry.get_financial_history(state["applicant_id"])
        comp_flags = await self.registry.get_compliance_flags(state["applicant_id"])
        
        ev_tool = AgentToolCalled(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "tool_name": "query_applicant_registry",
            "tool_input_summary": f"applicant_id={state['applicant_id']}",
            "tool_output_summary": f"{len(hist_fin)}yr financials, {len(comp_flags)} flags",
            "tool_duration_ms": 250
        })
        
        ev_node = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "load_applicant_registry",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 300
        })
        await self._append_session_event(sess_id, [ev_tool, ev_node])
        
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "historical_financials": [vars(f) for f in hist_fin],
            "compliance_flags": [vars(f) for f in comp_flags],
            "total_duration_ms": state["total_duration_ms"] + 300
        }

    async def load_extracted_facts(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        
        # Load from docpkg stream
        stream_id = f"docpkg-{state['application_id']}"
        events = await self.store.load_stream(stream_id)
        facts = {}
        for e in events:
            if e.event_type == "ExtractionCompleted":
                facts.update(e.payload.get("facts", {}))
        
        ev_tool = AgentToolCalled(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "tool_name": "load_extracted_facts",
            "tool_input_summary": f"stream_id={stream_id}",
            "tool_output_summary": f"loaded {len(facts)} fields",
            "tool_duration_ms": 100
        })
        
        ev_node = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "load_extracted_facts",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 150
        })
        await self._append_session_event(sess_id, [ev_tool, ev_node])
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1, 
            "total_duration_ms": state["total_duration_ms"] + 150,
            "current_year_facts": facts
        }

    async def analyze_credit_risk(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert credit analysis AI. Analyze the applicant's risk tier ('LOW', 'MEDIUM', 'HIGH'), recommend a credit limit in USD, and provide a confidence score between 0.0 and 1.0. IMPORTANT: If any critical financial data (like EBITDA) is missing or zero, you MUST list this in 'data_quality_caveats'."),
            ("user", "Application ID: {app_id}\nFlags: {flags}\nHistorical Financials: {financials}\nCurrent Documents Facts: {current_facts}")
        ])
        
        chain = prompt | self.llm
        
        t0 = datetime.datetime.now()
        result: CreditDecisionOutput = await chain.ainvoke({
            "app_id": state["application_id"],
            "flags": state["compliance_flags"],
            "financials": state["historical_financials"],
            "current_facts": state["current_year_facts"]
        })
        t1 = datetime.datetime.now()
        duration_ms = int((t1 - t0).total_seconds() * 1000)
        
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "analyze_credit_risk",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": True,
            "llm_tokens_input": 0,
            "llm_tokens_output": 0,
            "llm_cost_usd": 0.0,
            "duration_ms": duration_ms
        })
        await self._append_session_event(sess_id, [ev])
        
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "total_llm_calls": state["total_llm_calls"] + 1,
            "total_tokens_used": state["total_tokens_used"] + 0,
            "total_cost_usd": state["total_cost_usd"] + 0.0,
            "total_duration_ms": state["total_duration_ms"] + duration_ms,
            "credit_decision": {
                "risk_tier": result.risk_tier,
                "confidence": result.confidence,
                "recommended_limit": result.recommended_limit,
                "data_quality_caveats": result.data_quality_caveats
            }
        }

    async def apply_policy_constraints(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "apply_policy_constraints",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 200
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 200, "policy_violations": []}

    async def write_output(self, state: CreditAgentState) -> dict:
        sess_id = state["session_id"]
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": "write_output",
            "node_sequence": state["total_nodes_executed"] + 1,
            "llm_called": False,
            "duration_ms": 100
        })
        await self._append_session_event(sess_id, [ev])
        return {"total_nodes_executed": state["total_nodes_executed"] + 1, "total_duration_ms": state["total_duration_ms"] + 100}
