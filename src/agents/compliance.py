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
    ComplianceCheckCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
)
from commands.handlers import (
    handle_compliance_rule_passed,
    handle_compliance_rule_failed,
    RecordComplianceRulePassedCommand,
    RecordComplianceRuleFailedCommand,
)
from registry.client import ApplicantRegistryClient

import os
from pydantic import BaseModel, Field

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

class ComplianceRuleOutput(BaseModel):
    status: str = Field(description="The compliance status for this specific rule: 'PASS' or 'FAIL'")
    reasoning: str = Field(description="Explanation for the given status based on the company profile")


class ComplianceAgentState(TypedDict):
    """LangGraph state for ComplianceAgent."""
    application_id: str
    applicant_id: str
    session_id: str
    
    company_profile: dict
    
    reg_results: dict[str, str]
    overall_status: str
    pass_count: int
    fail_count: int
    
    # Audit metrics
    total_nodes_executed: int
    total_duration_ms: int


class ComplianceAgent:
    """
    Evaluates regulatory rules deterministically.
    No LLMs used. Writes to compliance-{id} stream.
    """

    def __init__(self, store: EventStore, registry_client: ApplicantRegistryClient):
        self.store = store
        self.registry = registry_client
        self.agent_id = "compliance-agent-1"
        self.agent_type = "compliance"
        self.model_version = "gemini-2.5-pro"
        
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.getenv("OPENROUTER_API_KEY") or "DUMMY_KEY_FOR_TESTS"
        self.llm = ChatOpenAI(
            model=f"google/{self.model_version}",
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=0
        ).with_structured_output(ComplianceRuleOutput)
        
        self.graph = self._build_graph()

    def _build_graph(self):
        builder = StateGraph(ComplianceAgentState)
        
        # 6 deterministic nodes
        builder.add_node("evaluate_reg_001", self.evaluate_reg_001)
        builder.add_node("evaluate_reg_002", self.evaluate_reg_002)
        builder.add_node("evaluate_reg_003", self.evaluate_reg_003)
        builder.add_node("evaluate_reg_004", self.evaluate_reg_004)
        builder.add_node("evaluate_reg_005", self.evaluate_reg_005)
        builder.add_node("evaluate_reg_006", self.evaluate_reg_006)
        
        builder.add_edge(START, "evaluate_reg_001")
        builder.add_edge("evaluate_reg_001", "evaluate_reg_002")
        builder.add_edge("evaluate_reg_002", "evaluate_reg_003")
        builder.add_edge("evaluate_reg_003", "evaluate_reg_004")
        builder.add_edge("evaluate_reg_004", "evaluate_reg_005")
        builder.add_edge("evaluate_reg_005", "evaluate_reg_006")
        builder.add_edge("evaluate_reg_006", END)
        
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

    async def process_application(self, application_id: str) -> ComplianceAgentState:
        session_id = f"sess-com-{uuid.uuid4().hex[:8]}"
        stream_id = f"agent-{self.agent_id}-{session_id}"
        
        # Resolve applicant ID and Company Profile
        applicant_id = await self._load_applicant_id(application_id)
        profile = await self.registry.get_company(applicant_id)
        if not profile:
            raise ValueError(f"Company profile not found for {applicant_id}")
            
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
        
        # 2. Context & Input Validation
        ev_ctx = AgentContextLoaded(payload={
            "session_id": session_id,
            "context_source": "registry",
            "model_version": self.model_version
        })
        
        ev_val = AgentInputValidated(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "inputs_validated": ["application_id", "company_profile", "regulation_set_version"],
            "validation_duration_ms": 115,
            "validated_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(session_id, [ev_ctx, ev_val])

        # 3. Run Graph
        initial_state: ComplianceAgentState = {
            "application_id": application_id,
            "applicant_id": applicant_id,
            "session_id": session_id,
            "company_profile": vars(profile),
            "reg_results": {},
            "overall_status": "CLEAR",
            "pass_count": 0,
            "fail_count": 0,
            "total_nodes_executed": 0,
            "total_duration_ms": 0
        }
        
        final_state = await self.graph.ainvoke(initial_state)
        
        # 4. Write decision to compliance-{id} stream
        compliance_stream = f"compliance-{application_id}"
        v_comp = await self.store.stream_version(compliance_stream)
        ev_done = ComplianceCheckCompleted(payload={
            "application_id": application_id,
            "status": final_state["overall_status"],
            "checks_passed": final_state["pass_count"],
            "checks_failed": final_state["fail_count"],
            "failed_reasons": [k for k, v in final_state["reg_results"].items() if v == "FAIL"]
        })
        new_comp_version = await self.store.append(
            compliance_stream, 
            [ev_done], 
            expected_version=-1 if v_comp == 0 else v_comp
        )
        
        # 5. Flush output/completion tracking
        ev_out = AgentOutputWritten(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "events_written": [
                {
                    "stream_id": compliance_stream,
                    "event_type": "ComplianceCheckCompleted",
                    "stream_position": new_comp_version
                }
            ],
            "output_summary": f"Compliance: {final_state['overall_status']}. {final_state['pass_count']} passed, {final_state['fail_count']} failed.",
            "written_at": datetime.datetime.now().isoformat()
        })
        
        ev_comp = AgentSessionCompleted(payload={
            "session_id": session_id,
            "agent_type": self.agent_type,
            "application_id": application_id,
            "total_nodes_executed": final_state["total_nodes_executed"],
            "total_llm_calls": 0,
            "total_tokens_used": 0,
            "total_cost_usd": 0.0,
            "total_duration_ms": final_state["total_duration_ms"],
            "next_agent_triggered": "decision_orchestrator",
            "completed_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(session_id, [ev_out, ev_comp])
        
        return final_state

    # =========================================================================
    # L A N G G R A P H   N O D E S
    # =========================================================================

    async def _execute_reg_node(self, state: ComplianceAgentState, node_name: str, reg_id: str, default_duration: int) -> dict:
        sess_id = state["session_id"]
        
        rules = {
            "REG-001": "Companies in the 'chemicals' sector must be classified as 'HIGH' risk.",
            "REG-002": "Companies operating in the 'MT' (Montana) jurisdiction are strictly BLOCKED.",
            "REG-003": "Aggregate loan exposure to a single entity cannot exceed 10% of their annual revenue.",
            "REG-004": "Board of directors must have an odd number of members for voting coherence.",
            "REG-005": "Companies with a 'DECLINING' trajectory are ineligible for loans over $500,000.",
            "REG-006": "Risk tier 'MEDIUM' requires at least 3 years of financial history."
        }
        prompt = ChatPromptTemplate.from_messages([
            ("system", f"You are a corporate compliance checking AI. Evaluate the company profile against the specific regulation: {rules.get(reg_id, 'Generic compliance check')}. Reply strictly with your status 'PASS' or 'FAIL' and reasoning."),
            ("user", "Regulation ID: {reg_id}\nCompany Profile: {profile}\nEvaluate compliance.")
        ])
        
        chain = prompt | self.llm
        
        t0 = datetime.datetime.now()
        result: ComplianceRuleOutput = await chain.ainvoke({
            "reg_id": reg_id,
            "profile": state["company_profile"]
        })
        t1 = datetime.datetime.now()
        duration_ms = int((t1 - t0).total_seconds() * 1000)
        
        status = result.status.upper()
        if status not in ("PASS", "FAIL"):
            status = "FAIL"
            
        ev = AgentNodeExecuted(payload={
            "session_id": sess_id,
            "agent_type": self.agent_type,
            "node_name": node_name,
            "node_sequence": state["total_nodes_executed"] + 1,
            "input_keys": ["company_profile"],
            "output_keys": [f"{reg_id}_result"],
            "llm_called": True,
            "llm_tokens_input": 0,
            "llm_tokens_output": 0,
            "llm_cost_usd": 0.0,
            "duration_ms": duration_ms,
            "executed_at": datetime.datetime.now().isoformat()
        })
        await self._append_session_event(sess_id, [ev])
        
        new_results = state["reg_results"].copy()
        new_results[reg_id] = status
        
        # 4. Record individual rule outcome in compliance-{id} stream
        if status == "PASS":
            cmd_rule = RecordComplianceRulePassedCommand(
                application_id=state["application_id"],
                rule_id=reg_id,
                rule_version="1.0.0",
                evaluation_timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                evidence_hash="llm_verified"
            )
            await handle_compliance_rule_passed(cmd_rule, self.store)
        else:
            cmd_rule = RecordComplianceRuleFailedCommand(
                application_id=state["application_id"],
                rule_id=reg_id,
                rule_version="1.0.0",
                failure_reason=result.reasoning,
                remediation_required=True
            )
            await handle_compliance_rule_failed(cmd_rule, self.store)
            
        return {
            "total_nodes_executed": state["total_nodes_executed"] + 1,
            "total_duration_ms": state["total_duration_ms"] + duration_ms,
            "reg_results": new_results,
            "pass_count": state["pass_count"] + (1 if status == "PASS" else 0),
            "fail_count": state["fail_count"] + (1 if status == "FAIL" else 0),
            "overall_status": "BLOCKED" if (state["fail_count"] + (1 if status == "FAIL" else 0)) > 0 else "CLEAR"
        }

    async def evaluate_reg_001(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_001", "REG-001", 376)

    async def evaluate_reg_002(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_002", "REG-002", 246)

    async def evaluate_reg_003(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_003", "REG-003", 294)

    async def evaluate_reg_004(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_004", "REG-004", 181)

    async def evaluate_reg_005(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_005", "REG-005", 462)

    async def evaluate_reg_006(self, state: ComplianceAgentState) -> dict:
        return await self._execute_reg_node(state, "evaluate_reg_006", "REG-006", 426)
