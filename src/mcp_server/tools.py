"""
The Ledger — MCP Command Tools (Phase 5)

Exposes 11 domain commands as FastMCP tools.
Implements:
  1. Idempotency (best-effort, single process)
  2. MCP-boundary defensive preconditions (UX)
  3. Structured error handling for LLM consumption
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from fastmcp import Context

from mcp_server.server import mcp, get_state
from models.events import DomainError, OptimisticConcurrencyError
from aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from commands.handlers import (
    SubmitApplicationCommand,
    RequestCreditAnalysisCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    GenerateDecisionCommand,
    ApproveApplicationCommand,
    HumanReviewCompletedCommand,
    StartAgentSessionCommand,
    RequestComplianceCheckCommand,
    RequestDecisionCommand,
    RecordComplianceRulePassedCommand,
    RecordComplianceRuleFailedCommand,
    handle_submit_application,
    handle_request_credit_analysis,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_decision_generated,
    handle_approve_application,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_compliance_check_requested,
    handle_decision_requested,
    handle_compliance_rule_passed,
    handle_compliance_rule_failed,
)
from integrity.audit_chain import run_integrity_check

logger = logging.getLogger(__name__)


# =============================================================================
# INFRA / HELPERS
# =============================================================================

def format_error(
    error_type: str, message: str, stream_id: str = "", suggested_action: str = "fix_input"
) -> dict[str, Any]:
    """Return a structured error dictionary for LLM consumption."""
    return {
        "error_type": error_type,
        "message": message,
        "stream_id": stream_id,
        "suggested_action": suggested_action,
    }


def check_idempotency(key: str | None) -> dict[str, Any] | None:
    """
    Check best-effort, single-process idempotency cache.
    Returns cached result dict if key exists and isn't expired (5 min TTL).
    Performs lazy eviction of expired entries to prevent memory leaks.
    """
    if not key:
        return None
        
    cache = get_state().idempotency_cache

    # Lazy TTL cleanup: evict expired entries when cache grows
    if len(cache) > 50:
        now = time.monotonic()
        expired = [k for k, (exp, _) in cache.items() if now >= exp]
        for k in expired:
            del cache[k]

    if key in cache:
        expire_at, result = cache[key]
        if time.monotonic() < expire_at:
            logger.info(f"Idempotency cache hit for key: {key}")
            return result
        else:
            del cache[key]
            
    return None


def cache_result(key: str | None, result: dict[str, Any]) -> dict[str, Any]:
    """Store result in best-effort memory cache for 5 minutes."""
    if key:
        ttl = 300  # 5 minutes
        get_state().idempotency_cache[key] = (time.monotonic() + ttl, result)
    return result


async def _run_handler_with_errors(
    handler_coro, stream_id: str
) -> dict[str, Any] | None:
    """Execute handler and trap standard errors into structured JSON."""
    try:
        return await handler_coro
    except OptimisticConcurrencyError as e:
        return format_error(
            "OptimisticConcurrencyError",
            e.message,
            stream_id=e.stream_id,
            suggested_action="reload_stream_and_retry",
        )
    except DomainError as e:
        return format_error(
            "DomainError",
            e.message,
            stream_id=stream_id,
            suggested_action="fix_input",
        )
    except Exception as e:
        return format_error(
            "SystemError",
            str(e),
            stream_id=stream_id,
            suggested_action="check_server_logs",
        )


# =============================================================================
# TOOLS
# =============================================================================

@mcp.tool()
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str = "",
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Submit a new loan application. Fails if application_id already exists.
    No preconditions. This creates a new loan application stream.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    stream_id = f"loan-{application_id}"

    # MCP-boundary precondition (Defensive UX)
    if await store.stream_version(stream_id) > 0:
        return format_error("PreconditionFailed", f"Application '{application_id}' already exists.", stream_id)

    cmd = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose=loan_purpose,
        correlation_id=idempotency_key,
    )

    err = await _run_handler_with_errors(handle_submit_application(cmd, store), stream_id)
    if isinstance(err, dict):
        return err  # Handler failed

    return cache_result(idempotency_key, {
        "stream_id": stream_id,
        "initial_version": 1
    })


@mcp.tool()
async def request_credit_analysis(
    application_id: str,
    assigned_agent_id: str = "",
    priority: str = "normal",
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Request credit analysis for a submitted application.
    Transitions SUBMITTED → AWAITING_ANALYSIS.
    Domain handler is authoritative for state validation.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    rate_limits = get_state().rate_limits
    rl_key = ("credit_analysis", application_id)
    now = time.monotonic()
    
    if rl_key in rate_limits:
        last_called = rate_limits[rl_key]
        if now - last_called < 60.0:
            return format_error("RateLimitExceeded", f"Rate limit of 1 request/min exceeded for {application_id}", f"loan-{application_id}", "wait_and_retry")
            
    rate_limits[rl_key] = now

    store = get_state().store
    stream_id = f"loan-{application_id}"

    cmd = RequestCreditAnalysisCommand(
        application_id=application_id,
        assigned_agent_id=assigned_agent_id,
        priority=priority,
        correlation_id=idempotency_key,
    )

    result = await _run_handler_with_errors(
        handle_request_credit_analysis(cmd, store), stream_id
    )

    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "stream_id": stream_id,
        "new_stream_version": result,
        "state": "AWAITING_ANALYSIS",
    })


@mcp.tool()
async def start_agent_session(
    agent_id: str,
    session_id: str,
    context_source: str = "fresh",
    model_version: str = "",
    context_token_count: int = 0,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Gas Town pattern: MUST call before `record_credit_analysis` or `record_fraud_screening`.
    Creates an agent session with context loaded.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    stream_id = f"agent-{agent_id}-{session_id}"

    # MCP-boundary precondition
    version = await store.stream_version(stream_id)
    if version > 0:
        return format_error("PreconditionFailed", f"Session '{session_id}' for agent '{agent_id}' already exists.", stream_id)

    cmd = StartAgentSessionCommand(
        agent_id=agent_id, session_id=session_id, context_source=context_source,
        model_version=model_version, context_token_count=context_token_count,
        correlation_id=idempotency_key
    )

    err = await _run_handler_with_errors(handle_start_agent_session(cmd, store), stream_id)
    if isinstance(err, dict):
        return err

    return cache_result(idempotency_key, {
        "session_stream_id": stream_id,
        "version": 2  # Started + ContextLoaded events
    })


@mcp.tool()
async def record_credit_analysis(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit_usd: float,
    analysis_duration_ms: int = 0,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Record completed credit analysis. 
    Preconditions: 
    1) Active session must exist (`start_agent_session` called).
    2) Application must be in AWAITING_ANALYSIS state.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    app_stream = f"loan-{application_id}"
    session_stream = f"agent-{agent_id}-{session_id}"

    # MCP-boundary preconditions (Defensive UX)
    if await store.stream_version(session_stream) == 0:
        return format_error("PreconditionFailed", "No active agent session. Call start_agent_session first.", session_stream, "check_preconditions")

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state != ApplicationState.AWAITING_ANALYSIS:
            return format_error("PreconditionFailed", f"Application not in AWAITING_ANALYSIS state. Current: {app.state.value}", app_stream, "check_preconditions")
    except Exception:
        pass  # Let authoritative domain handler fail

    cmd = CreditAnalysisCompletedCommand(
        application_id=application_id, agent_id=agent_id, session_id=session_id,
        model_version=model_version, confidence_score=confidence_score,
        risk_tier=risk_tier, recommended_limit_usd=recommended_limit_usd,
        analysis_duration_ms=analysis_duration_ms, correlation_id=idempotency_key
    )

    result = await _run_handler_with_errors(handle_credit_analysis_completed(cmd, store), app_stream)
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {"new_stream_version": result})


@mcp.tool()
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    anomaly_flags: list[str],
    screening_model_version: str = "",
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Record completed fraud screening.
    Preconditions:
    1) Active session must exist.
    2) Application must be in ANALYSIS_COMPLETE state (after credit analysis).
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    app_stream = f"loan-{application_id}"
    session_stream = f"agent-{agent_id}-{session_id}"

    if await store.stream_version(session_stream) == 0:
        return format_error("PreconditionFailed", "No active agent session. Call start_agent_session first.", session_stream)

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state != ApplicationState.ANALYSIS_COMPLETE:
            return format_error("PreconditionFailed", f"Application not in ANALYSIS_COMPLETE state. Current: {app.state.value}", app_stream)
    except Exception:
        pass

    cmd = FraudScreeningCompletedCommand(
        application_id=application_id, agent_id=agent_id, session_id=session_id,
        fraud_score=fraud_score, anomaly_flags=anomaly_flags,
        screening_model_version=screening_model_version, correlation_id=idempotency_key
    )

    result = await _run_handler_with_errors(handle_fraud_screening_completed(cmd, store), app_stream)
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {"new_stream_version": result})


@mcp.tool()
async def request_compliance_check(
    application_id: str,
    regulation_set_version: str = "",
    checks_required: list[str] = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Transitions application to COMPLIANCE_REVIEW. 
    Call `record_compliance_rule` for each rule, then `request_decision`.
    Precondition: Application in ANALYSIS_COMPLETE state.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    app_stream = f"loan-{application_id}"

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state != ApplicationState.ANALYSIS_COMPLETE:
            return format_error("PreconditionFailed", f"Application not in ANALYSIS_COMPLETE state. Current: {app.state.value}", app_stream)
    except Exception:
        pass

    cmd = RequestComplianceCheckCommand(
        application_id=application_id, regulation_set_version=regulation_set_version,
        checks_required=checks_required or [], correlation_id=idempotency_key
    )

    result = await _run_handler_with_errors(handle_compliance_check_requested(cmd, store), app_stream)
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "new_stream_version": result,
        "state": "COMPLIANCE_REVIEW"
    })


@mcp.tool()
async def record_compliance_rule(
    application_id: str,
    rule_id: str,
    passed: bool,
    rule_version: str = "",
    failure_reason: str = "",
    evidence_hash: str = "",
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Record a compliance rule pass or fail.
    Failure requires `failure_reason`.
    Precondition: Loan application stream must exist.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    compliance_stream = f"compliance-{application_id}"

    if not passed and not failure_reason:
        return format_error("ValidationError", "failure_reason is required when passed=False", compliance_stream)

    if await store.stream_version(f"loan-{application_id}") == 0:
        return format_error("PreconditionFailed", f"Application stream does not exist", f"loan-{application_id}")

    if passed:
        cmd = RecordComplianceRulePassedCommand(
            application_id=application_id, rule_id=rule_id, rule_version=rule_version,
            evidence_hash=evidence_hash, correlation_id=idempotency_key
        )
        result = await _run_handler_with_errors(handle_compliance_rule_passed(cmd, store), compliance_stream)
    else:
        cmd = RecordComplianceRuleFailedCommand(
            application_id=application_id, rule_id=rule_id, rule_version=rule_version,
            failure_reason=failure_reason, correlation_id=idempotency_key
        )
        result = await _run_handler_with_errors(handle_compliance_rule_failed(cmd, store), compliance_stream)

    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "compliance_stream_version": result,
        "rule_result": "PASSED" if passed else "FAILED"
    })


@mcp.tool()
async def request_decision(
    application_id: str,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Transitions application to PENDING_DECISION. 
    Must be called after all compliance rules are recorded. Required before `generate_decision`.
    Precondition: Application in COMPLIANCE_REVIEW.
    """
    if cached := check_idempotency(idempotency_key):
        return cached
        
    store = get_state().store
    app_stream = f"loan-{application_id}"

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state != ApplicationState.COMPLIANCE_REVIEW:
            return format_error("PreconditionFailed", f"Application not in COMPLIANCE_REVIEW state. Current: {app.state.value}", app_stream)
    except Exception:
        pass

    cmd = RequestDecisionCommand(application_id=application_id, correlation_id=idempotency_key)
    result = await _run_handler_with_errors(handle_decision_requested(cmd, store), app_stream)
    
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "new_stream_version": result,
        "state": "PENDING_DECISION"
    })


@mcp.tool()
async def generate_decision(
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list[str],
    decision_basis_summary: str = "",
    model_versions: dict[str, str] = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Orchestrator decision. Recommendation: APPROVE/DECLINE/REFER.
    Confidence < 0.60 forces REFER regardless of recommendation.
    Preconditions: Application in PENDING_DECISION state. Each contributing session stream exists.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    app_stream = f"loan-{application_id}"

    # Verify input sessions exist (Defensive UX)
    for session_id in contributing_agent_sessions:
        if await store.stream_version(session_id) == 0:
            return format_error("PreconditionFailed", f"Contributing session '{session_id}' does not exist.", session_id)

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state != ApplicationState.PENDING_DECISION:
            return format_error("PreconditionFailed", f"Application not in PENDING_DECISION state. Current: {app.state.value}", app_stream)
    except Exception:
        pass

    cmd = GenerateDecisionCommand(
        application_id=application_id, orchestrator_agent_id=orchestrator_agent_id,
        recommendation=recommendation, confidence_score=confidence_score,
        contributing_agent_sessions=contributing_agent_sessions,
        decision_basis_summary=decision_basis_summary,
        model_versions=model_versions or {}, correlation_id=idempotency_key
    )

    result = await _run_handler_with_errors(handle_decision_generated(cmd, store), app_stream)
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "new_stream_version": result,
        "final_recommendation": recommendation if confidence_score >= 0.60 else "REFER"
    })


@mcp.tool()
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    override: bool,
    final_decision: str,
    override_reason: str = "",
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """
    Record human review outcome.
    Preconditions: Application in reviewable state. If override=True, override_reason required.
    Auth Note: Authentication is OUT OF SCOPE. In production, requires RBAC.
    """
    if cached := check_idempotency(idempotency_key):
        return cached

    store = get_state().store
    app_stream = f"loan-{application_id}"

    if override and not override_reason:
        return format_error("PreconditionFailed", "override_reason is required when override=True", app_stream)

    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.state not in (ApplicationState.APPROVED_PENDING_HUMAN, ApplicationState.DECLINED_PENDING_HUMAN, ApplicationState.PENDING_DECISION):
            return format_error("PreconditionFailed", f"Application not reviewable. Current state: {app.state.value}", app_stream)
    except Exception:
        pass

    cmd = HumanReviewCompletedCommand(
        application_id=application_id, reviewer_id=reviewer_id, override=override,
        final_decision=final_decision, override_reason=override_reason,
        correlation_id=idempotency_key
    )

    result = await _run_handler_with_errors(handle_human_review_completed(cmd, store), app_stream)
    if isinstance(result, dict) and "error_type" in result:
        return result

    return cache_result(idempotency_key, {
        "final_decision": final_decision,
        "application_state": "FINAL_APPROVED" if final_decision == "APPROVE" else "FINAL_DECLINED"
    })


@mcp.tool()
async def run_integrity_audit_check(
    entity_type: str,
    entity_id: str,
) -> dict[str, Any]:
    """
    Run cryptographic audit integrity check.
    LIMITATION: Rate limited to 1 check per 60s per entity per process (not distributed).
    Long-running tools timeout after 5s.
    """
    rate_limits = get_state().rate_limits
    key = (entity_type, entity_id)
    now = time.monotonic()
    
    if key in rate_limits:
        last_called = rate_limits[key]
        if now - last_called < 60.0:
            return format_error("RateLimitExceeded", f"Rate limit of 1 check/min exceeded for {entity_type}-{entity_id}", f"audit-{entity_type}-{entity_id}", "wait_and_retry")
            
    rate_limits[key] = now
    
    try:
        # Wrap the audit check in a 5 second timeout
        result = await asyncio.wait_for(
            run_integrity_check(get_state().store, entity_type, entity_id),
            timeout=5.0
        )
        return {
            "events_verified": result.events_verified,
            "integrity_hash": result.integrity_hash,
            "chain_valid": result.chain_valid,
            "tamper_detected": result.tamper_detected
        }
    except asyncio.TimeoutError:
        return format_error("TimeoutExceeded", f"Integrity check for {entity_type}-{entity_id} took longer than 5s.", f"audit-{entity_type}-{entity_id}", "wait_and_retry")
    except Exception as e:
        return format_error("SystemError", str(e), f"audit-{entity_type}-{entity_id}", "check_server_logs")
