"""
The Ledger — MCP Query Resources (Phase 5)

Exposes 6 read-optimized resources via FastMCP.
Resources return `null` on "not found" to be LLM-friendly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
import inspect

from fastmcp import Context

from mcp_server.server import mcp, get_state


logger = logging.getLogger(__name__)


@mcp.resource("ledger://applications/{application_id}")
async def get_application_summary(application_id: str) -> dict[str, Any] | None:
    """Read-optimized summary of a loan application's current state."""
    state = get_state()
    if not state.app_summary:
        return None
    
    # Returns None if not found, per LLM-friendly design
    res = state.app_summary.get_summary(application_id)
    if inspect.isawaitable(res):
        res = await res
    return res


@mcp.resource("ledger://applications/{application_id}/compliance")
async def get_compliance_record(
    application_id: str, 
    as_of: str | None = None
) -> dict[str, Any] | None:
    """
    Comprehensive compliance audit record.
    If `as_of` (ISO8601 timestamp) provided, returns temporal state at that moment.
    """
    state = get_state()
    if not state.compliance_audit:
        return None
        
    try:
        if as_of:
            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            res = state.compliance_audit.get_compliance_at(application_id, dt)
        else:
            res = state.compliance_audit.get_current_compliance(application_id)
            
        if inspect.isawaitable(res):
            res = await res
        return res
    except Exception as e:
        logger.warning(f"Failed to fetch compliance record for {application_id}: {e}")
        return None


@mcp.resource("ledger://applications/{application_id}/audit-trail")
async def get_application_audit_trail(application_id: str) -> list[dict[str, Any]]:
    """
    Raw, ordered list of all domain events for an application.
    Justified stream replay: audit trail *is* the raw events.
    """
    state = get_state()
    if not state.store:
        return []
        
    try:
        events = await state.store.load_stream(f"loan-{application_id}")
        return [e.to_store_dict() for e in events]
    except Exception as e:
        logger.warning(f"Failed to load stream loan-{application_id}: {e}")
        return []


@mcp.resource("ledger://agents/{agent_id}/performance")
async def get_agent_performance(agent_id: str) -> list[dict[str, Any]]:
    """Performance metrics (counts, averages, ratios) for an AI agent."""
    state = get_state()
    if not state.agent_perf:
        return []
        
    all_perf = state.agent_perf.get_all_performance()
    if inspect.isawaitable(all_perf):
        all_perf = await all_perf
        
    return [p for p in all_perf if p.get("agent_id") == agent_id]


@mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
async def get_agent_session_events(agent_id: str, session_id: str) -> list[dict[str, Any]]:
    """
    Full event stream for a specific agent session.
    Justified stream replay: needed to inspect agent reasoning chain.
    """
    state = get_state()
    if not state.store:
        return []
        
    try:
        events = await state.store.load_stream(f"agent-{agent_id}-{session_id}")
        return [e.to_store_dict() for e in events]
    except Exception as e:
        logger.warning(f"Failed to load agent session stream {session_id}: {e}")
        return []


@mcp.resource("ledger://ledger/health")
async def get_ledger_health() -> dict[str, Any]:
    """
    System health, projection lag metrics, and daemon status.
    Monitors SLO violations across projections.
    """
    state = get_state()
    
    health = {
        "status": "unknown",
        "store_connected": state.store is not None,
        "daemon_alive": state.daemon_task is not None and not state.daemon_task.done(),
        "last_processed_at": datetime.now(timezone.utc).isoformat(),
        "projections": {}
    }
    
    if state.daemon:
        lags = state.daemon.get_all_lags()
        if inspect.isawaitable(lags):
            lags = await lags
        
        has_degraded = False
        has_critical = False
        
        for name, metrics in lags.items():
            lag_ms = metrics["lag_ms"]
            # Fast approximations of our daemon.py SLOs
            max_lag = state.daemon._slo_thresholds.get(name, 500)
            
            p_health = "healthy"
            if lag_ms > max_lag * 5:
                p_health = "critical"
                has_critical = True
            elif lag_ms > max_lag:
                p_health = "degraded"
                has_degraded = True
                
            health["projections"][name] = {
                "health": p_health,
                **metrics
            }
            
        if has_critical:
            health["status"] = "critical"
        elif has_degraded or not health["daemon_alive"]:
            health["status"] = "degraded"
        else:
            health["status"] = "healthy"
            
    return health
