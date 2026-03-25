"""
The Ledger — MCP Server (Phase 5)

Exposes the event store and projections as an enterprise MCP server.
Provides 10 tools for the command side and 6 resources for the query side.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time

import asyncpg
from fastmcp import FastMCP

from event_store import EventStore, create_pool
from projections.application_summary import ApplicationSummaryProjection
from projections.agent_performance import AgentPerformanceLedgerProjection
from projections.compliance_audit import ComplianceAuditViewProjection
from projections.daemon import ProjectionDaemon


logger = logging.getLogger(__name__)


@dataclass
class AppState:
    """Singleton application state for the MCP server."""
    store: EventStore | None = None
    app_summary: ApplicationSummaryProjection | None = None
    agent_perf: AgentPerformanceLedgerProjection | None = None
    compliance_audit: ComplianceAuditViewProjection | None = None
    daemon: ProjectionDaemon | None = None
    daemon_task: asyncio.Task | None = None
    pool: asyncpg.Pool | None = None
    
    # Idempotency cache: key -> (expire_at_monotonic, result_dict)
    idempotency_cache: dict[str, tuple[float, dict]] = field(default_factory=dict)
    
    # Rate limit tracking: (entity_type, entity_id) -> last_call_monotonic
    rate_limits: dict[tuple[str, str], float] = field(default_factory=dict)


# Global singleton accessible to tools and resources
_app_state = AppState()

def get_state() -> AppState:
    return _app_state


@asynccontextmanager
async def lifespan(server: FastMCP):
    """
    Lifecycle manager for the MCP server.
    Initializes PostgreSQL pool, projections, and starts the daemon.
    """
    import os
    db_url = os.environ.get(
        "DATABASE_URL", 
        "postgresql://postgres:postgres@localhost:5432/the_ledger"
    )
    
    logger.info("Starting up MCP Server...")
    
    # 1. Initialize Event Store
    pool = await create_pool(db_url)
    _app_state.pool = pool
    _app_state.store = EventStore(pool)
    
    # 2. Initialize Projections (using PgProjectionStore for production)
    from projections.base import PgProjectionStore
    pg_store = PgProjectionStore(pool)
    
    _app_state.app_summary = ApplicationSummaryProjection(pg_store)
    _app_state.agent_perf = AgentPerformanceLedgerProjection(pg_store)
    _app_state.compliance_audit = ComplianceAuditViewProjection(pg_store)
    
    # 3. Start Projection Daemon
    _app_state.daemon = ProjectionDaemon(
        store=_app_state.store,
        projections=[
            _app_state.app_summary,
            _app_state.agent_perf,
            _app_state.compliance_audit,
        ]
    )
    
    # Set SLOs based on specs
    _app_state.daemon.set_slo("application_summary", 500)
    _app_state.daemon.set_slo("agent_performance_ledger", 500)
    _app_state.daemon.set_slo("compliance_audit_view", 2000)
    
    _app_state.daemon_task = asyncio.create_task(_app_state.daemon.run_forever())
    logger.info("Projection daemon started in background")
    
    try:
        yield
    finally:
        logger.info("Shutting down MCP Server...")
        if _app_state.daemon:
            await _app_state.daemon.stop()
        if _app_state.daemon_task:
            await _app_state.daemon_task
        if _app_state.pool:
            await _app_state.pool.close()


mcp = FastMCP(
    "The Ledger",
    lifespan=lifespan,
)

# Import tools and resources to register decorators
import mcp_server.tools
import mcp_server.resources

if __name__ == "__main__":
    import os
    port = int(os.environ.get("MCP_PORT", "8765"))
    mcp.run(transport="sse", port=port)
