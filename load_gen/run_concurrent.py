import asyncio
import asyncpg
import time
import os
import sys

# Ensure src in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from event_store import EventStore, create_pool
from projections.daemon import ProjectionDaemon
from projections.base import PgProjectionStore
from projections.application_summary import ApplicationSummaryProjection
from projections.agent_performance import AgentPerformanceLedgerProjection
from projections.compliance_audit import ComplianceAuditViewProjection
from models.events import ApplicationSubmitted, CreditAnalysisCompleted, ComplianceCheckInitiated, ComplianceRulePassed

async def stress_test():
    from dotenv import load_dotenv
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL not set.")
        return
    pool = await create_pool(dsn, min_size=5, max_size=25)
    
    app_pool = await create_pool(dsn, min_size=5, max_size=25)
    
    event_store = EventStore(pool)
    pg_proj_store = PgProjectionStore(app_pool) # Using application database for projections as per architecture
    
    print("Cleaning up database for fresh stress test...")
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints CASCADE")
    async with app_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE application_summary, agent_performance, compliance_audit_events, compliance_snapshots CASCADE")
    
    # Wait, where is the schema applied? We assume it's created.
    daemon = ProjectionDaemon(
        event_store, 
        [
            ApplicationSummaryProjection(pg_proj_store),
            AgentPerformanceLedgerProjection(pg_proj_store),
            ComplianceAuditViewProjection(pg_proj_store)
        ]
    )
    
    print("Starting concurrent OCC load generation...")
    
    async def worker(loan_id):
        # We simulate multiple concurrent attempts on the identical stream to force OCC retries
        app_id = f"STRESS-{loan_id}"
        
        # 1. Base App submitted
        await event_store.append(
            stream_id=f"loan-{app_id}",
            events=[
                ApplicationSubmitted(payload={
                    "application_id": app_id, "applicant_id": f"APP-{loan_id}",
                    "requested_amount_usd": 100000.0,
                    "loan_purpose": "expansion", "submission_channel": "web",
                    "submitted_at": "2026-03-21T00:00:00Z"
                })
            ],
            expected_version=-1
        )

        # 2. Fire 3 concurrent updates
        async def concurrent_append(agent, i):
            from commands.handlers import with_occ_retry
            try:
                @with_occ_retry(max_retries=10)
                async def perform_append(store):
                    ver = await store.stream_version(f"loan-{app_id}")
                    await store.append(
                        stream_id=f"loan-{app_id}",
                        events=[
                            CreditAnalysisCompleted(payload={
                                "application_id": app_id, "agent_id": agent,
                                "session_id": f"s-{loan_id}-{i}", "model_version": "v1.0",
                                "confidence_score": 0.9, "risk_tier": "LOW",
                                "recommended_limit_usd": 90000.0,
                                "analysis_duration_ms": 1000, "input_data_hash": "abc"
                            })
                        ],
                        expected_version=ver
                    )
                await perform_append(event_store)
            except Exception as e:
                print(f"Failed OCC completely for {app_id}: {e}")

        await asyncio.gather(
            concurrent_append("agent-A", 1),
            concurrent_append("agent-B", 2),
            concurrent_append("agent-C", 3)
        )

        # 3. Compliance event
        await event_store.append(
            stream_id=f"compliance-{app_id}",
            events=[
                ComplianceCheckInitiated(payload={
                    "application_id": app_id, "regulation_set_version": "2026-Q1",
                    "checks_required": ["KYC", "AML"]
                }),
                ComplianceRulePassed(payload={
                    "application_id": app_id, "rule_id": "KYC", "rule_version": "v1",
                    "evaluation_timestamp": "2026-03-21T00:01:00Z"
                })
            ],
            expected_version=-1
        )

    t0 = time.time()
    batch = 50
    print(f"Initiating {batch} concurrent loan applications (3 agents each)...")
    await asyncio.gather(*(worker(i) for i in range(batch)))
    
    print("Appends complete, draining daemon...")
    lags = {}
    for _ in range(20):
        await daemon.run_once()
        lags = await daemon.get_all_lags()
        if sum(d["lag_events"] for d in lags.values()) == 0:
            break
        await asyncio.sleep(0.5)
        
    print(f"Daemon lag resolved: {lags}")
    
    # Verify DB
    count = await pg_proj_store.count("application_summary")
    print(f"Total Applications in DB: {count}")
    
    print(f"Finished in {time.time() - t0:.2f}s")
    
if __name__ == "__main__":
    asyncio.run(stress_test())
