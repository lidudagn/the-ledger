"""
STEP 5 — Gas Town Recovery (PostgreSQL)
========================================
Proves:
  1. The AgentSessionAggregate tracks agent state via the event log.
  2. reconstruct_agent_context() can rebuild prose context for an LLM after a crash.
  3. Pending work is deterministically identified, preventing duplicate LLM execution.
  4. Resumption is cleanly handled via AgentSessionRecovered linking the streams.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from event_store import EventStore
from models.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentSessionRecovered,
)
from integrity.gas_town import reconstruct_agent_context

load_dotenv(Path(__file__).parent.parent / ".env")

# ─── Terminal Colors ──────────────────────────────────────────────────────────

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
MAGENTA = "\033[95m"
RESET = "\033[0m"

def section(title):
    print(f"\n{BOLD}{CYAN}{'─'*80}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*80}{RESET}")

def sub_section(title):
    print(f"\n  {BOLD}{YELLOW}▸ {title}{RESET}")


async def run_step_5():
    print(f"\n{BOLD}{'='*80}{RESET}")
    print(f"{BOLD}  STEP 5: GAS TOWN RECOVERY (PostgreSQL){RESET}")
    print(f"{BOLD}{'='*80}{RESET}")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print(f"{RED}ERROR: DATABASE_URL not found in .env{RESET}")
        return

    # No upcasters needed for Step 5
    store = EventStore.from_url(db_url)
    await store.connect()

    try:
        ts = int(time.time())
        agent_id = "credit-reviewer"
        crashed_session_id = f"crash-{ts}"
        crashed_stream_id = f"agent-{agent_id}-{crashed_session_id}"

        expected_nodes = ["FetchCreditRecords", "AnalyzeRiskTier", "GenerateRecommendation"]

        section(f"1. THE CRASH — Session: {crashed_session_id}")
        print(f"  Simulating a multi-step LLM pipeline that halts unexpectedly.\n")

        # Start Session + Load Context (Gas Town Requirement)
        await store.append(
            stream_id=crashed_stream_id,
            events=[
                AgentSessionStarted(payload={"agent_id": agent_id, "session_id": crashed_session_id}),
                AgentContextLoaded(payload={
                    "agent_id": agent_id,
                    "session_id": crashed_session_id,
                    "context_source": "loan-APP-123",
                    "model_version": "gpt-4-turbo",
                })
            ],
            expected_version=-1,
            correlation_id=f"corr-{ts}"
        )
        print(f"  {DIM}[Log] Session started. Context loaded.{RESET}")
        time.sleep(0.5)

        # Node 1: Fetched Records
        await store.append(
            stream_id=crashed_stream_id,
            events=[
                AgentNodeExecuted(payload={
                    "node_name": "FetchCreditRecords",
                    "status": "success",
                    "llm_cost_usd": 0.02
                })
            ],
            expected_version=2,
            correlation_id=f"corr-{ts}"
        )
        print(f"  {GREEN}✅ Executed Node: FetchCreditRecords{RESET}")
        time.sleep(0.5)

        # Node 2: Analyzed Risk Tier
        await store.append(
            stream_id=crashed_stream_id,
            events=[
                AgentNodeExecuted(payload={
                    "node_name": "AnalyzeRiskTier",
                    "status": "success",
                    "llm_cost_usd": 0.05
                })
            ],
            expected_version=3,
            correlation_id=f"corr-{ts}"
        )
        print(f"  {GREEN}✅ Executed Node: AnalyzeRiskTier{RESET}")
        time.sleep(0.5)

        print(f"\n  {RED}{BOLD}💥 FATAL ERROR: Process killed (OOM / Pod Eviction){RESET}")
        print(f"  {DIM}The stream is incomplete. 'GenerateRecommendation' never ran.{RESET}")


        section("2. HISTORICAL RECONSTRUCTION")
        print(f"  Calling reconstruct_agent_context() on the crashed stream.")

        context = await reconstruct_agent_context(
            store=store,
            agent_id=agent_id,
            session_id=crashed_session_id,
            expected_nodes=expected_nodes,
            token_budget=4000
        )

        sub_section("Reconstructed State")
        print(f"  Health                : {MAGENTA}{context.session_health_status}{RESET}")
        print(f"  Last Successful Node  : {GREEN}{context.last_successful_node}{RESET}")
        print(f"  Pending Work          : {YELLOW}{context.pending_work}{RESET}")
        print(f"  Executed in Order     : {DIM}{context.executed_nodes_order}{RESET}")

        sub_section("Prose Context for Resumed LLM Prompt")
        for line in context.context_text.split('\n'):
            print(f"  {DIM}│{RESET} {line}")


        section("3. RESUMPTION & CONTINUITY")
        resumed_session_id = f"resume-{ts}"
        resumed_stream_id = f"agent-{agent_id}-{resumed_session_id}"
        
        print(f"  Creating new session `{resumed_session_id}` linked to crashed session.\n")

        await store.append(
            stream_id=resumed_stream_id,
            events=[
                AgentSessionStarted(payload={"agent_id": agent_id, "session_id": resumed_session_id}),
                AgentContextLoaded(payload={
                    "agent_id": agent_id,
                    "session_id": resumed_session_id,
                    "context_source": "loan-APP-123",
                    "event_replay_from_position": context.last_event_position,
                    "model_version": "gpt-4-turbo"
                }),
                AgentSessionRecovered(payload={
                    "recovered_from_session_id": crashed_session_id,
                    "recovery_point_position": context.last_event_position,
                    "pending_work": context.pending_work
                })
            ],
            expected_version=-1,
            correlation_id=f"corr-{ts}"
        )

        print(f"  {GREEN}✅ Injected AgentSessionRecovered{RESET}")
        print(f"  {DIM}The new agent loop uses 'pending_work' to skip 'FetchCreditRecords' and 'AnalyzeRiskTier'.{RESET}")
        
        # Node 3: Generated Recommendation
        await store.append(
            stream_id=resumed_stream_id,
            events=[
                AgentNodeExecuted(payload={
                    "node_name": "GenerateRecommendation",
                    "status": "success",
                    "llm_cost_usd": 0.08
                })
            ],
            expected_version=3,
            correlation_id=f"corr-{ts}"
        )
        print(f"  {GREEN}✅ Executed Node: GenerateRecommendation{RESET}")


        section("4. ARCHITECTURAL PROOF")
        print(f"""
  The system supports Deterministic Agent Crash Recovery.
  By persisting AI agent progress as discrete `AgentNodeExecuted` events in a 
  dedicated stream (`agent-credit-crash-xx`), we ensure we never pay for duplicate 
  LLM calls or duplicate side-effects (e.g. sending two emails) when a pod dies.

  Instead of keeping state in memory, the `reconstruct_agent_context()` method:
  1. Identifies the exact `last_successful_node`.
  2. Deduces the `pending_work` based on the defined pipeline.
  3. Prepares a token-budgeted prose summary so the new LLM retains full memory 
     of its prior thoughts.
  4. Binds the old process to the new via `AgentSessionRecovered`.
""")

        print(f"\n{'='*80}\n")
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(run_step_5())
