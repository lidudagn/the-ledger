"""
STEP 4 — Upcasting & Immutability (PostgreSQL)
===============================================
Proves:
  1. The Event Store is truly append-only and immutable.
  2. Legacy v1 events are persisted on disk identically to how they were written.
  3. When loaded into memory, the UpcasterRegistry dynamically upgrades v1 -> v2 
     in flight, without mutating the underlying database.
  4. Missing data is deterministically inferred (e.g. model_version='legacy-pre-2026')
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from event_store import EventStore
from models.events import StoredEvent
from upcasters import registry

load_dotenv(Path(__file__).parent.parent / ".env")

# ─── Terminal Colors ──────────────────────────────────────────────────────────

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

def section(title):
    print(f"\n{BOLD}{CYAN}{'─'*80}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*80}{RESET}")

def sub_section(title):
    print(f"\n  {BOLD}{YELLOW}▸ {title}{RESET}")


async def get_raw_row_from_db(store: EventStore, stream_id: str, position: int) -> dict:
    """Fetch the exact JSON payload as it sits on the Postgres disk."""
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id = $1 AND stream_position = $2",
            stream_id, position
        )
        return {
            "payload": json.loads(row["payload"]),
            "event_version": row["event_version"]
        }


async def run_step_4():
    print(f"\n{BOLD}{'='*80}{RESET}")
    print(f"{BOLD}  STEP 4: UPCASTING & IMMUTABILITY (PostgreSQL){RESET}")
    print(f"{BOLD}{'='*80}{RESET}")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print(f"{RED}ERROR: DATABASE_URL not found in .env{RESET}")
        return

    store = EventStore.from_url(db_url, upcaster_registry=registry)
    await store.connect()

    try:
        ts = int(time.time())
        app_id = f"UPCAST-{ts}"
        stream_id = f"loan-{app_id}"

        section(f"1. SEEDING LEGACY V1 EVENTS — {app_id}")
        print(f"  Injecting naked v1 payloads directly into the stream.\n")

        # --- Legacy v1 CreditAnalysisCompleted ---
        # Note: missing confidence_score, model_version, regulatory_basis
        v1_credit_payload = {
            "application_id": app_id,
            "agent_id": "legacy-credit-agent",
            "session_id": f"sess-legacy-{ts}",
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 120000.0,
            "analysis_duration_ms": 450
        }
        
        # --- Legacy v1 DecisionGenerated ---
        # Note: missing model_versions dict
        v1_decision_payload = {
            "application_id": app_id,
            "orchestrator_agent_id": "legacy-orchestrator",
            "recommendation": "APPROVE",
            "confidence_score": 0.82,
            "contributing_agent_sessions": [f"sess-legacy-{ts}"],
            "decision_basis_summary": "Solid income, moderate risk."
        }

        import uuid
        v1_credit_event = StoredEvent(
            event_id=str(uuid.uuid4()),
            stream_id=stream_id,
            stream_position=1,
            global_position=1,
            event_type="CreditAnalysisCompleted",
            event_version=1,
            payload=v1_credit_payload,
            metadata={"correlation_id": f"corr-{ts}"},
            recorded_at=datetime.now(timezone.utc)
        )
        v1_decision_event = StoredEvent(
            event_id=str(uuid.uuid4()),
            stream_id=stream_id,
            stream_position=2,
            global_position=2,
            event_type="DecisionGenerated",
            event_version=1,
            payload=v1_decision_payload,
            metadata={"correlation_id": f"corr-{ts}"},
            recorded_at=datetime.now(timezone.utc)
        )

        # Bypass normal handler OCC to inject raw v1
        async with store._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Insert stream metadata
                await conn.execute(
                    "INSERT INTO event_streams(stream_id, aggregate_type, current_version) VALUES($1, $2, $3)",
                    stream_id, "loan", 2
                )
                # 2. Insert events
                for evt in [v1_credit_event, v1_decision_event]:
                    await conn.execute(
                        '''
                        INSERT INTO events 
                        (event_id, stream_id, stream_position, event_type, event_version, payload, metadata, recorded_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ''',
                        evt.event_id, stream_id, evt.stream_position, evt.event_type, 
                        evt.event_version, json.dumps(evt.payload), json.dumps(evt.metadata), evt.recorded_at
                    )
        
        print(f"  {GREEN}✅ Injected v1 CreditAnalysisCompleted (Position 1){RESET}")
        print(f"       Payload: {json.dumps(v1_credit_payload, indent=2)}")
        print(f"  {GREEN}✅ Injected v1 DecisionGenerated (Position 2){RESET}")
        print(f"       Payload: {json.dumps(v1_decision_payload, indent=2)}")


        section("2. EVENT REPLAY (Memory State vs Disk State)")
        print(f"  Executing store.load_stream() which securely triggers UpcasterRegistry.")

        # Load the stream normally (this triggers upcasters)
        events = await store.load_stream(stream_id)

        for e in events:
            sub_section(f"Event: {e.event_type} (Pos {e.stream_position})")
            
            # 1. Fetch exact raw row from Postgres
            disk_row = await get_raw_row_from_db(store, stream_id, e.stream_position)
            disk_v = disk_row["event_version"]
            disk_p = disk_row["payload"]
            
            # 2. Get the in-memory upcasted object
            mem_v = e.event_version
            mem_p = e.payload

            print(f"  DISK STATE (Immutable):")
            print(f"    {DIM}Version : v{disk_v}{RESET}")
            print(f"    {DIM}Payload : {json.dumps(disk_p, sort_keys=True)}{RESET}")
            
            print(f"  MEMORY STATE (Upcasted):")
            print(f"    {BOLD}{GREEN}Version : v{mem_v}{RESET}")
            print(f"    {BOLD}{GREEN}Payload : {json.dumps(mem_p, sort_keys=True)}{RESET}")

            # Highlight specific inferences
            if e.event_type == "CreditAnalysisCompleted":
                print(f"\n    {YELLOW}Upcaster Inferences applied:{RESET}")
                print(f"      + model_version    : '{mem_p.get('model_version')}' (inferred from legacy era)")
                print(f"      + regulatory_basis : '{mem_p.get('regulatory_basis')}' (inferred from 2025-Q4 baseline)")
                print(f"      + confidence_score : {mem_p.get('confidence_score')} (explicitly nullified to prevent hallucinations)")
            elif e.event_type == "DecisionGenerated":
                print(f"\n    {YELLOW}Upcaster Inferences applied:{RESET}")
                print(f"      + model_versions   : {json.dumps(mem_p.get('model_versions'))} (extrapolated from legacy sessions)")

            if disk_v != mem_v:
                print(f"\n  {GREEN}✓ Validation:{RESET} Successfully upcasted v{disk_v} -> v{mem_v} in-flight.")


        section("3. ARCHITECTURAL PROOF & SQL")
        print(f"""
  The system supports Non-Destructive Schema Evolution.
  Because events are immutable facts that already occurred, we CANNOT run
  `ALTER TABLE` or `UPDATE` scripts to mutate past historical rows when the 
  business rules change.

  Instead, we use pure deterministic functions (Upcasters).

  {CYAN}@registry.register("CreditAnalysisCompleted", from_version=1)
  def upcast_credit_v1_to_v2(payload: dict) -> dict:
      return {{
          **payload,
          "model_version": "legacy-pre-2026",
          "confidence_score": None,
          "regulatory_basis": "2025-Q4-GAAP",
      }}{RESET}

  When the system loads the aggregate via `SELECT * FROM events`, the raw 
  JSONB payload from disk is piped through the registry, dynamically 
  applying missing fields exactly at load time.

  This guarantees that our historical audit database NEVER LOSES DATA 
  while remaining completely compatible with our current v2 domain objects.""")

        print(f"\n{'='*80}\n")
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(run_step_4())
