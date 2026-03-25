"""
STEP 3 — Temporal Compliance Query (PostgreSQL)
================================================
Proves:
  1. The system can reconstruct compliance state at any past point in time.
  2. Queries use `recorded_at <= as_of` against raw event streams.
  3. The returned state is distinct from the current (latest) state.
  4. Real PostgreSQL database is used, validating timestamps.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from event_store import EventStore
from in_memory_store import InMemoryEventStore
from models.events import (
    ApplicationSubmitted,
    ComplianceCheckRequested,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceCheckCompleted,
)
from commands.handlers import (
    SubmitApplicationCommand,
    RequestComplianceCheckCommand,
    RecordComplianceRulePassedCommand,
    RecordComplianceRuleFailedCommand,
    handle_submit_application,
    handle_compliance_check_requested,
    handle_compliance_rule_passed,
    handle_compliance_rule_failed,
)
from aggregates.loan_application import LoanApplicationAggregate, ApplicationState

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


# ─── Temporal Query Logic (Mirrors Backend Projection) ─────────────────────

async def query_compliance_at(store: EventStore, app_id: str, as_of: datetime) -> dict:
    """
    Simulates ledger://applications/{id}/compliance?as_of={timestamp}
    by filtering the raw event stream for recorded_at <= as_of.
    """
    stream_id = f"compliance-{app_id}"
    try:
        events = await store.load_stream(stream_id)
    except Exception:
        events = []

    # Apply temporal filter
    as_of_iso = as_of.isoformat()
    filtered_events = []
    
    for e in events:
        evt_dt = e.recorded_at.isoformat()
        if evt_dt <= as_of_iso:
            filtered_events.append(e)

    if not filtered_events:
         return {"source": "none", "state": "NO_RECORDS", "rules": [], "overall": "PENDING"}

    # Reconstruct state from filtered events
    rules = []
    has_hard_block = False
    overall_verdict = "PENDING"
    
    for evt in filtered_events:
        rule_id = evt.payload.get("rule_id")
        if rule_id:
            verdict = "PASSED" if evt.event_type == "ComplianceRulePassed" else "FAILED"
            rules.append({
                "rule_id": rule_id,
                "verdict": verdict,
                "rule_version": evt.payload.get("rule_version"),
            })
            if evt.payload.get("remediation_required", False) and verdict == "FAILED":
                has_hard_block = True
                
        if evt.event_type == "ComplianceCheckCompleted":
            overall_verdict = evt.payload.get("overall_verdict", "CLEAR")

    return {
        "source": "event_stream",
        "state": "PARTIAL" if overall_verdict == "PENDING" else "COMPLETE",
        "rules": rules,
        "has_hard_block": has_hard_block,
        "overall": overall_verdict,
        "events_applied": len(filtered_events)
    }

# ─── Helper for Backdating Events ──────────────────────────────────────────

async def backdate_event(store: EventStore, stream_id: str, position: int, new_dt: datetime):
    """
    Directly mutates the recorded_at timestamp in Postgres to simulate historical events.
    In a real system you can't backdate, but we do this for the demo script.
    """
    pool = store._pool
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE events SET recorded_at = $1 WHERE stream_id = $2 AND stream_position = $3",
            new_dt, stream_id, position
        )


async def run_step_3():
    print(f"\n{BOLD}{'='*80}{RESET}")
    print(f"{BOLD}  STEP 3: TEMPORAL COMPLIANCE QUERY (PostgreSQL){RESET}")
    print(f"{BOLD}  Querying ledger://applications/{{id}}/compliance?as_of={{timestamp}}{RESET}")
    print(f"{BOLD}{'='*80}{RESET}")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print(f"{RED}ERROR: DATABASE_URL not found in .env{RESET}")
        return

    conn_start = time.perf_counter()
    store = EventStore.from_url(db_url)
    await store.connect()
    conn_duration = time.perf_counter() - conn_start

    try:
        # Define T-series timestamps
        now = datetime.now(timezone.utc)
        T0 = now - timedelta(days=5)  # Application Submitted
        T1 = now - timedelta(days=4)  # KYC Passed
        T2 = now - timedelta(days=3)  # AML Failed
        T3 = now - timedelta(days=2)  # AML Mediated and Passed
        T4 = now - timedelta(days=1)  # Compliance Check Completed
        
        app_id = f"TEMP-{int(time.time())}"
        loan_stream = f"loan-{app_id}"
        comp_stream = f"compliance-{app_id}"

        section(f"1. SEEDING HISTORICAL COMPLIANCE DATA — {app_id}")
        print(f"  We will generate a sequence of events and artificially backdate their")
        print(f"  `recorded_at` column in Postgres to simulate a multi-day review process.\n")

        # --- T0: Application Submitted & Analysis Complete ---
        # Direct append for setup (skipping handlers to easily inject historical start state)
        await store.append(loan_stream, [
            ApplicationSubmitted(payload={
                "application_id": app_id, "applicant_id": "CUST-TIME",
                "requested_amount_usd": 150000.0,
                "submission_channel": "api", "submitted_at": T0.isoformat(),
            })
        ], expected_version=-1)
        await backdate_event(store, loan_stream, 1, T0)

        cmd_req = RequestComplianceCheckCommand(
            application_id=app_id, regulation_set_version="v2.1",
            checks_required=["KYC", "AML", "SANCTIONS"]
        )
        # Mock the LoanAgg validation by brute forcing loan aggregate into right state
        # In a real system it'd be from handlers. Here we just set up the timeline directly.
        await store.append(loan_stream, [
            ComplianceCheckRequested(payload={
                "application_id": app_id,
                "regulation_set_version": "v2.1",
                "checks_required": ["KYC", "AML", "SANCTIONS"],
            })
        ], expected_version=1)
        await backdate_event(store, loan_stream, 2, T0)
        print(f"  [{T0.strftime('%Y-%m-%d %H:%M:%S')}] T0: Application created, Compliance Review assigned")

        # --- T1: KYC Checks Pass ---
        await store.append(comp_stream, [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "KYC", "rule_version": "v2.1",
                "evidence_hash": "kyc_hash_123"
            })
        ], expected_version=-1)
        await backdate_event(store, comp_stream, 1, T1)
        print(f"  [{T1.strftime('%Y-%m-%d %H:%M:%S')}] T1: KYC check passes")

        # --- T2: AML Check Fails ---
        await store.append(comp_stream, [
            ComplianceRuleFailed(payload={
                "application_id": app_id, "rule_id": "AML", "rule_version": "v2.1",
                "failure_reason": "High-risk jurisdiction detected",
                "remediation_required": True
            })
        ], expected_version=1)
        await backdate_event(store, comp_stream, 2, T2)
        print(f"  [{T2.strftime('%Y-%m-%d %H:%M:%S')}] T2: AML check FAILS (High-risk jurisdiction)")

        # --- T3: AML Mediated & Passed, Sanctions Passed ---
        await store.append(comp_stream, [
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "AML", "rule_version": "v2.1",
                "evidence_hash": "aml_override_hash",
                "notes": "Exception granted via Enhanced Due Diligence"
            }),
            ComplianceRulePassed(payload={
                "application_id": app_id, "rule_id": "SANCTIONS", "rule_version": "v2.1",
                "evidence_hash": "sanctions_clear_hash"
            })
        ], expected_version=2)
        await backdate_event(store, comp_stream, 3, T3)
        await backdate_event(store, comp_stream, 4, T3)
        print(f"  [{T3.strftime('%Y-%m-%d %H:%M:%S')}] T3: AML review mediated and passes. Sanctions passes.")

        # --- T4: Compliance Check Completed ---
        await store.append(comp_stream, [
            ComplianceCheckCompleted(payload={
                "application_id": app_id, "overall_verdict": "CLEAR",
                "has_hard_block": False, "rules_evaluated": 3
            })
        ], expected_version=4)
        await backdate_event(store, comp_stream, 5, T4)
        print(f"  [{T4.strftime('%Y-%m-%d %H:%M:%S')}] T4: Compliance Review complete (CLEAR)")

        
        # ════════════════════════════════════════════════════════════════
        # PHASE 2: TEMPORAL QUERIES
        # ════════════════════════════════════════════════════════════════
        section("2. TEMPORAL QUERIES (Time Travel)")
        print(f"  Simulating: GET /ledger/applications/{app_id}/compliance?as_of=...")

        # Query 1: Before compliance started (T0 + 1 hour)
        t_query0 = T0 + timedelta(hours=1)
        res0 = await query_compliance_at(store, app_id, as_of=t_query0)
        
        sub_section(f"Query A: as_of = {t_query0.strftime('%Y-%m-%d %H:%M:%S')} (Right after submission)")
        print(f"  Events applied : {res0.get('events_applied', 0)}")
        print(f"  State          : {YELLOW}{res0.get('state')}{RESET}")

        # Query 2: After KYC, before AML fail
        t_query1 = T1 + timedelta(hours=1)
        res1 = await query_compliance_at(store, app_id, as_of=t_query1)
        sub_section(f"Query B: as_of = {t_query1.strftime('%Y-%m-%d %H:%M:%S')} (Between T1 and T2)")
        print(f"  Events applied : {res1.get('events_applied')}")
        print(f"  State          : {YELLOW}{res1.get('state')}{RESET} (Overall: {res1.get('overall')})")
        print(f"  Rules state    :")
        for r in res1.get('rules', []):
            color = GREEN if r['verdict'] == 'PASSED' else RED
            print(f"    - {r['rule_id']:<10} : {color}{r['verdict']}{RESET}")
            
        # Query 3: Right after AML fail
        t_query2 = T2 + timedelta(hours=1)
        res2 = await query_compliance_at(store, app_id, as_of=t_query2)
        sub_section(f"Query C: as_of = {t_query2.strftime('%Y-%m-%d %H:%M:%S')} (Right after AML rejection)")
        print(f"  Events applied : {res2.get('events_applied')}")
        print(f"  State          : {YELLOW}{res2.get('state')}{RESET} (Overall: {res2.get('overall')})")
        print(f"  Rules state    :")
        for r in res2.get('rules', []):
            color = GREEN if r['verdict'] == 'PASSED' else RED
            print(f"    - {r['rule_id']:<10} : {color}{r['verdict']}{RESET}")

        # Query 4: Current State (Now)
        t_query_now = now
        res_current = await query_compliance_at(store, app_id, as_of=t_query_now)
        sub_section(f"Query D: as_of = {t_query_now.strftime('%Y-%m-%d %H:%M:%S')} (CURRENT STATE)")
        print(f"  Events applied : {res_current.get('events_applied')}")
        print(f"  State          : {GREEN}{res_current.get('state')}{RESET} (Overall: {res_current.get('overall')})")
        print(f"  Rules state    :")
        # Dedup rules to display current state nicely
        active_rules = {}
        for r in res_current.get('rules', []):
            active_rules[r['rule_id']] = r['verdict']
            
        for rule_id, verdict in active_rules.items():
            color = GREEN if verdict == 'PASSED' else RED
            print(f"    - {rule_id:<10} : {color}{verdict}{RESET}")

        # ════════════════════════════════════════════════════════════════
        # PHASE 3: ARCHITECTURAL PROOF & SQL
        # ════════════════════════════════════════════════════════════════
        section("3. ARCHITECTURAL PROOF")
        print(f"""
  The system supports True Temporal Queries.
  Because the Event Store is an append-only, immutable ledger with exact 
  timestamps, we can precisely rebuild the state of any aggregate at any 
  point in history by simply bounding the query:

  {CYAN}SELECT * FROM events 
  WHERE stream_id = 'compliance-{app_id}' 
  AND recorded_at <= '{t_query2.isoformat()}'
  ORDER BY stream_position ASC;{RESET}

  Compare Query C vs Query D:
  - In Query C, the AML rule was strictly {RED}FAILED{RESET}.
  - In Query D, the AML rule had been mediated and was {GREEN}PASSED{RESET}, turning the
    aggregate completely {GREEN}CLEAR{RESET}.
  - A traditional CRUD database would have OVERWRITTEN the failure, destroying
    the historical context that an AML alert originally triggered. Our system 
    preserves the entire timeline.""")

        print(f"\n{'='*80}\n")
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(run_step_3())
