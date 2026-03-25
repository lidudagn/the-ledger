import pytest
import json
import os

from collections import defaultdict
from event_store import EventStore
from in_memory_store import InMemoryEventStore
from registry.client import ApplicantRegistryClient

from agents.credit_analysis import CreditAnalysisAgent
from agents.compliance import ComplianceAgent
from agents.fraud_detection import FraudDetectionAgent
from agents.decision_orchestrator import DecisionOrchestratorAgent

import models.events

@pytest.fixture
async def seeded_store() -> EventStore:
    store = InMemoryEventStore()
    
    # Load seed events
    seed_path = os.path.join(os.path.dirname(__file__), "..", "seed_events.jsonl")
    events_by_stream = defaultdict(list)
    
    with open(seed_path, "r") as f:
        for line in f:
            data = json.loads(line)
            # Only load domain events, ignore agent traces to test agents fresh
            if data["stream_id"].startswith("agent-"):
                continue
                
            stream_id = data.pop("stream_id")
            stream_pos = data.pop("stream_position", None)
            data.pop("global_position", None)
            event_type = data.pop("event_type")
            
            # Reconstruct dict
            try:
                EventClass = getattr(models.events, event_type)
                # Skip premature requests that break domain aggregate logic since we are simulating the agents from scratch
                if event_type in ("FraudScreeningRequested", "ComplianceCheckRequested", "DecisionRequested"):
                    continue
                    
                event_obj = EventClass(event_type=event_type, **data)
                events_by_stream[stream_id].append(event_obj)
            except AttributeError:
                pass # skip events we don't have python models for if any
                
    for stream_id, ev_list in events_by_stream.items():
        # Sort by timestamp roughly to ensure appending order if needed, but they are in order in file
        await store.append(stream_id, ev_list, expected_version=-1)
        
    return store


import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="DATABASE_URL not set — skipping real DB tests",
)

@pytest.fixture
async def real_registry() -> ApplicantRegistryClient:
    pool = await asyncpg.create_pool(
        DATABASE_URL, 
        min_size=1, max_size=3
    )
    client = ApplicantRegistryClient(pool)
    yield client
    await pool.close()


@pytest.mark.asyncio
async def test_incremental_apex_0019(seeded_store, real_registry):
    """
    APEX-0019 has all 3 async requests pending: Credit, Fraud, Compliance.
    We process them using the real agents and real registry.
    """
    app_id = "APEX-0019"
    
    # 1. Run Parallel Agents (Credit, Fraud)
    # Credit is already requested by seed
    credit_agent = CreditAnalysisAgent(seeded_store, real_registry)
    await credit_agent.process_application(app_id)
    
    # In real life, Daemon would emit FraudScreeningRequested here.
    from models.events import FraudScreeningRequested, ComplianceCheckRequested, DecisionRequested
    v = await seeded_store.stream_version(f"loan-{app_id}")
    await seeded_store.append(f"loan-{app_id}", [FraudScreeningRequested(payload={"application_id": app_id})], expected_version=v)
    
    fraud_agent = FraudDetectionAgent(seeded_store, real_registry)
    await fraud_agent.process_application(app_id)
    
    # Daemon emits ComplianceCheckRequested once Analysis is complete
    v2 = await seeded_store.stream_version(f"loan-{app_id}")
    await seeded_store.append(f"loan-{app_id}", [ComplianceCheckRequested(payload={"application_id": app_id, "rules_to_evaluate": []})], expected_version=v2)
    
    comp_agent = ComplianceAgent(seeded_store, real_registry)
    await comp_agent.process_application(app_id)
    
    # Daemon emits DecisionRequested once Compliance is complete
    v3 = await seeded_store.stream_version(f"loan-{app_id}")
    await seeded_store.append(f"loan-{app_id}", [DecisionRequested(payload={"application_id": app_id})], expected_version=v3)
    
    # 2. Run Orchestrator
    orchestrator = DecisionOrchestratorAgent(seeded_store)
    state = await orchestrator.process_application(app_id)
    
    # Assert Orchestrator finished
    assert state["final_decision"] is not None
    assert state["final_decision"]["recommendation"] in ("APPROVE", "DECLINE", "REFER")
    
    # 3. Assert Output Stream
    loan_events = await seeded_store.load_stream(f"loan-{app_id}")
    
    event_types = [e["event_type"] for e in loan_events]
    assert "CreditAnalysisCompleted" in event_types
    assert "FraudScreeningCompleted" in event_types
    assert "DecisionGenerated" in event_types
    
    # Ensure Compliance was tracked
    comp_events = await seeded_store.load_stream(f"compliance-{app_id}")
    assert len(comp_events) > 0
    assert comp_events[-1]["event_type"] == "ComplianceCheckCompleted"
