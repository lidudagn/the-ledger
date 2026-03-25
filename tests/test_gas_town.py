"""
The Ledger — Phase 4: Gas Town Recovery Tests

Covers:
  1. Normal reconstruction (healthy session → HEALTHY)
  2. Crash recovery identification (failed + recoverable → pending work)
  3. Partial decision detection (no completion → NEEDS_RECONCILIATION)
  4. Last 3 events preserved verbatim
  5. Token budget respected
  6. Empty session stream → FAILED
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from in_memory_store import InMemoryEventStore
from models.events import BaseEvent
from integrity.gas_town import (
    reconstruct_agent_context,
    _estimate_tokens,
    AgentContext,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def store():
    return InMemoryEventStore()


EXPECTED_FRAUD_NODES = [
    "validate_inputs",
    "load_facts",
    "cross_reference_registry",
    "analyze_fraud_patterns",
    "write_output",
]


async def _seed_agent_session(
    store,
    agent_id: str = "fraud_detection",
    session_id: str = "sess-001",
    nodes: list[str] | None = None,
    fail_after: str | None = None,
    complete: bool = True,
) -> int:
    """Seed an agent session with AgentNodeExecuted events."""
    stream_id = f"agent-{agent_id}-{session_id}"

    if nodes is None:
        nodes = EXPECTED_FRAUD_NODES

    # Start session
    version = await store.append(
        stream_id,
        [BaseEvent(
            event_type="AgentSessionStarted",
            event_version=1,
            payload={"agent_id": agent_id, "session_id": session_id},
        )],
        expected_version=-1,
    )

    # Execute nodes
    for node in nodes:
        version = await store.append(
            stream_id,
            [BaseEvent(
                event_type="AgentNodeExecuted",
                event_version=1,
                payload={
                    "node_name": node,
                    "status": "success",
                    "agent_id": agent_id,
                },
            )],
            expected_version=version,
        )

        if fail_after and node == fail_after:
            # Crash: emit AgentSessionFailed
            version = await store.append(
                stream_id,
                [BaseEvent(
                    event_type="AgentSessionFailed",
                    event_version=1,
                    payload={
                        "agent_id": agent_id,
                        "failure_reason": "OutOfMemoryError",
                        "recoverable": True,
                        "last_successful_node": node,
                    },
                )],
                expected_version=version,
            )
            return version

    if complete:
        # Write output
        version = await store.append(
            stream_id,
            [BaseEvent(
                event_type="AgentOutputWritten",
                event_version=1,
                payload={"agent_id": agent_id, "output_type": "fraud_report"},
            )],
            expected_version=version,
        )
        # Complete session
        version = await store.append(
            stream_id,
            [BaseEvent(
                event_type="AgentSessionCompleted",
                event_version=1,
                payload={"agent_id": agent_id},
            )],
            expected_version=version,
        )

    return version


# =============================================================================
# 1. Normal Reconstruction
# =============================================================================


class TestNormalReconstruction:

    @pytest.mark.asyncio
    async def test_healthy_session(self, store):
        """Completed session → HEALTHY, all nodes in executed_nodes_order."""
        await _seed_agent_session(store, complete=True)

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-001",
            expected_nodes=EXPECTED_FRAUD_NODES,
        )

        assert ctx.session_health_status == "HEALTHY"
        assert ctx.last_successful_node == "write_output"
        assert ctx.executed_nodes_order == EXPECTED_FRAUD_NODES
        assert ctx.pending_work == []
        assert ctx.recovered_from_session_id == "sess-001"


# =============================================================================
# 2. Crash Recovery
# =============================================================================


class TestCrashRecovery:

    @pytest.mark.asyncio
    async def test_crash_after_load_facts(self, store):
        """Failed + recoverable → NEEDS_RECONCILIATION with pending work."""
        await _seed_agent_session(
            store, fail_after="load_facts",
        )

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-001",
            expected_nodes=EXPECTED_FRAUD_NODES,
        )

        assert ctx.session_health_status == "NEEDS_RECONCILIATION"
        assert ctx.last_successful_node == "load_facts"
        assert ctx.pending_work == [
            "cross_reference_registry",
            "analyze_fraud_patterns",
            "write_output",
        ]
        assert ctx.executed_nodes_order == ["validate_inputs", "load_facts"]


# =============================================================================
# 3. Partial Decision (No Completion)
# =============================================================================


class TestPartialDecision:

    @pytest.mark.asyncio
    async def test_no_completion_event(self, store):
        """Node executed but no completion → NEEDS_RECONCILIATION."""
        await _seed_agent_session(
            store, complete=False,
        )

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-001",
            expected_nodes=EXPECTED_FRAUD_NODES,
        )

        assert ctx.session_health_status == "NEEDS_RECONCILIATION"
        assert ctx.last_successful_node == "write_output"


# =============================================================================
# 4. Verbatim Last 3 Events
# =============================================================================


class TestVerbatimEvents:

    @pytest.mark.asyncio
    async def test_last_3_events_preserved(self, store):
        """Last 3 events should be preserved verbatim in context."""
        await _seed_agent_session(store, complete=True)

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-001",
        )

        assert len(ctx.verbatim_events) == 3
        # Last 3 events should be the last node, output, and completion
        types = [e["event_type"] for e in ctx.verbatim_events]
        assert "AgentSessionCompleted" in types

    @pytest.mark.asyncio
    async def test_fewer_than_3_events(self, store):
        """Session with only 2 events → both preserved."""
        stream_id = "agent-fraud_detection-sess-tiny"
        version = await store.append(
            stream_id,
            [BaseEvent(
                event_type="AgentSessionStarted",
                event_version=1,
                payload={"agent_id": "fraud_detection"},
            )],
            expected_version=-1,
        )
        await store.append(
            stream_id,
            [BaseEvent(
                event_type="AgentSessionFailed",
                event_version=1,
                payload={
                    "failure_reason": "crash",
                    "recoverable": False,
                },
            )],
            expected_version=version,
        )

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-tiny",
        )

        assert len(ctx.verbatim_events) == 2


# =============================================================================
# 5. Token Budget
# =============================================================================


class TestTokenBudget:

    @pytest.mark.asyncio
    async def test_context_within_budget(self, store):
        """Context text should fit within token budget."""
        await _seed_agent_session(store, complete=True)

        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-001",
            token_budget=8000,
        )

        estimated = _estimate_tokens(ctx.context_text)
        assert estimated <= 8000


# =============================================================================
# 6. Empty Session
# =============================================================================


class TestEmptySession:

    @pytest.mark.asyncio
    async def test_no_events_returns_failed(self, store):
        """Empty session stream → FAILED status."""
        ctx = await reconstruct_agent_context(
            store, "fraud_detection", "sess-nonexistent",
            expected_nodes=EXPECTED_FRAUD_NODES,
        )

        assert ctx.session_health_status == "FAILED"
        assert ctx.last_successful_node is None
        assert ctx.pending_work == EXPECTED_FRAUD_NODES
        assert ctx.executed_nodes_order == []
        assert ctx.verbatim_events == []
