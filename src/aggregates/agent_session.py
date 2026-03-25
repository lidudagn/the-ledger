"""
The Ledger — AgentSessionAggregate

Reconstructs agent session state from agent-{agent_id}-{session_id} stream.
Enforces the Gas Town persistent ledger pattern: no decision events
allowed until AgentContextLoaded is present.

Rules (pure — no I/O):
  Gas Town: AgentContextLoaded must precede any decision event
  Model Version: Once set, cannot change within a session
  Completion: No appends after session completed/failed
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from models.events import DomainError, StoredEvent

if TYPE_CHECKING:
    from event_store import EventStore
    from in_memory_store import InMemoryEventStore


class SessionState(str, Enum):
    """Agent session lifecycle states."""

    STARTED = "STARTED"
    CONTEXT_LOADED = "CONTEXT_LOADED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"


class AgentSessionAggregate:
    """
    Reconstructs agent session state from events.
    Pure domain logic — no I/O except in the load() classmethod.
    """

    def __init__(self, agent_id: str, session_id: str) -> None:
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = 0
        self.state: SessionState | None = None

        # Gas Town tracking
        self.context_loaded: bool = False
        self.context_event_position: int | None = None
        self.context_source: str | None = None

        # Model version
        self.model_version: str | None = None

        # Execution tracking
        self.nodes_executed: int = 0
        self.last_node_name: str | None = None
        self.total_cost_usd: float = 0.0

        # Failure tracking
        self.recoverable: bool = False
        self.last_successful_node: str | None = None
        self.recovered_from_session_id: str | None = None

    @classmethod
    async def load(
        cls,
        store: "EventStore | InMemoryEventStore",
        agent_id: str,
        session_id: str,
    ) -> "AgentSessionAggregate":
        agg = cls(agent_id=agent_id, session_id=session_id)
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        if not events:
            return agg

        for event in events:
            agg._apply(event)
        return agg

    # -------------------------------------------------------------------------
    # Event replay
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
        self.state = SessionState.STARTED

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self.state = SessionState.CONTEXT_LOADED
        self.context_loaded = True
        self.context_event_position = event.stream_position
        self.context_source = event.payload.get("context_source")
        self.model_version = event.payload.get("model_version")

    def _on_AgentInputValidated(self, event: StoredEvent) -> None:
        if self.state == SessionState.CONTEXT_LOADED:
            self.state = SessionState.EXECUTING

    def _on_AgentNodeExecuted(self, event: StoredEvent) -> None:
        self.nodes_executed += 1
        self.last_node_name = event.payload.get("node_name")
        # Accumulate cost
        cost = event.payload.get("llm_cost_usd", 0.0)
        if cost:
            self.total_cost_usd += cost

    def _on_AgentToolCalled(self, event: StoredEvent) -> None:
        pass  # tracked for audit, no state change

    def _on_AgentOutputWritten(self, event: StoredEvent) -> None:
        pass  # tracked for audit

    def _on_AgentSessionCompleted(self, event: StoredEvent) -> None:
        self.state = SessionState.COMPLETED
        self.total_cost_usd = event.payload.get("total_cost_usd", self.total_cost_usd)

    def _on_AgentSessionFailed(self, event: StoredEvent) -> None:
        self.state = SessionState.FAILED
        self.recoverable = event.payload.get("recoverable", False)
        self.last_successful_node = event.payload.get("last_successful_node")

    def _on_AgentSessionRecovered(self, event: StoredEvent) -> None:
        self.state = SessionState.RECOVERED
        self.recovered_from_session_id = event.payload.get(
            "recovered_from_session_id"
        )

    # -------------------------------------------------------------------------
    # Business rule assertions (pure — no I/O)
    # -------------------------------------------------------------------------

    def assert_context_loaded(self) -> None:
        """Gas Town: AgentContextLoaded must exist before any decision."""
        if not self.context_loaded:
            raise DomainError(
                f"Gas Town violation: session agent-{self.agent_id}-"
                f"{self.session_id} has no AgentContextLoaded event. "
                "No decisions allowed without declared context."
            )

    def assert_model_version_current(self, expected_version: str) -> None:
        """Model version must match what was declared in AgentContextLoaded."""
        if self.model_version is None:
            raise DomainError(
                "Cannot verify model version: no context loaded yet"
            )
        if self.model_version != expected_version:
            raise DomainError(
                f"Model version mismatch: session declared '{self.model_version}', "
                f"command specifies '{expected_version}'"
            )

    def assert_not_completed(self) -> None:
        """No events after session completed or failed."""
        if self.state in (SessionState.COMPLETED, SessionState.FAILED):
            raise DomainError(
                f"Session agent-{self.agent_id}-{self.session_id} is "
                f"{self.state}. No further events allowed."
            )

    def get_context_event_position(self) -> int:
        """Return the stream position of AgentContextLoaded for referencing."""
        if self.context_event_position is None:
            raise DomainError("No context event position — context not loaded")
        return self.context_event_position
