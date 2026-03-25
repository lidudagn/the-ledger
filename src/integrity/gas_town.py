"""
The Ledger — Gas Town Agent Recovery (Phase 4)

Reconstruct agent context after a crash by replaying the agent's event stream.
Named after the "Gas Town" pattern in event sourcing: when an agent runs out of
"gas" (crashes mid-execution), we rebuild its state from the event log.

Design principles:
  - Context is reconstructed from actual AgentNodeExecuted events
    (data-driven, not graph-dependent — safe across graph version changes)
  - executed_nodes_order tracks what actually ran, not what was planned
  - Token budget controls context size for LLM consumption
  - Last 3 events + ERROR events preserved verbatim
  - Idempotent recovery: AgentSessionRecovered event prevents duplicate work
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AgentContext:
    """Reconstructed agent context after crash recovery.

    Fields:
      context_text: prose summary within token budget for LLM consumption
      last_event_position: position of last event in session stream
      last_successful_node: name of last completed node
      pending_work: remaining node names to execute
      session_health_status: HEALTHY | NEEDS_RECONCILIATION | FAILED
      recovered_from_session_id: the crashed session ID
      verbatim_events: last 3 events preserved exactly
      executed_nodes_order: ordered list of nodes actually executed
        (from AgentNodeExecuted events, NOT from graph reconstruction)
    """
    context_text: str
    last_event_position: int
    last_successful_node: str | None
    pending_work: list[str]
    session_health_status: str  # HEALTHY | NEEDS_RECONCILIATION | FAILED
    recovered_from_session_id: str | None
    verbatim_events: list[dict] = field(default_factory=list)
    executed_nodes_order: list[str] = field(default_factory=list)


def _estimate_tokens(text: str) -> int:
    """Estimate token count.

    Strategy:
    1. Try tiktoken with cl100k_base (GPT-4 tokenizer) if available
    2. Fallback: len(text) // 4 (OpenAI's documented approximation for English)

    This is a conservative UPPER BOUND estimate — we'd rather under-fill
    than overflow the LLM context window.
    """
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except ImportError:
        return len(text) // 4  # ~4 chars per token for English


def _summarize_event(event_dict: dict) -> str:
    """Create a one-line summary of an event for prose context."""
    event_type = event_dict.get("event_type", "Unknown")
    payload = event_dict.get("payload", {})

    if event_type == "AgentNodeExecuted":
        node = payload.get("node_name", "unknown")
        status = payload.get("status", "")
        return f"  - Executed node '{node}' (status: {status})"
    elif event_type == "AgentSessionStarted":
        agent = payload.get("agent_id", "unknown")
        return f"  - Session started for agent '{agent}'"
    elif event_type == "AgentSessionFailed":
        reason = payload.get("failure_reason", "unknown")
        recoverable = payload.get("recoverable", False)
        return f"  - Session FAILED: {reason} (recoverable={recoverable})"
    elif event_type == "AgentSessionCompleted":
        return "  - Session completed successfully"
    elif event_type == "AgentOutputWritten":
        output_type = payload.get("output_type", "unknown")
        return f"  - Output written: {output_type}"
    else:
        return f"  - {event_type}"


async def reconstruct_agent_context(
    store: Any,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
    expected_nodes: list[str] | None = None,
) -> AgentContext:
    """Reconstruct agent context from event stream after a crash.

    Recovery flow:
    1. Load full agent-{agent_id}-{session_id} stream
    2. Scan for AgentNodeExecuted events → build executed_nodes_order
    3. Identify last_successful_node from the last AgentNodeExecuted
    4. Check for AgentSessionFailed → flag pending work
    5. Check for partial decisions (no completion) → NEEDS_RECONCILIATION
    6. Summarize old events into prose (within token_budget)
    7. Preserve verbatim: last 3 events + any ERROR events

    Idempotent recovery guarantee:
      Caller MUST emit AgentSessionRecovered as first event of new session.
      AgentSessionRecovered contains recovered_from_session_id + recovery_point.
      New session's validate_inputs checks: skip nodes <= last_successful_node.
      This prevents duplicate execution of already-completed nodes.

    Args:
      store: EventStore or InMemoryEventStore instance
      agent_id: The agent identifier (e.g., "fraud_detection")
      session_id: The session identifier
      token_budget: Maximum tokens for context_text (default 8000)
      expected_nodes: Optional ordered list of expected node names.
        Used to determine pending_work. If not provided, pending_work
        is left empty (caller must determine remaining work).
    """
    stream_id = f"agent-{agent_id}-{session_id}"

    # Load all events from the session stream
    events = await store.load_stream(stream_id)

    if not events:
        return AgentContext(
            context_text="No events found for this session.",
            last_event_position=0,
            last_successful_node=None,
            pending_work=expected_nodes or [],
            session_health_status="FAILED",
            recovered_from_session_id=session_id,
            verbatim_events=[],
            executed_nodes_order=[],
        )

    # Convert StoredEvents to dicts for processing
    event_dicts = []
    for e in events:
        event_dicts.append({
            "event_id": str(e.event_id),
            "stream_id": e.stream_id,
            "stream_position": e.stream_position,
            "global_position": e.global_position,
            "event_type": e.event_type,
            "event_version": e.event_version,
            "payload": e.payload if isinstance(e.payload, dict) else dict(e.payload),
            "metadata": e.metadata if isinstance(e.metadata, dict) else dict(e.metadata or {}),
            "recorded_at": str(e.recorded_at),
        })

    # Build executed_nodes_order from actual AgentNodeExecuted events
    executed_nodes_order = []
    last_successful_node = None
    session_failed = False
    session_completed = False
    failure_recoverable = False

    for ed in event_dicts:
        et = ed["event_type"]
        payload = ed.get("payload", {})

        if et == "AgentNodeExecuted":
            node_name = payload.get("node_name", "unknown")
            status = payload.get("status", "success")
            executed_nodes_order.append(node_name)
            if status in ("success", "completed"):
                last_successful_node = node_name
        elif et == "AgentSessionFailed":
            session_failed = True
            failure_recoverable = payload.get("recoverable", False)
            # Use last_successful_node from the failure event if provided
            if payload.get("last_successful_node"):
                last_successful_node = payload["last_successful_node"]
        elif et == "AgentSessionCompleted":
            session_completed = True

    # Determine session health status
    if session_completed:
        health = "HEALTHY"
    elif session_failed and failure_recoverable:
        health = "NEEDS_RECONCILIATION"
    elif session_failed:
        health = "FAILED"
    elif not session_completed and executed_nodes_order:
        # Partial execution: has node events but no completion
        health = "NEEDS_RECONCILIATION"
    else:
        health = "FAILED"

    # Determine pending work from expected_nodes
    pending_work: list[str] = []
    if expected_nodes and last_successful_node:
        try:
            idx = expected_nodes.index(last_successful_node)
            pending_work = expected_nodes[idx + 1:]
        except ValueError:
            # last_successful_node not in expected list — all nodes pending
            pending_work = expected_nodes[:]
    elif expected_nodes and not last_successful_node:
        pending_work = expected_nodes[:]

    # Build context_text within token budget
    # Preserve last 3 events verbatim
    verbatim_events = event_dicts[-3:] if len(event_dicts) >= 3 else event_dicts[:]

    # Also preserve ERROR events verbatim
    error_events = [
        ed for ed in event_dicts[:-3]
        if ed.get("payload", {}).get("status") == "error"
        or ed["event_type"] == "AgentSessionFailed"
    ]

    # Summarize earlier events into prose
    earlier_events = event_dicts[:-3] if len(event_dicts) > 3 else []
    summary_lines = ["Agent session recovery context:"]
    summary_lines.append(f"  Agent: {agent_id}")
    summary_lines.append(f"  Session: {session_id}")
    summary_lines.append(f"  Status: {health}")
    summary_lines.append(f"  Nodes executed: {', '.join(executed_nodes_order)}")
    if last_successful_node:
        summary_lines.append(f"  Last successful: {last_successful_node}")
    if pending_work:
        summary_lines.append(f"  Pending: {', '.join(pending_work)}")

    summary_lines.append("")
    summary_lines.append("Event history (summarized):")

    for ed in earlier_events:
        if ed not in error_events:
            summary_lines.append(_summarize_event(ed))

    if error_events:
        summary_lines.append("")
        summary_lines.append("Error events (verbatim):")
        for ed in error_events:
            summary_lines.append(f"  {ed['event_type']}: {ed.get('payload', {})}")

    summary_lines.append("")
    summary_lines.append("Recent events (verbatim):")
    for ed in verbatim_events:
        summary_lines.append(f"  {ed['event_type']}: {ed.get('payload', {})}")

    context_text = "\n".join(summary_lines)

    # Trim to token budget if needed
    while _estimate_tokens(context_text) > token_budget and len(summary_lines) > 10:
        # Remove summarized events from the middle first
        for i, line in enumerate(summary_lines):
            if line.startswith("  - ") and "Executed node" in line:
                summary_lines.pop(i)
                context_text = "\n".join(summary_lines)
                break
        else:
            break  # Can't trim further without losing critical info

    last_event_position = events[-1].stream_position

    return AgentContext(
        context_text=context_text,
        last_event_position=last_event_position,
        last_successful_node=last_successful_node,
        pending_work=pending_work,
        session_health_status=health,
        recovered_from_session_id=session_id,
        verbatim_events=verbatim_events,
        executed_nodes_order=executed_nodes_order,
    )
