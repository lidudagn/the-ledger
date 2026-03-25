"""
The Ledger — In-Memory Event Store (Test-Only)

Same interface as EventStore but backed by Python dicts + asyncio.Lock.

WARNING — Known semantic gap vs PostgreSQL:
    - asyncio.Lock serialises ALL operations globally — no row-level locking
    - No transaction isolation levels (no READ COMMITTED vs SERIALIZABLE)
    - Concurrent readers are blocked during writes (Postgres allows concurrent reads)

    Consequence: Tests that pass in-memory may behave differently under real DB
    contention. The test_event_store.py suite (real DB) is the authoritative
    correctness gate. InMemoryEventStore tests validate LOGIC, not CONCURRENCY
    SEMANTICS.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from models.events import (
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamArchivedError,
    StreamMetadata,
    StreamNotFoundError,
)


class InMemoryEventStore:
    """
    In-memory event store for unit testing.
    Thread-safe via asyncio.Lock (global, not per-stream).
    """

    def __init__(self) -> None:
        self._streams: dict[str, list[StoredEvent]] = {}
        self._stream_meta: dict[str, StreamMetadata] = {}
        self._all_events: list[StoredEvent] = []
        self._global_counter: int = 0
        self._lock = asyncio.Lock()

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        """Atomically append events with OCC. Same contract as EventStore."""
        if not events:
            raise ValueError("Cannot append empty event list")

        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        async with self._lock:
            meta = self._stream_meta.get(stream_id)

            if meta is None:
                # Stream does not exist
                if expected_version != -1:
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id,
                        expected_version=expected_version,
                        actual_version=0,
                    )

                # Create new stream
                aggregate_type = stream_id.split("-")[0]
                meta = StreamMetadata(
                    stream_id=stream_id,
                    aggregate_type=aggregate_type,
                    current_version=0,
                    created_at=datetime.now(timezone.utc),
                )
                self._stream_meta[stream_id] = meta
                self._streams[stream_id] = []
            else:
                # Stream exists — check archive and OCC
                if meta.archived_at is not None:
                    raise StreamArchivedError(stream_id=stream_id)

                if meta.current_version != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id,
                        expected_version=expected_version,
                        actual_version=meta.current_version,
                    )

            # Append events with contiguous positions
            current_version = meta.current_version
            now = datetime.now(timezone.utc)

            for event in events:
                current_version += 1
                self._global_counter += 1

                metadata = {
                    "correlation_id": correlation_id,
                    "causation_id": causation_id,
                    **(event.metadata if event.metadata else {}),
                }

                stored = StoredEvent(
                    event_id=uuid.uuid4(),
                    stream_id=stream_id,
                    stream_position=current_version,
                    global_position=self._global_counter,
                    event_type=event.event_type,
                    event_version=event.event_version,
                    payload=deepcopy(event.payload),
                    metadata=metadata,
                    recorded_at=now,
                )

                self._streams[stream_id].append(stored)
                self._all_events.append(stored)

            # Update version
            self._stream_meta[stream_id] = meta.model_copy(
                update={"current_version": current_version}
            )

            return current_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """Load events from a stream. Returns empty list if missing."""
        if stream_id not in self._streams:
            return []

        events = self._streams[stream_id]
        filtered = [e for e in events if e.stream_position > from_position]

        if to_position is not None:
            filtered = [e for e in filtered if e.stream_position <= to_position]

        return [deepcopy(e) for e in filtered]

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """Async iterator over all events by global_position."""
        events = [
            e for e in self._all_events if e.global_position > from_global_position
        ]

        if event_types:
            events = [e for e in events if e.event_type in event_types]

        for event in events:
            yield deepcopy(event)

    async def stream_version(self, stream_id: str) -> int:
        """Returns 0 for non-existent streams (not an error)."""
        meta = self._stream_meta.get(stream_id)
        return meta.current_version if meta else 0

    async def archive_stream(self, stream_id: str) -> None:
        """One-way archive. Readable after, not appendable, not unarchivable."""
        if stream_id not in self._stream_meta:
            raise StreamNotFoundError(stream_id=stream_id)

        meta = self._stream_meta[stream_id]
        if meta.archived_at is None:
            self._stream_meta[stream_id] = meta.model_copy(
                update={"archived_at": datetime.now(timezone.utc)}
            )

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """Raises StreamNotFoundError if not found."""
        meta = self._stream_meta.get(stream_id)
        if meta is None:
            raise StreamNotFoundError(stream_id=stream_id)
        return meta.model_copy()
