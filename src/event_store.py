"""
The Ledger — Async EventStore (PostgreSQL + asyncpg)

The core event store with optimistic concurrency control.
All writes go through append() which enforces OCC atomically.

System Contracts:
    C1: Requires PostgreSQL READ COMMITTED isolation (default).
        SELECT ... FOR UPDATE guarantees single-writer per stream.
    C2: aggregate_type derived from stream_id.split("-")[0].
        Coupling risk accepted for Phase 1 simplicity.
    C3: global_position provides total ordering, NOT contiguous sequencing.
        Gaps on rollback. Use > not BETWEEN.
    C4: metadata always contains correlation_id and causation_id.
        EventStore owns metadata construction, not the caller.
    C5: Outbox provides at-least-once delivery.
        event_id is dedup key. Consumers MUST be idempotent.
    C6: Events in single append() get contiguous stream_positions in list order.
    C7: Append idempotency NOT guaranteed. Caller's responsibility.
    C8: events table is append-only by convention.
    C9: Low per-stream contention assumed. High contention serializes by design.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from models.events import (
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamArchivedError,
    StreamMetadata,
    StreamNotFoundError,
)


async def create_pool(database_url: str, **kwargs: Any) -> asyncpg.Pool:
    """Create an asyncpg connection pool."""
    return await asyncpg.create_pool(database_url, **kwargs)


class EventStore:
    """
    Async event store backed by PostgreSQL.

    All methods use asyncpg for non-blocking database access.
    Optimistic concurrency control is enforced in append() via
    SELECT ... FOR UPDATE within a transaction.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -------------------------------------------------------------------------
    # append() — The critical method
    # -------------------------------------------------------------------------

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        """
        Atomically appends events to stream_id.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction.

        Args:
            stream_id: Target stream (e.g. "loan-123")
            events: List of events to append
            expected_version: -1 for new stream, N for exact version match
            correlation_id: Groups related events (auto-generated if None — C4)
            causation_id: The event that caused these events (optional)

        Returns:
            New stream version after append.

        Raises:
            OptimisticConcurrencyError: Version mismatch
            StreamArchivedError: Stream is archived (one-way)
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        # C4: correlation_id auto-generated if not provided
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: Lock the stream row (C1: READ COMMITTED + FOR UPDATE)
                row = await conn.fetchrow(
                    """
                    SELECT current_version, archived_at
                    FROM event_streams
                    WHERE stream_id = $1
                    FOR UPDATE
                    """,
                    stream_id,
                )

                if row is None:
                    # Stream does not exist
                    if expected_version != -1:
                        # Caller expects a stream that doesn't exist
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=0,
                        )

                    # Create new stream (C2: aggregate_type from stream_id)
                    aggregate_type = stream_id.split("-")[0]
                    await conn.execute(
                        """
                        INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                        VALUES ($1, $2, 0)
                        """,
                        stream_id,
                        aggregate_type,
                    )
                    current_version = 0
                else:
                    # Stream exists
                    current_version = row["current_version"]
                    archived_at = row["archived_at"]

                    # Check archive status
                    if archived_at is not None:
                        raise StreamArchivedError(stream_id=stream_id)

                    # OCC check
                    if current_version != expected_version:
                        raise OptimisticConcurrencyError(
                            stream_id=stream_id,
                            expected_version=expected_version,
                            actual_version=current_version,
                        )

                # Step 2: Insert events (C6: contiguous positions in list order)
                new_version = current_version
                event_ids = []

                for event in events:
                    new_version += 1
                    event_id = uuid.uuid4()
                    event_ids.append(event_id)

                    # Build metadata (C4: EventStore owns metadata construction)
                    metadata = {
                        "correlation_id": correlation_id,
                        "causation_id": causation_id,
                        **(event.metadata if event.metadata else {}),
                    }

                    await conn.execute(
                        """
                        INSERT INTO events (
                            event_id, stream_id, stream_position,
                            event_type, event_version, payload, metadata
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        event_id,
                        stream_id,
                        new_version,
                        event.event_type,
                        event.event_version,
                        event.payload,
                        metadata,
                    )

                # Step 3: Insert into outbox (C5: same transaction)
                for i, event in enumerate(events):
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3)
                        """,
                        event_ids[i],
                        "default",
                        event.payload,
                    )

                # Step 4: Update stream version
                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $1
                    WHERE stream_id = $2
                    """,
                    new_version,
                    stream_id,
                )

                return new_version

    # -------------------------------------------------------------------------
    # load_stream()
    # -------------------------------------------------------------------------

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Load events from a stream in stream_position order.

        Raises StreamNotFoundError if the stream does not exist.
        Returns empty list for a stream with no events in the given range
        (but the stream itself must exist).
        """
        async with self._pool.acquire() as conn:
            # Verify stream exists
            exists = await conn.fetchval(
                "SELECT 1 FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            if exists is None:
                raise StreamNotFoundError(stream_id=stream_id)

            # Build query with optional range filters
            query = """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE stream_id = $1 AND stream_position > $2
            """
            params: list[Any] = [stream_id, from_position]

            if to_position is not None:
                query += " AND stream_position <= $3"
                params.append(to_position)

            query += " ORDER BY stream_position ASC"

            rows = await conn.fetch(query, *params)

            return [
                StoredEvent(
                    event_id=row["event_id"],
                    stream_id=row["stream_id"],
                    stream_position=row["stream_position"],
                    global_position=row["global_position"],
                    event_type=row["event_type"],
                    event_version=row["event_version"],
                    payload=dict(row["payload"]),
                    metadata=dict(row["metadata"]),
                    recorded_at=row["recorded_at"],
                )
                for row in rows
            ]

    # -------------------------------------------------------------------------
    # load_all() — async generator with pull-based backpressure
    # -------------------------------------------------------------------------

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """
        Load all events across all streams, ordered by global_position.

        Async iteration provides pull-based backpressure — the database
        is only queried when the consumer advances iteration. batch_size
        caps memory per fetch.
        """
        current_position = from_global_position

        async with self._pool.acquire() as conn:
            while True:
                if event_types:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1 AND event_type = ANY($2)
                        ORDER BY global_position ASC
                        LIMIT $3
                        """,
                        current_position,
                        event_types,
                        batch_size,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                        ORDER BY global_position ASC
                        LIMIT $2
                        """,
                        current_position,
                        batch_size,
                    )

                if not rows:
                    break

                for row in rows:
                    yield StoredEvent(
                        event_id=row["event_id"],
                        stream_id=row["stream_id"],
                        stream_position=row["stream_position"],
                        global_position=row["global_position"],
                        event_type=row["event_type"],
                        event_version=row["event_version"],
                        payload=dict(row["payload"]),
                        metadata=dict(row["metadata"]),
                        recorded_at=row["recorded_at"],
                    )
                    current_position = row["global_position"]

                # If we got fewer rows than batch_size, we've consumed everything
                if len(rows) < batch_size:
                    break

    # -------------------------------------------------------------------------
    # stream_version()
    # -------------------------------------------------------------------------

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns the current version of a stream.
        Returns 0 if the stream does not exist (not an error).
        """
        async with self._pool.acquire() as conn:
            version = await conn.fetchval(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return version if version is not None else 0

    # -------------------------------------------------------------------------
    # archive_stream() — one-way operation
    # -------------------------------------------------------------------------

    async def archive_stream(self, stream_id: str) -> None:
        """
        Archives a stream. One-way operation.
        Archived streams: can be read, cannot be appended to, cannot be unarchived.
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = $1 AND archived_at IS NULL
                """,
                stream_id,
            )
            if result == "UPDATE 0":
                # Either stream doesn't exist or already archived
                exists = await conn.fetchval(
                    "SELECT 1 FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                if exists is None:
                    raise StreamNotFoundError(stream_id=stream_id)
                # Already archived — idempotent, no error

    # -------------------------------------------------------------------------
    # get_stream_metadata()
    # -------------------------------------------------------------------------

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """
        Returns metadata for a stream.
        Raises StreamNotFoundError if not found.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version,
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
            if row is None:
                raise StreamNotFoundError(stream_id=stream_id)

            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=dict(row["metadata"]),
            )
