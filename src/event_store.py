"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
Append-only event store with OCC (optimistic concurrency control).
All agents and projections use this class.

Methods:
  1. stream_version()   — returns 0 if stream doesn't exist
  2. append()           — OCC-protected atomic append, returns new version
  3. load_stream()      — returns list[StoredEvent] in stream_position order
  4. load_all()         — async generator yielding StoredEvent by global_position
  5. get_event()        — single StoredEvent by UUID (for causation chain)
  6. save_checkpoint()  — projection checkpoint persistence
  7. load_checkpoint()  — projection checkpoint retrieval
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, AsyncGenerator
from uuid import UUID

import asyncpg

from models.events import (
    OptimisticConcurrencyError,
    StoredEvent,
    StreamArchivedError,
    StreamMetadata,
)

# ─── POOL FACTORY ─────────────────────────────────────────────────────────────

async def create_pool(db_url: str, **kwargs) -> asyncpg.Pool:
    """Create an asyncpg connection pool."""
    return await asyncpg.create_pool(
        db_url,
        min_size=kwargs.get("min_size", 2),
        max_size=kwargs.get("max_size", 10),
    )


from upcasting.registry import UpcasterRegistry


# ─── EVENT STORE ──────────────────────────────────────────────────────────────

class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    Usage (pool-based):
        pool = await create_pool(DATABASE_URL)
        store = EventStore(pool)
        ...
        await pool.close()

    Usage (url-based):
        store = EventStore.from_url(DATABASE_URL)
        await store.connect()
        ...
        await store.close()
    """

    def __init__(self, pool: asyncpg.Pool, upcaster_registry: UpcasterRegistry | None = None):
        self._pool = pool
        self.upcasters = upcaster_registry

    @classmethod
    def from_url(cls, db_url: str, upcaster_registry: UpcasterRegistry | None = None) -> "EventStore":
        """Create an EventStore that will manage its own pool via connect()/close()."""
        store = cls.__new__(cls)
        store._pool = None
        store._db_url = db_url
        store.upcasters = upcaster_registry
        return store

    async def connect(self) -> None:
        """Create the connection pool (only for url-based construction)."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._db_url, min_size=2, max_size=10)

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()

    # ── stream_version ────────────────────────────────────────────────────────

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or 0 if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else 0

    # ── append ────────────────────────────────────────────────────────────────

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,    # -1=new stream, 0+=expected current
        correlation_id: str | None = None,
        causation_id: str | None = None,
        command_id: str | None = None,
    ) -> int:
        """
        Appends events atomically with OCC. Returns the new stream version.
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 0. Idempotency check — deduplicate by command_id
                if command_id:
                    dup = await conn.fetchrow(
                        "SELECT event_id FROM events"
                        " WHERE stream_id = $1 AND metadata @> $2::jsonb",
                        stream_id,
                        json.dumps({"command_id": command_id}),
                    )
                    if dup:
                        # Already processed — return current version (idempotent)
                        ver = await conn.fetchrow(
                            "SELECT current_version FROM event_streams WHERE stream_id = $1",
                            stream_id,
                        )
                        return ver["current_version"] if ver else 0

                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version, archived_at FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE",
                    stream_id,
                )

                if row and row["archived_at"] is not None:
                    raise StreamArchivedError(stream_id)

                # 2. OCC check — separate logic for new vs existing streams
                if row is None:
                    # New stream: only expected_version=-1 is valid
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(stream_id, expected_version, 0)
                    current = 0  # positions start at 1 (current + 1)
                    # Derive aggregate type from known stream prefixes
                    parts = stream_id.split("-")
                    aggregate_type = parts[0]
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id,
                        aggregate_type,
                    )
                else:
                    # Existing stream: exact version match required
                    current = row["current_version"]
                    if current != expected_version:
                        raise OptimisticConcurrencyError(stream_id, expected_version, current)

                positions = []
                # Remove `metadata` dict merging, use correlation_id and causation_id
                meta: dict[str, str] = {}
                if correlation_id:
                    meta["correlation_id"] = correlation_id
                if causation_id:
                    meta["causation_id"] = causation_id
                if command_id:
                    meta["command_id"] = command_id

                now = datetime.now(timezone.utc)

                for i, event in enumerate(events):
                    if hasattr(event, "to_store_dict"):
                        event_dict = event.to_store_dict()
                    else:
                        event_dict = event.model_dump(exclude_none=True)

                    pos = current + 1 + i
                    # INSERT with RETURNING to get the actual event_id for outbox FK
                    row = await conn.fetchrow(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)"
                        " RETURNING event_id",
                        stream_id,
                        pos,
                        event_dict["event_type"],
                        event_dict.get("event_version", 1),
                        json.dumps(event_dict.get("payload", {})),
                        json.dumps(meta),
                        now,
                    )
                    actual_event_id = row["event_id"]
                    positions.append(pos)

                    # Outbox entry — uses the REAL event_id from the INSERT
                    await conn.execute(
                        "INSERT INTO outbox(event_id, destination, payload)"
                        " VALUES($1, $2, $3::jsonb)",
                        actual_event_id,
                        "internal.bus",
                        json.dumps({
                            "stream_id": stream_id,
                            "event_type": event_dict["event_type"],
                            "payload": event_dict.get("payload", {}),
                            "metadata": meta,
                        }),
                    )

                # 5. Update stream version
                new_version = current + len(events)
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    new_version,
                    stream_id,
                )

                return new_version

    # ── load_stream ───────────────────────────────────────────────────────────

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Loads events from a stream in stream_position order.
        Returns list[StoredEvent]. Applies upcasters if registered.
        Returns empty list if stream does not exist.
        """
        async with self._pool.acquire() as conn:
            # Check stream exists for consistent behavior with InMemoryEventStore
            stream_row = await conn.fetchrow(
                "SELECT 1 FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            if stream_row is None:
                return []

            q = (
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE stream_id=$1 AND stream_position>$2"
            )
            params: list[Any] = [stream_id, from_position]

            if to_position is not None:
                q += " AND stream_position<=$3"
                params.append(to_position)

            q += " ORDER BY stream_position ASC"
            rows = await conn.fetch(q, *params)

            events: list[StoredEvent] = []
            for row in rows:
                e = StoredEvent(
                    event_id=row["event_id"],
                    stream_id=row["stream_id"],
                    stream_position=row["stream_position"],
                    global_position=row["global_position"],
                    event_type=row["event_type"],
                    event_version=row["event_version"],
                    payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else dict(row["payload"]) if row["payload"] else {},
                    metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else dict(row["metadata"]) if row["metadata"] else {},
                    recorded_at=row["recorded_at"],
                )
                if self.upcasters:
                    e = self.upcasters.upcast(e)
                events.append(e)
            return events

    # ── load_all ──────────────────────────────────────────────────────────────

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncGenerator[StoredEvent, None]:
        """
        Async generator yielding all events by global_position as StoredEvent.
        Used by the ProjectionDaemon.
        """
        async with self._pool.acquire() as conn:
            pos = from_global_position
            while True:
                q = (
                    "SELECT global_position, event_id, stream_id, stream_position,"
                    " event_type, event_version, payload, metadata, recorded_at"
                    " FROM events WHERE global_position > $1"
                )
                params: list[Any] = [pos]

                if event_types:
                    q += " AND event_type = ANY($2)"
                    params.append(event_types)

                q += f" ORDER BY global_position ASC LIMIT ${len(params) + 1}"
                params.append(batch_size)

                rows = await conn.fetch(q, *params)
                if not rows:
                    break
                for row in rows:
                    e = StoredEvent(
                        event_id=row["event_id"],
                        stream_id=row["stream_id"],
                        stream_position=row["stream_position"],
                        global_position=row["global_position"],
                        event_type=row["event_type"],
                        event_version=row["event_version"],
                        payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else dict(row["payload"]) if row["payload"] else {},
                        metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else dict(row["metadata"]) if row["metadata"] else {},
                        recorded_at=row["recorded_at"],
                    )
                    if self.upcasters:
                        e = self.upcasters.upcast(e)
                    yield e
                pos = rows[-1]["global_position"]
                if len(rows) < batch_size:
                    break

    # ── get_event ─────────────────────────────────────────────────────────────

    async def get_event(self, event_id: UUID) -> StoredEvent | None:
        """
        Loads one event by UUID. Used for causation chain lookups.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE event_id=$1",
                event_id,
            )
            if not row:
                return None
            return StoredEvent(
                event_id=row["event_id"],
                stream_id=row["stream_id"],
                stream_position=row["stream_position"],
                global_position=row["global_position"],
                event_type=row["event_type"],
                event_version=row["event_version"],
                payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else dict(row["payload"]) if row["payload"] else {},
                metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else dict(row["metadata"]) if row["metadata"] else {},
                recorded_at=row["recorded_at"],
            )

    # ── Checkpoint management ─────────────────────────────────────────────────

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        """Persist the last-processed global_position for a projection."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO projection_checkpoints(projection_name, last_position, updated_at)"
                " VALUES($1, $2, NOW())"
                " ON CONFLICT(projection_name) DO UPDATE"
                " SET last_position = $2, updated_at = NOW()",
                projection_name,
                position,
            )

    async def load_checkpoint(self, projection_name: str) -> int:
        """Load the last-processed global_position for a projection. Default 0."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints"
                " WHERE projection_name = $1",
                projection_name,
            )
            return row["last_position"] if row else 0

    # ── Archive ───────────────────────────────────────────────────────────────

    async def archive_stream(self, stream_id: str) -> None:
        """Mark stream as archived. Archived streams cannot be appended to."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE event_streams SET archived_at = NOW()"
                " WHERE stream_id = $1 AND archived_at IS NULL",
                stream_id,
            )
            if result == "UPDATE 0":
                # Either stream doesn't exist or already archived
                row = await conn.fetchrow(
                    "SELECT stream_id FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                if not row:
                    from models.events import StreamNotFoundError
                    raise StreamNotFoundError(stream_id)

    # ── Stream metadata ──────────────────────────────────────────────────────

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        """Return stream metadata as a StreamMetadata object."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id, aggregate_type, current_version, created_at,"
                " archived_at, metadata"
                " FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            if not row:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=dict(row["metadata"]) if row["metadata"] else {},
            )
