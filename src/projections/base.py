"""
The Ledger — Projection Base Classes

Provides the Projection ABC that all projections implement,
and InMemoryProjectionStore for testing without PostgreSQL.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from models.events import StoredEvent


class Projection(ABC):
    """
    Base class for all projections.

    Contract:
      - handle() MUST be idempotent (at-least-once delivery from daemon)
      - subscribed_events determines which events reach this projection
      - rebuild_from_scratch() truncates + resets checkpoint → daemon replays all
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique projection name (used as projection_checkpoints key)."""

    @property
    @abstractmethod
    def subscribed_events(self) -> set[str]:
        """Set of event_type strings this projection processes."""

    @abstractmethod
    async def handle(self, event: StoredEvent) -> None:
        """Process a single event. Must be idempotent."""

    @abstractmethod
    async def rebuild_from_scratch(self) -> None:
        """Clear all projection state and reset checkpoint to 0."""

    async def get_checkpoint(self) -> int:
        """Returns last processed global_position. Override for DB-backed."""
        return getattr(self, "_checkpoint", 0)

    async def set_checkpoint(self, position: int) -> None:
        """Update last processed global_position. Override for DB-backed."""
        self._checkpoint = position


class InMemoryProjectionStore:
    """
    Dict-backed projection storage for unit testing.

    Supports UPSERT (by primary key) and simple queries.
    Each 'table' is a dict keyed by primary key → row dict.
    """

    def __init__(self) -> None:
        self._tables: dict[str, dict[Any, dict[str, Any]]] = {}

    def upsert(self, table: str, key: Any, row: dict[str, Any]) -> None:
        """Insert or update a row by primary key."""
        if table not in self._tables:
            self._tables[table] = {}
        existing = self._tables[table].get(key, {})
        existing.update(row)
        self._tables[table][key] = existing

    def insert_if_absent(self, table: str, key: Any, row: dict[str, Any]) -> bool:
        """Insert only if key doesn't exist. Returns True if inserted."""
        if table not in self._tables:
            self._tables[table] = {}
        if key in self._tables[table]:
            return False
        self._tables[table][key] = row
        return True

    def get(self, table: str, key: Any) -> dict[str, Any] | None:
        """Get a single row by primary key."""
        return self._tables.get(table, {}).get(key)

    def get_all(self, table: str) -> list[dict[str, Any]]:
        """Get all rows from a table."""
        return list(self._tables.get(table, {}).values())

    def query(
        self, table: str, filter_fn: Any = None
    ) -> list[dict[str, Any]]:
        """Query rows with optional filter function."""
        rows = self.get_all(table)
        if filter_fn:
            rows = [r for r in rows if filter_fn(r)]
        return rows

    def truncate(self, table: str) -> None:
        """Remove all rows from a table."""
        self._tables[table] = {}

    def count(self, table: str) -> int:
        """Count rows in a table."""
        return len(self._tables.get(table, {}))
