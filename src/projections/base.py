"""
The Ledger — Projection Base Classes

Provides the Projection ABC that all projections implement,
and InMemoryProjectionStore for testing without PostgreSQL.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
import asyncpg


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
    async def handle(self, event: dict) -> None:
        """Process a single event dict. Must be idempotent."""

    @abstractmethod
    async def rebuild_from_scratch(self) -> None:
        """Clear all projection state and reset checkpoint to 0."""

    async def get_checkpoint(self) -> int:
        """Returns last processed global_position. Override for DB-backed."""
        return getattr(self, "_checkpoint", -1)

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

    async def upsert(self, table: str, key: Any, row: dict[str, Any]) -> None:
        """Insert or update a row by primary key."""
        if table not in self._tables:
            self._tables[table] = {}
        existing = self._tables[table].get(key, {})
        existing.update(row)
        self._tables[table][key] = existing

    async def insert_if_absent(self, table: str, key: Any, row: dict[str, Any]) -> bool:
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

    async def truncate(self, table: str) -> None:
        """Remove all rows from a table."""
        self._tables[table] = {}

    def count(self, table: str) -> int:
        """Count rows in a table."""
        return len(self._tables.get(table, {}))

# -------------------------------------------------------------------------
# SQL Identifier Safety
# -------------------------------------------------------------------------
# All VALUES in queries use parameterized $N placeholders (injection-safe).
# Only SQL identifiers (table/column names) are interpolated via f-strings.
# To prevent injection via identifiers, we validate against explicit whitelists.
# -------------------------------------------------------------------------

_ALLOWED_TABLES = frozenset({
    "application_summary", "agent_performance", "compliance_audit_events",
    "compliance_snapshots", "projection_checkpoints",
})

_ALLOWED_COLUMNS = frozenset({
    # application_summary
    "application_id", "state", "applicant_id", "requested_amount_usd",
    "approved_amount_usd", "risk_tier", "fraud_score", "decision",
    "decision_confidence", "reviewer_id", "created_at", "updated_at",
    "interest_rate", "conditions", "approved_by", "effective_date",
    "loan_purpose", "submission_channel", "submitted_at",
    # agent_performance
    "agent_id", "model_version", "analyses_completed",
    "avg_confidence_score", "avg_analysis_duration_ms",
    "decisions_generated", "approve_count", "decline_count", "refer_count",
    "human_override_count", "last_updated_at",
    # compliance_audit_events
    "global_position", "event_type", "event_version", "stream_id",
    "payload", "metadata", "recorded_at",
    # compliance_snapshots
    "overall_verdict", "has_hard_block", "snapshot_at",
    "rules", "total_events",
    # projection_checkpoints
    "projection_name", "last_position",
    # generic
    "id",
})


def _validate_identifier(name: str, kind: str, allowed: frozenset[str]) -> None:
    """Raise ValueError if name is not in the allowed set."""
    if name not in allowed:
        raise ValueError(
            f"SQL {kind} '{name}' not in allowed whitelist. "
            f"Add it to _ALLOWED_{kind.upper()}S if this is intentional."
        )


class PgProjectionStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    def _get_pk_cols(self, table: str) -> list[str]:
        if table == "agent_performance":
            return ["agent_id", "model_version"]
        if table in ("application_summary", "compliance_snapshots"):
            return ["application_id"]
        if table == "compliance_audit_events":
            return ["global_position"]
        if table == "projection_checkpoints":
            return ["projection_name"]
        return ["id"]

    async def _execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)
            
    async def _fetchval(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def _fetch(self, query: str, *args):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, *args)
            return [dict(r) for r in records]

    async def upsert(self, table: str, key: Any, row: dict[str, Any]) -> None:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        pks = self._get_pk_cols(table)
        cols = list(row.keys())
        for c in cols:
            _validate_identifier(c, "column", _ALLOWED_COLUMNS)
        vals = list(row.values())
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        col_names = ", ".join(cols)
        
        update_sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in pks)
        pk_names = ", ".join(pks)
        
        if update_sets:
            q = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({pk_names}) DO UPDATE SET {update_sets}"
        else:
            q = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({pk_names}) DO NOTHING"
        
        await self._execute(q, *vals)

    async def insert_if_absent(self, table: str, key: Any, row: dict[str, Any]) -> bool:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        pks = self._get_pk_cols(table)
        cols = list(row.keys())
        for c in cols:
            _validate_identifier(c, "column", _ALLOWED_COLUMNS)
        vals = list(row.values())
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        col_names = ", ".join(cols)
        pk_names = ", ".join(pks)
        
        q = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({pk_names}) DO NOTHING RETURNING 1"
        res = await self._fetchval(q, *vals)
        return bool(res)

    async def get(self, table: str, key: Any) -> dict[str, Any] | None:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        pks = self._get_pk_cols(table)
        where_clause = " AND ".join(f"{p} = ${i+1}" for i, p in enumerate(pks))
        q = f"SELECT * FROM {table} WHERE {where_clause}"
        
        # Key might be single value or tuple
        args = key if isinstance(key, (tuple, list)) else [key]
        rows = await self._fetch(q, *args)
        return rows[0] if rows else None

    async def get_all(self, table: str) -> list[dict[str, Any]]:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        return await self._fetch(f"SELECT * FROM {table}")

    async def query(self, table: str, filter_fn: Any = None) -> list[dict[str, Any]]:
        rows = await self.get_all(table)
        if filter_fn:
            rows = [r for r in rows if filter_fn(r)]
        return rows

    async def truncate(self, table: str) -> None:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        await self._execute(f"TRUNCATE TABLE {table} CASCADE")

    async def count(self, table: str) -> int:
        _validate_identifier(table, "table", _ALLOWED_TABLES)
        return await self._fetchval(f"SELECT COUNT(*) FROM {table}")
