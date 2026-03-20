"""
Real PostgreSQL EventStore tests.
Skipped if DATABASE_URL is not set.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Skip entire module if no DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="DATABASE_URL not set — skipping real DB tests",
)


@pytest.fixture
async def db_store():
    """EventStore connected to real DB. Cleans up after each test."""
    from event_store import EventStore, create_pool

    pool = await create_pool(DATABASE_URL)

    # Clean tables before test
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM outbox")
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM event_streams")

    store = EventStore(pool)
    yield store
    await pool.close()


@pytest.fixture
def sample_events():
    from models.events import ApplicationSubmitted, CreditAnalysisCompleted

    return [
        ApplicationSubmitted(payload={"application_id": "APP-001"}),
        CreditAnalysisCompleted(payload={"application_id": "APP-001"}),
    ]


class TestDBAppend:
    async def test_new_stream(self, db_store, sample_events):
        v = await db_store.append("loan-001", sample_events[:1], expected_version=-1)
        assert v == 1

    async def test_occ_mismatch(self, db_store, sample_events):
        from models.events import OptimisticConcurrencyError

        await db_store.append("loan-001", sample_events[:1], expected_version=-1)
        with pytest.raises(OptimisticConcurrencyError):
            await db_store.append("loan-001", sample_events[1:], expected_version=5)


class TestDBLoadStream:
    async def test_ordering(self, db_store, sample_events):
        await db_store.append("loan-001", sample_events, expected_version=-1)
        events = await db_store.load_stream("loan-001")
        assert [e.stream_position for e in events] == [1, 2]


class TestDBConcurrentDoubleAppend:
    async def test_exactly_one_succeeds(self, db_store):
        """THE GATE TEST: two concurrent appends, exactly one wins."""
        from models.events import ApplicationSubmitted, OptimisticConcurrencyError

        # Setup: stream at version 3
        for i in range(3):
            await db_store.append(
                "loan-001",
                [ApplicationSubmitted(payload={"seq": i})],
                expected_version=i - 1 if i == 0 else i,
            )

        results = {"success": 0, "occ_error": 0}

        async def try_append(agent_id: str):
            try:
                await db_store.append(
                    "loan-001",
                    [ApplicationSubmitted(payload={"agent": agent_id})],
                    expected_version=3,
                )
                results["success"] += 1
            except OptimisticConcurrencyError:
                results["occ_error"] += 1

        await asyncio.gather(
            try_append("agent-01"),
            try_append("agent-02"),
        )

        assert results["success"] == 1
        assert results["occ_error"] == 1
        assert await db_store.stream_version("loan-001") == 4
