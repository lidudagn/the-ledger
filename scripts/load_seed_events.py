"""
Load seed events from seed_events.jsonl into the PostgreSQL event store.

Uses the EventStore.append() method to properly write each stream's events
with correct stream positions, OCC enforcement, and outbox entries.
"""
import asyncio
import json
import uuid
from collections import defaultdict
from pathlib import Path

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from event_store import EventStore, create_pool
from in_memory_store import OptimisticConcurrencyError
from models.events import BaseEvent
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

SEED_FILE = Path(__file__).parent.parent / "seed_events.jsonl"


async def load_seed_events():
    """Load all seed events into the event store, grouped by stream."""
    # Load and group events by stream_id (preserving order)
    events_by_stream: dict[str, list[dict]] = defaultdict(list)
    with open(SEED_FILE) as f:
        for line in f:
            e = json.loads(line.strip())
            events_by_stream[e["stream_id"]].append(e)

    total = sum(len(v) for v in events_by_stream.values())
    print(f"Loaded {total} events across {len(events_by_stream)} streams")

    pool = await create_pool(DATABASE_URL, min_size=2, max_size=5)
    store = EventStore(pool)

    streams_loaded = 0
    events_loaded = 0
    skipped = 0

    for stream_id, raw_events in sorted(events_by_stream.items()):
        # Check if already loaded (idempotent)
        current_version = await store.stream_version(stream_id)
        if current_version >= len(raw_events):
            skipped += len(raw_events)
            continue

        # Convert remaining events to BaseEvent objects
        batch: list[BaseEvent] = []
        for raw in raw_events[current_version:]:
            batch.append(BaseEvent(
                event_type=raw["event_type"],
                payload=raw["payload"],
                event_version=raw.get("event_version", 1),
            ))

        if not batch:
            continue

        # Use -1 for brand new streams, current_version for existing ones
        ev = -1 if current_version == 0 else current_version

        try:
            await store.append(
                stream_id=stream_id,
                events=batch,
                expected_version=ev,
                correlation_id=str(uuid.uuid4()),
            )
            events_loaded += len(batch)
            streams_loaded += 1
        except OptimisticConcurrencyError:
            print(f"  [OCC] {stream_id}: version conflict, skipping")
            skipped += len(batch)
        except Exception as exc:
            print(f"  [ERROR] {stream_id}: {exc}")
            raise

    await pool.close()

    print(f"\n=== SEED LOAD COMPLETE ===")
    print(f"  Streams loaded: {streams_loaded}")
    print(f"  Events loaded:  {events_loaded}")
    print(f"  Events skipped: {skipped}")
    print(f"  Total:          {events_loaded + skipped}")


if __name__ == "__main__":
    asyncio.run(load_seed_events())
