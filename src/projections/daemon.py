"""
The Ledger — ProjectionDaemon

Background asyncio task that polls the events table via load_all(),
routes events to subscribed projections, and manages checkpoints.

Fault tolerance:
  - Per-event retry up to max_retries, then skip + log
  - Individual projection failure cannot crash the daemon
  - Checkpoints updated after each successfully processed event
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from models.events import StoredEvent
from projections.base import Projection

if TYPE_CHECKING:
    from event_store import EventStore
    from in_memory_store import InMemoryEventStore

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Async daemon that keeps projections in sync with the event store.

    Usage:
        daemon = ProjectionDaemon(store, [app_summary, agent_perf, compliance])
        task = asyncio.create_task(daemon.run_forever())
        # ... later ...
        await daemon.stop()
    """

    def __init__(
        self,
        store: "EventStore | InMemoryEventStore",
        projections: list[Projection],
        max_retries: int = 3,
    ) -> None:
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._running = False
        self._max_retries = max_retries
        # Track per-event retry counts: global_position -> count
        self._error_counts: dict[int, int] = {}
        # Track the latest global_position we've seen (for lag calculation)
        self._latest_global_position: int = 0
        # SLO thresholds per projection (ms). Override via set_slo().
        self._slo_thresholds: dict[str, int] = {}

    def set_slo(self, projection_name: str, max_lag_ms: int) -> None:
        """Set a lag SLO threshold for a projection."""
        self._slo_thresholds[projection_name] = max_lag_ms

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """Main loop: poll → process → sleep. Runs until stop() is called."""
        self._running = True
        while self._running:
            processed = await self._process_batch()
            if processed == 0:
                await asyncio.sleep(poll_interval_ms / 1000)

    async def run_once(self) -> int:
        """Process a single batch. Useful for testing."""
        return await self._process_batch()

    async def stop(self) -> None:
        """Signal the daemon to stop after the current batch."""
        self._running = False

    async def _process_batch(self, batch_size: int = 500) -> int:
        """
        Core processing loop:
        1. Find lowest checkpoint across all projections
        2. Load events from that position
        3. Route to subscribed projections
        4. Update checkpoints
        """
        # Find the lowest checkpoint position
        min_checkpoint = float("inf")
        checkpoints: dict[str, int] = {}
        for name, proj in self._projections.items():
            cp = await proj.get_checkpoint()
            checkpoints[name] = cp
            if cp < min_checkpoint:
                min_checkpoint = cp

        if min_checkpoint == float("inf"):
            min_checkpoint = 0

        # Load events from the lowest checkpoint
        processed = 0
        async for event in self._store.load_all(
            from_global_position=int(min_checkpoint),
            batch_size=batch_size,
        ):
            # Track latest position for lag calculation
            if event.global_position > self._latest_global_position:
                self._latest_global_position = event.global_position

            # Route to each subscribed projection
            for name, proj in self._projections.items():
                # Skip if this projection already processed this event
                if event.global_position <= checkpoints.get(name, 0):
                    continue

                # Skip if event type not in subscription set
                if event.event_type not in proj.subscribed_events:
                    # Still advance checkpoint past this irrelevant event
                    await proj.set_checkpoint(event.global_position)
                    checkpoints[name] = event.global_position
                    continue

                # Try to handle the event
                try:
                    await proj.handle(event)
                    await proj.set_checkpoint(event.global_position)
                    checkpoints[name] = event.global_position
                    # Clear error count on success
                    self._error_counts.pop(event.global_position, None)
                except Exception as exc:
                    gp = event.global_position
                    self._error_counts[gp] = self._error_counts.get(gp, 0) + 1
                    retries = self._error_counts[gp]

                    if retries >= self._max_retries:
                        logger.error(
                            "Projection '%s' failed on event %d after %d retries, "
                            "SKIPPING: %s",
                            name, gp, retries, exc,
                        )
                        # Skip the event — advance checkpoint past it
                        await proj.set_checkpoint(event.global_position)
                        checkpoints[name] = event.global_position
                        self._error_counts.pop(gp, None)
                    else:
                        logger.warning(
                            "Projection '%s' failed on event %d (attempt %d/%d): %s",
                            name, gp, retries, self._max_retries, exc,
                        )

            processed += 1

        # Check SLO violations
        await self._check_slo_violations()

        return processed

    async def get_all_lags(self) -> dict[str, dict[str, Any]]:
        """
        Returns lag metrics for every registered projection.

        {
            "application_summary": {
                "lag_events": 12,
                "checkpoint": 88,
                "latest_position": 100,
            },
            ...
        }
        """
        result: dict[str, dict[str, Any]] = {}
        for name, proj in self._projections.items():
            cp = await proj.get_checkpoint()
            lag = max(0, self._latest_global_position - cp)
            result[name] = {
                "lag_events": lag,
                "checkpoint": cp,
                "latest_position": self._latest_global_position,
            }
        return result

    async def _check_slo_violations(self) -> None:
        """Log warnings when projection lag exceeds SLO thresholds."""
        lags = await self.get_all_lags()
        for name, metrics in lags.items():
            threshold = self._slo_thresholds.get(name)
            if threshold and metrics["lag_events"] > 0:
                logger.debug(
                    "Projection '%s' lag: %d events behind",
                    name, metrics["lag_events"],
                )

    @property
    def projection_names(self) -> list[str]:
        """List registered projection names."""
        return list(self._projections.keys())
