"""
The Ledger — ProjectionDaemon

Background asyncio task that polls the events table via load_all(),
routes events to subscribed projections, and manages checkpoints.

Events from load_all() are StoredEvent objects with attributes:
  event_type, event_version, payload, metadata, global_position, etc.

Fault tolerance:
  - Per-event retry up to max_retries, then skip + log
  - Individual projection failure cannot crash the daemon
  - Checkpoints updated after each successfully processed event

Observability:
  - get_all_lags() returns lag_events + lag_ms (wall-clock from oldest unprocessed)
  - SLO enforcement: dynamic poll_interval tightens on breach
  - ProjectionLagExceeded / ProjectionLagCritical log levels
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from projections.base import Projection

if TYPE_CHECKING:
    pass

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
        store: Any,
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
        self._latest_global_position: int = -1
        # SLO thresholds per projection (ms). Override via set_slo().
        self._slo_thresholds: dict[str, int] = {}
        # Cache event timestamps for lag computation: gp -> recorded_at
        self._event_timestamps: dict[int, datetime] = {}
        # Estimated lag_ms per projection (fallback when timestamps not cached)
        self._estimated_lag_ms: dict[str, float] = {}
        # Dynamic poll interval (adjusted by backpressure)
        self._base_poll_interval: float = 0.1
        self._current_poll_interval: float = 0.1

    def set_slo(self, projection_name: str, max_lag_ms: int) -> None:
        """Set a lag SLO threshold for a projection."""
        self._slo_thresholds[projection_name] = max_lag_ms

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        """Main loop: poll → process → sleep. Runs until stop() is called.

        Integrates backpressure:
          - Normal: polls at poll_interval_ms
          - SLO breached: tightens to 10ms (catch up faster)
          - Critical (5× SLO): emits CRITICAL log for alerting
        """
        self._running = True
        self._base_poll_interval = poll_interval_ms / 1000
        self._current_poll_interval = self._base_poll_interval

        while self._running:
            processed = await self._process_batch()

            # Check SLO and adjust polling dynamically
            lags = await self.get_all_lags()
            any_breached = any(m["slo_breached"] for m in lags.values())

            if any_breached:
                # Tight poll: 10ms to catch up faster
                self._current_poll_interval = 0.01
                for name, metrics in lags.items():
                    if metrics["slo_breached"]:
                        threshold = self._slo_thresholds.get(name, 0)
                        logger.warning(
                            "ProjectionLagExceeded: '%s' lag=%.0fms (SLO=%dms)",
                            name, metrics["lag_ms"], threshold,
                        )
                        if threshold and metrics["lag_ms"] > threshold * 5:
                            logger.critical(
                                "ProjectionLagCritical: '%s' lag=%.0fms (5x SLO=%dms)",
                                name, metrics["lag_ms"], threshold * 5,
                            )
            else:
                self._current_poll_interval = self._base_poll_interval

            if processed == 0:
                await asyncio.sleep(self._current_poll_interval)

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
            min_checkpoint = -1

        # Load events from the lowest checkpoint
        processed = 0
        async for event in self._store.load_all(
            from_global_position=int(min_checkpoint),
            batch_size=batch_size,
        ):
            gp = event.global_position

            # Track latest position for lag calculation
            if gp > self._latest_global_position:
                self._latest_global_position = gp

            # Cache event timestamp for oldest-unprocessed lag computation
            self._event_timestamps[gp] = event.recorded_at

            # Route to each subscribed projection
            for name, proj in self._projections.items():
                # Skip if this projection already processed this event
                if gp <= checkpoints.get(name, 0):
                    continue

                # Skip if event type not in subscription set
                if event.event_type not in proj.subscribed_events:
                    # Still advance checkpoint past this irrelevant event
                    await proj.set_checkpoint(gp)
                    checkpoints[name] = gp
                    continue

                # Try to handle the event (convert StoredEvent to dict for projections)
                event_dict = {
                    "event_id": str(event.event_id),
                    "stream_id": event.stream_id,
                    "stream_position": event.stream_position,
                    "global_position": event.global_position,
                    "event_type": event.event_type,
                    "event_version": event.event_version,
                    "payload": event.payload if isinstance(event.payload, dict) else dict(event.payload),
                    "metadata": event.metadata if isinstance(event.metadata, dict) else dict(event.metadata or {}),
                    "recorded_at": event.recorded_at,
                }
                try:
                    await proj.handle(event_dict)
                    await proj.set_checkpoint(gp)
                    checkpoints[name] = gp
                    # Clear error count on success
                    self._error_counts.pop(gp, None)
                except Exception as exc:
                    self._error_counts[gp] = self._error_counts.get(gp, 0) + 1
                    retries = self._error_counts[gp]

                    if retries >= self._max_retries:
                        logger.error(
                            "Projection '%s' failed on event %d after %d retries, "
                            "SKIPPING: %s",
                            name, gp, retries, exc,
                        )
                        # Skip the event — advance checkpoint past it
                        await proj.set_checkpoint(gp)
                        checkpoints[name] = gp
                        self._error_counts.pop(gp, None)
                    else:
                        logger.warning(
                            "Projection '%s' failed on event %d (attempt %d/%d): %s",
                            name, gp, retries, self._max_retries, exc,
                        )

            processed += 1

        # Evict old timestamps: remove entries where ALL projections have processed past
        self._evict_old_timestamps(checkpoints)

        return processed

    def _evict_old_timestamps(self, checkpoints: dict[str, int]) -> None:
        """Remove cached timestamps for positions all projections have processed."""
        if not checkpoints:
            return
        min_cp = min(checkpoints.values())
        stale = [gp for gp in self._event_timestamps if gp <= min_cp]
        for gp in stale:
            del self._event_timestamps[gp]

    async def get_all_lags(self) -> dict[str, dict[str, Any]]:
        """Returns lag metrics for every registered projection.

        lag_ms is computed from the OLDEST UNPROCESSED event (not last processed).
        This is the true measure of projection staleness:
          - If all caught up: lag_ms = 0
          - Otherwise: lag_ms = now - recorded_at of first unprocessed event
        """
        now = datetime.now(timezone.utc)
        result: dict[str, dict[str, Any]] = {}

        for name, proj in self._projections.items():
            cp = await proj.get_checkpoint()
            lag_events = max(0, self._latest_global_position - cp)

            if lag_events > 0:
                # Oldest unprocessed = first event with gp > checkpoint
                oldest_unprocessed_gp = cp + 1
                oldest_ts = self._event_timestamps.get(oldest_unprocessed_gp)
                if oldest_ts:
                    if isinstance(oldest_ts, str):
                        oldest_ts = datetime.fromisoformat(oldest_ts.replace("Z", "+00:00"))
                    if oldest_ts.tzinfo is None:
                        oldest_ts = oldest_ts.replace(tzinfo=timezone.utc)
                    lag_ms = (now - oldest_ts).total_seconds() * 1000
                else:
                    # Timestamp not cached — use conservative estimate
                    lag_ms = self._estimated_lag_ms.get(name, 0.0)
            else:
                lag_ms = 0.0

            threshold = self._slo_thresholds.get(name, float("inf"))
            result[name] = {
                "lag_events": lag_events,
                "lag_ms": lag_ms,
                "checkpoint": cp,
                "latest_position": self._latest_global_position,
                "slo_breached": lag_ms > threshold,
            }
            # Update estimated lag for fallback
            self._estimated_lag_ms[name] = lag_ms

        return result

    async def _check_slo_violations(self) -> None:
        """Log warnings when projection lag exceeds SLO thresholds."""
        lags = await self.get_all_lags()
        for name, metrics in lags.items():
            threshold = self._slo_thresholds.get(name)
            if threshold and metrics["slo_breached"]:
                logger.warning(
                    "ProjectionLagExceeded: '%s' lag=%.0fms (SLO=%dms)",
                    name, metrics["lag_ms"], threshold,
                )

    @property
    def projection_names(self) -> list[str]:
        """List registered projection names."""
        return list(self._projections.keys())
