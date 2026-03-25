from typing import Callable
from models.events import StoredEvent

# ─── UPCASTER REGISTRY (Phase 4) ─────────────────────────────────────────────

# Maximum number of upcasting steps before raising UpcastingError.
# Prevents infinite loops from misconfigured upcaster chains.
MAX_UPCAST_STEPS = 10

# Known latest versions per event type. Upcaster stops here.
# Add entries as new versions are introduced.
LATEST_VERSIONS: dict[str, int] = {
    "CreditAnalysisCompleted": 2,
    "DecisionGenerated": 2,
}


class UpcastingError(Exception):
    """Raised when upcasting fails (infinite chain, malformed payload)."""
    pass


class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.register("CreditAnalysisCompleted", from_version=1)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            payload.setdefault("model_version", "legacy-pre-2026")
            return payload

    Safety guarantees:
        - MAX_UPCAST_STEPS loop guard prevents infinite chains
        - LATEST_VERSIONS ceiling prevents over-upcasting
        - Events without registered upcasters pass through unchanged
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, Callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def register(self, event_type: str, from_version: int):
        """Decorator. Registers fn as upcaster from event_type@from_version."""
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """Apply chain of upcasters with safety limits.

        Loop guard: raises UpcastingError after MAX_UPCAST_STEPS iterations.
        Ceiling: stops at LATEST_VERSIONS[event_type] if defined.
        """
        et = event.event_type
        v = event.event_version
        chain = self._upcasters.get(et, {})
        current = event
        ceiling = LATEST_VERSIONS.get(et)
        steps = 0

        while v in chain:
            # Ceiling check: don't upcast beyond known latest
            if ceiling and v >= ceiling:
                break

            steps += 1
            if steps > MAX_UPCAST_STEPS:
                raise UpcastingError(
                    f"Infinite upcast chain detected for {et} "
                    f"at version {v} after {steps} steps. "
                    f"Check for circular registrations."
                )

            new_payload = chain[v](dict(current.payload))
            v += 1
            current = current.model_copy(update={"payload": new_payload, "event_version": v})
        return current
