from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from .contracts import CognitiveCapabilities


class RouteUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class RouteSelection:
    requested_route: str
    selected_route: str
    fallback_reason: str | None


class CapabilityRegistry:
    """Capability discovery keeps declared claims separate from observations."""
    def __init__(self, *, max_probe_age_seconds: int = 600) -> None:
        if max_probe_age_seconds < 1:
            raise ValueError("probe freshness window must be positive")
        self.max_probe_age_seconds = max_probe_age_seconds
        self._routes: dict[str, CognitiveCapabilities] = {}

    def register(self, capabilities: CognitiveCapabilities) -> None:
        if (not capabilities.route_id or capabilities.concurrency_limit < 1
                or capabilities.health not in {"UNKNOWN", "HEALTHY", "DEGRADED", "UNAVAILABLE"}):
            raise ValueError("route id and positive concurrency are required")
        self._routes[capabilities.route_id] = capabilities

    def get(self, route_id: str) -> CognitiveCapabilities:
        return self._routes[route_id]

    def routes(self) -> tuple[CognitiveCapabilities, ...]:
        return tuple(self._routes[key] for key in sorted(self._routes))

    def select(self, requested: str, *, role: str, capabilities: Iterable[str] = (),
               allowed_fallbacks: Iterable[str] = ()) -> RouteSelection:
        need = set(capabilities)
        now = datetime.now(timezone.utc)
        def fresh(route: CognitiveCapabilities) -> bool:
            if not route.last_probe:
                return False
            try:
                probed = datetime.fromisoformat(route.last_probe.replace("Z", "+00:00"))
                if probed.tzinfo is None:
                    return False
                age = (now - probed.astimezone(timezone.utc)).total_seconds()
                return 0 <= age <= self.max_probe_age_seconds
            except (ValueError, TypeError):
                return False
        eligible = [r for r in self.routes() if role in r.supported_roles
                    and need <= r.observed and r.health == "HEALTHY" and fresh(r)]
        by_id = {r.route_id: r for r in eligible}
        if requested in by_id:
            return RouteSelection(requested, requested, None)
        if requested not in self._routes:
            raise RouteUnavailable(f"unknown requested route: {requested}")
        for candidate in allowed_fallbacks:
            if candidate in by_id:
                return RouteSelection(requested, candidate,
                    f"requested route unavailable or not observed capable; explicit fallback to {candidate}")
        raise RouteUnavailable("requested route is unhealthy or lacks observed capabilities; no allowed fallback")
