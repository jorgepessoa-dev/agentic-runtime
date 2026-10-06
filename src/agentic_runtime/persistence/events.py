"""Persistence primitives shared by evolution services."""

from __future__ import annotations

import uuid
from typing import Any, Mapping

from agentic_runtime.contracts.serialization import canonical_hash


def jsonb(value: Any) -> Any:
    from psycopg.types.json import Jsonb

    return Jsonb(value)


def evolution_event(
    db: Any,
    event_type: str,
    *,
    scope_id: str | None,
    actor_id: str,
    payload: Mapping[str, Any],
    causation_ref: str | None = None,
) -> str:
    event_id = f"eevt_{uuid.uuid4().hex}"
    db.execute(
        """INSERT INTO evolution.evolution_events
        (event_id,event_type,scope_id,actor_id,causation_ref,payload,payload_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        (
            event_id,
            event_type,
            scope_id,
            actor_id,
            causation_ref,
            jsonb(dict(payload)),
            canonical_hash(payload),
        ),
    )
    return event_id


def runtime_event(
    db: Any,
    event_id: str,
    event_type: str,
    *,
    actor_id: str,
    correlation_id: str,
    payload: Mapping[str, Any],
    campaign_id: str | None = None,
    goal_id: str | None = None,
    task_id: str | None = None,
    attempt_id: str | None = None,
    causation_id: str | None = None,
) -> str:
    """Append runtime event evidence within the caller's transaction."""
    data = dict(payload)
    db.execute(
        """INSERT INTO runtime.events
        (event_id,event_type,campaign_id,goal_id,task_id,attempt_id,actor_type,actor_id,
         causation_id,correlation_id,schema_version,payload,payload_hash)
        VALUES (%s,%s,%s,%s,%s,%s,'RUNTIME',%s,%s,%s,'1',%s,%s)""",
        (
            event_id,
            event_type,
            campaign_id,
            goal_id,
            task_id,
            attempt_id,
            actor_id,
            causation_id,
            correlation_id,
            jsonb(data),
            canonical_hash(data),
        ),
    )
    return event_id
