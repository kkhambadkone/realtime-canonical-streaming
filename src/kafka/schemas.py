"""
src/kafka/schemas.py  -  Kafka event schemas.

Every event published to Kafka follows this envelope:

{
  "event_id":       "uuid",
  "event_type":     "INSERT" | "UPDATE" | "DELETE",
  "source":         "postgres" | "sqlserver" | "teradata",
  "entity":         "customer" | "order" | "product",
  "timestamp":      "2024-01-01T00:00:00Z",
  "payload":        { ...source record fields... },
  "before":         { ...previous record (UPDATE/DELETE only)... },
  "metadata": {
    "table":        "customer",
    "database":     "customers_db",
    "lsn":          "optional CDC log sequence number",
  }
}
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

EventType = Literal["INSERT", "UPDATE", "DELETE", "SNAPSHOT"]


def make_event(
    source: str,
    entity: str,
    event_type: EventType,
    payload: dict[str, Any],
    before: dict[str, Any] | None = None,
    table: str | None = None,
    database: str | None = None,
    lsn: str | None = None,
) -> dict[str, Any]:
    """Build a canonical CDC event envelope."""
    return {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "source":     source,
        "entity":     entity,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "payload":    payload,
        "before":     before,
        "metadata": {
            "table":    table or entity,
            "database": database or "",
            "lsn":      lsn or "",
        },
    }


def serialize(event: dict[str, Any]) -> bytes:
    """Serialize event to JSON bytes for Kafka."""
    return json.dumps(event, default=str).encode("utf-8")


def deserialize(data: bytes) -> dict[str, Any]:
    """Deserialize Kafka message bytes to event dict."""
    return json.loads(data.decode("utf-8"))


def make_snapshot_event(
    source: str,
    entity: str,
    records: list[dict[str, Any]],
    table: str | None = None,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """Build snapshot events for initial load — one event per record."""
    return [
        make_event(
            source=source,
            entity=entity,
            event_type="SNAPSHOT",
            payload=record,
            table=table or entity,
            database=database or "",
        )
        for record in records
    ]
