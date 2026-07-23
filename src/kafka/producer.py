"""
src/kafka/producer.py  -  CDC event producers for all 3 sources.

Simulates Change Data Capture by polling sources at a configurable interval
and publishing events to Kafka topics.

Supports two modes:
  snapshot  → publish all existing records as SNAPSHOT events (initial load)
  cdc       → poll for changes since last watermark, publish INSERT/UPDATE events

Sources:
  Postgres    → direct psycopg2 query with timestamp watermark
  SQL Server  → DAB REST API with timestamp watermark
  Teradata    → teradatasql with timestamp watermark
"""
from __future__ import annotations

import asyncio
import json
import structlog
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import settings
from src.kafka.schemas import make_event, make_snapshot_event, serialize

log = structlog.get_logger()


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=serialize,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def _get_key(record: dict[str, Any], entity: str) -> str | None:
    """Extract partition key from record."""
    for key_field in [f"{entity}_id", "id", "ID"]:
        if key_field in record:
            return str(record[key_field])
    return None


class KafkaEventProducer:
    """Publishes CDC events from all 3 sources to Kafka."""

    def __init__(self):
        self.producer  = _make_producer()
        self.watermarks: dict[str, datetime] = {}

    def publish(
        self,
        entity: str,
        source: str,
        records: list[dict[str, Any]],
        event_type: str = "SNAPSHOT",
        table: str | None = None,
        database: str | None = None,
    ) -> int:
        """Publish records as Kafka events. Returns count published."""
        topic = settings.kafka_topic.get(entity,
               f"{settings.kafka_topic_prefix}.{entity}")
        count = 0

        for record in records:
            event = make_event(
                source=source, entity=entity,
                event_type=event_type,
                payload=record,
                table=table or entity,
                database=database or "",
            )
            key = _get_key(record, entity)
            try:
                self.producer.send(topic, value=event, key=key)
                count += 1
            except KafkaError as exc:
                log.error("kafka.publish_failed", entity=entity,
                          source=source, error=str(exc))

        self.producer.flush()
        log.info("kafka.published", entity=entity, source=source,
                 topic=topic, count=count, event_type=event_type)
        return count

    # ── Postgres producer ─────────────────────────────────────────────────────

    async def snapshot_postgres(self, entity_map: dict[str, str]) -> None:
        """Publish all Postgres records as SNAPSHOT events."""
        try:
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(settings.postgres_connection_string)
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            for canonical_name, table in entity_map.items():
                cur.execute(f"SELECT * FROM {table}")
                records = [dict(r) for r in cur.fetchall()]
                self.publish(canonical_name, "postgres", records,
                             "SNAPSHOT", table=table, database="customers_db")

            cur.close()
            conn.close()
        except Exception as exc:
            log.error("producer.postgres_snapshot_failed", error=str(exc))

    async def poll_postgres(
        self, entity_map: dict[str, str],
        ts_column: str = "updated_at",
    ) -> None:
        """Poll Postgres for changes since last watermark."""
        try:
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(settings.postgres_connection_string)
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            for canonical_name, table in entity_map.items():
                wm  = self.watermarks.get(f"postgres.{canonical_name}")
                sql = f"SELECT * FROM {table}"
                if wm:
                    sql += f" WHERE {ts_column} > '{wm.isoformat()}'"
                cur.execute(sql)
                records = [dict(r) for r in cur.fetchall()]
                if records:
                    self.publish(canonical_name, "postgres", records,
                                 "INSERT", table=table, database="customers_db")
                    self.watermarks[f"postgres.{canonical_name}"] = \
                        datetime.now(timezone.utc)

            cur.close()
            conn.close()
        except Exception as exc:
            log.error("producer.postgres_poll_failed", error=str(exc))

    # ── SQL Server producer (DAB REST) ────────────────────────────────────────

    async def snapshot_sqlserver(self, entity_map: dict[str, str]) -> None:
        """Publish all SQL Server records as SNAPSHOT events via DAB REST."""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                for canonical_name, entity in entity_map.items():
                    url = f"{settings.dab_base_url}/api/{entity}"
                    async with session.get(url) as resp:
                        data    = await resp.json()
                        records = data.get("value", [])
                        self.publish(canonical_name, "sqlserver", records,
                                     "SNAPSHOT", table=entity, database="CRM_DB")
        except Exception as exc:
            log.error("producer.sqlserver_snapshot_failed", error=str(exc))

    async def poll_sqlserver(self, entity_map: dict[str, str]) -> None:
        """Poll SQL Server for changes via DAB REST."""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                for canonical_name, entity in entity_map.items():
                    url = f"{settings.dab_base_url}/api/{entity}"
                    async with session.get(url) as resp:
                        data    = await resp.json()
                        records = data.get("value", [])
                        if records:
                            self.publish(canonical_name, "sqlserver", records,
                                         "INSERT", table=entity, database="CRM_DB")
        except Exception as exc:
            log.error("producer.sqlserver_poll_failed", error=str(exc))

    # ── Teradata producer ─────────────────────────────────────────────────────

    async def snapshot_teradata(self, entity_map: dict[str, str]) -> None:
        """Publish all Teradata records as SNAPSHOT events."""
        try:
            import teradatasql
            loop = asyncio.get_event_loop()

            def _fetch(table: str) -> list[dict]:
                with teradatasql.connect(
                    host=settings.teradata_host,
                    user=settings.teradata_user,
                    password=settings.teradata_password,
                    database=settings.teradata_database,
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT * FROM {settings.teradata_database}.{table}"
                        )
                        cols = [d[0].lower() for d in cur.description]
                        return [dict(zip(cols, row)) for row in cur.fetchall()]

            for canonical_name, table in entity_map.items():
                records = await loop.run_in_executor(None, _fetch, table)
                self.publish(canonical_name, "teradata", records,
                             "SNAPSHOT", table=table,
                             database=settings.teradata_database)

        except Exception as exc:
            log.error("producer.teradata_snapshot_failed", error=str(exc))

    async def poll_teradata(
        self, entity_map: dict[str, str],
        ts_column: str = "CREATED_TS",
    ) -> None:
        """Poll Teradata for changes since last watermark."""
        try:
            import teradatasql
            loop = asyncio.get_event_loop()

            def _fetch(table: str, wm: datetime | None) -> list[dict]:
                sql = f"SELECT * FROM {settings.teradata_database}.{table}"
                if wm:
                    sql += f" WHERE {ts_column} > TIMESTAMP '{wm.isoformat()}'"
                with teradatasql.connect(
                    host=settings.teradata_host,
                    user=settings.teradata_user,
                    password=settings.teradata_password,
                    database=settings.teradata_database,
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        cols = [d[0].lower() for d in cur.description]
                        return [dict(zip(cols, row)) for row in cur.fetchall()]

            for canonical_name, table in entity_map.items():
                wm      = self.watermarks.get(f"teradata.{canonical_name}")
                records = await loop.run_in_executor(None, _fetch, table, wm)
                if records:
                    self.publish(canonical_name, "teradata", records,
                                 "INSERT", table=table,
                                 database=settings.teradata_database)
                    self.watermarks[f"teradata.{canonical_name}"] = \
                        datetime.now(timezone.utc)

        except Exception as exc:
            log.error("producer.teradata_poll_failed", error=str(exc))

    def close(self) -> None:
        self.producer.close()
