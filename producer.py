"""
producer.py  -  Standalone CDC producer runner.

Runs continuously, polling all 3 sources for changes
and publishing events to Kafka topics.

Usage:
  python producer.py                  # poll mode (continuous)
  python producer.py --snapshot       # publish all existing records once
  python producer.py --entity customer  # only produce for customer
"""
from __future__ import annotations

import asyncio
import signal
import structlog
import typer
from rich.console import Console
from rich import print as rprint

from src.config import settings
from src.kafka.topics import create_topics
from src.kafka.producer import KafkaEventProducer

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()
console = Console()
app = typer.Typer()

# Entity → source table mappings (customize per your environment)
PG_ENTITY_MAP = {
    "customer": "customer",
    "order":    "orders",
    "product":  "products",
}
SQL_ENTITY_MAP = {
    "customer": "customers",
    "order":    "orders",
    "product":  "products",
}
TD_ENTITY_MAP = {
    "customer": "CUSTOMER",
}

_running = True


async def _run_cdc_loop(
    producer: KafkaEventProducer,
    entity: str | None = None,
) -> None:
    """Continuously poll all sources and publish changes to Kafka."""
    global _running

    pg_map  = {k: v for k, v in PG_ENTITY_MAP.items()
               if entity is None or k == entity}
    sql_map = {k: v for k, v in SQL_ENTITY_MAP.items()
               if entity is None or k == entity}
    td_map  = {k: v for k, v in TD_ENTITY_MAP.items()
               if entity is None or k == entity}

    log.info("producer.cdc_loop_start",
             entities=list(pg_map.keys()),
             poll_interval=settings.cdc_poll_interval_seconds)

    while _running:
        try:
            # Poll Postgres
            if pg_map:
                await producer.poll_postgres(pg_map)

            # Poll SQL Server via DAB REST
            if sql_map:
                await producer.poll_sqlserver(sql_map)

            # Poll Teradata
            if td_map and settings.teradata_host:
                await producer.poll_teradata(td_map)

            await asyncio.sleep(settings.cdc_poll_interval_seconds)

        except Exception as exc:
            log.error("producer.cdc_loop_error", error=str(exc))
            await asyncio.sleep(5)

    log.info("producer.cdc_loop_stopped")


async def _run_snapshot(
    producer: KafkaEventProducer,
    entity: str | None = None,
) -> None:
    """Publish all existing source records as SNAPSHOT events."""
    pg_map  = {k: v for k, v in PG_ENTITY_MAP.items()
               if entity is None or k == entity}
    sql_map = {k: v for k, v in SQL_ENTITY_MAP.items()
               if entity is None or k == entity}
    td_map  = {k: v for k, v in TD_ENTITY_MAP.items()
               if entity is None or k == entity}

    log.info("producer.snapshot_start",
             entities=list(pg_map.keys()))

    if pg_map:
        await producer.snapshot_postgres(pg_map)
    if sql_map:
        await producer.snapshot_sqlserver(sql_map)
    if td_map and settings.teradata_host:
        await producer.snapshot_teradata(td_map)

    log.info("producer.snapshot_done")


@app.command()
def main(
    snapshot: bool = typer.Option(
        False, "--snapshot",
        help="Publish all existing records once then exit"
    ),
    entity: str = typer.Option(
        None, "--entity", "-e",
        help="Only produce events for this entity (e.g. customer)"
    ),
) -> None:
    """CDC event producer — polls sources and publishes to Kafka."""

    async def _run():
        global _running

        # Create Kafka topics
        entities = [entity] if entity else settings.entities
        create_topics(entities)

        producer = KafkaEventProducer()

        def _shutdown(signum, frame):
            global _running
            log.info("producer.shutdown_signal")
            _running = False
            producer.close()

        signal.signal(signal.SIGINT,  _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        if snapshot:
            await _run_snapshot(producer, entity)
            producer.close()
        else:
            rprint("[green]Starting CDC producer — Ctrl+C to stop[/green]")
            await _run_cdc_loop(producer, entity)
            producer.close()

    asyncio.run(_run())


if __name__ == "__main__":
    app()
