"""
main.py  -  Real-time streaming canonical pipeline.

Startup sequence (LangGraph):
  1. Schema discovery — Postgres (MCP), SQL Server (DAB REST), Teradata (MCP)
  2. Entity matching — LLM matches tables across sources
  3. Canonical model — LLM generates unified schema + field mappings
  4. Stream coordinator — creates Kafka topics, starts Spark streaming queries

Continuous streaming (Spark Structured Streaming):
  Kafka → Iceberg raw (append, every 30s)
  Kafka → canonical transform → Iceberg canonical (upsert, every 30s)
  Kafka → canonical transform → Snowflake (append, every 60s)

Usage:
  python main.py              # start streaming pipeline
  python main.py --snapshot   # snapshot existing data then stream
  python main.py --status     # show active streaming query status
"""
from __future__ import annotations

import asyncio
import signal
import structlog
import typer
from rich.console import Console
from rich.table import Table
from rich import print as rprint

from src.config import settings
from src.graph.state import initial_state
from src.graph.supervisor import build_graph
from src.mcp.client import get_mcp_tools
from src.graph.nodes.stream_coordinator import (
    get_active_queries, await_all_queries, stop_all_queries
)

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


def _build_llm():
    if settings.llm_provider == "ollama":
        from langchain_ollama import ChatOllama
        return ChatOllama(model=settings.llm_model,
                          base_url=settings.ollama_base_url, temperature=0)
    elif settings.llm_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=settings.llm_model,
                             api_key=settings.anthropic_api_key, temperature=0)
    else:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=settings.llm_model,
                          api_key=settings.openai_api_key, temperature=0)


def _print_summary(final_state: dict) -> None:
    console.rule("[bold cyan]Streaming Pipeline Started")
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("Item", style="dim")
    t.add_column("Value")
    t.add_row("Phase",            final_state.get("phase", "?"))
    t.add_row("Errors",           str(len(final_state.get("errors", []))))
    t.add_row("Postgres tables",  str(len(final_state.get("postgres_schemas", {}))))
    t.add_row("SQL Server tables",str(len(final_state.get("sqlserver_schemas", {}))))
    t.add_row("Teradata tables",  str(len(final_state.get("teradata_schemas", {}))))
    t.add_row("Entity matches",   str(len(final_state.get("entity_matches", []))))
    t.add_row("Canonical models", str(len(final_state.get("entity_canonicals", {}))))
    t.add_row("Kafka topics",     str(len(final_state.get("kafka_topics_created", []))))
    for qname, status in final_state.get("streaming_queries", {}).items():
        t.add_row(f"  Query: {qname}", status)
    console.print(t)
    if final_state.get("errors"):
        console.rule("[bold red]Errors")
        for err in final_state["errors"]:
            rprint(f"  [red]x[/red] {err}")


async def _run_pipeline(snapshot: bool) -> None:
    log.info("streaming_pipeline.start",
             kafka=settings.kafka_bootstrap_servers,
             spark=settings.spark_master,
             entities=settings.entities)

    # Connect MCP servers
    tools_by_server = await get_mcp_tools()
    pg_tools  = tools_by_server.get("postgres",  [])
    td_tools  = tools_by_server.get("teradata",  [])
    dh_tools  = tools_by_server.get("datahub",   [])

    log.info("streaming_pipeline.mcp_connected",
             pg=len(pg_tools), td=len(td_tools), dh=len(dh_tools))

    llm   = _build_llm()
    graph = build_graph(
        llm=llm,
        pg_tools=pg_tools,
        td_tools=td_tools,
        dh_tools=dh_tools,
    )

    # Run LangGraph startup phase
    state       = initial_state()
    final_state: dict = {}

    async for event in graph.astream(state, stream_mode="updates"):
        node_name  = list(event.keys())[0] if event else "?"
        node_state = list(event.values())[0] if event else {}
        phase      = node_state.get("phase", "?")
        log.info("pipeline.node_complete", node=node_name, next_phase=phase)
        final_state.update(node_state)

    _print_summary(final_state)

    if final_state.get("phase") != "streaming":
        log.error("streaming_pipeline.startup_failed",
                  errors=final_state.get("errors"))
        return

    # Run initial snapshot if requested
    if snapshot:
        log.info("streaming_pipeline.running_snapshot")
        await _run_snapshot(final_state)

    # Handle graceful shutdown
    def _shutdown(signum, frame):
        log.info("streaming_pipeline.shutdown_signal")
        stop_all_queries()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("streaming_pipeline.running",
             note="Press Ctrl+C to stop")
    console.rule("[bold green]Streaming queries active — Ctrl+C to stop")

    # Block until all queries terminate
    await_all_queries()
    log.info("streaming_pipeline.stopped")


async def _run_snapshot(final_state: dict) -> None:
    """Publish all existing source records to Kafka as SNAPSHOT events."""
    from src.kafka.producer import KafkaEventProducer
    entity_matches   = final_state.get("entity_matches", [])
    producer         = KafkaEventProducer()

    for match in entity_matches:
        canonical_name = match["canonical_name"]

        if match.get("postgres_table"):
            await producer.snapshot_postgres(
                {canonical_name: match["postgres_table"]}
            )
        if match.get("sqlserver_entity"):
            await producer.snapshot_sqlserver(
                {canonical_name: match["sqlserver_entity"]}
            )
        if match.get("teradata_table") and settings.teradata_host:
            await producer.snapshot_teradata(
                {canonical_name: match["teradata_table"]}
            )

    producer.close()
    log.info("streaming_pipeline.snapshot_done")


@app.command()
def main(
    snapshot: bool = typer.Option(
        False, "--snapshot",
        help="Publish existing source records before streaming"
    ),
    status: bool = typer.Option(
        False, "--status",
        help="Show active streaming query status"
    ),
) -> None:
    """Real-time streaming canonical pipeline."""
    if status:
        queries = get_active_queries()
        if not queries:
            rprint("[yellow]No active streaming queries[/yellow]")
        else:
            for name, query in queries.items():
                rprint(f"[green]✓[/green] {name}: {query.status}")
        return

    asyncio.run(_run_pipeline(snapshot=snapshot))


if __name__ == "__main__":
    app()
