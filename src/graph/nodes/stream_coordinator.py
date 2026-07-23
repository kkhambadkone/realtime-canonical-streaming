"""
src/graph/nodes/stream_coordinator.py

Starts all Spark Structured Streaming queries after schema discovery
and canonical model generation.

For each entity:
  1. Create Kafka topic
  2. Start raw stream: Kafka → Iceberg raw
  3. Start canonical stream: Kafka → transform → Iceberg canonical
  4. Start Snowflake stream: Kafka → transform → Snowflake (micro-batch)
"""
from __future__ import annotations

import structlog
from typing import Any

from src.graph.state import StreamingPipelineState
from src.config import settings
from src.spark.session import get_spark
from src.spark.kafka_reader import read_stream
from src.spark.transformer import load_mappings, apply_canonical_mapping, expand_canonical_fields
from src.spark.iceberg_writer import write_raw_stream, write_canonical_stream
from src.spark.snowflake_writer import write_canonical_to_snowflake
from src.kafka.topics import create_topics

log = structlog.get_logger()

# Active streaming queries — kept in memory for monitoring
_active_queries: dict[str, Any] = {}


async def stream_coordinator_node(
    state: StreamingPipelineState,
) -> dict:
    log.info("stream_coordinator.start")
    errors           = list(state.get("errors", []))
    entity_canonicals = state.get("entity_canonicals", {})
    entity_matches   = state.get("entity_matches", [])
    streaming_queries: dict[str, str] = {}

    if not entity_canonicals:
        errors.append("stream_coordinator: no canonical models — run schema discovery first")
        return {"phase": "error", "errors": errors}

    # Load mappings into transformer cache
    load_mappings(entity_canonicals)

    # Create Kafka topics
    entities = list(entity_canonicals.keys())
    try:
        create_topics(entities)
        log.info("stream_coordinator.topics_created", entities=entities)
    except Exception as exc:
        log.warning("stream_coordinator.topics_failed", error=str(exc))

    spark = get_spark()

    for canonical_name, ec in entity_canonicals.items():
        match = next(
            (m for m in entity_matches
             if m["canonical_name"] == canonical_name), None
        )
        if not match:
            continue

        # Get merge keys from canonical schema (non-nullable ID fields)
        merge_keys = ec.get("merge_keys") or [
            f["name"] for f in ec["canonical_schema"]
            if not f.get("nullable", True) and f["name"].endswith("_id")
        ] or ["_event_id"]

        sources = {
            "postgres":  match.get("postgres_table"),
            "sqlserver": match.get("sqlserver_entity"),
            "teradata":  match.get("teradata_table"),
        }
        active_sources = {s: t for s, t in sources.items() if t}

        try:
            # ── Raw stream: Kafka → Iceberg raw ──────────────────────────────
            raw_df = read_stream(spark, canonical_name)

            raw_query = write_raw_stream(
                spark=spark,
                stream_df=raw_df,
                entity=canonical_name,
                source="all",
            )
            _active_queries[f"raw_{canonical_name}"] = raw_query
            streaming_queries[f"raw_{canonical_name}"] = "started"

            # ── Canonical stream: Kafka → transform → Iceberg canonical ──────
            canonical_df = apply_canonical_mapping(raw_df, canonical_name)
            expanded_df  = expand_canonical_fields(canonical_df, canonical_name)

            canonical_query = write_canonical_stream(
                spark=spark,
                stream_df=expanded_df,
                entity=canonical_name,
                merge_keys=merge_keys,
            )
            _active_queries[f"canonical_{canonical_name}"] = canonical_query
            streaming_queries[f"canonical_{canonical_name}"] = "started"

            # ── Snowflake stream: Kafka → transform → Snowflake (60s) ────────
            if settings.snowflake_account:
                sf_query = write_canonical_to_snowflake(
                    stream_df=expanded_df,
                    entity=canonical_name,
                )
                _active_queries[f"snowflake_{canonical_name}"] = sf_query
                streaming_queries[f"snowflake_{canonical_name}"] = "started"

            log.info("stream_coordinator.entity_started",
                     entity=canonical_name,
                     sources=list(active_sources.keys()),
                     merge_keys=merge_keys,
                     queries=3 if settings.snowflake_account else 2)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            errors.append(f"Stream start failed for {canonical_name}: {exc}")
            log.exception("stream_coordinator.entity_failed",
                          entity=canonical_name)

    return {
        "phase":             "streaming",
        "streaming_queries": streaming_queries,
        "kafka_topics_created": entities,
        "errors":            errors,
    }


def get_active_queries() -> dict[str, Any]:
    """Return all active streaming queries for monitoring."""
    return _active_queries


def await_all_queries() -> None:
    """Block until all streaming queries terminate."""
    for name, query in _active_queries.items():
        try:
            log.info("stream_coordinator.awaiting", query=name)
            query.awaitTermination()
        except Exception as exc:
            log.error("stream_coordinator.query_failed",
                      query=name, error=str(exc))


def stop_all_queries() -> None:
    """Gracefully stop all streaming queries."""
    for name, query in _active_queries.items():
        try:
            query.stop()
            log.info("stream_coordinator.query_stopped", query=name)
        except Exception as exc:
            log.warning("stream_coordinator.stop_failed",
                        query=name, error=str(exc))
