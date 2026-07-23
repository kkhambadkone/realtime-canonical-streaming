"""
src/graph/state.py  -  LangGraph state for streaming pipeline.

The streaming pipeline uses LangGraph only for the startup phase:
  1. Schema discovery (Postgres, SQL Server, Teradata)
  2. Entity matching (LLM)
  3. Canonical model generation (LLM)
  4. Mapping cache loaded into transformer
  5. Kafka topics created
  6. Spark streaming queries started

After startup, Spark Structured Streaming takes over.
"""
from __future__ import annotations
from typing import Annotated, Any
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class ColumnInfo(TypedDict, total=False):
    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool


class SourceSchema(TypedDict):
    source: str
    platform: str
    database: str
    table: str
    columns: list[ColumnInfo]
    row_count: int | None


class EntityMatch(TypedDict):
    canonical_name: str
    postgres_table: str | None
    sqlserver_entity: str | None
    teradata_table: str | None


class CanonicalField(TypedDict, total=False):
    name: str
    data_type: str
    nullable: bool
    description: str
    pii: bool


class FieldMapping(TypedDict):
    source: str
    source_field: str
    canonical_field: str
    transform: str | None


class EntityCanonical(TypedDict):
    canonical_name: str
    canonical_schema: list[CanonicalField]
    pg_mappings: list[FieldMapping]
    sql_mappings: list[FieldMapping]
    td_mappings: list[FieldMapping]
    merge_keys: list[str]


class StreamingPipelineState(TypedDict):
    # ── Schema discovery ──────────────────────────────────────────────────────
    postgres_schemas: dict[str, SourceSchema]
    sqlserver_schemas: dict[str, SourceSchema]
    teradata_schemas: dict[str, SourceSchema]

    # ── Entity matching ───────────────────────────────────────────────────────
    entity_matches: list[EntityMatch]

    # ── Canonical model ───────────────────────────────────────────────────────
    entity_canonicals: dict[str, EntityCanonical]

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_topics_created: list[str]

    # ── Streaming queries ─────────────────────────────────────────────────────
    streaming_queries: dict[str, str]   # query_name → status

    # ── Control ───────────────────────────────────────────────────────────────
    phase: str
    errors: list[str]
    messages: Annotated[list[BaseMessage], add_messages]


def initial_state() -> StreamingPipelineState:
    return StreamingPipelineState(
        postgres_schemas={}, sqlserver_schemas={}, teradata_schemas={},
        entity_matches=[], entity_canonicals={},
        kafka_topics_created=[], streaming_queries={},
        phase="schema_discovery", errors=[], messages=[],
    )
