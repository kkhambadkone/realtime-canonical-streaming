"""
src/graph/nodes/canonical_builder.py  -  LLM canonical model generation for streaming.
"""
from __future__ import annotations

import json
import structlog
from typing import Any
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage
from src.graph.state import StreamingPipelineState, EntityCanonical

log = structlog.get_logger()

SYSTEM_PROMPT = """You are a senior data architect specialising in canonical data modelling.
Given source schemas, produce a canonical schema and field mappings.
Also identify the primary key field(s) to use as merge_keys for upserts.

Respond ONLY with valid JSON:
{
  "canonical_schema": [
    {"name": "customer_id", "data_type": "integer", "nullable": false, "pii": false}
  ],
  "pg_mappings":  [{"source": "postgres",  "source_field": "id", "canonical_field": "customer_id", "transform": null}],
  "sql_mappings": [{"source": "sqlserver", "source_field": "id", "canonical_field": "customer_id", "transform": null}],
  "td_mappings":  [{"source": "teradata",  "source_field": "CUST_ID", "canonical_field": "customer_id", "transform": null}],
  "merge_keys":   ["customer_id"]
}"""


def _schema_to_text(schema) -> str:
    lines = [f"Source: {schema['source']}  Table: {schema['database']}.{schema['table']}"]
    for col in schema["columns"]:
        pk   = " [PK]" if col.get("is_primary_key") else ""
        null = "NULL" if col.get("nullable") else "NOT NULL"
        lines.append(f"  {col['name']}  {col['data_type']}  {null}{pk}")
    return "\n".join(lines)


async def canonical_builder_node(
    state: StreamingPipelineState,
    *,
    llm: BaseChatModel,
) -> dict:
    log.info("canonical_builder.start")
    errors            = list(state.get("errors", []))
    entity_matches    = state.get("entity_matches", [])
    postgres_schemas  = state.get("postgres_schemas", {})
    sqlserver_schemas = state.get("sqlserver_schemas", {})
    teradata_schemas  = state.get("teradata_schemas", {})
    entity_canonicals: dict[str, EntityCanonical] = {}

    for match in entity_matches:
        canonical_name = match["canonical_name"]

        pg_schema  = postgres_schemas.get(match.get("postgres_table") or "")
        sql_schema = sqlserver_schemas.get(match.get("sqlserver_entity") or "")
        td_schema  = teradata_schemas.get(match.get("teradata_table") or "")

        available = [s for s in [pg_schema, sql_schema, td_schema] if s]
        if not available:
            errors.append(f"canonical_builder: no schema for {canonical_name}")
            continue

        schema_text = "\n\n".join(_schema_to_text(s) for s in available)
        prompt = (
            f"Entity: {canonical_name}\n\n{schema_text}\n\n"
            "Produce canonical schema, field mappings, and merge_keys. Return ONLY JSON."
        )

        try:
            response = await llm.ainvoke([
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ])
            raw = response.content.strip()
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            start = raw.find("{")
            end   = raw.rfind("}") + 1
            if start >= 0 and end > start:
                raw = raw[start:end]
            parsed: dict[str, Any] = json.loads(raw)
        except Exception as exc:
            errors.append(f"canonical_builder failed for {canonical_name}: {exc}")
            log.exception("canonical_builder.error", entity=canonical_name)
            continue

        entity_canonicals[canonical_name] = EntityCanonical(
            canonical_name=canonical_name,
            canonical_schema=parsed.get("canonical_schema", []),
            pg_mappings=parsed.get("pg_mappings", []),
            sql_mappings=parsed.get("sql_mappings", []),
            td_mappings=parsed.get("td_mappings", []),
            merge_keys=parsed.get("merge_keys", [f"{canonical_name}_id"]),
        )
        log.info("canonical_builder.done",
                 entity=canonical_name,
                 fields=len(parsed.get("canonical_schema", [])),
                 merge_keys=parsed.get("merge_keys", []))

    return {
        "phase":             "stream_coordinator",
        "entity_canonicals": entity_canonicals,
        "errors":            errors,
    }
