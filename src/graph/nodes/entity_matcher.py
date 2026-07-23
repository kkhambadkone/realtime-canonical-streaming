"""
src/graph/nodes/entity_matcher.py  -  LLM entity matching for streaming pipeline.
"""
from __future__ import annotations

import json
import structlog
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage, HumanMessage
from src.graph.state import StreamingPipelineState, EntityMatch

log = structlog.get_logger()

SYSTEM_PROMPT = """You are a senior data architect.
Match tables across PostgreSQL, SQL Server, and Teradata representing the same business entity.
Assign a clean canonical_name (singular snake_case).

Respond ONLY with valid JSON:
{
  "entity_matches": [
    {
      "canonical_name": "customer",
      "postgres_table": "customer",
      "sqlserver_entity": "customers",
      "teradata_table": "CUSTOMER"
    }
  ]
}"""


async def entity_matcher_node(
    state: StreamingPipelineState,
    *,
    llm: BaseChatModel,
) -> dict:
    log.info("entity_matcher.start")
    errors = list(state.get("errors", []))

    pg_tables    = list(state.get("postgres_schemas", {}).keys())
    sql_entities = list(state.get("sqlserver_schemas", {}).keys())
    td_tables    = list(state.get("teradata_schemas", {}).keys())

    if not any([pg_tables, sql_entities, td_tables]):
        errors.append("entity_matcher: no tables found")
        return {"phase": "error", "errors": errors}

    user_msg = (
        f"PostgreSQL tables:         {pg_tables or 'none'}\n"
        f"SQL Server entities:       {sql_entities or 'none'}\n"
        f"Teradata tables:           {td_tables or 'none'}\n\n"
        "Match entities across all sources."
    )

    try:
        response = await llm.ainvoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_msg),
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
        parsed = json.loads(raw)
        entity_matches: list[EntityMatch] = parsed.get("entity_matches", [])
        log.info("entity_matcher.done",
                 matches=len(entity_matches),
                 entities=[m["canonical_name"] for m in entity_matches])
    except Exception as exc:
        errors.append(f"entity_matcher LLM failed: {exc}")
        log.exception("entity_matcher.error")
        return {"phase": "error", "errors": errors}

    return {
        "phase":          "canonical_builder",
        "entity_matches": entity_matches,
        "errors":         errors,
    }
