"""
src/graph/nodes/schema_discovery.py  -  Schema discovery for streaming pipeline.
Reuses the same discovery logic as the batch pipeline.
"""
from __future__ import annotations

import json
import structlog
from typing import Any

from langchain_core.tools import BaseTool

from src.graph.state import StreamingPipelineState, SourceSchema, ColumnInfo
from src.config import settings

log = structlog.get_logger()


def _parse_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list) and raw and isinstance(raw[0], dict) and "text" in raw[0]:
        rows = []
        for block in raw:
            try:
                rows.append(json.loads(block["text"]))
            except Exception:
                pass
        return rows
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if isinstance(raw, list):
        return raw
    for key in ("rows", "result", "data", "records", "value"):
        if isinstance(raw, dict) and key in raw:
            return raw[key]
    return []


def _parse_columns(rows: list[dict]) -> list[ColumnInfo]:
    return [
        ColumnInfo(
            name=r.get("name", r.get("column_name", "")),
            data_type=r.get("data_type", "unknown"),
            nullable=bool(r.get("nullable", True)),
            is_primary_key=bool(r.get("is_primary_key", False)),
        )
        for r in [{k.lower(): v for k, v in row.items()} for row in rows]
    ]


async def _pg_query(tools: list[BaseTool], sql: str) -> list[dict]:
    from src.mcp.client import get_tool
    connect_tool = get_tool(tools, "connect")
    query_tool   = get_tool(tools, "pg_query")
    if not connect_tool or not query_tool:
        raise RuntimeError("pg-mcp tools not found")
    conn_result = await connect_tool.ainvoke({
        "connection_string": settings.postgres_connection_string
    })
    if isinstance(conn_result, list):
        conn_result = json.loads(conn_result[0]["text"])
    elif isinstance(conn_result, str):
        conn_result = json.loads(conn_result)
    conn_id = conn_result.get("conn_id")
    raw = await query_tool.ainvoke({"conn_id": conn_id, "query": sql})
    return _parse_rows(raw)


async def schema_discovery_node(
    state: StreamingPipelineState,
    *,
    pg_tools: list[BaseTool] = [],
    sql_tools: list[BaseTool] = [],
    td_tools: list[BaseTool] = [],
) -> dict:
    log.info("schema_discovery.start")
    errors = list(state.get("errors", []))
    postgres_schemas: dict[str, SourceSchema]  = {}
    sqlserver_schemas: dict[str, SourceSchema] = {}
    teradata_schemas: dict[str, SourceSchema]  = {}

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    if pg_tools:
        try:
            schema     = settings.postgres_schema
            table_rows = await _pg_query(pg_tools, f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [r.get("table_name", "") for r in table_rows if r.get("table_name")]
            for table in tables:
                col_rows = await _pg_query(pg_tools, f"""
                    SELECT column_name AS name, data_type,
                        (is_nullable = 'YES') AS nullable
                    FROM information_schema.columns
                    WHERE table_name = '{table}' AND table_schema = '{schema}'
                    ORDER BY ordinal_position;
                """)
                postgres_schemas[table] = SourceSchema(
                    source="postgres", platform="postgres",
                    database="customers_db", table=table,
                    columns=_parse_columns(col_rows), row_count=None,
                )
            log.info("schema_discovery.pg_done", tables=tables)
        except Exception as exc:
            errors.append(f"Postgres discovery failed: {exc}")
            log.exception("schema_discovery.pg_error")

    # ── SQL Server via DAB REST ───────────────────────────────────────────────
    try:
        from src.mcp.dab_client import fetch_all_dab_schemas
        dab_results = await fetch_all_dab_schemas()
        for entity, (schema_obj, _) in dab_results.items():
            sqlserver_schemas[entity] = schema_obj
        log.info("schema_discovery.sql_done",
                 entities=list(sqlserver_schemas.keys()))
    except Exception as exc:
        errors.append(f"SQL Server discovery failed: {exc}")
        log.warning("schema_discovery.sql_error", error=str(exc))

    # ── Teradata via MCP or direct driver ─────────────────────────────────────
    if td_tools:
        try:
            from src.mcp.client import find_tool_fuzzy
            exec_tool = find_tool_fuzzy(
                td_tools, "base_readQuery", "execute_query", "query"
            )
            if exec_tool:
                db  = settings.teradata_database
                raw = await exec_tool.ainvoke({"query": f"""
                    SELECT TableName AS table_name
                    FROM DBC.TablesV
                    WHERE DatabaseName = '{db}' AND TableKind = 'T'
                """})
                rows   = _parse_rows(raw)
                tables = [r.get("table_name", "").strip()
                          for r in rows if r.get("table_name")]
                for table in tables:
                    raw_cols = await exec_tool.ainvoke({"query": f"""
                        SELECT ColumnName AS name, TRIM(ColumnType) AS data_type,
                            CASE Nullable WHEN 'Y' THEN 1 ELSE 0 END AS nullable
                        FROM DBC.ColumnsV
                        WHERE DatabaseName = '{db}' AND TableName = '{table}'
                        ORDER BY ColumnId
                    """})
                    teradata_schemas[table] = SourceSchema(
                        source="teradata", platform="teradata",
                        database=db, table=table,
                        columns=_parse_columns(_parse_rows(raw_cols)),
                        row_count=None,
                    )
                log.info("schema_discovery.td_mcp_done", tables=tables)
        except Exception as exc:
            errors.append(f"Teradata MCP discovery failed: {exc}")
    elif settings.teradata_host:
        try:
            from src.teradata.client import fetch_all_schemas
            teradata_schemas = await fetch_all_schemas()
            log.info("schema_discovery.td_driver_done",
                     tables=list(teradata_schemas.keys()))
        except Exception as exc:
            errors.append(f"Teradata discovery failed: {exc}")

    has_schemas = any([postgres_schemas, sqlserver_schemas, teradata_schemas])
    return {
        "phase":             "entity_matcher" if has_schemas else "error",
        "postgres_schemas":  postgres_schemas,
        "sqlserver_schemas": sqlserver_schemas,
        "teradata_schemas":  teradata_schemas,
        "errors":            errors,
    }
