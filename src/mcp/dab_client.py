"""
src/mcp/dab_client.py  -  DAB REST client for SQL Server schema discovery.
"""
from __future__ import annotations

import structlog
import aiohttp
from typing import Any
from src.config import settings
from src.graph.state import SourceSchema, ColumnInfo

log = structlog.get_logger()


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):   return "boolean"
    if isinstance(value, int):    return "integer"
    if isinstance(value, float):  return "float"
    if isinstance(value, str):
        if len(value) == 10 and value[4] == "-": return "date"
        return "string"
    return "string"


def _infer_columns(records: list[dict]) -> list[ColumnInfo]:
    if not records:
        return []
    return [
        ColumnInfo(name=col, data_type=_infer_type(records[0][col]),
                   nullable=True, is_primary_key=(col == "id"))
        for col in records[0]
    ]


async def fetch_dab_records(entity: str) -> list[dict]:
    url = f"{settings.dab_base_url}/api/{entity}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("value", data) if isinstance(data, dict) else data


async def fetch_all_dab_schemas() -> dict[str, tuple[SourceSchema, list[dict]]]:
    result = {}
    for entity in settings.dab_entities:
        try:
            records = await fetch_dab_records(entity)
            columns = _infer_columns(records)
            schema  = SourceSchema(
                source="sqlserver", platform="mssql",
                database="CRM_DB", table=f"dbo.{entity}",
                columns=columns, row_count=len(records),
            )
            result[entity] = (schema, records)
            log.info("dab.schema_done", entity=entity, rows=len(records))
        except Exception as exc:
            log.error("dab.entity_failed", entity=entity, error=str(exc))
    return result
