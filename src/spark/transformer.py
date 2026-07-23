"""
src/spark/transformer.py  -  Canonical transformation for streaming DataFrames.

Applies LLM-generated field mappings to streaming events in real-time.
Mappings are loaded once at startup and cached — no LLM calls per record.

Pipeline:
  raw event (JSON payload) → parse fields → apply mappings → canonical record
"""
from __future__ import annotations

import json
import structlog
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.config import settings

log = structlog.get_logger()


# ── Field mapping cache ───────────────────────────────────────────────────────

_mapping_cache: dict[str, dict[str, Any]] = {}


def load_mappings(entity_canonicals: dict[str, Any]) -> None:
    """
    Load LLM-generated canonical mappings into cache.
    Called once at pipeline startup after LangGraph schema discovery.

    entity_canonicals: {
      "customer": {
        "canonical_schema": [...],
        "pg_mappings": [...],
        "sql_mappings": [...],
        "td_mappings": [...],
      }
    }
    """
    global _mapping_cache
    _mapping_cache = entity_canonicals
    log.info("transformer.mappings_loaded",
             entities=list(entity_canonicals.keys()))


def get_canonical_fields(entity: str) -> list[str]:
    """Get canonical field names for an entity."""
    ec = _mapping_cache.get(entity, {})
    return [f["name"] for f in ec.get("canonical_schema", [])]


def _build_mapping_lut(entity: str, source: str) -> dict[str, tuple[str, str | None]]:
    """Build source_field → (canonical_field, transform) lookup."""
    ec = _mapping_cache.get(entity, {})
    mapping_key = f"{source[:2]}_mappings"  # pg_mappings, sql_mappings, td_mappings
    if source == "sqlserver":
        mapping_key = "sql_mappings"

    lut: dict[str, tuple[str, str | None]] = {}
    for m in ec.get(mapping_key, []):
        src = m.get("source_field", "")
        lut[src]         = (m["canonical_field"], m.get("transform"))
        lut[src.lower()] = (m["canonical_field"], m.get("transform"))
        lut[src.upper()] = (m["canonical_field"], m.get("transform"))
    return lut


# ── Spark UDF for payload transformation ──────────────────────────────────────

def _make_transform_udf(entity: str):
    """
    Create a Spark UDF that transforms a JSON payload string
    to a canonical JSON string using cached mappings.
    """
    # Capture mapping state for UDF closure
    mapping_cache = _mapping_cache.copy()

    def transform_payload(payload_str: str, source: str) -> str | None:
        if not payload_str:
            return None
        try:
            payload = json.loads(payload_str)
            ec      = mapping_cache.get(entity, {})

            # Select mapping set based on source
            if source == "postgres":
                mappings = ec.get("pg_mappings", [])
            elif source == "sqlserver":
                mappings = ec.get("sql_mappings", [])
            else:
                mappings = ec.get("td_mappings", [])

            # Build lookup
            lut: dict[str, tuple[str, str | None]] = {}
            for m in mappings:
                src = m.get("source_field", "")
                val = (m["canonical_field"], m.get("transform"))
                lut[src]         = val
                lut[src.lower()] = val
                lut[src.upper()] = val

            # Apply mappings
            canonical_fields = [f["name"] for f in ec.get("canonical_schema", [])]
            canonical: dict[str, Any] = {f: None for f in canonical_fields}

            for src_field, value in payload.items():
                for key in (src_field, src_field.lower(), src_field.upper()):
                    if key in lut:
                        canon_field, transform = lut[key]
                        # Apply simple transforms
                        if transform:
                            h = transform.upper()
                            if "UPPER" in h:
                                value = str(value).upper() if value else value
                            elif "LOWER" in h:
                                value = str(value).lower() if value else value
                            elif "CAST AS INT" in h:
                                value = int(value) if value is not None else None
                            elif "CAST AS FLOAT" in h:
                                value = float(value) if value is not None else None
                        canonical[canon_field] = value
                        break

            return json.dumps(canonical, default=str)
        except Exception:
            return None

    return F.udf(transform_payload, T.StringType())


def apply_canonical_mapping(
    df: DataFrame,
    entity: str,
) -> DataFrame:
    """
    Apply canonical mappings to a streaming DataFrame.

    Input:  streaming DF with 'payload' (JSON string) and 'source' columns
    Output: streaming DF with 'canonical_payload' (JSON string) added
    """
    transform_udf = _make_transform_udf(entity)

    return df.withColumn(
        "canonical_payload",
        transform_udf(F.col("payload"), F.col("source"))
    )


def expand_canonical_fields(
    df: DataFrame,
    entity: str,
) -> DataFrame:
    """
    Expand canonical_payload JSON string into individual columns.
    Adds _source, _event_type, _event_timestamp, _ingest_date metadata.
    """
    ec     = _mapping_cache.get(entity, {})
    fields = ec.get("canonical_schema", [])

    type_map = {
        "string":    T.StringType(),
        "integer":   T.LongType(),
        "float":     T.DoubleType(),
        "boolean":   T.BooleanType(),
        "date":      T.DateType(),
        "timestamp": T.TimestampType(),
    }

    # Build schema for canonical payload
    canonical_schema = T.StructType([
        T.StructField(
            f["name"],
            type_map.get(f.get("data_type", "string"), T.StringType()),
            True
        )
        for f in fields
    ])

    expanded = df \
        .withColumn("canonical",
                    F.from_json(F.col("canonical_payload"), canonical_schema)) \
        .select(
            # Canonical fields
            *[F.col(f"canonical.{f['name']}").alias(f["name"]) for f in fields],
            # Metadata
            F.col("source").alias("_source"),
            F.col("event_type").alias("_event_type"),
            F.col("event_timestamp").alias("_event_timestamp"),
            F.current_date().alias("_ingest_date"),
            F.lit(True).alias("_is_golden"),
        )

    log.info("transformer.expanded", entity=entity, fields=len(fields))
    return expanded
