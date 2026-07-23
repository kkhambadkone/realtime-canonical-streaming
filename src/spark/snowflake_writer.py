"""
src/spark/snowflake_writer.py  -  Micro-batch Snowflake writer.

Writes canonical records to Snowflake every N seconds using foreachBatch.
Uses write_pandas for simplicity — switches to COPY INTO for large volumes.
"""
from __future__ import annotations

import structlog
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import settings

log = structlog.get_logger()


def _coerce_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce pandas DataFrame types for Snowflake compatibility."""
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_datetime(df[col], errors="ignore")
            except Exception:
                pass
    return df


_schema_ensured = False


def _ensure_schema_exists() -> None:
    """
    Create the target Snowflake database/schema if they don't exist yet.
    write_pandas(auto_create_table=True) only auto-creates the TABLE —
    it will not create the schema, so the first write against a fresh
    database fails with a SQL compilation error ("schema not created yet").
    Runs once per process (module-level flag), not once per micro-batch.
    """
    global _schema_ensured
    if _schema_ensured:
        return

    import snowflake.connector
    conn = snowflake.connector.connect(**{
        k: v for k, v in settings.snowflake_conn_params.items()
        if k not in ("database", "schema")
    })
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS {settings.snowflake_database}"
        )
        cur.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{settings.snowflake_database}.{settings.snowflake_schema}"
        )
        log.info("snowflake.schema_ensured",
                 database=settings.snowflake_database,
                 schema=settings.snowflake_schema)
    finally:
        conn.close()

    _schema_ensured = True


def write_canonical_to_snowflake(
    stream_df: DataFrame,
    entity: str,
    batch_interval_seconds: int | None = None,
) -> any:
    """
    Write canonical streaming DataFrame to Snowflake as micro-batches.
    Triggered every N seconds (default: settings.micro_batch_interval_seconds).
    """
    interval   = batch_interval_seconds or settings.micro_batch_interval_seconds
    table_name = f"STREAM_CANONICAL_{entity.upper()}"
    checkpoint = f"{settings.iceberg_checkpoint_dir}/snowflake/{entity}"

    _ensure_schema_exists()

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        count = batch_df.count()
        log.info("snowflake.micro_batch_start",
                 entity=entity, batch_id=batch_id, rows=count)

        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas

            pdf = _coerce_df(batch_df.toPandas())

            # Add batch metadata
            pdf["_batch_id"]        = batch_id
            pdf["_batch_timestamp"] = datetime.now(timezone.utc).isoformat()

            conn = snowflake.connector.connect(**settings.snowflake_conn_params)
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=pdf,
                table_name=table_name,
                database=settings.snowflake_database,
                schema=settings.snowflake_schema,
                auto_create_table=True,
                overwrite=False,       # append mode for streaming
                quote_identifiers=False,
            )
            conn.close()

            if success:
                log.info("snowflake.micro_batch_done",
                         entity=entity, batch_id=batch_id,
                         rows=nrows, chunks=nchunks)
            else:
                log.error("snowflake.micro_batch_failed",
                          entity=entity, batch_id=batch_id)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            log.error("snowflake.micro_batch_error",
                      entity=entity, batch_id=batch_id, error=str(exc))

    query = stream_df.writeStream \
        .foreachBatch(_write_batch) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime=f"{interval} seconds") \
        .start()

    log.info("snowflake.stream_started",
             entity=entity, table=table_name, interval_s=interval)
    return query
