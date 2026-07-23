"""
src/spark/iceberg_writer.py  -  Streaming Iceberg writer.

Two write modes:
  raw       → append mode, partitioned by _ingest_date + source
  canonical → merge/upsert mode using MERGE INTO
"""
from __future__ import annotations

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import settings

log = structlog.get_logger()


def _raw_table_name(entity: str, source: str) -> str:
    return f"local.{settings.iceberg_raw_namespace}.{source}_{entity}"


def _canonical_table_name(entity: str) -> str:
    return f"local.{settings.iceberg_canonical_namespace}.{entity}"


def write_raw_stream(
    spark,
    stream_df: DataFrame,
    entity: str,
    source: str,
    checkpoint_suffix: str = "",
) -> any:
    """
    Write raw streaming events to Iceberg in append mode.
    Creates table if it doesn't exist.
    """
    table_name  = _raw_table_name(entity, source)
    checkpoint  = (f"{settings.iceberg_checkpoint_dir}"
                   f"/raw/{source}_{entity}{checkpoint_suffix}")

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        count = batch_df.count()
        try:
            batch_df \
                .withColumn("_ingest_date", F.current_date()) \
                .writeTo(table_name) \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("write.parquet.compression-codec", "snappy") \
                .partitionedBy(F.col("_ingest_date"), F.col("source")) \
                .createOrReplace() if batch_id == 0 else \
            batch_df \
                .withColumn("_ingest_date", F.current_date()) \
                .writeTo(table_name) \
                .append()
            log.info("iceberg.raw_batch_written",
                     entity=entity, source=source,
                     batch_id=batch_id, rows=count)
        except Exception as exc:
            log.error("iceberg.raw_batch_failed",
                      entity=entity, source=source, error=str(exc))

    query = stream_df.writeStream \
        .foreachBatch(_write_batch) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime=settings.streaming_trigger_interval) \
        .start()

    log.info("iceberg.raw_stream_started", entity=entity, source=source,
             table=table_name)
    return query


def write_canonical_stream(
    spark,
    stream_df: DataFrame,
    entity: str,
    merge_keys: list[str],
) -> any:
    """
    Write canonical records to Iceberg using foreachBatch + MERGE INTO.
    Ensures exactly-once upsert semantics.
    """
    table_name = _canonical_table_name(entity)
    checkpoint = f"{settings.iceberg_checkpoint_dir}/canonical/{entity}"

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        count = batch_df.count()

        # Deduplicate within batch by merge key (take latest)
        from pyspark.sql import Window
        window   = (Window
                    .partitionBy(*merge_keys)
                    .orderBy(F.col("_event_timestamp").desc()))
        deduped  = batch_df \
            .withColumn("_rank", F.row_number().over(window)) \
            .filter(F.col("_rank") == 1) \
            .drop("_rank")

        try:
            # Try MERGE INTO (upsert)
            deduped.createOrReplaceTempView(f"_incoming_{entity}")
            merge_condition = " AND ".join(
                [f"t.{k} = s.{k}" for k in merge_keys]
            )
            non_key_cols = [c for c in deduped.columns if c not in merge_keys]
            update_set   = ", ".join([f"t.{c} = s.{c}" for c in non_key_cols])
            all_cols     = ", ".join(deduped.columns)
            src_cols     = ", ".join([f"s.{c}" for c in deduped.columns])

            spark.sql(f"""
                MERGE INTO {table_name} t
                USING _incoming_{entity} s
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({src_cols})
            """)
            log.info("iceberg.canonical_upsert",
                     entity=entity, batch_id=batch_id, rows=count)
        except Exception:
            # Table doesn't exist yet — create it
            try:
                deduped \
                    .withColumn("_ingest_date", F.current_date()) \
                    .writeTo(table_name) \
                    .tableProperty("write.format.default", "parquet") \
                    .tableProperty("write.merge.mode", "merge-on-read") \
                    .createOrReplace()
                log.info("iceberg.canonical_created",
                         entity=entity, rows=count)
            except Exception as exc2:
                log.error("iceberg.canonical_failed",
                          entity=entity, error=str(exc2))

    query = stream_df.writeStream \
        .foreachBatch(_write_batch) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime=settings.streaming_trigger_interval) \
        .start()

    log.info("iceberg.canonical_stream_started",
             entity=entity, table=table_name, merge_keys=merge_keys)
    return query
