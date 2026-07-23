"""
src/spark/kafka_reader.py  -  Spark Structured Streaming Kafka source.

Reads from canonical Kafka topics and returns a streaming DataFrame.

Event envelope (JSON):
{
  "event_id":   "uuid",
  "event_type": "INSERT|UPDATE|DELETE|SNAPSHOT",
  "source":     "postgres|sqlserver|teradata",
  "entity":     "customer|order|product",
  "timestamp":  "2024-01-01T00:00:00Z",
  "payload":    { ...source record fields... },
  "metadata":   { "table": "...", "database": "..." }
}
"""
from __future__ import annotations

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.config import settings

log = structlog.get_logger()

# Schema for the Kafka event envelope
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id",   T.StringType(),  True),
    T.StructField("event_type", T.StringType(),  True),
    T.StructField("source",     T.StringType(),  True),
    T.StructField("entity",     T.StringType(),  True),
    T.StructField("timestamp",  T.StringType(),  True),
    T.StructField("payload",    T.StringType(),  True),  # JSON string
    T.StructField("before",     T.StringType(),  True),  # JSON string (nullable)
    T.StructField("metadata", T.StructType([
        T.StructField("table",    T.StringType(), True),
        T.StructField("database", T.StringType(), True),
        T.StructField("lsn",      T.StringType(), True),
    ]), True),
])


def read_stream(spark, entity: str) -> DataFrame:
    """
    Create a streaming DataFrame from a Kafka topic for a single entity.

    Returns parsed DataFrame with columns:
      event_id, event_type, source, entity, timestamp, payload (string JSON),
      kafka_offset, kafka_partition, kafka_timestamp
    """
    topic = f"{settings.kafka_topic_prefix}.{entity}"

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", settings.kafka_auto_offset_reset) \
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger) \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse the JSON event envelope
    parsed_df = raw_df \
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
        ) \
        .select(
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.col("kafka_timestamp"),
            F.col("event.event_id"),
            F.col("event.event_type"),
            F.col("event.source"),
            F.col("event.entity"),
            F.col("event.timestamp").alias("event_timestamp"),
            F.col("event.payload"),                          # raw JSON string
            F.col("event.metadata.table").alias("source_table"),
            F.col("event.metadata.database").alias("source_database"),
        )

    log.info("kafka.stream_created", entity=entity, topic=topic)
    return parsed_df


def read_all_streams(spark, entities: list[str]) -> DataFrame:
    """
    Read from multiple Kafka topics and union into one streaming DataFrame.
    Useful for processing all entities in a single streaming query.
    """
    topics = ",".join([
        f"{settings.kafka_topic_prefix}.{entity}"
        for entity in entities
    ])

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
        .option("subscribe", topics) \
        .option("startingOffsets", settings.kafka_auto_offset_reset) \
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger) \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = raw_df \
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
        ) \
        .select(
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.col("kafka_timestamp"),
            F.col("event.event_id"),
            F.col("event.event_type"),
            F.col("event.source"),
            F.col("event.entity"),
            F.col("event.timestamp").alias("event_timestamp"),
            F.col("event.payload"),
            F.col("event.metadata.table").alias("source_table"),
            F.col("event.metadata.database").alias("source_database"),
        )

    log.info("kafka.multi_stream_created", entities=entities, topics=topics)
    return parsed_df
