"""
src/spark/session.py  -  SparkSession factory for streaming pipeline.

Configures:
  - Kafka connector (spark-sql-kafka)
  - Iceberg extensions
  - Local[*] mode for development
"""
from __future__ import annotations

import os
import structlog
from functools import lru_cache

from src.config import settings

log = structlog.get_logger()

# Spark package coordinates
SPARK_PACKAGES = ",".join([
    # Kafka connector for Spark 3.5 / Scala 2.12
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    # Iceberg for Spark 3.5 / Scala 2.12
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2",
    # Snowflake connector for Spark 3.5 / Scala 2.12
    "net.snowflake:spark-snowflake_2.12:2.14.0-spark_3.4",
    "net.snowflake:snowflake-jdbc:3.14.4",
])


@lru_cache(maxsize=1)
def get_spark():
    """Get or create a SparkSession configured for streaming."""
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages {SPARK_PACKAGES} pyspark-shell"
    )

    warehouse = settings.iceberg_warehouse


    spark = SparkSession.builder \
       .master(settings.spark_master) \
       .appName(settings.spark_app_name) \
       .config("spark.driver.memory", settings.spark_driver_memory) \
       .config("spark.sql.shuffle.partitions",
            str(settings.spark_shuffle_partitions)) \
       .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
       .config("spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog") \
       .config("spark.sql.catalog.local.type", "hadoop") \
       .config("spark.sql.catalog.local.warehouse", warehouse) \
       .config("spark.sql.defaultCatalog", "local") \
       .config("spark.streaming.stopGracefullyOnShutdown", "true") \
       .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info("spark.session_created",
             master=settings.spark_master,
             version=spark.version,
             warehouse=warehouse)
    return spark


def stop_spark() -> None:
    try:
        get_spark().stop()
        log.info("spark.stopped")
    except Exception:
        pass
