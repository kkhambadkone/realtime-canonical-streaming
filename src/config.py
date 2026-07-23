"""
src/config.py  -  Real-time streaming pipeline configuration.

Architecture:
  Sources (Postgres/SQL Server/Teradata)
      ↓ CDC events
  Kafka (existing DataHub Kafka on port 9092)
      ↓
  Spark Structured Streaming (local[*])
      ↓
  Iceberg raw (append, streaming)
      ↓
  Canonical transformation (LLM-generated mappings)
      ↓
  Iceberg canonical (merge/upsert)
      ↓
  Snowflake (micro-batch COPY INTO every 60s)
      ↓
  DataHub (lineage per batch)
"""
from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── LLM ──────────────────────────────────────────────────────────────────
    llm_provider: str = "ollama"
    llm_model: str = "qwen2.5"
    ollama_base_url: str = "http://localhost:11434"
    anthropic_api_key: str = ""
    openai_api_key: str = ""

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prefix: str = "canonical"          # topics: canonical.customer, canonical.order
    kafka_consumer_group: str = "canonical-pipeline"
    kafka_auto_offset_reset: str = "latest"         # latest | earliest
    kafka_num_partitions: int = 3
    kafka_replication_factor: int = 1

    # ── Spark ─────────────────────────────────────────────────────────────────
    spark_master: str = "local[*]"
    spark_app_name: str = "realtime-canonical-pipeline"
    spark_driver_memory: str = "4g"
    spark_executor_memory: str = "4g"
    spark_shuffle_partitions: int = 4

    # ── Iceberg ───────────────────────────────────────────────────────────────
    iceberg_warehouse: str = "file:///tmp/streaming_iceberg_warehouse"
    iceberg_raw_namespace: str = "raw_stream"
    iceberg_canonical_namespace: str = "canonical_stream"
    iceberg_checkpoint_dir: str = "/tmp/streaming_checkpoints"

    # ── Streaming ─────────────────────────────────────────────────────────────
    micro_batch_interval_seconds: int = 60          # how often to write to Snowflake
    streaming_trigger_interval: str = "30 seconds"  # Spark trigger interval
    max_offsets_per_trigger: int = 10000            # max records per micro-batch

    # ── Snowflake ─────────────────────────────────────────────────────────────
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_authenticator: str = "snowflake"
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "CANONICAL_DB"
    snowflake_schema: str = "CANONICAL_STREAM"
    snowflake_role: str = "ACCOUNTADMIN"

    # ── DataHub ───────────────────────────────────────────────────────────────
    datahub_gms_url: str = "http://localhost:8095"
    datahub_token: str = ""
    datahub_env: str = "DEV"

    # ── MCP servers ───────────────────────────────────────────────────────────
    postgres_mcp_url: str = "http://localhost:8765/sse"
    datahub_mcp_url: str = "http://localhost:5012/sse"
    teradata_mcp_url: str = "http://localhost:8767/mcp/"

    # ── Postgres (source) ─────────────────────────────────────────────────────
    postgres_connection_string: str = "postgresql://postgres@localhost:5432/customers_db"
    postgres_schema: str = "public"

    # ── SQL Server (source via DAB REST) ──────────────────────────────────────
    dab_base_url: str = "http://localhost:5000"
    dab_entities: list[str] = ["customers", "products", "orders", "order_items"]

    # ── Teradata (source) ─────────────────────────────────────────────────────
    teradata_host: str = ""
    teradata_user: str = ""
    teradata_password: str = ""
    teradata_database: str = "demo_user"
    teradata_enabled: bool = True

    # ── CDC polling ───────────────────────────────────────────────────────────
    cdc_poll_interval_seconds: int = 10    # how often to poll sources for changes
    cdc_batch_size: int = 100              # max records per CDC poll

    # ── Pipeline ──────────────────────────────────────────────────────────────
    pipeline_log_level: str = "INFO"
    entities: list[str] = ["customer", "order", "product"]

    @property
    def kafka_topic(self) -> dict[str, str]:
        """Map canonical entity name to Kafka topic name."""
        return {
            entity: f"{self.kafka_topic_prefix}.{entity}"
            for entity in self.entities
        }

    @property
    def snowflake_conn_params(self) -> dict:
        return {
            "account":       self.snowflake_account,
            "user":          self.snowflake_user,
            "password":      self.snowflake_password,
            "authenticator": self.snowflake_authenticator,
            "role":          self.snowflake_role,
            "warehouse":     self.snowflake_warehouse,
            "database":      self.snowflake_database,
            "schema":        self.snowflake_schema,
        }


settings = Settings()
