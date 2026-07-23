"""
src/kafka/topics.py  -  Kafka topic management.

Creates topics per canonical entity:
  canonical.customer
  canonical.order
  canonical.product

Uses the existing DataHub Kafka broker on port 9092.
"""
from __future__ import annotations

import structlog
from typing import Any

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.config import settings

log = structlog.get_logger()


def create_topics(entities: list[str] | None = None) -> None:
    """Create Kafka topics for each canonical entity."""
    entities = entities or settings.entities
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="canonical-admin",
    )

    topics = [
        NewTopic(
            name=f"{settings.kafka_topic_prefix}.{entity}",
            num_partitions=settings.kafka_num_partitions,
            replication_factor=settings.kafka_replication_factor,
        )
        for entity in entities
    ]

    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        for entity in entities:
            log.info("kafka.topic_created",
                     topic=f"{settings.kafka_topic_prefix}.{entity}")
    except TopicAlreadyExistsError:
        log.info("kafka.topics_exist", entities=entities)
    except Exception as exc:
        log.error("kafka.topic_creation_failed", error=str(exc))
    finally:
        admin.close()


def list_topics() -> list[str]:
    """List all canonical topics in Kafka."""
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="canonical-admin",
    )
    try:
        all_topics = admin.list_topics()
        return [t for t in all_topics
                if t.startswith(settings.kafka_topic_prefix)]
    finally:
        admin.close()


def delete_topics(entities: list[str] | None = None) -> None:
    """Delete canonical topics (for cleanup/reset)."""
    entities = entities or settings.entities
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="canonical-admin",
    )
    try:
        topics = [f"{settings.kafka_topic_prefix}.{e}" for e in entities]
        admin.delete_topics(topics=topics)
        log.info("kafka.topics_deleted", topics=topics)
    except Exception as exc:
        log.warning("kafka.delete_failed", error=str(exc))
    finally:
        admin.close()
