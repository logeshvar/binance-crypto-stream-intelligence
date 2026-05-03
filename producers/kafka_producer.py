from __future__ import annotations

import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from producers.config import ProducerConfig
from producers.event_router import RoutedEvent


logger = logging.getLogger(__name__)


def _json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _key_serializer(value: str) -> bytes:
    return value.encode("utf-8")


class MarketKafkaProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._producer: AIOKafkaProducer | None = None
        self._published_count = 0

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            client_id=self._config.client_id,
            acks=self._config.kafka_acks,
            linger_ms=self._config.kafka_linger_ms,
            request_timeout_ms=self._config.kafka_request_timeout_ms,
            key_serializer=_key_serializer,
            value_serializer=_json_serializer,
        )
        self._producer = producer
        try:
            await producer.start()
        except Exception:
            await producer.stop()
            self._producer = None
            raise

        logger.info(
            "kafka_producer_started",
            extra={
                "bootstrap_servers": self._config.bootstrap_servers,
                "client_id": self._config.client_id,
            },
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            logger.info("kafka_producer_stopped", extra={"published_count": self._published_count})

    async def publish(self, event: RoutedEvent) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer has not been started")

        metadata = await self._producer.send_and_wait(
            topic=event.topic,
            key=event.key,
            value=event.value,
            headers=list(event.headers),
        )
        self._published_count += 1

        if self._published_count % self._config.publish_log_interval == 0:
            logger.info(
                "messages_published",
                extra={
                    "published_count": self._published_count,
                    "topic": event.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "event_type": event.event_type,
                },
            )
