from __future__ import annotations

from typing import Any

from producers.config import ProducerConfig
from producers.event_router import RoutedEvent, utc_now_iso
from producers.error_context import UNKNOWN_SOURCE_TOPIC, stringify_raw_payload
from producers.kafka_producer import MarketKafkaProducer


VALIDATION_ERROR = "VALIDATION_ERROR"
JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
SCHEMA_ERROR = "SCHEMA_ERROR"
ROUTING_ERROR = "ROUTING_ERROR"
PRODUCER_ERROR = "PRODUCER_ERROR"


class DlqPublisher:
    def __init__(self, config: ProducerConfig, kafka_producer: MarketKafkaProducer) -> None:
        self._config = config
        self._kafka_producer = kafka_producer

    async def publish_invalid_event(
        self,
        *,
        raw_payload: str | bytes,
        source_topic: str,
        error_type: str,
        error_message: str,
        symbol: str | None = None,
    ) -> None:
        payload = build_invalid_event(
            config=self._config,
            raw_payload=raw_payload,
            source_topic=source_topic,
            error_type=error_type,
            error_message=error_message,
            symbol=symbol,
        )
        key = source_topic if source_topic != UNKNOWN_SOURCE_TOPIC else error_type
        await self._kafka_producer.publish(
            RoutedEvent(
                topic=self._config.topic_events_invalid,
                key=key,
                value=payload,
                event_type="invalid",
                headers=(
                    ("source", self._config.source.encode("utf-8")),
                    ("schema_version", self._config.schema_version.encode("utf-8")),
                    ("event_type", b"invalid"),
                    ("error_type", error_type.encode("utf-8")),
                ),
            )
        )


def build_invalid_event(
    *,
    config: ProducerConfig,
    raw_payload: str | bytes,
    source_topic: str,
    error_type: str,
    error_message: str,
    symbol: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "schema_version": config.schema_version,
        "source_topic": source_topic,
        "error_type": error_type,
        "error_message": error_message,
        "raw_payload": stringify_raw_payload(raw_payload),
        "error_time": utc_now_iso(),
    }
    if symbol:
        event["symbol"] = symbol
    return event
