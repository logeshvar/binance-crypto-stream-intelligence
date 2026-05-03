from __future__ import annotations

import json
from typing import Any

from producers.config import ProducerConfig
from producers.event_router import RoutedEvent, utc_now_iso
from producers.kafka_producer import MarketKafkaProducer


VALIDATION_ERROR = "VALIDATION_ERROR"
JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
SCHEMA_ERROR = "SCHEMA_ERROR"
ROUTING_ERROR = "ROUTING_ERROR"
PRODUCER_ERROR = "PRODUCER_ERROR"
UNKNOWN_SOURCE_TOPIC = "unknown"


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


def stringify_raw_payload(raw_payload: str | bytes) -> str:
    if isinstance(raw_payload, bytes):
        return raw_payload.decode("utf-8", errors="replace")
    return raw_payload


def infer_source_topic(raw_payload: str | bytes, config: ProducerConfig) -> str:
    try:
        envelope = json.loads(stringify_raw_payload(raw_payload))
    except json.JSONDecodeError:
        return UNKNOWN_SOURCE_TOPIC

    payload = envelope.get("data", envelope) if isinstance(envelope, dict) else None
    if not isinstance(payload, dict):
        return UNKNOWN_SOURCE_TOPIC

    event_type = payload.get("e")
    if event_type == "trade":
        return config.topic_trades_raw
    if event_type == "kline":
        return config.topic_klines_raw
    if event_type == "24hrTicker":
        return config.topic_tickers_raw
    return UNKNOWN_SOURCE_TOPIC


def infer_symbol(raw_payload: str | bytes) -> str | None:
    try:
        envelope = json.loads(stringify_raw_payload(raw_payload))
    except json.JSONDecodeError:
        return None

    payload = envelope.get("data", envelope) if isinstance(envelope, dict) else None
    if not isinstance(payload, dict):
        return None

    symbol = payload.get("s")
    if symbol is None:
        return None
    return str(symbol).upper()
