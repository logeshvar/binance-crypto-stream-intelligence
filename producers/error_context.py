from __future__ import annotations

import json

from producers.config import ProducerConfig


UNKNOWN_SOURCE_TOPIC = "unknown"


def stringify_raw_payload(raw_payload: str | bytes) -> str:
    if isinstance(raw_payload, bytes):
        return raw_payload.decode("utf-8", errors="replace")
    return raw_payload


def infer_source_topic(raw_payload: str | bytes, config: ProducerConfig) -> str:
    payload = extract_binance_payload(raw_payload)
    if payload is None:
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
    payload = extract_binance_payload(raw_payload)
    if payload is None:
        return None

    symbol = payload.get("s")
    if symbol is None:
        return None
    return str(symbol).upper()


def extract_binance_payload(raw_payload: str | bytes) -> dict | None:
    try:
        envelope = json.loads(stringify_raw_payload(raw_payload))
    except json.JSONDecodeError:
        return None

    payload = envelope.get("data", envelope) if isinstance(envelope, dict) else None
    if not isinstance(payload, dict):
        return None
    return payload
