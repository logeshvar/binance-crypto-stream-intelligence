from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from producers.config import ProducerConfig


class EventRoutingError(ValueError):
    """Raised when a Binance payload cannot be routed to a project topic."""


@dataclass(frozen=True)
class RoutedEvent:
    topic: str
    key: str
    value: dict[str, Any]
    event_type: str
    headers: tuple[tuple[str, bytes], ...]
    raw_stream: str | None = None


def route_binance_message(message: str | bytes, config: ProducerConfig) -> RoutedEvent:
    envelope = json.loads(message)
    raw_stream = envelope.get("stream")
    payload = envelope.get("data", envelope)
    if not isinstance(payload, dict):
        raise EventRoutingError("Binance payload is not a JSON object")

    binance_event_type = payload.get("e")
    ingest_time = utc_now_iso()

    if binance_event_type == "trade":
        event_type = "trade"
        value = normalize_trade_event(payload, config, ingest_time)
    elif binance_event_type == "kline":
        event_type = "kline"
        value = normalize_kline_event(payload, config, ingest_time)
    elif binance_event_type == "24hrTicker":
        event_type = "ticker"
        value = normalize_ticker_event(payload, config, ingest_time)
    else:
        raise EventRoutingError(f"Unsupported Binance event type: {binance_event_type}")

    topic = config.topic_by_event_type[event_type]
    key = str(value["symbol"])
    headers = (
        ("source", config.source.encode("utf-8")),
        ("schema_version", config.schema_version.encode("utf-8")),
        ("event_type", event_type.encode("utf-8")),
    )
    return RoutedEvent(
        topic=topic,
        key=key,
        value=value,
        event_type=event_type,
        headers=headers,
        raw_stream=raw_stream,
    )


def normalize_trade_event(
    payload: dict[str, Any], config: ProducerConfig, ingest_time: str
) -> dict[str, Any]:
    price = decimal_string(payload["p"])
    quantity = decimal_string(payload["q"])
    trade_value = decimal_product(price, quantity)

    return {
        "schema_version": config.schema_version,
        "event_type": "trade",
        "symbol": str(payload["s"]).upper(),
        "event_time": millis_to_utc_iso(payload["E"]),
        "trade_id": int(payload["t"]),
        "price": price,
        "quantity": quantity,
        "trade_value": trade_value,
        "is_buyer_market_maker": bool(payload["m"]),
        "source": config.source,
        "ingest_time": ingest_time,
    }


def normalize_kline_event(
    payload: dict[str, Any], config: ProducerConfig, ingest_time: str
) -> dict[str, Any]:
    kline = payload["k"]

    return {
        "schema_version": config.schema_version,
        "event_type": "kline",
        "symbol": str(payload["s"]).upper(),
        "event_time": millis_to_utc_iso(payload["E"]),
        "kline_start_time": millis_to_utc_iso(kline["t"]),
        "kline_close_time": millis_to_utc_iso(kline["T"]),
        "interval": str(kline["i"]),
        "open_price": decimal_string(kline["o"]),
        "high_price": decimal_string(kline["h"]),
        "low_price": decimal_string(kline["l"]),
        "close_price": decimal_string(kline["c"]),
        "volume": decimal_string(kline["v"]),
        "quote_asset_volume": decimal_string(kline["q"]),
        "number_of_trades": int(kline["n"]),
        "taker_buy_base_asset_volume": decimal_string(kline["V"]),
        "taker_buy_quote_asset_volume": decimal_string(kline["Q"]),
        "is_closed": bool(kline["x"]),
        "source": config.source,
        "ingest_time": ingest_time,
    }


def normalize_ticker_event(
    payload: dict[str, Any], config: ProducerConfig, ingest_time: str
) -> dict[str, Any]:
    return {
        "schema_version": config.schema_version,
        "event_type": "ticker",
        "symbol": str(payload["s"]).upper(),
        "event_time": millis_to_utc_iso(payload["E"]),
        "price_change": decimal_string(payload["p"]),
        "price_change_percent": decimal_string(payload["P"]),
        "weighted_avg_price": decimal_string(payload["w"]),
        "last_price": decimal_string(payload["c"]),
        "last_quantity": decimal_string(payload["Q"]),
        "open_price": decimal_string(payload["o"]),
        "high_price": decimal_string(payload["h"]),
        "low_price": decimal_string(payload["l"]),
        "volume": decimal_string(payload["v"]),
        "quote_volume": decimal_string(payload["q"]),
        "open_time": millis_to_utc_iso(payload["O"]),
        "close_time": millis_to_utc_iso(payload["C"]),
        "first_trade_id": int(payload["F"]),
        "last_trade_id": int(payload["L"]),
        "trade_count": int(payload["n"]),
        "source": config.source,
        "ingest_time": ingest_time,
    }


def millis_to_utc_iso(value: Any) -> str:
    millis = int(value)
    dt = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def decimal_string(value: Any) -> str:
    raw_value = str(value)
    try:
        Decimal(raw_value)
    except InvalidOperation as exc:
        raise EventRoutingError(f"Invalid decimal value: {raw_value}") from exc
    return raw_value


def decimal_product(left: str, right: str) -> str:
    return format(Decimal(left) * Decimal(right), "f")
