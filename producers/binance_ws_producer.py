from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any

import websockets

from producers.config import ProducerConfig
from producers.dlq_publisher import (
    JSON_DECODE_ERROR,
    ROUTING_ERROR,
    VALIDATION_ERROR,
    DlqPublisher,
)
from producers.error_context import infer_source_topic, infer_symbol
from producers.event_router import EventRoutingError, route_binance_message
from producers.kafka_producer import MarketKafkaProducer
from producers.validators import ValidationError, validate_market_event


logger = logging.getLogger(__name__)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
        }

        reserved = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
        }
        for key, value in record.__dict__.items():
            if key not in reserved and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())


async def run_producer(config: ProducerConfig) -> None:
    stop_event = asyncio.Event()
    register_shutdown_signals(stop_event)

    kafka_producer = MarketKafkaProducer(config)
    await kafka_producer.start()
    dlq_publisher = DlqPublisher(config, kafka_producer)

    reconnect_count = 0
    delay_seconds = config.reconnect_initial_delay_seconds

    try:
        while not stop_event.is_set():
            try:
                logger.info(
                    "binance_websocket_connecting",
                    extra={
                        "url": config.websocket_url,
                        "stream_count": len(config.stream_names),
                        "symbols": ",".join(config.symbols),
                    },
                )
                async with websockets.connect(
                    config.websocket_url,
                    ping_interval=config.websocket_ping_interval_seconds,
                    ping_timeout=config.websocket_ping_timeout_seconds,
                    max_queue=config.websocket_max_queue,
                ) as websocket:
                    logger.info("binance_websocket_connected")
                    delay_seconds = config.reconnect_initial_delay_seconds

                    async for message in websocket:
                        if stop_event.is_set():
                            break
                        try:
                            routed_event = route_binance_message(message, config)
                            validate_market_event(routed_event.value).raise_for_errors()
                        except json.JSONDecodeError as exc:
                            await dlq_publisher.publish_invalid_event(
                                raw_payload=message,
                                source_topic=infer_source_topic(message, config),
                                error_type=JSON_DECODE_ERROR,
                                error_message=str(exc),
                                symbol=infer_symbol(message),
                            )
                            logger.exception("binance_message_json_decode_failed")
                            continue
                        except ValidationError as exc:
                            await dlq_publisher.publish_invalid_event(
                                raw_payload=message,
                                source_topic=routed_event.topic,
                                error_type=VALIDATION_ERROR,
                                error_message=str(exc),
                                symbol=routed_event.key,
                            )
                            logger.warning(
                                "binance_message_validation_failed",
                                extra={
                                    "topic": routed_event.topic,
                                    "symbol": routed_event.key,
                                    "error_message": str(exc),
                                },
                            )
                            continue
                        except (EventRoutingError, KeyError, TypeError, ValueError) as exc:
                            await dlq_publisher.publish_invalid_event(
                                raw_payload=message,
                                source_topic=infer_source_topic(message, config),
                                error_type=ROUTING_ERROR,
                                error_message=str(exc),
                                symbol=infer_symbol(message),
                            )
                            logger.exception("binance_message_route_failed")
                            continue

                        await kafka_producer.publish(routed_event)

            except asyncio.CancelledError:
                raise
            except Exception:
                reconnect_count += 1
                logger.exception(
                    "binance_websocket_disconnected",
                    extra={
                        "reconnect_count": reconnect_count,
                        "delay_seconds": delay_seconds,
                    },
                )
                await wait_for_reconnect(stop_event, delay_seconds)
                delay_seconds = min(delay_seconds * 2, config.reconnect_max_delay_seconds)
    finally:
        await kafka_producer.stop()


def register_shutdown_signals(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda _signum, _frame: stop_event.set())


async def wait_for_reconnect(stop_event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except asyncio.TimeoutError:
        return


def main() -> None:
    config = ProducerConfig.from_env()
    configure_logging(config.log_level)
    asyncio.run(run_producer(config))


if __name__ == "__main__":
    main()
