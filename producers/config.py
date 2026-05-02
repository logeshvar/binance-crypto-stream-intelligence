from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "MATICUSDT",
)

SUPPORTED_STREAM_TYPES = ("trade", "kline_1m", "ticker")


def load_env_file(path: str | Path = ".env") -> None:
    """Load a small dotenv-style file without adding another runtime dependency."""
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _csv(value: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None or value.strip() == "":
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _normalize_symbols(symbols: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(symbol.strip().upper() for symbol in symbols if symbol.strip())
    invalid = [symbol for symbol in normalized if not symbol.isalnum()]
    if invalid:
        raise ValueError(f"Invalid symbols configured: {invalid}")
    return normalized


def _normalize_stream_types(stream_types: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(stream.strip().lower() for stream in stream_types if stream.strip())
    invalid = [stream for stream in normalized if stream not in SUPPORTED_STREAM_TYPES]
    if invalid:
        raise ValueError(
            f"Invalid stream types configured: {invalid}. "
            f"Supported values: {SUPPORTED_STREAM_TYPES}"
        )
    return normalized


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    binance_ws_base_url: str
    symbols: tuple[str, ...]
    stream_types: tuple[str, ...]
    topic_trades_raw: str
    topic_klines_raw: str
    topic_tickers_raw: str
    topic_events_invalid: str
    topic_signals_alerts: str
    client_id: str
    schema_version: str
    source: str
    log_level: str
    publish_log_interval: int
    reconnect_initial_delay_seconds: float
    reconnect_max_delay_seconds: float
    kafka_acks: str
    kafka_linger_ms: int
    kafka_request_timeout_ms: int
    websocket_ping_interval_seconds: float
    websocket_ping_timeout_seconds: float
    websocket_max_queue: int

    @classmethod
    def from_env(cls, env_file: str | Path = ".env") -> "ProducerConfig":
        load_env_file(env_file)

        symbols = _normalize_symbols(_csv(os.getenv("CRYPTO_SYMBOLS"), DEFAULT_SYMBOLS))
        stream_types = _normalize_stream_types(
            _csv(os.getenv("BINANCE_STREAM_TYPES"), SUPPORTED_STREAM_TYPES)
        )

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            binance_ws_base_url=os.getenv(
                "BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443/stream"
            ),
            symbols=symbols,
            stream_types=stream_types,
            topic_trades_raw=os.getenv("TOPIC_TRADES_RAW", "market.trades.raw"),
            topic_klines_raw=os.getenv("TOPIC_KLINES_RAW", "market.klines.raw"),
            topic_tickers_raw=os.getenv("TOPIC_TICKERS_RAW", "market.tickers.raw"),
            topic_events_invalid=os.getenv("TOPIC_EVENTS_INVALID", "market.events.invalid"),
            topic_signals_alerts=os.getenv("TOPIC_SIGNALS_ALERTS", "market.signals.alerts"),
            client_id=os.getenv("PRODUCER_CLIENT_ID", "crypto-market-intelligence-producer"),
            schema_version=os.getenv("PRODUCER_SCHEMA_VERSION", "1.0"),
            source=os.getenv("PRODUCER_SOURCE", "binance"),
            log_level=os.getenv("PRODUCER_LOG_LEVEL", "INFO"),
            publish_log_interval=_int_env("PRODUCER_PUBLISH_LOG_INTERVAL", 1000),
            reconnect_initial_delay_seconds=_float_env(
                "PRODUCER_RECONNECT_INITIAL_DELAY_SECONDS", 1.0
            ),
            reconnect_max_delay_seconds=_float_env("PRODUCER_RECONNECT_MAX_DELAY_SECONDS", 60.0),
            kafka_acks=os.getenv("PRODUCER_KAFKA_ACKS", "all"),
            kafka_linger_ms=_int_env("PRODUCER_KAFKA_LINGER_MS", 50),
            kafka_request_timeout_ms=_int_env("PRODUCER_KAFKA_REQUEST_TIMEOUT_MS", 30000),
            websocket_ping_interval_seconds=_float_env("WEBSOCKET_PING_INTERVAL_SECONDS", 20.0),
            websocket_ping_timeout_seconds=_float_env("WEBSOCKET_PING_TIMEOUT_SECONDS", 60.0),
            websocket_max_queue=_int_env("WEBSOCKET_MAX_QUEUE", 1024),
        )

    @property
    def topic_by_event_type(self) -> Mapping[str, str]:
        return {
            "trade": self.topic_trades_raw,
            "kline": self.topic_klines_raw,
            "ticker": self.topic_tickers_raw,
        }

    @property
    def stream_names(self) -> tuple[str, ...]:
        streams: list[str] = []
        for symbol in self.symbols:
            symbol_lower = symbol.lower()
            for stream_type in self.stream_types:
                if stream_type == "trade":
                    streams.append(f"{symbol_lower}@trade")
                elif stream_type == "kline_1m":
                    streams.append(f"{symbol_lower}@kline_1m")
                elif stream_type == "ticker":
                    streams.append(f"{symbol_lower}@ticker")
        return tuple(streams)

    @property
    def websocket_url(self) -> str:
        base_url = self.binance_ws_base_url.rstrip("/")
        stream_path = "/".join(self.stream_names)
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}streams={stream_path}"
