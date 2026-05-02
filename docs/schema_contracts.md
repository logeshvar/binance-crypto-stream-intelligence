# Schema Contracts

## Contract Strategy

Kafka payloads use normalized JSON contracts stored in `schemas/`. The producer converts Binance public WebSocket payloads into these project-owned shapes before publishing to Kafka. This keeps downstream Spark jobs independent from Binance's compact field names and gives every stream an explicit schema boundary.

The contracts use JSON Schema draft 2020-12. Decimal values are encoded as strings in raw Kafka events to avoid floating-point rounding during ingestion. Spark Silver jobs cast those fields into typed decimal columns.

## Event Envelopes

All valid market events include:

| Field | Rule |
| --- | --- |
| `schema_version` | Required, currently `1.0` |
| `event_type` | Required for market events: `trade`, `kline`, or `ticker` |
| `symbol` | Required uppercase Binance symbol, such as `BTCUSDT` |
| `source` | Required, currently `binance` |
| `event_time` | Required exchange event timestamp in UTC |
| `ingest_time` | Required producer timestamp in UTC |

Dead-letter events do not require `event_type` because they may represent malformed payloads that cannot be routed safely.

## Schema Files

| File | Kafka Topic | Purpose |
| --- | --- | --- |
| `schemas/trade_event_v1.json` | `market.trades.raw` | Normalized public trade event |
| `schemas/kline_event_v1.json` | `market.klines.raw` | Normalized public 1-minute kline event |
| `schemas/ticker_event_v1.json` | `market.tickers.raw` | Normalized public 24-hour ticker event |
| `schemas/invalid_event_v1.json` | `market.events.invalid` | Invalid, malformed, or unroutable event |
| `schemas/alert_event_v1.json` | `market.signals.alerts` | Market signal alert produced by Gold logic |

## Trade Event

Topic: `market.trades.raw`

Key: `symbol`

Required fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Must equal `1.0` |
| `event_type` | string | Must equal `trade` |
| `symbol` | string | Required |
| `event_time` | timestamp string | Required UTC timestamp |
| `trade_id` | integer | Required, non-negative |
| `price` | decimal string | Required, greater than zero by business validation |
| `quantity` | decimal string | Required, greater than zero by business validation |
| `trade_value` | decimal string | Required, `price * quantity` |
| `is_buyer_market_maker` | boolean | Required |
| `source` | string | Must equal `binance` |
| `ingest_time` | timestamp string | Required UTC timestamp |

Business validation:

- `symbol` is present and supported by configuration.
- `event_time` is present and parseable as UTC time.
- `trade_id` is present.
- `price > 0`.
- `quantity > 0`.
- `trade_value` equals `price * quantity` within decimal precision used by the producer.

Deduplication key for Silver: `symbol`, `trade_id`.

## Kline Event

Topic: `market.klines.raw`

Key: `symbol`

Required fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Must equal `1.0` |
| `event_type` | string | Must equal `kline` |
| `symbol` | string | Required |
| `event_time` | timestamp string | Required UTC timestamp |
| `kline_start_time` | timestamp string | Required UTC timestamp |
| `kline_close_time` | timestamp string | Required UTC timestamp |
| `interval` | string | Must equal `1m` |
| `open_price` | decimal string | Required numeric value |
| `high_price` | decimal string | Required numeric value |
| `low_price` | decimal string | Required numeric value |
| `close_price` | decimal string | Required numeric value |
| `volume` | decimal string | Required, non-negative by business validation |
| `quote_asset_volume` | decimal string | Required, non-negative by business validation |
| `number_of_trades` | integer | Required, non-negative |
| `taker_buy_base_asset_volume` | decimal string | Required, non-negative |
| `taker_buy_quote_asset_volume` | decimal string | Required, non-negative |
| `is_closed` | boolean | Required |
| `source` | string | Must equal `binance` |
| `ingest_time` | timestamp string | Required UTC timestamp |

Business validation:

- `symbol` is present and supported by configuration.
- `kline_start_time` and `kline_close_time` are present and parseable as UTC time.
- `open_price`, `high_price`, `low_price`, and `close_price` are numeric.
- `high_price >= low_price`.
- `volume >= 0`.
- `number_of_trades >= 0`.

Deduplication key for Silver: `symbol`, `kline_start_time`, `kline_close_time`.

## Ticker Event

Topic: `market.tickers.raw`

Key: `symbol`

Required fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Must equal `1.0` |
| `event_type` | string | Must equal `ticker` |
| `symbol` | string | Required |
| `event_time` | timestamp string | Required UTC timestamp |
| `price_change` | decimal string | Required numeric value |
| `price_change_percent` | decimal string | Required numeric value |
| `weighted_avg_price` | decimal string | Required numeric value |
| `last_price` | decimal string | Required, greater than zero by business validation |
| `last_quantity` | decimal string | Required, non-negative |
| `open_price` | decimal string | Required numeric value |
| `high_price` | decimal string | Required numeric value |
| `low_price` | decimal string | Required numeric value |
| `volume` | decimal string | Required, non-negative by business validation |
| `quote_volume` | decimal string | Required, non-negative |
| `open_time` | timestamp string | Required UTC timestamp |
| `close_time` | timestamp string | Required UTC timestamp |
| `first_trade_id` | integer | Required, non-negative |
| `last_trade_id` | integer | Required, non-negative |
| `trade_count` | integer | Required, non-negative |
| `source` | string | Must equal `binance` |
| `ingest_time` | timestamp string | Required UTC timestamp |

Business validation:

- `symbol` is present and supported by configuration.
- `event_time` is present and parseable as UTC time.
- `last_price > 0`.
- `volume >= 0`.
- `high_price >= low_price`.
- `trade_count >= 0`.

Silver keeps ticker data as a latest-observation stream rather than using it as the source of truth for OHLC. Trade and kline streams remain the analytical source for windowed market calculations.

## Invalid Event

Topic: `market.events.invalid`

Key: `source_topic` when available, otherwise `error_type`

Required fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Must equal `1.0` |
| `source_topic` | string | Source topic or `unknown` |
| `error_type` | string | One of `VALIDATION_ERROR`, `JSON_DECODE_ERROR`, `SCHEMA_ERROR`, `ROUTING_ERROR`, `PRODUCER_ERROR` |
| `error_message` | string | Human-readable error summary |
| `raw_payload` | string | Original payload as text |
| `error_time` | timestamp string | UTC timestamp when the error was recorded |
| `symbol` | string | Optional, only when safely extractable |

Invalid events are never silently dropped. The DLQ stores enough context for replay, debugging, and quality metrics.

## Alert Event

Topic: `market.signals.alerts`

Key: `symbol`

Required fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Must equal `1.0` |
| `symbol` | string | Required |
| `alert_type` | string | `VOLUME_SPIKE`, `PRICE_SURGE`, `PRICE_DROP`, or `HIGH_VOLATILITY` |
| `severity` | string | `LOW`, `MEDIUM`, or `HIGH` |
| `window_start` | timestamp string | Event-time window start |
| `window_end` | timestamp string | Event-time window end |
| `metric_value` | number | Metric that triggered the alert |
| `description` | string | Human-readable alert context |
| `created_at` | timestamp string | UTC timestamp when the alert was emitted |

## Compatibility Rules

- Backward-compatible additions require a new optional field and no change to existing field meaning.
- Breaking changes require a new schema file name and `schema_version`.
- Producers must publish the schema version in the payload and Kafka headers.
- Spark jobs should reject or route unsupported schema versions to the DLQ.
- Decimal precision should be preserved as strings until Silver casts to decimal types.
