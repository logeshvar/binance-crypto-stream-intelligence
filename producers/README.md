# Binance WebSocket Producer

The producer consumes Binance public market WebSocket streams, normalizes payloads into project schemas, and publishes them to Kafka raw topics.

It uses only public market streams:

- `<symbol>@trade`
- `<symbol>@kline_1m`
- `<symbol>@ticker`

No API key, private account stream, or trading endpoint is used.

## Runtime Flow

```text
Binance combined WebSocket stream
  -> event router
  -> normalized JSON payload
  -> Kafka topic keyed by symbol
```

## Topic Routing

| Binance Event | Kafka Topic | Message Key | Schema |
| --- | --- | --- | --- |
| `trade` | `market.trades.raw` | `symbol` | `schemas/trade_event_v1.json` |
| `kline` | `market.klines.raw` | `symbol` | `schemas/kline_event_v1.json` |
| `24hrTicker` | `market.tickers.raw` | `symbol` | `schemas/ticker_event_v1.json` |

Kafka headers:

| Header | Value |
| --- | --- |
| `source` | `binance` |
| `schema_version` | `1.0` |
| `event_type` | `trade`, `kline`, or `ticker` |

## Configuration

The producer loads environment variables from `.env` when present. Defaults are defined in `producers/config.py` and documented in `.env.example`.

Important variables:

| Variable | Purpose |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address from the host |
| `BINANCE_WS_BASE_URL` | Binance combined stream base URL |
| `CRYPTO_SYMBOLS` | Comma-separated symbols |
| `BINANCE_STREAM_TYPES` | Comma-separated stream types: `trade`, `kline_1m`, `ticker` |
| `PRODUCER_RECONNECT_INITIAL_DELAY_SECONDS` | Initial reconnect backoff |
| `PRODUCER_RECONNECT_MAX_DELAY_SECONDS` | Maximum reconnect backoff |
| `PRODUCER_PUBLISH_LOG_INTERVAL` | Publish-count logging interval |

## Run Locally

Install dependencies:

```bash
make setup-venv
source .venv/bin/activate
```

Start Kafka and create topics:

```bash
docker compose up -d
bash kafka/create_topics.sh
```

Run the producer:

```bash
python -m producers.binance_ws_producer
```

Inspect topics in Kafka UI:

```text
http://localhost:8080
```

## Reliability Behavior

- Uses Binance combined streams to keep all configured market streams on one connection.
- Publishes Kafka messages with `symbol` as the key to preserve per-symbol ordering.
- Adds Kafka headers for source, schema version, and event type.
- Reconnects with exponential backoff after WebSocket failures.
- Logs connection events, publish counts, reconnect counts, and route failures as JSON lines.
