# Kafka Design

## Purpose

Kafka is the streaming backbone for raw Binance public market events, invalid-event routing, and downstream market signal alerts. The design uses explicit topics per event family so retention, partitioning, replay, and consumer ownership can be managed independently.

## Local Broker

The local stack runs Kafka in KRaft mode through Docker Compose.

| Listener | Used By | Address |
| --- | --- | --- |
| `INTERNAL` | Docker services such as Kafka UI and future containerized jobs | `kafka:9092` |
| `EXTERNAL` | Host processes such as local Python producers and PySpark jobs | `localhost:9092` |

Local replication factor is `1` because this is a single-broker development environment. Production-like deployments should increase replication factor and configure broker-level durability controls according to platform requirements.

## Topic Contracts

| Topic | Purpose | Partitions | Retention | Message Key | Schema |
| --- | --- | ---: | --- | --- | --- |
| `market.trades.raw` | Normalized public trade events | 6 | 72 hours | `symbol` | `schemas/trade_event_v1.json` |
| `market.klines.raw` | Normalized public 1-minute kline events | 3 | 7 days | `symbol` | `schemas/kline_event_v1.json` |
| `market.tickers.raw` | Normalized public 24-hour ticker events | 3 | 3 days | `symbol` | `schemas/ticker_event_v1.json` |
| `market.events.invalid` | Dead-letter queue for malformed, invalid, or unroutable payloads | 3 | 14 days | `source_topic` or `error_type` | `schemas/invalid_event_v1.json` |
| `market.signals.alerts` | Market intelligence alerts emitted from Gold logic | 3 | 7 days | `symbol` | `schemas/alert_event_v1.json` |

Retention is intentionally short for local raw topics to keep disk usage predictable. Gold and serving data are persisted in Delta tables rather than relying on long Kafka retention.

## Partitioning Strategy

Market event topics use `symbol` as the Kafka message key. This gives the pipeline two important properties:

- Per-symbol ordering is preserved within a partition.
- Multiple symbols can be processed in parallel across partitions.

The trade topic has more partitions because trade events have higher frequency than kline and ticker events. Kline and ticker topics use fewer partitions because their event rates are lower and their consumers are generally lighter.

## Producer Headers

Producers should include these Kafka headers when the client library supports headers:

| Header | Example | Purpose |
| --- | --- | --- |
| `source` | `binance` | Identifies the upstream system |
| `schema_version` | `1.0` | Allows downstream compatibility checks |
| `event_type` | `trade` | Supports debugging and generic consumers |

Headers are metadata only. Consumers must still validate the payload schema because headers can be missing or malformed.

## Topic Creation

Topic definitions live in `kafka/topic_config.yaml`. Create or update topics with:

```bash
bash kafka/create_topics.sh
```

List topics:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Describe topic configuration:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
```

## Consumer Expectations

Bronze consumers preserve Kafka metadata including topic, partition, offset, key, raw value, Kafka timestamp, ingestion timestamp, and process date. This makes raw data auditable and replayable.

Silver consumers parse and type payloads according to the schema files. Invalid JSON, unsupported schema versions, and failed business validations should be routed to `market.events.invalid` rather than dropped.

Gold consumers use event-time semantics and checkpointed state to compute windowed metrics. Kafka offsets are recovered through Structured Streaming checkpoint locations.
