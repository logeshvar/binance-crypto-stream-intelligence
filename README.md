# Real-Time Crypto Market Intelligence Pipeline

Portfolio-grade streaming data engineering project for ingesting public crypto market data, processing it with Kafka and Spark Structured Streaming, storing Bronze/Silver/Gold Delta Lake datasets, and serving real-time market intelligence signals.

This project is not a trading bot, does not execute trades, and does not provide financial advice. It uses only Binance public market WebSocket streams with no API key and no account-specific data.

## Project Goal

Crypto markets generate continuous, high-frequency trade and price events across many assets. Raw WebSocket payloads are semi-structured and not immediately suitable for analytics, monitoring, or dashboarding. This project builds a production-style streaming pipeline that validates, routes, stores, deduplicates, aggregates, and serves those events as market intelligence outputs.

## Core Components

- Local Kafka broker using Docker Compose
- Kafka UI for topic inspection
- Project folder scaffold
- Topic configuration and creation script
- Storage folders for Bronze, Silver, Gold, and checkpoints
- JSON Schema contracts for raw market events, invalid events, and alerts
- Kafka topic design documentation
- Async Binance WebSocket producer for trade, kline, and ticker streams
- Producer-side validation and invalid-event publishing to `market.events.invalid`
- Bronze Structured Streaming jobs that preserve Kafka metadata in Delta Lake

## Architecture

```mermaid
flowchart LR
    A["Binance public WebSocket streams"] --> B["Python async producer"]
    B --> C["Kafka raw topics"]
    B --> D["Kafka invalid/DLQ topic"]
    C --> E["Spark Structured Streaming Bronze"]
    D --> E
    E --> F["Spark Structured Streaming Silver"]
    F --> G["Spark Structured Streaming Gold"]
    G --> H["Alert Kafka topic"]
    G --> I["Dashboard/serving layer"]
```

## Local Services

| Service | Purpose | Local URL |
| --- | --- | --- |
| Kafka | Local broker for raw, DLQ, and alert topics | `localhost:9092` |
| Kafka UI | Inspect topics, partitions, messages, and consumer groups | `http://localhost:8080` |

## Kafka Topics

| Topic | Purpose | Partitions | Key | Schema |
| --- | --- | ---: | --- | --- |
| `market.trades.raw` | Raw trade events from Binance WebSocket | 6 | `symbol` | `schemas/trade_event_v1.json` |
| `market.klines.raw` | Raw 1-minute candlestick events | 3 | `symbol` | `schemas/kline_event_v1.json` |
| `market.tickers.raw` | Raw 24-hour ticker events | 3 | `symbol` | `schemas/ticker_event_v1.json` |
| `market.events.invalid` | Dead-letter queue for invalid or malformed events | 3 | `source_topic` or `error_type` | `schemas/invalid_event_v1.json` |
| `market.signals.alerts` | Gold-level market intelligence alerts | 3 | `symbol` | `schemas/alert_event_v1.json` |

## Quick Start

Prerequisites:

- Docker Desktop or compatible Docker runtime
- Docker Compose v2
- Python 3.11+
- Bash

Run the local stack:

```bash
cp .env.example .env
docker compose up -d
bash kafka/create_topics.sh
docker compose ps
```

Open Kafka UI:

```text
http://localhost:8080
```

List topics:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

The same workflow is available through `make`:

```bash
make up
make topics
make topics-list
```

Install and run the producer:

```bash
make setup-venv
source .venv/bin/activate
python -m producers.binance_ws_producer
```

Or run through Make with the virtualenv interpreter:

```bash
make producer PYTHON=.venv/bin/python
```

Run a Bronze stream:

```bash
make bronze-trades PYTHON=.venv/bin/python
```

Bronze Delta outputs are written under `./storage/bronze`. Checkpoints are written under `./storage/checkpoints/bronze`.
Local Spark jobs use the virtualenv PySpark runtime by default so an unrelated machine-level `SPARK_HOME` does not leak into the project.

## Repository Layout

```text
.
├── docker-compose.yml
├── Makefile
├── README.md
├── kafka/
│   ├── create_topics.sh
│   ├── kafka_design.md
│   └── topic_config.yaml
├── producers/
│   ├── binance_ws_producer.py
│   ├── config.py
│   ├── event_router.py
│   ├── kafka_producer.py
│   └── README.md
├── schemas/
│   ├── alert_event_v1.json
│   ├── invalid_event_v1.json
│   ├── kline_event_v1.json
│   ├── ticker_event_v1.json
│   └── trade_event_v1.json
├── streaming/
│   ├── bronze/
│   │   ├── bronze_invalid_events.py
│   │   ├── bronze_klines.py
│   │   ├── bronze_tickers.py
│   │   ├── bronze_trades.py
│   │   └── common.py
│   ├── silver/
│   ├── gold/
│   └── alerts/
├── sinks/
├── dashboards/
├── tests/
├── storage/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── checkpoints/
│   └── spark/
└── docs/
    ├── architecture.md
    ├── data_quality.md
    ├── kafka_design.md
    └── schema_contracts.md
```

## Design Docs

- [Architecture](docs/architecture.md)
- [Data quality](docs/data_quality.md)
- [Kafka design](docs/kafka_design.md)
- [Schema contracts](docs/schema_contracts.md)

## Engineering Focus

This project is designed to demonstrate senior-level streaming data engineering:

- Kafka topic design and partitioning
- Producer reliability and reconnect handling
- Schema contracts and data validation
- Dead-letter queue handling
- Spark Structured Streaming with event-time semantics
- Watermarking, checkpointing, and recovery
- Delta Lake medallion architecture
- Stateful aggregations and alerting
- Operational dashboarding and documentation

## Future Enhancements

Optional enhancements include order book depth streams, schema registry, Prometheus/Grafana, Databricks deployment, AWS MSK, S3-backed Delta storage, ML-based anomaly detection, and CI/CD.
