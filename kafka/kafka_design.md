# Kafka Design

This document captures the local Kafka setup used by Milestone 1. The fuller topic contracts, schema contracts, and validation rules are expanded in Milestone 2.

## Local Broker

The local stack runs a single Kafka broker in KRaft mode through Docker Compose. This keeps the development environment small while still exercising the same producer and consumer contracts used by a multi-broker deployment.

| Listener | Used By | Address |
| --- | --- | --- |
| `INTERNAL` | Containers such as Kafka UI and future containerized jobs | `kafka:9092` |
| `EXTERNAL` | Host processes such as local Python producers and PySpark jobs | `localhost:9092` |

## Topic Creation

Topic definitions live in [topic_config.yaml](topic_config.yaml). The creation script reads that file and applies the topic name, partition count, replication factor, retention period, and cleanup policy.

```bash
bash kafka/create_topics.sh
```

## Partitioning Principle

Market event topics use `symbol` as the Kafka message key. This preserves per-symbol ordering while still allowing parallel processing across partitions.

## Milestone 1 Topics

| Topic | Partitions | Local Retention | Message Key |
| --- | ---: | --- | --- |
| `market.trades.raw` | 6 | 72 hours | `symbol` |
| `market.klines.raw` | 3 | 7 days | `symbol` |
| `market.tickers.raw` | 3 | 3 days | `symbol` |
| `market.events.invalid` | 3 | 14 days | `source_topic` or `error_type` |
| `market.signals.alerts` | 3 | 7 days | `symbol` |
