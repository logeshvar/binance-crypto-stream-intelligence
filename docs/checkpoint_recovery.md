# Checkpointing and Recovery

## Purpose

The pipeline uses Spark Structured Streaming checkpoints to make each streaming job restartable. Checkpoints store source offsets, state store files, progress metadata, and sink commit metadata. They are part of the runtime contract, not temporary cache files.

## Checkpoint Layout

| Layer | Stream | Checkpoint path |
| --- | --- | --- |
| Bronze | Trades | `storage/checkpoints/bronze/trades` |
| Bronze | Klines | `storage/checkpoints/bronze/klines` |
| Bronze | Tickers | `storage/checkpoints/bronze/tickers` |
| Bronze | Invalid events | `storage/checkpoints/bronze/invalid_events` |
| Silver | Trades | `storage/checkpoints/silver/trades` |
| Silver | Klines | `storage/checkpoints/silver/klines` |
| Silver | Tickers | `storage/checkpoints/silver/tickers` |
| Gold | OHLC, summary, volatility, spikes, price alerts, watchlist | `storage/checkpoints/gold/*` |
| Alerts | Volume and price alert publishers | `storage/checkpoints/alerts/*` |

Grouped runners such as `make bronze-all`, `make silver-all`, and `make gold-all` run multiple streaming queries inside one Spark application, but each query still keeps its own checkpoint directory.

## Restart Behavior

When a stream restarts with the same checkpoint path, Spark resumes from the last committed source offsets. For Kafka-backed Bronze streams, this means the job should continue from the last processed Kafka offset rather than rereading all retained messages.

For Delta-backed Silver, Gold, and alert streams, Spark tracks the Delta source versions and the sink commits through the checkpoint. This is what prevents duplicated writes after an application restart.

## Local Recovery Demo

Use separate terminals for long-running processes.

1. Start Kafka and create topics.

```bash
make up
make topics
```

2. Start the producer.

```bash
make producer PYTHON=.venv/bin/python
```

3. Start Bronze ingestion.

```bash
make bronze-all PYTHON=.venv/bin/python
```

4. Stop the Bronze process with `Ctrl+C`, but keep the producer running.

5. Wait a minute so Kafka receives new records.

6. Restart Bronze with the same checkpoint path.

```bash
make bronze-all PYTHON=.venv/bin/python
```

7. Inspect the Bronze Delta tables in the notebook or Spark shell. The stream should continue from the last committed Kafka offsets rather than replaying the full topic.

## Kafka Durability and Local Resets

Kafka broker data is persisted through a named Docker volume. Ordinary `docker compose down` keeps topics, retained messages, and committed offsets. `docker compose down -v` intentionally removes the Kafka volume and resets broker state.

If Kafka is reset but Delta tables and checkpoints are kept, a stream can fail or behave unexpectedly because its checkpoint may point to offsets that no longer exist. For local development, reset Kafka, Delta output, and checkpoints together when you want a truly clean run.

Safe local reset pattern:

```bash
make down
docker compose down -v
```

Then clear local Delta outputs and checkpoints manually only when you intend to discard local data:

```bash
rm -rf storage/bronze/* storage/silver/* storage/gold/* storage/checkpoints/*
touch storage/bronze/.gitkeep storage/silver/.gitkeep storage/gold/.gitkeep storage/checkpoints/.gitkeep
```

Do not use this reset pattern for production-like environments. In production, broker state, checkpoints, and tables should be managed through controlled retention, backfill, and deployment procedures.

## Recovery Rules

- Keep checkpoint paths stable across restarts.
- Do not share one checkpoint path between different queries.
- Do not delete checkpoints while keeping the matching output table unless you intentionally want a replay or rebuild.
- Reset dependent layers together when changing schemas, source semantics, or historical data.
- Treat checkpoint directories as part of the data pipeline state.
