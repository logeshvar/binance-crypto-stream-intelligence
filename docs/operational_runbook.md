# Operational Runbook

## Purpose

This runbook describes how to operate the local streaming pipeline, verify data movement, inspect outputs, and recover from common local failures.

## Start Order

Use one terminal per long-running process.

1. Start Kafka and Kafka UI.

```bash
make up
make topics
```

2. Start the Binance public WebSocket producer.

```bash
make producer PYTHON=.venv/bin/python
```

3. Start Bronze ingestion.

```bash
make bronze-all PYTHON=.venv/bin/python
```

4. Start Silver normalization after Bronze has written records.

```bash
make silver-all PYTHON=.venv/bin/python
```

5. Start Gold analytics after Silver trades have data.

```bash
make gold-all PYTHON=.venv/bin/python
```

6. Start alert publishers.

```bash
make alerts-volume PYTHON=.venv/bin/python
make alerts-price PYTHON=.venv/bin/python
```

7. Start the dashboard.

```bash
make dashboard PYTHON=.venv/bin/python
```

## Local URLs

| Service | URL |
| --- | --- |
| Kafka UI | `http://localhost:8080` |
| Bronze Spark UI | `http://localhost:4040` |
| Silver Spark UI | `http://localhost:4050` |
| Gold Spark UI | `http://localhost:4060` |
| Dashboard | `http://localhost:8501` |

Spark UI ports can move upward if a port is already in use. That is normal for local multi-process Spark development.

## Health Checks

Kafka topics:

```bash
make topics-list
make topics-describe
```

Alert topic sample:

```bash
make alerts-consume
```

Python tests:

```bash
make test PYTHON=.venv/bin/python
```

Delta table inspection:

```bash
.venv/bin/jupyter lab
```

Open `notebooks/inspect_delta_tables.ipynb`.

## Operational Metrics

Track these signals during demos and debugging:

| Area | Metric |
| --- | --- |
| Producer | messages received, messages published, reconnect count, publish failures |
| Kafka | topic message count, partition distribution, retained offsets |
| Bronze | rows processed, latest Kafka offset, latest ingest time |
| Silver | parsed rows, dropped invalid business records, deduplicated records |
| Gold | latest window end, row counts by table, stateful batch duration |
| Alerts | published alert count, latest alert time, alert severity mix |
| Quality | DLQ count, error type distribution |
| Freshness | latest event time, latest Gold update time, dashboard freshness label |

## Common Issues

### Kafka UI is empty

Run:

```bash
make topics
make topics-list
```

If topics are missing after a container restart, check whether Docker volumes were removed with `docker compose down -v`.

### Producer is running but no messages appear

Check the producer logs for WebSocket reconnects, validation errors, or Kafka publish errors. Confirm `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` for host-based execution.

### Spark UI port keeps increasing

Multiple Spark applications are running. This is normal when Bronze, Silver, Gold, and dashboard jobs run at the same time. The grouped runners set preferred ports, but Spark will choose the next free port if the preferred one is busy.

### Stream fails after Kafka reset

Kafka offsets in the checkpoint may no longer exist. For local development, reset Kafka, Delta outputs, and checkpoints together when intentionally starting from scratch.

### Dashboard shows old data

Use the dashboard Time Range filter. `Today` focuses on the current local day using the configured dashboard timezone. `All` shows every retained Delta row.

## Stop Order

Stop long-running Python/Spark processes with `Ctrl+C`.

Then stop containers:

```bash
make down
```

Use volume removal only when intentionally resetting Kafka state:

```bash
docker compose down -v
```

## Demo Checklist

- Kafka UI shows raw topics and alert topic.
- Producer logs show live Binance messages.
- Bronze tables contain raw Kafka metadata and payloads.
- Silver tables contain typed records.
- Gold tables contain windowed market signals.
- Dashboard shows recent symbols, volume spikes, volatility, alerts, and pipeline health.
- Tests pass locally.
