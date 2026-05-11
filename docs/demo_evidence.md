# Demo Evidence

## Purpose

This page is an editable walkthrough for screenshots and notes that prove the pipeline works end to end. Add screenshots under `docs/assets/` using the filenames below, then update any captions with details from your local run.

## Run Commands

Start the local infrastructure:

```bash
make up
make topics
```

Start the live producer:

```bash
make producer PYTHON=.venv/bin/python
```

Start the streaming layers in separate terminals:

```bash
make bronze-all PYTHON=.venv/bin/python
make silver-all PYTHON=.venv/bin/python
make gold-all PYTHON=.venv/bin/python
```

Start alert publishers in separate terminals:

```bash
make alerts-volume PYTHON=.venv/bin/python
make alerts-price PYTHON=.venv/bin/python
```

Start the dashboard:

```bash
make dashboard PYTHON=.venv/bin/python
```

Run tests:

```bash
make test PYTHON=.venv/bin/python
```

## Evidence Checklist

| Evidence | Screenshot file | What it proves |
| --- | --- | --- |
| Kafka topics | `docs/assets/kafka-ui-topics.png` | Required Kafka topics exist with the expected event separation. |
| Producer running | `docs/assets/producer-running.png` | Binance public WebSocket events are being consumed and published. |
| Streams running | `docs/assets/bronze-silver-gold-streams.png` | Bronze, Silver, and Gold streaming jobs are active. |
| Bronze table | `docs/assets/delta-bronze-table.png` | Raw Kafka metadata and payloads are persisted in Delta. |
| Silver table | `docs/assets/delta-silver-table.png` | Raw events are parsed into typed, normalized records. |
| Gold table | `docs/assets/delta-gold-table.png` | Windowed market intelligence outputs are queryable. |
| Dashboard command center | `docs/assets/dashboard-command-center.png` | Current market state and attention-ranked symbols are served. |
| Dashboard symbol drilldown | `docs/assets/dashboard-symbol-drilldown.png` | Per-symbol price, volume, and spike history can be inspected. |
| Dashboard alerts | `docs/assets/dashboard-alerts.png` | Alert events are visible without Kafka transport metadata. |
| Dashboard pipeline health | `docs/assets/dashboard-pipeline-health.png` | Table freshness and row counts are visible. |
| Tests passing | `docs/assets/tests-passing.png` | Unit and transformation tests pass locally. |
| Alert topic | `docs/assets/kafka-alert-topic.png` | Gold alerts are published to the alert topic. |

## Screenshots

### Kafka Topics

![Kafka UI topics](assets/kafka-ui-topics.png)

Expected notes:

- Raw topics are split by event type.
- Invalid events and alert topics are separated from raw market events.
- Symbol-based message routing supports per-symbol ordering.

### Producer Running

![Producer running](assets/producer-running.png)

Expected notes:

- Producer connects to Binance public combined streams.
- Events are routed to trade, kline, and ticker topics.
- Invalid messages are sent to the DLQ instead of being silently dropped.

### Streaming Jobs

![Bronze Silver Gold streams](assets/bronze-silver-gold-streams.png)

Expected notes:

- Bronze, Silver, and Gold streams run as grouped Spark applications.
- Each streaming query uses its own checkpoint path.
- Spark UI can be used to inspect query progress.

### Bronze Delta Table

![Bronze Delta table](assets/delta-bronze-table.png)

Expected notes:

- Kafka metadata such as topic, partition, offset, message routing value, and timestamp is preserved in Bronze.
- Raw payload strings remain available for audit and replay.

### Silver Delta Table

![Silver Delta table](assets/delta-silver-table.png)

Expected notes:

- Silver records contain typed columns such as symbol, event time, price, quantity, and trade value.
- Duplicate trades are removed using symbol and trade ID.
- Invalid business records do not enter Silver.

### Gold Delta Table

![Gold Delta table](assets/delta-gold-table.png)

Expected notes:

- Gold tables contain event-time windowed analytics.
- Useful examples include OHLC, volume spikes, volatility, and price movement alerts.

### Dashboard Command Center

![Dashboard command center](assets/dashboard-command-center.png)

Expected notes:

- The time range filter keeps old test data from cluttering the current demo.
- Symbols are ranked by attention score.
- Strongest spike, biggest move, and freshness are visible at a glance.

### Dashboard Symbol Drilldown

![Dashboard symbol drilldown](assets/dashboard-symbol-drilldown.png)

Expected notes:

- Drilldown shows 1-minute close price history.
- 5-minute volume and spike ratio help explain why a symbol is interesting.

### Dashboard Alerts

![Dashboard alerts](assets/dashboard-alerts.png)

Expected notes:

- Alerts are shown as business events.
- Kafka topic, partition, offset, and routing metadata are intentionally hidden from the dashboard.

### Dashboard Pipeline Health

![Dashboard pipeline health](assets/dashboard-pipeline-health.png)

Expected notes:

- Gold table row counts and latest update times are visible.
- Freshness labels help identify stale outputs.

### Tests Passing

![Tests passing](assets/tests-passing.png)

Expected notes:

- Tests cover producer routing, validators, DLQ behavior, Silver transforms, Gold calculations, alert generation, and dashboard query helpers.

### Alert Topic

![Kafka alert topic](assets/kafka-alert-topic.png)

Expected notes:

- Gold signal publishers emit alert records to `market.signals.alerts`.
- Alert events follow the documented alert schema.

## Interview Evidence Map

| Topic | Evidence |
| --- | --- |
| Kafka topic design | Kafka topics screenshot, topic config, Kafka design docs |
| Producer reliability | Producer logs, reconnect handling in code |
| Data quality | DLQ topic, validators, invalid event schema |
| Bronze/Silver/Gold architecture | Delta table screenshots and architecture docs |
| Event-time processing | Gold table screenshot, streaming semantics docs |
| Checkpoint recovery | Stream screenshots, checkpoint recovery docs |
| Alerting | Dashboard alerts and alert topic screenshots |
| Serving layer | Dashboard screenshots |
| Testing | Tests passing screenshot and pytest suite |
