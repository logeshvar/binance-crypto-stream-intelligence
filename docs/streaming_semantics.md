# Streaming Semantics

## Event-Time Processing

The pipeline is designed around event time rather than processing time. Binance market events carry exchange timestamps that are normalized into timestamp columns such as `event_time`, `kline_start_time`, `kline_close_time`, `window_start`, and `window_end`.

Gold aggregations use event-time windows so metrics represent when market activity happened, not when the local process happened to receive the data.

## Watermarking

Stateful Gold aggregations apply watermarks to bound state and handle late records. Watermarks allow Spark to keep recent state open for delayed events while eventually closing older windows.

The practical effect:

- A late trade can still update a recent event-time window.
- Very late records can be dropped after the watermark has passed.
- State store growth remains bounded for long-running local streams.

## Deduplication

Silver tables deduplicate records before they feed Gold analytics.

| Entity | Deduplication key |
| --- | --- |
| Trades | `symbol`, `trade_id` |
| Klines | `symbol`, `kline_start_time`, `kline_close_time` |
| Tickers | `symbol`, `event_time` |

Deduplication is important because producers and streaming jobs can restart, WebSocket clients can reconnect, and exchange events can be observed more than once.

## Output Modes

The project uses output modes based on the behavior of each table:

- Append-style raw Bronze tables preserve source events and Kafka metadata.
- Silver tables append normalized records after parsing, validation, and deduplication.
- Gold aggregations write windowed outputs to Delta with checkpointing so they can be queried by the dashboard and alert publishers.
- Alert publishers convert selected Gold signal rows into JSON events and write them to Kafka.

## Delivery Expectations

The local implementation targets practical exactly-once-style behavior at the table level through Spark checkpointing and Delta sink commits. Kafka producers are configured for reliability, but local development still has failure modes that should be discussed clearly:

- A producer can reconnect and continue publishing live events.
- Spark can restart from checkpoints and avoid replaying already committed source offsets.
- If checkpoint state is deleted, replay behavior depends on source retention and configured starting offsets.
- If Kafka retention removes offsets before a downstream stream consumes them, the stream may need an explicit reset or backfill strategy.

## State Store Considerations

Windowed Gold tables use Spark state stores. Local warnings about state snapshots and delta files can appear during startup, especially after a query has run for a while. Those warnings are usually normal if the query continues processing.

Operational checks:

- Confirm the stream is making progress.
- Confirm checkpoint directories are being updated.
- Check that row counts in Gold tables change after new Silver records arrive.
- Watch Spark UI for state operator metrics and batch duration.

## Dashboard Time Filtering

The dashboard applies a selected time range to the Gold and alert query layer. This keeps old test runs from obscuring the current live run while preserving historical Delta data for inspection.

Supported views:

- Today
- Last Week
- Last Month
- Last Year
- All

This filter changes the serving view only. It does not delete historical data.
