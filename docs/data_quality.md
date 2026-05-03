# Data Quality

## Validation Strategy

The producer performs business validation after Binance payloads are normalized into project-owned event shapes. This keeps validation independent from Binance's compact field names and aligned with the schema contracts in `schemas/`.

Invalid, malformed, or unroutable records are published to `market.events.invalid`. They are not silently dropped.

## Validation Rules

### Trade Events

Topic: `market.trades.raw`

Required checks:

- `symbol` is present.
- `event_time` is present and parseable as a timezone-aware timestamp.
- `trade_id` is present.
- `price > 0`.
- `quantity > 0`.

### Kline Events

Topic: `market.klines.raw`

Required checks:

- `symbol` is present.
- `kline_start_time` is present and parseable as a timezone-aware timestamp.
- `open_price`, `high_price`, `low_price`, and `close_price` are numeric.
- `volume >= 0`.

### Ticker Events

Topic: `market.tickers.raw`

Required checks:

- `symbol` is present.
- `event_time` is present and parseable as a timezone-aware timestamp.
- `last_price > 0`.
- `volume >= 0`.

## Dead-Letter Queue

DLQ topic: `market.events.invalid`

DLQ schema: `schemas/invalid_event_v1.json`

The invalid event payload includes:

| Field | Purpose |
| --- | --- |
| `schema_version` | Contract version for the invalid event |
| `source_topic` | Intended source topic when known, otherwise `unknown` |
| `error_type` | Error category |
| `error_message` | Human-readable error message |
| `raw_payload` | Original payload as text |
| `error_time` | Timestamp when the producer recorded the error |
| `symbol` | Optional symbol when safely extractable |

Error categories:

| Error Type | Meaning |
| --- | --- |
| `JSON_DECODE_ERROR` | Payload could not be parsed as JSON |
| `ROUTING_ERROR` | Payload could not be mapped to a supported market event |
| `VALIDATION_ERROR` | Normalized event failed business validation |
| `SCHEMA_ERROR` | Reserved for schema validation failures |
| `PRODUCER_ERROR` | Reserved for producer-side publish failures |

## Operational Checks

Useful local checks:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic market.events.invalid \
  --from-beginning \
  --max-messages 5
```

Expected behavior:

- Valid events are published only to their raw market topic.
- Malformed JSON is published to `market.events.invalid`.
- Unsupported event types are published to `market.events.invalid`.
- Business-invalid events are published to `market.events.invalid` with `error_type=VALIDATION_ERROR`.

DLQ volume should be monitored as a producer data quality metric. A sudden increase can indicate upstream payload changes, schema drift, or a producer transformation bug.
