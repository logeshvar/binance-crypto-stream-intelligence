from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from jsonschema import validate
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from sinks.alert_kafka_writer import build_price_alert_events
from sinks.alert_kafka_writer import build_volume_alert_events
from sinks.alert_kafka_writer import to_kafka_messages
from tests.silver_trade_fixtures import utc_ts


ALERT_SCHEMA = json.loads(Path("schemas/alert_event_v1.json").read_text())

VOLUME_SPIKE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("current_volume", DecimalType(38, 18), nullable=True),
        StructField("baseline_volume", DecimalType(38, 22), nullable=True),
        StructField("volume_spike_ratio", DoubleType(), nullable=True),
        StructField("signal_strength", StringType(), nullable=False),
        StructField("number_of_trades", LongType(), nullable=False),
    ]
)

PRICE_ALERT_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("start_price", DecimalType(38, 18), nullable=False),
        StructField("end_price", DecimalType(38, 18), nullable=False),
        StructField("price_change_pct", DoubleType(), nullable=False),
        StructField("alert_type", StringType(), nullable=False),
        StructField("severity", StringType(), nullable=False),
    ]
)


def test_volume_alert_events_match_alert_schema_and_kafka_shape(spark_session):
    volume_df = spark_session.createDataFrame(
        [
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:00:00Z"),
                utc_ts("2026-04-30T10:05:00Z"),
                Decimal("10.00"),
                Decimal("3.00"),
                3.33,
                "HIGH",
                42,
            ),
            (
                "ETHUSDT",
                utc_ts("2026-04-30T10:00:00Z"),
                utc_ts("2026-04-30T10:05:00Z"),
                Decimal("3.00"),
                Decimal("3.00"),
                1.0,
                "LOW",
                10,
            ),
        ],
        schema=VOLUME_SPIKE_SCHEMA,
    )

    rows = to_kafka_messages(build_volume_alert_events(volume_df)).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    payload = json.loads(row["value"])
    assert row.get("key") in [payload["symbol"]]
    validate(instance=payload, schema=ALERT_SCHEMA)
    assert payload["alert_type"] == "VOLUME_SPIKE"
    assert payload["severity"] == "HIGH"
    assert payload["metric_value"] == 3.33


def test_price_alert_events_match_alert_schema_and_kafka_shape(spark_session):
    price_df = spark_session.createDataFrame(
        [
            (
                "BTCUSDT",
                utc_ts("2026-04-30T10:00:00Z"),
                utc_ts("2026-04-30T10:05:00Z"),
                Decimal("100.00"),
                Decimal("102.00"),
                2.0,
                "PRICE_SURGE",
                "HIGH",
            )
        ],
        schema=PRICE_ALERT_SCHEMA,
    )

    rows = to_kafka_messages(build_price_alert_events(price_df)).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    payload = json.loads(row["value"])
    assert row.get("key") in [payload["symbol"]]
    validate(instance=payload, schema=ALERT_SCHEMA)
    assert payload["alert_type"] == "PRICE_SURGE"
    assert payload["severity"] == "HIGH"
    assert payload["metric_value"] == 2.0
