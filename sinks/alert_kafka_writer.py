from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery


ALERT_COLUMNS = [
    "schema_version",
    "symbol",
    "alert_type",
    "severity",
    "window_start",
    "window_end",
    "metric_value",
    "description",
    "created_at",
]


@dataclass(frozen=True)
class AlertKafkaConfig:
    bootstrap_servers: str
    topic: str
    checkpoint_path: str
    trigger_processing_time: str | None

    @classmethod
    def from_env(cls, checkpoint_subdir: str) -> "AlertKafkaConfig":
        checkpoint_root = Path(os.getenv("CHECKPOINT_ROOT", "./storage/checkpoints"))
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("TOPIC_SIGNALS_ALERTS", "market.signals.alerts"),
            checkpoint_path=str(checkpoint_root / checkpoint_subdir),
            trigger_processing_time=os.getenv("ALERT_TRIGGER_PROCESSING_TIME"),
        )


def format_alert_timestamp(timestamp_col: Column) -> Column:
    return F.date_format(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")


def build_volume_alert_events(volume_spike_df: DataFrame, schema_version: str = "1.0") -> DataFrame:
    return volume_spike_df.where(F.col("signal_strength").isin("MEDIUM", "HIGH")).select(
        F.lit(schema_version).alias("schema_version"),
        F.col("symbol"),
        F.lit("VOLUME_SPIKE").alias("alert_type"),
        F.col("signal_strength").alias("severity"),
        format_alert_timestamp(F.col("window_start")).alias("window_start"),
        format_alert_timestamp(F.col("window_end")).alias("window_end"),
        F.col("volume_spike_ratio").cast("double").alias("metric_value"),
        F.concat(
            F.col("symbol"),
            F.lit(" volume is "),
            F.format_number(F.col("volume_spike_ratio"), 2),
            F.lit("x above recent baseline"),
        ).alias("description"),
        format_alert_timestamp(F.current_timestamp()).alias("created_at"),
    )


def build_price_alert_events(price_alert_df: DataFrame, schema_version: str = "1.0") -> DataFrame:
    return price_alert_df.select(
        F.lit(schema_version).alias("schema_version"),
        F.col("symbol"),
        F.col("alert_type"),
        F.col("severity"),
        format_alert_timestamp(F.col("window_start")).alias("window_start"),
        format_alert_timestamp(F.col("window_end")).alias("window_end"),
        F.col("price_change_pct").cast("double").alias("metric_value"),
        F.concat(
            F.col("symbol"),
            F.lit(" moved "),
            F.format_number(F.col("price_change_pct"), 2),
            F.lit("% within the 5-minute window"),
        ).alias("description"),
        format_alert_timestamp(F.current_timestamp()).alias("created_at"),
    )


def to_kafka_messages(alert_events_df: DataFrame) -> DataFrame:
    return alert_events_df.select(
        F.col("symbol").cast("string").alias("key"),
        F.to_json(F.struct(*(F.col(column_name) for column_name in ALERT_COLUMNS))).alias("value"),
    )


def write_alerts_to_kafka(kafka_messages_df: DataFrame, config: AlertKafkaConfig) -> StreamingQuery:
    writer = (
        kafka_messages_df.writeStream.format("kafka")
        .queryName(f"publish-{config.topic}")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("topic", config.topic)
        .option("checkpointLocation", config.checkpoint_path)
        .outputMode("append")
    )

    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)

    return writer.start()
