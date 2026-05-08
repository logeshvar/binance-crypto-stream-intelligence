from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from streaming.spark_session import create_spark_session


@dataclass(frozen=True)
class AlertPublisherConfig:
    app_name: str
    source_table_name: str
    source_path: str
    checkpoint_subdir: str
    max_files_per_trigger: str | None
    spark_log_level: str

    @classmethod
    def from_env(
        cls,
        *,
        app_name: str,
        source_table_name: str,
        checkpoint_subdir: str,
    ) -> "AlertPublisherConfig":
        gold_root = Path(os.getenv("GOLD_PATH", "./storage/gold"))
        return cls(
            app_name=app_name,
            source_table_name=source_table_name,
            source_path=str(gold_root / source_table_name),
            checkpoint_subdir=checkpoint_subdir,
            max_files_per_trigger=os.getenv("ALERT_MAX_FILES_PER_TRIGGER"),
            spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        )


def create_alert_spark_session(app_name: str) -> SparkSession:
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("ALERT_SPARK_UI_PORT", "4070"))
    return create_spark_session(app_name)


def read_gold_delta_stream(spark: SparkSession, config: AlertPublisherConfig) -> DataFrame:
    reader = spark.readStream.format("delta")
    if config.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", config.max_files_per_trigger)
    return reader.load(config.source_path)
