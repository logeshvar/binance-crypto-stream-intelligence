from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.spark_session import create_spark_session


@dataclass(frozen=True)
class GoldJobConfig:
    app_name: str
    source_table_name: str
    output_table_name: str
    source_path: str
    output_path: str
    checkpoint_path: str
    watermark_delay: str
    max_files_per_trigger: str | None
    trigger_processing_time: str | None
    spark_log_level: str

    @classmethod
    def from_env(
        cls,
        *,
        app_name: str,
        source_table_name: str,
        output_table_name: str,
        checkpoint_subdir: str,
        source_root_env: str = "SILVER_PATH",
    ) -> "GoldJobConfig":
        source_root_default = "./storage/gold" if source_root_env == "GOLD_PATH" else "./storage/silver"
        source_root = Path(os.getenv(source_root_env, source_root_default))
        gold_root = Path(os.getenv("GOLD_PATH", "./storage/gold"))
        checkpoint_root = Path(os.getenv("CHECKPOINT_ROOT", "./storage/checkpoints"))

        return cls(
            app_name=app_name,
            source_table_name=source_table_name,
            output_table_name=output_table_name,
            source_path=str(source_root / source_table_name),
            output_path=str(gold_root / output_table_name),
            checkpoint_path=str(checkpoint_root / checkpoint_subdir),
            watermark_delay=os.getenv("GOLD_WATERMARK_DELAY", "10 minutes"),
            max_files_per_trigger=os.getenv("GOLD_MAX_FILES_PER_TRIGGER"),
            trigger_processing_time=os.getenv("GOLD_TRIGGER_PROCESSING_TIME"),
            spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        )


def apply_event_time_watermark(df: DataFrame, event_time_col: str, watermark_delay: str) -> DataFrame:
    if df.isStreaming:
        return df.withWatermark(event_time_col, watermark_delay)
    return df


def read_silver_delta_stream(spark: SparkSession, config: GoldJobConfig) -> DataFrame:
    reader = spark.readStream.format("delta")
    if config.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", config.max_files_per_trigger)
    return reader.load(config.source_path)


def write_gold_stream(gold_df: DataFrame, config: GoldJobConfig) -> StreamingQuery:
    writer = (
        gold_df.writeStream.format("delta")
        .outputMode("append")
        .queryName(config.output_table_name)
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_path)
    )

    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)

    return writer.start()


def run_gold_job(
    config: GoldJobConfig,
    transform: Callable[[DataFrame, str], DataFrame],
) -> None:
    spark = create_spark_session(config.app_name)
    spark.sparkContext.setLogLevel(config.spark_log_level)

    silver_df = read_silver_delta_stream(spark, config)
    gold_df = transform(silver_df, config.watermark_delay)
    query = write_gold_stream(gold_df, config)
    query.awaitTermination()
