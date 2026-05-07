from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.spark_session import create_spark_session


@dataclass(frozen=True)
class SilverJobConfig:
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
    ) -> "SilverJobConfig":
        bronze_root = Path(os.getenv("BRONZE_PATH", "./storage/bronze"))
        silver_root = Path(os.getenv("SILVER_PATH", "./storage/silver"))
        checkpoint_root = Path(os.getenv("CHECKPOINT_ROOT", "./storage/checkpoints"))

        return cls(
            app_name=app_name,
            source_table_name=source_table_name,
            output_table_name=output_table_name,
            source_path=str(bronze_root / source_table_name),
            output_path=str(silver_root / output_table_name),
            checkpoint_path=str(checkpoint_root / checkpoint_subdir),
            watermark_delay=os.getenv("SILVER_WATERMARK_DELAY", "10 minutes"),
            max_files_per_trigger=os.getenv("SILVER_MAX_FILES_PER_TRIGGER"),
            trigger_processing_time=os.getenv("SILVER_TRIGGER_PROCESSING_TIME"),
            spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        )


def read_bronze_delta_stream(spark: SparkSession, config: SilverJobConfig) -> DataFrame:
    reader = spark.readStream.format("delta")
    if config.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", config.max_files_per_trigger)
    return reader.load(config.source_path)


def write_silver_stream(silver_df: DataFrame, config: SilverJobConfig) -> StreamingQuery:
    writer = (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .queryName(config.output_table_name)
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_path)
    )

    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)

    return writer.start()


def run_silver_job(
    config: SilverJobConfig,
    transform: Callable[[DataFrame, str], DataFrame],
) -> None:
    spark = create_spark_session(config.app_name)
    spark.sparkContext.setLogLevel(config.spark_log_level)

    bronze_df = read_bronze_delta_stream(spark, config)
    silver_df = transform(bronze_df, config.watermark_delay)
    query = write_silver_stream(silver_df, config)
    query.awaitTermination()
