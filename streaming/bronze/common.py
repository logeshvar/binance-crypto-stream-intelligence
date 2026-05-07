from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp
from pyspark.sql.streaming import StreamingQuery

from streaming.spark_session import create_spark_session


@dataclass(frozen=True)
class BronzeJobConfig:
    app_name: str
    topic: str
    table_name: str
    output_path: str
    checkpoint_path: str
    kafka_bootstrap_servers: str
    starting_offsets: str
    fail_on_data_loss: str
    max_offsets_per_trigger: str | None
    trigger_processing_time: str | None
    spark_log_level: str

    @classmethod
    def from_env(
        cls,
        *,
        app_name: str,
        topic_env: str,
        default_topic: str,
        table_name: str,
        checkpoint_subdir: str,
    ) -> "BronzeJobConfig":
        bronze_root = Path(os.getenv("BRONZE_PATH", "./storage/bronze"))
        checkpoint_root = Path(os.getenv("CHECKPOINT_ROOT", "./storage/checkpoints"))

        return cls(
            app_name=app_name,
            topic=os.getenv(topic_env) or default_topic,
            table_name=table_name,
            output_path=str(bronze_root / table_name),
            checkpoint_path=str(checkpoint_root / checkpoint_subdir),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            starting_offsets=os.getenv("BRONZE_STARTING_OFFSETS", "latest"),
            fail_on_data_loss=os.getenv("BRONZE_FAIL_ON_DATA_LOSS", "false"),
            max_offsets_per_trigger=os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER"),
            trigger_processing_time=os.getenv("BRONZE_TRIGGER_PROCESSING_TIME"),
            spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
        )


def read_kafka_stream(spark: SparkSession, config: BronzeJobConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", config.fail_on_data_loss)
    )

    if config.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)

    return reader.load()


def select_bronze_columns(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingest_time"),
        current_date().alias("process_date"),
    )


def write_bronze_stream(bronze_df: DataFrame, config: BronzeJobConfig) -> StreamingQuery:
    writer = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .queryName(config.table_name)
        .option("path", config.output_path)
        .option("checkpointLocation", config.checkpoint_path)
        .partitionBy("process_date")
    )

    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)

    return writer.start()


def run_bronze_job(config: BronzeJobConfig) -> None:
    spark = create_spark_session(config.app_name)
    spark.sparkContext.setLogLevel(config.spark_log_level)

    kafka_df = read_kafka_stream(spark, config)
    bronze_df = select_bronze_columns(kafka_df)
    query = write_bronze_stream(bronze_df, config)
    query.awaitTermination()
