from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp
from pyspark.sql.streaming import StreamingQuery


def prepare_pyspark_environment() -> Path:
    if os.getenv("USE_SYSTEM_SPARK_HOME", "false").lower() not in {"1", "true", "yes"}:
        os.environ.pop("SPARK_HOME", None)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    ivy_dir = Path(os.getenv("SPARK_IVY_DIR", "./storage/spark/ivy2")).resolve()
    ivy_dir.mkdir(parents=True, exist_ok=True)
    return ivy_dir


def resolve_spark_extra_packages() -> list[str]:
    scala_binary_version = os.getenv("SPARK_SCALA_BINARY_VERSION", "2.13")
    kafka_package = os.getenv("SPARK_KAFKA_PACKAGE")
    if not kafka_package:
        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary_version}:{pyspark.__version__}"
    extra_packages = [kafka_package]

    configured_packages = os.getenv("SPARK_EXTRA_PACKAGES", "")
    extra_packages.extend(
        package.strip()
        for package in configured_packages.split(",")
        if package.strip()
    )
    return extra_packages


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


def create_spark_session(app_name: str) -> SparkSession:
    ivy_dir = prepare_pyspark_environment()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.jars.ivy", str(ivy_dir))
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=resolve_spark_extra_packages(),
    ).getOrCreate()


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
