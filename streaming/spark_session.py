from __future__ import annotations

import os
import sys
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


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
