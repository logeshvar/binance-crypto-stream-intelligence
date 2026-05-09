from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_ROOT = REPO_ROOT / "storage" / "gold"

GOLD_TABLE_NAMES = [
    "gold_symbol_1min_ohlc",
    "gold_symbol_5min_trade_summary",
    "gold_symbol_5min_volatility",
    "gold_volume_spike_signals",
    "gold_price_movement_alerts",
    "gold_market_watchlist_summary",
]

ALERT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("symbol", StringType()),
        StructField("alert_type", StringType()),
        StructField("severity", StringType()),
        StructField("window_start", StringType()),
        StructField("window_end", StringType()),
        StructField("metric_value", DoubleType()),
        StructField("description", StringType()),
        StructField("created_at", StringType()),
    ]
)


def get_gold_root() -> Path:
    return Path(os.getenv("GOLD_PATH", str(DEFAULT_GOLD_ROOT))).expanduser().resolve()


def get_gold_table_paths(gold_root: Path | None = None) -> dict[str, Path]:
    root = gold_root or get_gold_root()
    return {table_name: root / table_name for table_name in GOLD_TABLE_NAMES}


def delta_table_exists(path: Path) -> bool:
    return (path / "_delta_log").exists()


def read_delta_table(spark: SparkSession, path: Path) -> DataFrame | None:
    if not delta_table_exists(path):
        return None
    return spark.read.format("delta").load(str(path))


def empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def latest_watchlist(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_market_watchlist_summary"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "latest_price",
                "price_change_5m_pct",
                "volume_5m",
                "volatility_level",
                "latest_signal",
                "last_updated_time",
            ]
        )

    window = Window.partitionBy("symbol").orderBy(F.col("last_updated_time").desc())
    return (
        df.withColumn("rank", F.row_number().over(window))
        .where(F.col("rank") == 1)
        .drop("rank")
        .orderBy(F.col("volume_5m").desc())
        .limit(limit)
        .toPandas()
    )


def latest_by_symbol(df: DataFrame, time_col: str) -> DataFrame:
    window = Window.partitionBy("symbol").orderBy(F.col(time_col).desc())
    return df.withColumn("rank", F.row_number().over(window)).where(F.col("rank") == 1).drop("rank")


def add_attention_score(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return snapshot_df

    enriched = snapshot_df.copy()
    price_change_pct = pd.to_numeric(enriched.get("price_change_5m_pct"), errors="coerce")
    price_change = price_change_pct.fillna(0).abs()
    spike_ratio = pd.to_numeric(enriched.get("volume_spike_ratio"), errors="coerce").fillna(0)
    signal_bonus = enriched.get("signal_strength", pd.Series(index=enriched.index, dtype="object")).map(
        {"HIGH": 3.0, "MEDIUM": 1.5, "LOW": 0.0}
    ).fillna(0)
    volatility_bonus = enriched.get("volatility_level", pd.Series(index=enriched.index, dtype="object")).map(
        {"HIGH": 2.0, "MEDIUM": 1.0, "LOW": 0.0}
    ).fillna(0)

    enriched["attention_score"] = (price_change * 1.5 + spike_ratio + signal_bonus + volatility_bonus).round(3)
    enriched["market_direction"] = price_change_direction(price_change_pct)
    enriched["attention_reason"] = enriched.apply(attention_reason, axis=1)
    return enriched.sort_values(["attention_score", "volume_5m"], ascending=[False, False])


def price_change_direction(price_change_pct: pd.Series) -> pd.Series:
    def classify(value: float) -> str:
        if pd.isna(value) or abs(value) < 0.1:
            return "FLAT"
        return "UP" if value > 0 else "DOWN"

    return price_change_pct.map(classify)


def attention_reason(row: pd.Series) -> str:
    spike_ratio = pd.to_numeric(row.get("volume_spike_ratio"), errors="coerce")
    price_change = pd.to_numeric(row.get("price_change_5m_pct"), errors="coerce")
    signal_strength = row.get("signal_strength")
    volatility_level = row.get("volatility_level")

    if signal_strength == "HIGH" or (not pd.isna(spike_ratio) and spike_ratio >= 2.0):
        return "Volume spike"
    if not pd.isna(price_change) and abs(price_change) >= 1.5:
        return "Sharp price move"
    if volatility_level == "HIGH":
        return "High volatility"
    if signal_strength == "MEDIUM" or (not pd.isna(spike_ratio) and spike_ratio >= 1.5):
        return "Elevated volume"
    return "Normal"


def market_snapshot(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    paths = get_gold_table_paths()
    watchlist_df = read_delta_table(spark, paths["gold_market_watchlist_summary"])
    if watchlist_df is None:
        return empty_frame(
            [
                "symbol",
                "latest_price",
                "price_change_5m_pct",
                "volume_5m",
                "volatility_level",
                "latest_signal",
                "volume_spike_ratio",
                "signal_strength",
                "market_direction",
                "attention_reason",
                "attention_score",
                "last_updated_time",
            ]
        )

    latest_watchlist_df = latest_by_symbol(watchlist_df, "last_updated_time").alias("watchlist")

    spike_df = read_delta_table(spark, paths["gold_volume_spike_signals"])
    if spike_df is not None:
        latest_spike_df = (
            latest_by_symbol(spike_df, "window_end")
            .select(
                "symbol",
                F.col("volume_spike_ratio").cast("double").alias("volume_spike_ratio"),
                "signal_strength",
                F.col("window_end").alias("volume_spike_window_end"),
            )
            .alias("spikes")
        )
        latest_watchlist_df = latest_watchlist_df.join(latest_spike_df, on="symbol", how="left")
    else:
        latest_watchlist_df = (
            latest_watchlist_df.withColumn("volume_spike_ratio", F.lit(None).cast("double"))
            .withColumn("signal_strength", F.lit(None).cast("string"))
            .withColumn("volume_spike_window_end", F.lit(None).cast("timestamp"))
        )

    snapshot_df = latest_watchlist_df.select(
        "symbol",
        "latest_price",
        "price_change_5m_pct",
        "volume_5m",
        "volatility_level",
        "latest_signal",
        "volume_spike_ratio",
        "signal_strength",
        "last_updated_time",
    ).toPandas()

    return add_attention_score(snapshot_df).head(limit).reset_index(drop=True)


def latest_volume_spikes(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_volume_spike_signals"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "current_volume",
                "baseline_volume",
                "volume_spike_ratio",
                "signal_strength",
                "number_of_trades",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.col("volume_spike_ratio").desc()).limit(limit).toPandas()


def latest_volatility(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_5min_volatility"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "avg_price",
                "price_change_pct",
                "volatility_level",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.abs(F.col("price_change_pct")).desc()).limit(limit).toPandas()


def latest_ohlc(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_1min_ohlc"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "trade_count",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.col("trade_count").desc()).limit(limit).toPandas()


def latest_price_alerts(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_price_movement_alerts"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "start_price",
                "end_price",
                "price_change_pct",
                "alert_type",
                "severity",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.abs(F.col("price_change_pct")).desc()).limit(limit).toPandas()


def symbol_ohlc_history(spark: SparkSession, symbol: str, limit: int = 120) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_1min_ohlc"])
    if df is None:
        return empty_frame(["window_start", "window_end", "open_price", "high_price", "low_price", "close_price"])
    return (
        df.where(F.col("symbol") == symbol)
        .orderBy(F.col("window_end").desc())
        .limit(limit)
        .orderBy(F.col("window_end").asc())
        .toPandas()
    )


def symbol_trade_summary_history(spark: SparkSession, symbol: str, limit: int = 60) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_5min_trade_summary"])
    if df is None:
        return empty_frame(["window_start", "window_end", "total_volume", "number_of_trades"])
    return (
        df.where(F.col("symbol") == symbol)
        .orderBy(F.col("window_end").desc())
        .limit(limit)
        .orderBy(F.col("window_end").asc())
        .toPandas()
    )


def symbol_spike_history(spark: SparkSession, symbol: str, limit: int = 60) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_volume_spike_signals"])
    if df is None:
        return empty_frame(["window_start", "window_end", "volume_spike_ratio", "signal_strength"])
    return (
        df.where(F.col("symbol") == symbol)
        .orderBy(F.col("window_end").desc(), F.col("volume_spike_ratio").desc())
        .limit(limit)
        .orderBy(F.col("window_end").asc())
        .toPandas()
    )


def table_health(spark: SparkSession) -> pd.DataFrame:
    rows = []
    for table_name, path in get_gold_table_paths().items():
        exists = delta_table_exists(path)
        row = {
            "table": table_name,
            "path": str(path),
            "delta_log_exists": exists,
            "row_count": 0,
            "latest_time": None,
        }
        if exists:
            df = spark.read.format("delta").load(str(path))
            row["row_count"] = df.count()
            time_cols = [col for col in ("window_end", "last_updated_time") if col in df.columns]
            if time_cols:
                row["latest_time"] = df.agg(F.max(time_cols[0]).alias("latest_time")).collect()[0]["latest_time"]
        rows.append(row)
    return pd.DataFrame(rows)


def freshness_label(latest_time: object, now: datetime | None = None) -> str:
    if latest_time is None or pd.isna(latest_time):
        return "NO_DATA"

    now_utc = now or datetime.now(timezone.utc)
    timestamp = pd.Timestamp(latest_time)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    age_seconds = (pd.Timestamp(now_utc) - timestamp).total_seconds()

    if age_seconds <= 120:
        return "FRESH"
    if age_seconds <= 900:
        return "LAGGING"
    return "STALE"


def overview_metrics(spark: SparkSession) -> dict[str, object]:
    watchlist = market_snapshot(spark)
    spikes = latest_volume_spikes(spark)
    price_alerts = latest_price_alerts(spark)
    health = table_health(spark)

    latest_time = None
    if not health.empty and "latest_time" in health:
        latest_values = health["latest_time"].dropna()
        if not latest_values.empty:
            latest_time = max(latest_values)

    strongest_spike = pd.to_numeric(watchlist.get("volume_spike_ratio"), errors="coerce").max()
    biggest_abs_move = pd.to_numeric(watchlist.get("price_change_5m_pct"), errors="coerce").abs().max()

    return {
        "symbols": int(watchlist["symbol"].nunique()) if not watchlist.empty else 0,
        "high_volume_spikes": int((spikes.get("signal_strength") == "HIGH").sum()) if not spikes.empty else 0,
        "price_alerts": int(len(price_alerts)),
        "strongest_spike": float(strongest_spike) if not watchlist.empty and not pd.isna(strongest_spike) else 0.0,
        "biggest_abs_move": float(biggest_abs_move) if not watchlist.empty and not pd.isna(biggest_abs_move) else 0.0,
        "latest_gold_time": latest_time,
        "freshness": freshness_label(latest_time),
    }


def alert_topic_messages(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "market.signals.alerts",
    limit: int = 50,
) -> pd.DataFrame:
    try:
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
    except Exception:
        return empty_frame(
            [
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "symbol_key",
                "alert_type",
                "severity",
                "metric_value",
                "description",
                "created_at",
            ]
        )

    parsed = (
        kafka_df.select(
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("symbol_key"),
            F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("alert"),
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "symbol_key",
            "alert.*",
        )
        .orderBy(F.col("kafka_timestamp").desc(), F.col("offset").desc())
    )
    return parsed.limit(limit).toPandas()
