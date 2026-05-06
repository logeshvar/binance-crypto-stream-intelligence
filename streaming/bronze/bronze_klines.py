from __future__ import annotations

from streaming.bronze.common import BronzeJobConfig, run_bronze_job


def main() -> None:
    config = BronzeJobConfig.from_env(
        app_name="bronze-market-klines-raw",
        topic_env="TOPIC_KLINES_RAW",
        default_topic="market.klines.raw",
        table_name="bronze_market_klines_raw",
        checkpoint_subdir="bronze/klines",
    )
    run_bronze_job(config)


if __name__ == "__main__":
    main()
