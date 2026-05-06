from __future__ import annotations

from streaming.bronze.common import BronzeJobConfig, run_bronze_job


def main() -> None:
    config = BronzeJobConfig.from_env(
        app_name="bronze-market-tickers-raw",
        topic_env="TOPIC_TICKERS_RAW",
        default_topic="market.tickers.raw",
        table_name="bronze_market_tickers_raw",
        checkpoint_subdir="bronze/tickers",
    )
    run_bronze_job(config)


if __name__ == "__main__":
    main()
