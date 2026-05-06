from __future__ import annotations

from streaming.bronze.common import BronzeJobConfig, run_bronze_job


def main() -> None:
    config = BronzeJobConfig.from_env(
        app_name="bronze-market-trades-raw",
        topic_env="TOPIC_TRADES_RAW",
        default_topic="market.trades.raw",
        table_name="bronze_market_trades_raw",
        checkpoint_subdir="bronze/trades",
    )
    run_bronze_job(config)


if __name__ == "__main__":
    main()
