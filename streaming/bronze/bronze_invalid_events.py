from __future__ import annotations

from streaming.bronze.common import BronzeJobConfig, run_bronze_job


def main() -> None:
    config = BronzeJobConfig.from_env(
        app_name="bronze-market-invalid-events",
        topic_env="TOPIC_EVENTS_INVALID",
        default_topic="market.events.invalid",
        table_name="bronze_market_invalid_events",
        checkpoint_subdir="bronze/invalid_events",
    )
    run_bronze_job(config)


if __name__ == "__main__":
    main()
