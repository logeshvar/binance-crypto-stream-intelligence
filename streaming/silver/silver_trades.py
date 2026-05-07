from __future__ import annotations

from pyspark.sql import DataFrame

from streaming.silver.common import SilverJobConfig, run_silver_job
from streaming.silver.transformations import transform_trades


def transform(bronze_df: DataFrame, watermark_delay: str) -> DataFrame:
    return transform_trades(bronze_df, watermark_delay=watermark_delay)


def main() -> None:
    config = SilverJobConfig.from_env(
        app_name="silver-market-trades",
        source_table_name="bronze_market_trades_raw",
        output_table_name="silver_market_trades",
        checkpoint_subdir="silver/trades",
    )
    run_silver_job(config, transform)


if __name__ == "__main__":
    main()
