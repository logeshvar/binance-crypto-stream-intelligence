from __future__ import annotations

from pyspark.sql import DataFrame

from streaming.silver.common import SilverJobConfig, run_silver_job
from streaming.silver.transformations import transform_tickers


def transform(bronze_df: DataFrame, watermark_delay: str) -> DataFrame:
    return transform_tickers(bronze_df, watermark_delay=watermark_delay)


def main() -> None:
    config = SilverJobConfig.from_env(
        app_name="silver-market-tickers",
        source_table_name="bronze_market_tickers_raw",
        output_table_name="silver_market_tickers",
        checkpoint_subdir="silver/tickers",
    )
    run_silver_job(config, transform)


if __name__ == "__main__":
    main()
