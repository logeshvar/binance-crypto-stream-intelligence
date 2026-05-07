from __future__ import annotations

from decimal import Decimal

from streaming.silver.transformations import transform_tickers
from tests.spark_bronze_fixtures import bronze_df, ticker_event


def test_ticker_transformations_parse_cast_and_filter_invalid_records(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.tickers.raw",
        [
            ticker_event(symbol="solusdt"),
            ticker_event(symbol="SOLUSDT", last_price="0"),
            ticker_event(symbol="SOLUSDT", volume="-1"),
        ],
    )

    rows = transform_tickers(source_df).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "SOLUSDT"
    assert row.last_price == Decimal("153.700000000000000000")
    assert row.price_change == Decimal("1.250000000000000000")
    assert row.price_change_percent == Decimal("0.820000000000000000")
    assert row.weighted_avg_price == Decimal("152.430000000000000000")
    assert row.high_price == Decimal("155.100000000000000000")
    assert row.low_price == Decimal("149.800000000000000000")
    assert row.volume == Decimal("512430.120000000000000000")
    assert row.quote_volume == Decimal("78100234.990000000000000000")
    assert row.process_time is not None
