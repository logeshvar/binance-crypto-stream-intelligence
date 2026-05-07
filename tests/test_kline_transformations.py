from __future__ import annotations

from decimal import Decimal

from streaming.silver.transformations import transform_klines
from tests.spark_bronze_fixtures import bronze_df, kline_event


def test_kline_transformations_parse_cast_and_filter_invalid_records(spark_session):
    source_df = bronze_df(
        spark_session,
        "market.klines.raw",
        [
            kline_event(symbol="ethusdt"),
            kline_event(symbol="ETHUSDT", kline_start_time=None),
            kline_event(symbol="ETHUSDT", volume="-0.01"),
        ],
    )

    rows = transform_klines(source_df).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "ETHUSDT"
    assert row.open_price == Decimal("3120.100000000000000000")
    assert row.high_price == Decimal("3124.000000000000000000")
    assert row.low_price == Decimal("3118.450000000000000000")
    assert row.close_price == Decimal("3122.300000000000000000")
    assert row.volume == Decimal("18.420000000000000000")
    assert row.number_of_trades == 394
    assert row.is_closed is True
    assert row.process_time is not None
